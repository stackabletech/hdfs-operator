//! This module configures required HDFS containers and their volumes.
//!
//! ## Features
//!
//! - Create all required main, side and init containers for Namenodes, Datanodes and Journalnodes
//! - Adds required volumes and volume mounts
//! - Set required env variables
//! - Build container commands and args
//! - Set resources
//! - Add tcp probes and container ports (to the main containers)
//!
use crate::product_logging::{
    FORMAT_NAMENODES_LOG4J_CONFIG_FILE, FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE, HDFS_LOG4J_CONFIG_FILE,
    MAX_LOG_FILES_SIZE_IN_MIB, STACKABLE_LOG_DIR, WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
    ZKFC_LOG4J_CONFIG_FILE,
};

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{
        CORE_SITE_XML, DATANODE_ROOT_DATA_DIR_PREFIX, DEFAULT_DATA_NODE_METRICS_PORT,
        DEFAULT_JOURNAL_NODE_METRICS_PORT, DEFAULT_NAME_NODE_METRICS_PORT, HDFS_SITE_XML,
        LOG4J_PROPERTIES, NAMENODE_ROOT_DATA_DIR, SERVICE_PORT_NAME_IPC, SERVICE_PORT_NAME_RPC,
        STACKABLE_ROOT_DATA_DIR,
    },
    storage::DataNodeStorageConfig,
    DataNodeContainer, HdfsPodRef, HdfsRole, MergedConfig, NameNodeContainer,
};
use stackable_operator::product_logging::spec::LogLevel;
use stackable_operator::{
    builder::{ContainerBuilder, PodBuilder, VolumeBuilder, VolumeMountBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapKeySelector, ConfigMapVolumeSource, Container, ContainerPort,
            EmptyDirVolumeSource, EnvVar, EnvVarSource, ObjectFieldSelector, PersistentVolumeClaim,
            Probe, ResourceRequirements, TCPSocketAction, Volume, VolumeMount,
        },
        apimachinery::pkg::{api::resource::Quantity, util::intstr::IntOrString},
    },
    kube::ResourceExt,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging,
    product_logging::spec::{
        ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice, CustomContainerLogConfig,
    },
};
use std::{cmp, collections::BTreeMap, str::FromStr};
use strum::{Display, EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Invalid java heap config for [{role}]"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
        role: String,
    },
    #[snafu(display("Could not determine any ContainerConfig actions for [{container_name}]. Container not recognized."))]
    UnrecognizedContainerName { container_name: String },
    #[snafu(display("Invalid container name [{name}]"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
        name: String,
    },
}

/// ContainerConfig contains information to create all main, side and init containers for
/// the HDFS cluster.
#[derive(Display)]
pub enum ContainerConfig {
    Hdfs {
        /// HDFS role (name-, data-, journal-node) which will be the container_name.
        role: HdfsRole,
        /// The container name derived from the provided role.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
        /// Readiness and liveness probe service port name.
        tcp_socket_action_port_name: &'static str,
        /// The JMX Exporter metrics port.
        metrics_port: u16,
    },
    Zkfc {
        /// The provided custom container name.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
    },
    FormatNameNodes {
        /// The provided custom container name.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
    },
    FormatZooKeeper {
        /// The provided custom container name.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
    },
    WaitForNameNodes {
        /// The provided custom container name.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
    },
}

impl ContainerConfig {
    // volumes
    pub const STACKABLE_LOG_VOLUME_MOUNT_NAME: &'static str = "log";
    pub const DATA_VOLUME_MOUNT_NAME: &'static str = "data";
    pub const HDFS_CONFIG_VOLUME_MOUNT_NAME: &'static str = "hdfs-config";

    const HDFS_LOG_VOLUME_MOUNT_NAME: &'static str = "hdfs-log-config";
    const ZKFC_CONFIG_VOLUME_MOUNT_NAME: &'static str = "zkfc-config";
    const ZKFC_LOG_VOLUME_MOUNT_NAME: &'static str = "zkfc-log-config";
    const FORMAT_NAMENODES_CONFIG_VOLUME_MOUNT_NAME: &'static str = "format-namenodes-config";
    const FORMAT_NAMENODES_LOG_VOLUME_MOUNT_NAME: &'static str = "format-namenodes-log-config";
    const FORMAT_ZOOKEEPER_CONFIG_VOLUME_MOUNT_NAME: &'static str = "format-zookeeper-config";
    const FORMAT_ZOOKEEPER_LOG_VOLUME_MOUNT_NAME: &'static str = "format-zookeeper-log-config";
    const WAIT_FOR_NAMENODES_CONFIG_VOLUME_MOUNT_NAME: &'static str = "wait-for-namenodes-config";
    const WAIT_FOR_NAMENODES_LOG_VOLUME_MOUNT_NAME: &'static str = "wait-for-namenodes-log-config";

    const JVM_HEAP_FACTOR: f32 = 0.8;
    const HADOOP_HOME: &'static str = "/stackable/hadoop";

    // We have a maximum of 4 continuous logging files for Namenodes. Datanodes and Journalnodes
    // require less. We add another 1MB as buffer for each possible logging file.
    // - name node main container
    // - zkfc side container
    // - format namenode init container
    // - format zookeeper init container
    const LOG_VOLUME_SIZE_IN_MIB: u32 = 4 * (MAX_LOG_FILES_SIZE_IN_MIB + 1);

    /// Add all main, side and init containers as well as required volumes to the pod builder.
    #[allow(clippy::too_many_arguments)]
    pub fn add_containers_and_volumes(
        pb: &mut PodBuilder,
        role: &HdfsRole,
        resolved_product_image: &ResolvedProductImage,
        merged_config: &(dyn MergedConfig + Send + 'static),
        env_overrides: Option<&BTreeMap<String, String>>,
        zk_config_map_name: &str,
        object_name: &str,
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<(), Error> {
        // HDFS main container
        let main_container_config = Self::from(role.clone());
        pb.add_volumes(main_container_config.volumes(merged_config, object_name));
        pb.add_container(main_container_config.main_container(
            resolved_product_image,
            zk_config_map_name,
            env_overrides,
            merged_config,
        )?);

        // Vector side container
        if merged_config.vector_logging_enabled() {
            pb.add_container(product_logging::framework::vector_container(
                resolved_product_image,
                ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME,
                Some(&merged_config.vector_logging()),
            ));
        }

        // role specific pod settings configured here
        match role {
            HdfsRole::NameNode => {
                // Zookeeper fail over container
                let zkfc_container_config = Self::try_from(NameNodeContainer::Zkfc.to_string())?;
                pb.add_volumes(zkfc_container_config.volumes(merged_config, object_name));
                pb.add_container(zkfc_container_config.main_container(
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    merged_config,
                )?);

                // Format namenode init container
                let format_namenodes_container_config =
                    Self::try_from(NameNodeContainer::FormatNameNodes.to_string())?;
                pb.add_volumes(
                    format_namenodes_container_config.volumes(merged_config, object_name),
                );
                pb.add_init_container(format_namenodes_container_config.init_container(
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                )?);

                // Format ZooKeeper init container
                let format_zookeeper_container_config =
                    Self::try_from(NameNodeContainer::FormatZooKeeper.to_string())?;
                pb.add_volumes(
                    format_zookeeper_container_config.volumes(merged_config, object_name),
                );
                pb.add_init_container(format_zookeeper_container_config.init_container(
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                )?);
            }
            HdfsRole::DataNode => {
                // Wait for namenode init container
                let wait_for_namenodes_container_config =
                    Self::try_from(DataNodeContainer::WaitForNameNodes.to_string())?;
                pb.add_volumes(
                    wait_for_namenodes_container_config.volumes(merged_config, object_name),
                );
                pb.add_init_container(wait_for_namenodes_container_config.init_container(
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                )?);
            }
            HdfsRole::JournalNode => {}
        }

        Ok(())
    }

    pub fn volume_claim_templates(
        role: &HdfsRole,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Option<Vec<PersistentVolumeClaim>> {
        match role {
            HdfsRole::NameNode | HdfsRole::JournalNode => merged_config.resources().map(|r| {
                vec![r.storage.data.build_pvc(
                    ContainerConfig::DATA_VOLUME_MOUNT_NAME,
                    Some(vec!["ReadWriteOnce"]),
                )]
            }),
            HdfsRole::DataNode => merged_config
                .data_node_resources()
                .map(|r| r.storage)
                .map(|storage| DataNodeStorageConfig { pvcs: storage }.build_pvcs()),
        }
    }

    /// Creates the main/side containers for:
    /// - Namenode main process
    /// - Namenode ZooKeeper fail over controller (ZKFC)
    /// - Datanode main process
    /// - Journalnode main process
    fn main_container(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Result<Container, Error> {
        let mut cb =
            ContainerBuilder::new(self.name()).with_context(|_| InvalidContainerNameSnafu {
                name: self.name().to_string(),
            })?;

        let resources = self.resources(merged_config);

        cb.image_from_product_image(resolved_product_image)
            .command(self.command())
            .args(self.args(merged_config, &[]))
            .add_env_vars(self.env(zookeeper_config_map_name, env_overrides, resources.as_ref()))
            .add_volume_mounts(self.volume_mounts(merged_config))
            .add_container_ports(self.container_ports());

        if let Some(resources) = resources {
            cb.resources(resources);
        }

        if let Some(probe) = self.tcp_socket_action_probe(10, 10) {
            cb.readiness_probe(probe.clone());
            cb.liveness_probe(probe);
        }

        Ok(cb.build())
    }

    /// Creates respective init containers for:
    /// - Namenode (format-namenodes, format-zookeeper)
    /// - Datanode (wait-for-namenodes)
    fn init_container(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Result<Container, Error> {
        Ok(ContainerBuilder::new(self.name())
            .with_context(|_| InvalidContainerNameSnafu { name: self.name() })?
            .image_from_product_image(resolved_product_image)
            .command(self.command())
            .args(self.args(merged_config, namenode_podrefs))
            .add_env_vars(self.env(zookeeper_config_map_name, env_overrides, None))
            .add_volume_mounts(self.volume_mounts(merged_config))
            .build())
    }

    /// Return the container name.
    fn name(&self) -> &str {
        match &self {
            ContainerConfig::Hdfs { container_name, .. } => container_name.as_str(),
            ContainerConfig::Zkfc { container_name, .. } => container_name.as_str(),
            ContainerConfig::FormatNameNodes { container_name, .. } => container_name.as_str(),
            ContainerConfig::FormatZooKeeper { container_name, .. } => container_name.as_str(),
            ContainerConfig::WaitForNameNodes { container_name, .. } => container_name.as_str(),
        }
    }

    /// Return volume mount directories depending on the container.
    fn volume_mount_dirs(&self) -> &ContainerVolumeDirs {
        match &self {
            ContainerConfig::Hdfs { volume_mounts, .. } => volume_mounts,
            ContainerConfig::Zkfc { volume_mounts, .. } => volume_mounts,
            ContainerConfig::FormatNameNodes { volume_mounts, .. } => volume_mounts,
            ContainerConfig::FormatZooKeeper { volume_mounts, .. } => volume_mounts,
            ContainerConfig::WaitForNameNodes { volume_mounts, .. } => volume_mounts,
        }
    }

    /// Returns the container command.
    fn command(&self) -> Vec<String> {
        match self {
            ContainerConfig::Hdfs { .. } | ContainerConfig::Zkfc { .. } => vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ],
            ContainerConfig::FormatNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. }
            | ContainerConfig::WaitForNameNodes { .. } => {
                vec!["bash".to_string(), "-c".to_string()]
            }
        }
    }

    /// Returns the container command arguments.
    fn args(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
        namenode_podrefs: &[HdfsPodRef],
    ) -> Vec<String> {
        let mut args = vec![
            self.create_config_directory_cmd(),
            self.copy_hdfs_and_core_site_xml_cmd(),
        ];

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                args.push(self.copy_log4j_properties_cmd(
                    HDFS_LOG4J_CONFIG_FILE,
                    merged_config.hdfs_logging(),
                ));

                // If max(root_log_level, console_log_level) is lower (where TRACE is lowest and
                // NONE is highest) than INFO (e.g. DEBUG, TRACE), add the `--debug` flag to the
                // start arguments to get additional console output (this is not collected by vector).
                // This prints startup scripts debug settings like
                // DEBUG: HADOOP_CONF_DIR=/stackable/config/namenode
                args.push(
                    if use_start_up_script_debug(&merged_config.hdfs_logging()) {
                        format!(
                            "{hadoop_home}/bin/hdfs --debug {role}",
                            hadoop_home = Self::HADOOP_HOME,
                        )
                    } else {
                        format!(
                            "{hadoop_home}/bin/hdfs {role}",
                            hadoop_home = Self::HADOOP_HOME,
                        )
                    },
                );
            }
            ContainerConfig::Zkfc { .. } => {
                if let Some(container_config) = merged_config.zkfc_logging() {
                    args.push(
                        self.copy_log4j_properties_cmd(ZKFC_LOG4J_CONFIG_FILE, container_config),
                    );
                }

                args.push(format!(
                    "{hadoop_home}/bin/hdfs zkfc",
                    hadoop_home = Self::HADOOP_HOME
                ));
            }
            ContainerConfig::FormatNameNodes { .. } => {
                if let Some(container_config) = merged_config.format_namenodes_logging() {
                    args.push(self.copy_log4j_properties_cmd(
                        FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
                        container_config,
                    ));
                }
                // First step we check for active namenodes. This step should return an active namenode
                // for e.g. scaling. It may fail if the active namenode is restarted and the standby
                // namenode takes over.
                // This is why in the second part we check if the node is formatted already via
                // $NAMENODE_DIR/current/VERSION. Then we dont do anything.
                // If there is no active namenode, the current pod is not formatted we format as
                // active namenode. Otherwise as standby node.
                args.push(formatdoc!(r###"
                    echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
                    for id in {pod_names}
                    do
                      echo -n "Checking pod $id... "
                      SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                      if [ "$SERVICE_STATE" == "active" ]
                      then
                        ACTIVE_NAMENODE=$id
                        echo "active"
                        break
                      fi
                      echo ""
                    done

                    set -e
                    if [ ! -f "{NAMENODE_ROOT_DATA_DIR}/current/VERSION" ]
                    then
                      if [ -z ${{ACTIVE_NAMENODE+x}} ]
                      then
                        echo "Create pod $POD_NAME as active namenode."
                        {hadoop_home}/bin/hdfs namenode -format -noninteractive
                      else
                        echo "Create pod $POD_NAME as standby namenode."
                        {hadoop_home}/bin/hdfs namenode -bootstrapStandby -nonInteractive
                      fi
                    else
                      echo "Pod $POD_NAME already formatted. Skipping..."
                    fi"###,
                    hadoop_home = Self::HADOOP_HOME,
                    pod_names = namenode_podrefs
                        .iter()
                        .map(|pod_ref| pod_ref.pod_name.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" "),
                ));
            }
            ContainerConfig::FormatZooKeeper { .. } => {
                if let Some(container_config) = merged_config.format_zookeeper_logging() {
                    args.push(self.copy_log4j_properties_cmd(
                        FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
                        container_config,
                    ));
                }
                args.push(formatdoc!(
                    r###"
                    echo "Attempt to format ZooKeeper..."
                    if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
                      {hadoop_home}/bin/hdfs zkfc -formatZK -nonInteractive || true
                    else
                      echo "ZooKeeper already formatted!"
                    fi"###,
                    hadoop_home = Self::HADOOP_HOME
                ));
            }
            ContainerConfig::WaitForNameNodes { .. } => {
                if let Some(container_config) = merged_config.wait_for_namenodes() {
                    args.push(self.copy_log4j_properties_cmd(
                        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
                        container_config,
                    ));
                }
                args.push(formatdoc!(r###"
                    echo "Waiting for namenodes to get ready:"
                    n=0
                    while [ ${{n}} -lt 12 ];
                    do
                      ALL_NODES_READY=true
                      for id in {pod_names}
                      do
                        echo -n "Checking pod $id... "
                        SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                        if [ "$SERVICE_STATE" = "active" ] || [ "$SERVICE_STATE" = "standby" ]
                        then
                          echo "$SERVICE_STATE"
                        else
                          echo "not ready"
                          ALL_NODES_READY=false
                        fi
                      done
                      if [ "$ALL_NODES_READY" == "true" ]
                      then
                        echo "All namenodes ready!"
                        break
                      fi
                      echo ""
                      n=$(( n  + 1))
                      sleep 5
                    done"###,
                hadoop_home = Self::HADOOP_HOME,
                pod_names = namenode_podrefs
                    .iter()
                    .map(|pod_ref| pod_ref.pod_name.as_ref())
                    .collect::<Vec<&str>>()
                    .join(" ")
                ));
            }
        }
        vec![args.join(" && ")]
    }

    /// Returns the container env variables.
    fn env(
        &self,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        resources: Option<&ResourceRequirements>,
    ) -> Vec<EnvVar> {
        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);

        env.extend(Self::shared_env_vars(
            self.volume_mount_dirs().final_config(),
            zookeeper_config_map_name,
        ));

        if let ContainerConfig::Hdfs { role, .. } = self {
            if let Some(resources) = resources {
                env.push(EnvVar {
                    name: role.hadoop_opts().to_string(),
                    value: self.build_hadoop_opts(resources).ok(),
                    ..EnvVar::default()
                });
            }
        }

        env
    }

    /// Returns the container resources.
    fn resources(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Option<ResourceRequirements> {
        // Only the Hadoop main containers will get resources
        match self {
            ContainerConfig::Hdfs { role, .. } if role != &HdfsRole::DataNode => {
                merged_config.resources().map(|c| c.into())
            }
            ContainerConfig::Hdfs { role, .. } if role == &HdfsRole::DataNode => {
                merged_config.data_node_resources().map(|c| c.into())
            }
            _ => None,
        }
    }

    /// Creates a probe for [`stackable_operator::k8s_openapi::api::core::v1::TCPSocketAction`]
    /// for liveness or readiness probes
    fn tcp_socket_action_probe(
        &self,
        period_seconds: i32,
        initial_delay_seconds: i32,
    ) -> Option<Probe> {
        match self {
            ContainerConfig::Hdfs {
                tcp_socket_action_port_name,
                ..
            } => Some(Probe {
                tcp_socket: Some(TCPSocketAction {
                    port: IntOrString::String(String::from(*tcp_socket_action_port_name)),
                    ..TCPSocketAction::default()
                }),
                period_seconds: Some(period_seconds),
                initial_delay_seconds: Some(initial_delay_seconds),
                ..Probe::default()
            }),
            _ => None,
        }
    }

    /// Return the container volumes.
    fn volumes(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
        object_name: &str,
    ) -> Vec<Volume> {
        let mut volumes = vec![];

        let container_log_config = match self {
            ContainerConfig::Hdfs { .. } => {
                volumes.push(
                    VolumeBuilder::new(ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME)
                        .empty_dir(EmptyDirVolumeSource {
                            medium: None,
                            size_limit: Some(Quantity(format!(
                                "{volume_size_in_mb}Mi",
                                volume_size_in_mb = Self::LOG_VOLUME_SIZE_IN_MIB
                            ))),
                        })
                        .build(),
                );

                Some(merged_config.hdfs_logging())
            }
            ContainerConfig::Zkfc { .. } => merged_config.zkfc_logging(),
            ContainerConfig::FormatNameNodes { .. } => merged_config.format_namenodes_logging(),
            ContainerConfig::FormatZooKeeper { .. } => merged_config.format_zookeeper_logging(),
            ContainerConfig::WaitForNameNodes { .. } => merged_config.wait_for_namenodes(),
        };

        volumes.extend(Self::common_container_volumes(
            container_log_config,
            object_name,
            self.volume_mount_dirs().config_mount_name(),
            self.volume_mount_dirs().log_mount_name(),
        ));

        volumes
    }

    /// Returns the container volume mounts.
    fn volume_mounts(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Vec<VolumeMount> {
        let mut volume_mounts = vec![
            VolumeMountBuilder::new(Self::STACKABLE_LOG_VOLUME_MOUNT_NAME, STACKABLE_LOG_DIR)
                .build(),
            VolumeMountBuilder::new(
                self.volume_mount_dirs().config_mount_name(),
                self.volume_mount_dirs().config_mount(),
            )
            .build(),
            VolumeMountBuilder::new(
                self.volume_mount_dirs().log_mount_name(),
                self.volume_mount_dirs().log_mount(),
            )
            .build(),
        ];

        match self {
            ContainerConfig::FormatNameNodes { .. } => {
                // As FormatNameNodes only runs on the Namenodes we can safely assume the only pvc is called "data".
                volume_mounts.push(
                    VolumeMountBuilder::new(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
                        .build(),
                );
            }
            ContainerConfig::Hdfs { role, .. } => match role {
                HdfsRole::NameNode | HdfsRole::JournalNode => {
                    volume_mounts.push(
                        VolumeMountBuilder::new(
                            Self::DATA_VOLUME_MOUNT_NAME,
                            STACKABLE_ROOT_DATA_DIR,
                        )
                        .build(),
                    );
                }
                HdfsRole::DataNode => {
                    for pvc in Self::volume_claim_templates(role, merged_config)
                        .iter()
                        .flatten()
                    {
                        let pvc_name = pvc.name_unchecked();
                        volume_mounts.push(VolumeMount {
                            mount_path: format!("{DATANODE_ROOT_DATA_DIR_PREFIX}{pvc_name}"),
                            name: pvc_name,
                            ..VolumeMount::default()
                        });
                    }
                }
            },
            // The other containers don't need any data pvcs to be mounted
            ContainerConfig::Zkfc { .. }
            | ContainerConfig::WaitForNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. } => {}
        }
        volume_mounts
    }

    /// Create a config directory for the respective container.
    fn create_config_directory_cmd(&self) -> String {
        format!(
            "mkdir -p {config_dir_name}",
            config_dir_name = self.volume_mount_dirs().final_config()
        )
    }

    /// Copy the `core-site.xml` and `hdfs-site.xml` to the respective container config dir.
    fn copy_hdfs_and_core_site_xml_cmd(&self) -> String {
        vec![
            format!(
                "cp {config_dir_mount}/{HDFS_SITE_XML} {config_dir_name}/{HDFS_SITE_XML}",
                config_dir_mount = self.volume_mount_dirs().config_mount(),
                config_dir_name = self.volume_mount_dirs().final_config()
            ),
            format!(
                "cp {config_dir_mount}/{CORE_SITE_XML} {config_dir_name}/{CORE_SITE_XML}",
                config_dir_mount = self.volume_mount_dirs().config_mount(),
                config_dir_name = self.volume_mount_dirs().final_config()
            ),
        ]
        .join(" && ")
    }

    /// Copy the `log4j.properties` to the respective container config dir.
    /// This will be copied from:
    /// - Custom: the log dir mount of the custom config map
    /// - Automatic: the container config mount dir
    fn copy_log4j_properties_cmd(
        &self,
        log4j_config_file: &str,
        container_log_config: ContainerLogConfig,
    ) -> String {
        let source_log4j_properties_dir = if let ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Custom(_)),
        } = container_log_config
        {
            self.volume_mount_dirs().log_mount()
        } else {
            self.volume_mount_dirs().config_mount()
        };

        format!(
            "cp {log4j_properties_dir}/{file_name} {config_dir}/{LOG4J_PROPERTIES}",
            log4j_properties_dir = source_log4j_properties_dir,
            file_name = log4j_config_file,
            config_dir = self.volume_mount_dirs().final_config()
        )
    }

    /// Build HADOOP_{*node}_OPTS for each namenode, datanodes and journalnodes.
    fn build_hadoop_opts(&self, resources: &ResourceRequirements) -> Result<String, Error> {
        match self {
            ContainerConfig::Hdfs {
                role, metrics_port, ..
            } => {
                let mut jvm_args = vec![
                    format!(
                        "-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={metrics_port}:/stackable/jmx/{role}.yaml",
                    )];
                if let Some(Some(memory_limit)) =
                    resources.limits.as_ref().map(|limits| limits.get("memory"))
                {
                    let memory_limit =
                        MemoryQuantity::try_from(memory_limit).with_context(|_| {
                            InvalidJavaHeapConfigSnafu {
                                role: role.to_string(),
                            }
                        })?;
                    jvm_args.push(format!(
                        "-Xmx{}",
                        (memory_limit * Self::JVM_HEAP_FACTOR)
                            .scale_to(BinaryMultiple::Kibi)
                            .format_for_java()
                            .with_context(|_| {
                                InvalidJavaHeapConfigSnafu {
                                    role: role.to_string(),
                                }
                            })?
                    ));
                }

                Ok(jvm_args.join(" ").trim().to_string())
            }
            _ => Ok("".to_string()),
        }
    }

    /// Container ports for the main containers namenode, datanode and journalnode.
    fn container_ports(&self) -> Vec<ContainerPort> {
        match self {
            ContainerConfig::Hdfs { role, .. } => role
                .ports()
                .into_iter()
                .map(|(name, value)| ContainerPort {
                    name: Some(name),
                    container_port: i32::from(value),
                    protocol: Some("TCP".to_string()),
                    ..ContainerPort::default()
                })
                .collect(),
            _ => {
                vec![]
            }
        }
    }

    /// Transform the ProductConfig map structure to a Vector of env vars.
    fn transform_env_overrides_to_env_vars(
        env_overrides: Option<&BTreeMap<String, String>>,
    ) -> Vec<EnvVar> {
        env_overrides
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| EnvVar {
                name: k,
                value: Some(v),
                ..EnvVar::default()
            })
            .collect()
    }

    /// Common shared or required container env variables.
    fn shared_env_vars(hadoop_conf_dir: &str, zk_config_map_name: &str) -> Vec<EnvVar> {
        vec![
            EnvVar {
                name: "HADOOP_HOME".to_string(),
                value: Some(String::from(Self::HADOOP_HOME)),
                ..EnvVar::default()
            },
            EnvVar {
                name: "HADOOP_CONF_DIR".to_string(),
                value: Some(String::from(hadoop_conf_dir)),
                ..EnvVar::default()
            },
            EnvVar {
                name: "POD_NAME".to_string(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: String::from("metadata.name"),
                        ..ObjectFieldSelector::default()
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "ZOOKEEPER".to_string(),
                value_from: Some(EnvVarSource {
                    config_map_key_ref: Some(ConfigMapKeySelector {
                        name: Some(String::from(zk_config_map_name)),
                        key: "ZOOKEEPER".to_string(),
                        ..ConfigMapKeySelector::default()
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
        ]
    }

    /// Common container specific log and config volumes
    fn common_container_volumes(
        container_log_config: Option<ContainerLogConfig>,
        object_name: &str,
        config_volume_name: &str,
        log_volume_name: &str,
    ) -> Vec<Volume> {
        let mut volumes = vec![];
        if let Some(container_log_config) = container_log_config {
            volumes.push(
                VolumeBuilder::new(config_volume_name)
                    .config_map(ConfigMapVolumeSource {
                        name: Some(object_name.to_string()),
                        ..ConfigMapVolumeSource::default()
                    })
                    .build(),
            );
            if let ContainerLogConfig {
                choice:
                    Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                        custom: ConfigMapLogConfig { config_map },
                    })),
            } = container_log_config
            {
                volumes.push(
                    VolumeBuilder::new(log_volume_name)
                        .config_map(ConfigMapVolumeSource {
                            name: Some(config_map),
                            ..ConfigMapVolumeSource::default()
                        })
                        .build(),
                );
            } else {
                volumes.push(
                    VolumeBuilder::new(log_volume_name)
                        .config_map(ConfigMapVolumeSource {
                            name: Some(object_name.to_string()),
                            ..ConfigMapVolumeSource::default()
                        })
                        .build(),
                );
            }
        }
        volumes
    }
}

/// Determine if the Hadoop start up scripts should log additional DEBUG information.
fn use_start_up_script_debug(log_config: &ContainerLogConfig) -> bool {
    if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = log_config
    {
        let root_log_level = log_config.root_log_level();
        let console_log_level = log_config
            .console
            .as_ref()
            .and_then(|console| console.level)
            .unwrap_or_default();

        cmp::max(root_log_level, console_log_level) < LogLevel::INFO
    } else {
        false
    }
}

impl From<HdfsRole> for ContainerConfig {
    fn from(role: HdfsRole) -> Self {
        match role {
            HdfsRole::NameNode => Self::Hdfs {
                role: role.clone(),
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                tcp_socket_action_port_name: SERVICE_PORT_NAME_RPC,
                metrics_port: DEFAULT_NAME_NODE_METRICS_PORT,
            },
            HdfsRole::DataNode => Self::Hdfs {
                role: role.clone(),
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                tcp_socket_action_port_name: SERVICE_PORT_NAME_IPC,
                metrics_port: DEFAULT_DATA_NODE_METRICS_PORT,
            },
            HdfsRole::JournalNode => Self::Hdfs {
                role: role.clone(),
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                tcp_socket_action_port_name: SERVICE_PORT_NAME_RPC,
                metrics_port: DEFAULT_JOURNAL_NODE_METRICS_PORT,
            },
        }
    }
}

impl TryFrom<String> for ContainerConfig {
    type Error = Error;

    fn try_from(container_name: String) -> Result<Self, Self::Error> {
        match HdfsRole::from_str(container_name.as_str()) {
            Ok(role) => Ok(ContainerConfig::from(role)),
            // No hadoop main process container
            Err(_) => match container_name {
                // namenode side container
                name if name == NameNodeContainer::Zkfc.to_string() => Ok(Self::Zkfc {
                    volume_mounts: ContainerVolumeDirs::try_from(name.as_str())?,
                    container_name: name,
                }),
                // namenode init containers
                name if name == NameNodeContainer::FormatNameNodes.to_string() => {
                    Ok(Self::FormatNameNodes {
                        volume_mounts: ContainerVolumeDirs::try_from(name.as_str())?,
                        container_name: name,
                    })
                }
                name if name == NameNodeContainer::FormatZooKeeper.to_string() => {
                    Ok(Self::FormatZooKeeper {
                        volume_mounts: ContainerVolumeDirs::try_from(name.as_str())?,
                        container_name: name,
                    })
                }
                // datanode init containers
                name if name == DataNodeContainer::WaitForNameNodes.to_string() => {
                    Ok(Self::WaitForNameNodes {
                        volume_mounts: ContainerVolumeDirs::try_from(name.as_str())?,
                        container_name: name,
                    })
                }
                _ => Err(Error::UnrecognizedContainerName { container_name }),
            },
        }
    }
}

/// Helper struct to collect required config and logging dirs.
pub struct ContainerVolumeDirs {
    /// The final config dir where to store core-site.xml, hdfs-size.xml and logging configs.
    final_config_dir: String,
    /// The mount dir for the config files to be mounted via config map.
    config_mount: String,
    /// The config mount name.
    config_mount_name: String,
    /// The mount dir for the logging config files to be mounted via config map.
    log_mount: String,
    /// The log mount name.
    log_mount_name: String,
}

impl ContainerVolumeDirs {
    const NODE_BASE_CONFIG_DIR: &'static str = "/stackable/config";
    const NODE_BASE_CONFIG_DIR_MOUNT: &'static str = "/stackable/mount/config";
    const NODE_BASE_LOG_DIR_MOUNT: &'static str = "/stackable/mount/log";

    pub fn final_config(&self) -> &str {
        self.final_config_dir.as_str()
    }

    pub fn config_mount(&self) -> &str {
        self.config_mount.as_str()
    }
    pub fn config_mount_name(&self) -> &str {
        self.config_mount_name.as_str()
    }

    pub fn log_mount(&self) -> &str {
        self.log_mount.as_str()
    }
    pub fn log_mount_name(&self) -> &str {
        self.log_mount_name.as_str()
    }
}

impl From<HdfsRole> for ContainerVolumeDirs {
    fn from(role: HdfsRole) -> Self {
        ContainerVolumeDirs {
            final_config_dir: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR),
            config_mount: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR_MOUNT),
            config_mount_name: ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME.to_string(),
            log_mount: format!("{base}/{role}", base = Self::NODE_BASE_LOG_DIR_MOUNT),
            log_mount_name: ContainerConfig::HDFS_LOG_VOLUME_MOUNT_NAME.to_string(),
        }
    }
}

impl TryFrom<&str> for ContainerVolumeDirs {
    type Error = Error;

    fn try_from(container_name: &str) -> Result<Self, Error> {
        if let Ok(role) = HdfsRole::from_str(container_name) {
            return Ok(ContainerVolumeDirs::from(role));
        }

        let (config_mount_name, log_mount_name) = match container_name {
            // namenode side container
            name if name == NameNodeContainer::Zkfc.to_string() => (
                ContainerConfig::ZKFC_CONFIG_VOLUME_MOUNT_NAME.to_string(),
                ContainerConfig::ZKFC_LOG_VOLUME_MOUNT_NAME.to_string(),
            ),
            // namenode init containers
            name if name == NameNodeContainer::FormatNameNodes.to_string() => (
                ContainerConfig::FORMAT_NAMENODES_CONFIG_VOLUME_MOUNT_NAME.to_string(),
                ContainerConfig::FORMAT_NAMENODES_LOG_VOLUME_MOUNT_NAME.to_string(),
            ),
            name if name == NameNodeContainer::FormatZooKeeper.to_string() => (
                ContainerConfig::FORMAT_ZOOKEEPER_CONFIG_VOLUME_MOUNT_NAME.to_string(),
                ContainerConfig::FORMAT_ZOOKEEPER_LOG_VOLUME_MOUNT_NAME.to_string(),
            ),
            // datanode init containers
            name if name == DataNodeContainer::WaitForNameNodes.to_string() => (
                ContainerConfig::WAIT_FOR_NAMENODES_CONFIG_VOLUME_MOUNT_NAME.to_string(),
                ContainerConfig::WAIT_FOR_NAMENODES_LOG_VOLUME_MOUNT_NAME.to_string(),
            ),
            _ => {
                return Err(Error::UnrecognizedContainerName {
                    container_name: container_name.to_string(),
                })
            }
        };

        let final_config_dir =
            format!("{base}/{container_name}", base = Self::NODE_BASE_CONFIG_DIR);
        let config_mount = format!(
            "{base}/{container_name}",
            base = Self::NODE_BASE_CONFIG_DIR_MOUNT
        );
        let log_mount = format!(
            "{base}/{container_name}",
            base = Self::NODE_BASE_LOG_DIR_MOUNT
        );

        Ok(ContainerVolumeDirs {
            final_config_dir,
            config_mount,
            config_mount_name,
            log_mount,
            log_mount_name,
        })
    }
}
