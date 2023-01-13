use crate::{
    config::{HdfsNodeDataDirectory, STACKABLE_ROOT_DATA_DIR},
    product_logging::{HDFS_LOG4J_CONFIG_FILE, STACKABLE_LOG_DIR, ZKFC_LOG4J_CONFIG_FILE},
};

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{
        CORE_SITE_XML, DEFAULT_DATA_NODE_METRICS_PORT, DEFAULT_JOURNAL_NODE_METRICS_PORT,
        DEFAULT_NAME_NODE_METRICS_PORT, HDFS_SITE_XML, LOG4J_PROPERTIES, SERVICE_PORT_NAME_IPC,
        SERVICE_PORT_NAME_RPC,
    },
    HdfsPodRef, HdfsRole, MergedConfig,
};
use stackable_operator::{
    builder::{ContainerBuilder, VolumeMountBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapKeySelector, Container, ContainerPort, EnvVar, EnvVarSource,
            ObjectFieldSelector, Probe, ResourceRequirements, TCPSocketAction, VolumeMount,
        },
        apimachinery::pkg::util::intstr::IntOrString,
    },
    memory::to_java_heap,
    product_logging,
    product_logging::spec::{ContainerLogConfig, ContainerLogConfigChoice},
};
use std::{collections::BTreeMap, str::FromStr};
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
    // extra side containers
    pub const ZKFC_CONTAINER_NAME: &'static str = "zkfc";
    // extra init containers
    pub const FORMAT_NAMENODES_CONTAINER_NAME: &'static str = "format-namenodes";
    pub const FORMAT_ZOOKEEPER_CONTAINER_NAME: &'static str = "format-zookeeper";
    pub const WAIT_FOR_NAMENODES_CONTAINER_NAME: &'static str = "wait-for-namenodes";
    // volumes
    pub const STACKABLE_LOG_VOLUME_MOUNT_NAME: &'static str = "log";
    pub const DATA_VOLUME_MOUNT_NAME: &'static str = "data";
    pub const HDFS_CONFIG_VOLUME_MOUNT_NAME: &'static str = "hdfs-config";
    pub const HDFS_LOG_VOLUME_MOUNT_NAME: &'static str = "hdfs-log-config";
    pub const ZKFC_CONFIG_VOLUME_MOUNT_NAME: &'static str = "zkfc-config";
    pub const ZKFC_LOG_VOLUME_MOUNT_NAME: &'static str = "zkfc-log-config";

    const JVM_HEAP_FACTOR: f32 = 0.8;
    const HADOOP_HOME: &'static str = "/stackable/hadoop";

    /// Creates the main process containers for:
    /// - Namenode main process
    /// - Namenode ZooKeeper fail over controller (ZKFC)
    /// - Datanode main process
    /// - Journalnode main process
    pub fn main_container(
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
            .add_volume_mounts(self.volume_mounts())
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
    /// - Namenode (format-namenode, format-zookeeper)
    /// - Datanode (wait-for-namenodes)
    pub fn init_container(
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
            .add_volume_mounts(self.volume_mounts())
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
        let mut args = vec![];

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                args.push(self.create_config_directory_cmd());
                args.push(self.copy_hdfs_and_core_site_xml_cmd());
                args.push(self.copy_log4j_properties_cmd(
                    HDFS_LOG4J_CONFIG_FILE,
                    merged_config.hdfs_logging(),
                ));

                args.push(format!(
                    "{hadoop_home}/bin/hdfs --debug {role}",
                    hadoop_home = Self::HADOOP_HOME,
                ));
            }
            ContainerConfig::Zkfc { .. } => {
                args.push(self.create_config_directory_cmd());
                args.push(self.copy_hdfs_and_core_site_xml_cmd());
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
                if let Some(logging_args) = Self::init_container_logging_args(
                    merged_config.format_namenodes_logging(),
                    self.name(),
                ) {
                    args.push(logging_args);
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
                    if [ ! -f "{namenode_dir}/current/VERSION" ]
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
                    namenode_dir = HdfsNodeDataDirectory::default().namenode,
                ));
            }
            ContainerConfig::FormatZooKeeper { .. } => {
                if let Some(logging_args) = Self::init_container_logging_args(
                    merged_config.format_zookeeper_logging(),
                    self.name(),
                ) {
                    args.push(logging_args);
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
                if let Some(logging_args) = Self::init_container_logging_args(
                    merged_config.wait_for_namenodes(),
                    self.name(),
                ) {
                    args.push(logging_args);
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

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                env.extend(Self::shared_env_vars(
                    self.volume_mount_dirs().final_config(),
                    zookeeper_config_map_name,
                ));
                if let Some(resources) = resources {
                    env.push(EnvVar {
                        name: role.hadoop_opts().to_string(),
                        value: self.build_hadoop_opts(resources).ok(),
                        ..EnvVar::default()
                    });
                }
            }
            ContainerConfig::Zkfc { .. } => {
                env.extend(Self::shared_env_vars(
                    self.volume_mount_dirs().final_config(),
                    zookeeper_config_map_name,
                ));
            }
            ContainerConfig::FormatNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. }
            | ContainerConfig::WaitForNameNodes { .. } => {
                env.extend(Self::shared_env_vars(
                    // We use the config mount dir directly here to not have to copy
                    self.volume_mount_dirs().config_mount(),
                    zookeeper_config_map_name,
                ));
            }
        }

        env
    }

    /// Returns the container env variables.
    fn resources(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Option<ResourceRequirements> {
        // Only the Hadoop main containers will get resources
        match self {
            ContainerConfig::Hdfs { .. } => Some(merged_config.resources().into()),
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

    /// Returns the main container volume mounts.
    fn volume_mounts(&self) -> Vec<VolumeMount> {
        let mut volume_mounts =
            vec![
                VolumeMountBuilder::new(Self::STACKABLE_LOG_VOLUME_MOUNT_NAME, STACKABLE_LOG_DIR)
                    .build(),
            ];

        match self {
            ContainerConfig::Hdfs { .. } => {
                volume_mounts.extend(vec![
                    VolumeMountBuilder::new(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
                        .build(),
                    VolumeMountBuilder::new(
                        Self::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                        self.volume_mount_dirs().config_mount(),
                    )
                    .build(),
                    VolumeMountBuilder::new(
                        Self::HDFS_LOG_VOLUME_MOUNT_NAME,
                        self.volume_mount_dirs().log_mount(),
                    )
                    .build(),
                ]);
            }
            ContainerConfig::Zkfc { .. } => {
                volume_mounts.extend(vec![
                    VolumeMountBuilder::new(
                        Self::ZKFC_CONFIG_VOLUME_MOUNT_NAME,
                        self.volume_mount_dirs().config_mount(),
                    )
                    .build(),
                    VolumeMountBuilder::new(
                        Self::ZKFC_LOG_VOLUME_MOUNT_NAME,
                        self.volume_mount_dirs().log_mount(),
                    )
                    .build(),
                ]);
            }
            ContainerConfig::FormatNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. }
            | ContainerConfig::WaitForNameNodes { .. } => {
                volume_mounts.extend(vec![
                    VolumeMountBuilder::new(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
                        .build(),
                    VolumeMountBuilder::new(
                        Self::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                        self.volume_mount_dirs().config_mount(),
                    )
                    .build(),
                ]);
            }
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
            ContainerConfig::Hdfs { role, metrics_port, .. } => {
                Ok(vec![
                    format!(
                        "-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={metrics_port}:/stackable/jmx/{role}.yaml",
                    ),
                    resources
                        .limits
                        .as_ref()
                        .and_then(|l| l.get("memory"))
                        .map(|m| to_java_heap(m, Self::JVM_HEAP_FACTOR))
                        .unwrap_or_else(|| Ok("".to_string()))
                        .with_context(|_| InvalidJavaHeapConfigSnafu {
                            role: role.to_string(),
                        })?,
                ]
                .into_iter()
                .collect::<Vec<String>>()
                .join(" ")
                .trim()
                .to_string())
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

    /// Get the logging command for init containers
    fn init_container_logging_args(
        container_log_config: Option<ContainerLogConfig>,
        container_name: &str,
    ) -> Option<String> {
        if let Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) = container_log_config
        {
            Some(product_logging::framework::capture_shell_output(
                STACKABLE_LOG_DIR,
                container_name,
                &log_config,
            ))
        } else {
            None
        }
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

impl TryFrom<&str> for ContainerConfig {
    type Error = Error;

    fn try_from(container_name: &str) -> Result<Self, Self::Error> {
        match HdfsRole::from_str(container_name) {
            Ok(role) => Ok(ContainerConfig::from(role)),
            // No hadoop main process container
            Err(_) => match container_name {
                // side container
                Self::ZKFC_CONTAINER_NAME => Ok(Self::Zkfc {
                    container_name: container_name.to_string(),
                    volume_mounts: ContainerVolumeDirs::from(container_name),
                }),
                // init containers
                Self::FORMAT_NAMENODES_CONTAINER_NAME => Ok(Self::FormatNameNodes {
                    container_name: container_name.to_string(),
                    volume_mounts: ContainerVolumeDirs::from(container_name),
                }),
                Self::FORMAT_ZOOKEEPER_CONTAINER_NAME => Ok(Self::FormatZooKeeper {
                    container_name: container_name.to_string(),
                    volume_mounts: ContainerVolumeDirs::from(container_name),
                }),
                Self::WAIT_FOR_NAMENODES_CONTAINER_NAME => Ok(Self::WaitForNameNodes {
                    container_name: container_name.to_string(),
                    volume_mounts: ContainerVolumeDirs::from(container_name),
                }),
                _ => Err(Error::UnrecognizedContainerName {
                    container_name: container_name.to_string(),
                }),
            },
        }
    }
}

/// Helper struct to collect required config and logging dirs.
pub struct ContainerVolumeDirs {
    /// The final config dir where to store core-site.xml, hdfs-size.xml and logging configs.
    final_config: String,
    /// The mount dir for the config files to be mounted via config map.
    config_mount: String,
    /// The mount dir for the logging config files to be mounted via config map.
    log_mount: String,
}

impl ContainerVolumeDirs {
    const NODE_BASE_CONFIG_DIR: &'static str = "/stackable/config";
    const NODE_BASE_CONFIG_DIR_MOUNT: &'static str = "/stackable/mount/config";
    const NODE_BASE_LOG_DIR_MOUNT: &'static str = "/stackable/mount/log";

    pub fn final_config(&self) -> &str {
        self.final_config.as_str()
    }

    pub fn config_mount(&self) -> &str {
        self.config_mount.as_str()
    }

    pub fn log_mount(&self) -> &str {
        self.log_mount.as_str()
    }
}

impl From<HdfsRole> for ContainerVolumeDirs {
    fn from(role: HdfsRole) -> Self {
        ContainerVolumeDirs {
            final_config: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR),
            config_mount: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR_MOUNT),
            log_mount: format!("{base}/{role}", base = Self::NODE_BASE_LOG_DIR_MOUNT),
        }
    }
}

impl From<&str> for ContainerVolumeDirs {
    fn from(container_name: &str) -> Self {
        ContainerVolumeDirs {
            final_config: format!("{base}/{container_name}", base = Self::NODE_BASE_CONFIG_DIR),
            config_mount: format!(
                "{base}/{container_name}",
                base = Self::NODE_BASE_CONFIG_DIR_MOUNT
            ),
            log_mount: format!(
                "{base}/{container_name}",
                base = Self::NODE_BASE_LOG_DIR_MOUNT
            ),
        }
    }
}
