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
use std::{collections::BTreeMap, str::FromStr};

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{
                ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
                ListenerReference, SecretFormat, SecretOperatorVolumeSourceBuilder,
                SecretOperatorVolumeSourceBuilderError, VolumeBuilder, VolumeMountBuilder,
            },
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapKeySelector, ConfigMapVolumeSource, Container, ContainerPort,
            EmptyDirVolumeSource, EnvVar, EnvVarSource, ObjectFieldSelector, PersistentVolumeClaim,
            Probe, ResourceRequirements, TCPSocketAction, Volume, VolumeMount,
        },
        apimachinery::pkg::util::intstr::IntOrString,
    },
    kube::{ResourceExt, core::ObjectMeta},
    kvp::Labels,
    product_logging::{
        self,
        framework::{
            LoggingError, create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
        },
        spec::{
            AutomaticContainerLogConfig, ConfigMapLogConfig, ContainerLogConfig,
            ContainerLogConfigChoice, CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    utils::{COMMON_BASH_TRAP_FUNCTIONS, cluster_info::KubernetesClusterInfo},
};
use strum::{Display, EnumDiscriminants, IntoStaticStr};

use crate::{
    config::{
        self,
        jvm::{construct_global_jvm_args, construct_role_specific_jvm_args},
    },
    crd::{
        AnyNodeConfig, DataNodeContainer, HdfsNodeRole, HdfsPodRef, NameNodeContainer,
        UpgradeState,
        constants::{
            DATANODE_ROOT_DATA_DIR_PREFIX, DEFAULT_DATA_NODE_METRICS_PORT,
            DEFAULT_JOURNAL_NODE_METRICS_PORT, DEFAULT_NAME_NODE_METRICS_PORT, LISTENER_VOLUME_DIR,
            LISTENER_VOLUME_NAME, LIVENESS_PROBE_FAILURE_THRESHOLD,
            LIVENESS_PROBE_INITIAL_DELAY_SECONDS, LIVENESS_PROBE_PERIOD_SECONDS, LOG4J_PROPERTIES,
            NAMENODE_ROOT_DATA_DIR, READINESS_PROBE_FAILURE_THRESHOLD,
            READINESS_PROBE_INITIAL_DELAY_SECONDS, READINESS_PROBE_PERIOD_SECONDS,
            SERVICE_PORT_NAME_HTTP, SERVICE_PORT_NAME_HTTPS, SERVICE_PORT_NAME_IPC,
            SERVICE_PORT_NAME_RPC, STACKABLE_ROOT_DATA_DIR,
        },
        storage::DataNodeStorageConfig,
        v1alpha1,
    },
    product_logging::{
        FORMAT_NAMENODES_LOG4J_CONFIG_FILE, FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
        HDFS_LOG4J_CONFIG_FILE, MAX_FORMAT_NAMENODE_LOG_FILE_SIZE,
        MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE, MAX_HDFS_LOG_FILE_SIZE,
        MAX_WAIT_NAMENODES_LOG_FILE_SIZE, MAX_ZKFC_LOG_FILE_SIZE, STACKABLE_LOG_DIR,
        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE, ZKFC_LOG4J_CONFIG_FILE,
    },
    security::kerberos::KERBEROS_CONTAINER_PATH,
};

pub(crate) const TLS_STORE_DIR: &str = "/stackable/tls";
pub(crate) const TLS_STORE_VOLUME_NAME: &str = "tls";
pub(crate) const TLS_STORE_PASSWORD: &str = "changeit";
pub(crate) const KERBEROS_VOLUME_NAME: &str = "kerberos";

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to construct JVM arguments fro role {role:?}"))]
    ConstructJvmArguments {
        source: config::jvm::Error,
        role: String,
    },

    #[snafu(display(
        "could not determine any ContainerConfig actions for {container_name:?}. Container not recognized."
    ))]
    UnrecognizedContainerName { container_name: String },

    #[snafu(display("invalid container name {name:?}"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
        name: String,
    },

    #[snafu(display("failed to build secret volume for {volume_name:?}"))]
    BuildSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
        volume_name: String,
    },

    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("missing or invalid labels for the listener volume"))]
    ListenerVolumeLabels {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,
}

/// ContainerConfig contains information to create all main, side and init containers for
/// the HDFS cluster.
#[derive(Display)]
pub enum ContainerConfig {
    Hdfs {
        /// HDFS role (name-, data-, journal-node) which will be the container_name.
        role: HdfsNodeRole,
        /// The container name derived from the provided role.
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
        /// Port name of the IPC/RPC port, used for the readiness probe.
        ipc_port_name: &'static str,
        /// Port name of the web UI HTTP port, used for the liveness probe.
        web_ui_http_port_name: &'static str,
        /// Port name of the web UI HTTPS port, used for the liveness probe.
        web_ui_https_port_name: &'static str,
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
    pub const DATA_VOLUME_MOUNT_NAME: &'static str = "data";
    const FORMAT_NAMENODES_CONFIG_VOLUME_MOUNT_NAME: &'static str = "format-namenodes-config";
    const FORMAT_NAMENODES_LOG_VOLUME_MOUNT_NAME: &'static str = "format-namenodes-log-config";
    const FORMAT_ZOOKEEPER_CONFIG_VOLUME_MOUNT_NAME: &'static str = "format-zookeeper-config";
    const FORMAT_ZOOKEEPER_LOG_VOLUME_MOUNT_NAME: &'static str = "format-zookeeper-log-config";
    const HADOOP_HOME: &'static str = "/stackable/hadoop";
    pub const HDFS_CONFIG_VOLUME_MOUNT_NAME: &'static str = "hdfs-config";
    const HDFS_LOG_VOLUME_MOUNT_NAME: &'static str = "hdfs-log-config";
    // volumes
    pub const STACKABLE_LOG_VOLUME_MOUNT_NAME: &'static str = "log";
    const WAIT_FOR_NAMENODES_CONFIG_VOLUME_MOUNT_NAME: &'static str = "wait-for-namenodes-config";
    const WAIT_FOR_NAMENODES_LOG_VOLUME_MOUNT_NAME: &'static str = "wait-for-namenodes-log-config";
    const ZKFC_CONFIG_VOLUME_MOUNT_NAME: &'static str = "zkfc-config";
    const ZKFC_LOG_VOLUME_MOUNT_NAME: &'static str = "zkfc-log-config";

    /// Add all main, side and init containers as well as required volumes to the pod builder.
    #[allow(clippy::too_many_arguments)]
    pub fn add_containers_and_volumes(
        pb: &mut PodBuilder,
        hdfs: &v1alpha1::HdfsCluster,
        cluster_info: &KubernetesClusterInfo,
        role: &HdfsNodeRole,
        rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
        resolved_product_image: &ResolvedProductImage,
        merged_config: &AnyNodeConfig,
        env_overrides: Option<&BTreeMap<String, String>>,
        zk_config_map_name: &str,
        namenode_podrefs: &[HdfsPodRef],
        labels: &Labels,
    ) -> Result<(), Error> {
        // HDFS main container
        let main_container_config = Self::from(*role);
        let object_name = rolegroup_ref.object_name();

        pb.add_volumes(main_container_config.volumes(merged_config, &object_name, labels)?)
            .context(AddVolumeSnafu)?;
        pb.add_container(main_container_config.main_container(
            hdfs,
            cluster_info,
            role,
            rolegroup_ref,
            resolved_product_image,
            zk_config_map_name,
            env_overrides,
            merged_config,
            labels,
        )?);

        // Vector side container
        if merged_config.vector_logging_enabled() {
            match &hdfs.spec.cluster_config.vector_aggregator_config_map_name {
                Some(vector_aggregator_config_map_name) => {
                    pb.add_container(
                        product_logging::framework::vector_container(
                            resolved_product_image,
                            ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                            ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME,
                            Some(&merged_config.vector_logging()),
                            ResourceRequirementsBuilder::new()
                                .with_cpu_request("250m")
                                .with_cpu_limit("500m")
                                .with_memory_request("128Mi")
                                .with_memory_limit("128Mi")
                                .build(),
                            vector_aggregator_config_map_name,
                        )
                        .context(ConfigureLoggingSnafu)?,
                    );
                }
                None => {
                    VectorAggregatorConfigMapMissingSnafu.fail()?;
                }
            }
        }

        if let Some(authentication_config) = hdfs.authentication_config() {
            pb.add_volume(
                VolumeBuilder::new(TLS_STORE_VOLUME_NAME)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            &authentication_config.tls_secret_class,
                        )
                        .with_pod_scope()
                        .with_node_scope()
                        // To scrape metrics behind TLS endpoint (without FQDN)
                        .with_service_scope(rolegroup_ref.rolegroup_metrics_service_name())
                        .with_format(SecretFormat::TlsPkcs12)
                        .with_tls_pkcs12_password(TLS_STORE_PASSWORD)
                        .with_auto_tls_cert_lifetime(
                            merged_config
                                .requested_secret_lifetime()
                                .context(MissingSecretLifetimeSnafu)?,
                        )
                        .build()
                        .context(BuildSecretVolumeSnafu {
                            volume_name: TLS_STORE_VOLUME_NAME,
                        })?,
                    )
                    .build(),
            )
            .context(AddVolumeSnafu)?;

            pb.add_volume(
                VolumeBuilder::new(KERBEROS_VOLUME_NAME)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            &authentication_config.kerberos.secret_class,
                        )
                        .with_service_scope(hdfs.name_any())
                        .with_kerberos_service_name(role.kerberos_service_name())
                        .with_kerberos_service_name("HTTP")
                        .build()
                        .context(BuildSecretVolumeSnafu {
                            volume_name: KERBEROS_VOLUME_NAME,
                        })?,
                    )
                    .build(),
            )
            .context(AddVolumeSnafu)?;
        }

        // role specific pod settings configured here
        match role {
            HdfsNodeRole::Name => {
                // Zookeeper fail over container
                let zkfc_container_config = Self::try_from(NameNodeContainer::Zkfc.to_string())?;
                pb.add_volumes(zkfc_container_config.volumes(
                    merged_config,
                    &object_name,
                    labels,
                )?)
                .context(AddVolumeSnafu)?;
                pb.add_container(zkfc_container_config.main_container(
                    hdfs,
                    cluster_info,
                    role,
                    rolegroup_ref,
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    merged_config,
                    labels,
                )?);

                // Format namenode init container
                let format_namenodes_container_config =
                    Self::try_from(NameNodeContainer::FormatNameNodes.to_string())?;
                pb.add_volumes(format_namenodes_container_config.volumes(
                    merged_config,
                    &object_name,
                    labels,
                )?)
                .context(AddVolumeSnafu)?;
                pb.add_init_container(format_namenodes_container_config.init_container(
                    hdfs,
                    cluster_info,
                    role,
                    &rolegroup_ref.role_group,
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                    labels,
                )?);

                // Format ZooKeeper init container
                let format_zookeeper_container_config =
                    Self::try_from(NameNodeContainer::FormatZooKeeper.to_string())?;
                pb.add_volumes(format_zookeeper_container_config.volumes(
                    merged_config,
                    &object_name,
                    labels,
                )?)
                .context(AddVolumeSnafu)?;
                pb.add_init_container(format_zookeeper_container_config.init_container(
                    hdfs,
                    cluster_info,
                    role,
                    &rolegroup_ref.role_group,
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                    labels,
                )?);
            }
            HdfsNodeRole::Data => {
                // Wait for namenode init container
                let wait_for_namenodes_container_config =
                    Self::try_from(DataNodeContainer::WaitForNameNodes.to_string())?;
                pb.add_volumes(wait_for_namenodes_container_config.volumes(
                    merged_config,
                    &object_name,
                    labels,
                )?)
                .context(AddVolumeSnafu)?;
                pb.add_init_container(wait_for_namenodes_container_config.init_container(
                    hdfs,
                    cluster_info,
                    role,
                    &rolegroup_ref.role_group,
                    resolved_product_image,
                    zk_config_map_name,
                    env_overrides,
                    namenode_podrefs,
                    merged_config,
                    labels,
                )?);
            }
            HdfsNodeRole::Journal => {}
        }

        Ok(())
    }

    pub fn volume_claim_templates(
        merged_config: &AnyNodeConfig,
        labels: &Labels,
    ) -> Result<Vec<PersistentVolumeClaim>> {
        match merged_config {
            AnyNodeConfig::Name(node) => {
                let listener = ListenerOperatorVolumeSourceBuilder::new(
                    &ListenerReference::ListenerClass(node.listener_class.to_string()),
                    labels,
                )
                .build_ephemeral()
                .context(BuildListenerVolumeSnafu)?
                .volume_claim_template
                .unwrap();

                let pvcs = vec![
                    node.resources.storage.data.build_pvc(
                        ContainerConfig::DATA_VOLUME_MOUNT_NAME,
                        Some(vec!["ReadWriteOnce"]),
                    ),
                    PersistentVolumeClaim {
                        metadata: ObjectMeta {
                            name: Some(LISTENER_VOLUME_NAME.to_string()),
                            ..listener.metadata.unwrap()
                        },
                        spec: Some(listener.spec),
                        ..Default::default()
                    },
                ];

                Ok(pvcs)
            }
            AnyNodeConfig::Journal(node) => Ok(vec![node.resources.storage.data.build_pvc(
                ContainerConfig::DATA_VOLUME_MOUNT_NAME,
                Some(vec!["ReadWriteOnce"]),
            )]),
            AnyNodeConfig::Data(node) => Ok(DataNodeStorageConfig {
                pvcs: node.resources.storage.clone(),
            }
            .build_pvcs()),
        }
    }

    /// Creates the main/side containers for:
    /// - Namenode main process
    /// - Namenode ZooKeeper fail over controller (ZKFC)
    /// - Datanode main process
    /// - Journalnode main process
    #[allow(clippy::too_many_arguments)]
    fn main_container(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        cluster_info: &KubernetesClusterInfo,
        role: &HdfsNodeRole,
        rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        merged_config: &AnyNodeConfig,
        labels: &Labels,
    ) -> Result<Container, Error> {
        let mut cb =
            ContainerBuilder::new(self.name()).with_context(|_| InvalidContainerNameSnafu {
                name: self.name().to_string(),
            })?;

        let resources = self.resources(merged_config);

        cb.image_from_product_image(resolved_product_image)
            .command(Self::command())
            .args(self.args(hdfs, cluster_info, role, merged_config, &[])?)
            .add_env_vars(self.env(
                hdfs,
                &rolegroup_ref.role_group,
                zookeeper_config_map_name,
                env_overrides,
                resources.as_ref(),
            )?)
            .add_volume_mounts(self.volume_mounts(hdfs, merged_config, labels)?)
            .context(AddVolumeMountSnafu)?
            .add_container_ports(self.container_ports(hdfs));

        if let Some(resources) = resources {
            cb.resources(resources);
        }

        if let Some(probe) = self.web_ui_port_probe(
            hdfs,
            LIVENESS_PROBE_PERIOD_SECONDS,
            LIVENESS_PROBE_INITIAL_DELAY_SECONDS,
            LIVENESS_PROBE_FAILURE_THRESHOLD,
        ) {
            cb.liveness_probe(probe);
        }

        if let Some(probe) = self.ipc_port_probe(
            READINESS_PROBE_PERIOD_SECONDS,
            READINESS_PROBE_INITIAL_DELAY_SECONDS,
            READINESS_PROBE_FAILURE_THRESHOLD,
        ) {
            cb.readiness_probe(probe.clone());
        }

        Ok(cb.build())
    }

    /// Creates respective init containers for:
    /// - Namenode (format-namenodes, format-zookeeper)
    /// - Datanode (wait-for-namenodes)
    #[allow(clippy::too_many_arguments)]
    fn init_container(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        cluster_info: &KubernetesClusterInfo,
        role: &HdfsNodeRole,
        role_group: &str,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
        merged_config: &AnyNodeConfig,
        labels: &Labels,
    ) -> Result<Container, Error> {
        let mut cb = ContainerBuilder::new(self.name())
            .with_context(|_| InvalidContainerNameSnafu { name: self.name() })?;

        cb.image_from_product_image(resolved_product_image)
            .command(Self::command())
            .args(self.args(hdfs, cluster_info, role, merged_config, namenode_podrefs)?)
            .add_env_vars(self.env(
                hdfs,
                role_group,
                zookeeper_config_map_name,
                env_overrides,
                None,
            )?)
            .add_volume_mounts(self.volume_mounts(hdfs, merged_config, labels)?)
            .context(AddVolumeMountSnafu)?;

        // We use the main app container resources here in contrast to several operators (which use
        // hardcoded resources) due to the different code structure.
        // Going forward this should be replaced by calculating init container resources in the pod builder.
        if let Some(resources) = self.resources(merged_config) {
            cb.resources(resources);
        }

        Ok(cb.build())
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
    fn command() -> Vec<String> {
        vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ]
    }

    /// Returns the container command arguments.
    fn args(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        cluster_info: &KubernetesClusterInfo,
        role: &HdfsNodeRole,
        merged_config: &AnyNodeConfig,
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<Vec<String>, Error> {
        let mut args = String::new();
        args.push_str(&self.create_config_directory_cmd());
        args.push_str(&self.copy_config_xml_cmd());

        // This env var is required for reading the core-site.xml
        if hdfs.has_kerberos_enabled() {
            args.push_str(&Self::export_kerberos_real_env_var_command());
        }

        let upgrade_args = if hdfs.upgrade_state().ok() == Some(Some(UpgradeState::Upgrading))
            && *role == HdfsNodeRole::Name
        {
            "-rollingUpgrade started"
        } else {
            ""
        };

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                args.push_str(&self.copy_log4j_properties_cmd(
                    HDFS_LOG4J_CONFIG_FILE,
                    &merged_config.hdfs_logging(),
                ));

                args.push_str(&formatdoc!(
                    r#"\
                {COMMON_BASH_TRAP_FUNCTIONS}
                {remove_vector_shutdown_file_command}
                prepare_signal_handlers
                containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
                if [[ -d {LISTENER_VOLUME_DIR} ]]; then
                    export POD_ADDRESS=$(cat {LISTENER_VOLUME_DIR}/default-address/address)
                    for i in {LISTENER_VOLUME_DIR}/default-address/ports/*; do
                        export $(basename $i | tr a-z- A-Z_)_PORT="$(cat $i)"
                    done
                fi
                {hadoop_home}/bin/hdfs {role} {upgrade_args} &
                wait_for_termination $!
                {create_vector_shutdown_file_command}
                "#,
                    hadoop_home = Self::HADOOP_HOME,
                    remove_vector_shutdown_file_command =
                        remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
                    create_vector_shutdown_file_command =
                        create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
                ));
            }
            ContainerConfig::Zkfc { .. } => {
                if let Some(container_config) = merged_config
                    .as_namenode()
                    .map(|node| node.logging.for_container(&NameNodeContainer::Zkfc))
                {
                    args.push_str(
                        &self.copy_log4j_properties_cmd(ZKFC_LOG4J_CONFIG_FILE, &container_config),
                    );
                }
                args.push_str(&format!(
                    "{hadoop_home}/bin/hdfs zkfc\n",
                    hadoop_home = Self::HADOOP_HOME
                ));
            }
            ContainerConfig::FormatNameNodes { container_name, .. } => {
                args.push_str(&bash_capture_shell_helper(container_name));
                args.push_str("start_capture\n");

                if let Some(container_config) = merged_config.as_namenode().map(|node| {
                    node.logging
                        .for_container(&NameNodeContainer::FormatNameNodes)
                }) {
                    args.push_str(&self.copy_log4j_properties_cmd(
                        FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
                        &container_config,
                    ));
                }
                // First step we check for active namenodes. This step should return an active namenode
                // for e.g. scaling. It may fail if the active namenode is restarted and the standby
                // namenode takes over.
                // This is why in the second part we check if the node is formatted already via
                // $NAMENODE_DIR/current/VERSION. Then we don't do anything.
                // If there is no active namenode, the current pod is not formatted we format as
                // active namenode. Otherwise as standby node.
                if hdfs.has_kerberos_enabled() {
                    args.push_str(&Self::get_kerberos_ticket(hdfs, role, cluster_info)?);
                }
                args.push_str(&formatdoc!(
                    r###"
                    echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
                    for namenode_id in {pod_names}
                    do
                      echo -n "Checking pod $namenode_id... "
                      # We only redirect 2 (stderr) to 4 (console). 
                      # We leave 1 (stdout) alone so the $(...) can catch it.
                      SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState "$namenode_id" 2>&4 | tail -n1 || true)
                      
                      if [ "$SERVICE_STATE" == "active" ]
                      then
                        ACTIVE_NAMENODE="$namenode_id"
                        echo "active"
                        break
                      else
                        echo "unknown"  
                      fi
                    done

                    if [ ! -f "{NAMENODE_ROOT_DATA_DIR}/current/VERSION" ]
                    then
                      if [ -z ${{ACTIVE_NAMENODE+x}} ]
                      then
                        echo "Create pod $POD_NAME as active namenode."
                        exclude_from_capture {hadoop_home}/bin/hdfs namenode -format -noninteractive
                      else
                        echo "Create pod $POD_NAME as standby namenode."
                        exclude_from_capture {hadoop_home}/bin/hdfs namenode -bootstrapStandby -nonInteractive
                      fi
                    else
                      cat "{NAMENODE_ROOT_DATA_DIR}/current/VERSION"
                      echo "Pod $POD_NAME already formatted. Skipping..."
                    fi
                    "###,
                    hadoop_home = Self::HADOOP_HOME,
                    pod_names = namenode_podrefs
                        .iter()
                        .map(|pod_ref| pod_ref.pod_name.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" "),
                ));
            }
            ContainerConfig::FormatZooKeeper { container_name, .. } => {
                args.push_str(&bash_capture_shell_helper(container_name));
                args.push_str("start_capture\n");
                if let Some(container_config) = merged_config.as_namenode().map(|node| {
                    node.logging
                        .for_container(&NameNodeContainer::FormatZooKeeper)
                }) {
                    args.push_str(&self.copy_log4j_properties_cmd(
                        FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
                        &container_config,
                    ));
                }
                args.push_str(&formatdoc!(
                    r###"
                    echo "Attempt to format ZooKeeper..."
                    if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
                      set +e
                      # Restore original stdout/stderr from FD 3 and 4
                      exec 1>&3 2>&4
                      # Clean up (close FD 3 and 4)
                      exec 3>&- 4>&-
                      {hadoop_home}/bin/hdfs zkfc -formatZK -nonInteractive
                      EXITCODE=$?
                      set -e
                      if [[ $EXITCODE -eq 0 ]]; then
                        echo "Successfully formatted"
                      elif [[ $EXITCODE -eq 2 ]]; then
                        echo "ZNode already existed, did nothing"
                      else
                        echo "Zookeeper format failed with exit code $EXITCODE"
                        exit $EXITCODE
                      fi

                    else
                      echo "ZooKeeper already formatted!"
                    fi
                    "###,
                    hadoop_home = Self::HADOOP_HOME,
                ));
            }
            ContainerConfig::WaitForNameNodes { .. } => {
                if let Some(container_config) = merged_config.as_datanode().map(|node| {
                    node.logging
                        .for_container(&DataNodeContainer::WaitForNameNodes)
                }) {
                    args.push_str(&self.copy_log4j_properties_cmd(
                        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
                        &container_config,
                    ));
                }
                if hdfs.has_kerberos_enabled() {
                    args.push_str(&Self::get_kerberos_ticket(hdfs, role, cluster_info)?);
                }
                args.push_str(&formatdoc!(
                    r###"
                    echo "Waiting for namenodes to get ready:"
                    n=0
                    while [ ${{n}} -lt 12 ];
                    do
                      ALL_NODES_READY=true
                      for namenode_id in {pod_names}
                      do
                        echo -n "Checking pod $namenode_id... "
                        {get_service_state_command}
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
                    done
                    "###,
                    get_service_state_command = Self::get_namenode_service_state_command(),
                    pod_names = namenode_podrefs
                        .iter()
                        .map(|pod_ref| pod_ref.pod_name.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" ")
                ));
            }
        }
        Ok(vec![args])
    }

    // Command to export `KERBEROS_REALM` env var to default real from krb5.conf, e.g. `CLUSTER.LOCAL`
    fn export_kerberos_real_env_var_command() -> String {
        format!(
            "export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {KERBEROS_CONTAINER_PATH}/krb5.conf)\n"
        )
    }

    /// Command to `kinit` a ticket using the principal created for the specified hdfs role
    /// Needs the KERBEROS_REALM env var, which will be written with `export_kerberos_real_env_var_command`
    /// Needs the POD_NAME env var to be present, which will be provided by the PodSpec
    fn get_kerberos_ticket(
        hdfs: &v1alpha1::HdfsCluster,
        role: &HdfsNodeRole,
        cluster_info: &KubernetesClusterInfo,
    ) -> Result<String, Error> {
        let principal = format!(
            "{service_name}/{hdfs_name}.{namespace}.svc.{cluster_domain}@${{KERBEROS_REALM}}",
            service_name = role.kerberos_service_name(),
            hdfs_name = hdfs.name_any(),
            namespace = hdfs.namespace().context(ObjectHasNoNamespaceSnafu)?,
            cluster_domain = cluster_info.cluster_domain,
        );
        Ok(formatdoc!(
            r###"
            echo "Getting ticket for {principal}" from {KERBEROS_CONTAINER_PATH}/keytab
            kinit "{principal}" -kt {KERBEROS_CONTAINER_PATH}/keytab
            "###,
        ))
    }

    fn get_namenode_service_state_command() -> String {
        formatdoc!(
            r###"
                  SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)"###,
            hadoop_home = Self::HADOOP_HOME,
        )
    }

    /// Returns the container env variables.
    fn env(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        role_group: &str,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        resources: Option<&ResourceRequirements>,
    ) -> Result<Vec<EnvVar>, Error> {
        // Maps env var name to env var object. This allows env_overrides to work
        // as expected (i.e. users can override the env var value).
        let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

        env.extend(
            Self::shared_env_vars(
                self.volume_mount_dirs().final_config(),
                zookeeper_config_map_name,
            )
            .into_iter()
            .map(|env_var| (env_var.name.clone(), env_var)),
        );

        // For the main container we use specialized env variables for every role
        // (think of like HDFS_NAMENODE_OPTS or HDFS_DATANODE_OPTS)
        // We do so, so that users shelling into the hdfs Pods will not have problems
        // because the will read out the HADOOP_OPTS env var as well for the cli clients
        // (but *not* the HDFS_NAMENODE_OPTS env var)!
        // The hadoop opts contain a Prometheus metric emitter, which binds itself to a static port.
        // When the users tries to start a cli tool the port is already taken by the hdfs services,
        // so we don't want to stuff all the config into HADOOP_OPTS, but rather into the specialized env variables
        // See https://github.com/stackabletech/hdfs-operator/issues/138 for details
        if let ContainerConfig::Hdfs { role, .. } = self {
            let role_opts_name = role.hadoop_opts_env_var_for_role().to_string();
            env.insert(
                role_opts_name.clone(),
                EnvVar {
                    name: role_opts_name,
                    value: Some(self.build_hadoop_opts(hdfs, role_group, resources)?),
                    ..EnvVar::default()
                },
            );
        }

        env.insert(
            "HADOOP_OPTS".to_string(),
            EnvVar {
                name: "HADOOP_OPTS".to_string(),
                value: Some(construct_global_jvm_args(hdfs.has_kerberos_enabled())),
                ..EnvVar::default()
            },
        );
        if hdfs.has_kerberos_enabled() {
            env.insert(
                "KRB5_CONFIG".to_string(),
                EnvVar {
                    name: "KRB5_CONFIG".to_string(),
                    value: Some(format!("{KERBEROS_CONTAINER_PATH}/krb5.conf")),
                    ..EnvVar::default()
                },
            );
            env.insert(
                "KRB5_CLIENT_KTNAME".to_string(),
                EnvVar {
                    name: "KRB5_CLIENT_KTNAME".to_string(),
                    value: Some(format!("{KERBEROS_CONTAINER_PATH}/keytab")),
                    ..EnvVar::default()
                },
            );
        }

        // Needed for the `containerdebug` process to log it's tracing information to.
        env.insert(
            "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
            EnvVar {
                name: "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
                value: Some(format!("{STACKABLE_LOG_DIR}/containerdebug")),
                value_from: None,
            },
        );

        // Overrides need to come last
        let mut env_override_vars: BTreeMap<String, EnvVar> =
            Self::transform_env_overrides_to_env_vars(env_overrides)
                .into_iter()
                .map(|env_var| (env_var.name.clone(), env_var))
                .collect();

        env.append(&mut env_override_vars);

        Ok(env.into_values().collect())
    }

    /// Returns the container resources.
    pub fn resources(&self, merged_config: &AnyNodeConfig) -> Option<ResourceRequirements> {
        match self {
            // Namenode sidecar containers
            ContainerConfig::Zkfc { .. } => Some(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("100m")
                    .with_cpu_limit("400m")
                    .with_memory_request("512Mi")
                    .with_memory_limit("512Mi")
                    .build(),
            ),
            // Main container and init containers
            ContainerConfig::Hdfs { .. }
            | ContainerConfig::FormatNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. }
            | ContainerConfig::WaitForNameNodes { .. } => match merged_config {
                AnyNodeConfig::Name(node) => Some(node.resources.clone().into()),
                AnyNodeConfig::Data(node) => Some(node.resources.clone().into()),
                AnyNodeConfig::Journal(node) => Some(node.resources.clone().into()),
            },
        }
    }

    /// Creates a probe for the web UI port
    fn web_ui_port_probe(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        period_seconds: i32,
        initial_delay_seconds: i32,
        failure_threshold: i32,
    ) -> Option<Probe> {
        let ContainerConfig::Hdfs {
            web_ui_http_port_name,
            web_ui_https_port_name,
            ..
        } = self
        else {
            return None;
        };

        let port = if hdfs.has_https_enabled() {
            web_ui_https_port_name
        } else {
            web_ui_http_port_name
        };

        Some(Probe {
            // Use tcp_socket instead of http_get so that the probe is independent of the authentication settings.
            tcp_socket: Some(Self::tcp_socket_action_for_port(*port)),
            period_seconds: Some(period_seconds),
            initial_delay_seconds: Some(initial_delay_seconds),
            failure_threshold: Some(failure_threshold),
            ..Probe::default()
        })
    }

    /// Creates a probe for the IPC/RPC port
    fn ipc_port_probe(
        &self,
        period_seconds: i32,
        initial_delay_seconds: i32,
        failure_threshold: i32,
    ) -> Option<Probe> {
        match self {
            ContainerConfig::Hdfs { ipc_port_name, .. } => Some(Probe {
                tcp_socket: Some(Self::tcp_socket_action_for_port(*ipc_port_name)),
                period_seconds: Some(period_seconds),
                initial_delay_seconds: Some(initial_delay_seconds),
                failure_threshold: Some(failure_threshold),
                ..Probe::default()
            }),
            _ => None,
        }
    }

    fn tcp_socket_action_for_port(port: impl Into<String>) -> TCPSocketAction {
        TCPSocketAction {
            port: IntOrString::String(port.into()),
            ..Default::default()
        }
    }

    /// Return the container volumes.
    fn volumes(
        &self,
        merged_config: &AnyNodeConfig,
        object_name: &str,
        labels: &Labels,
    ) -> Result<Vec<Volume>> {
        let mut volumes = vec![];

        if let ContainerConfig::Hdfs { .. } = self {
            if let AnyNodeConfig::Data(node) = merged_config {
                volumes.push(
                    VolumeBuilder::new(LISTENER_VOLUME_NAME)
                        .ephemeral(
                            ListenerOperatorVolumeSourceBuilder::new(
                                &ListenerReference::ListenerClass(node.listener_class.to_string()),
                                labels,
                            )
                            .build_ephemeral()
                            .context(BuildListenerVolumeSnafu)?,
                        )
                        .build(),
                );
            }

            volumes.push(
                VolumeBuilder::new(ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME)
                    .empty_dir(EmptyDirVolumeSource {
                        medium: None,
                        size_limit: Some(
                            product_logging::framework::calculate_log_volume_size_limit(&[
                                MAX_HDFS_LOG_FILE_SIZE,
                                MAX_ZKFC_LOG_FILE_SIZE,
                                MAX_FORMAT_NAMENODE_LOG_FILE_SIZE,
                                MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE,
                                MAX_WAIT_NAMENODES_LOG_FILE_SIZE,
                            ]),
                        ),
                    })
                    .build(),
            );
        }

        let container_log_config = match self {
            ContainerConfig::Hdfs { .. } => Some(merged_config.hdfs_logging()),
            ContainerConfig::Zkfc { .. } => merged_config
                .as_namenode()
                .map(|node| node.logging.for_container(&NameNodeContainer::Zkfc)),
            ContainerConfig::FormatNameNodes { .. } => merged_config.as_namenode().map(|node| {
                node.logging
                    .for_container(&NameNodeContainer::FormatNameNodes)
            }),
            ContainerConfig::FormatZooKeeper { .. } => merged_config.as_namenode().map(|node| {
                node.logging
                    .for_container(&NameNodeContainer::FormatZooKeeper)
            }),
            ContainerConfig::WaitForNameNodes { .. } => merged_config.as_datanode().map(|node| {
                node.logging
                    .for_container(&DataNodeContainer::WaitForNameNodes)
            }),
        };
        volumes.extend(Self::common_container_volumes(
            container_log_config.as_deref(),
            object_name,
            self.volume_mount_dirs().config_mount_name(),
            self.volume_mount_dirs().log_mount_name(),
        ));

        Ok(volumes)
    }

    /// Returns the container volume mounts.
    fn volume_mounts(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        merged_config: &AnyNodeConfig,
        labels: &Labels,
    ) -> Result<Vec<VolumeMount>> {
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

        // Adding this for all containers, as not only the main container needs Kerberos or TLS
        if hdfs.has_kerberos_enabled() {
            volume_mounts
                .push(VolumeMountBuilder::new("kerberos", KERBEROS_CONTAINER_PATH).build());
        }
        if hdfs.has_https_enabled() {
            // This volume will be propagated by the create-tls-cert-bundle container
            volume_mounts
                .push(VolumeMountBuilder::new(TLS_STORE_VOLUME_NAME, TLS_STORE_DIR).build());
        }

        match self {
            ContainerConfig::FormatNameNodes { .. } => {
                // As FormatNameNodes only runs on the Namenodes we can safely assume the only pvc is called "data".
                volume_mounts.push(
                    VolumeMountBuilder::new(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
                        .build(),
                );
            }
            ContainerConfig::Hdfs { role, .. } => {
                // JournalNode doesn't use listeners, since it's only used internally by the namenodes
                if let HdfsNodeRole::Name | HdfsNodeRole::Data = role {
                    volume_mounts.push(
                        VolumeMountBuilder::new(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR).build(),
                    );
                }

                // Add data volume
                match role {
                    HdfsNodeRole::Name | HdfsNodeRole::Journal => {
                        volume_mounts.push(
                            VolumeMountBuilder::new(
                                Self::DATA_VOLUME_MOUNT_NAME,
                                STACKABLE_ROOT_DATA_DIR,
                            )
                            .build(),
                        );
                    }
                    HdfsNodeRole::Data => {
                        for pvc in Self::volume_claim_templates(merged_config, labels)? {
                            let pvc_name = pvc.name_any();
                            volume_mounts.push(VolumeMount {
                                mount_path: format!("{DATANODE_ROOT_DATA_DIR_PREFIX}{pvc_name}"),
                                name: pvc_name,
                                ..VolumeMount::default()
                            });
                        }
                    }
                }
            }
            // The other containers don't need any data pvcs to be mounted
            ContainerConfig::Zkfc { .. }
            | ContainerConfig::WaitForNameNodes { .. }
            | ContainerConfig::FormatZooKeeper { .. } => {}
        }

        Ok(volume_mounts)
    }

    /// Create a config directory for the respective container.
    fn create_config_directory_cmd(&self) -> String {
        format!(
            "mkdir -p {config_dir_name}\n",
            config_dir_name = self.volume_mount_dirs().final_config()
        )
    }

    /// Copy all the configuration files to the respective container config dir.
    fn copy_config_xml_cmd(&self) -> String {
        format!(
            "cp {config_dir_mount}/*.xml {config_dir_name}\n",
            config_dir_mount = self.volume_mount_dirs().config_mount(),
            config_dir_name = self.volume_mount_dirs().final_config()
        )
    }

    /// Copy the `log4j.properties` to the respective container config dir.
    /// This will be copied from:
    /// - Custom: the log dir mount of the custom config map
    /// - Automatic: the container config mount dir
    fn copy_log4j_properties_cmd(
        &self,
        log4j_config_file: &str,
        container_log_config: &ContainerLogConfig,
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
            "cp {log4j_properties_dir}/{file_name} {config_dir}/{LOG4J_PROPERTIES}\n",
            log4j_properties_dir = source_log4j_properties_dir,
            file_name = log4j_config_file,
            config_dir = self.volume_mount_dirs().final_config()
        )
    }

    /// Build HADOOP_{*node}_OPTS for each namenode, datanodes and journalnodes.
    fn build_hadoop_opts(
        &self,
        hdfs: &v1alpha1::HdfsCluster,
        role_group: &str,
        resources: Option<&ResourceRequirements>,
    ) -> Result<String, Error> {
        match self {
            ContainerConfig::Hdfs {
                role, metrics_port, ..
            } => {
                let cvd = ContainerVolumeDirs::from(role);
                let config_dir = cvd.final_config();
                construct_role_specific_jvm_args(
                    hdfs,
                    role,
                    role_group,
                    hdfs.has_kerberos_enabled(),
                    resources,
                    config_dir,
                    *metrics_port,
                )
                .with_context(|_| ConstructJvmArgumentsSnafu {
                    role: role.to_string(),
                })
            }
            _ => Ok("".to_string()),
        }
    }

    /// Container ports for the main containers namenode, datanode and journalnode.
    fn container_ports(&self, hdfs: &v1alpha1::HdfsCluster) -> Vec<ContainerPort> {
        match self {
            ContainerConfig::Hdfs { role, .. } => {
                // data ports
                hdfs.hdfs_main_container_ports(role)
                    .into_iter()
                    .map(|(name, value)| ContainerPort {
                        name: Some(name),
                        container_port: i32::from(value),
                        protocol: Some("TCP".to_string()),
                        ..ContainerPort::default()
                    })
                    .collect()
            }
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
                        name: String::from(zk_config_map_name),
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
        container_log_config: Option<&ContainerLogConfig>,
        object_name: &str,
        config_volume_name: &str,
        log_volume_name: &str,
    ) -> Vec<Volume> {
        let mut volumes = vec![];
        if let Some(container_log_config) = container_log_config {
            volumes.push(
                VolumeBuilder::new(config_volume_name)
                    .config_map(ConfigMapVolumeSource {
                        name: object_name.to_string(),
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
                            name: config_map.clone(),
                            ..ConfigMapVolumeSource::default()
                        })
                        .build(),
                );
            } else {
                volumes.push(
                    VolumeBuilder::new(log_volume_name)
                        .config_map(ConfigMapVolumeSource {
                            name: object_name.to_string(),
                            ..ConfigMapVolumeSource::default()
                        })
                        .build(),
                );
            }
        }
        volumes
    }
}

impl From<HdfsNodeRole> for ContainerConfig {
    fn from(role: HdfsNodeRole) -> Self {
        match role {
            HdfsNodeRole::Name => Self::Hdfs {
                role,
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                ipc_port_name: SERVICE_PORT_NAME_RPC,
                web_ui_http_port_name: SERVICE_PORT_NAME_HTTP,
                web_ui_https_port_name: SERVICE_PORT_NAME_HTTPS,
                metrics_port: DEFAULT_NAME_NODE_METRICS_PORT,
            },
            HdfsNodeRole::Data => Self::Hdfs {
                role,
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                ipc_port_name: SERVICE_PORT_NAME_IPC,
                web_ui_http_port_name: SERVICE_PORT_NAME_HTTP,
                web_ui_https_port_name: SERVICE_PORT_NAME_HTTPS,
                metrics_port: DEFAULT_DATA_NODE_METRICS_PORT,
            },
            HdfsNodeRole::Journal => Self::Hdfs {
                role,
                container_name: role.to_string(),
                volume_mounts: ContainerVolumeDirs::from(role),
                ipc_port_name: SERVICE_PORT_NAME_RPC,
                web_ui_http_port_name: SERVICE_PORT_NAME_HTTP,
                web_ui_https_port_name: SERVICE_PORT_NAME_HTTPS,
                metrics_port: DEFAULT_JOURNAL_NODE_METRICS_PORT,
            },
        }
    }
}

impl TryFrom<String> for ContainerConfig {
    type Error = Error;

    fn try_from(container_name: String) -> Result<Self, Self::Error> {
        match HdfsNodeRole::from_str(container_name.as_str()) {
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

impl From<HdfsNodeRole> for ContainerVolumeDirs {
    fn from(role: HdfsNodeRole) -> Self {
        ContainerVolumeDirs {
            final_config_dir: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR),
            config_mount: format!("{base}/{role}", base = Self::NODE_BASE_CONFIG_DIR_MOUNT),
            config_mount_name: ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME.to_string(),
            log_mount: format!("{base}/{role}", base = Self::NODE_BASE_LOG_DIR_MOUNT),
            log_mount_name: ContainerConfig::HDFS_LOG_VOLUME_MOUNT_NAME.to_string(),
        }
    }
}

impl From<&HdfsNodeRole> for ContainerVolumeDirs {
    fn from(role: &HdfsNodeRole) -> Self {
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
        if let Ok(role) = HdfsNodeRole::from_str(container_name) {
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
                });
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

fn bash_capture_shell_helper(container_name: &str) -> String {
    let capture_shell_output = product_logging::framework::capture_shell_output(
        STACKABLE_LOG_DIR,
        container_name,
        // we do not access any of the crd config options for this and just log it to file
        &AutomaticContainerLogConfig::default(),
    );

    formatdoc! {
        r###"
        # Store the original stdout/stderr globally so we can always find our way back
        # 3 and 4 are usually safe, but we'll be explicit.
        exec 3>&1
        exec 4>&2

        start_capture() {{
            # We redirect 1 and 2 to the background tee processes
            {capture_shell_output}
        }}

        stop_capture() {{
            # Restore stdout and stderr from our saved descriptors
            exec 1>&3 2>&4
        }}

        exclude_from_capture() {{
            # Temporarily restore original FDs just for the duration of this command
            # We use 'local' for the exit code to keep things clean
            set +e
            "$@" 1>&3 2>&4
            local exit_code=$?
            set -e
            
            # If the command failed, we manually trigger the exit since we set +e
            if [ $exit_code -ne 0 ]; then
                exit $exit_code
            fi
        }}
        "###
    }
}
