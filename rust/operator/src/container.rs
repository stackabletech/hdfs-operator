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
use crate::{
    hdfs_controller::KEYSTORE_DIR_NAME,
    product_logging::{
        FORMAT_NAMENODES_LOG4J_CONFIG_FILE, FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
        HDFS_LOG4J_CONFIG_FILE, MAX_LOG_FILES_SIZE_IN_MIB, STACKABLE_LOG_DIR,
        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE, ZKFC_LOG4J_CONFIG_FILE,
    },
};

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{
        DATANODE_ROOT_DATA_DIR_PREFIX, DEFAULT_DATA_NODE_METRICS_PORT,
        DEFAULT_JOURNAL_NODE_METRICS_PORT, DEFAULT_NAME_NODE_METRICS_PORT, LOG4J_PROPERTIES,
        NAMENODE_ROOT_DATA_DIR, SERVICE_PORT_NAME_IPC, SERVICE_PORT_NAME_RPC,
        STACKABLE_ROOT_DATA_DIR,
    },
    storage::DataNodeStorageConfig,
    DataNodeContainer, HdfsCluster, HdfsPodRef, HdfsRole, MergedConfig, NameNodeContainer,
};
use stackable_operator::{
    builder::{
        resources::ResourceRequirementsBuilder, ContainerBuilder, PodBuilder,
        SecretOperatorVolumeSourceBuilder, VolumeBuilder, VolumeMountBuilder,
    },
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
use std::{collections::BTreeMap, str::FromStr};
use strum::{Display, EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
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
        hdfs: &HdfsCluster,
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
            hdfs,
            role,
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
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("250m")
                    .with_cpu_limit("500m")
                    .with_memory_request("128Mi")
                    .with_memory_limit("128Mi")
                    .build(),
            ));
        }

        if let Some(authentication_config) = hdfs.authentication_config() {
            pb.add_volume(
                VolumeBuilder::new("tls")
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            &authentication_config.tls_secret_class,
                        )
                        .with_pod_scope()
                        .with_node_scope()
                        .build(),
                    )
                    .build(),
            );

            pb.add_volume(
                VolumeBuilder::new("keystore")
                    .with_empty_dir(Option::<String>::None, None)
                    .build(),
            );

            pb.add_volume(
                VolumeBuilder::new("kerberos")
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            &authentication_config.kerberos.secret_class,
                        )
                        .with_service_scope(hdfs.name_any())
                        .with_kerberos_service_name(role.kerberos_service_name())
                        .with_kerberos_service_name("HTTP")
                        .build(),
                    )
                    .build(),
            );

            let create_tls_cert_bundle_init_container =
                ContainerBuilder::new("create-tls-cert-bundle")
                    .unwrap()
                    .image_from_product_image(resolved_product_image)
                    .command(Self::command())
                    .args(vec![formatdoc!(
                            r###"
                            echo "Cleaning up truststore - just in case"
                            rm -f {KEYSTORE_DIR_NAME}/truststore.p12
                            echo "Creating truststore"
                            keytool -importcert -file /stackable/tls/ca.crt -keystore {KEYSTORE_DIR_NAME}/truststore.p12 -storetype pkcs12 -noprompt -alias ca_cert -storepass changeit
                            echo "Creating certificate chain"
                            cat /stackable/tls/ca.crt /stackable/tls/tls.crt > {KEYSTORE_DIR_NAME}/chain.crt
                            echo "Cleaning up keystore - just in case"
                            rm -f {KEYSTORE_DIR_NAME}/keystore.p12
                            echo "Creating keystore"
                            openssl pkcs12 -export -in {KEYSTORE_DIR_NAME}/chain.crt -inkey /stackable/tls/tls.key -out {KEYSTORE_DIR_NAME}/keystore.p12 --passout pass:changeit"###
                        )])
                        // Only this init container needs the actual cert (from tls volume) to create the truststore + keystore from
                        .add_volume_mount("tls", "/stackable/tls")
                        .add_volume_mount("keystore", KEYSTORE_DIR_NAME)
                    .build();
            pb.add_init_container(create_tls_cert_bundle_init_container);
        }

        // role specific pod settings configured here
        match role {
            HdfsRole::NameNode => {
                // Zookeeper fail over container
                let zkfc_container_config = Self::try_from(NameNodeContainer::Zkfc.to_string())?;
                pb.add_volumes(zkfc_container_config.volumes(merged_config, object_name));
                pb.add_container(zkfc_container_config.main_container(
                    hdfs,
                    role,
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
                    hdfs,
                    role,
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
                    hdfs,
                    role,
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
                    hdfs,
                    role,
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
        hdfs: &HdfsCluster,
        role: &HdfsRole,
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
            .command(Self::command())
            .args(self.args(hdfs, role, merged_config, &[])?)
            .add_env_vars(self.env(
                hdfs,
                zookeeper_config_map_name,
                env_overrides,
                resources.as_ref(),
            ))
            .add_volume_mounts(self.volume_mounts(hdfs, merged_config))
            .add_container_ports(self.container_ports(hdfs));

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
    #[allow(clippy::too_many_arguments)]
    fn init_container(
        &self,
        hdfs: &HdfsCluster,
        role: &HdfsRole,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Result<Container, Error> {
        Ok(ContainerBuilder::new(self.name())
            .with_context(|_| InvalidContainerNameSnafu { name: self.name() })?
            .image_from_product_image(resolved_product_image)
            .command(Self::command())
            .args(self.args(hdfs, role, merged_config, namenode_podrefs)?)
            .add_env_vars(self.env(hdfs, zookeeper_config_map_name, env_overrides, None))
            .add_volume_mounts(self.volume_mounts(hdfs, merged_config))
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
        hdfs: &HdfsCluster,
        role: &HdfsRole,
        merged_config: &(dyn MergedConfig + Send + 'static),
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<Vec<String>, Error> {
        let mut args = String::new();
        args.push_str(&self.create_config_directory_cmd());
        args.push_str(&self.copy_config_xml_cmd());

        // We can't influence the order of the init containers.
        // Some init containers - such as format-namenodes - need the tls certs or kerberos tickets, so let's wait for them to be properly set up
        if hdfs.has_https_enabled() {
            args.push_str(&Self::wait_for_trust_and_keystore_command());
        }
        if hdfs.has_kerberos_enabled() {
            args.push_str(&Self::export_kerberos_real_env_var_command());
        }

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                args.push_str(&self.copy_log4j_properties_cmd(
                    HDFS_LOG4J_CONFIG_FILE,
                    merged_config.hdfs_logging(),
                ));
                args.push_str(&format!(
                    "{hadoop_home}/bin/hdfs {role}\n",
                    hadoop_home = Self::HADOOP_HOME,
                ));
            }
            ContainerConfig::Zkfc { .. } => {
                if let Some(container_config) = merged_config.zkfc_logging() {
                    args.push_str(
                        &self.copy_log4j_properties_cmd(ZKFC_LOG4J_CONFIG_FILE, container_config),
                    );
                }
                args.push_str(&format!(
                    "{hadoop_home}/bin/hdfs zkfc\n",
                    hadoop_home = Self::HADOOP_HOME
                ));
            }
            ContainerConfig::FormatNameNodes { .. } => {
                if let Some(container_config) = merged_config.format_namenodes_logging() {
                    args.push_str(&self.copy_log4j_properties_cmd(
                        FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
                        container_config,
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
                    args.push_str(&Self::get_kerberos_ticket(hdfs, role)?);
                }
                args.push_str(&formatdoc!(
                    r###"
                    echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
                    for namenode_id in {pod_names}
                    do
                      echo -n "Checking pod $namenode_id... "
                      {get_service_state_command}
                      if [ "$SERVICE_STATE" == "active" ]
                      then
                        ACTIVE_NAMENODE=$namenode_id
                        echo "active"
                        break
                      fi
                      echo ""
                    done

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
                      cat "{NAMENODE_ROOT_DATA_DIR}/current/VERSION"
                      echo "Pod $POD_NAME already formatted. Skipping..."
                    fi
                    "###,
                    get_service_state_command = Self::get_namenode_service_state_command(),
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
                    args.push_str(&self.copy_log4j_properties_cmd(
                        FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
                        container_config,
                    ));
                }
                args.push_str(&formatdoc!(
                    r###"
                    echo "Attempt to format ZooKeeper..."
                    if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
                      set +e
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
                    hadoop_home = Self::HADOOP_HOME
                ));
            }
            ContainerConfig::WaitForNameNodes { .. } => {
                if let Some(container_config) = merged_config.wait_for_namenodes() {
                    args.push_str(&self.copy_log4j_properties_cmd(
                        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
                        container_config,
                    ));
                }
                if hdfs.has_kerberos_enabled() {
                    args.push_str(&Self::get_kerberos_ticket(hdfs, role)?);
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

    /// Wait until the init container has created global trust and keystore shared between all containers
    fn wait_for_trust_and_keystore_command() -> String {
        formatdoc!(
            r###"until [ -f {KEYSTORE_DIR_NAME}/truststore.p12 ]; do
                echo 'Waiting for truststore to be created'
                sleep 1
            done
            until [ -f {KEYSTORE_DIR_NAME}/keystore.p12 ]; do
                echo 'Waiting for keystore to be created'
                sleep 1
            done
            "###
        )
    }

    // Command to export `KERBEROS_REALM` env var to default real from krb5.conf, e.g. `CLUSTER.LOCAL`
    fn export_kerberos_real_env_var_command() -> String {
        "export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /stackable/kerberos/krb5.conf)\n"
            .to_string()
    }

    /// Command to `kinit` a ticket using the principal created for the specified hdfs role
    /// Needs the KERBEROS_REALM env var, which will be written with `export_kerberos_real_env_var_command`
    /// Needs the POD_NAME env var to be present, which will be provided by the PodSpec
    fn get_kerberos_ticket(hdfs: &HdfsCluster, role: &HdfsRole) -> Result<String, Error> {
        let principal = format!(
            "{service_name}/{hdfs_name}.{namespace}.svc.cluster.local@${{KERBEROS_REALM}}",
            service_name = role.kerberos_service_name(),
            hdfs_name = hdfs.name_any(),
            namespace = hdfs.namespace().context(ObjectHasNoNamespaceSnafu)?,
        );
        Ok(formatdoc!(
            r###"
            echo "Getting ticket for {principal}" from /stackable/kerberos/keytab
            kinit "{principal}" -kt /stackable/kerberos/keytab
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
        hdfs: &HdfsCluster,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        resources: Option<&ResourceRequirements>,
    ) -> Vec<EnvVar> {
        let mut env = Vec::new();

        env.extend(Self::shared_env_vars(
            self.volume_mount_dirs().final_config(),
            zookeeper_config_map_name,
        ));

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
            env.push(EnvVar {
                name: role.hadoop_opts_env_var_for_role().to_string(),
                value: self.build_hadoop_opts(hdfs, resources).ok(),
                ..EnvVar::default()
            });
        }
        // Additionally, any other init or sidecar container must have access to the following settings.
        // As the Prometheus metric emitter is not part of this config it's safe to use for hdfs cli tools as well.
        // This will not only enable the init containers to work, but also the user to run e.g.
        // `bin/hdfs dfs -ls /` without getting `Caused by: java.lang.IllegalArgumentException: KrbException: Cannot locate default realm`
        // because the `-Djava.security.krb5.conf` setting is missing
        if hdfs.has_kerberos_enabled() {
            env.push(EnvVar {
                name: "HADOOP_OPTS".to_string(),
                value: Some("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_string()),
                ..EnvVar::default()
            });

            env.push(EnvVar {
                name: "KRB5_CONFIG".to_string(),
                value: Some("/stackable/kerberos/krb5.conf".to_string()),
                ..EnvVar::default()
            });
            env.push(EnvVar {
                name: "KRB5_CLIENT_KTNAME".to_string(),
                value: Some("/stackable/kerberos/keytab".to_string()),
                ..EnvVar::default()
            });
        }

        // Overrides need to come last
        env.extend(Self::transform_env_overrides_to_env_vars(env_overrides));
        env
    }

    /// Returns the container resources.
    fn resources(
        &self,
        merged_config: &(dyn MergedConfig + Send + 'static),
    ) -> Option<ResourceRequirements> {
        // See resource collection https://docs.google.com/spreadsheets/d/1iWX1g4HaY3sFN9846BYd8kXZDU6FQkwSPmILsWMClE0/edit#gid=379007403
        match self {
            ContainerConfig::Hdfs { role, .. } if role != &HdfsRole::DataNode => {
                merged_config.resources().map(|c| c.into())
            }
            ContainerConfig::Hdfs { role, .. } if role == &HdfsRole::DataNode => {
                merged_config.data_node_resources().map(|c| c.into())
            }
            ContainerConfig::Zkfc { .. } => Some(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("100m")
                    .with_cpu_limit("400m")
                    .with_memory_request("500Mi")
                    .with_memory_limit("500Mi")
                    .build(),
            ),
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
        hdfs: &HdfsCluster,
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

        // Adding this for all containers, as not only the main container needs Kerberos or TLS
        if hdfs.has_kerberos_enabled() {
            volume_mounts.push(VolumeMountBuilder::new("kerberos", "/stackable/kerberos").build());
        }
        if hdfs.has_https_enabled() {
            // This volume will be propagated by the create-tls-cert-bundle container
            volume_mounts.push(VolumeMountBuilder::new("keystore", KEYSTORE_DIR_NAME).build());
        }

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
                        let pvc_name = pvc.name_any();
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
            "cp {log4j_properties_dir}/{file_name} {config_dir}/{LOG4J_PROPERTIES}\n",
            log4j_properties_dir = source_log4j_properties_dir,
            file_name = log4j_config_file,
            config_dir = self.volume_mount_dirs().final_config()
        )
    }

    /// Build HADOOP_{*node}_OPTS for each namenode, datanodes and journalnodes.
    fn build_hadoop_opts(
        &self,
        hdfs: &HdfsCluster,
        resources: Option<&ResourceRequirements>,
    ) -> Result<String, Error> {
        match self {
            ContainerConfig::Hdfs {
                role, metrics_port, ..
            } => {
                let mut jvm_args = vec![
                    format!(
                        "-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={metrics_port}:/stackable/jmx/{role}.yaml",
                    )];

                if hdfs.has_kerberos_enabled() {
                    jvm_args.push(
                        "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_string(),
                    );
                }

                if let Some(memory_limit) = resources.and_then(|r| r.limits.as_ref()?.get("memory"))
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
    fn container_ports(&self, hdfs: &HdfsCluster) -> Vec<ContainerPort> {
        match self {
            ContainerConfig::Hdfs { role, .. } => hdfs
                .ports(role)
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
