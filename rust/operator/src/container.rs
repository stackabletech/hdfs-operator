use crate::{
    config::HdfsNodeDataDirectory,
    product_logging::{HDFS_LOG4J_CONFIG_FILE, STACKABLE_LOG_DIR, ZKFC_LOG4J_CONFIG_FILE},
};

use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{
        CORE_SITE_XML, DEFAULT_DATA_NODE_METRICS_PORT, DEFAULT_JOURNAL_NODE_METRICS_PORT,
        DEFAULT_NAME_NODE_METRICS_PORT, HDFS_SITE_XML, LOG4J_PROPERTIES, SERVICE_PORT_NAME_IPC,
        SERVICE_PORT_NAME_RPC,
    },
    HdfsPodRef, HdfsRole,
};
use stackable_operator::builder::VolumeMountBuilder;
use stackable_operator::{
    builder::ContainerBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapKeySelector, Container, ContainerPort, EnvVar, EnvVarSource,
            ObjectFieldSelector, Probe, ResourceRequirements, TCPSocketAction, VolumeMount,
        },
        apimachinery::pkg::util::intstr::IntOrString,
    },
    memory::to_java_heap,
    product_logging::spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
};
use std::{collections::BTreeMap, str::FromStr};
use strum::{Display, EnumDiscriminants, IntoStaticStr};

pub const HADOOP_HOME: &str = "/stackable/hadoop";

pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_ROOT_DATA_DIR: &str = "/stackable/data";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Invalid role [{name}] for HDFS main container configuration."))]
    InvalidContainerRole { name: String },
    #[snafu(display("Invalid java heap config for [{role}]"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
        role: String,
    },
    #[snafu(display("Invalid container configuration for [{container_name}]"))]
    InvalidContainerConfig { container_name: String },
    #[snafu(display("Invalid container name [{name}]"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Could not create ready or liveness probe for [{role}]"))]
    MissingProbe { role: String },
    #[snafu(display("Could not determine a metrics port for [{role}]"))]
    MissingMetricsPort { role: String },
}

#[derive(Display)]
pub enum ContainerConfig {
    Hdfs {
        /// HDFS rol (name-, data-, journal-node) which will be the container_name.
        role: HdfsRole,
        /// The container_name from the provide role
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
        /// Readiness and liveliness probe service port names
        tcp_socket_action_port_name: &'static str,
        /// The JMX Exporter metrics port
        metrics_port: u16,
    },
    Zkfc {
        container_name: String,
        /// Volume mounts for config and logging.
        volume_mounts: ContainerVolumeDirs,
    },
}

impl ContainerConfig {
    // extra containers
    pub const ZKFC_CONTAINER_NAME: &'static str = "zkfc";
    // volumes
    pub const STACKABLE_CONFIG_VOLUME_MOUNT_NAME: &'static str = "config";
    pub const STACKABLE_LOG_VOLUME_MOUNT_NAME: &'static str = "log";
    pub const DATA_VOLUME_MOUNT_NAME: &'static str = "data";
    pub const HDFS_CONFIG_VOLUME_MOUNT_NAME: &'static str = "hdfs-config";
    pub const HDFS_LOG_VOLUME_MOUNT_NAME: &'static str = "hdfs-log-config";
    pub const ZKFC_CONFIG_VOLUME_MOUNT_NAME: &'static str = "zkfc-config";
    pub const ZKFC_LOG_VOLUME_MOUNT_NAME: &'static str = "zkfc-log-config";

    const JVM_HEAP_FACTOR: f32 = 0.8;
    const VECTOR_TOML: &'static str = "vector.toml";

    /// Creates the main process containers for
    /// - Namenode main process
    /// - Namenode ZooKeeper fail over controller
    /// - Datanode main process
    /// - Journalnode main process
    pub fn main_container(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zk_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        resources: &ResourceRequirements,
        logging: &Logging<stackable_hdfs_crd::Container>,
    ) -> Result<Container, Error> {
        let mut cb =
            ContainerBuilder::new(self.name()).with_context(|_| InvalidContainerNameSnafu {
                name: self.name().to_string(),
            })?;

        cb.image_from_product_image(resolved_product_image)
            .command(Self::command())
            .args(self.args(logging))
            .add_env_vars(self.env(zk_config_map_name, env_overrides, resources)?)
            .add_volume_mounts(self.volume_mounts())
            .add_container_ports(self.container_ports())
            .resources(resources.clone());

        if let Some(probe) = self.tcp_socket_action_probe(10, 10) {
            cb.readiness_probe(probe.clone());
            cb.liveness_probe(probe);
        }

        Ok(cb.build())
    }

    pub fn init_containers(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zk_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<Vec<Container>, Error> {
        let mut init_containers = vec![];

        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);
        env.extend(Self::shared_env_vars(
            // We use the config mount dir directly here to not have to copy to the standard directory
            self.volume_mount_dirs().config_mount(),
            zk_config_map_name,
        ));

        match self {
            ContainerConfig::Hdfs { role, .. } => match role {
                HdfsRole::NameNode => {
                    init_containers.push(self.namenode_init_container_format_namenode(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        namenode_podrefs,
                    )?);
                    init_containers.push(self.namenode_init_container_format_zk(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                    )?)
                }
                HdfsRole::DataNode => {
                    init_containers.push(self.datanode_init_container_wait_for_namenode(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        namenode_podrefs,
                    )?);
                }
                HdfsRole::JournalNode => {}
            },
            ContainerConfig::Zkfc { .. } => {}
        }

        Ok(init_containers)
    }

    fn namenode_init_container_format_namenode(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<Container, Error> {
        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);
        env.extend(Self::shared_env_vars(
            // We use the config mount dir directly here to not have to copy
            self.volume_mount_dirs().config_mount(),
            zookeeper_config_map_name,
        ));

        Ok(ContainerBuilder::new("format-namenode")
            .with_context(|_| InvalidContainerNameSnafu {
                name: "format-namenode",
            })?
            .image_from_product_image(resolved_product_image)
            .args(vec![
                "sh".to_string(),
                "-c".to_string(),
                // First step we check for active namenodes. This step should return an active namenode
                // for e.g. scaling. It may fail if the active namenode is restarted and the standby
                // namenode takes over.
                // This is why in the second part we check if the node is formatted already via
                // $NAMENODE_DIR/current/VERSION. Then we dont do anything.
                // If there is no active namenode, the current pod is not formatted we format as
                // active namenode. Otherwise as standby node.
                format!(
                    "
                 echo \"Start formatting namenode $POD_NAME. Checking for active namenodes:\"
                 for id in {pod_names}
                 do
                   echo -n \"Checking pod $id... \"
                   SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                   if [ \"$SERVICE_STATE\" == \"active\" ]
                   then
                     ACTIVE_NAMENODE=$id
                     echo \"active\"
                     break
                   fi
                   echo \"\"
                 done

                 set -e
                 if [ ! -f \"{namenode_dir}/current/VERSION\" ]
                 then
                   if [ -z ${{ACTIVE_NAMENODE+x}} ]
                   then
                     echo \"Create pod $POD_NAME as active namenode.\"
                     {hadoop_home}/bin/hdfs namenode -format -noninteractive
                   else
                     echo \"Create pod $POD_NAME as standby namenode.\"
                     {hadoop_home}/bin/hdfs namenode -bootstrapStandby -nonInteractive
                   fi
                 else
                   echo \"Pod $POD_NAME already formatted. Skipping...\"
                 fi",
                    hadoop_home = HADOOP_HOME,
                    pod_names = namenode_podrefs
                        .iter()
                        .map(|pod_ref| pod_ref.pod_name.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" "),
                    namenode_dir = HdfsNodeDataDirectory::default().namenode,
                ),
            ])
            .add_env_vars(env)
            .add_volume_mount(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
            .add_volume_mount(
                Self::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                self.volume_mount_dirs().config_mount(),
            )
            .build())
    }

    fn namenode_init_container_format_zk(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
    ) -> Result<Container, Error> {
        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);
        env.extend(Self::shared_env_vars(
            // We use the config mount dir directly here to not have to copy
            self.volume_mount_dirs().config_mount(),
            zookeeper_config_map_name,
        ));

        Ok(ContainerBuilder::new("format-zk")
            .with_context(|_| InvalidContainerNameSnafu {
                name: "format-zk"
            })?
            .image_from_product_image(resolved_product_image)
            .args(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("
                    test \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && {HADOOP_HOME}/bin/hdfs zkfc -formatZK -nonInteractive || true"
                )
            ])
            .add_env_vars(env)
            .add_volume_mount(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
            .add_volume_mount(Self::HDFS_CONFIG_VOLUME_MOUNT_NAME, self.volume_mount_dirs().config_mount()).build())
    }

    fn datanode_init_container_wait_for_namenode(
        &self,
        resolved_product_image: &ResolvedProductImage,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        namenode_podrefs: &[HdfsPodRef],
    ) -> Result<Container, Error> {
        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);
        env.extend(Self::shared_env_vars(
            // We use the config mount dir directly here to not have to copy
            self.volume_mount_dirs().config_mount(),
            zookeeper_config_map_name,
        ));

        Ok(ContainerBuilder::new("wait-for-namenodes")
            .with_context(|_| InvalidContainerNameSnafu {
                name: "wait-for-namenodes",
            })?
            .image_from_product_image(resolved_product_image)
            .args(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!(
                    "
                echo \"Waiting for namenodes to get ready:\"
                n=0
                while [ ${{n}} -lt 12 ];
                do
                  ALL_NODES_READY=true
                  for id in {pod_names}
                  do
                    echo -n \"Checking pod $id... \"
                    SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                    if [ \"$SERVICE_STATE\" = \"active\" ] || [ \"$SERVICE_STATE\" = \"standby\" ]
                    then
                      echo \"$SERVICE_STATE\"
                    else
                      echo \"not ready\"
                      ALL_NODES_READY=false
                    fi
                  done
                  if [ \"$ALL_NODES_READY\" == \"true\" ]
                  then
                    echo \"All namenodes ready!\"
                    break
                  fi
                  echo \"\"
                  n=$(( n  + 1))
                  sleep 5
                done
            ",
                    hadoop_home = HADOOP_HOME,
                    pod_names = namenode_podrefs
                        .iter()
                        .map(|pod_ref| pod_ref.pod_name.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" ")
                ),
            ])
            .add_env_vars(env)
            .add_volume_mount(Self::DATA_VOLUME_MOUNT_NAME, STACKABLE_ROOT_DATA_DIR)
            .add_volume_mount(
                Self::HDFS_CONFIG_VOLUME_MOUNT_NAME,
                self.volume_mount_dirs().config_mount(),
            )
            .build())
    }

    /// Return the container name.
    pub fn name(&self) -> &str {
        match &self {
            ContainerConfig::Hdfs { container_name, .. } => container_name.as_str(),
            ContainerConfig::Zkfc { container_name, .. } => container_name.as_str(),
        }
    }

    /// Return volume mount directories depending on the container.
    fn volume_mount_dirs(&self) -> &ContainerVolumeDirs {
        match &self {
            ContainerConfig::Hdfs { volume_mounts, .. } => volume_mounts,
            ContainerConfig::Zkfc { volume_mounts, .. } => volume_mounts,
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
    fn args(&self, logging: &Logging<stackable_hdfs_crd::Container>) -> Vec<String> {
        let mut args = vec![
            self.create_config_directory_cmd(),
            self.copy_hdfs_and_core_site_xml_cmd(),
        ];

        if let Some(cp_vector_toml_cmd) = self.copy_vector_toml_cmd(logging) {
            args.push(cp_vector_toml_cmd);
        }

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                args.push(self.copy_log4j_properties_cmd(
                    logging,
                    HDFS_LOG4J_CONFIG_FILE,
                    stackable_hdfs_crd::Container::Hdfs,
                ));

                args.push(format!(
                    "{hadoop_home}/bin/hdfs --debug {role}",
                    hadoop_home = HADOOP_HOME,
                ));
            }
            ContainerConfig::Zkfc { .. } => {
                args.push(self.copy_log4j_properties_cmd(
                    logging,
                    ZKFC_LOG4J_CONFIG_FILE,
                    stackable_hdfs_crd::Container::Zkfc,
                ));

                args.push(format!("{HADOOP_HOME}/bin/hdfs zkfc"));
            }
        }
        vec![args.join(" && ")]
    }

    fn env(
        &self,
        zookeeper_config_map_name: &str,
        env_overrides: Option<&BTreeMap<String, String>>,
        resources: &ResourceRequirements,
    ) -> Result<Vec<EnvVar>, Error> {
        let mut env = Self::transform_env_overrides_to_env_vars(env_overrides);
        env.extend(Self::shared_env_vars(
            self.volume_mount_dirs().final_config(),
            zookeeper_config_map_name,
        ));

        match self {
            ContainerConfig::Hdfs { role, .. } => {
                env.push(EnvVar {
                    name: role.hadoop_opts().to_string(),
                    value: self.build_hadoop_opts(resources).ok(),
                    ..EnvVar::default()
                });
            }
            ContainerConfig::Zkfc { .. } => {}
        }

        Ok(env)
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

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        let mut volume_mounts = vec![
            VolumeMountBuilder::new(
                Self::STACKABLE_CONFIG_VOLUME_MOUNT_NAME,
                STACKABLE_CONFIG_DIR,
            )
            .build(),
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
        }
        volume_mounts
    }

    fn create_config_directory_cmd(&self) -> String {
        format!(
            "mkdir -p {config_dir_name}",
            config_dir_name = self.volume_mount_dirs().final_config()
        )
    }

    fn copy_vector_toml_cmd(
        &self,
        logging: &Logging<stackable_hdfs_crd::Container>,
    ) -> Option<String> {
        if logging.enable_vector_agent {
            return Some(format!(
                "cp {vector_toml_location}/{vector_file} {STACKABLE_CONFIG_DIR}/{vector_file}",
                vector_toml_location = self.volume_mount_dirs().config_mount(),
                vector_file = Self::VECTOR_TOML
            ));
        }
        None
    }

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

    fn copy_log4j_properties_cmd(
        &self,
        logging: &Logging<stackable_hdfs_crd::Container>,
        log4j_config_file: &str,
        container: stackable_hdfs_crd::Container,
    ) -> String {
        let source_log4j_properties_dir = if let Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Custom(_)),
        }) = logging.containers.get(&container)
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
            ContainerConfig::Zkfc { .. } => Ok("".to_string()),
        }
    }

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

    fn shared_env_vars(hadoop_conf_dir: &str, zk_config_map_name: &str) -> Vec<EnvVar> {
        vec![
            EnvVar {
                name: "HADOOP_HOME".to_string(),
                value: Some(String::from(HADOOP_HOME)),
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
            ContainerConfig::Zkfc { .. } => {
                vec![]
            }
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
                Self::ZKFC_CONTAINER_NAME => Ok(Self::Zkfc {
                    container_name: container_name.to_string(),
                    volume_mounts: ContainerVolumeDirs::from(Self::ZKFC_CONTAINER_NAME),
                }),
                _ => Err(Error::InvalidContainerConfig {
                    container_name: container_name.to_string(),
                }),
            },
        }
    }
}

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
