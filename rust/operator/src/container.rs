use crate::{
    config::HdfsNodeDataDirectory,
    product_logging::{LOG4J_CONFIG_FILE, STACKABLE_LOG_DIR, ZKFC_LOG4J_CONFIG_FILE},
};

use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{
        CORE_SITE_XML, DEFAULT_DATA_NODE_METRICS_PORT, DEFAULT_JOURNAL_NODE_METRICS_PORT,
        DEFAULT_NAME_NODE_METRICS_PORT, HDFS_SITE_XML, LOG4J_PROPERTIES, SERVICE_PORT_NAME_IPC,
        SERVICE_PORT_NAME_RPC,
    },
    HdfsCluster, HdfsPodRef, HdfsRole,
};
use stackable_operator::{
    builder::ContainerBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapKeySelector, Container, ContainerPort, EnvVar, EnvVarSource,
            ObjectFieldSelector, Probe, ResourceRequirements, TCPSocketAction,
        },
        apimachinery::pkg::util::intstr::IntOrString,
    },
    memory::to_java_heap,
    product_logging::spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
};
use std::collections::BTreeMap;
use strum::{EnumDiscriminants, IntoStaticStr};

const JVM_HEAP_FACTOR: f32 = 0.8;

const ZKFC_CONTAINER_NAME: &str = "zkfc";

const VECTOR_TOML: &str = "vector.toml";

const HDFS_NAME_NODE_OPTS: &str = "HDFS_NAMENODE_OPTS";
const HDFS_DATA_NODE_OPTS: &str = "HDFS_DATANODE_OPTS";
const HDFS_JOURNAL_NODE_OPTS: &str = "HDFS_JOURNALNODE_OPTS";

pub const HADOOP_HOME: &str = "/stackable/hadoop";

pub const STACKABLE_CONFIG_DIR: &str = "/stackable/config";
pub const STACKABLE_ROOT_DATA_DIR: &str = "/stackable/data";

pub const NAME_NODE_CONFIG_DIR: &str = "/stackable/config/namenode";
pub const NAME_NODE_CONFIG_DIR_MOUNT: &str = "/stackable/mount/config/namenode";
pub const NAME_NODE_LOG_DIR_MOUNT: &str = "/stackable/mount/log/namenode";

pub const NAME_NODE_ZKFC_CONFIG_DIR: &str = "/stackable/config/zkfc";
pub const NAME_NODE_ZKFC_CONFIG_DIR_MOUNT: &str = "/stackable/mount/config/zkfc";
pub const NAME_NODE_ZKFC_LOG_CONFIG_DIR: &str = "/stackable/mount/log/zkfc";

pub const DATA_NODE_CONFIG_DIR: &str = "/stackable/config/datanode";
pub const DATA_NODE_CONFIG_DIR_MOUNT: &str = "/stackable/mount/config/datanode";
pub const DATA_NODE_LOG_DIR_MOUNT: &str = "/stackable/mount/log/datanode";

pub const JOURNAL_NODE_CONFIG_DIR: &str = "/stackable/config/journalnode";
pub const JOURNAL_NODE_CONFIG_DIR_MOUNT: &str = "/stackable/mount/config/journalnode";
pub const JOURNAL_NODE_LOG_DIR_MOUNT: &str = "/stackable/mount/log/journalnode";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Invalid java heap config for [{role}]"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
        role: String,
    },
    #[snafu(display("Invalid container name [{name}]"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
        name: String,
    },
}

#[derive(Clone, Debug)]
struct ContainerConfig {
    /// Name of the container.
    pub container_name: String,
    /// Name of the Hadoop process OPTS e.g. HADOOP_NAMENODE_OPTS.
    pub hdfs_opts_name: &'static str,
    /// The config dir where to store core-site.xml, hdfs-size.xml and logging configs.
    pub config_dir: &'static str,
    /// The mount dir for the config files to be mounted via config map.
    pub config_dir_mount: &'static str,
    /// The log dir where to store the hdfs / zkfc container log files.
    pub log_dir: &'static str,
    /// The mount dir for the logging config files to be mounted via config map.
    pub log_dir_mount: &'static str,
    /// Metrics port used for different Hadoop processes.
    pub metrics_port: u16,
    /// Readiness and liveliness probe service port names
    pub probe_port_name: &'static str,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self {
            container_name: HdfsRole::NameNode.to_string(),
            hdfs_opts_name: HDFS_NAME_NODE_OPTS,
            config_dir: NAME_NODE_CONFIG_DIR,
            config_dir_mount: NAME_NODE_CONFIG_DIR_MOUNT,
            log_dir: STACKABLE_LOG_DIR,
            log_dir_mount: NAME_NODE_LOG_DIR_MOUNT,
            metrics_port: DEFAULT_NAME_NODE_METRICS_PORT,
            probe_port_name: SERVICE_PORT_NAME_RPC,
        }
    }
}

impl From<&HdfsRole> for ContainerConfig {
    fn from(role: &HdfsRole) -> Self {
        match role {
            HdfsRole::NameNode => Self::default(),
            HdfsRole::DataNode => Self {
                container_name: role.to_string(),
                hdfs_opts_name: HDFS_DATA_NODE_OPTS,
                config_dir: DATA_NODE_CONFIG_DIR,
                config_dir_mount: DATA_NODE_CONFIG_DIR_MOUNT,
                log_dir_mount: DATA_NODE_LOG_DIR_MOUNT,
                metrics_port: DEFAULT_DATA_NODE_METRICS_PORT,
                probe_port_name: SERVICE_PORT_NAME_IPC,
                ..ContainerConfig::default()
            },
            HdfsRole::JournalNode => Self {
                container_name: role.to_string(),
                hdfs_opts_name: HDFS_JOURNAL_NODE_OPTS,
                config_dir: JOURNAL_NODE_CONFIG_DIR,
                config_dir_mount: JOURNAL_NODE_CONFIG_DIR_MOUNT,
                log_dir_mount: JOURNAL_NODE_LOG_DIR_MOUNT,
                metrics_port: DEFAULT_JOURNAL_NODE_METRICS_PORT,
                probe_port_name: SERVICE_PORT_NAME_RPC,
                ..ContainerConfig::default()
            },
        }
    }
}

pub fn hdfs_main_container(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    resources: &ResourceRequirements,
    logging: &Logging<stackable_hdfs_crd::Container>,
) -> Result<Container, Error> {
    let container_config: ContainerConfig = ContainerConfig::from(role);

    let mut env = transform_env_overrides_to_env_vars(env_overrides);
    env.extend(shared_env_vars(
        container_config.config_dir,
        &hdfs.spec.zookeeper_config_map_name,
    ));

    env.push(EnvVar {
        name: container_config.hdfs_opts_name.to_string(),
        value: Some(get_opts(role, container_config.metrics_port, resources)?),
        ..EnvVar::default()
    });

    Ok(ContainerBuilder::new(&container_config.container_name)
        .with_context(|_| InvalidContainerNameSnafu {
            name: container_config.container_name.to_string(),
        })?
        .image_from_product_image(resolved_product_image)
        .command(container_command())
        .args(main_container_args(role, &container_config, logging))
        .add_env_vars(env)
        .add_container_ports(container_ports(role))
        .add_volume_mount("data", STACKABLE_ROOT_DATA_DIR)
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("log", container_config.log_dir)
        .add_volume_mount("hdfs-config", container_config.config_dir_mount)
        .add_volume_mount("hdfs-log-config", container_config.log_dir_mount)
        .readiness_probe(tcp_socket_action_probe(
            container_config.probe_port_name,
            10,
            10,
        ))
        .liveness_probe(tcp_socket_action_probe(
            container_config.probe_port_name,
            10,
            10,
        ))
        .resources(resources.clone())
        .build())
}

pub fn namenode_zkfc_container(
    hdfs: &HdfsCluster,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    logging: &Logging<stackable_hdfs_crd::Container>,
) -> Result<Container, Error> {
    let container_config = ContainerConfig {
        container_name: ZKFC_CONTAINER_NAME.to_string(),
        config_dir: NAME_NODE_ZKFC_CONFIG_DIR,
        config_dir_mount: NAME_NODE_ZKFC_CONFIG_DIR_MOUNT,
        log_dir_mount: NAME_NODE_ZKFC_LOG_CONFIG_DIR,
        ..ContainerConfig::default()
    };

    let mut env = transform_env_overrides_to_env_vars(env_overrides);
    env.extend(shared_env_vars(
        container_config.config_dir,
        &hdfs.spec.zookeeper_config_map_name,
    ));

    Ok(ContainerBuilder::new(&container_config.container_name)
        .with_context(|_| InvalidContainerNameSnafu {
            name: container_config.container_name.as_str(),
        })?
        .image_from_product_image(resolved_product_image)
        .command(container_command())
        .args(vec![[
            create_config_directory_cmd(&container_config),
            copy_vector_toml_cmd(&container_config, logging),
            copy_hdfs_and_core_site_xml_cmd(&container_config),
            copy_log4j_properties_cmd(
                &container_config,
                logging,
                ZKFC_LOG4J_CONFIG_FILE,
                stackable_hdfs_crd::Container::Zkfc,
            ),
            format!("{HADOOP_HOME}/bin/hdfs zkfc"),
        ]
        .join(" && ")])
        .add_env_vars(env)
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("log", container_config.log_dir)
        .add_volume_mount("zkfc-config", container_config.config_dir_mount)
        .add_volume_mount("zkfc-log-config", container_config.log_dir_mount)
        .build())
}

pub fn namenode_init_containers(
    hdfs: &HdfsCluster,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
) -> Result<Vec<Container>, Error> {
    let mut env = transform_env_overrides_to_env_vars(env_overrides);
    env.extend(shared_env_vars(
        // We use the config mount dir directly here to not have to copy
        NAME_NODE_CONFIG_DIR_MOUNT,
        &hdfs.spec.zookeeper_config_map_name,
    ));

    Ok(vec![ContainerBuilder::new("format-namenode")
             .with_context(|_| InvalidContainerNameSnafu {
                 name: "format-namenode"
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
                 format!("
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
                    pod_names = namenode_podrefs.iter().map(|pod_ref| pod_ref.pod_name.as_ref()).collect::<Vec<&str>>().join(" "),
                    namenode_dir = HdfsNodeDataDirectory::default().namenode,
                 ),
             ])
             .add_env_vars(env.clone())
             .add_volume_mount("data", STACKABLE_ROOT_DATA_DIR)
             .add_volume_mount("hdfs-config", NAME_NODE_CONFIG_DIR_MOUNT)
             .build(),
        ContainerBuilder::new("format-zk")
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
            .add_volume_mount("data", STACKABLE_ROOT_DATA_DIR)
            .add_volume_mount("hdfs-config", NAME_NODE_CONFIG_DIR_MOUNT)
            .build(),
    ])
}

pub fn datanode_init_containers(
    hdfs: &HdfsCluster,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
) -> Result<Vec<Container>, Error> {
    let mut env = transform_env_overrides_to_env_vars(env_overrides);
    env.extend(shared_env_vars(
        // We use the config mount dir directly here to not have to copy
        DATA_NODE_CONFIG_DIR_MOUNT,
        &hdfs.spec.zookeeper_config_map_name,
    ));

    Ok(vec![ContainerBuilder::new("wait-for-namenodes")
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
        .add_volume_mount("data", STACKABLE_ROOT_DATA_DIR)
        .add_volume_mount("hdfs-config", DATA_NODE_CONFIG_DIR_MOUNT)
        .build()])
}

fn create_config_directory_cmd(container_config: &ContainerConfig) -> String {
    format!(
        "mkdir -p {config_dir_name}",
        config_dir_name = container_config.config_dir
    )
}

fn copy_vector_toml_cmd(
    container_config: &ContainerConfig,
    logging: &Logging<stackable_hdfs_crd::Container>,
) -> String {
    if logging.enable_vector_agent {
        format!(
            "cp {vector_toml_location}/{vector_file} {STACKABLE_CONFIG_DIR}/{vector_file}",
            vector_toml_location = container_config.config_dir_mount,
            vector_file = VECTOR_TOML
        )
    } else {
        "echo test".to_string()
    }
}

fn copy_hdfs_and_core_site_xml_cmd(container_config: &ContainerConfig) -> String {
    vec![
        format!(
            "cp {config_dir_mount}/{HDFS_SITE_XML} {config_dir_name}/{HDFS_SITE_XML}",
            config_dir_mount = container_config.config_dir_mount,
            config_dir_name = container_config.config_dir
        ),
        format!(
            "cp {config_dir_mount}/{CORE_SITE_XML} {config_dir_name}/{CORE_SITE_XML}",
            config_dir_mount = container_config.config_dir_mount,
            config_dir_name = container_config.config_dir
        ),
    ]
    .join(" && ")
}

fn copy_log4j_properties_cmd(
    container_config: &ContainerConfig,
    logging: &Logging<stackable_hdfs_crd::Container>,
    log4j_config_file: &str,
    container: stackable_hdfs_crd::Container,
) -> String {
    let source_log4j_properties_dir = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Custom(_)),
    }) = logging.containers.get(&container)
    {
        container_config.log_dir_mount
    } else {
        container_config.config_dir_mount
    };

    format!(
        "cp {log4j_properties_dir}/{file_name} {config_dir}/{LOG4J_PROPERTIES}",
        log4j_properties_dir = source_log4j_properties_dir,
        file_name = log4j_config_file,
        config_dir = container_config.config_dir
    )
}

fn main_container_args(
    role: &HdfsRole,
    container_config: &ContainerConfig,
    logging: &Logging<stackable_hdfs_crd::Container>,
) -> Vec<String> {
    vec![[
        create_config_directory_cmd(container_config),
        copy_vector_toml_cmd(container_config, &logging),
        copy_hdfs_and_core_site_xml_cmd(container_config),
        copy_log4j_properties_cmd(
            container_config,
            logging,
            LOG4J_CONFIG_FILE,
            stackable_hdfs_crd::Container::Hdfs,
        ),
        format!(
            "{hadoop_home}/bin/hdfs --debug {role}",
            hadoop_home = HADOOP_HOME,
        ),
    ]
    .join(" && ")]
}

fn container_command() -> Vec<String> {
    vec![
        "/bin/bash".to_string(),
        "-x".to_string(),
        "-euo".to_string(),
        "pipefail".to_string(),
        "-c".to_string(),
    ]
}

fn container_ports(role: &HdfsRole) -> Vec<ContainerPort> {
    role.ports()
        .into_iter()
        .map(|(name, value)| ContainerPort {
            name: Some(name),
            container_port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        })
        .collect()
}

fn get_opts(
    role: &HdfsRole,
    metrics_port: u16,
    resources: &ResourceRequirements,
) -> Result<String, Error> {
    Ok(vec![
        Some(jmx_metrics_opts(&role.to_string(), metrics_port)),
        Some(java_heap_opts(role, resources)?),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(" ")
    .trim()
    .to_string())
}

fn jmx_metrics_opts(role: &str, metrics_port: u16) -> String {
    format!(
        "-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
        metrics_port, role
    )
}

fn java_heap_opts(role: &HdfsRole, resources: &ResourceRequirements) -> Result<String, Error> {
    resources
        .limits
        .as_ref()
        .and_then(|l| l.get("memory"))
        .map(|m| to_java_heap(m, JVM_HEAP_FACTOR))
        .unwrap_or_else(|| Ok("".to_string()))
        .with_context(|_| InvalidJavaHeapConfigSnafu {
            role: role.to_string(),
        })
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

/// Creates a probe for [`stackable_operator::k8s_openapi::api::core::v1::TCPSocketAction`]
/// for liveness or readiness probes
fn tcp_socket_action_probe(
    port_name: &str,
    period_seconds: i32,
    initial_delay_seconds: i32,
) -> Probe {
    Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::String(String::from(port_name)),
            ..TCPSocketAction::default()
        }),
        period_seconds: Some(period_seconds),
        initial_delay_seconds: Some(initial_delay_seconds),
        ..Probe::default()
    }
}
