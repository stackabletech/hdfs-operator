//! Builds the log4j and Vector logging configuration for the rolegroup `ConfigMap`.

use std::{borrow::Cow, fmt::Display};

use stackable_operator::{
    kube::runtime::reflector::ObjectRef,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
    v2::types::operator::RoleGroupName,
};

use crate::{
    controller::ValidatedCluster,
    crd::{AnyNodeConfig, DataNodeContainer, HdfsNodeRole, NameNodeContainer},
};

pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
// We have a maximum of 4 continuous logging files for Namenodes. Datanodes and Journalnodes
// require less.
// - name node main container
// - zkfc side container
// - format namenode init container
// - format zookeeper init container
pub const MAX_HDFS_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};
pub const MAX_ZKFC_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};
pub const MAX_FORMAT_NAMENODE_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};
pub const MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};
pub const MAX_WAIT_NAMENODES_LOG_FILE_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

pub const HDFS_LOG4J_CONFIG_FILE: &str = "hdfs.log4j.properties";
pub const ZKFC_LOG4J_CONFIG_FILE: &str = "zkfc.log4j.properties";
pub const FORMAT_NAMENODES_LOG4J_CONFIG_FILE: &str = "format-namenodes.log4j.properties";
pub const FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE: &str = "format-zookeeper.log4j.properties";
pub const WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE: &str = "wait-for-namenodes.log4j.properties";

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n";

const HDFS_LOG_FILE: &str = "hdfs.log4j.xml";
const ZKFC_LOG_FILE: &str = "zkfc.log4j.xml";
const FORMAT_NAMENODES_LOG_FILE: &str = "format-namenodes.log4j.xml";
const FORMAT_ZOOKEEPER_LOG_FILE: &str = "format-zookeeper.log4j.xml";
const WAIT_FOR_NAMENODES_LOG_FILE: &str = "wait-for-namenodes.log4j.xml";

/// Renders the `*.log4j.properties` files for every container of this role group that uses the
/// operator's automatic logging configuration.
///
/// Returns `(filename, rendered content)` pairs; containers using a custom log ConfigMap are
/// skipped, so the result is empty when none use automatic logging.
pub fn build_log4j_configs(merged_config: &AnyNodeConfig) -> Vec<(&'static str, String)> {
    let mut configs = Vec::new();

    add_log4j_config_if_automatic(
        &mut configs,
        Some(merged_config.hdfs_logging()),
        HDFS_LOG4J_CONFIG_FILE,
        "hdfs",
        HDFS_LOG_FILE,
        MAX_HDFS_LOG_FILE_SIZE,
    );
    add_log4j_config_if_automatic(
        &mut configs,
        merged_config
            .as_namenode()
            .map(|nn| nn.logging.for_container(&NameNodeContainer::Zkfc)),
        ZKFC_LOG4J_CONFIG_FILE,
        &NameNodeContainer::Zkfc,
        ZKFC_LOG_FILE,
        MAX_ZKFC_LOG_FILE_SIZE,
    );
    add_log4j_config_if_automatic(
        &mut configs,
        merged_config.as_namenode().map(|nn| {
            nn.logging
                .for_container(&NameNodeContainer::FormatNameNodes)
        }),
        FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
        &NameNodeContainer::FormatNameNodes,
        FORMAT_NAMENODES_LOG_FILE,
        MAX_FORMAT_NAMENODE_LOG_FILE_SIZE,
    );
    add_log4j_config_if_automatic(
        &mut configs,
        merged_config.as_namenode().map(|nn| {
            nn.logging
                .for_container(&NameNodeContainer::FormatZooKeeper)
        }),
        FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
        &NameNodeContainer::FormatZooKeeper,
        FORMAT_ZOOKEEPER_LOG_FILE,
        MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE,
    );
    add_log4j_config_if_automatic(
        &mut configs,
        merged_config.as_datanode().map(|dn| {
            dn.logging
                .for_container(&DataNodeContainer::WaitForNameNodes)
        }),
        WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
        &DataNodeContainer::WaitForNameNodes,
        WAIT_FOR_NAMENODES_LOG_FILE,
        MAX_WAIT_NAMENODES_LOG_FILE_SIZE,
    );

    configs
}

fn add_log4j_config_if_automatic(
    configs: &mut Vec<(&'static str, String)>,
    log_config: Option<Cow<ContainerLogConfig>>,
    log_config_file: &'static str,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = log_config.as_deref()
    {
        configs.push((
            log_config_file,
            product_logging::framework::create_log4j_config(
                &format!("{STACKABLE_LOG_DIR}/{container_name}"),
                log_file,
                max_log_file_size
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                log_config,
            ),
        ));
    }
}

/// Renders the Vector agent config (`vector.yaml`).
///
/// Returns `None` when the Vector agent is disabled for this role group.
pub fn build_vector_config(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
    merged_config: &AnyNodeConfig,
) -> Option<String> {
    if !merged_config.vector_logging_enabled() {
        return None;
    }

    let vector_log_config = merged_config.vector_logging();
    let vector_log_config = if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = &*vector_log_config
    {
        Some(log_config)
    } else {
        None
    };

    // TODO: The framework's `create_vector_config` still requires a `RoleGroupRef`. We build one
    // over the `ValidatedCluster` (not the raw cluster) purely to satisfy this API; it only reads
    // the cluster name/namespace and the role/role-group strings, so the output is unchanged.
    // Hive ships a static `vector.yaml` instead and avoids `RoleGroupRef` entirely - we should
    // follow once a static config is available for HDFS, which would drop this last usage.
    let rolegroup = RoleGroupRef {
        cluster: ObjectRef::<ValidatedCluster>::from_obj(cluster),
        role: role.to_string(),
        role_group: role_group_name.to_string(),
    };

    Some(product_logging::framework::create_vector_config(
        &rolegroup,
        vector_log_config,
    ))
}
