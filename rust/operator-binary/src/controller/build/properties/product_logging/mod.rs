//! Builders for the logging-related files in the rolegroup `ConfigMap`: the per-container
//! `*.log4j.properties` configs and the (static) Vector agent config (`vector.yaml`).

use std::{borrow::Cow, fmt::Display};

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    v2::product_logging::framework::STACKABLE_LOG_DIR,
};

use crate::crd::{AnyNodeConfig, DataNodeContainer, NameNodeContainer};

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

/// The vendored Vector agent configuration (`vector.yaml`).
///
/// It is static: per-rolegroup values (namespace, cluster, role, role group, log/data dirs and the
/// aggregator address) are interpolated at runtime by Vector from injected environment variables.
/// The accompanying `vector-test.yaml` exercises the VRL in this file; run it with
/// `./test-vector.sh` (requires the `vector` binary).
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the content of the static Vector agent config (`vector.yaml`).
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // HDFS containers log via log4j to `*.log4j.xml`, so the `files_log4j` source matches them.
        assert!(content.contains("files_log4j"));
    }
}
