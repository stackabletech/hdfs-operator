//! Builders for the logging-related files in the rolegroup `ConfigMap`: the per-container
//! `*.log4j.properties` configs and the (static) Vector agent config (`vector.yaml`).

use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    v2::product_logging::framework::STACKABLE_LOG_DIR,
};

use crate::controller::build::container::{
    ContainerConfig, FORMAT_NAMENODES_CONTAINER_NAME, FORMAT_ZOOKEEPER_CONTAINER_NAME,
    WAIT_FOR_NAMENODES_CONTAINER_NAME, ZKFC_CONTAINER_NAME,
};

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

/// The main `hdfs` container of every role logs into this directory, whatever the container
/// itself is named (`namenode`, `datanode`, `journalnode`).
///
/// Vector parses the `container` label out of the log path (see the `files_log4j` source in
/// `vector.yaml`), so this name reaches the aggregated logs.
const HDFS_LOG_DIR_NAME: &str = "hdfs";

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

/// Everything about how one container logs: which file holds its `log4j.properties` in the role
/// group `ConfigMap`, which directory it logs into, which file it writes and how large that file
/// may grow.
///
/// One match, so a container's four log4j facts sit together and cannot drift apart.
struct Log4jSpec {
    config_file: &'static str,
    log_dir_name: &'static str,
    log_file: &'static str,
    max_log_file_size: MemoryQuantity,
}

fn log4j_spec(container: &ContainerConfig) -> Log4jSpec {
    match container {
        ContainerConfig::Hdfs { .. } => Log4jSpec {
            config_file: HDFS_LOG4J_CONFIG_FILE,
            log_dir_name: HDFS_LOG_DIR_NAME,
            log_file: HDFS_LOG_FILE,
            max_log_file_size: MAX_HDFS_LOG_FILE_SIZE,
        },
        ContainerConfig::Zkfc => Log4jSpec {
            config_file: ZKFC_LOG4J_CONFIG_FILE,
            log_dir_name: ZKFC_CONTAINER_NAME.as_ref(),
            log_file: ZKFC_LOG_FILE,
            max_log_file_size: MAX_ZKFC_LOG_FILE_SIZE,
        },
        ContainerConfig::FormatNameNodes => Log4jSpec {
            config_file: FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
            log_dir_name: FORMAT_NAMENODES_CONTAINER_NAME.as_ref(),
            log_file: FORMAT_NAMENODES_LOG_FILE,
            max_log_file_size: MAX_FORMAT_NAMENODE_LOG_FILE_SIZE,
        },
        ContainerConfig::FormatZooKeeper => Log4jSpec {
            config_file: FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
            log_dir_name: FORMAT_ZOOKEEPER_CONTAINER_NAME.as_ref(),
            log_file: FORMAT_ZOOKEEPER_LOG_FILE,
            max_log_file_size: MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE,
        },
        ContainerConfig::WaitForNameNodes => Log4jSpec {
            config_file: WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
            log_dir_name: WAIT_FOR_NAMENODES_CONTAINER_NAME.as_ref(),
            log_file: WAIT_FOR_NAMENODES_LOG_FILE,
            max_log_file_size: MAX_WAIT_NAMENODES_LOG_FILE_SIZE,
        },
    }
}

/// The `ConfigMap` key holding the given container's `log4j.properties`.
///
/// The container copies the file from there into its config directory on startup, so the key the
/// `ConfigMap` is written with and the name the container copies must agree.
pub(crate) fn log4j_config_file(container: &ContainerConfig) -> &'static str {
    log4j_spec(container).config_file
}

/// Renders the given container's `log4j.properties` into the role group `ConfigMap`, if that
/// container uses the operator's automatic logging configuration.
///
/// A container using a custom log `ConfigMap` mounts its own and is skipped here.
pub(crate) fn add_log4j_config(
    builder: &mut ConfigMapBuilder,
    container: &ContainerConfig,
    container_log_config: &ContainerLogConfig,
) {
    let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = container_log_config
    else {
        return;
    };

    let spec = log4j_spec(container);

    builder.add_data(
        spec.config_file,
        product_logging::framework::create_log4j_config(
            &format!(
                "{STACKABLE_LOG_DIR}/{log_dir_name}",
                log_dir_name = spec.log_dir_name
            ),
            spec.log_file,
            spec.max_log_file_size
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN,
            log_config,
        ),
    );
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
