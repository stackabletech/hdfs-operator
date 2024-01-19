use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::{DataNodeContainer, HdfsCluster, MergedConfig, NameNodeContainer};
use stackable_operator::{
    builder::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to retrieve the ConfigMap [{cm_name}]"))]
    ConfigMapNotFound {
        source: stackable_operator::error::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry [{entry}] for ConfigMap [{cm_name}]"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

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

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n";

const HDFS_LOG_FILE: &str = "hdfs.log4j.xml";
const ZKFC_LOG_FILE: &str = "zkfc.log4j.xml";
const FORMAT_NAMENODES_LOG_FILE: &str = "format-namenodes.log4j.xml";
const FORMAT_ZOOKEEPER_LOG_FILE: &str = "format-zookeeper.log4j.xml";
const WAIT_FOR_NAMENODES_LOG_FILE: &str = "wait-for-namenodes.log4j.xml";

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    hdfs: &HdfsCluster,
    client: &Client,
) -> Result<Option<String>> {
    let vector_aggregator_address = if let Some(vector_aggregator_config_map_name) =
        &hdfs.spec.cluster_config.vector_aggregator_config_map_name
    {
        let vector_aggregator_address = client
            .get::<ConfigMap>(
                vector_aggregator_config_map_name,
                hdfs.namespace()
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
            )
            .await
            .context(ConfigMapNotFoundSnafu {
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?
            .data
            .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
            .context(MissingConfigMapEntrySnafu {
                entry: VECTOR_AGGREGATOR_CM_ENTRY,
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?;
        Some(vector_aggregator_address)
    } else {
        None
    };

    Ok(vector_aggregator_address)
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<HdfsCluster>,
    vector_aggregator_address: Option<&str>,
    merged_config: &(dyn MergedConfig + Send + 'static),
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = merged_config.hdfs_logging()
    {
        cm_builder.add_data(
            HDFS_LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!("{STACKABLE_LOG_DIR}/hdfs"),
                HDFS_LOG_FILE,
                MAX_HDFS_LOG_FILE_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                &log_config,
            ),
        );
    }

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = merged_config.zkfc_logging()
    {
        cm_builder.add_data(
            ZKFC_LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!(
                    "{STACKABLE_LOG_DIR}/{container_name}",
                    container_name = NameNodeContainer::Zkfc
                ),
                ZKFC_LOG_FILE,
                MAX_ZKFC_LOG_FILE_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                &log_config,
            ),
        );
    }

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = merged_config.format_namenodes_logging()
    {
        cm_builder.add_data(
            FORMAT_NAMENODES_LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!(
                    "{STACKABLE_LOG_DIR}/{container_name}",
                    container_name = NameNodeContainer::FormatNameNodes
                ),
                FORMAT_NAMENODES_LOG_FILE,
                MAX_FORMAT_NAMENODE_LOG_FILE_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                &log_config,
            ),
        );
    }

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = merged_config.format_zookeeper_logging()
    {
        cm_builder.add_data(
            FORMAT_ZOOKEEPER_LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!(
                    "{STACKABLE_LOG_DIR}/{container_name}",
                    container_name = NameNodeContainer::FormatZooKeeper
                ),
                FORMAT_ZOOKEEPER_LOG_FILE,
                MAX_FORMAT_ZOOKEEPER_LOG_FILE_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                &log_config,
            ),
        );
    }

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = merged_config.wait_for_namenodes()
    {
        cm_builder.add_data(
            WAIT_FOR_NAMENODES_LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!(
                    "{STACKABLE_LOG_DIR}/{container_name}",
                    container_name = DataNodeContainer::WaitForNameNodes
                ),
                WAIT_FOR_NAMENODES_LOG_FILE,
                MAX_WAIT_NAMENODES_LOG_FILE_SIZE
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN,
                &log_config,
            ),
        );
    }

    let vector_log_config = if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = merged_config.vector_logging()
    {
        Some(log_config)
    } else {
        None
    };

    if merged_config.vector_logging_enabled() {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config.as_ref(),
            ),
        );
    }

    Ok(())
}
