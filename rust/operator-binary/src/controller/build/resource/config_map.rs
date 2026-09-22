//! Build the per-rolegroup `ConfigMap` for the HdfsCluster.
//!
//! [`common_config_map`] writes the files every role group gets, the role builder adds the log4j
//! config of each container its role runs, and [`finish_config_map`] assembles the result.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE, v2::config_file_writer::PropertiesWriterError,
};

use crate::{
    controller::build::{
        self,
        container::ContainerConfig,
        properties::{
            ConfigFileName, core_site, hadoop_policy, hdfs_site, product_logging,
            security_properties, ssl_client, ssl_server,
        },
        role_group::RoleGroupCommon,
    },
    crd::storage::DataNodeStorageConfigInnerType,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to serialize {} for {rolegroup}", ConfigFileName::Security))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("cannot build config map for role {role:?} and role group {role_group:?}"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
        role: String,
        role_group: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The files every role group's `ConfigMap` gets: the Hadoop XML configs, the JVM security
/// properties, the main `hdfs` container's `log4j.properties` and, when the Vector agent is
/// enabled, the static Vector config.
///
/// `datanode_storage` drives `dfs.datanode.data.dir` and is `Some` only for datanodes; the other
/// two roles do not configure it.
pub(crate) fn common_config_map(
    common: &RoleGroupCommon,
    datanode_storage: Option<DataNodeStorageConfigInnerType>,
) -> Result<ConfigMapBuilder> {
    let cluster = common.cluster;
    let cluster_info = common.cluster_info;
    let role = &common.role;
    let role_group_name = &common.role_group_name;

    tracing::info!(
        "Setting up ConfigMap for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let metadata = build::rolegroup_metadata(cluster, role, role_group_name);

    let config_overrides = &common.config_overrides;
    let cluster_config = &cluster.cluster_config;

    let hdfs_site_xml = hdfs_site::build(
        cluster,
        cluster_info,
        datanode_storage,
        config_overrides.hdfs_site_xml.clone(),
    );
    let core_site_xml = core_site::build(
        cluster,
        *role,
        cluster_info,
        config_overrides.core_site_xml.clone(),
    );
    let hadoop_policy_xml = hadoop_policy::build(config_overrides.hadoop_policy_xml.clone());
    let ssl_server_xml = ssl_server::build(
        cluster_config.authentication.is_some(),
        config_overrides.ssl_server_xml.clone(),
    );
    let ssl_client_xml = ssl_client::build(
        cluster_config.authentication.is_some(),
        config_overrides.ssl_client_xml.clone(),
    );

    let mut builder = ConfigMapBuilder::new();
    builder
        .metadata(metadata.build())
        .add_data(ConfigFileName::CoreSite.to_string(), core_site_xml)
        .add_data(ConfigFileName::HdfsSite.to_string(), hdfs_site_xml)
        .add_data(ConfigFileName::HadoopPolicy.to_string(), hadoop_policy_xml)
        .add_data(ConfigFileName::SslServer.to_string(), ssl_server_xml)
        .add_data(ConfigFileName::SslClient.to_string(), ssl_client_xml)
        .add_data(
            ConfigFileName::Security.to_string(),
            security_properties::build(config_overrides.security_properties.clone()).with_context(
                |_| JvmSecurityPropertiesSnafu {
                    rolegroup: role_group_name.to_string(),
                },
            )?,
        );

    product_logging::add_log4j_config(
        &mut builder,
        &ContainerConfig::from(*role),
        &common.hdfs_logging,
    );

    if common.vector_logging.is_some() {
        builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    Ok(builder)
}

/// Assembles the role group's `ConfigMap`, once the role builder has added the `log4j.properties`
/// of each container its role runs.
pub(crate) fn finish_config_map(
    builder: ConfigMapBuilder,
    common: &RoleGroupCommon,
) -> Result<ConfigMap> {
    builder.build().with_context(|_| AssembleSnafu {
        role: common.role.to_string(),
        role_group: common.role_group_name.to_string(),
    })
}
