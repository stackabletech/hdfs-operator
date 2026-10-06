//! Build the per-rolegroup `ConfigMap` for the HdfsCluster.
//!
//! [`common_config_map_data`] renders the files every role group gets, during the gather phase.
//! [`build_config_map`] then wraps whatever the role group ended up with, its own containers'
//! `log4j.properties` included, in the `ConfigMap` object.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE, v2::config_file_writer::PropertiesWriterError,
};

use crate::controller::build::{
    self,
    container::ContainerConfig,
    properties::{
        ConfigFileName, core_site, hadoop_policy, hdfs_site, product_logging, security_properties,
        ssl_client, ssl_server,
    },
    role_group::{RoleGroupBuilder, RoleGroupInputs},
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

/// The `ConfigMap` entries every role group gets: the Hadoop XML configs, the JVM security
/// properties, the main `hdfs` container's `log4j.properties` and, when the Vector agent is
/// enabled, the static Vector config.
pub(crate) fn common_config_map_data(inputs: &RoleGroupInputs) -> Result<Vec<(String, String)>> {
    let cluster = inputs.cluster;
    let cluster_info = inputs.cluster_info;
    let role = &inputs.role;
    let role_group_name = &inputs.role_group_name;
    let config_overrides = &inputs.config_overrides;
    let cluster_config = &cluster.cluster_config;

    let mut data = vec![
        (
            ConfigFileName::CoreSite.to_string(),
            core_site::build(
                cluster,
                *role,
                cluster_info,
                config_overrides.core_site_xml.clone(),
            ),
        ),
        (
            ConfigFileName::HdfsSite.to_string(),
            hdfs_site::build(
                cluster,
                cluster_info,
                inputs.datanode_storage.clone(),
                config_overrides.hdfs_site_xml.clone(),
            ),
        ),
        (
            ConfigFileName::HadoopPolicy.to_string(),
            hadoop_policy::build(config_overrides.hadoop_policy_xml.clone()),
        ),
        (
            ConfigFileName::SslServer.to_string(),
            ssl_server::build(
                cluster_config.authentication.is_some(),
                config_overrides.ssl_server_xml.clone(),
            ),
        ),
        (
            ConfigFileName::SslClient.to_string(),
            ssl_client::build(
                cluster_config.authentication.is_some(),
                config_overrides.ssl_client_xml.clone(),
            ),
        ),
        (
            ConfigFileName::Security.to_string(),
            security_properties::build(config_overrides.security_properties.clone()).with_context(
                |_| JvmSecurityPropertiesSnafu {
                    rolegroup: role_group_name.to_string(),
                },
            )?,
        ),
    ];

    data.extend(product_logging::log4j_config(
        &ContainerConfig::from(*role),
        &inputs.hdfs_logging,
    ));

    if inputs.vector_logging.is_some() {
        data.push((
            VECTOR_CONFIG_FILE.to_owned(),
            product_logging::vector_config_file_content(),
        ));
    }

    Ok(data)
}

/// The role group's `ConfigMap`, from the entries gathered for it.
pub(crate) fn build_config_map(builder: &RoleGroupBuilder) -> Result<ConfigMap> {
    let role = &builder.role;
    let role_group_name = &builder.role_group_name;

    tracing::info!(
        "Setting up ConfigMap for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let mut config_map = ConfigMapBuilder::new();
    config_map.metadata(build::rolegroup_metadata(builder.cluster, role, role_group_name).build());
    for (file_name, content) in &builder.config_map_data {
        config_map.add_data(file_name, content);
    }

    config_map.build().with_context(|_| AssembleSnafu {
        role: role.to_string(),
        role_group: role_group_name.to_string(),
    })
}
