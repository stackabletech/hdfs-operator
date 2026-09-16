//! Build the per-rolegroup `ConfigMap` for the HdfsCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        config_file_writer::PropertiesWriterError,
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            self, ResolvedRoleGroup, RoleGroupResolver,
            properties::{
                ConfigFileName, core_site, hadoop_policy, hdfs_site, product_logging,
                security_properties, ssl_client, ssl_server,
            },
        },
    },
    crd::v1alpha1,
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

/// Builds the [`ConfigMap`] of one role group.
///
/// Every role-specific value is resolved by the caller into `resolved`. The role comes from
/// `C::ROLE`, and `C`'s [`RoleGroupResolver`] bound ties it to `resolved`, so this cannot read one
/// role's `HdfsNodeRole` alongside another role's resolved values. The datanode storage
/// configuration comes from `resolved` rather than a separate parameter: taking it independently
/// would let a caller pass a datanode without its storage, which silently drops
/// `dfs.datanode.data.dir`.
pub fn build_rolegroup_config_map<C: RoleGroupResolver>(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rolegroup_config: &RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    resolved: &ResolvedRoleGroup<C>,
) -> Result<ConfigMap> {
    let role = C::ROLE;

    tracing::info!(
        "Setting up ConfigMap for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let metadata = build::rolegroup_metadata(cluster, &role, role_group_name);

    let config_overrides = &rolegroup_config.config_overrides;
    let cluster_config = &cluster.cluster_config;

    let hdfs_site_xml = hdfs_site::build(
        cluster,
        cluster_info,
        resolved.role.datanode_storage().cloned(),
        config_overrides.hdfs_site_xml.clone(),
    );
    let core_site_xml = core_site::build(
        cluster,
        role,
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

    for (log_config_file, log4j_config) in
        product_logging::build_log4j_configs(&resolved.logging, &resolved.role)
    {
        builder.add_data(log_config_file, log4j_config);
    }
    if resolved.logging.vector.is_some() {
        builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    builder.build().with_context(|_| AssembleSnafu {
        role: role.to_string(),
        role_group: role_group_name.to_string(),
    })
}
