//! Build the per-rolegroup `ConfigMap` for the HdfsCluster.

use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    role_utils::RoleGroupRef,
    utils::cluster_info::KubernetesClusterInfo,
    v2::config_file_writer::PropertiesWriterError,
};

use crate::{
    controller::build::properties::{
        ConfigFileName, core_site, hadoop_policy, hdfs_site, logging, security_properties,
        ssl_client, ssl_server,
    },
    crd::{HdfsNodeRole, v1alpha1},
    hdfs_controller::ValidatedCluster,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("could not parse HDFS role [{role}]"))]
    UnidentifiedHdfsRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("the validated cluster has no role group {role_group:?} for role {role:?}"))]
    MissingRoleGroup { role: String, role_group: String },

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

pub fn build_rolegroup_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    metadata: &ObjectMetaBuilder,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
) -> Result<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);

    let role = HdfsNodeRole::from_str(&rolegroup_ref.role).with_context(|_| {
        UnidentifiedHdfsRoleSnafu {
            role: rolegroup_ref.role.clone(),
        }
    })?;
    let rolegroup_config = cluster
        .role_groups
        .get(&role)
        .and_then(|role_groups| role_groups.get(&rolegroup_ref.role_group))
        .with_context(|| MissingRoleGroupSnafu {
            role: rolegroup_ref.role.clone(),
            role_group: rolegroup_ref.role_group.clone(),
        })?;
    let merged_config = &rolegroup_config.merged_config;
    let config_overrides = &rolegroup_config.config_overrides;
    let cluster_config = &cluster.cluster_config;

    let hdfs_site_xml = hdfs_site::build(
        cluster,
        cluster_info,
        merged_config,
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
                    rolegroup: rolegroup_ref.role_group.clone(),
                },
            )?,
        );

    for (log_config_file, log4j_config) in logging::build_log4j_configs(merged_config) {
        builder.add_data(log_config_file, log4j_config);
    }
    if let Some(vector_config) = logging::build_vector_config(rolegroup_ref, merged_config) {
        builder.add_data(VECTOR_CONFIG_FILE, vector_config);
    }

    builder.build().with_context(|_| AssembleSnafu {
        role: rolegroup_ref.role.clone(),
        role_group: rolegroup_ref.role_group.clone(),
    })
}
