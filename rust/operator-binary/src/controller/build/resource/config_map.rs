//! Build the per-rolegroup `ConfigMap` for the HdfsCluster.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{config_file_writer::PropertiesWriterError, types::operator::RoleGroupName},
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            self,
            properties::{
                ConfigFileName, core_site, hadoop_policy, hdfs_site, product_logging,
                security_properties, ssl_client, ssl_server,
            },
        },
    },
    crd::{DataNodeContainer, HdfsNodeRole, NameNodeContainer},
};

#[derive(Snafu, Debug)]
pub enum Error {
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
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<ConfigMap> {
    tracing::info!(
        "Setting up ConfigMap for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let metadata = build::rolegroup_metadata(cluster, role, role_group_name);

    let rolegroup_config = cluster
        .role_groups
        .get(role)
        .and_then(|role_groups| role_groups.get(role_group_name))
        .with_context(|| MissingRoleGroupSnafu {
            role: role.to_string(),
            role_group: role_group_name.to_string(),
        })?;
    let merged_config = &rolegroup_config.config;
    let config_overrides = &rolegroup_config.config_overrides;
    let cluster_config = &cluster.cluster_config;

    let hdfs_site_xml = hdfs_site::build(
        cluster,
        cluster_info,
        merged_config
            .as_datanode()
            .map(|node| node.resources.storage.clone()),
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

    let hdfs_logging = merged_config.hdfs_logging();
    let (zkfc_logging, format_namenodes_logging, format_zookeeper_logging) =
        match merged_config.as_namenode() {
            Some(namenode) => (
                Some(namenode.logging.for_container(&NameNodeContainer::Zkfc)),
                Some(
                    namenode
                        .logging
                        .for_container(&NameNodeContainer::FormatNameNodes),
                ),
                Some(
                    namenode
                        .logging
                        .for_container(&NameNodeContainer::FormatZooKeeper),
                ),
            ),
            None => (None, None, None),
        };
    let wait_for_namenodes_logging = merged_config.as_datanode().map(|dn| {
        dn.logging
            .for_container(&DataNodeContainer::WaitForNameNodes)
    });
    let log4j_configs = product_logging::build_log4j_configs(
        Some(&*hdfs_logging),
        zkfc_logging.as_deref(),
        format_namenodes_logging.as_deref(),
        format_zookeeper_logging.as_deref(),
        wait_for_namenodes_logging.as_deref(),
    );
    for (log_config_file, log4j_config) in log4j_configs {
        builder.add_data(log_config_file, log4j_config);
    }
    if merged_config.vector_logging_enabled() {
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
