//! Build the discovery `ConfigMap` for the HdfsCluster.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        kvp::label::recommended_labels,
        types::operator::{ControllerName, RoleGroupName},
    },
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            kerberos::KerberosConfig,
            properties::{
                ConfigFileName, core_site::CoreSiteConfigBuilder, hdfs_site::HdfsSiteConfigBuilder,
            },
        },
        operator_name, product_name,
    },
    crd::{HdfsNodeRole, HdfsPodRef},
    hdfs_controller::HDFS_CONTROLLER_NAME,
};

type Result<T, E = Error> = std::result::Result<T, E>;

stackable_operator::constant!(DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },
}

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
///
/// The rendered content as well as the ConfigMap ObjectMeta / owner reference come
/// entirely from `cluster` (with the externally-resolved `cluster_info` and
/// `namenode_podrefs`).
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    namenode_podrefs: &[HdfsPodRef],
) -> Result<ConfigMap> {
    // The discovery ConfigMap deliberately deviates from the standard resource identity: it is
    // labelled with the `hdfs-controller` controller name (NOT `controller_name()`, i.e.
    // `hdfs-operator-hdfs-controller` like the role-group resources), which keeps it outside their
    // `ClusterResources` orphan-matching, and with the namenode role plus a `discovery` role-group.
    let labels = recommended_labels(
        cluster,
        &product_name(),
        &cluster.product_version,
        &operator_name(),
        &ControllerName::from_str(HDFS_CONTROLLER_NAME)
            .expect("the hdfs controller name is a valid label value"),
        &ValidatedCluster::role_name(&HdfsNodeRole::Name),
        &DISCOVERY_ROLE_GROUP,
    );

    let metadata = cluster.object_meta(cluster.name.clone(), labels).build();

    ConfigMapBuilder::new()
        .metadata(metadata)
        .add_data(
            ConfigFileName::HdfsSite.to_string(),
            build_discovery_hdfs_site_xml(cluster, cluster_info, namenode_podrefs),
        )
        .add_data(
            ConfigFileName::CoreSite.to_string(),
            build_discovery_core_site_xml(cluster, cluster_info),
        )
        .build()
        .context(BuildConfigMapSnafu)
}

fn build_discovery_hdfs_site_xml(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    namenode_podrefs: &[HdfsPodRef],
) -> String {
    HdfsSiteConfigBuilder::new(cluster.name.as_ref().to_owned())
        .dfs_name_services()
        .dfs_ha_namenodes(namenode_podrefs)
        .dfs_namenode_rpc_address_ha(cluster_info, namenode_podrefs)
        .dfs_namenode_http_address_ha(
            cluster.cluster_config.authentication.is_some(),
            cluster_info,
            namenode_podrefs,
        )
        .dfs_client_failover_proxy_provider()
        .security_discovery_config(cluster.cluster_config.authentication.is_some())
        .build_as_xml()
}

fn build_discovery_core_site_xml(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> String {
    let cluster_config = &cluster.cluster_config;
    let kerberos = KerberosConfig {
        cluster_name: cluster.name.as_ref(),
        cluster_namespace: cluster.namespace.as_ref(),
        authentication_enabled: cluster_config.authentication.is_some(),
        kerberos_enabled: cluster_config.authentication.is_some(),
        authorization_enabled: cluster_config.authorization.is_some(),
    };
    CoreSiteConfigBuilder::new(cluster.name.as_ref().to_owned())
        .fs_default_fs()
        .security_discovery_config(&kerberos, cluster_info)
        .build_as_xml()
}
