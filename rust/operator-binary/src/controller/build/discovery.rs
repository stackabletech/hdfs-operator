//! Build the discovery `ConfigMap` for the HdfsCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    controller::build::properties::ConfigFileName,
    crd::{HdfsNodeRole, HdfsPodRef, v1alpha1},
    hdfs_controller::{HDFS_CONTROLLER_NAME, ValidatedCluster},
    security::kerberos::KerberosConfig,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {hdfs} is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        hdfs: ObjectRef<v1alpha1::HdfsCluster>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
///
/// The rendered content comes entirely from `cluster` (with the externally-resolved
/// `cluster_info` and `namenode_podrefs`); `owner_ref` is retained only for the ConfigMap
/// ObjectMeta / owner reference.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    namenode_podrefs: &[HdfsPodRef],
    owner_ref: &v1alpha1::HdfsCluster,
) -> Result<ConfigMap> {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(owner_ref)
        .ownerreference_from_resource(owner_ref, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu {
            hdfs: ObjectRef::from_obj(owner_ref),
        })?
        .with_recommended_labels(&build_recommended_labels(
            owner_ref,
            HDFS_CONTROLLER_NAME,
            &cluster.image.app_version_label_value,
            &HdfsNodeRole::Name.to_string(),
            "discovery",
        ))
        .context(ObjectMetaSnafu)?
        .build();

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
