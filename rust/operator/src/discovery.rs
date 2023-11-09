use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
};
use stackable_hdfs_crd::{
    constants::{CORE_SITE_XML, HDFS_SITE_XML},
    HdfsCluster, HdfsPodRef, HdfsRole,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    error::OperatorResult,
    k8s_openapi::api::core::v1::ConfigMap,
};

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
pub fn build_discovery_configmap(
    hdfs: &HdfsCluster,
    hdfs_name: &str,
    hdfs_namespace: &str,
    controller: &str,
    namenode_podrefs: &[HdfsPodRef],
    resolved_product_image: &ResolvedProductImage,
) -> OperatorResult<ConfigMap> {
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .ownerreference_from_resource(hdfs, None, Some(true))?
                .with_recommended_labels(build_recommended_labels(
                    hdfs,
                    controller,
                    &resolved_product_image.app_version_label,
                    &HdfsRole::NameNode.to_string(),
                    "discovery",
                ))
                .build(),
        )
        .add_data(
            HDFS_SITE_XML,
            build_discovery_hdfs_site_xml(hdfs, hdfs_name, namenode_podrefs),
        )
        .add_data(
            CORE_SITE_XML,
            build_discovery_core_site_xml(hdfs, hdfs_name, hdfs_namespace),
        )
        .build()
}

fn build_discovery_hdfs_site_xml(
    hdfs: &HdfsCluster,
    hdfs_name: &str,
    namenode_podrefs: &[HdfsPodRef],
) -> String {
    HdfsSiteConfigBuilder::new(hdfs_name.to_string())
        .dfs_name_services()
        .dfs_ha_namenodes(namenode_podrefs)
        .dfs_namenode_rpc_address_ha(namenode_podrefs)
        .dfs_namenode_http_address_ha(hdfs, namenode_podrefs)
        .dfs_client_failover_proxy_provider()
        .security_discovery_config(hdfs)
        .build_as_xml()
}

fn build_discovery_core_site_xml(
    hdfs: &HdfsCluster,
    hdfs_name: &str,
    hdfs_namespace: &str,
) -> String {
    CoreSiteConfigBuilder::new(hdfs_name.to_string())
        .fs_default_fs()
        .security_discovery_config(hdfs, hdfs_name, hdfs_namespace)
        .build_as_xml()
}
