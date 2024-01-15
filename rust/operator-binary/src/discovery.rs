use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    hdfs_controller::Error,
};
use stackable_hdfs_crd::{
    constants::{CORE_SITE_XML, HDFS_SITE_XML},
    HdfsCluster, HdfsPodRef, HdfsRole,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{runtime::reflector::ObjectRef, ResourceExt},
};

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
pub fn build_discovery_configmap(
    hdfs: &HdfsCluster,
    controller: &str,
    namenode_podrefs: &[HdfsPodRef],
    resolved_product_image: &ResolvedProductImage,
) -> Result<ConfigMap, crate::hdfs_controller::Error> {
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .ownerreference_from_resource(hdfs, None, Some(true))
                .map_err(|err| Error::ObjectMissingMetadataForOwnerRef {
                    source: err,
                    obj_ref: ObjectRef::from_obj(hdfs),
                })?
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
            build_discovery_hdfs_site_xml(hdfs, hdfs.name_any(), namenode_podrefs),
        )
        .add_data(
            CORE_SITE_XML,
            build_discovery_core_site_xml(hdfs, hdfs.name_any())?,
        )
        .build()
        .map_err(|err| Error::BuildDiscoveryConfigMap { source: err })
}

fn build_discovery_hdfs_site_xml(
    hdfs: &HdfsCluster,
    logical_name: String,
    namenode_podrefs: &[HdfsPodRef],
) -> String {
    HdfsSiteConfigBuilder::new(logical_name)
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
    logical_name: String,
) -> Result<String, Error> {
    Ok(CoreSiteConfigBuilder::new(logical_name)
        .fs_default_fs()
        .security_discovery_config(hdfs)?
        .build_as_xml())
}
