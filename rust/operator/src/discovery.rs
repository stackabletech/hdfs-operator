use crate::{
    config::{CoreSiteConfigBuilder, HdfsNodeDataDirectory, HdfsSiteConfigBuilder},
    OPERATOR_NAME,
};
use stackable_hdfs_crd::{
    constants::{APP_NAME, CORE_SITE_XML, HDFS_SITE_XML},
    HdfsCluster, HdfsPodRef, HdfsRole,
};
use stackable_operator::error::{Error, OperatorResult};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
};

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
pub fn build_discovery_configmap(
    hdfs: &HdfsCluster,
    namenode_podrefs: &[HdfsPodRef],
) -> OperatorResult<ConfigMap> {
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .ownerreference_from_resource(hdfs, None, Some(true))?
                .with_recommended_labels(
                    hdfs,
                    APP_NAME,
                    hdfs.hdfs_version()
                        .map_err(|_| Error::MissingObjectKey { key: "version" })?,
                    OPERATOR_NAME,
                    &HdfsRole::NameNode.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data(
            HDFS_SITE_XML,
            build_discovery_hdfs_site_xml(hdfs.name_any(), namenode_podrefs),
        )
        .add_data(
            CORE_SITE_XML,
            build_discovery_core_site_xml(hdfs.name_any()),
        )
        .build()
}

fn build_discovery_hdfs_site_xml(logical_name: String, namenode_podrefs: &[HdfsPodRef]) -> String {
    HdfsSiteConfigBuilder::new(logical_name, HdfsNodeDataDirectory::default())
        .dfs_name_services()
        .dfs_ha_namenodes(namenode_podrefs)
        .dfs_namenode_rpc_address_ha(namenode_podrefs)
        .dfs_namenode_http_address_ha(namenode_podrefs)
        .dfs_client_failover_proxy_provider()
        .build_as_xml()
}

fn build_discovery_core_site_xml(logical_name: String) -> String {
    CoreSiteConfigBuilder::new(logical_name)
        .fs_default_fs()
        .build_as_xml()
}
