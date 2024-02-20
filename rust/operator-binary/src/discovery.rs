use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{CORE_SITE_XML, HDFS_SITE_XML},
    HdfsCluster, HdfsPodRef, HdfsRole,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder, ObjectMetaBuilderError},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{runtime::reflector::ObjectRef, ResourceExt},
};

use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    security::kerberos,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {hdfs} is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hdfs: ObjectRef<HdfsCluster>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::error::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta { source: ObjectMetaBuilderError },

    #[snafu(display("failed to build security discovery config map"))]
    BuildSecurityDiscoveryConfigMap { source: kerberos::Error },
}

/// Creates a discovery config map containing the `hdfs-site.xml` and `core-site.xml`
/// for clients.
pub fn build_discovery_configmap(
    hdfs: &HdfsCluster,
    controller: &str,
    namenode_podrefs: &[HdfsPodRef],
    resolved_product_image: &ResolvedProductImage,
) -> Result<ConfigMap> {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hdfs)
        .ownerreference_from_resource(hdfs, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu {
            hdfs: ObjectRef::from_obj(hdfs),
        })?
        .with_recommended_labels(build_recommended_labels(
            hdfs,
            controller,
            &resolved_product_image.app_version_label,
            &HdfsRole::NameNode.to_string(),
            "discovery",
        ))
        .context(ObjectMetaSnafu)?
        .build();

    ConfigMapBuilder::new()
        .metadata(metadata)
        .add_data(
            HDFS_SITE_XML,
            build_discovery_hdfs_site_xml(hdfs, hdfs.name_any(), namenode_podrefs),
        )
        .add_data(
            CORE_SITE_XML,
            build_discovery_core_site_xml(hdfs, hdfs.name_any())?,
        )
        .build()
        .context(BuildConfigMapSnafu)
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

fn build_discovery_core_site_xml(hdfs: &HdfsCluster, logical_name: String) -> Result<String> {
    Ok(CoreSiteConfigBuilder::new(logical_name)
        .fs_default_fs()
        .security_discovery_config(hdfs)
        .context(BuildSecurityDiscoveryConfigMapSnafu)?
        .build_as_xml())
}
