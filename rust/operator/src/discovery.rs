use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::constants::APP_NAME;
use stackable_operator::builder::{ConfigMapBuilder, ObjectMetaBuilder};
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::{Resource, ResourceExt};
use stackable_operator::product_config::writer;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", zk))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        zk: ObjectRef<ZookeeperCluster>,
    },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`ZookeeperCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    client: &stackable_operator::client::Client,
    owner: &impl Resource<DynamicType = ()>,
    zk: &ZookeeperCluster,
    svc: &Service,
    chroot: Option<&str>,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name();
    Ok(vec![
        build_discovery_configmap(&name, owner, zk, chroot, pod_hosts(zk)?)?,
        build_discovery_configmap(
            &format!("{}-nodeport", name),
            owner,
            zk,
            chroot,
            nodeport_hosts(client, svc, "zk").await?,
        )?,
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`ZookeeperCluster`]
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    hdfs: HdfsCluster,
    chroot: Option<&str>,
    hosts: impl IntoIterator<Item = (impl Into<String>, u16)>,
) -> Result<ConfigMap, Error> {
    // Write a connection string of the format that Java ZooKeeper client expects:
    // "{host1}:{port1},{host2:port2},.../{chroot}"
    // See https://zookeeper.apache.org/doc/current/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#ZooKeeper-java.lang.String-int-org.apache.zookeeper.Watcher-
    let hosts = hosts
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host.into(), port))
        .collect::<Vec<_>>()
        .join(",");
    let mut conn_str = hosts.clone();
    if let Some(chroot) = chroot {
        if !chroot.starts_with('/') {
            return RelativeChrootSnafu { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .name(name)
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    zk: ObjectRef::from_obj(hdfs),
                })?
                .with_recommended_labels(
                    hdfs,
                    APP_NAME,
                    zk_version(hdfs).unwrap_or("unknown"),
                    &ZookeeperRole::Server.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data("hdfs-site.xml", conn_str)
        .add_data("core-site.xml", hosts)
        .add_data("HDFS_NAMENODE", chroot.unwrap_or("/"))
        .build()
        .context(BuildConfigMapSnafu)
}
