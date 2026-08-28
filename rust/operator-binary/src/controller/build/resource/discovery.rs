//! Build the discovery `ConfigMap` for the HdfsCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::ClusterName,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            kerberos::KerberosConfig,
            object_meta, pod_refs,
            properties::{
                ConfigFileName, core_site::CoreSiteConfigBuilder, hdfs_site::HdfsSiteConfigBuilder,
            },
            recommended_labels_for_role_resources,
        },
    },
    crd::{HdfsNodeRole, HdfsPodRef, namenode_listener_refs},
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to collect the namenode listener refs"))]
    CollectListenerRefs { source: crate::crd::Error },
}

/// Builds the discovery [`ConfigMap`] containing the `hdfs-site.xml` and `core-site.xml` for
/// clients, rendered from the namenode `Listener`s' ingress addresses.
///
/// The namenode `Listener`s are per-pod and only the listener-operator writes their
/// addresses, so there are recurring windows where not every address is available: around
/// the first reconcile runs, and whenever a namenode is scaled up. Failing the build in such
/// a window would prevent the apply step from running at all, so instead:
///
/// - a discovery ConfigMap already stored in the cluster is re-emitted unchanged via
///   [`reemit_discovery_config_map`], keeping it tracked by the apply step;
/// - otherwise (it has never been built yet) `None` is returned and the ConfigMap is
///   skipped for this run.
///
/// Either way the Listener watch triggers a new run once the listener-operator has caught
/// up, and a fresh ConfigMap is built then.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<Option<ConfigMap>> {
    let namenode_podrefs = pod_refs(cluster, &HdfsNodeRole::Name);
    let Some(listener_refs) = namenode_listener_refs(namenode_podrefs, &cluster.namenode_listeners)
        .context(CollectListenerRefsSnafu)?
    else {
        return Ok(match &cluster.discovery_config_map {
            Some(existing) => {
                tracing::debug!(
                    "not all namenode Listeners have an ingress address yet, re-emitting the \
                     stored discovery ConfigMap unchanged"
                );
                Some(reemit_discovery_config_map(cluster, existing))
            }
            None => {
                tracing::debug!(
                    "not all namenode Listeners have an ingress address yet and no discovery \
                     ConfigMap exists, skipping it"
                );
                None
            }
        });
    };

    let config_map = ConfigMapBuilder::new()
        .metadata(discovery_config_map_meta(cluster).build())
        .add_data(
            ConfigFileName::HdfsSite.to_string(),
            build_discovery_hdfs_site_xml(cluster, cluster_info, &listener_refs),
        )
        .add_data(
            ConfigFileName::CoreSite.to_string(),
            build_discovery_core_site_xml(cluster, cluster_info),
        )
        .build()
        .expect("The ConfigMap metadata is set in this function.");

    Ok(Some(config_map))
}

/// Re-emits the stored discovery [`ConfigMap`] so that the apply step keeps tracking it in
/// `ClusterResources` while no fresh one can be built (a tracked resource that is not
/// re-added in a run would be deleted as an orphan).
///
/// The fetched `data` is carried over unchanged, so applying it is a no-op on the server.
/// The metadata is built fresh instead of echoing the fetched metadata: a fetched object
/// carries server-populated fields (`resourceVersion`, `uid`, `managedFields`) that must not
/// appear in an apply patch, and the labels required by `ClusterResources::add` are added
/// here.
fn reemit_discovery_config_map(cluster: &ValidatedCluster, existing: &ConfigMap) -> ConfigMap {
    ConfigMap {
        metadata: discovery_config_map_meta(cluster).build(),
        data: existing.data.clone(),
        ..ConfigMap::default()
    }
}

/// The discovery `ConfigMap`'s name: the cluster name itself.
///
/// This is the single place the name is derived. It is a public contract (downstream
/// clients mount the discovery ConfigMap by the cluster name) and the dereference step
/// fetches the stored ConfigMap under this name for the re-emit path, so it must never
/// change: a build-side rename would leave the dereference step fetching `None` forever,
/// silently disabling the re-emit protection.
pub(crate) fn discovery_config_map_name(cluster_name: &ClusterName) -> String {
    cluster_name.to_string()
}

/// Shared metadata for both the freshly built and the re-emitted discovery ConfigMap, so
/// that the two are identical apart from their contents. The ConfigMap carries the standard
/// recommended labels (required by `ClusterResources::add`), attributed to the namenode role.
fn discovery_config_map_meta(cluster: &ValidatedCluster) -> ObjectMetaBuilder {
    object_meta(
        cluster,
        discovery_config_map_name(&cluster.name),
        recommended_labels_for_role_resources(cluster, &HdfsNodeRole::Name),
    )
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::kube::api::ObjectMeta;

    use super::*;
    use crate::{
        controller::build::properties::test_support::{cluster_info, validated_cluster},
        test_support::namenode_listener,
    };

    /// With every namenode Listener carrying an ingress address, a fresh discovery ConfigMap
    /// is built: named after the cluster, rendering the listener addresses, and carrying the
    /// standard recommended labels that `ClusterResources::add` requires.
    #[test]
    fn builds_a_fresh_config_map_when_all_listeners_have_addresses() {
        let mut cluster = validated_cluster();
        // The minimal fixture has one namenode replica, so one pod Listener suffices.
        cluster.namenode_listeners = vec![namenode_listener(
            "listener-hdfs-namenode-default-0",
            "namenode-0.example.org",
            31000,
        )];

        let config_map = build_discovery_config_map(&cluster, &cluster_info())
            .expect("the build must not fail")
            .expect("a fresh ConfigMap must be built");

        assert_eq!(config_map.metadata.name.as_deref(), Some("hdfs"));
        let hdfs_site = config_map
            .data
            .as_ref()
            .and_then(|data| data.get("hdfs-site.xml"))
            .expect("hdfs-site.xml is rendered");
        assert!(
            hdfs_site.contains("namenode-0.example.org"),
            "the listener address must be rendered into hdfs-site.xml, got: {hdfs_site}"
        );
        let labels = config_map.metadata.labels.expect("labels are set");
        assert_eq!(
            labels.get("app.kubernetes.io/instance").map(String::as_str),
            Some("hdfs"),
            "the labels required by ClusterResources::add must be set"
        );
    }

    /// While no fresh ConfigMap can be built (a namenode Listener is missing or has no ingress
    /// address, e.g. during a namenode scale-up), an already stored discovery ConfigMap is
    /// re-emitted unchanged instead of being dropped: a tracked resource missing from a run is
    /// deleted as an orphan by the apply step, which would break clients using the ConfigMap.
    #[test]
    fn reemits_the_stored_config_map_when_a_fresh_one_cannot_be_built() {
        let mut cluster = validated_cluster();
        // No Listeners stored: the listener-operator has not created them yet.
        cluster.namenode_listeners = vec![];
        let stored_data = BTreeMap::from([(
            "hdfs-site.xml".to_string(),
            "<configuration>stored</configuration>".to_string(),
        )]);
        cluster.discovery_config_map = Some(ConfigMap {
            metadata: ObjectMeta {
                name: Some("hdfs".to_string()),
                resource_version: Some("42".to_string()),
                uid: Some("6a8f6428-6f45-4bd6-9a9c-3d040ec93cca".to_string()),
                ..ObjectMeta::default()
            },
            data: Some(stored_data.clone()),
            ..ConfigMap::default()
        });

        let config_map = build_discovery_config_map(&cluster, &cluster_info())
            .expect("the build must not fail")
            .expect("the stored ConfigMap must be re-emitted");

        assert_eq!(
            config_map.data,
            Some(stored_data),
            "the stored values must be carried over unchanged"
        );
        assert_eq!(config_map.metadata.name.as_deref(), Some("hdfs"));
        assert!(
            config_map.metadata.resource_version.is_none(),
            "server-populated fields must not be echoed into an apply patch"
        );
        let labels = config_map.metadata.labels.expect("labels are set");
        assert_eq!(
            labels.get("app.kubernetes.io/instance").map(String::as_str),
            Some("hdfs"),
            "the labels required by ClusterResources::add must be set"
        );
    }

    /// Before the first successful build there is nothing to re-emit: the ConfigMap is skipped
    /// for this run instead of failing it. The Listener watch triggers a new run once the
    /// listener-operator has written the addresses.
    #[test]
    fn skips_the_config_map_when_it_has_never_been_built() {
        let mut cluster = validated_cluster();
        cluster.namenode_listeners = vec![];
        cluster.discovery_config_map = None;

        let config_map =
            build_discovery_config_map(&cluster, &cluster_info()).expect("the build must not fail");

        assert!(config_map.is_none());
    }
}
