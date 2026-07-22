use std::collections::HashMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    kvp::{LabelError, Labels},
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::{common::Port, operator::RoleGroupName},
};

use crate::{
    controller::{
        KubernetesResources, ValidatedCluster,
        build::resource::rbac::{build_role_binding, build_service_account},
    },
    crd::{
        HdfsNodeRole, HdfsPodRef,
        constants::{
            APP_NAME, DEFAULT_DATA_NODE_DATA_PORT, DEFAULT_DATA_NODE_HTTP_PORT,
            DEFAULT_DATA_NODE_HTTPS_PORT, DEFAULT_DATA_NODE_IPC_PORT,
            DEFAULT_DATA_NODE_METRICS_PORT, DEFAULT_DATA_NODE_NATIVE_METRICS_HTTP_PORT,
            DEFAULT_DATA_NODE_NATIVE_METRICS_HTTPS_PORT, DEFAULT_JOURNAL_NODE_HTTP_PORT,
            DEFAULT_JOURNAL_NODE_HTTPS_PORT, DEFAULT_JOURNAL_NODE_METRICS_PORT,
            DEFAULT_JOURNAL_NODE_NATIVE_METRICS_HTTP_PORT,
            DEFAULT_JOURNAL_NODE_NATIVE_METRICS_HTTPS_PORT, DEFAULT_JOURNAL_NODE_RPC_PORT,
            DEFAULT_NAME_NODE_HTTP_PORT, DEFAULT_NAME_NODE_HTTPS_PORT,
            DEFAULT_NAME_NODE_METRICS_PORT, DEFAULT_NAME_NODE_NATIVE_METRICS_HTTP_PORT,
            DEFAULT_NAME_NODE_NATIVE_METRICS_HTTPS_PORT, DEFAULT_NAME_NODE_RPC_PORT,
            SERVICE_PORT_NAME_DATA, SERVICE_PORT_NAME_HTTP, SERVICE_PORT_NAME_HTTPS,
            SERVICE_PORT_NAME_IPC, SERVICE_PORT_NAME_JMX_METRICS, SERVICE_PORT_NAME_METRICS,
            SERVICE_PORT_NAME_RPC,
        },
    },
};

pub mod container;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod resource;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build Service for role {role} role group {role_group}"))]
    Service {
        source: resource::service::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role {role} role group {role_group}"))]
    ConfigMap {
        source: resource::config_map::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role {role} role group {role_group}"))]
    StatefulSet {
        source: resource::statefulset::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every external reference is already dereferenced and
/// validated by this point, so the errors returned here are resource-assembly failures only.
/// `cluster_info` carries static cluster information resolved at operator startup (e.g. the
/// cluster domain used to build Kerberos principals), not a live client.
///
/// The resources are returned as flat, unordered collections. The reconcile step re-groups the
/// StatefulSets by role to preserve HDFS's ordered, rollout-gated deployment during upgrades. The
/// discovery `ConfigMap` is deliberately not built here: it needs a live client to resolve
/// listener addresses and is therefore handled in the reconcile step.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources, Error> {
    let mut services = vec![];
    let mut config_maps = vec![];
    let mut stateful_sets = vec![];
    let mut pod_disruption_budgets = vec![];

    for (role, role_group_configs) in &cluster.role_groups {
        for (role_group_name, rg_config) in role_group_configs {
            services.push(
                resource::service::rolegroup_headless_service(cluster, role, role_group_name)
                    .context(ServiceSnafu {
                        role: *role,
                        role_group: role_group_name.clone(),
                    })?,
            );
            services.push(
                resource::service::rolegroup_metrics_service(cluster, role, role_group_name)
                    .context(ServiceSnafu {
                        role: *role,
                        role_group: role_group_name.clone(),
                    })?,
            );
            config_maps.push(
                resource::config_map::build_rolegroup_config_map(
                    cluster,
                    cluster_info,
                    role,
                    role_group_name,
                )
                .context(ConfigMapSnafu {
                    role: *role,
                    role_group: role_group_name.clone(),
                })?,
            );
            stateful_sets.push(
                resource::statefulset::build_rolegroup_statefulset(
                    cluster,
                    cluster_info,
                    role,
                    role_group_name,
                    rg_config,
                )
                .context(StatefulSetSnafu {
                    role: *role,
                    role_group: role_group_name.clone(),
                })?,
            );
        }

        if let Some(pdb) = resource::pdb::build_pdb(cluster, role) {
            pod_disruption_budgets.push(pdb);
        }
    }

    Ok(KubernetesResources {
        services,
        config_maps,
        pod_disruption_budgets,
        stateful_sets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
    })
}

/// Builds the [`HdfsPodRef`]s expected for every pod of the given `role`, across all
/// of its role groups.
///
/// These pod refs can only access HDFS from inside the Kubernetes cluster (they use
/// the cluster-internal headless service DNS names). For downstream clients, the
/// listener-based refs collected during reconciliation are used instead.
///
/// This is infallible: all required information (namespace, replicas and ports) is
/// already resolved on `cluster` during validation.
pub(crate) fn pod_refs(cluster: &ValidatedCluster, role: &HdfsNodeRole) -> Vec<HdfsPodRef> {
    let ports: HashMap<String, Port> = role_data_ports(role, cluster.has_https_enabled())
        .into_iter()
        .collect();

    cluster
        .role_groups
        .get(role)
        .into_iter()
        .flatten()
        .flat_map(|(role_group_name, role_group)| {
            let service_name = cluster.governing_service_name(role, role_group_name);
            let object_name = service_name.to_string();
            let namespace = cluster.namespace.clone();
            let ports = ports.clone();
            (0..role_group.replicas.unwrap_or(1)).map(move |i| HdfsPodRef {
                namespace: namespace.clone(),
                role_group_service_name: service_name.clone(),
                pod_name: format!("{object_name}-{i}"),
                ports: ports.clone(),
                fqdn_override: None,
            })
        })
        .collect()
}

/// Builds the common [`ObjectMetaBuilder`] shared by a role group's owned resources
/// (the ConfigMap and the StatefulSet): name, namespace, owner reference and the
/// recommended labels, all derived from the validated cluster.
///
/// This is infallible: a [`ValidatedCluster`] always carries a name, namespace and
/// uid, and its fail-safe typed values always produce valid label values, so neither
/// the owner reference nor the recommended labels can fail to build.
pub(crate) fn rolegroup_metadata(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> ObjectMetaBuilder {
    cluster.object_meta(
        cluster
            .role_group_resource_names(role, role_group_name)
            .qualified_role_group_name()
            .to_string(),
        cluster.recommended_labels(role, role_group_name),
    )
}

/// The rolegroup selector labels (also used as `Service`/`StatefulSet` selectors) for
/// the given role group.
pub(crate) fn rolegroup_selector_labels(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Labels, LabelError> {
    let role_name = role.to_string();
    let mut group_labels =
        Labels::role_group_selector(cluster, APP_NAME, &role_name, role_group_name.as_ref())?;
    group_labels.parse_insert(("role", &role_name))?;
    group_labels.parse_insert(("group", role_group_name.as_ref()))?;

    Ok(group_labels)
}

/// The total number of datanode replicas across all datanode role groups.
pub(crate) fn num_datanodes(cluster: &ValidatedCluster) -> u16 {
    cluster
        .role_groups
        .get(&HdfsNodeRole::Data)
        .into_iter()
        .flatten()
        .map(|(_, role_group)| role_group.replicas.unwrap_or(1))
        .sum()
}

/// The ports exposed by the rolegroup headless service for the given `role`.
pub(crate) fn headless_service_ports(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
) -> Vec<(String, Port)> {
    role_data_ports(role, cluster.has_https_enabled())
}

/// The ports exposed by the main container of the given `role`.
pub(crate) fn hdfs_main_container_ports(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
) -> Vec<(String, Port)> {
    role_data_ports(role, cluster.has_https_enabled())
}

/// The ports exposed by the rolegroup metrics service for the given `role` (native
/// Prometheus endpoint plus the deprecated JMX exporter port).
pub(crate) fn metrics_service_ports(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
) -> Vec<(String, Port)> {
    vec![
        (
            SERVICE_PORT_NAME_METRICS.to_string(),
            native_metrics_port(cluster, role),
        ),
        (
            SERVICE_PORT_NAME_JMX_METRICS.to_string(),
            jmx_metrics_port(role),
        ),
    ]
}

/// The native (built-in) Prometheus metrics port for the given `role`.
pub(crate) fn native_metrics_port(cluster: &ValidatedCluster, role: &HdfsNodeRole) -> Port {
    match (role, cluster.has_https_enabled()) {
        (HdfsNodeRole::Name, false) => DEFAULT_NAME_NODE_NATIVE_METRICS_HTTP_PORT,
        (HdfsNodeRole::Name, true) => DEFAULT_NAME_NODE_NATIVE_METRICS_HTTPS_PORT,
        (HdfsNodeRole::Data, false) => DEFAULT_DATA_NODE_NATIVE_METRICS_HTTP_PORT,
        (HdfsNodeRole::Data, true) => DEFAULT_DATA_NODE_NATIVE_METRICS_HTTPS_PORT,
        (HdfsNodeRole::Journal, false) => DEFAULT_JOURNAL_NODE_NATIVE_METRICS_HTTP_PORT,
        (HdfsNodeRole::Journal, true) => DEFAULT_JOURNAL_NODE_NATIVE_METRICS_HTTPS_PORT,
    }
}

/// The deprecated JMX exporter metrics port for the given `role`.
fn jmx_metrics_port(role: &HdfsNodeRole) -> Port {
    match role {
        HdfsNodeRole::Name => DEFAULT_NAME_NODE_METRICS_PORT,
        HdfsNodeRole::Data => DEFAULT_DATA_NODE_METRICS_PORT,
        HdfsNodeRole::Journal => DEFAULT_JOURNAL_NODE_METRICS_PORT,
    }
}

/// The required port name/number tuples exposed by pods of the given `role`,
/// depending on whether HTTPS is enabled.
fn role_data_ports(role: &HdfsNodeRole, https_enabled: bool) -> Vec<(String, Port)> {
    match role {
        HdfsNodeRole::Name => vec![
            (
                String::from(SERVICE_PORT_NAME_RPC),
                DEFAULT_NAME_NODE_RPC_PORT,
            ),
            if https_enabled {
                (
                    String::from(SERVICE_PORT_NAME_HTTPS),
                    DEFAULT_NAME_NODE_HTTPS_PORT,
                )
            } else {
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_NAME_NODE_HTTP_PORT,
                )
            },
        ],
        HdfsNodeRole::Data => vec![
            (
                String::from(SERVICE_PORT_NAME_DATA),
                DEFAULT_DATA_NODE_DATA_PORT,
            ),
            (
                String::from(SERVICE_PORT_NAME_IPC),
                DEFAULT_DATA_NODE_IPC_PORT,
            ),
            if https_enabled {
                (
                    String::from(SERVICE_PORT_NAME_HTTPS),
                    DEFAULT_DATA_NODE_HTTPS_PORT,
                )
            } else {
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_DATA_NODE_HTTP_PORT,
                )
            },
        ],
        HdfsNodeRole::Journal => vec![
            (
                String::from(SERVICE_PORT_NAME_RPC),
                DEFAULT_JOURNAL_NODE_RPC_PORT,
            ),
            if https_enabled {
                (
                    String::from(SERVICE_PORT_NAME_HTTPS),
                    DEFAULT_JOURNAL_NODE_HTTPS_PORT,
                )
            } else {
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_JOURNAL_NODE_HTTP_PORT,
                )
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::kube::Resource;

    use super::build;
    use crate::{
        controller::build::properties::test_support::{self, cluster_info, validated_cluster},
        test_support::deserialize_and_validate_cluster,
    };

    /// The sorted `metadata.name`s of a resource collection.
    fn sorted_names(resources: &[impl Resource]) -> Vec<String> {
        let mut names: Vec<String> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.clone())
            .collect();
        names.sort();
        names
    }

    /// The aggregator emits, for the minimal three-role cluster (one `default` role group each):
    /// one StatefulSet and one ConfigMap per role group, one headless plus one metrics Service per
    /// role group, and one default PDB per role.
    #[test]
    fn build_produces_expected_resource_names() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "hdfs-datanode-default",
                "hdfs-journalnode-default",
                "hdfs-namenode-default",
            ]
        );
        // One headless (un-suffixed, see `ValidatedCluster::governing_service_name`) and one
        // metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "hdfs-datanode-default",
                "hdfs-datanode-default-metrics",
                "hdfs-journalnode-default",
                "hdfs-journalnode-default-metrics",
                "hdfs-namenode-default",
                "hdfs-namenode-default-metrics",
            ]
        );
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "hdfs-datanode-default",
                "hdfs-journalnode-default",
                "hdfs-namenode-default",
            ]
        );
        // A default PDB per role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["hdfs-datanode", "hdfs-journalnode", "hdfs-namenode"]
        );
    }

    /// Every StatefulSet's (immutable) `serviceName` must reference a headless Service that the
    /// build step actually produces — the pods' DNS names depend on the pair agreeing. Guards the
    /// coupling that `ValidatedCluster::governing_service_name` centralises.
    #[test]
    fn statefulset_service_name_references_built_service() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        let service_names = sorted_names(&resources.services);
        for stateful_set in &resources.stateful_sets {
            let service_name = stateful_set
                .spec
                .as_ref()
                .and_then(|spec| spec.service_name.as_deref())
                .expect("every StatefulSet sets serviceName");
            assert!(
                service_names.iter().any(|name| name == service_name),
                "StatefulSet references headless Service {service_name:?}, which is not built \
                (built Services: {service_names:?})"
            );
        }
    }

    /// Locks the RBAC resource names, the roleRef, and the recommended label set against
    /// accidental drift. The cluster name deliberately differs from the product name so that
    /// swapped `name`/`instance` label values cannot pass unnoticed (the shared fixture is named
    /// `hdfs`, which would mask exactly that swap).
    #[test]
    fn build_produces_rbac() {
        let cluster = deserialize_and_validate_cluster(
            &test_support::MINIMAL_HDFS_YAML.replace("name: hdfs", "name: my-hdfs"),
        );
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["my-hdfs-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["my-hdfs-rolebinding"]
        );

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "none"),
                ("app.kubernetes.io/instance", "my-hdfs"),
                (
                    "app.kubernetes.io/managed-by",
                    "hdfs.stackable.tech_hdfs-operator-hdfs-controller",
                ),
                ("app.kubernetes.io/name", "hdfs"),
                ("app.kubernetes.io/role-group", "none"),
                ("app.kubernetes.io/version", "3.4.0-stackable0.0.0-dev"),
                ("stackable.tech/vendor", "Stackable"),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let service_account = resources
            .service_accounts
            .first()
            .expect("a ServiceAccount is built");
        assert_eq!(
            service_account.metadata.labels,
            Some(expected_labels.clone())
        );

        let role_binding = resources
            .role_bindings
            .first()
            .expect("a RoleBinding is built");
        assert_eq!(role_binding.metadata.labels, Some(expected_labels));
        assert_eq!(role_binding.role_ref.name, "hdfs-clusterrole");
    }
}
