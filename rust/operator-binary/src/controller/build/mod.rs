use std::{collections::HashMap, str::FromStr};

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    kvp::{LabelError, Labels},
    v2::{
        builder::meta::ownerreference_from_resource,
        types::{common::Port, kubernetes::ServiceName, operator::RoleGroupName},
    },
};

use crate::{
    build_recommended_labels,
    controller::ValidatedCluster,
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
    hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER,
};

pub mod container;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod resource;

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
            // The headless Service that governs the pods is named after the qualified role group
            // name (see `build::resource::service::rolegroup_headless_service`).
            let service_name = ServiceName::from_str(
                cluster
                    .resource_names(role, role_group_name)
                    .qualified_role_group_name()
                    .as_ref(),
            )
            .expect("a qualified role group name is a valid Service name");
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
    let role_name = role.to_string();
    let mut metadata = ObjectMetaBuilder::new();
    metadata
        .name_and_namespace(cluster)
        .name(
            cluster
                .resource_names(role, role_group_name)
                .qualified_role_group_name(),
        )
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_recommended_labels(&build_recommended_labels(
            cluster,
            RESOURCE_MANAGER_HDFS_CONTROLLER,
            &cluster.image.app_version_label_value,
            &role_name,
            role_group_name.as_ref(),
        ))
        .expect(
            "the recommended labels are valid because the ValidatedCluster uses \
            fail-safe typed values",
        );
    metadata
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
