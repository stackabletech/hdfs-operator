use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, ResourceRequirements, Service, Volume},
    kvp::{LabelError, Labels},
    product_logging::spec::ContainerLogConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::meta::ownerreference_from_resource,
        kvp::label,
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::{
            common::Port,
            operator::{RoleGroupName, RoleName},
        },
    },
};

use crate::{
    controller::{
        CONTROLLER_NAME, KubernetesResources, OPERATOR_NAME, PRODUCT_NAME, Prepared,
        ValidatedCluster,
        build::{
            container::ContainerConfig,
            resource::rbac::{build_role_binding, build_service_account},
        },
    },
    crd::{
        CommonNodeConfig, DataNodeContainer, HdfsNodeRole, HdfsPodRef, JournalNodeContainer,
        NameNodeContainer,
        constants::{
            DEFAULT_DATA_NODE_DATA_PORT, DEFAULT_DATA_NODE_HTTP_PORT, DEFAULT_DATA_NODE_HTTPS_PORT,
            DEFAULT_DATA_NODE_IPC_PORT, DEFAULT_DATA_NODE_METRICS_PORT,
            DEFAULT_DATA_NODE_NATIVE_METRICS_HTTP_PORT,
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
        v1alpha1,
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
    #[snafu(display("failed to build Service for role {role} role group {role_group}", role = role.as_ref()))]
    Service {
        source: resource::service::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build ConfigMap for role {role} role group {role_group}", role = role.as_ref()))]
    ConfigMap {
        source: resource::config_map::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role {role} role group {role_group}", role = role.as_ref()))]
    StatefulSet {
        source: resource::statefulset::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build the discovery ConfigMap"))]
    DiscoveryConfigMap { source: resource::discovery::Error },

    #[snafu(display("failed to build selector labels for role {role} role group {role_group}", role = role.as_ref()))]
    RoleGroupSelectorLabels {
        source: LabelError,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build volume claim templates for role {role} role group {role_group}", role = role.as_ref()))]
    VolumeClaimTemplates {
        source: container::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build listener volume for role {role} role group {role_group}", role = role.as_ref()))]
    ListenerVolume {
        source: container::Error,
        role: HdfsNodeRole,
        role_group: RoleGroupName,
    },
}

/// The log configuration of every container in one role group, resolved during the build step
/// by code that knows the role, so the shared builders never see a role-specific
/// `Logging<C>`.
#[derive(Clone, Debug)]
pub struct RoleGroupLogging {
    /// The main `hdfs` container, which every role has.
    pub hdfs: ContainerLogConfig,
    /// The Vector sidecar; `None` when the Vector agent is disabled for this role group.
    pub vector: Option<ContainerLogConfig>,
    /// The namenode `zkfc` side container.
    pub zkfc: Option<ContainerLogConfig>,
    /// The namenode `format-namenodes` init container.
    pub format_namenodes: Option<ContainerLogConfig>,
    /// The namenode `format-zookeeper` init container.
    pub format_zookeeper: Option<ContainerLogConfig>,
    /// The datanode `wait-for-namenodes` init container.
    pub wait_for_namenodes: Option<ContainerLogConfig>,
}

/// Everything about one role group that the shared builders below cannot derive themselves: the
/// values resolved from its role-specific config, plus the selector labels, which the build loop
/// already needs for the listener volume and the PVC templates.
///
/// Resolving these in the build loop, which knows the role, is what lets the builders be generic
/// over the role group's config type.
pub struct ResolvedRoleGroup {
    /// The selector labels of the role group's pods, also used as the `StatefulSet` selector and
    /// on its listener volume.
    ///
    /// We must use the selector labels and not the recommended labels for the listener volumes.
    /// This is because the recommended set contains a "managed-by" label. That label triggers the
    /// cluster resources to "manage" listeners, which is wrong and leads to errors. The listeners
    /// are managed by the listener-operator.
    pub selector_labels: Labels,
    /// The role group's merged config that is common to every role.
    pub common: CommonNodeConfig,
    /// The resource requirements of the role group's containers.
    pub resources: ResourceRequirements,
    /// The `StatefulSet`'s persistent volume claim templates.
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// The ephemeral listener volume; only datanodes have one (namenodes get their listener from
    /// a volume claim template and journalnodes have no listener at all).
    pub listener_volume: Option<Volume>,
    /// The log config of each of the role group's containers.
    pub logging: RoleGroupLogging,
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every external reference is already dereferenced and
/// validated by this point, so the errors returned here are resource-assembly failures only.
/// `cluster_info` carries static cluster information resolved at operator startup (e.g. the
/// cluster domain used to build Kerberos principals), not a live client.
///
/// The resources are returned as flat collections. `stateful_sets` is ordered by role —
/// journalnodes, then namenodes, then datanodes — because the apply step rolls them out in that
/// order during upgrades to preserve HDFS's rollout-gated deployment (see
/// [`crate::controller::apply::Applier::apply`]).
/// The discovery `ConfigMap` is included when it can be built or re-emitted (see
/// [`resource::discovery::build_discovery_config_map`]); it is only absent before its first
/// successful build.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources<Prepared>, Error> {
    let mut services = vec![];
    let mut config_maps = vec![];
    let mut stateful_sets = vec![];
    let mut pod_disruption_budgets = vec![];

    // The roles are built in the order journalnode, namenode, datanode, and the resulting
    // `stateful_sets` order is load-bearing: the apply step rolls the StatefulSets out in that
    // order during upgrades, each role gated on the previous one (see
    // [`crate::controller::apply::Applier::apply`]).
    for (role_group_name, rg_config) in &cluster.journalnode_role_group_configs {
        let role = &HdfsNodeRole::Journal;
        let config = &rg_config.config;

        build_role_group_services(cluster, role, role_group_name, &mut services)?;

        let selector_labels = rolegroup_selector_labels(cluster, role, role_group_name).context(
            RoleGroupSelectorLabelsSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?;
        let resolved = ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(config),
            listener_volume: None,
            logging: RoleGroupLogging {
                hdfs: config
                    .logging
                    .for_container(&JournalNodeContainer::Hdfs)
                    .into_owned(),
                vector: config.logging.enable_vector_agent.then(|| {
                    config
                        .logging
                        .for_container(&JournalNodeContainer::Vector)
                        .into_owned()
                }),
                zkfc: None,
                format_namenodes: None,
                format_zookeeper: None,
                wait_for_namenodes: None,
            },
        };

        config_maps.push(
            resource::config_map::build_rolegroup_config_map(
                cluster,
                cluster_info,
                role,
                role_group_name,
                rg_config,
                None,
                &resolved.logging,
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
                resolved,
            )
            .context(StatefulSetSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            })?,
        );
    }
    if let Some(pdb) = resource::pdb::build_pdb(cluster, &HdfsNodeRole::Journal) {
        pod_disruption_budgets.push(pdb);
    }

    for (role_group_name, rg_config) in &cluster.namenode_role_group_configs {
        let role = &HdfsNodeRole::Name;
        let config = &rg_config.config;

        build_role_group_services(cluster, role, role_group_name, &mut services)?;

        let selector_labels = rolegroup_selector_labels(cluster, role, role_group_name).context(
            RoleGroupSelectorLabelsSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?;
        // Namenodes get their listener from a persistent volume claim template, for stable
        // per-pod identity, rather than from an ephemeral volume.
        let volume_claim_templates =
            ContainerConfig::namenode_volume_claim_templates(config, &selector_labels).context(
                VolumeClaimTemplatesSnafu {
                    role: *role,
                    role_group: role_group_name.clone(),
                },
            )?;
        let resolved = ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates,
            listener_volume: None,
            logging: RoleGroupLogging {
                hdfs: config
                    .logging
                    .for_container(&NameNodeContainer::Hdfs)
                    .into_owned(),
                vector: config.logging.enable_vector_agent.then(|| {
                    config
                        .logging
                        .for_container(&NameNodeContainer::Vector)
                        .into_owned()
                }),
                zkfc: Some(
                    config
                        .logging
                        .for_container(&NameNodeContainer::Zkfc)
                        .into_owned(),
                ),
                format_namenodes: Some(
                    config
                        .logging
                        .for_container(&NameNodeContainer::FormatNameNodes)
                        .into_owned(),
                ),
                format_zookeeper: Some(
                    config
                        .logging
                        .for_container(&NameNodeContainer::FormatZooKeeper)
                        .into_owned(),
                ),
                wait_for_namenodes: None,
            },
        };

        config_maps.push(
            resource::config_map::build_rolegroup_config_map(
                cluster,
                cluster_info,
                role,
                role_group_name,
                rg_config,
                None,
                &resolved.logging,
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
                resolved,
            )
            .context(StatefulSetSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            })?,
        );
    }
    if let Some(pdb) = resource::pdb::build_pdb(cluster, &HdfsNodeRole::Name) {
        pod_disruption_budgets.push(pdb);
    }

    for (role_group_name, rg_config) in &cluster.datanode_role_group_configs {
        let role = &HdfsNodeRole::Data;
        let config = &rg_config.config;

        build_role_group_services(cluster, role, role_group_name, &mut services)?;

        let selector_labels = rolegroup_selector_labels(cluster, role, role_group_name).context(
            RoleGroupSelectorLabelsSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?;
        // Datanodes use an ephemeral listener volume, since they need no stable per-pod identity.
        let listener_volume = Some(
            ContainerConfig::datanode_listener_volume(config, &selector_labels).context(
                ListenerVolumeSnafu {
                    role: *role,
                    role_group: role_group_name.clone(),
                },
            )?,
        );
        let resolved = ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(config),
            listener_volume,
            logging: RoleGroupLogging {
                hdfs: config
                    .logging
                    .for_container(&DataNodeContainer::Hdfs)
                    .into_owned(),
                vector: config.logging.enable_vector_agent.then(|| {
                    config
                        .logging
                        .for_container(&DataNodeContainer::Vector)
                        .into_owned()
                }),
                zkfc: None,
                format_namenodes: None,
                format_zookeeper: None,
                wait_for_namenodes: Some(
                    config
                        .logging
                        .for_container(&DataNodeContainer::WaitForNameNodes)
                        .into_owned(),
                ),
            },
        };

        config_maps.push(
            resource::config_map::build_rolegroup_config_map(
                cluster,
                cluster_info,
                role,
                role_group_name,
                rg_config,
                Some(config.resources.storage.clone()),
                &resolved.logging,
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
                resolved,
            )
            .context(StatefulSetSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            })?,
        );
    }
    if let Some(pdb) = resource::pdb::build_pdb(cluster, &HdfsNodeRole::Data) {
        pod_disruption_budgets.push(pdb);
    }

    // The discovery ConfigMap is skipped only before its first successful build (no namenode
    // Listener addresses yet, nothing stored to re-emit); afterwards a stored ConfigMap is
    // re-emitted unchanged whenever it cannot be rebuilt, so it stays tracked.
    if let Some(discovery_config_map) =
        resource::discovery::build_discovery_config_map(cluster, cluster_info)
            .context(DiscoveryConfigMapSnafu)?
    {
        config_maps.push(discovery_config_map);
    }

    Ok(KubernetesResources {
        services,
        config_maps,
        pod_disruption_budgets,
        stateful_sets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

/// Builds the two Services for one role group. Role-agnostic: it reads nothing from the role
/// config.
fn build_role_group_services(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
    services: &mut Vec<Service>,
) -> Result<(), Error> {
    services.push(
        resource::service::rolegroup_headless_service(cluster, role, role_group_name).context(
            ServiceSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?,
    );
    services.push(
        resource::service::rolegroup_metrics_service(cluster, role, role_group_name).context(
            ServiceSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?,
    );

    Ok(())
}

/// The replica count of every role group in the map, defaulting to one where it is unset.
fn role_group_replicas<C>(
    role_group_configs: &BTreeMap<
        RoleGroupName,
        RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
) -> Vec<(&RoleGroupName, u16)> {
    role_group_configs
        .iter()
        .map(|(role_group_name, role_group)| (role_group_name, role_group.replicas.unwrap_or(1)))
        .collect()
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

    let replicas_per_role_group = match role {
        HdfsNodeRole::Name => role_group_replicas(&cluster.namenode_role_group_configs),
        HdfsNodeRole::Data => role_group_replicas(&cluster.datanode_role_group_configs),
        HdfsNodeRole::Journal => role_group_replicas(&cluster.journalnode_role_group_configs),
    };

    replicas_per_role_group
        .into_iter()
        .flat_map(|(role_group_name, replicas)| {
            let service_name = cluster.governing_service_name(role, role_group_name);
            let object_name = service_name.to_string();
            let namespace = cluster.namespace.clone();
            let ports = ports.clone();
            (0..replicas).map(move |i| HdfsPodRef {
                namespace: namespace.clone(),
                role_group_service_name: service_name.clone(),
                pod_name: format!("{object_name}-{i}"),
                ports: ports.clone(),
                fqdn_override: None,
            })
        })
        .collect()
}

/// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, the resource `name`, an owner
/// reference back to the cluster, and the given recommended `labels`.
pub(crate) fn object_meta(
    cluster: &ValidatedCluster,
    name: impl Into<String>,
    labels: Labels,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(labels);
    builder
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
    object_meta(
        cluster,
        cluster
            .role_group_resource_names(role, role_group_name)
            .qualified_role_group_name()
            .to_string(),
        recommended_labels_for_role_group_resources(cluster, role, role_group_name),
    )
}

/// The rolegroup selector labels (also used as `Service`/`StatefulSet` selectors) for
/// the given role group.
pub(crate) fn rolegroup_selector_labels(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Labels, LabelError> {
    let mut group_labels = role_group_selector(cluster, role, role_group_name);
    group_labels.parse_insert(("role", role.as_ref()))?;
    group_labels.parse_insert(("group", role_group_name.as_ref()))?;

    Ok(group_labels)
}

/// The total number of datanode replicas across all datanode role groups.
pub(crate) fn num_datanodes(cluster: &ValidatedCluster) -> u16 {
    cluster
        .datanode_role_group_configs
        .values()
        .map(|role_group| role_group.replicas.unwrap_or(1))
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

pub(crate) fn recommended_labels_for_cluster_resources(cluster: &ValidatedCluster) -> Labels {
    label::recommended_labels_for_cluster_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
}

pub(crate) fn recommended_labels_for_role_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
) -> Labels {
    label::recommended_labels_for_role_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
    )
}

pub(crate) fn recommended_labels_for_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

/// Selector labels matching the pods of a role group.
pub(crate) fn role_group_selector(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::role_group_selector(&cluster.name, &PRODUCT_NAME, role_name, role_group_name)
}

#[cfg(test)]
mod tests {
    use stackable_operator::kube::Resource;

    use super::build;
    use crate::{
        controller::build::properties::test_support::{cluster_info, validated_cluster},
        test_support::namenode_listener,
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
        // The cluster-shared RBAC pair.
        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["hdfs-serviceaccount"]
        );
        assert_eq!(sorted_names(&resources.role_bindings), ["hdfs-rolebinding"]);
    }

    /// The StatefulSets must come out in role order — journalnodes, then namenodes, then
    /// datanodes — because the apply step rolls them out in exactly that order during upgrades,
    /// each role gated on the previous one's rollout completing (see
    /// [`crate::controller::apply::Applier::apply`]). The other tests here sort the names, which
    /// would hide a reordering, so this one asserts on the order as built.
    #[test]
    fn stateful_sets_are_ordered_by_role() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        let names: Vec<String> = resources
            .stateful_sets
            .iter()
            .filter_map(|stateful_set| stateful_set.meta().name.clone())
            .collect();

        assert_eq!(
            names,
            [
                "hdfs-journalnode-default",
                "hdfs-namenode-default",
                "hdfs-datanode-default",
            ]
        );
    }

    /// With every namenode Listener carrying an ingress address, the build step emits the
    /// discovery ConfigMap (named after the cluster) alongside the role-group ConfigMaps, so
    /// the apply step tracks it like any other resource. Without ready Listeners it is
    /// skipped — `build_produces_expected_resource_names` covers that side.
    #[test]
    fn build_includes_the_discovery_config_map_when_listeners_are_ready() {
        let mut cluster = validated_cluster();
        cluster.namenode_listeners = vec![namenode_listener(
            "listener-hdfs-namenode-default-0",
            "namenode-0.example.org",
            31000,
        )];

        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "hdfs",
                "hdfs-datanode-default",
                "hdfs-journalnode-default",
                "hdfs-namenode-default",
            ]
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
}
