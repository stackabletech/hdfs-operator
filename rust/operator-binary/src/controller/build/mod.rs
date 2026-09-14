use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    marker::PhantomData,
};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, PersistentVolumeClaim, ResourceRequirements, Service, Volume},
        policy::v1::PodDisruptionBudget,
    },
    kvp::{LabelError, Labels},
    product_logging::spec::{ContainerLogConfig, Logging},
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
        CommonNodeConfig, DataNodeConfig, DataNodeContainer, HdfsNodeRole, HdfsPodRef,
        JournalNodeConfig, JournalNodeContainer, NameNodeConfig, NameNodeContainer,
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
        storage::DataNodeStorageConfigInnerType,
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
#[derive(Debug)]
pub struct RoleGroupLogging {
    /// The main `hdfs` container, which every role has.
    pub hdfs: ContainerLogConfig,
    /// The Vector sidecar; `None` when the Vector agent is disabled for this role group.
    pub vector: Option<ContainerLogConfig>,
    /// The containers only one role has.
    pub role: RoleContainerLogging,
}

/// The log configuration of the side and init containers that only one role runs.
///
/// These live in an enum rather than in `Option` fields so that "the `zkfc` log config exists
/// exactly when this is a namenode" is checked by the compiler at every construction site. A
/// missing log config is otherwise silent: the container's `log4j.properties` is left out of both
/// the `ConfigMap` and the `cp` in the container args, so it logs with Hadoop's built-in defaults
/// and Vector collects nothing for it.
#[derive(Debug)]
pub enum RoleContainerLogging {
    /// Journalnodes run no role-specific container.
    Journal,
    /// The namenode `zkfc` side container and its two init containers.
    Name {
        zkfc: ContainerLogConfig,
        format_namenodes: ContainerLogConfig,
        format_zookeeper: ContainerLogConfig,
    },
    /// The datanode `wait-for-namenodes` init container.
    Data {
        wait_for_namenodes: ContainerLogConfig,
    },
}

impl RoleContainerLogging {
    /// The namenode `zkfc` side container's log config; `None` for the other roles.
    pub fn zkfc(&self) -> Option<&ContainerLogConfig> {
        match self {
            Self::Name { zkfc, .. } => Some(zkfc),
            Self::Journal | Self::Data { .. } => None,
        }
    }

    /// The namenode `format-namenodes` init container's log config; `None` for the other roles.
    pub fn format_namenodes(&self) -> Option<&ContainerLogConfig> {
        match self {
            Self::Name {
                format_namenodes, ..
            } => Some(format_namenodes),
            Self::Journal | Self::Data { .. } => None,
        }
    }

    /// The namenode `format-zookeeper` init container's log config; `None` for the other roles.
    pub fn format_zookeeper(&self) -> Option<&ContainerLogConfig> {
        match self {
            Self::Name {
                format_zookeeper, ..
            } => Some(format_zookeeper),
            Self::Journal | Self::Data { .. } => None,
        }
    }

    /// The datanode `wait-for-namenodes` init container's log config; `None` for the other roles.
    pub fn wait_for_namenodes(&self) -> Option<&ContainerLogConfig> {
        match self {
            Self::Data { wait_for_namenodes } => Some(wait_for_namenodes),
            Self::Journal | Self::Name { .. } => None,
        }
    }
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
    /// The resource requirements of the role group's main and init containers; the ZKFC sidecar
    /// has fixed requirements of its own and ignores this.
    pub resources: ResourceRequirements,
    /// The `StatefulSet`'s persistent volume claim templates.
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// The values that exist for one role only.
    pub role: RoleSpecificResources,
    /// The log config of each of the role group's containers.
    pub logging: RoleGroupLogging,
}

/// The role group values that exist for one role only.
///
/// These live in an enum rather than in `Option` fields so the compiler checks the pairing at
/// every construction site: a datanode without its storage configuration does not compile — that
/// would silently drop `dfs.datanode.data.dir` and send the datanodes' blocks to container-local
/// storage — and neither does a namenode with a pod-level listener volume, which would collide
/// with the identically named volume claim template and be rejected at apply time.
pub enum RoleSpecificResources {
    /// Journalnodes have no listener and no role-specific storage configuration.
    Journal,
    /// Namenodes get their listener from a volume claim template in `volume_claim_templates`, for
    /// stable per-pod identity, so they have no pod-level listener volume.
    Name,
    /// Datanodes need no stable per-pod identity, so their listener is an ephemeral pod volume.
    /// They are also the only role that configures `dfs.datanode.data.dir`.
    Data {
        listener_volume: Volume,
        storage: DataNodeStorageConfigInnerType,
    },
}

impl RoleSpecificResources {
    /// The role these values belong to.
    pub fn node_role(&self) -> HdfsNodeRole {
        match self {
            Self::Journal => HdfsNodeRole::Journal,
            Self::Name => HdfsNodeRole::Name,
            Self::Data { .. } => HdfsNodeRole::Data,
        }
    }

    /// The role group's ephemeral listener volume; only datanodes have one.
    pub fn listener_volume(&self) -> Option<&Volume> {
        match self {
            Self::Data {
                listener_volume, ..
            } => Some(listener_volume),
            Self::Journal | Self::Name => None,
        }
    }

    /// The datanode data volume configuration, which drives `dfs.datanode.data.dir`; `None` for
    /// the other roles.
    pub fn datanode_storage(&self) -> Option<&DataNodeStorageConfigInnerType> {
        match self {
            Self::Data { storage, .. } => Some(storage),
            Self::Journal | Self::Name => None,
        }
    }
}

/// The log config of the two containers every role has: the main `hdfs` container, and the Vector
/// sidecar, which is `None` when the Vector agent is disabled for the role group.
///
/// Each role names these containers with its own enum, so this is generic over that enum rather
/// than repeated once per role.
fn common_container_logging<T>(
    logging: &Logging<T>,
    hdfs: T,
    vector: T,
) -> (ContainerLogConfig, Option<ContainerLogConfig>)
where
    T: Clone + Display + Ord,
{
    (
        logging.for_container(&hdfs).into_owned(),
        logging
            .enable_vector_agent
            .then(|| logging.for_container(&vector).into_owned()),
    )
}

/// How to resolve one role group's role-specific values, implemented once per role config type.
///
/// This is what lets [`build_role`] be written once: the trait supplies the role and the single
/// role-dependent step, and everything else about building a role group is identical across the
/// three roles.
///
/// [`Self::ROLE`] is also the single source of truth for the role in the shared builders: they
/// take the role group's config type and read the role from it, rather than taking the role as a
/// second parameter that a caller could pair with the wrong config.
pub(crate) trait RoleGroupResolver {
    /// The role whose config this is.
    const ROLE: HdfsNodeRole;

    /// Resolves everything the shared builders cannot derive themselves. Takes the selector
    /// labels because two of the three roles need them to build their listener.
    fn resolve(
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error>;
}

impl RoleGroupResolver for JournalNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Journal;

    fn resolve(
        &self,
        _role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        let (hdfs, vector) = common_container_logging(
            &self.logging,
            JournalNodeContainer::Hdfs,
            JournalNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(self),
            role: RoleSpecificResources::Journal,
            logging: RoleGroupLogging {
                hdfs,
                vector,
                role: RoleContainerLogging::Journal,
            },
        })
    }
}

impl RoleGroupResolver for NameNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Name;

    fn resolve(
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        // Namenodes get their listener from a persistent volume claim template, for stable
        // per-pod identity, rather than from an ephemeral volume.
        let volume_claim_templates =
            ContainerConfig::namenode_volume_claim_templates(self, &selector_labels).context(
                VolumeClaimTemplatesSnafu {
                    role: Self::ROLE,
                    role_group: role_group_name.clone(),
                },
            )?;

        let (hdfs, vector) = common_container_logging(
            &self.logging,
            NameNodeContainer::Hdfs,
            NameNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates,
            role: RoleSpecificResources::Name,
            logging: RoleGroupLogging {
                hdfs,
                vector,
                role: RoleContainerLogging::Name {
                    zkfc: self
                        .logging
                        .for_container(&NameNodeContainer::Zkfc)
                        .into_owned(),
                    format_namenodes: self
                        .logging
                        .for_container(&NameNodeContainer::FormatNameNodes)
                        .into_owned(),
                    format_zookeeper: self
                        .logging
                        .for_container(&NameNodeContainer::FormatZooKeeper)
                        .into_owned(),
                },
            },
        })
    }
}

impl RoleGroupResolver for DataNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Data;

    fn resolve(
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        // Datanodes use an ephemeral listener volume, since they need no stable per-pod identity.
        let listener_volume = ContainerConfig::datanode_listener_volume(self, &selector_labels)
            .context(ListenerVolumeSnafu {
                role: Self::ROLE,
                role_group: role_group_name.clone(),
            })?;

        let (hdfs, vector) = common_container_logging(
            &self.logging,
            DataNodeContainer::Hdfs,
            DataNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(self),
            role: RoleSpecificResources::Data {
                listener_volume,
                storage: self.resources.storage.clone(),
            },
            logging: RoleGroupLogging {
                hdfs,
                vector,
                role: RoleContainerLogging::Data {
                    wait_for_namenodes: self
                        .logging
                        .for_container(&DataNodeContainer::WaitForNameNodes)
                        .into_owned(),
                },
            },
        })
    }
}

/// The resources built for the role groups of one role, accumulated across the roles by
/// [`build`].
#[derive(Default)]
struct RoleGroupResources {
    services: Vec<Service>,
    config_maps: Vec<ConfigMap>,
    /// Keyed by role so that flattening the map yields the StatefulSets in rollout order,
    /// whatever order [`build`] happens to call [`build_role`] in. See [`HdfsNodeRole`], whose
    /// variant order defines that rollout order.
    stateful_sets: BTreeMap<HdfsNodeRole, Vec<StatefulSet>>,
    pod_disruption_budgets: Vec<PodDisruptionBudget>,
}

/// Builds every resource of every role group of one role, plus that role's PDB, appending them to
/// `out`.
fn build_role<C: RoleGroupResolver>(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role_group_configs: &BTreeMap<
        RoleGroupName,
        RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
    out: &mut RoleGroupResources,
) -> Result<(), Error> {
    let role = &C::ROLE;

    for (role_group_name, rg_config) in role_group_configs {
        build_role_group_services(cluster, role, role_group_name, &mut out.services)?;

        let selector_labels = rolegroup_selector_labels(cluster, role, role_group_name).context(
            RoleGroupSelectorLabelsSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            },
        )?;
        let resolved = rg_config.config.resolve(role_group_name, selector_labels)?;

        out.config_maps.push(
            resource::config_map::build_rolegroup_config_map(
                cluster,
                cluster_info,
                role_group_name,
                rg_config,
                &resolved,
            )
            .context(ConfigMapSnafu {
                role: *role,
                role_group: role_group_name.clone(),
            })?,
        );
        out.stateful_sets.entry(C::ROLE).or_default().push(
            resource::statefulset::build_rolegroup_statefulset(
                cluster,
                cluster_info,
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

    if let Some(pdb) = resource::pdb::build_pdb(cluster, role) {
        out.pod_disruption_budgets.push(pdb);
    }

    Ok(())
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
/// [`crate::controller::apply::Applier::apply`]). That ordering is structural: they are
/// accumulated in a [`BTreeMap`] keyed by [`HdfsNodeRole`] and flattened in key order.
/// The discovery `ConfigMap` is included when it can be built or re-emitted (see
/// [`resource::discovery::build_discovery_config_map`]); it is only absent before its first
/// successful build.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources<Prepared>, Error> {
    let mut built = RoleGroupResources::default();

    // The rollout order of the StatefulSets is load-bearing: the apply step rolls them out
    // journalnodes first, then namenodes, then datanodes, each role gated on the previous one
    // (see [`crate::controller::apply::Applier::apply`]). That order comes from the
    // `HdfsNodeRole` key of `RoleGroupResources::stateful_sets`, not from the order of the calls
    // below, which are free to be rearranged.
    build_role(
        cluster,
        cluster_info,
        &cluster.journalnode_role_group_configs,
        &mut built,
    )?;
    build_role(
        cluster,
        cluster_info,
        &cluster.namenode_role_group_configs,
        &mut built,
    )?;
    build_role(
        cluster,
        cluster_info,
        &cluster.datanode_role_group_configs,
        &mut built,
    )?;

    let RoleGroupResources {
        services,
        mut config_maps,
        stateful_sets,
        pod_disruption_budgets,
    } = built;

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
        // `BTreeMap` iterates in key order, so this is the rollout order the apply step needs.
        stateful_sets: stateful_sets.into_values().flatten().collect(),
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

/// The replica count a role group gets when it does not set one: Kubernetes runs a single pod for
/// a `StatefulSet` with `replicas: null`.
pub(crate) const DEFAULT_REPLICAS: u16 = 1;

/// The replica count of every role group in the map, defaulting to [`DEFAULT_REPLICAS`] where it
/// is unset.
fn role_group_replicas<C>(
    role_group_configs: &BTreeMap<
        RoleGroupName,
        RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
) -> Vec<(&RoleGroupName, u16)> {
    role_group_configs
        .iter()
        .map(|(role_group_name, role_group)| {
            (
                role_group_name,
                role_group.replicas.unwrap_or(DEFAULT_REPLICAS),
            )
        })
        .collect()
}

/// The total number of replicas across the role groups of one role, counting a role group without
/// an explicit replica count as [`DEFAULT_REPLICAS`].
pub(crate) fn total_replicas<C>(
    role_group_configs: &BTreeMap<
        RoleGroupName,
        RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
) -> u16 {
    role_group_configs
        .values()
        .map(|role_group| role_group.replicas.unwrap_or(DEFAULT_REPLICAS))
        .sum()
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
    total_replicas(&cluster.datanode_role_group_configs)
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
    use stackable_operator::{k8s_openapi::api::core::v1::ConfigMap, kube::Resource};

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

    /// The named role group `ConfigMap`.
    fn config_map<'a>(config_maps: &'a [ConfigMap], name: &str) -> &'a ConfigMap {
        config_maps
            .iter()
            .find(|config_map| config_map.meta().name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("the {name} ConfigMap is built"))
    }

    /// The sorted data keys of the named role group `ConfigMap`.
    fn config_map_keys(config_maps: &[ConfigMap], name: &str) -> Vec<String> {
        let mut keys: Vec<String> = config_map(config_maps, name)
            .data
            .as_ref()
            .unwrap_or_else(|| panic!("the {name} ConfigMap has data"))
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    /// Each role gets the `log4j.properties` of exactly its own containers: the namenode's
    /// `zkfc`, `format-namenodes` and `format-zookeeper`, the datanode's `wait-for-namenodes`,
    /// and nothing role-specific for the journalnode. A missing file here is invisible at
    /// runtime — the container just falls back to Hadoop's built-in logging and Vector collects
    /// nothing for it — so it is pinned here.
    #[test]
    fn each_role_gets_only_its_own_log4j_configs() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");
        let config_maps = &resources.config_maps;

        assert_eq!(
            config_map_keys(config_maps, "hdfs-journalnode-default"),
            [
                "core-site.xml",
                "hadoop-policy.xml",
                "hdfs-site.xml",
                "hdfs.log4j.properties",
                "security.properties",
                "ssl-client.xml",
                "ssl-server.xml",
            ]
        );
        assert_eq!(
            config_map_keys(config_maps, "hdfs-namenode-default"),
            [
                "core-site.xml",
                "format-namenodes.log4j.properties",
                "format-zookeeper.log4j.properties",
                "hadoop-policy.xml",
                "hdfs-site.xml",
                "hdfs.log4j.properties",
                "security.properties",
                "ssl-client.xml",
                "ssl-server.xml",
                "zkfc.log4j.properties",
            ]
        );
        assert_eq!(
            config_map_keys(config_maps, "hdfs-datanode-default"),
            [
                "core-site.xml",
                "hadoop-policy.xml",
                "hdfs-site.xml",
                "hdfs.log4j.properties",
                "security.properties",
                "ssl-client.xml",
                "ssl-server.xml",
                "wait-for-namenodes.log4j.properties",
            ]
        );
    }

    /// `dfs.datanode.data.dir` must appear in the datanode's `hdfs-site.xml` and nowhere else.
    /// Losing it is silent: the datanodes fall back to Hadoop's default directory, which is
    /// container-local, so their blocks are gone on the next restart.
    #[test]
    fn only_the_datanode_config_map_sets_the_datanode_data_dir() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        for (name, expected) in [
            ("hdfs-datanode-default", true),
            ("hdfs-namenode-default", false),
            ("hdfs-journalnode-default", false),
        ] {
            let hdfs_site = config_map(&resources.config_maps, name)
                .data
                .as_ref()
                .and_then(|data| data.get("hdfs-site.xml"))
                .unwrap_or_else(|| panic!("the {name} ConfigMap has an hdfs-site.xml"));

            assert_eq!(
                hdfs_site.contains("dfs.datanode.data.dir"),
                expected,
                "{name}'s hdfs-site.xml should {}contain dfs.datanode.data.dir",
                if expected { "" } else { "not " }
            );
        }
    }

    /// Datanodes get their listener from an ephemeral pod volume; namenodes get theirs from a
    /// volume claim template, for stable per-pod identity; journalnodes have no listener at all.
    /// Both at once for one role would mean a pod volume and a claim template of the same name,
    /// which the API server rejects at apply time.
    #[test]
    fn the_listener_is_a_pod_volume_for_datanodes_and_a_claim_template_for_namenodes() {
        let cluster = validated_cluster();
        let resources = build(&cluster, &cluster_info()).expect("build succeeds");

        for (name, expect_pod_volume, expect_claim_template) in [
            ("hdfs-datanode-default", true, false),
            ("hdfs-namenode-default", false, true),
            ("hdfs-journalnode-default", false, false),
        ] {
            let spec = resources
                .stateful_sets
                .iter()
                .find(|stateful_set| stateful_set.meta().name.as_deref() == Some(name))
                .and_then(|stateful_set| stateful_set.spec.as_ref())
                .unwrap_or_else(|| panic!("the {name} StatefulSet is built"));

            let has_pod_volume = spec
                .template
                .spec
                .as_ref()
                .and_then(|pod_spec| pod_spec.volumes.as_ref())
                .is_some_and(|volumes| volumes.iter().any(|volume| volume.name == "listener"));
            let has_claim_template = spec.volume_claim_templates.as_ref().is_some_and(|claims| {
                claims
                    .iter()
                    .any(|claim| claim.meta().name.as_deref() == Some("listener"))
            });

            assert_eq!(
                has_pod_volume, expect_pod_volume,
                "{name} listener pod volume"
            );
            assert_eq!(
                has_claim_template, expect_claim_template,
                "{name} listener volume claim template"
            );
        }
    }
}
