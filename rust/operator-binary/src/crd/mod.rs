use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    num::TryFromIntError,
    ops::Deref,
};

use futures::future::try_join_all;
use security::AuthorizationConfig;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{Fragment, ValidationError},
        merge::Merge,
    },
    crd::listener,
    deep_merger::ObjectOverrides,
    k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::api::resource::Quantity},
    kube::{CustomResource, runtime::reflector::ObjectRef},
    product_logging::{
        self,
        spec::{ContainerLogConfig, Logging},
    },
    role_utils::{self, GenericRoleConfig, Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        config_overrides::KeyValueConfigOverrides,
        role_utils::JavaCommonConfig,
        types::{common::Port, kubernetes::ConfigMapName},
    },
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::crd::{
    affinity::get_affinity,
    constants::{
        DEFAULT_DATA_NODE_GRACEFUL_SHUTDOWN_TIMEOUT, DEFAULT_DFS_REPLICATION_FACTOR,
        DEFAULT_JOURNAL_NODE_GRACEFUL_SHUTDOWN_TIMEOUT, DEFAULT_LISTENER_CLASS,
        DEFAULT_NAME_NODE_GRACEFUL_SHUTDOWN_TIMEOUT, LISTENER_VOLUME_NAME,
    },
    security::AuthenticationConfig,
    storage::{
        DataNodePvcFragment, DataNodeStorageConfigInnerType, HdfsStorageConfig,
        HdfsStorageConfigFragment, HdfsStorageType,
    },
};

pub mod affinity;
pub mod constants;
pub mod security;
pub mod storage;

pub type NameNodeRoleType = Role<
    NameNodeConfigFragment,
    v1alpha1::HdfsConfigOverrides,
    GenericRoleConfig,
    JavaCommonConfig,
>;

pub type DataNodeRoleType = Role<
    DataNodeConfigFragment,
    v1alpha1::HdfsConfigOverrides,
    GenericRoleConfig,
    JavaCommonConfig,
>;

pub type JournalNodeRoleType = Role<
    JournalNodeConfigFragment,
    v1alpha1::HdfsConfigOverrides,
    GenericRoleConfig,
    JavaCommonConfig,
>;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no associated namespace"))]
    NoNamespace,

    #[snafu(display("missing role {role:?}"))]
    MissingRole { role: String },

    #[snafu(display("missing role group {role_group:?} for role {role:?}"))]
    MissingRoleGroup { role: String, role_group: String },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("unable to get {listener} (for {pod})"))]
    GetPodListener {
        source: stackable_operator::client::Error,
        listener: ObjectRef<listener::v1alpha1::Listener>,
        pod: ObjectRef<Pod>,
    },

    #[snafu(display("{listener} (for {pod}) has no address"))]
    PodListenerHasNoAddress {
        listener: ObjectRef<listener::v1alpha1::Listener>,
        pod: ObjectRef<Pod>,
    },

    #[snafu(display("port {port} ({port_name:?}) is out of bounds, must be within {range:?}", range = 0..=u16::MAX))]
    PortOutOfBounds {
        source: TryFromIntError,
        port_name: String,
        port: i32,
    },

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// An HDFS cluster stacklet. This resource is managed by the Stackable operator for Apache Hadoop HDFS.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hdfs/).
    ///
    /// The CRD contains three roles: `nameNodes`, `dataNodes` and `journalNodes`.
    #[versioned(crd(
        group = "hdfs.stackable.tech",
        kind = "HdfsCluster",
        plural = "hdfsclusters",
        shortname = "hdfs",
        status = "HdfsClusterStatus",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HdfsClusterSpec {
        /// Configuration that applies to all roles and role groups.
        /// This includes settings for authentication, logging and the ZooKeeper cluster to use.
        pub cluster_config: v1alpha1::HdfsClusterConfig,

        // no doc string - See ProductImage struct
        pub image: ProductImage,

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        // no doc string - See ObjectOverrides struct
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        // no doc string - See Role struct
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub name_nodes: Option<NameNodeRoleType>,

        // no doc string - See Role struct
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub data_nodes: Option<DataNodeRoleType>,

        // no doc string - See Role struct
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub journal_nodes: Option<JournalNodeRoleType>,
    }

    #[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HdfsClusterConfig {
        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/usage-guide/security).
        pub authentication: Option<AuthenticationConfig>,

        /// Authorization options for HDFS.
        /// Learn more in the [HDFS authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/hdfs/usage-guide/security#authorization).
        #[serde(skip_serializing_if = "Option::is_none")]
        pub authorization: Option<AuthorizationConfig>,

        /// `dfsReplication` is the factor of how many times a file will be replicated to different data nodes.
        /// The default is 3.
        /// You need at least the same amount of data nodes so each file can be replicated correctly, otherwise a warning will be printed.
        #[serde(default = "default_dfs_replication_factor")]
        pub dfs_replication: u8,

        // Scheduled for removal in v1alpha2, see https://github.com/stackabletech/issues/issues/504
        /// Deprecated, please use `.spec.nameNodes.config.listenerClass` and `.spec.dataNodes.config.listenerClass` instead.
        #[serde(default)]
        pub listener_class: DeprecatedClusterListenerClass,

        /// Configuration to control HDFS topology (rack) awareness feature
        pub rack_awareness: Option<Vec<TopologyLabel>>,

        /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
        /// to learn how to configure log aggregation with Vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<ConfigMapName>,

        /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
        /// for a ZooKeeper cluster.
        pub zookeeper_config_map_name: ConfigMapName,
    }

    #[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, Merge, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HdfsConfigOverrides {
        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "hdfs-site.xml")]
        pub hdfs_site_xml: KeyValueConfigOverrides,

        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "core-site.xml")]
        pub core_site_xml: KeyValueConfigOverrides,

        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "hadoop-policy.xml")]
        pub hadoop_policy_xml: KeyValueConfigOverrides,

        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "ssl-server.xml")]
        pub ssl_server_xml: KeyValueConfigOverrides,

        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "ssl-client.xml")]
        pub ssl_client_xml: KeyValueConfigOverrides,

        // File name defined in [`crate::controller::build::properties::ConfigFileName`]
        #[serde(default, rename = "security.properties")]
        pub security_properties: KeyValueConfigOverrides,
    }
}

impl HasStatusCondition for v1alpha1::HdfsCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha1::HdfsCluster {
    pub fn role_config(&self, hdfs_role: &HdfsNodeRole) -> Option<&GenericRoleConfig> {
        match hdfs_role {
            HdfsNodeRole::Name => self.spec.name_nodes.as_ref().map(|nn| &nn.role_config),
            HdfsNodeRole::Data => self.spec.data_nodes.as_ref().map(|dn| &dn.role_config),
            HdfsNodeRole::Journal => self.spec.journal_nodes.as_ref().map(|jn| &jn.role_config),
        }
    }

    pub fn rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::HdfsCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    pub fn rolegroup_ref_and_replicas(
        &self,
        role: &HdfsNodeRole,
    ) -> Vec<(RoleGroupRef<v1alpha1::HdfsCluster>, u16)> {
        match role {
            HdfsNodeRole::Name => self
                .spec
                .name_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsNodeRole::Name.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            HdfsNodeRole::Data => self
                .spec
                .data_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsNodeRole::Data.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            HdfsNodeRole::Journal => self
                .spec
                .journal_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsNodeRole::Journal.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
        }
    }

    pub fn upgrade_state(&self) -> Result<Option<UpgradeState>, UpgradeStateError> {
        use upgrade_state_error::*;
        let Some(status) = self.status.as_ref() else {
            return Ok(None);
        };
        let requested_version = self.spec.image.product_version();
        let Some(deployed_version) = status.deployed_product_version.as_deref() else {
            // If no deployed version, fresh install -> no upgrade
            return Ok(None);
        };
        let current_upgrade_target_version = status.upgrade_target_product_version.as_deref();

        if requested_version != deployed_version {
            // If we're requesting a different version than what is deployed, assume that we're upgrading.
            // Could also be a downgrade to an older version, but we don't support downgrades after upgrade finalization.
            match current_upgrade_target_version {
                Some(upgrading_version) if requested_version != upgrading_version => {
                    // If we're in an upgrade, do not allow switching to a third version
                    InvalidCrossgradeSnafu {
                        requested_version,
                        deployed_version,
                        upgrading_version,
                    }
                    .fail()
                }
                _ => Ok(Some(UpgradeState::Upgrading)),
            }
        } else if current_upgrade_target_version.is_some_and(|x| requested_version != x) {
            // If we're requesting the old version mid-upgrade, assume that we're downgrading.
            // We only support downgrading to the exact previous version.
            Ok(Some(UpgradeState::Downgrading))
        } else {
            // All three versions match, upgrade was completed without clearing `upgrading_product_version`.
            Ok(None)
        }
    }

    pub fn authentication_config(&self) -> Option<&AuthenticationConfig> {
        self.spec.cluster_config.authentication.as_ref()
    }

    pub fn rackawareness_config(&self) -> Option<String> {
        self.spec
            .cluster_config
            .rack_awareness
            .as_ref()
            .map(|label_list| {
                label_list
                    .iter()
                    .map(TopologyLabel::to_config)
                    .collect::<Vec<_>>()
                    .join(";")
            })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TopologyLabel {
    /// Name of the label on the Kubernetes Node (where the Pod is placed on) used to resolve a datanode to a topology
    /// zone.
    NodeLabel(String),

    /// Name of the label on the Kubernetes Pod used to resolve a datanode to a topology zone.
    PodLabel(String),
}

impl TopologyLabel {
    pub fn to_config(&self) -> String {
        match &self {
            TopologyLabel::NodeLabel(l) => format!("Node:{l}"),
            TopologyLabel::PodLabel(l) => format!("Pod:{l}"),
        }
    }
}

fn default_dfs_replication_factor() -> u8 {
    DEFAULT_DFS_REPLICATION_FACTOR
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DeprecatedClusterListenerClass {
    #[default]
    ClusterInternal,
}

/// Configuration options that are available for all roles.
#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct CommonNodeConfig {
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

/// Configuration for a rolegroup of an unknown type.
#[derive(Clone, Debug)]
pub enum AnyNodeConfig {
    Name(NameNodeConfig),
    Data(DataNodeConfig),
    Journal(JournalNodeConfig),
}

impl Deref for AnyNodeConfig {
    type Target = CommonNodeConfig;

    fn deref(&self) -> &Self::Target {
        match self {
            AnyNodeConfig::Name(node) => &node.common,
            AnyNodeConfig::Data(node) => &node.common,
            AnyNodeConfig::Journal(node) => &node.common,
        }
    }
}

impl AnyNodeConfig {
    // Downcasting helpers for each variant
    pub fn as_namenode(&self) -> Option<&NameNodeConfig> {
        if let Self::Name(node) = self {
            Some(node)
        } else {
            None
        }
    }

    pub fn as_datanode(&self) -> Option<&DataNodeConfig> {
        if let Self::Data(node) = self {
            Some(node)
        } else {
            None
        }
    }

    #[allow(unused)]
    pub fn as_journalnode(&self) -> Option<&JournalNodeConfig> {
        if let Self::Journal(node) = self {
            Some(node)
        } else {
            None
        }
    }

    // Logging config is distinct between each role, due to the different enum types,
    // so provide helpers for containers that are common between all roles.
    pub fn hdfs_logging(&'_ self) -> Cow<'_, ContainerLogConfig> {
        match self {
            AnyNodeConfig::Name(node) => node.logging.for_container(&NameNodeContainer::Hdfs),
            AnyNodeConfig::Data(node) => node.logging.for_container(&DataNodeContainer::Hdfs),
            AnyNodeConfig::Journal(node) => node.logging.for_container(&JournalNodeContainer::Hdfs),
        }
    }

    pub fn vector_logging(&'_ self) -> Cow<'_, ContainerLogConfig> {
        match &self {
            AnyNodeConfig::Name(node) => node.logging.for_container(&NameNodeContainer::Vector),
            AnyNodeConfig::Data(node) => node.logging.for_container(&DataNodeContainer::Vector),
            AnyNodeConfig::Journal(node) => {
                node.logging.for_container(&JournalNodeContainer::Vector)
            }
        }
    }

    pub fn vector_logging_enabled(&self) -> bool {
        match self {
            AnyNodeConfig::Name(node) => node.logging.enable_vector_agent,
            AnyNodeConfig::Data(node) => node.logging.enable_vector_agent,
            AnyNodeConfig::Journal(node) => node.logging.enable_vector_agent,
        }
    }

    pub fn requested_secret_lifetime(&self) -> Option<Duration> {
        match self {
            AnyNodeConfig::Name(node) => node.common.requested_secret_lifetime,
            AnyNodeConfig::Data(node) => node.common.requested_secret_lifetime,
            AnyNodeConfig::Journal(node) => node.common.requested_secret_lifetime,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    EnumString,
    IntoStaticStr,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub enum HdfsNodeRole {
    #[serde(rename = "journalnode")]
    #[strum(serialize = "journalnode")]
    Journal,
    #[serde(rename = "namenode")]
    #[strum(serialize = "namenode")]
    Name,
    #[serde(rename = "datanode")]
    #[strum(serialize = "datanode")]
    Data,
}

impl HdfsNodeRole {
    pub fn min_replicas(&self) -> u16 {
        match self {
            HdfsNodeRole::Name => 2,
            HdfsNodeRole::Data => 1,
            HdfsNodeRole::Journal => 3,
        }
    }

    pub fn replicas_can_be_even(&self) -> bool {
        match self {
            HdfsNodeRole::Name => true,
            HdfsNodeRole::Data => true,
            HdfsNodeRole::Journal => false,
        }
    }

    pub fn check_valid_dfs_replication(&self) -> bool {
        match self {
            HdfsNodeRole::Name => false,
            HdfsNodeRole::Data => true,
            HdfsNodeRole::Journal => false,
        }
    }

    /// Name of the Hadoop process HADOOP_OPTS.
    pub fn hadoop_opts_env_var_for_role(&self) -> &'static str {
        match self {
            HdfsNodeRole::Name => "HDFS_NAMENODE_OPTS",
            HdfsNodeRole::Data => "HDFS_DATANODE_OPTS",
            HdfsNodeRole::Journal => "HDFS_JOURNALNODE_OPTS",
        }
    }

    pub fn kerberos_service_name(&self) -> &'static str {
        match self {
            HdfsNodeRole::Name => "nn",
            HdfsNodeRole::Data => "dn",
            HdfsNodeRole::Journal => "jn",
        }
    }
}

/// Returns the required port name and port number tuples exposed by pods of the
/// given `role`, depending on whether HTTPS is enabled.
/// The rolegroup selector labels for `rolegroup_ref`, owned by `owner` (either the
/// raw `HdfsCluster` or the [`crate::controller::ValidatedCluster`]).
/// Resolve the listener-based [`HdfsPodRef`]s for the given namenode `namenode_podrefs`,
/// configured to access the cluster via [`Listener`](listener::v1alpha1::Listener) rather
/// than direct [`Pod`] access.
///
/// This enables access from outside the Kubernetes cluster (if using a
/// [`listener::v1alpha1::ListenerClass`] configured for this). It assumes that all
/// `Listener`s have been created, and may fail while waiting for the cluster to come online.
///
/// This _only_ supports accessing namenodes, since journalnodes are considered internal,
/// and datanodes are registered dynamically with the namenodes.
pub(crate) async fn namenode_listener_refs(
    client: &stackable_operator::client::Client,
    namenode_podrefs: Vec<HdfsPodRef>,
) -> Result<Vec<HdfsPodRef>, Error> {
    try_join_all(namenode_podrefs.into_iter().map(|pod_ref| async {
        let listener_name = format!("{}-{}", *LISTENER_VOLUME_NAME, pod_ref.pod_name);
        let listener_ref = || {
            ObjectRef::<listener::v1alpha1::Listener>::new(&listener_name)
                .within(&pod_ref.namespace)
        };
        let pod_obj_ref = || ObjectRef::<Pod>::new(&pod_ref.pod_name).within(&pod_ref.namespace);
        let listener = client
            .get::<listener::v1alpha1::Listener>(&listener_name, &pod_ref.namespace)
            .await
            .context(GetPodListenerSnafu {
                listener: listener_ref(),
                pod: pod_obj_ref(),
            })?;
        let listener_address = listener
            .status
            .and_then(|s| s.ingress_addresses?.into_iter().next())
            .context(PodListenerHasNoAddressSnafu {
                listener: listener_ref(),
                pod: pod_obj_ref(),
            })?;
        Ok(HdfsPodRef {
            fqdn_override: Some(listener_address.address),
            ports: listener_address
                .ports
                .into_iter()
                .map(|(port_name, port)| {
                    let port = Port(u16::try_from(port).context(PortOutOfBoundsSnafu {
                        port_name: &port_name,
                        port,
                    })?);
                    Ok((port_name, port))
                })
                .collect::<Result<_, _>>()?,
            ..pod_ref
        })
    }))
    .await
}

/// Reference to a single `Pod` that is a component of a [`HdfsCluster`]
///
/// Used for service discovery.
#[derive(Clone, Debug)]
pub struct HdfsPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
    pub fqdn_override: Option<String>,
    pub ports: HashMap<String, Port>,
}

impl HdfsPodRef {
    pub fn fqdn(&'_ self, cluster_info: &KubernetesClusterInfo) -> Cow<'_, str> {
        self.fqdn_override.as_deref().map_or_else(
            || {
                Cow::Owned(format!(
                    "{pod_name}.{role_group_service_name}.{namespace}.svc.{cluster_domain}",
                    pod_name = self.pod_name,
                    role_group_service_name = self.role_group_service_name,
                    namespace = self.namespace,
                    cluster_domain = cluster_info.cluster_domain,
                ))
            },
            Cow::Borrowed,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeState {
    /// The cluster is currently being upgraded to a new version.
    Upgrading,

    /// The cluster is currently being downgraded to the previous version.
    Downgrading,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum UpgradeStateError {
    #[snafu(display(
        "requested version {requested_version:?} while still upgrading from {deployed_version:?} to {upgrading_version:?}, please finish the upgrade or downgrade first"
    ))]
    InvalidCrossgrade {
        requested_version: String,
        deployed_version: String,
        upgrading_version: String,
    },
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum NameNodeContainer {
    #[strum(serialize = "hdfs")]
    Hdfs,
    #[strum(serialize = "vector")]
    Vector,
    #[strum(serialize = "zkfc")]
    Zkfc,
    #[strum(serialize = "format-namenodes")]
    FormatNameNodes,
    #[strum(serialize = "format-zookeeper")]
    FormatZooKeeper,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct NameNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HdfsStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<NameNodeContainer>,
    /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose this rolegroup.
    /// NameNodes should have a stable ListenerClass, such as `cluster-internal` or `external-stable`.
    #[fragment_attrs(serde(default))]
    pub listener_class: String,
    #[fragment_attrs(serde(flatten))]
    pub common: CommonNodeConfig,
}

impl NameNodeConfigFragment {
    const DEFAULT_NAME_NODE_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(cluster_name: &str, role: &HdfsNodeRole) -> Self {
        Self {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1000m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1024Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HdfsStorageConfigFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("2Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: product_logging::spec::default_logging(),
            listener_class: Some(DEFAULT_LISTENER_CLASS.to_string()),
            common: CommonNodeConfigFragment {
                affinity: get_affinity(cluster_name, role),
                graceful_shutdown_timeout: Some(DEFAULT_NAME_NODE_GRACEFUL_SHUTDOWN_TIMEOUT),
                requested_secret_lifetime: Some(Self::DEFAULT_NAME_NODE_SECRET_LIFETIME),
            },
        }
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum DataNodeContainer {
    #[strum(serialize = "hdfs")]
    Hdfs,
    #[strum(serialize = "vector")]
    Vector,
    #[strum(serialize = "wait-for-namenodes")]
    WaitForNameNodes,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct DataNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<DataNodeStorageConfigInnerType, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<DataNodeContainer>,
    /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose this rolegroup.
    /// DataNodes should have a direct ListenerClass, such as `cluster-internal` or `external-unstable`.
    #[fragment_attrs(serde(default))]
    pub listener_class: String,
    #[fragment_attrs(serde(flatten))]
    pub common: CommonNodeConfig,
}

impl DataNodeConfigFragment {
    const DEFAULT_DATA_NODE_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(cluster_name: &str, role: &HdfsNodeRole) -> Self {
        Self {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("100m".to_owned())),
                    max: Some(Quantity("400m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: BTreeMap::from([(
                    "data".to_string(),
                    DataNodePvcFragment {
                        pvc: PvcConfigFragment {
                            capacity: Some(Quantity("10Gi".to_owned())),
                            storage_class: None,
                            selectors: None,
                        },
                        count: Some(1),
                        hdfs_storage_type: Some(HdfsStorageType::default()),
                    },
                )]),
            },
            logging: product_logging::spec::default_logging(),
            listener_class: Some(DEFAULT_LISTENER_CLASS.to_string()),
            common: CommonNodeConfigFragment {
                affinity: get_affinity(cluster_name, role),
                graceful_shutdown_timeout: Some(DEFAULT_DATA_NODE_GRACEFUL_SHUTDOWN_TIMEOUT),
                requested_secret_lifetime: Some(Self::DEFAULT_DATA_NODE_SECRET_LIFETIME),
            },
        }
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum JournalNodeContainer {
    #[strum(serialize = "hdfs")]
    Hdfs,
    #[strum(serialize = "vector")]
    Vector,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct JournalNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HdfsStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<JournalNodeContainer>,
    #[fragment_attrs(serde(flatten))]
    pub common: CommonNodeConfig,
}

impl JournalNodeConfigFragment {
    const DEFAULT_JOURNAL_NODE_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(cluster_name: &str, role: &HdfsNodeRole) -> Self {
        Self {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("100m".to_owned())),
                    max: Some(Quantity("400m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HdfsStorageConfigFragment {
                    data: PvcConfigFragment {
                        capacity: Some(Quantity("1Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: product_logging::spec::default_logging(),
            common: CommonNodeConfigFragment {
                affinity: get_affinity(cluster_name, role),
                graceful_shutdown_timeout: Some(DEFAULT_JOURNAL_NODE_GRACEFUL_SHUTDOWN_TIMEOUT),
                requested_secret_lifetime: Some(Self::DEFAULT_JOURNAL_NODE_SECRET_LIFETIME),
            },
        }
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,

    /// The product version that the HDFS cluster is currently running.
    ///
    /// During upgrades, this field contains the *old* version.
    pub deployed_product_version: Option<String>,

    /// The product version that is currently being upgraded to, otherwise null.
    pub upgrade_target_product_version: Option<String>,
}

#[cfg(test)]
mod test {
    use stackable_operator::{
        k8s_openapi::{
            api::core::v1::ResourceRequirements, apimachinery::pkg::api::resource::Quantity,
        },
        versioned::test_utils::RoundtripTestData,
    };

    use super::*;
    use crate::{
        controller::build,
        crd::storage::{DataNodePvc, HdfsStorageType},
        test_support::{datanode_config, deserialize_and_validate_cluster},
    };

    fn datanode_pvc<'a>(
        datanode_config: &'a DataNodeConfig,
        storage_name: &str,
    ) -> &'a DataNodePvc {
        datanode_config
            .resources
            .storage
            .get(storage_name)
            .expect("storage should be defined")
    }

    #[test]
    pub fn test_pvc_rolegroup_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default:
        config:
          resources:
            storage:
              data:
                capacity: 10Gi
        replicas: 1
";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");
        let pvc = datanode_pvc(datanode_config, "data");

        assert_eq!(pvc.count, 1);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("10Gi".to_string())));
    }

    #[test]
    pub fn test_pvc_role_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    config:
      resources:
        storage:
          data:
            capacity: 10Gi
    roleGroups:
      default:
        replicas: 1
";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");
        let pvc = datanode_pvc(datanode_config, "data");

        assert_eq!(pvc.count, 1);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("10Gi".to_string())));
    }

    #[test]
    pub fn test_pvc_default_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default:
        replicas: 1
";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");
        let pvc = datanode_pvc(datanode_config, "data");

        assert_eq!(pvc.count, 1);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("10Gi".to_string())));
    }

    #[test]
    pub fn test_pvc_rolegroup_multiple_pvcs_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    rackAwareness:
      - nodeLabel: kubernetes.io/zone
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  dataNodes:
    roleGroups:
      default:
        replicas: 1
        config:
          resources:
            storage:
              data: # We need to overwrite the data pvcs coming from the default value
                count: 0
              my-disks:
                capacity: 100Gi
                count: 5
                hdfsStorageType: Disk
              my-ssds:
                capacity: 10Gi
                storageClass: premium
                count: 3
                hdfsStorageType: SSD
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");

        let pvc = datanode_pvc(datanode_config, "data");
        assert_eq!(pvc.count, 0);

        let pvc = datanode_pvc(datanode_config, "my-disks");
        assert_eq!(pvc.count, 5);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("100Gi".to_string())));
        assert_eq!(pvc.pvc.storage_class, None);

        let pvc = datanode_pvc(datanode_config, "my-ssds");
        assert_eq!(pvc.count, 3);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Ssd);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("10Gi".to_string())));
        assert_eq!(pvc.pvc.storage_class, Some("premium".to_string()));
    }

    #[test]
    pub fn test_rr_role_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    config:
      resources:
        memory:
          limit: '64Mi'
        cpu:
          max: '500m'
          min: '250m'
    roleGroups:
      default:
        replicas: 1
";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");
        let rr = datanode_config.resources.clone().into();

        let expected = ResourceRequirements {
            requests: Some(
                [
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                    ("cpu".to_string(), Quantity("250m".to_string())),
                ]
                .into(),
            ),
            limits: Some(
                [
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                    ("cpu".to_string(), Quantity("500m".to_string())),
                ]
                .into(),
            ),
            ..ResourceRequirements::default()
        };
        assert_eq!(expected, rr);
    }
    #[test]
    pub fn test_rr_rg_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default:
        config:
          resources:
            memory:
              limit: '64Mi'
            cpu:
              max: '500m'
              min: '250m'
";

        let validated_cluster = deserialize_and_validate_cluster(cr);
        let datanode_config = datanode_config(&validated_cluster, "default");
        let rr = datanode_config.resources.clone().into();

        let expected = ResourceRequirements {
            requests: Some(
                [
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                    ("cpu".to_string(), Quantity("250m".to_string())),
                ]
                .into(),
            ),
            limits: Some(
                [
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                    ("cpu".to_string(), Quantity("500m".to_string())),
                ]
                .into(),
            ),
            ..ResourceRequirements::default()
        };
        assert_eq!(expected, rr);
    }

    #[test]
    pub fn test_num_datanodes() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default: {}
      second:
        replicas: 2
      third:
        replicas: 42
";

        let validated_cluster = deserialize_and_validate_cluster(cr);

        assert_eq!(build::num_datanodes(&validated_cluster), 45);
    }

    #[test]
    pub fn test_rack_awareness_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.4.2
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    rackAwareness:
      - nodeLabel: kubernetes.io/zone
      - podLabel: app.kubernetes.io/role-group
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  dataNodes:
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let deserializer = serde_yaml::Deserializer::from_str(cr);
        let hdfs: v1alpha1::HdfsCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let rack_awareness = hdfs.rackawareness_config();
        // test the expected value to be used as an env-var: the mapper will use this to
        // convert to an HDFS-internal label
        assert_eq!(
            Some("Node:kubernetes.io/zone;Pod:app.kubernetes.io/role-group".to_string()),
            rack_awareness
        );
    }

    impl RoundtripTestData for v1alpha1::HdfsClusterSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - image:
                  productVersion: 3.4.2
                  pullPolicy: IfNotPresent
                clusterOperation:
                  reconciliationPaused: false
                  stopped: true
                clusterConfig:
                  zookeeperConfigMapName: hdfs-zk
                  dfsReplication: 1
                  vectorAggregatorConfigMapName: vector-aggregator-discovery
                  authentication:
                    tlsSecretClass: tls
                    kerberos:
                      secretClass: kerberos
                  authorization:
                    opa:
                      configMapName: opa
                      package: hdfs
                  rackAwareness:
                    - nodeLabel: kubernetes.io/zone
                    - podLabel: app.kubernetes.io/role-group
                nameNodes:
                  envOverrides:
                    COMMON_VAR: role-value
                    ROLE_VAR: role-value
                  config:
                    listenerClass: cluster-internal
                    resources:
                      cpu:
                        min: 250m
                        max: "1"
                      memory:
                        limit: 1Gi
                    logging:
                      enableVectorAgent: true
                      containers:
                        hdfs:
                          console:
                            level: INFO
                  roleGroups:
                    default:
                      replicas: 2
                      envOverrides:
                        COMMON_VAR: group-value
                        GROUP_VAR: group-value
                dataNodes:
                  config:
                    listenerClass: cluster-internal
                    logging:
                      enableVectorAgent: true
                  roleGroups:
                    default:
                      replicas: 1
                journalNodes:
                  config:
                    logging:
                      enableVectorAgent: true
                  roleGroups:
                    default:
                      replicas: 1
            "#})
            .expect("Failed to parse HdfsClusterSpec YAML")
        }
    }
}
