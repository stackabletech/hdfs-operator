use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Display,
    num::TryFromIntError,
    ops::Deref,
};

use futures::future::try_join_all;
use product_config::types::PropertyNameKind;
use security::AuthorizationConfig;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        listener::Listener,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::{
        api::core::v1::{Pod, PodTemplateSpec},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    kvp::{LabelError, Labels},
    product_config_utils::{Configuration, Error as ConfigError},
    product_logging::{
        self,
        spec::{ContainerLogConfig, Logging},
    },
    role_utils::{
        GenericProductSpecificCommonConfig, GenericRoleConfig, Role, RoleGroup, RoleGroupRef,
    },
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

use crate::{
    affinity::get_affinity,
    constants::*,
    security::{AuthenticationConfig, KerberosConfig},
    storage::{
        DataNodePvcFragment, DataNodeStorageConfigInnerType, HdfsStorageConfig,
        HdfsStorageConfigFragment, HdfsStorageType,
    },
};

#[cfg(doc)]
use stackable_operator::commons::listener::ListenerClass;

pub mod affinity;
pub mod constants;
pub mod security;
pub mod storage;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no associated namespace"))]
    NoNamespace,

    #[snafu(display("missing node role {role:?}"))]
    MissingRole { role: String },

    #[snafu(display("missing role group {role_group:?} for role {role:?}"))]
    MissingRoleGroup { role: String, role_group: String },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("unable to get {listener} (for {pod})"))]
    GetPodListener {
        source: stackable_operator::client::Error,
        listener: ObjectRef<Listener>,
        pod: ObjectRef<Pod>,
    },

    #[snafu(display("{listener} (for {pod}) has no address"))]
    PodListenerHasNoAddress {
        listener: ObjectRef<Listener>,
        pod: ObjectRef<Pod>,
    },

    #[snafu(display("port {port} ({port_name:?}) is out of bounds, must be within {range:?}", range = 0..=u16::MAX))]
    PortOutOfBounds {
        source: TryFromIntError,
        port_name: String,
        port: i32,
    },

    #[snafu(display("failed to build role-group selector label"))]
    BuildRoleGroupSelectorLabel { source: LabelError },
}

/// An HDFS cluster stacklet. This resource is managed by the Stackable operator for Apache Hadoop HDFS.
/// Find more information on how to use it and the resources that the operator generates in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hdfs/).
///
/// The CRD contains three roles: `nameNodes`, `dataNodes` and `journalNodes`.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hdfs.stackable.tech",
    version = "v1alpha1",
    kind = "HdfsCluster",
    plural = "hdfsclusters",
    shortname = "hdfs",
    status = "HdfsClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterSpec {
    /// Configuration that applies to all roles and role groups.
    /// This includes settings for authentication, logging and the ZooKeeper cluster to use.
    pub cluster_config: HdfsClusterConfig,

    // no doc string - See ProductImage struct
    pub image: ProductImage,

    // no doc string - See ClusterOperation struct
    #[serde(default)]
    pub cluster_operation: ClusterOperation,

    // no doc string - See Role struct
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_nodes: Option<Role<NameNodeConfigFragment>>,

    // no doc string - See Role struct
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_nodes: Option<Role<DataNodeConfigFragment>>,

    // no doc string - See Role struct
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_nodes: Option<Role<JournalNodeConfigFragment>>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterConfig {
    /// `dfsReplication` is the factor of how many times a file will be replicated to different data nodes.
    /// The default is 3.
    /// You need at least the same amount of data nodes so each file can be replicated correctly, otherwise a warning will be printed.
    #[serde(default = "default_dfs_replication_factor")]
    pub dfs_replication: u8,

    /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
    /// to learn how to configure log aggregation with Vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
    /// for a ZooKeeper cluster.
    pub zookeeper_config_map_name: String,

    /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/usage-guide/security).
    pub authentication: Option<AuthenticationConfig>,

    /// Authorization options for HDFS.
    /// Learn more in the [HDFS authorization usage guide](DOCS_BASE_URL_PLACEHOLDER/hdfs/usage-guide/security#authorization).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization: Option<AuthorizationConfig>,

    // Scheduled for removal in v1alpha2, see https://github.com/stackabletech/issues/issues/504
    /// Deprecated, please use `.spec.nameNodes.config.listenerClass` and `.spec.dataNodes.config.listenerClass` instead.
    #[serde(default)]
    pub listener_class: DeprecatedClusterListenerClass,

    /// Configuration to control HDFS topology (rack) awareness feature
    pub rack_awareness: Option<Vec<TopologyLabel>>,
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
#[derive(Debug)]
pub enum AnyNodeConfig {
    NameNode(NameNodeConfig),
    DataNode(DataNodeConfig),
    JournalNode(JournalNodeConfig),
}

impl Deref for AnyNodeConfig {
    type Target = CommonNodeConfig;
    fn deref(&self) -> &Self::Target {
        match self {
            AnyNodeConfig::NameNode(node) => &node.common,
            AnyNodeConfig::DataNode(node) => &node.common,
            AnyNodeConfig::JournalNode(node) => &node.common,
        }
    }
}

impl AnyNodeConfig {
    // Downcasting helpers for each variant
    pub fn as_namenode(&self) -> Option<&NameNodeConfig> {
        if let Self::NameNode(node) = self {
            Some(node)
        } else {
            None
        }
    }
    pub fn as_datanode(&self) -> Option<&DataNodeConfig> {
        if let Self::DataNode(node) = self {
            Some(node)
        } else {
            None
        }
    }
    pub fn as_journalnode(&self) -> Option<&JournalNodeConfig> {
        if let Self::JournalNode(node) = self {
            Some(node)
        } else {
            None
        }
    }

    // Logging config is distinct between each role, due to the different enum types,
    // so provide helpers for containers that are common between all roles.
    pub fn hdfs_logging(&self) -> Cow<ContainerLogConfig> {
        match self {
            AnyNodeConfig::NameNode(node) => node.logging.for_container(&NameNodeContainer::Hdfs),
            AnyNodeConfig::DataNode(node) => node.logging.for_container(&DataNodeContainer::Hdfs),
            AnyNodeConfig::JournalNode(node) => {
                node.logging.for_container(&JournalNodeContainer::Hdfs)
            }
        }
    }
    pub fn vector_logging(&self) -> Cow<ContainerLogConfig> {
        match &self {
            AnyNodeConfig::NameNode(node) => node.logging.for_container(&NameNodeContainer::Vector),
            AnyNodeConfig::DataNode(node) => node.logging.for_container(&DataNodeContainer::Vector),
            AnyNodeConfig::JournalNode(node) => {
                node.logging.for_container(&JournalNodeContainer::Vector)
            }
        }
    }
    pub fn vector_logging_enabled(&self) -> bool {
        match self {
            AnyNodeConfig::NameNode(node) => node.logging.enable_vector_agent,
            AnyNodeConfig::DataNode(node) => node.logging.enable_vector_agent,
            AnyNodeConfig::JournalNode(node) => node.logging.enable_vector_agent,
        }
    }
    pub fn requested_secret_lifetime(&self) -> Option<Duration> {
        match self {
            AnyNodeConfig::NameNode(node) => node.common.requested_secret_lifetime,
            AnyNodeConfig::DataNode(node) => node.common.requested_secret_lifetime,
            AnyNodeConfig::JournalNode(node) => node.common.requested_secret_lifetime,
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
    PartialEq,
    Serialize,
)]
pub enum HdfsRole {
    #[serde(rename = "journalnode")]
    #[strum(serialize = "journalnode")]
    JournalNode,
    #[serde(rename = "namenode")]
    #[strum(serialize = "namenode")]
    NameNode,
    #[serde(rename = "datanode")]
    #[strum(serialize = "datanode")]
    DataNode,
}

impl HdfsRole {
    pub fn min_replicas(&self) -> u16 {
        match self {
            HdfsRole::NameNode => 2,
            HdfsRole::DataNode => 1,
            HdfsRole::JournalNode => 3,
        }
    }

    pub fn replicas_can_be_even(&self) -> bool {
        match self {
            HdfsRole::NameNode => true,
            HdfsRole::DataNode => true,
            HdfsRole::JournalNode => false,
        }
    }

    pub fn check_valid_dfs_replication(&self) -> bool {
        match self {
            HdfsRole::NameNode => false,
            HdfsRole::DataNode => true,
            HdfsRole::JournalNode => false,
        }
    }

    /// Merge the [Name|Data|Journal]NodeConfigFragment defaults, role and role group settings.
    /// The priority is: default < role config < role_group config
    pub fn merged_config(
        &self,
        hdfs: &HdfsCluster,
        role_group: &str,
    ) -> Result<AnyNodeConfig, Error> {
        match self {
            HdfsRole::NameNode => {
                let default_config = NameNodeConfigFragment::default_config(&hdfs.name_any(), self);
                let role = hdfs
                    .spec
                    .name_nodes
                    .as_ref()
                    .with_context(|| MissingRoleSnafu {
                        role: HdfsRole::NameNode.to_string(),
                    })?;

                let mut role_config = role.config.config.clone();
                let mut role_group_config = hdfs
                    .namenode_rolegroup(role_group)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: HdfsRole::NameNode.to_string(),
                        role_group: role_group.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyNodeConfig::NameNode(
                    fragment::validate::<NameNodeConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
            HdfsRole::DataNode => {
                let default_config = DataNodeConfigFragment::default_config(&hdfs.name_any(), self);
                let role = hdfs
                    .spec
                    .data_nodes
                    .as_ref()
                    .with_context(|| MissingRoleSnafu {
                        role: HdfsRole::DataNode.to_string(),
                    })?;

                let mut role_config = role.config.config.clone();
                let mut role_group_config = hdfs
                    .datanode_rolegroup(role_group)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: HdfsRole::DataNode.to_string(),
                        role_group: role_group.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyNodeConfig::DataNode(
                    fragment::validate::<DataNodeConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
            HdfsRole::JournalNode => {
                let default_config =
                    JournalNodeConfigFragment::default_config(&hdfs.name_any(), self);
                let role = hdfs
                    .spec
                    .journal_nodes
                    .as_ref()
                    .with_context(|| MissingRoleSnafu {
                        role: HdfsRole::JournalNode.to_string(),
                    })?;

                let mut role_config = role.config.config.clone();
                let mut role_group_config = hdfs
                    .journalnode_rolegroup(role_group)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: HdfsRole::JournalNode.to_string(),
                        role_group: role_group.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyNodeConfig::JournalNode(
                    fragment::validate::<JournalNodeConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
        }
    }

    /// Name of the Hadoop process HADOOP_OPTS.
    pub fn hadoop_opts_env_var_for_role(&self) -> &'static str {
        match self {
            HdfsRole::NameNode => "HDFS_NAMENODE_OPTS",
            HdfsRole::DataNode => "HDFS_DATANODE_OPTS",
            HdfsRole::JournalNode => "HDFS_JOURNALNODE_OPTS",
        }
    }

    pub fn kerberos_service_name(&self) -> &'static str {
        match self {
            HdfsRole::NameNode => "nn",
            HdfsRole::DataNode => "dn",
            HdfsRole::JournalNode => "jn",
        }
    }

    /// Return replicas for a certain rolegroup.
    pub fn role_group_replicas(&self, hdfs: &HdfsCluster, role_group: &str) -> Option<u16> {
        match self {
            HdfsRole::NameNode => hdfs
                .namenode_rolegroup(role_group)
                .and_then(|rg| rg.replicas),
            HdfsRole::DataNode => hdfs
                .datanode_rolegroup(role_group)
                .and_then(|rg| rg.replicas),
            HdfsRole::JournalNode => hdfs
                .journalnode_rolegroup(role_group)
                .and_then(|rg| rg.replicas),
        }
    }
}

impl HdfsCluster {
    /// Return the namespace of the cluster or an error in case it is not set.
    pub fn namespace_or_error(&self) -> Result<String, Error> {
        self.namespace().context(NoNamespaceSnafu)
    }

    /// Kubernetes labels to attach to Pods within a role group.
    ///
    /// The same labels are also used as selectors for Services and StatefulSets.
    pub fn rolegroup_selector_labels(
        &self,
        rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    ) -> Result<Labels> {
        let mut group_labels = Labels::role_group_selector(
            self,
            APP_NAME,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        )
        .context(BuildRoleGroupSelectorLabelSnafu)?;
        group_labels
            .parse_insert(("role", rolegroup_ref.role.deref()))
            .context(BuildRoleGroupSelectorLabelSnafu)?;
        group_labels
            .parse_insert(("group", rolegroup_ref.role_group.deref()))
            .context(BuildRoleGroupSelectorLabelSnafu)?;

        Ok(group_labels)
    }

    /// Get a reference to the namenode [`RoleGroup`] struct if it exists.
    pub fn namenode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<NameNodeConfigFragment, GenericProductSpecificCommonConfig>> {
        self.spec.name_nodes.as_ref()?.role_groups.get(role_group)
    }

    /// Get a reference to the datanode [`RoleGroup`] struct if it exists.
    pub fn datanode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<DataNodeConfigFragment, GenericProductSpecificCommonConfig>> {
        self.spec.data_nodes.as_ref()?.role_groups.get(role_group)
    }

    /// Get a reference to the journalnode [`RoleGroup`] struct if it exists.
    pub fn journalnode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<JournalNodeConfigFragment, GenericProductSpecificCommonConfig>> {
        self.spec
            .journal_nodes
            .as_ref()?
            .role_groups
            .get(role_group)
    }

    pub fn role_config(&self, role: &HdfsRole) -> Option<&GenericRoleConfig> {
        match role {
            HdfsRole::NameNode => self.spec.name_nodes.as_ref().map(|nn| &nn.role_config),
            HdfsRole::DataNode => self.spec.data_nodes.as_ref().map(|dn| &dn.role_config),
            HdfsRole::JournalNode => self.spec.journal_nodes.as_ref().map(|jn| &jn.role_config),
        }
    }

    pub fn pod_overrides_for_role(&self, role: &HdfsRole) -> Option<&PodTemplateSpec> {
        match role {
            HdfsRole::NameNode => self
                .spec
                .name_nodes
                .as_ref()
                .map(|n| &n.config.pod_overrides),
            HdfsRole::DataNode => self
                .spec
                .data_nodes
                .as_ref()
                .map(|n| &n.config.pod_overrides),
            HdfsRole::JournalNode => self
                .spec
                .journal_nodes
                .as_ref()
                .map(|n| &n.config.pod_overrides),
        }
    }

    pub fn pod_overrides_for_role_group(
        &self,
        role: &HdfsRole,
        role_group: &str,
    ) -> Option<&PodTemplateSpec> {
        match role {
            HdfsRole::NameNode => self
                .namenode_rolegroup(role_group)
                .map(|r| &r.config.pod_overrides),
            HdfsRole::DataNode => self
                .datanode_rolegroup(role_group)
                .map(|r| &r.config.pod_overrides),
            HdfsRole::JournalNode => self
                .journalnode_rolegroup(role_group)
                .map(|r| &r.config.pod_overrides),
        }
    }

    pub fn rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<HdfsCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    /// List all [`HdfsPodRef`]s expected for the given [`role`](HdfsRole).
    ///
    /// The `validated_config` is used to extract the ports exposed by the pods.
    ///
    /// The pod refs returned by `pod_refs` will only be able to able to access HDFS
    /// from inside the Kubernetes cluster. For configuring downstream clients,
    /// consider using [`Self::namenode_listener_refs`] instead.
    pub fn pod_refs(&self, role: &HdfsRole) -> Result<Vec<HdfsPodRef>, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;

        let rolegroup_ref_and_replicas = self.rolegroup_ref_and_replicas(role);

        Ok(rolegroup_ref_and_replicas
            .iter()
            .flat_map(|(rolegroup_ref, replicas)| {
                let ns = ns.clone();
                (0..*replicas).map(move |i| HdfsPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                    ports: self
                        .ports(role)
                        .iter()
                        .map(|(n, p)| (n.clone(), *p))
                        .collect(),
                    fqdn_override: None,
                })
            })
            .collect())
    }

    /// List all [`HdfsPodRef`]s for the running namenodes, configured to access the cluster via
    /// [Listener] rather than direct [Pod] access.
    ///
    /// This enables access from outside the Kubernetes cluster (if using a [ListenerClass] configured for this).
    ///
    /// This method assumes that all [Listener]s have been created, and may fail while waiting for the cluster to come online.
    /// If this is unacceptable (mainly for configuring the cluster itself), consider [`Self::pod_refs`] instead.
    ///
    /// This method _only_ supports accessing namenodes, since journalnodes are considered internal, and datanodes are registered
    /// dynamically with the namenodes.
    pub async fn namenode_listener_refs(
        &self,
        client: &stackable_operator::client::Client,
    ) -> Result<Vec<HdfsPodRef>, Error> {
        let pod_refs = self.pod_refs(&HdfsRole::NameNode)?;
        try_join_all(pod_refs.into_iter().map(|pod_ref| async {
            let listener_name = format!("{LISTENER_VOLUME_NAME}-{}", pod_ref.pod_name);
            let listener_ref =
                || ObjectRef::<Listener>::new(&listener_name).within(&pod_ref.namespace);
            let pod_obj_ref =
                || ObjectRef::<Pod>::new(&pod_ref.pod_name).within(&pod_ref.namespace);
            let listener = client
                .get::<Listener>(&listener_name, &pod_ref.namespace)
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
                        let port = u16::try_from(port).context(PortOutOfBoundsSnafu {
                            port_name: &port_name,
                            port,
                        })?;
                        Ok((port_name, port))
                    })
                    .collect::<Result<_, _>>()?,
                ..pod_ref
            })
        }))
        .await
    }

    pub fn rolegroup_ref_and_replicas(
        &self,
        role: &HdfsRole,
    ) -> Vec<(RoleGroupRef<HdfsCluster>, u16)> {
        match role {
            HdfsRole::NameNode => self
                .spec
                .name_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsRole::NameNode.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            HdfsRole::DataNode => self
                .spec
                .data_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsRole::DataNode.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
            HdfsRole::JournalNode => self
                .spec
                .journal_nodes
                .iter()
                .flat_map(|role| &role.role_groups)
                // Order rolegroups consistently, to avoid spurious downstream rewrites
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(rolegroup_name, role_group)| {
                    (
                        self.rolegroup_ref(HdfsRole::JournalNode.to_string(), rolegroup_name),
                        role_group.replicas.unwrap_or_default(),
                    )
                })
                .collect(),
        }
    }

    pub fn build_role_properties(
        &self,
    ) -> Result<
        HashMap<
            String,
            (
                Vec<PropertyNameKind>,
                Role<impl Configuration<Configurable = HdfsCluster>>,
            ),
        >,
        Error,
    > {
        let mut result = HashMap::new();
        let pnk = vec![
            PropertyNameKind::File(HDFS_SITE_XML.to_string()),
            PropertyNameKind::File(CORE_SITE_XML.to_string()),
            PropertyNameKind::File(HADOOP_POLICY_XML.to_string()),
            PropertyNameKind::File(SSL_SERVER_XML.to_string()),
            PropertyNameKind::File(SSL_CLIENT_XML.to_string()),
            PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
            PropertyNameKind::Env,
        ];

        if let Some(name_nodes) = &self.spec.name_nodes {
            result.insert(
                HdfsRole::NameNode.to_string(),
                (pnk.clone(), name_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingRole {
                role: HdfsRole::NameNode.to_string(),
            });
        }

        if let Some(data_nodes) = &self.spec.data_nodes {
            result.insert(
                HdfsRole::DataNode.to_string(),
                (pnk.clone(), data_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingRole {
                role: HdfsRole::DataNode.to_string(),
            });
        }

        if let Some(journal_nodes) = &self.spec.journal_nodes {
            result.insert(
                HdfsRole::JournalNode.to_string(),
                (pnk, journal_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingRole {
                role: HdfsRole::JournalNode.to_string(),
            });
        }

        Ok(result)
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

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_config().is_some()
    }

    pub fn kerberos_config(&self) -> Option<&KerberosConfig> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|s| &s.kerberos)
    }

    pub fn has_https_enabled(&self) -> bool {
        self.https_secret_class().is_some()
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

    pub fn https_secret_class(&self) -> Option<&str> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|k| k.tls_secret_class.as_str())
    }

    pub fn has_authorization_enabled(&self) -> bool {
        self.spec.cluster_config.authorization.is_some()
    }

    pub fn num_datanodes(&self) -> u16 {
        self.spec
            .data_nodes
            .iter()
            .flat_map(|dn| dn.role_groups.values())
            .map(|rg| rg.replicas.unwrap_or(1))
            .sum()
    }

    /// Returns required port name and port number tuples depending on the role.
    pub fn ports(&self, role: &HdfsRole) -> Vec<(String, u16)> {
        match role {
            HdfsRole::NameNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_NAME_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    DEFAULT_NAME_NODE_RPC_PORT,
                ),
                if self.has_https_enabled() {
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
            HdfsRole::DataNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_DATA_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_DATA),
                    DEFAULT_DATA_NODE_DATA_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_IPC),
                    DEFAULT_DATA_NODE_IPC_PORT,
                ),
                if self.has_https_enabled() {
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
            HdfsRole::JournalNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_JOURNAL_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    DEFAULT_JOURNAL_NODE_RPC_PORT,
                ),
                if self.has_https_enabled() {
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
}
/// Reference to a single `Pod` that is a component of a [`HdfsCluster`]
///
/// Used for service discovery.
pub struct HdfsPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
    pub fqdn_override: Option<String>,
    pub ports: HashMap<String, u16>,
}

impl HdfsPodRef {
    pub fn fqdn(&self, cluster_info: &KubernetesClusterInfo) -> Cow<str> {
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
    #[snafu(display("requested version {requested_version:?} while still upgrading from {deployed_version:?} to {upgrading_version:?}, please finish the upgrade or downgrade first"))]
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

    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
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

impl Configuration for NameNodeConfigFragment {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        // If rack awareness is configured, insert the labels into an env var to configure
        // the topology-provider and add the artifact to the classpath.
        // This is only needed on namenodes.
        if role_name == HdfsRole::NameNode.to_string() {
            if let Some(awareness_config) = resource.rackawareness_config() {
                result.insert("TOPOLOGY_LABELS".to_string(), Some(awareness_config));
            }
        }
        Ok(result)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();
        if file == HDFS_SITE_XML {
            config.insert(
                DFS_REPLICATION.to_string(),
                Some(resource.spec.cluster_config.dfs_replication.to_string()),
            );
        } else if file == CORE_SITE_XML && role_name == HdfsRole::NameNode.to_string() {
            if let Some(_awareness_config) = resource.rackawareness_config() {
                config.insert(
                    "net.topology.node.switch.mapping.impl".to_string(),
                    Some("tech.stackable.hadoop.StackableTopologyProvider".to_string()),
                );
            }
        }

        Ok(config)
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

    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
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

impl Configuration for DataNodeConfigFragment {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();
        if file == HDFS_SITE_XML {
            config.insert(
                DFS_REPLICATION.to_string(),
                Some(resource.spec.cluster_config.dfs_replication.to_string()),
            );
        }

        Ok(config)
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
    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
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

impl Configuration for JournalNodeConfigFragment {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
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

impl HasStatusCondition for HdfsCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

// TODO: upstream?
pub trait LoggingExt {
    type Container;
    fn for_container(&self, container: &Self::Container) -> Cow<ContainerLogConfig>;
}
impl<T> LoggingExt for Logging<T>
where
    T: Ord + Clone + Display,
{
    type Container = T;

    fn for_container(&self, container: &Self::Container) -> Cow<ContainerLogConfig> {
        self.containers
            .get(container)
            .map(Cow::Borrowed)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod test {
    use crate::storage::HdfsStorageType;

    use super::{HdfsCluster, HdfsRole};
    use stackable_operator::k8s_openapi::{
        api::core::v1::ResourceRequirements, apimachinery::pkg::api::resource::Quantity,
    };

    #[test]
    pub fn test_pvc_rolegroup_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
spec:
  image:
    productVersion: 3.4.0
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let config = &role.merged_config(&hdfs, "default").unwrap();
        let resources = &config.as_datanode().unwrap().resources;
        let pvc = resources.storage.get("data").unwrap();

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
spec:
  image:
    productVersion: 3.4.0
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let config = &role.merged_config(&hdfs, "default").unwrap();
        let resources = &config.as_datanode().unwrap().resources;
        let pvc = resources.storage.get("data").unwrap();

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
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default:
        replicas: 1
";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let config = role.merged_config(&hdfs, "default").unwrap();
        let resources = &config.as_datanode().unwrap().resources;
        let pvc = resources.storage.get("data").unwrap();

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
spec:
  image:
    productVersion: 3.4.0
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

        let deserializer = serde_yaml::Deserializer::from_str(cr);
        let hdfs: HdfsCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let role = HdfsRole::DataNode;
        let config = &role.merged_config(&hdfs, "default").unwrap();
        let resources = &config.as_datanode().unwrap().resources;

        let pvc = resources.storage.get("data").unwrap();
        assert_eq!(pvc.count, 0);

        let pvc = resources.storage.get("my-disks").unwrap();
        assert_eq!(pvc.count, 5);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("100Gi".to_string())));
        assert_eq!(pvc.pvc.storage_class, None);

        let pvc = resources.storage.get("my-ssds").unwrap();
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
spec:
  image:
    productVersion: 3.4.0
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let rr: ResourceRequirements = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .as_datanode()
            .unwrap()
            .resources
            .clone()
            .into();

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
spec:
  image:
    productVersion: 3.4.0
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let rr: ResourceRequirements = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .as_datanode()
            .unwrap()
            .resources
            .clone()
            .into();

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
spec:
  image:
    productVersion: 3.4.0
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();

        assert_eq!(hdfs.num_datanodes(), 45);
    }

    #[test]
    pub fn test_rack_awareness_from_yaml() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
spec:
  image:
    productVersion: 3.4.0
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
        let hdfs: HdfsCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let rack_awareness = hdfs.rackawareness_config();
        // test the expected value to be used as an env-var: the mapper will use this to
        // convert to an HDFS-internal label
        assert_eq!(
            Some("Node:kubernetes.io/zone;Pod:app.kubernetes.io/role-group".to_string()),
            rack_awareness
        );
    }
}
