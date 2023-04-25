pub mod affinity;
pub mod constants;
pub mod storage;

use affinity::get_affinity;
use constants::*;
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
        fragment,
        fragment::{Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    labels::role_group_selector_labels,
    product_config::types::PropertyNameKind,
    product_config_utils::{ConfigError, Configuration},
    product_logging,
    product_logging::spec::{ContainerLogConfig, Logging},
    role_utils::{Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
};
use std::collections::{BTreeMap, HashMap};
use storage::{
    DataNodePvcFragment, DataNodeStorageConfigInnerType, HdfsStorageConfig,
    HdfsStorageConfigFragment, HdfsStorageType,
};
use strum::{Display, EnumIter, EnumString};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Object has no associated namespace"))]
    NoNamespace,
    #[snafu(display("Missing node role [{role}]"))]
    MissingRole { role: String },
    #[snafu(display("Missing role group [{role_group}] for role [{role}]"))]
    MissingRoleGroup { role: String, role_group: String },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
}

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
    pub image: ProductImage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_nodes: Option<Role<NameNodeConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_nodes: Option<Role<DataNodeConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_nodes: Option<Role<JournalNodeConfigFragment>>,
    pub cluster_config: HdfsClusterConfig,
    /// Cluster operations like pause reconciliation or cluster stop.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterConfig {
    pub auto_format_fs: Option<bool>,
    pub dfs_replication: Option<u8>,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    /// Name of the ZooKeeper discovery config map.
    pub zookeeper_config_map_name: String,
    /// In the future this setting will control, which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service.
    /// Currently only a subset of the ListenerClasses are supported by choosing the type of the created Services
    /// by looking at the ListenerClass name specified,
    /// In a future release support for custom ListenerClasses will be introduced without a breaking change:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
    /// Configuration to set up a cluster secured using Kerberos.
    pub kerberos: Option<KerberosConfig>,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(
    Clone, Debug, Default, Display, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HDFS services.
    #[serde(default = "default_kerberos_kerberos_secret_class")]
    pub kerberos_secret_class: String,
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_kerberos_tls_secret_class")]
    pub tls_secret_class: String,
    /// Wether a principal including the Kubernetes node name should be requested.
    /// The principal could e.g. be `HTTP/my-k8s-worker-0.mycorp.lan`.
    /// This feature is disabled by default, as the resulting principals can already by existent
    /// e.g. in Active Directory which can cause problems.
    #[serde(default)]
    pub request_node_principals: bool,
    /// Configures how communication between hdfs nodes as well as between hdfs clients and cluster are secured.
    /// Possible values are:
    ///
    /// Authentication:
    /// Establishes mutual authentication between the client and the server.
    /// Sets `hadoop.rpc.protection` to `authentication`, `hadoop.data.transfer.protection` to `authentication` and `dfs.encrypt.data.transfer` to `false`.
    ///
    /// Integrity:
    /// In addition to authentication, it guarantees that a man-in-the-middle cannot tamper with messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `integrity`, `hadoop.data.transfer.protection` to `integrity` and `dfs.encrypt.data.transfer` to `false`.
    ///
    /// Privacy:
    /// In addition to the features offered by authentication and integrity, it also fully encrypts the messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `privacy`, `hadoop.data.transfer.protection` to `privacy` and `dfs.encrypt.data.transfer` to `true`.
    ///
    /// Defaults to privacy for best security
    #[serde(default)]
    pub wire_encryption: WireEncryption,
}

fn default_kerberos_tls_secret_class() -> String {
    "tls".to_string()
}

fn default_kerberos_kerberos_secret_class() -> String {
    "kerberos".to_string()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum WireEncryption {
    /// Establishes mutual authentication between the client and the server.
    /// Sets `hadoop.rpc.protection` to `authentication`, `hadoop.data.transfer.protection` to `authentication` and `dfs.encrypt.data.transfer` to `false`.
    Authentication,
    /// In addition to authentication, it guarantees that a man-in-the-middle cannot tamper with messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `integrity`, `hadoop.data.transfer.protection` to `integrity` and `dfs.encrypt.data.transfer` to `false`.
    Integrity,
    /// In addition to the features offered by authentication and integrity, it also fully encrypts the messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `privacy`, `hadoop.data.transfer.protection` to `privacy` and `dfs.encrypt.data.transfer` to `true`.
    #[default]
    Privacy,
}

/// This is a shared trait for all role/role-group config structs to avoid duplication
/// when extracting role specific configuration structs like resources or logging.
pub trait MergedConfig {
    /// Resources shared by all roles (except datanodes).
    /// DataNodes must use `data_node_resources`
    fn resources(&self) -> Option<Resources<HdfsStorageConfig, NoRuntimeLimits>> {
        None
    }
    /// Resources for datanodes.
    /// Other roles must use `resources`.
    fn data_node_resources(
        &self,
    ) -> Option<Resources<DataNodeStorageConfigInnerType, NoRuntimeLimits>> {
        None
    }
    fn affinity(&self) -> &StackableAffinity;
    /// Main container shared by all roles
    fn hdfs_logging(&self) -> ContainerLogConfig;
    /// Vector container shared by all roles
    fn vector_logging(&self) -> ContainerLogConfig;
    /// Helper method to access if vector container should be deployed
    fn vector_logging_enabled(&self) -> bool;
    /// Namenode side container (ZooKeeperFailOverController)
    fn zkfc_logging(&self) -> Option<ContainerLogConfig> {
        None
    }
    /// Namenode init container to format namenode
    fn format_namenodes_logging(&self) -> Option<ContainerLogConfig> {
        None
    }
    /// Namenode init container to format zookeeper
    fn format_zookeeper_logging(&self) -> Option<ContainerLogConfig> {
        None
    }
    /// Datanode init container to wait for namenodes to become ready
    fn wait_for_namenodes(&self) -> Option<ContainerLogConfig> {
        None
    }
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    EnumString,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
)]
pub enum HdfsRole {
    #[serde(rename = "namenode")]
    #[strum(serialize = "namenode")]
    NameNode,
    #[serde(rename = "datanode")]
    #[strum(serialize = "datanode")]
    DataNode,
    #[serde(rename = "journalnode")]
    #[strum(serialize = "journalnode")]
    JournalNode,
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
    ) -> Result<Box<dyn MergedConfig + Send + 'static>, Error> {
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

                if let Some(RoleGroup {
                    selector: Some(selector),
                    ..
                }) = role.role_groups.get(role_group)
                {
                    // Migrate old `selector` attribute, see ADR 26 affinities.
                    // TODO Can be removed after support for the old `selector` field is dropped.
                    #[allow(deprecated)]
                    role_group_config.affinity.add_legacy_selector(selector);
                }

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(Box::new(
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

                if let Some(RoleGroup {
                    selector: Some(selector),
                    ..
                }) = role.role_groups.get(role_group)
                {
                    // Migrate old `selector` attribute, see ADR 26 affinities.
                    // TODO Can be removed after support for the old `selector` field is dropped.
                    #[allow(deprecated)]
                    role_group_config.affinity.add_legacy_selector(selector);
                }

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(Box::new(
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

                if let Some(RoleGroup {
                    selector: Some(selector),
                    ..
                }) = role.role_groups.get(role_group)
                {
                    // Migrate old `selector` attribute, see ADR 26 affinities.
                    // TODO Can be removed after support for the old `selector` field is dropped.
                    #[allow(deprecated)]
                    role_group_config.affinity.add_legacy_selector(selector);
                }

                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(Box::new(
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
    pub fn role_group_replicas(&self, hdfs: &HdfsCluster, role_group: &str) -> i32 {
        match self {
            HdfsRole::NameNode => hdfs
                .namenode_rolegroup(role_group)
                .and_then(|rg| rg.replicas)
                .unwrap_or_default()
                .into(),
            HdfsRole::DataNode => hdfs
                .datanode_rolegroup(role_group)
                .and_then(|rg| rg.replicas)
                .unwrap_or_default()
                .into(),
            HdfsRole::JournalNode => hdfs
                .journalnode_rolegroup(role_group)
                .and_then(|rg| rg.replicas)
                .unwrap_or_default()
                .into(),
        }
    }

    /// Return the node/label selector for a certain rolegroup.
    pub fn role_group_node_selector(
        &self,
        hdfs: &HdfsCluster,
        role_group: &str,
    ) -> Option<LabelSelector> {
        match self {
            HdfsRole::NameNode => hdfs
                .namenode_rolegroup(role_group)
                .and_then(|rg| rg.selector.clone()),
            HdfsRole::DataNode => hdfs
                .datanode_rolegroup(role_group)
                .and_then(|rg| rg.selector.clone()),
            HdfsRole::JournalNode => hdfs
                .journalnode_rolegroup(role_group)
                .and_then(|rg| rg.selector.clone()),
        }
    }
}

impl HdfsCluster {
    /// Kubernetes labels to attach to Pods within a role group.
    ///
    /// The same labels are also used as selectors for Services and StatefulSets.
    pub fn rolegroup_selector_labels(
        &self,
        rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    ) -> BTreeMap<String, String> {
        let mut group_labels = role_group_selector_labels(
            self,
            APP_NAME,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        );
        group_labels.insert(String::from("role"), rolegroup_ref.role.clone());
        group_labels.insert(String::from("group"), rolegroup_ref.role_group.clone());

        if self.spec.cluster_config.listener_class
            == CurrentlySupportedListenerClasses::ExternalUnstable
        {
            // TODO: in a production environment, probably not all roles need to be exposed with one NodePort per Pod but it's
            // useful for development purposes.

            group_labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
        }

        group_labels
    }

    /// Get a reference to the namenode [`RoleGroup`] struct if it exists.
    pub fn namenode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<NameNodeConfigFragment>> {
        self.spec.name_nodes.as_ref()?.role_groups.get(role_group)
    }

    /// Get a reference to the datanode [`RoleGroup`] struct if it exists.
    pub fn datanode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<DataNodeConfigFragment>> {
        self.spec.data_nodes.as_ref()?.role_groups.get(role_group)
    }

    /// Get a reference to the journalnode [`RoleGroup`] struct if it exists.
    pub fn journalnode_rolegroup(
        &self,
        role_group: &str,
    ) -> Option<&RoleGroup<JournalNodeConfigFragment>> {
        self.spec
            .journal_nodes
            .as_ref()?
            .role_groups
            .get(role_group)
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

    /// List all [HdfsPodRef]s expected for the given `role`
    ///
    /// The `validated_config` is used to extract the ports exposed by the pods.
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
                })
            })
            .collect())
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
            PropertyNameKind::File(SSL_SERVER_XML.to_string()),
            PropertyNameKind::File(SSL_CLIENT_XML.to_string()),
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

    pub fn has_kerberos_enabled(&self) -> bool {
        self.spec.cluster_config.kerberos.is_some()
    }

    pub fn kerberos_config(&self) -> Option<&KerberosConfig> {
        self.spec.cluster_config.kerberos.as_ref()
    }

    pub fn has_https_enabled(&self) -> bool {
        self.https_secret_class().is_some()
    }

    pub fn https_secret_class(&self) -> Option<&str> {
        self.spec
            .cluster_config
            .kerberos
            .as_ref()
            .map(|k| k.tls_secret_class.as_str())
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
    pub ports: HashMap<String, u16>,
}

impl HdfsPodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
}

fn default_resources_fragment() -> ResourcesFragment<HdfsStorageConfig, NoRuntimeLimits> {
    ResourcesFragment {
        cpu: CpuLimitsFragment {
            min: Some(Quantity("100m".to_owned())),
            max: Some(Quantity("4".to_owned())),
        },
        memory: MemoryLimitsFragment {
            limit: Some(Quantity("1Gi".to_owned())),
            runtime_limits: NoRuntimeLimitsFragment {},
        },
        storage: HdfsStorageConfigFragment {
            data: PvcConfigFragment {
                capacity: Some(Quantity("2Gi".to_owned())),
                storage_class: None,
                selectors: None,
            },
        },
    }
}

pub fn default_data_node_resources_fragment(
) -> ResourcesFragment<DataNodeStorageConfigInnerType, NoRuntimeLimits> {
    let default_resources_fragment = default_resources_fragment();
    ResourcesFragment {
        cpu: default_resources_fragment.cpu,
        memory: default_resources_fragment.memory,
        storage: BTreeMap::from([(
            "data".to_string(),
            DataNodePvcFragment {
                pvc: PvcConfigFragment {
                    capacity: Some(Quantity("5Gi".to_owned())),
                    storage_class: None,
                    selectors: None,
                },
                count: Some(1),
                hdfs_storage_type: Some(HdfsStorageType::default()),
            },
        )]),
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
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl MergedConfig for NameNodeConfig {
    fn resources(&self) -> Option<Resources<HdfsStorageConfig, NoRuntimeLimits>> {
        Some(self.resources.clone())
    }

    fn affinity(&self) -> &StackableAffinity {
        &self.affinity
    }

    fn hdfs_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&NameNodeContainer::Hdfs)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&NameNodeContainer::Vector)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging_enabled(&self) -> bool {
        self.logging.enable_vector_agent
    }

    fn zkfc_logging(&self) -> Option<ContainerLogConfig> {
        self.logging
            .containers
            .get(&NameNodeContainer::Zkfc)
            .cloned()
    }

    fn format_namenodes_logging(&self) -> Option<ContainerLogConfig> {
        self.logging
            .containers
            .get(&NameNodeContainer::FormatNameNodes)
            .cloned()
    }

    fn format_zookeeper_logging(&self) -> Option<ContainerLogConfig> {
        self.logging
            .containers
            .get(&NameNodeContainer::FormatZooKeeper)
            .cloned()
    }
}

impl NameNodeConfigFragment {
    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
        Self {
            resources: default_resources_fragment(),
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
        }
    }
}

impl Configuration for NameNodeConfigFragment {
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
            if let Some(replication) = &resource.spec.cluster_config.dfs_replication {
                config.insert(DFS_REPLICATION.to_string(), Some(replication.to_string()));
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
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl MergedConfig for DataNodeConfig {
    fn data_node_resources(
        &self,
    ) -> Option<Resources<DataNodeStorageConfigInnerType, NoRuntimeLimits>> {
        Some(self.resources.clone())
    }

    fn affinity(&self) -> &StackableAffinity {
        &self.affinity
    }

    fn hdfs_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&DataNodeContainer::Hdfs)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&DataNodeContainer::Vector)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging_enabled(&self) -> bool {
        self.logging.enable_vector_agent
    }

    fn wait_for_namenodes(&self) -> Option<ContainerLogConfig> {
        self.logging
            .containers
            .get(&DataNodeContainer::WaitForNameNodes)
            .cloned()
    }
}

impl DataNodeConfigFragment {
    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
        Self {
            resources: default_data_node_resources_fragment(),
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
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
            if let Some(replication) = &resource.spec.cluster_config.dfs_replication {
                config.insert(DFS_REPLICATION.to_string(), Some(replication.to_string()));
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
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl MergedConfig for JournalNodeConfig {
    fn resources(&self) -> Option<Resources<HdfsStorageConfig, NoRuntimeLimits>> {
        Some(self.resources.clone())
    }

    fn affinity(&self) -> &StackableAffinity {
        &self.affinity
    }

    fn hdfs_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&JournalNodeContainer::Hdfs)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging(&self) -> ContainerLogConfig {
        self.logging
            .containers
            .get(&JournalNodeContainer::Vector)
            .cloned()
            .unwrap_or_default()
    }

    fn vector_logging_enabled(&self) -> bool {
        self.logging.enable_vector_agent
    }
}

impl JournalNodeConfigFragment {
    pub fn default_config(cluster_name: &str, role: &HdfsRole) -> Self {
        Self {
            resources: default_resources_fragment(),
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
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
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for HdfsCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
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
        let resources = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .data_node_resources()
            .unwrap();
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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
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
        let resources = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .data_node_resources()
            .unwrap();
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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  dataNodes:
    roleGroups:
      default:
        replicas: 1
";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let resources = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .data_node_resources()
            .unwrap();
        let pvc = resources.storage.get("data").unwrap();

        assert_eq!(pvc.count, 1);
        assert_eq!(pvc.hdfs_storage_type, HdfsStorageType::Disk);
        assert_eq!(pvc.pvc.capacity, Some(Quantity("5Gi".to_string())));
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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let role = HdfsRole::DataNode;
        let resources = role
            .merged_config(&hdfs, "default")
            .unwrap()
            .data_node_resources()
            .unwrap();

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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
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
            .data_node_resources()
            .unwrap()
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
    productVersion: 3.3.4
    stackableVersion: 0.2.0
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
            .data_node_resources()
            .unwrap()
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
}
