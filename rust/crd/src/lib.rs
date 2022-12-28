pub mod constants;
pub mod error;

use constants::*;
use error::{Error, HdfsOperatorResult};
use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::{
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            PvcConfig, PvcConfigFragment, Resources, ResourcesFragment,
        },
    },
    config::{
        fragment,
        fragment::{Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, ResourceRequirements},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{runtime::reflector::ObjectRef, CustomResource},
    labels::role_group_selector_labels,
    product_config::types::PropertyNameKind,
    product_config_utils::{ConfigError, Configuration},
    product_logging,
    product_logging::spec::{Logging, LoggingFragment},
    role_utils::{Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::{BTreeMap, HashMap};
use strum::{Display, EnumIter, EnumString};

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hdfs.stackable.tech",
    version = "v1alpha1",
    kind = "HdfsCluster",
    plural = "hdfsclusters",
    shortname = "hdfs",
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
    pub auto_format_fs: Option<bool>,
    pub dfs_replication: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_nodes: Option<Role<NameNodeConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_nodes: Option<Role<DataNodeConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_nodes: Option<Role<JournalNodeConfigFragment>>,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    /// Name of the ZooKeeper discovery config map.
    pub zookeeper_config_map_name: String,
}

/// This is a shared trait for all role/role-group config structs to avoid duplication
/// when extracting role specific configuration structs.
pub trait MergedConfig {
    fn resources(&self) -> Resources<HdfsStorageConfig, NoRuntimeLimits>;
    fn logging(&self) -> Logging<Container>;
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

    /// Returns required port name and port number tuples depending on the role.
    pub fn ports(&self) -> Vec<(String, i32)> {
        match self {
            HdfsRole::NameNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_NAME_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_NAME_NODE_HTTP_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    DEFAULT_NAME_NODE_RPC_PORT,
                ),
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
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_DATA_NODE_HTTP_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_IPC),
                    DEFAULT_DATA_NODE_IPC_PORT,
                ),
            ],
            HdfsRole::JournalNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_JOURNAL_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    DEFAULT_JOURNAL_NODE_HTTP_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTPS),
                    DEFAULT_JOURNAL_NODE_HTTPS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    DEFAULT_JOURNAL_NODE_RPC_PORT,
                ),
            ],
        }
    }

    pub fn merged_config(
        &self,
        hdfs: &HdfsCluster,
        role_group: &str,
    ) -> Box<dyn MergedConfig + 'static> {
        // TODO: error handling
        match self {
            HdfsRole::NameNode => {
                let default_config = NameNodeConfigFragment::default();
                let mut role_config = hdfs.spec.name_nodes.as_ref().unwrap().config.config.clone();
                let mut role_group_config = hdfs
                    .spec
                    .name_nodes
                    .as_ref()
                    .unwrap()
                    .role_groups
                    .get(role_group)
                    .unwrap()
                    .config
                    .config
                    .clone();
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Box::new(fragment::validate::<NameNodeConfig>(role_group_config.clone()).unwrap())
            }
            HdfsRole::DataNode => {
                let default_config = DataNodeConfigFragment::default();
                let mut role_config = hdfs.spec.data_nodes.as_ref().unwrap().config.config.clone();
                let mut role_group_config = hdfs
                    .spec
                    .data_nodes
                    .as_ref()
                    .unwrap()
                    .role_groups
                    .get(role_group)
                    .unwrap()
                    .config
                    .config
                    .clone();
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Box::new(fragment::validate::<DataNodeConfig>(role_group_config.clone()).unwrap())
            }
            HdfsRole::JournalNode => {
                let default_config = JournalNodeConfigFragment::default();
                let mut role_config = hdfs
                    .spec
                    .journal_nodes
                    .as_ref()
                    .unwrap()
                    .config
                    .config
                    .clone();
                let mut role_group_config = hdfs
                    .spec
                    .journal_nodes
                    .as_ref()
                    .unwrap()
                    .role_groups
                    .get(role_group)
                    .unwrap()
                    .config
                    .config
                    .clone();
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Box::new(
                    fragment::validate::<JournalNodeConfig>(role_group_config.clone()).unwrap(),
                )
            }
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
        // TODO: in a production environment, probably not all roles need to be exposed with one NodePort per Pod but it's
        // useful for development purposes.
        group_labels.insert(LABEL_ENABLE.to_string(), "true".to_string());

        group_labels
    }

    /// Get a reference to the datanode [`RoleGroup`] struct if it exists.
    pub fn datanode_rolegroup(
        &self,
        rg_ref: &RoleGroupRef<Self>,
    ) -> Option<&RoleGroup<DataNodeConfigFragment>> {
        self.spec
            .data_nodes
            .as_ref()?
            .role_groups
            .get(&rg_ref.role_group)
    }

    /// Get a reference to the namenode [`RoleGroup`] struct if it exists.
    pub fn namenode_rolegroup(
        &self,
        rg_ref: &RoleGroupRef<Self>,
    ) -> Option<&RoleGroup<NameNodeConfigFragment>> {
        self.spec
            .name_nodes
            .as_ref()?
            .role_groups
            .get(&rg_ref.role_group)
    }

    /// Get a reference to the journalnode [`RoleGroup`] struct if it exists.
    pub fn journalnode_rolegroup(
        &self,
        rg_ref: &RoleGroupRef<Self>,
    ) -> Option<&RoleGroup<JournalNodeConfigFragment>> {
        self.spec
            .journal_nodes
            .as_ref()?
            .role_groups
            .get(&rg_ref.role_group)
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
    pub fn pod_refs(&self, role: &HdfsRole) -> HdfsOperatorResult<Vec<HdfsPodRef>> {
        let ns = self
            .metadata
            .namespace
            .clone()
            .ok_or(Error::NoNamespaceContext)?;

        let rolegroup_ref_and_replicas = self.rolegroup_ref_and_replicas(role);

        Ok(rolegroup_ref_and_replicas
            .iter()
            .flat_map(|(rolegroup_ref, replicas)| {
                let ns = ns.clone();
                (0..*replicas).map(move |i| HdfsPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                    ports: role.ports().iter().map(|(n, p)| (n.clone(), *p)).collect(),
                })
            })
            .collect())
    }

    pub fn rolegroup_ref_and_replicas(
        &self,
        role: &HdfsRole,
    ) -> Vec<(RoleGroupRef<HdfsCluster>, u16)> {
        match role {
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
        }
    }

    pub fn build_role_properties(
        &self,
    ) -> HdfsOperatorResult<
        HashMap<
            String,
            (
                Vec<PropertyNameKind>,
                Role<impl Configuration<Configurable = HdfsCluster>>,
            ),
        >,
    > {
        let mut result = HashMap::new();
        let pnk = vec![
            PropertyNameKind::File(HDFS_SITE_XML.to_string()),
            PropertyNameKind::File(CORE_SITE_XML.to_string()),
            PropertyNameKind::File(LOG4J_PROPERTIES.to_string()),
            PropertyNameKind::Env,
        ];

        if let Some(name_nodes) = &self.spec.name_nodes {
            result.insert(
                HdfsRole::NameNode.to_string(),
                (pnk.clone(), name_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingNodeRole {
                role: HdfsRole::NameNode.to_string(),
            });
        }

        if let Some(data_nodes) = &self.spec.data_nodes {
            result.insert(
                HdfsRole::DataNode.to_string(),
                (pnk.clone(), data_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingNodeRole {
                role: HdfsRole::DataNode.to_string(),
            });
        }

        if let Some(journal_nodes) = &self.spec.journal_nodes {
            result.insert(
                HdfsRole::JournalNode.to_string(),
                (pnk, journal_nodes.clone().erase()),
            );
        } else {
            return Err(Error::MissingNodeRole {
                role: HdfsRole::JournalNode.to_string(),
            });
        }

        Ok(result)
    }
}
/// Reference to a single `Pod` that is a component of a [`HdfsCluster`]
///
/// Used for service discovery.
pub struct HdfsPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
    pub ports: HashMap<String, i32>,
}

impl HdfsPodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
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
pub struct HdfsStorageConfig {
    #[fragment_attrs(serde(default))]
    data: PvcConfig,
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
pub enum Container {
    Hdfs,
    Vector,
}

fn default_resources_fragment() -> ResourcesFragment<HdfsStorageConfig, NoRuntimeLimits> {
    ResourcesFragment {
        cpu: CpuLimitsFragment {
            min: Some(Quantity("500m".to_owned())),
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

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(Clone, Debug, Deserialize, Merge, JsonSchema, PartialEq, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct NameNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HdfsStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
}

impl MergedConfig for NameNodeConfig {
    fn resources(&self) -> Resources<HdfsStorageConfig, NoRuntimeLimits> {
        self.resources.clone()
    }
    fn logging(&self) -> Logging<Container> {
        self.logging.clone()
    }
}

impl Default for NameNodeConfigFragment {
    fn default() -> Self {
        Self {
            resources: default_resources_fragment(),
            logging: product_logging::spec::default_logging(),
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
            if let Some(replication) = &resource.spec.dfs_replication {
                config.insert(DFS_REPLICATION.to_string(), Some(replication.to_string()));
            }
        }

        Ok(config)
    }
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(Clone, Debug, Deserialize, Merge, JsonSchema, PartialEq, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct DataNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HdfsStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
}

impl MergedConfig for DataNodeConfig {
    fn resources(&self) -> Resources<HdfsStorageConfig, NoRuntimeLimits> {
        self.resources.clone()
    }
    fn logging(&self) -> Logging<Container> {
        self.logging.clone()
    }
}

impl Default for DataNodeConfigFragment {
    fn default() -> Self {
        Self {
            resources: default_resources_fragment(),
            logging: product_logging::spec::default_logging(),
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
            if let Some(replication) = &resource.spec.dfs_replication {
                config.insert(DFS_REPLICATION.to_string(), Some(replication.to_string()));
            }
        }

        Ok(config)
    }
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(Clone, Debug, Deserialize, Merge, JsonSchema, PartialEq, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct JournalNodeConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HdfsStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
}

impl MergedConfig for JournalNodeConfig {
    fn resources(&self) -> Resources<HdfsStorageConfig, NoRuntimeLimits> {
        self.resources.clone()
    }
    fn logging(&self) -> Logging<Container> {
        self.logging.clone()
    }
}

impl Default for JournalNodeConfigFragment {
    fn default() -> Self {
        Self {
            resources: default_resources_fragment(),
            logging: product_logging::spec::default_logging(),
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

#[cfg(test)]
mod test {
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
  zookeeperConfigMapName: hdfs-zk
  dfsReplication: 1
  log4j: |-
    hadoop.root.logger=INFO,console
  nameNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 2
  dataNodes:
    roleGroups:
      default:
        config:
          resources:
            storage:
              data:
                capacity: 5Gi
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvc, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("5Gi".to_owned()),
            pvc[0]
                .clone()
                .spec
                .unwrap()
                .resources
                .unwrap()
                .requests
                .unwrap()
                .get("storage")
                .unwrap()
        );
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
  zookeeperConfigMapName: hdfs-zk
  dfsReplication: 1
  log4j: |-
    hadoop.root.logger=INFO,console
  nameNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 2
  dataNodes:
    config:
      resources:
        storage:
          data:
            capacity: 5Gi
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1

        ";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvc, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("5Gi".to_owned()),
            pvc[0]
                .clone()
                .spec
                .unwrap()
                .resources
                .unwrap()
                .requests
                .unwrap()
                .get("storage")
                .unwrap()
        );
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
  zookeeperConfigMapName: hdfs-zk
  dfsReplication: 1
  log4j: |-
    hadoop.root.logger=INFO,console
  nameNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 2
  dataNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1

        ";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvc, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("2Gi".to_owned()),
            pvc[0]
                .clone()
                .spec
                .unwrap()
                .resources
                .unwrap()
                .requests
                .unwrap()
                .get("storage")
                .unwrap()
        );
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
  zookeeperConfigMapName: hdfs-zk
  dfsReplication: 1
  log4j: |-
    hadoop.root.logger=INFO,console
  nameNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 2
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
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1

        ";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (_, rr) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

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
  zookeeperConfigMapName: hdfs-zk
  dfsReplication: 1
  log4j: |-
    hadoop.root.logger=INFO,console
  nameNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 2
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
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        selector:
          matchLabels:
            kubernetes.io/os: linux
        replicas: 1

        ";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (_, rr) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

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
        };
        assert_eq!(expected, rr);
    }
}
