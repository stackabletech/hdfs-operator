pub mod constants;
pub mod error;

use constants::*;
use error::{Error, HdfsOperatorResult};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::commons::resources::{NoRuntimeLimits, PvcConfig, Resources};
use stackable_operator::commons::resources::{
    NoRuntimeLimitsFragment, PvcConfigFragment, ResourcesFragment,
};
use stackable_operator::config::fragment::Fragment;
use stackable_operator::config::merge::{Atomic, Merge};
use stackable_operator::k8s_openapi::api::core::v1::{PersistentVolumeClaim, ResourceRequirements};
use stackable_operator::k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::labels::role_group_selector_labels;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::schemars::{self, JsonSchema};
use std::collections::{BTreeMap, HashMap};
use strum::{Display, EnumIter, EnumString};

use stackable_operator::commons::resources::CpuLimitsFragment;
use stackable_operator::commons::resources::MemoryLimitsFragment;
use stackable_operator::config::fragment;

// Dirs
pub const ROOT_DATA_DIR: &str = "/stackable/data";
pub const JOURNALNODE_DIR: &str = "/stackable/data/journal";
pub const NAMENODE_DIR: &str = "/stackable/data/name";

// Will end up with something like `/stackable/data/data` and `/stackable/data-1/data` etc.
pub const DATANODE_DIR_PREFIX: &str = "/stackable/";
pub const DATANODE_DIR_SUFFIX: &str = "/data";

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
    pub zookeeper_config_map_name: String,
    pub data_nodes: Option<Role<DataNodeConfig>>,
    pub name_nodes: Option<Role<NameNodeConfig>>,
    pub journal_nodes: Option<Role<JournalNodeConfig>>,
    pub dfs_replication: Option<u8>,
    pub log4j: Option<String>,
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
            HdfsRole::JournalNode => 3,
            HdfsRole::DataNode => 1,
            HdfsRole::NameNode => 2,
        }
    }

    pub fn replicas_can_be_even(&self) -> bool {
        match self {
            HdfsRole::JournalNode => false,
            HdfsRole::DataNode => true,
            HdfsRole::NameNode => true,
        }
    }

    pub fn check_valid_dfs_replication(&self) -> bool {
        match self {
            HdfsRole::JournalNode => false,
            HdfsRole::NameNode => false,
            HdfsRole::DataNode => true,
        }
    }
}

lazy_static! {
    pub static ref ROLE_PORTS: HashMap<HdfsRole, Vec<(String, i32)>> = [
        (
            HdfsRole::NameNode,
            vec![
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
            ]
        ),
        (
            HdfsRole::DataNode,
            vec![
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
            ]
        ),
        (
            HdfsRole::JournalNode,
            vec![
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
            ]
        ),
    ]
    .into_iter()
    .collect();
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

    /// Number of journal node replicas configured for the given `rolegroup_ref`
    pub fn rolegroup_journalnode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        HdfsCluster::extract_replicas(self.spec.journal_nodes.as_ref(), rolegroup_ref)
    }

    /// Number of name node replicas configured for the given `rolegroup_ref`
    pub fn rolegroup_namenode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        HdfsCluster::extract_replicas(self.spec.name_nodes.as_ref(), rolegroup_ref)
    }

    /// Number of data node replicas configured for the given `rolegroup_ref`.
    pub fn rolegroup_datanode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        HdfsCluster::extract_replicas(self.spec.data_nodes.as_ref(), rolegroup_ref)
    }

    /// Number of replicas for a given `role` and `rolegroup_ref`.
    fn extract_replicas<T>(
        role: Option<&Role<T>>,
        rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    ) -> HdfsOperatorResult<u16> {
        Ok(role
            .as_ref()
            .ok_or(Error::MissingNodeRole {
                role: rolegroup_ref.role.clone(),
            })?
            .role_groups
            .get(&rolegroup_ref.role_group)
            .ok_or(Error::RoleGroupNotFound {
                rolegroup: rolegroup_ref.role_group.clone(),
            })?
            .replicas
            .unwrap_or_default())
    }

    /// Build the [`PersistentVolumeClaim`]s and [`ResourceRequirements`] for the given `rolegroup_ref`.
    /// These can be defined at the role or rolegroup level and as usual, the
    /// following precedence rules are implemented:
    /// 1. group pvc
    /// 2. role pvc
    /// 3. a default PVC with 1Gi capacity
    pub fn resources(
        &self,
        role: &HdfsRole,
        rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    ) -> Result<
        (
            Vec<PersistentVolumeClaim>,
            ResourceRequirements,
            Option<DataNodeStorage>,
        ),
        Error,
    > {
        match role {
            HdfsRole::NameNode | HdfsRole::JournalNode => {
                // Normal flow as in other operators
                let default_resources = self.default_resources();

                let mut role_resources = match role {
                    HdfsRole::JournalNode => self
                        .spec
                        .journal_nodes
                        .as_ref()
                        .map(|role| &role.config.config)
                        .and_then(|node_config| node_config.resources.clone())
                        .unwrap_or_default(),
                    HdfsRole::NameNode => self
                        .spec
                        .name_nodes
                        .as_ref()
                        .map(|role| &role.config.config)
                        .and_then(|node_config| node_config.resources.clone())
                        .unwrap_or_default(),
                    HdfsRole::DataNode => unreachable!(),
                };

                let mut rg_resources = match role {
                    HdfsRole::JournalNode => self
                        .spec
                        .journal_nodes
                        .as_ref()
                        .and_then(|role| {
                            role.role_groups
                                .get(&rolegroup_ref.role_group)
                                .map(|rg| &rg.config.config)
                        })
                        .and_then(|node_config| node_config.resources.clone())
                        .unwrap_or_default(),
                    HdfsRole::NameNode => self
                        .spec
                        .name_nodes
                        .as_ref()
                        .and_then(|role| {
                            role.role_groups
                                .get(&rolegroup_ref.role_group)
                                .map(|rg| &rg.config.config)
                        })
                        .and_then(|node_config| node_config.resources.clone())
                        .unwrap_or_default(),
                    HdfsRole::DataNode => unreachable!(),
                };

                role_resources.merge(&default_resources);
                rg_resources.merge(&role_resources);

                let resources: Resources<Storage, NoRuntimeLimits> =
                    fragment::validate(rg_resources)
                        .map_err(|source| Error::FragmentValidationFailure { source })?;

                let data_pvc = resources
                    .storage
                    .data
                    .build_pvc("data", Some(vec!["ReadWriteOnce"]));

                Ok((vec![data_pvc], resources.into(), None))
            }
            HdfsRole::DataNode => {
                // DataNodes need some special handling as they can have multiple pvcs
                let default_resources = self.default_data_node_resources();

                let mut role_resources = self
                    .spec
                    .data_nodes
                    .as_ref()
                    .map(|role| &role.config.config)
                    .and_then(|node_config| node_config.resources.clone())
                    .unwrap_or_default();

                let mut rg_resources = self
                    .spec
                    .data_nodes
                    .as_ref()
                    .and_then(|role| {
                        role.role_groups
                            .get(&rolegroup_ref.role_group)
                            .map(|rg| &rg.config.config)
                    })
                    .and_then(|node_config| node_config.resources.clone())
                    .unwrap_or_default();

                role_resources.merge(&default_resources);
                rg_resources.merge(&role_resources);

                let resources: Resources<DataNodeStorageInnerType, NoRuntimeLimits> =
                    fragment::validate(rg_resources)
                        .map_err(|source| Error::FragmentValidationFailure { source })?;

                let datanode_storage = DataNodeStorage {
                    pvcs: resources.storage.clone(),
                };
                let pvcs = datanode_storage.build_pvcs();

                Ok((pvcs, resources.into(), Some(datanode_storage)))
            }
        }
    }

    fn default_resources(&self) -> ResourcesFragment<Storage, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("500m".to_owned())),
                max: Some(Quantity("4".to_owned())),
            },
            memory: MemoryLimitsFragment {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
            storage: StorageFragment {
                data: PvcConfigFragment {
                    capacity: Some(Quantity("2Gi".to_owned())),
                    storage_class: None,
                    selectors: None,
                },
            },
        }
    }

    fn default_data_node_resources(
        &self,
    ) -> ResourcesFragment<DataNodeStorageInnerType, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: self.default_resources().cpu,
            memory: self.default_resources().memory,
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
                    ports: ROLE_PORTS
                        .get(role)
                        .unwrap_or(&Vec::new())
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
struct Storage {
    #[fragment_attrs(serde(default))]
    data: PvcConfig,
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
pub struct DataNodePvc {
    #[fragment_attrs(serde(default, flatten))]
    pvc: PvcConfig,

    #[serde(default = "default_number_of_datanode_pvcs")]
    count: u16,

    #[fragment_attrs(serde(default))]
    hdfs_storage_type: HdfsStorageType,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum HdfsStorageType {
    Archive,
    Disk,
    Ssd,
    RamDisk,
}

impl Atomic for HdfsStorageType {}

impl Default for HdfsStorageType {
    fn default() -> Self {
        Self::Disk
    }
}

impl HdfsStorageType {
    pub fn as_hdfs_config_literal(&self) -> &str {
        match self {
            HdfsStorageType::Archive => "ARCHIVE",
            HdfsStorageType::Disk => "DISK",
            HdfsStorageType::Ssd => "SSD",
            HdfsStorageType::RamDisk => "RAM_DISK",
        }
    }
}

fn default_number_of_datanode_pvcs() -> u16 {
    1
}

/// We can't use a struct with a BTreeMap attribute that is serde(flatten),
/// as the whole struct will be missing in the generated CRD for some reasons.
/// So we define a type for the inner BTreeMap and a struct with some helper functions.
pub type DataNodeStorageInnerType = BTreeMap<String, DataNodePvc>;

/// Use this struct to call functions on the `DataNodeStorageInnerType` type.
pub struct DataNodeStorage {
    pub pvcs: DataNodeStorageInnerType,
}

impl DataNodeStorage {
    /// Builds a list of pvcs with the length being `self.number_of_data_pvcs`.
    /// The spec - such as size, storageClass or selector - is used from the regular `PvcConfig` struct used for the `data` attribute.
    pub fn build_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let mut pvcs = vec![];

        for (pvc_name_prefix, pvc) in &self.pvcs {
            let disk_pvc_template = pvc
                .pvc
                .build_pvc(pvc_name_prefix, Some(vec!["ReadWriteOnce"]));

            pvcs.extend(
                Self::pvc_names(pvc_name_prefix, pvc.count)
                    .into_iter()
                    .map(|pvc_name| {
                        let mut pvc = disk_pvc_template.clone();
                        pvc.metadata.name = Some(pvc_name);
                        pvc
                    }),
            )
        }
        pvcs
    }

    /// Returns the a String to be put into `dfs.datanode.data.dir`.
    /// The config will include the HDFS storage type.
    pub fn get_datanode_data_dir(&self) -> String {
        self.pvcs
            .iter()
            .flat_map(|(pvc_name_prefix, pvc)| {
                Self::pvc_names(pvc_name_prefix, pvc.count)
                    .into_iter()
                    .map(|pvc_name| {
                        let storage_type = pvc.hdfs_storage_type.as_hdfs_config_literal();
                        format!(
                            "[{storage_type}]{DATANODE_DIR_PREFIX}{pvc_name}{DATANODE_DIR_SUFFIX}"
                        )
                    })
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns a list with the names of the pvcs to be used for nodes.
    /// The pvc names will be prefixed with the name given in the `pvc_name_prefix` argument.
    ///
    /// There are two options when it comes to naming:
    /// 1. Name the pvcs `data-0, data-1, data-2, ... , data-{n-1}`
    /// ** Good, because consistent naming
    /// ** Bad, because existing deployments (using release 22.11 or earlier) will need to migrate their data by renaming their pvcs
    ///
    /// 2. Name the pvcs `data, data-1, data-2, ... , data-{n-1}`
    /// ** Good, if nodes only have a single pvc (which probably most of the deployments will have) they name of the pvc will be consistent with the name of all the other pvcs out there
    /// ** It is important that the first pvc will be called `data` (without suffix), regardless of the number of pvcs to be used. This is needed as nodes should be able to alter the number of pvcs attached and the use-case number of pvcs 1 -> 2 should be supported without renaming pvcs.
    ///
    /// This function uses the 2. option.
    pub fn pvc_names(pvc_name_prefix: &str, number_of_pvcs: u16) -> Vec<String> {
        (0..number_of_pvcs)
            .map(|pvc_index| {
                if pvc_index == 0 {
                    pvc_name_prefix.to_string()
                } else {
                    format!("{pvc_name_prefix}-{pvc_index}")
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NameNodeConfig {
    resources: Option<ResourcesFragment<Storage, NoRuntimeLimits>>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataNodeConfig {
    resources: Option<ResourcesFragment<DataNodeStorageInnerType, NoRuntimeLimits>>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalNodeConfig {
    resources: Option<ResourcesFragment<Storage, NoRuntimeLimits>>,
}

impl Configuration for NameNodeConfig {
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

impl Configuration for DataNodeConfig {
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

impl Configuration for JournalNodeConfig {
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
    use std::collections::BTreeMap;

    use crate::{DataNodePvc, DataNodeStorage, HdfsStorageType};

    use super::{HdfsCluster, HdfsRole};
    use stackable_operator::{
        commons::resources::PvcConfig,
        k8s_openapi::{
            api::core::v1::{PersistentVolumeClaimSpec, ResourceRequirements},
            apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
        },
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
              data:
                capacity: 10Gi
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvcs, _, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("10Gi".to_owned()),
            pvcs[0]
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
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  dataNodes:
    config:
      resources:
        storage:
          data:
            capacity: 10Gi
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvcs, _, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("10Gi".to_owned()),
            pvcs[0]
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

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvcs, _, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(
            &Quantity("5Gi".to_owned()),
            pvcs[0]
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
                hdfsStorageType: Ssd
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();

        let name_node_rg_ref = hdfs.rolegroup_ref("name_nodes", "default");
        let (pvcs, _, _) = hdfs
            .resources(&HdfsRole::NameNode, &name_node_rg_ref)
            .unwrap();

        assert_eq!(pvcs.len(), 1);
        assert_eq!(pvcs[0].metadata.name, Some("data".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("2Gi".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: None,
                ..PersistentVolumeClaimSpec::default()
            })
        );

        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (pvcs, _, _) = hdfs
            .resources(&HdfsRole::DataNode, &data_node_rg_ref)
            .unwrap();

        assert_eq!(pvcs.len(), 8);
        assert_eq!(pvcs[0].metadata.name, Some("my-disks".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("100Gi".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: None,
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(pvcs[1].metadata.name, Some("my-disks-1".to_string()));
        assert_eq!(pvcs[2].metadata.name, Some("my-disks-2".to_string()));
        assert_eq!(pvcs[3].metadata.name, Some("my-disks-3".to_string()));
        assert_eq!(pvcs[4].metadata.name, Some("my-disks-4".to_string()));
        assert_eq!(pvcs[5].metadata.name, Some("my-ssds".to_string()));
        assert_eq!(
            pvcs[5].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("10Gi".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: Some("premium".to_string()),
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(pvcs[6].metadata.name, Some("my-ssds-1".to_string()));
        assert_eq!(pvcs[7].metadata.name, Some("my-ssds-2".to_string()));
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
  nameNodes:
    roleGroups:
      default:
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
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (_, rr, _) = hdfs
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
  nameNodes:
    roleGroups:
      default:
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
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();
        let data_node_rg_ref = hdfs.rolegroup_ref("data_nodes", "default");
        let (_, rr, _) = hdfs
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
    pub fn test_datanode_storage_defaults() {
        let datanode_storage = DataNodeStorage {
            pvcs: BTreeMap::from([(
                "data".to_string(),
                DataNodePvc {
                    pvc: PvcConfig {
                        capacity: Some(Quantity("5Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                    count: 1,
                    hdfs_storage_type: HdfsStorageType::default(),
                },
            )]),
        };
        let pvcs = datanode_storage.build_pvcs();
        let datanode_data_dir = datanode_storage.get_datanode_data_dir();

        assert_eq!(pvcs.len(), 1);
        assert_eq!(pvcs[0].metadata.name, Some("data".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("5Gi".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: None,
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(datanode_data_dir, "[DISK]/stackable/data/data")
    }

    #[test]
    pub fn test_datanode_storage_multiple_storage_types() {
        let datanode_storage = DataNodeStorage {
            pvcs: BTreeMap::from([
                (
                    "hdd".to_string(),
                    DataNodePvc {
                        pvc: PvcConfig {
                            capacity: Some(Quantity("12Ti".to_owned())),
                            storage_class: Some("hdd-storage-class".to_string()),
                            selectors: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([(
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )])),
                            }),
                        },
                        count: 8,
                        hdfs_storage_type: HdfsStorageType::Disk,
                    },
                ),
                (
                    "ssd".to_string(),
                    DataNodePvc {
                        pvc: PvcConfig {
                            capacity: Some(Quantity("2Ti".to_owned())),
                            storage_class: Some("premium-ssd".to_string()),
                            selectors: None,
                        },
                        count: 4,
                        hdfs_storage_type: HdfsStorageType::Ssd,
                    },
                ),
            ]),
        };
        let pvcs = datanode_storage.build_pvcs();
        let datanode_data_dir = datanode_storage.get_datanode_data_dir();

        assert_eq!(pvcs.len(), 8 + 4);
        assert_eq!(pvcs[0].metadata.name, Some("hdd".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("12Ti".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: Some("hdd-storage-class".to_string()),
                selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(BTreeMap::from([("foo".to_string(), "bar".to_string())]))
                }),
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(datanode_data_dir, "[DISK]/stackable/hdd/data,[DISK]/stackable/hdd-1/data,[DISK]/stackable/hdd-2/data,[DISK]/stackable/hdd-3/data,[DISK]/stackable/hdd-4/data,[DISK]/stackable/hdd-5/data,[DISK]/stackable/hdd-6/data,[DISK]/stackable/hdd-7/data,[SSD]/stackable/ssd/data,[SSD]/stackable/ssd-1/data,[SSD]/stackable/ssd-2/data,[SSD]/stackable/ssd-3/data")
    }
}
