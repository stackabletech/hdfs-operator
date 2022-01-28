pub mod constants;
pub mod error;

use constants::*;
use error::{Error, HdfsOperatorResult};
use serde::{Deserialize, Serialize};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::labels::role_group_selector_labels;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config_utils::{
    ConfigError, Configuration, ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::schemars::{self, JsonSchema};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
use strum_macros::EnumIter;
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
    pub version: Option<String>,
    pub auto_format_fs: Option<bool>,
    pub zookeeper_config_map_name: String,
    pub data_nodes: Option<Role<DataNodeConfig>>,
    pub name_nodes: Option<Role<NameNodeConfig>>,
    pub journal_nodes: Option<Role<JournalNodeConfig>>,
    pub log4j: Option<String>,
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
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

impl HdfsCluster {
    pub fn hdfs_version(&self) -> HdfsOperatorResult<&str> {
        self.spec
            .version
            .as_deref()
            .ok_or(Error::ObjectHasNoVersion {
                obj_ref: ObjectRef::from_obj(self),
            })
    }

    pub fn hdfs_image(&self) -> HdfsOperatorResult<String> {
        Ok(format!(
            "docker.stackable.tech/stackable/hadoop:{}-stackable0",
            self.hdfs_version()?
        ))
    }

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
        match rolegroup_ref.role.as_str() {
            // TODO: in a production environment, probably not all roles need to be exposed with one NodePort per Pod but it's
            // useful for development purposes.
            "namenode" | "journalnode" | "datanode" => {
                group_labels.insert(LABEL_ENABLE.to_string(), "true".to_string());
            }
            &_ => {}
        };
        group_labels
    }

    /// Number of journal node replicas configured for the given `rolegroup_ref` or 1 if none is configured.
    pub fn rolegroup_journalnode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        Ok(max(
            1,
            self.spec
                .journal_nodes
                .as_ref()
                .ok_or(Error::NoJournalNodeRole)?
                .role_groups
                .get(&rolegroup_ref.role_group)
                .ok_or(Error::RoleGroupNotFound {
                    rolegroup: rolegroup_ref.role_group.clone(),
                })?
                .replicas
                .unwrap_or_default(),
        ))
    }

    /// Number of name node replicas configured for the given `rolegroup_ref` or 2 if none is configured.
    pub fn rolegroup_namenode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        Ok(max(
            2,
            self.spec
                .name_nodes
                .as_ref()
                .ok_or(Error::NoJournalNodeRole)?
                .role_groups
                .get(&rolegroup_ref.role_group)
                .ok_or(Error::RoleGroupNotFound {
                    rolegroup: rolegroup_ref.role_group.clone(),
                })?
                .replicas
                .unwrap_or_default(),
        ))
    }

    /// Number of data node replicas configured for the given `rolegroup_ref`.
    pub fn rolegroup_datanode_replicas(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> HdfsOperatorResult<u16> {
        Ok(self
            .spec
            .data_nodes
            .as_ref()
            .ok_or(Error::NoJournalNodeRole)?
            .role_groups
            .get(&rolegroup_ref.role_group)
            .ok_or(Error::RoleGroupNotFound {
                rolegroup: rolegroup_ref.role_group.clone(),
            })?
            .replicas
            .unwrap_or_default())
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
    pub fn pod_refs(
        &self,
        role: HdfsRole,
        validated_config: &ValidatedRoleConfigByPropertyKind,
    ) -> HdfsOperatorResult<Vec<HdfsPodRef>> {
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
                    ports: HdfsCluster::rolegroup_ports(rolegroup_ref, validated_config)
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                })
            })
            .collect())
    }

    fn rolegroup_ref_and_replicas(&self, role: HdfsRole) -> Vec<(RoleGroupRef<HdfsCluster>, u16)> {
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

    /// Return a list of porrts exposed by pods of the given `rolegroup_ref`.
    pub fn rolegroup_ports(
        rolegroup_ref: &RoleGroupRef<HdfsCluster>,
        validated_config: &ValidatedRoleConfigByPropertyKind,
    ) -> HdfsOperatorResult<Vec<(String, i32)>> {
        let rolegroup_config = validated_config
            .get(&rolegroup_ref.role)
            .and_then(|groups| groups.get(&rolegroup_ref.role_group))
            .ok_or(Error::RolegroupNotInValidatedConfig {
                group: rolegroup_ref.role_group.clone(),
                role: rolegroup_ref.role.clone(),
            })?;

        Ok(match serde_yaml::from_str(&rolegroup_ref.role).unwrap() {
            HdfsRole::NameNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_NAME_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_NAME_NODE_HTTP_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:9870"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("9870"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_NAME_NODE_HTTP_ADDRESS),
                        })?,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_NAME_NODE_RPC_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:8020"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("8020"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_NAME_NODE_RPC_ADDRESS),
                        })?,
                ),
            ],
            HdfsRole::DataNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_DATA_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_DATA),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_DATA_NODE_DATA_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:9866"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("9866"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_DATA_NODE_DATA_ADDRESS),
                        })?,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_DATA_NODE_HTTP_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:9864"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("9864"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_DATA_NODE_HTTP_ADDRESS),
                        })?,
                ),
                (
                    String::from(SERVICE_PORT_NAME_IPC),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_DATA_NODE_IPC_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:9867"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("9867"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_DATA_NODE_IPC_ADDRESS),
                        })?,
                ),
            ],
            HdfsRole::JournalNode => vec![
                (
                    String::from(SERVICE_PORT_NAME_METRICS),
                    DEFAULT_JOURNAL_NODE_METRICS_PORT,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTP),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_JOURNAL_NODE_HTTP_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:8480"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("8480"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_JOURNAL_NODE_HTTP_ADDRESS),
                        })?,
                ),
                (
                    String::from(SERVICE_PORT_NAME_HTTPS),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_JOURNAL_NODE_HTTPS_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:8481"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("8481"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_JOURNAL_NODE_HTTPS_ADDRESS),
                        })?,
                ),
                (
                    String::from(SERVICE_PORT_NAME_RPC),
                    rolegroup_config
                        .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                        .and_then(|c| c.get(DFS_JOURNAL_NODE_RPC_ADDRESS))
                        .unwrap_or(&String::from("0.0.0.0:8485"))
                        .split(':')
                        .last()
                        .unwrap_or(&String::from("8485"))
                        .parse::<i32>()
                        .map_err(|source| Error::HdfsAddressPortParseError {
                            source,
                            address: String::from(DFS_JOURNAL_NODE_RPC_ADDRESS),
                        })?,
                ),
            ],
        })
    }

    pub fn default_role_metric_port(&self, role: HdfsRole) -> i32 {
        match role {
            HdfsRole::DataNode => DEFAULT_DATA_NODE_METRICS_PORT,
            HdfsRole::JournalNode => DEFAULT_JOURNAL_NODE_METRICS_PORT,
            HdfsRole::NameNode => DEFAULT_NAME_NODE_METRICS_PORT,
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
            return Err(Error::NoNameNodeRole);
        }

        if let Some(data_nodes) = &self.spec.data_nodes {
            result.insert(
                HdfsRole::DataNode.to_string(),
                (pnk.clone(), data_nodes.clone().erase()),
            );
        } else {
            return Err(Error::NoDataNodeRole);
        }

        if let Some(journal_nodes) = &self.spec.journal_nodes {
            result.insert(
                HdfsRole::JournalNode.to_string(),
                (pnk, journal_nodes.clone().erase()),
            );
        } else {
            return Err(Error::NoJournalNodeRole);
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
/// This is a struct to represent HDFS addresses (node_name/ip/interface and port)
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsAddress {
    pub interface: Option<String>,
    pub port: u16,
}

impl HdfsAddress {
    pub fn port(address: Option<&String>) -> Result<Option<String>, error::Error> {
        if let Some(address) = address {
            return Ok(Some(Self::try_from(address)?.port.to_string()));
        }
        Ok(None)
    }
}

impl Display for HdfsAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.interface.as_deref().unwrap_or("0.0.0.0"),
            self.port
        )
    }
}

impl TryFrom<&String> for HdfsAddress {
    type Error = error::Error;

    fn try_from(address: &String) -> Result<Self, Self::Error> {
        let elements = address.split(':').collect::<Vec<&str>>();

        if let Some(port) = elements.get(1) {
            return Ok(HdfsAddress {
                interface: elements.get(0).map(|interface| interface.to_string()),
                port: port
                    .parse::<u16>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: address.clone(),
                    })?,
            });
        }

        Err(Error::HdfsAddressParseError {
            address: address.clone(),
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NameNodeConfig {
    pub dfs_namenode_name_dir: Option<String>,
    pub dfs_replication: Option<u8>,
    pub metrics_port: Option<u16>,
    pub ipc_address: Option<HdfsAddress>,
    pub http_address: Option<HdfsAddress>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataNodeConfig {
    pub dfs_datanode_name_dir: Option<String>,
    pub dfs_replication: Option<u8>,
    pub metrics_port: Option<u16>,
    pub ipc_address: Option<HdfsAddress>,
    pub http_address: Option<HdfsAddress>,
    pub data_address: Option<HdfsAddress>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalNodeConfig {
    pub http_address: Option<HdfsAddress>,
    pub https_address: Option<HdfsAddress>,
    pub rpc_address: Option<HdfsAddress>,
    pub metrics_port: Option<u16>,
}

impl Configuration for NameNodeConfig {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(metrics_port) = self.metrics_port {
            result.insert(
                METRICS_PORT_PROPERTY.to_string(),
                Some(metrics_port.to_string()),
            );
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
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if file == HDFS_SITE_XML {
            if let Some(dfs_namenode_name_dir) = &self.dfs_namenode_name_dir {
                result.insert(
                    DFS_NAME_NODE_NAME_DIR.to_string(),
                    Some(dfs_namenode_name_dir.to_string()),
                );
            }

            if let Some(dfs_replication) = &self.dfs_replication {
                result.insert(
                    DFS_REPLICATION.to_string(),
                    Some(dfs_replication.to_string()),
                );
            }

            if let Some(http_address) = &self.http_address {
                result.insert(
                    DFS_NAME_NODE_HTTP_ADDRESS.to_string(),
                    Some(http_address.to_string()),
                );
            }
        } else if file == CORE_SITE_XML {
            if let Some(ipc_address) = &self.ipc_address {
                result.insert(FS_DEFAULT.to_string(), Some(ipc_address.to_string()));
            }
        }

        Ok(result)
    }
}

impl Configuration for DataNodeConfig {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(metrics_port) = self.metrics_port {
            result.insert(
                METRICS_PORT_PROPERTY.to_string(),
                Some(metrics_port.to_string()),
            );
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
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if file == HDFS_SITE_XML {
            if let Some(dfs_datanode_name_dir) = &self.dfs_datanode_name_dir {
                result.insert(
                    DFS_DATA_NODE_DATA_DIR.to_string(),
                    Some(dfs_datanode_name_dir.to_string()),
                );
            }

            if let Some(dfs_replication) = &self.dfs_replication {
                result.insert(
                    DFS_REPLICATION.to_string(),
                    Some(dfs_replication.to_string()),
                );
            }

            if let Some(ipc_address) = &self.ipc_address {
                result.insert(
                    DFS_DATA_NODE_IPC_ADDRESS.to_string(),
                    Some(ipc_address.to_string()),
                );
            }

            if let Some(http_address) = &self.http_address {
                result.insert(
                    DFS_DATA_NODE_HTTP_ADDRESS.to_string(),
                    Some(http_address.to_string()),
                );
            }

            if let Some(data_address) = &self.data_address {
                result.insert(
                    DFS_DATA_NODE_DATA_ADDRESS.to_string(),
                    Some(data_address.to_string()),
                );
            }
        }

        Ok(result)
    }
}

impl Configuration for JournalNodeConfig {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(metrics_port) = self.metrics_port {
            result.insert(
                METRICS_PORT_PROPERTY.to_string(),
                Some(metrics_port.to_string()),
            );
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
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if file == HDFS_SITE_XML {
            if let Some(http_address) = &self.http_address {
                result.insert(
                    DFS_JOURNAL_NODE_HTTP_ADDRESS.to_string(),
                    Some(http_address.to_string()),
                );
            }
            if let Some(https_address) = &self.https_address {
                result.insert(
                    DFS_JOURNAL_NODE_HTTPS_ADDRESS.to_string(),
                    Some(https_address.to_string()),
                );
            }
            if let Some(rpc_address) = &self.rpc_address {
                result.insert(
                    DFS_JOURNAL_NODE_RPC_ADDRESS.to_string(),
                    Some(rpc_address.to_string()),
                );
            }
        }

        Ok(result)
    }
}
