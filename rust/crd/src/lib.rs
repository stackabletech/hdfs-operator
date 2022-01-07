pub mod constants;
pub mod error;

use constants::*;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::{CommonConfiguration, Role, RoleGroupRef};
use stackable_operator::schemars::{self, JsonSchema};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
use strum_macros::EnumIter;
use error::{HdfsOperatorResult, Error};
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
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
pub enum HdfsRole {
    #[strum(serialize = "namenode")]
    NameNode,
    #[strum(serialize = "datanode")]
    DataNode,
    #[strum(serialize = "journalnode")]
    JournalNode,
}

impl HdfsCluster {

    pub fn version(&self) -> HdfsOperatorResult<&str> {
        self.spec.version.as_deref().ok_or(Error::ObjectHasNoVersion {
            obj_ref: ObjectRef::from_obj(self),
        })
    }

    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.server_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// Metadata about a server rolegroup
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
                    .map_err(|e| error::Error::HdfsAddressPortParseError {
                        port: port.to_string(),
                        reason: e.to_string(),
                    })?,
            });
        }

        Err(error::Error::HdfsAddressParseError {
            address: address.clone(),
        })
    }
}

impl HdfsRole {
    /// Returns the container start command for a HDFS node
    /// Right now works only for images using hadoop2.7
    /// # Arguments
    ///
    /// * `version` - Current specified cluster version
    /// * `auto_format_fs` - Format directory via 'start-namenode' script
    ///
    pub fn get_command(&self, auto_format_fs: bool) -> Vec<String> {
        match &self {
            HdfsRole::DataNode => vec![
                "bin/hdfs".to_string(),
                "--config".to_string(),
                CONFIG_DIR_NAME.to_string(),
                "datanode".to_string(),
            ],
            HdfsRole::NameNode => {
                if auto_format_fs {
                    vec![
                        "bin/start-namenode".to_string(),
                        "--config".to_string(),
                        CONFIG_DIR_NAME.to_string(),
                    ]
                } else {
                    vec![
                        "bin/hdfs".to_string(),
                        "--config".to_string(),
                        CONFIG_DIR_NAME.to_string(),
                        "namenode".to_string(),
                    ]
                }
            }
            HdfsRole::JournalNode => vec!["bin/hdfs".to_string(), "journalnode".to_string()],
        }
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
