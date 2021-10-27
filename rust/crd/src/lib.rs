pub mod commands;
pub mod discovery;
pub mod error;

use crate::commands::{Format, Restart, Start, Stop};

use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_operator::command::{CommandRef, HasCommands, HasRoleRestartOrder};
use stackable_operator::controller::HasOwned;
use stackable_operator::crd::HasApplication;
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::k8s_openapi::schemars::_serde_json::Value;
use stackable_operator::kube::api::ApiResource;
use stackable_operator::kube::CustomResource;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::schemars::{self, JsonSchema};
use stackable_operator::status::{
    ClusterExecutionStatus, Conditions, HasClusterExecutionStatus, HasCurrentCommand, Status,
    Versioned,
};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "hdfs";
pub const MANAGED_BY: &str = "hdfs-operator";

pub const DFS_NAME_NODE_NAME_DIR: &str = "dfs.namenode.name.dir";
pub const DFS_DATA_NODE_DATA_DIR: &str = "dfs.datanode.data.dir";
pub const DFS_REPLICATION: &str = "dfs.replication";

pub const FS_DEFAULT: &str = "fs.defaultFS";
pub const DFS_NAME_NODE_HTTP_ADDRESS: &str = "dfs.namenode.http-address";

pub const DFS_DATA_NODE_IPC_ADDRESS: &str = "dfs.datanode.ipc.address";
pub const DFS_DATA_NODE_HTTP_ADDRESS: &str = "dfs.datanode.http.address";
pub const DFS_DATA_NODE_ADDRESS: &str = "dfs.datanode.address";

pub const METRICS_PORT_PROPERTY: &str = "metricsPort";
pub const JAVA_HOME: &str = "JAVA_HOME";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";
pub const CONFIG_DIR_NAME: &str = "conf";

pub const HDFS_SITE_XML: &str = "hdfs-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";

pub const IPC_PORT: &str = "ipc";
pub const HTTP_PORT: &str = "http";
pub const DATA_PORT: &str = "data";
pub const METRICS_PORT: &str = "metrics";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hdfs.stackable.tech",
    version = "v1alpha1",
    kind = "HdfsCluster",
    plural = "hdfsclusters",
    shortname = "hdfs",
    status = "HdfsClusterStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterSpec {
    pub version: HdfsVersion,
    pub auto_format_fs: Option<bool>,
    pub data_nodes: Role<DataNodeConfig>,
    pub name_nodes: Role<NameNodeConfig>,
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
pub enum HdfsRole {
    #[strum(serialize = "namenode")]
    NameNode,
    #[strum(serialize = "datanode")]
    DataNode,
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
    pub fn get_command(&self, version: &HdfsVersion, auto_format_fs: bool) -> Vec<String> {
        match &self {
            HdfsRole::DataNode => vec![
                format!("{}/bin/hdfs", version.package_name()),
                "--config".to_string(),
                format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
                "datanode".to_string(),
            ],
            HdfsRole::NameNode => {
                if auto_format_fs {
                    vec![
                        format!("{}/stackable/bin/start-namenode", version.package_name()),
                        "--config".to_string(),
                        format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
                    ]
                } else {
                    vec![
                        format!("{}/bin/hdfs", version.package_name()),
                        "--config".to_string(),
                        format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
                        "namenode".to_string(),
                    ]
                }
            }
        }
    }
}

impl Status<HdfsClusterStatus> for HdfsCluster {
    fn status(&self) -> &Option<HdfsClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<HdfsClusterStatus> {
        &mut self.status
    }
}

impl HasRoleRestartOrder for HdfsCluster {
    fn get_role_restart_order() -> Vec<String> {
        vec![
            HdfsRole::DataNode.to_string(),
            HdfsRole::NameNode.to_string(),
        ]
    }
}

impl HasCommands for HdfsCluster {
    fn get_command_types() -> Vec<ApiResource> {
        vec![
            Start::api_resource(),
            Stop::api_resource(),
            Restart::api_resource(),
            Format::api_resource(),
        ]
    }
}

impl HasOwned for HdfsCluster {
    fn owned_objects() -> Vec<&'static str> {
        vec![Restart::crd_name(), Start::crd_name(), Stop::crd_name()]
    }
}

impl HasApplication for HdfsCluster {
    fn get_application_name() -> &'static str {
        APP_NAME
    }
}

impl HasClusterExecutionStatus for HdfsCluster {
    fn cluster_execution_status(&self) -> Option<ClusterExecutionStatus> {
        self.status
            .as_ref()
            .and_then(|status| status.cluster_execution_status.clone())
    }

    fn cluster_execution_status_patch(&self, execution_status: &ClusterExecutionStatus) -> Value {
        json!({ "clusterExecutionStatus": execution_status })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NameNodeConfig {
    pub dfs_namenode_name_dir: Option<String>,
    pub dfs_replication: Option<u8>,
    pub java_home: Option<String>,
    pub metrics_port: Option<u16>,
    pub ipc_address: Option<HdfsAddress>,
    pub http_address: Option<HdfsAddress>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataNodeConfig {
    pub dfs_datanode_name_dir: Option<String>,
    pub dfs_replication: Option<u8>,
    pub java_home: Option<String>,
    pub metrics_port: Option<u16>,
    pub ipc_address: Option<HdfsAddress>,
    pub http_address: Option<HdfsAddress>,
    pub data_address: Option<HdfsAddress>,
}

impl Configuration for NameNodeConfig {
    type Configurable = HdfsCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(java_home) = &self.java_home {
            result.insert(JAVA_HOME.to_string(), Some(java_home.to_string()));
        }

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

        if let Some(java_home) = &self.java_home {
            result.insert(JAVA_HOME.to_string(), Some(java_home.to_string()));
        }

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
                    DFS_DATA_NODE_ADDRESS.to_string(),
                    Some(data_address.to_string()),
                );
            }
        }

        Ok(result)
    }
}

#[allow(non_camel_case_types)]
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum HdfsVersion {
    #[serde(rename = "3.2.2")]
    #[strum(serialize = "3.2.2")]
    v3_2_2,

    #[serde(rename = "3.3.1")]
    #[strum(serialize = "3.3.1")]
    v3_3_1,
}

impl HdfsVersion {
    pub fn package_name(&self) -> String {
        format!("hadoop-{}", self.to_string())
    }
}

impl Versioning for HdfsVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            //Ordering::Greater => VersioningState::ValidUpgrade,
            //Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
            _ => VersioningState::Invalid(
                "Upgrading or Downgrading HDFS is not supported at this time!".to_string(),
            ),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<HdfsVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CommandRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
}

impl Versioned<HdfsVersion> for HdfsClusterStatus {
    fn version(&self) -> &Option<ProductVersion<HdfsVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<HdfsVersion>> {
        &mut self.version
    }
}

impl Conditions for HdfsClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

impl HasCurrentCommand for HdfsClusterStatus {
    fn current_command(&self) -> Option<CommandRef> {
        self.current_command.clone()
    }
    fn set_current_command(&mut self, command: CommandRef) {
        self.current_command = Some(command);
    }
    fn clear_current_command(&mut self) {
        self.current_command = None
    }
    fn tracking_location() -> &'static str {
        "/status/currentCommand"
    }
}

#[cfg(test)]
mod tests {
    use crate::HdfsVersion;
    use stackable_operator::versioning::{Versioning, VersioningState};
    use std::str::FromStr;

    #[test]
    fn test_hdfs_version_versioning() {
        // TODO: adapt if versioning is working
        assert_eq!(
            HdfsVersion::v3_2_2.versioning_state(&HdfsVersion::v3_3_1),
            VersioningState::Invalid(
                "Upgrading or Downgrading HDFS is not supported at this time!".to_string()
            )
        );
        assert_eq!(
            HdfsVersion::v3_3_1.versioning_state(&HdfsVersion::v3_2_2),
            VersioningState::Invalid(
                "Upgrading or Downgrading HDFS is not supported at this time!".to_string()
            )
        );
        assert_eq!(
            HdfsVersion::v3_3_1.versioning_state(&HdfsVersion::v3_3_1),
            VersioningState::NoOp
        );
    }

    #[test]
    fn test_version_conversion() {
        HdfsVersion::from_str("3.2.2").unwrap();
        HdfsVersion::from_str("3.3.1").unwrap();
        HdfsVersion::from_str("1.2.3").unwrap_err();
    }

    #[test]
    fn test_package_name() {
        assert_eq!(
            HdfsVersion::v3_3_1.package_name(),
            format!("hadoop-{}", HdfsVersion::v3_3_1.to_string())
        );
    }
}
