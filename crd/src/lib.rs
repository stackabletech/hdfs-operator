use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use semver::{SemVerError, Version};
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;
use strum_macros;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hdfs.stackable.tech",
    version = "v1",
    kind = "HdfsCluster",
    shortname = "hdfs",
    namespaced
)]
#[kube(status = "HdfsClusterStatus")]
pub struct HdfsClusterSpec {
    pub version: HdfsVersion,
    pub servers: Vec<HdfsServer>,
}

impl Crd for HdfsCluster {
    const RESOURCE_NAME: &'static str = "hdfsclusters.Hdfs.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/hdfscluster.crd.yaml");
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct HdfsServer {
    pub node_name: String,
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
}

impl HdfsVersion {
    pub fn is_valid_upgrade(&self, to: &Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&self.to_string())?;
        let to_version = Version::parse(&to.to_string())?;

        Ok(to_version > from_version)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HdfsClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<HdfsVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<HdfsVersion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "stackable_operator::conditions::schema")]
    pub conditions: Vec<Condition>,
}

impl HdfsClusterStatus {
    pub fn target_image_name(&self) -> Option<String> {
        match &self.target_version {
            None => None,
            Some(version) => Some(format!(
                "stackable/apache-hadoop:{}",
                serde_json::json!(version).as_str().unwrap()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::HdfsVersion;
    use std::str::FromStr;

    #[test]
    fn test_version_upgrade() {}

    #[test]
    fn test_version_conversion() {
        HdfsVersion::from_str("3.2.2").unwrap();
        HdfsVersion::from_str("1.2.3").unwrap_err();
    }
}
