use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use kube_derive::CustomResource;
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use semver::{SemVerError, Version};
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use stackable_operator::Crd;
use std::collections::HashMap;
use strum_macros;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    pub namenode: NodeGroup<HdfsConfig>,
    pub datanode: NodeGroup<HdfsConfig>,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct NodeGroup<T> {
    pub selectors: HashMap<String, SelectorAndConfig<HdfsConfig>>,
    pub config: T,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct SelectorAndConfig<T> {
    pub instances: u8,
    pub instances_per_node: u8,
    pub config: T,
    #[schemars(schema_with = "schema")]
    pub selector: Option<LabelSelector>,
}

#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct HdfsConfig {}

impl Crd for HdfsCluster {
    const RESOURCE_NAME: &'static str = "hdfsclusters.Hdfs.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../hdfscluster.crd.yaml");
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

pub fn schema(_: &mut SchemaGenerator) -> Schema {
    from_value(json!({
      "description": "A label selector is a label query over a set of resources. The result of matchLabels and matchExpressions are ANDed. An empty label selector matches all objects. A null label selector matches no objects.",
      "properties": {
        "matchExpressions": {
          "description": "matchExpressions is a list of label selector requirements. The requirements are ANDed.",
          "items": {
            "description": "A label selector requirement is a selector that contains values, a key, and an operator that relates the key and values.",
            "properties": {
              "key": {
                "description": "key is the label key that the selector applies to.",
                "type": "string",
                "x-kubernetes-patch-merge-key": "key",
                "x-kubernetes-patch-strategy": "merge"
              },
              "operator": {
                "description": "operator represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists and DoesNotExist.",
                "type": "string"
              },
              "values": {
                "description": "values is an array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. This array is replaced during a strategic merge patch.",
                "items": {
                  "type": "string"
                },
                "type": "array"
              }
            },
            "required": [
              "key",
              "operator"
            ],
            "type": "object"
          },
          "type": "array"
        },
        "matchLabels": {
          "additionalProperties": {
            "type": "string"
          },
          "description": "matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels map is equivalent to an element of matchExpressions, whose key field is \"key\", the operator is \"In\", and the values array contains only \"value\". The requirements are ANDed.",
          "type": "object"
        }
      },
      "type": "object"
    })).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HdfsClusterSpec, HdfsVersion, SelectorAndConfig};
    use std::error::Error;
    use std::str::FromStr;

    #[test]
    fn print_crd() {
        let test = include_str!("../testdata/simple_cluster.yaml");
        let schema = HdfsCluster::crd();
        let string_schema = serde_yaml::to_string(&schema).unwrap();
        println!("HdfsCluster CRD:\n{}\n", string_schema);
    }

    #[test]
    fn print_schema() {
        let schema = schema(&mut SchemaGenerator::default());

        let string_schema = serde_yaml::to_string(&schema).unwrap();
        println!("LabelSelector Schema:\n{}\n", string_schema);
    }
}
