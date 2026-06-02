//! Per-file builders for the HDFS config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`crate::config::writer`] module serializes maps to
//! the Hadoop-XML / Java-properties on-wire format.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

pub mod core_site;
pub mod hadoop_policy;
pub mod hdfs_site;
pub mod security_properties;
pub mod ssl_client;
pub mod ssl_server;

/// Keep only the set (`Some`) entries of a `key -> optional value` map, as `(key, value)` pairs.
fn defined_entries(
    entries: BTreeMap<String, Option<String>>,
) -> impl Iterator<Item = (String, String)> {
    entries
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
}

/// Resolve user-provided [`KeyValueConfigOverrides`] into the key/value pairs to merge
/// into a config file, dropping entries whose value is unset (`None`).
fn resolved_overrides(
    overrides: KeyValueConfigOverrides,
) -> impl Iterator<Item = (String, String)> {
    defined_entries(overrides.overrides)
}

/// The names of the HDFS config files assembled into the rolegroup `ConfigMap`.
#[derive(Clone, Copy, Debug, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "hdfs-site.xml")]
    HdfsSite,
    #[strum(serialize = "core-site.xml")]
    CoreSite,
    #[strum(serialize = "hadoop-policy.xml")]
    HadoopPolicy,
    #[strum(serialize = "ssl-server.xml")]
    SslServer,
    #[strum(serialize = "ssl-client.xml")]
    SslClient,
    #[strum(serialize = "security.properties")]
    Security,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_match_the_hadoop_on_disk_names() {
        assert_eq!(ConfigFileName::HdfsSite.to_string(), "hdfs-site.xml");
        assert_eq!(ConfigFileName::CoreSite.to_string(), "core-site.xml");
        assert_eq!(ConfigFileName::HadoopPolicy.to_string(), "hadoop-policy.xml");
        assert_eq!(ConfigFileName::SslServer.to_string(), "ssl-server.xml");
        assert_eq!(ConfigFileName::SslClient.to_string(), "ssl-client.xml");
        assert_eq!(ConfigFileName::Security.to_string(), "security.properties");
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::{
        commons::networking::DomainName, utils::cluster_info::KubernetesClusterInfo,
        v2::config_overrides::KeyValueConfigOverrides,
    };

    use crate::crd::v1alpha1;

    /// Builds a [`KeyValueConfigOverrides`] from `(key, value)` pairs for tests.
    pub fn config_overrides(pairs: &[(&str, &str)]) -> KeyValueConfigOverrides {
        KeyValueConfigOverrides {
            overrides: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect(),
        }
    }

    /// A minimal three-role HdfsCluster used to drive the per-file builder tests.
    pub const MINIMAL_HDFS_YAML: &str = r#"
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
  nameNodes:
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1
  dataNodes:
    roleGroups:
      default:
        replicas: 1
"#;

    pub fn minimal_hdfs() -> v1alpha1::HdfsCluster {
        serde_yaml::from_str(MINIMAL_HDFS_YAML).expect("invalid test HdfsCluster YAML")
    }

    pub fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }
}
