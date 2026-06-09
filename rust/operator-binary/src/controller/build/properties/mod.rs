//! Per-file builders for the HDFS config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`stackable_operator::v2::config_file_writer`]
//! module serializes maps to the Hadoop-XML / Java-properties on-wire format.

pub mod core_site;
pub mod hadoop_policy;
pub mod hdfs_site;
pub mod logging;
pub mod security_properties;
pub mod ssl_client;
pub mod ssl_server;

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
        assert_eq!(
            ConfigFileName::HadoopPolicy.to_string(),
            "hadoop-policy.xml"
        );
        assert_eq!(ConfigFileName::SslServer.to_string(), "ssl-server.xml");
        assert_eq!(ConfigFileName::SslClient.to_string(), "ssl-client.xml");
        assert_eq!(ConfigFileName::Security.to_string(), "security.properties");
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::{
        commons::networking::DomainName, utils::cluster_info::KubernetesClusterInfo,
    };

    use crate::{
        controller::validate::validate_cluster, crd::v1alpha1, hdfs_controller::ValidatedCluster,
    };

    /// A minimal three-role HdfsCluster used to drive the per-file builder tests.
    pub const MINIMAL_HDFS_YAML: &str = r#"
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
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

    pub fn validated_cluster() -> ValidatedCluster {
        validate_cluster(&minimal_hdfs(), "oci.example.org", None)
            .expect("validate should succeed for the minimal fixture")
    }
}
