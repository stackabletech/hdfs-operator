//! Per-file builders for the HDFS config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`stackable_operator::v2::config_file_writer`]
//! module serializes maps to the Hadoop-XML / Java-properties on-wire format.

use crate::controller::build::container::{TLS_STORE_DIR, TLS_STORE_PASSWORD};

pub mod core_site;
pub mod hadoop_policy;
pub mod hdfs_site;
pub mod product_logging;
pub mod security_properties;
pub mod ssl_client;
pub mod ssl_server;

/// The `<prefix>.truststore.*` entries (location, type, password) injected into
/// `ssl-client.xml` / `ssl-server.xml` when HTTPS is enabled.
fn truststore_entries(prefix: &str) -> [(String, String); 3] {
    [
        (
            format!("{prefix}.truststore.location"),
            format!("{TLS_STORE_DIR}/truststore.p12"),
        ),
        (format!("{prefix}.truststore.type"), "pkcs12".to_string()),
        (
            format!("{prefix}.truststore.password"),
            TLS_STORE_PASSWORD.to_string(),
        ),
    ]
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

    use crate::{controller::ValidatedCluster, test_support::deserialize_and_validate_cluster};

    /// The rendered output of an empty Hadoop-XML configuration (no entries).
    pub const EMPTY_HADOOP_XML: &str = concat!(
        "<?xml version=\"1.0\"?>\n",
        "<configuration>\n",
        "</configuration>"
    );

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

    pub fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }

    pub fn validated_cluster() -> ValidatedCluster {
        deserialize_and_validate_cluster(MINIMAL_HDFS_YAML)
    }
}
