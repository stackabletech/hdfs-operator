//! Per-file builders for the HDFS config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`crate::config::writer`] module serializes maps to
//! the Hadoop-XML / Java-properties on-wire format.

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
