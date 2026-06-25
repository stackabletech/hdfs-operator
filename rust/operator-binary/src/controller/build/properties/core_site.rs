//! Builds the `core-site.xml` config file.

use std::collections::BTreeMap;

use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo,
    v2::{config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides},
};

use crate::{
    controller::{ValidatedCluster, build::kerberos::KerberosConfig},
    crd::{
        HdfsNodeRole,
        constants::{FS_DEFAULT_FS, HA_ZOOKEEPER_QUORUM, PROMETHEUS_ENDPOINT_ENABLED},
    },
};

/// Renders `core-site.xml`: operator defaults + kerberos/OPA security config,
/// with user `configOverrides` applied last.
pub fn build(
    cluster: &ValidatedCluster,
    role: HdfsNodeRole,
    cluster_info: &KubernetesClusterInfo,
    overrides: KeyValueConfigOverrides,
) -> String {
    let cluster_config = &cluster.cluster_config;
    let kerberos = KerberosConfig {
        cluster_name: cluster.name.as_ref(),
        cluster_namespace: cluster.namespace.as_ref(),
        authentication_enabled: cluster_config.authentication.is_some(),
        kerberos_enabled: cluster_config.authentication.is_some(),
        authorization_enabled: cluster_config.authorization.is_some(),
    };

    let mut core_site = CoreSiteConfigBuilder::new(cluster.name.as_ref().to_owned());
    core_site
        .fs_default_fs()
        .ha_zookeeper_quorum()
        .security_config(&kerberos, cluster_info)
        .enable_prometheus_endpoint()
        // The default (4096) hasn't changed since 2009.
        // Increase to 128k to allow for faster transfers.
        .add("io.file.buffer.size", "131072");
    // Rack awareness topology provider, namenode only.
    if role == HdfsNodeRole::Name && cluster_config.rack_awareness.is_some() {
        core_site.add(
            "net.topology.node.switch.mapping.impl",
            "tech.stackable.hadoop.StackableTopologyProvider",
        );
    }
    if let Some(opa_config) = &cluster_config.authorization {
        opa_config.add_core_site_config(&mut core_site);
    }
    // the extend with config must come last in order to have overrides working!!!
    core_site.extend(overrides).build_as_xml()
}

#[derive(Clone, Default)]
pub struct CoreSiteConfigBuilder {
    config: BTreeMap<String, String>,
    logical_name: String,
}

impl CoreSiteConfigBuilder {
    pub fn new(logical_name: String) -> Self {
        CoreSiteConfigBuilder {
            config: BTreeMap::new(),
            logical_name,
        }
    }

    pub fn add(&mut self, property: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.config.insert(property.into(), value.into());
        self
    }

    pub fn extend(&mut self, properties: impl IntoIterator<Item = (String, String)>) -> &mut Self {
        self.config.extend(properties);
        self
    }

    pub fn fs_default_fs(&mut self) -> &mut Self {
        self.config.insert(
            FS_DEFAULT_FS.to_string(),
            format!("hdfs://{}/", self.logical_name),
        );
        self
    }

    pub fn ha_zookeeper_quorum(&mut self) -> &mut Self {
        self.config.insert(
            HA_ZOOKEEPER_QUORUM.to_string(),
            "${env.ZOOKEEPER}".to_string(),
        );
        self
    }

    pub fn build_as_xml(&self) -> String {
        to_hadoop_xml(self.config.iter())
    }

    pub fn enable_prometheus_endpoint(&mut self) -> &mut Self {
        self.config
            .insert(PROMETHEUS_ENDPOINT_ENABLED.to_string(), "true".to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::controller::build::properties::test_support::{cluster_info, validated_cluster};

    #[test]
    fn renders_operator_defaults() {
        let xml = build(
            &validated_cluster(),
            HdfsNodeRole::Name,
            &cluster_info(),
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>fs.defaultFS</name>
                    <value>hdfs://hdfs/</value>"}),
            "{xml}"
        );
        assert!(
            xml.contains(indoc! {"
                <name>hadoop.prometheus.endpoint.enabled</name>
                    <value>true</value>"}),
            "{xml}"
        );
        assert!(
            xml.contains(indoc! {"
                <name>io.file.buffer.size</name>
                    <value>131072</value>"}),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_defaults() {
        let xml = build(
            &validated_cluster(),
            HdfsNodeRole::Name,
            &cluster_info(),
            [("io.file.buffer.size", "65536")].into(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>io.file.buffer.size</name>
                    <value>65536</value>"}),
            "{xml}"
        );
    }
}
