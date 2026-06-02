//! Builds the `core-site.xml` config file.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::config_overrides::KeyValueConfigOverrides,
};

use crate::{
    config::CoreSiteConfigBuilder,
    controller::build::properties::resolved_overrides,
    crd::HdfsNodeRole,
    hdfs_controller::ValidatedClusterConfig,
    security::kerberos::{self, KerberosConfig},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build security config"))]
    BuildSecurityConfig { source: kerberos::Error },
}

/// Renders `core-site.xml`: operator defaults + kerberos/OPA security config,
/// with user `configOverrides` applied last.
pub fn build(
    cluster_config: &ValidatedClusterConfig,
    role: HdfsNodeRole,
    cluster_info: &KubernetesClusterInfo,
    overrides: KeyValueConfigOverrides,
) -> Result<String, Error> {
    let kerberos = KerberosConfig {
        cluster_name: &cluster_config.name,
        cluster_namespace: cluster_config.namespace.as_deref(),
        authentication_enabled: cluster_config.authentication_enabled,
        kerberos_enabled: cluster_config.kerberos_enabled,
        authorization_enabled: cluster_config.authorization_enabled,
    };

    let mut core_site = CoreSiteConfigBuilder::new(cluster_config.name.clone());
    core_site
        .fs_default_fs()
        .ha_zookeeper_quorum()
        .security_config(&kerberos, cluster_info)
        .context(BuildSecurityConfigSnafu)?
        .enable_prometheus_endpoint()
        // The default (4096) hasn't changed since 2009.
        // Increase to 128k to allow for faster transfers.
        .add("io.file.buffer.size", "131072");
    // Rack awareness topology provider, namenode only. Previously injected via
    // the product-config `Configuration::compute_files`.
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
    let overrides: BTreeMap<String, String> = resolved_overrides(overrides).collect();
    Ok(core_site.extend(&overrides).build_as_xml())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::{
        cluster_info, config_overrides, validated_cluster_config,
    };

    #[test]
    fn renders_operator_defaults() {
        let xml = build(
            &validated_cluster_config(),
            HdfsNodeRole::Name,
            &cluster_info(),
            config_overrides(&[]),
        )
        .unwrap();
        assert!(
            xml.contains("<name>fs.defaultFS</name>\n    <value>hdfs://hdfs/</value>"),
            "{xml}"
        );
        assert!(
            xml.contains(
                "<name>hadoop.prometheus.endpoint.enabled</name>\n    <value>true</value>"
            ),
            "{xml}"
        );
        assert!(
            xml.contains("<name>io.file.buffer.size</name>\n    <value>131072</value>"),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_defaults() {
        let xml = build(
            &validated_cluster_config(),
            HdfsNodeRole::Name,
            &cluster_info(),
            config_overrides(&[("io.file.buffer.size", "65536")]),
        )
        .unwrap();
        assert!(
            xml.contains("<name>io.file.buffer.size</name>\n    <value>65536</value>"),
            "{xml}"
        );
    }
}
