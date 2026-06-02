//! Builds the `core-site.xml` config file.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::config_overrides::KeyValueConfigOverrides,
};

use crate::{
    config::CoreSiteConfigBuilder,
    controller::build::properties::resolved_overrides,
    crd::{HdfsNodeRole, v1alpha1},
    security::{kerberos, opa::HdfsOpaConfig},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build security config"))]
    BuildSecurityConfig { source: kerberos::Error },
}

/// Renders `core-site.xml`: operator defaults + kerberos/OPA security config,
/// with user `configOverrides` applied last.
pub fn build(
    hdfs: &v1alpha1::HdfsCluster,
    hdfs_name: &str,
    role: HdfsNodeRole,
    cluster_info: &KubernetesClusterInfo,
    opa_config: Option<&HdfsOpaConfig>,
    overrides: KeyValueConfigOverrides,
) -> Result<String, Error> {
    let mut core_site = CoreSiteConfigBuilder::new(hdfs_name.to_string());
    core_site
        .fs_default_fs()
        .ha_zookeeper_quorum()
        .security_config(hdfs, cluster_info)
        .context(BuildSecurityConfigSnafu)?
        .enable_prometheus_endpoint()
        // The default (4096) hasn't changed since 2009.
        // Increase to 128k to allow for faster transfers.
        .add("io.file.buffer.size", "131072");
    // Rack awareness topology provider, namenode only. Previously injected via
    // the product-config `Configuration::compute_files`.
    if role == HdfsNodeRole::Name && hdfs.rackawareness_config().is_some() {
        core_site.add(
            "net.topology.node.switch.mapping.impl",
            "tech.stackable.hadoop.StackableTopologyProvider",
        );
    }
    if let Some(opa_config) = opa_config {
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
        cluster_info, config_overrides, minimal_hdfs,
    };

    #[test]
    fn renders_operator_defaults() {
        let hdfs = minimal_hdfs();
        let xml = build(
            &hdfs,
            "hdfs",
            HdfsNodeRole::Name,
            &cluster_info(),
            None,
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
        let hdfs = minimal_hdfs();
        let xml = build(
            &hdfs,
            "hdfs",
            HdfsNodeRole::Name,
            &cluster_info(),
            None,
            config_overrides(&[("io.file.buffer.size", "65536")]),
        )
        .unwrap();
        assert!(
            xml.contains("<name>io.file.buffer.size</name>\n    <value>65536</value>"),
            "{xml}"
        );
    }
}
