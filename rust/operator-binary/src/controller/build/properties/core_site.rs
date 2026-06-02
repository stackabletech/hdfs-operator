//! Builds the `core-site.xml` config file.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::utils::cluster_info::KubernetesClusterInfo;

use crate::{
    config::CoreSiteConfigBuilder,
    crd::v1alpha1,
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
    cluster_info: &KubernetesClusterInfo,
    opa_config: Option<&HdfsOpaConfig>,
    overrides: &BTreeMap<String, String>,
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
    if let Some(opa_config) = opa_config {
        opa_config.add_core_site_config(&mut core_site);
    }
    // the extend with config must come last in order to have overrides working!!!
    Ok(core_site.extend(overrides).build_as_xml())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::{cluster_info, minimal_hdfs};

    #[test]
    fn renders_operator_defaults() {
        let hdfs = minimal_hdfs();
        let xml = build(&hdfs, "hdfs", &cluster_info(), None, &BTreeMap::new()).unwrap();
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
        let overrides =
            BTreeMap::from([("io.file.buffer.size".to_string(), "65536".to_string())]);
        let xml = build(&hdfs, "hdfs", &cluster_info(), None, &overrides).unwrap();
        assert!(
            xml.contains("<name>io.file.buffer.size</name>\n    <value>65536</value>"),
            "{xml}"
        );
    }
}
