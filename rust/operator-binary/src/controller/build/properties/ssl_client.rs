//! Builds the `ssl-client.xml` config file.
//!
//! When HTTPS is enabled the operator injects the truststore location, type and
//! password; user `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

use crate::{
    container::{TLS_STORE_DIR, TLS_STORE_PASSWORD},
    controller::build::properties::resolved_overrides,
};

/// Renders `ssl-client.xml` for the given HTTPS state and user overrides.
pub fn build(https_enabled: bool, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();
    if https_enabled {
        config.extend([
            (
                "ssl.client.truststore.location".to_string(),
                format!("{TLS_STORE_DIR}/truststore.p12"),
            ),
            (
                "ssl.client.truststore.type".to_string(),
                "pkcs12".to_string(),
            ),
            (
                "ssl.client.truststore.password".to_string(),
                TLS_STORE_PASSWORD.to_string(),
            ),
        ]);
    }
    // Overrides applied last so users win.
    config.extend(resolved_overrides(overrides));
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::config_overrides;

    #[test]
    fn disabled_https_without_overrides_renders_empty_configuration() {
        assert_eq!(
            build(false, config_overrides(&[])),
            concat!(
                "<?xml version=\"1.0\"?>\n",
                "<configuration>\n",
                "</configuration>"
            )
        );
    }

    #[test]
    fn enabled_https_injects_truststore() {
        let xml = build(true, config_overrides(&[]));
        assert!(
            xml.contains(&format!(
                "<name>ssl.client.truststore.location</name>\n    <value>{TLS_STORE_DIR}/truststore.p12</value>"
            )),
            "{xml}"
        );
        assert!(
            xml.contains(&format!(
                "<name>ssl.client.truststore.password</name>\n    <value>{TLS_STORE_PASSWORD}</value>"
            )),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        let xml = build(
            true,
            config_overrides(&[("ssl.client.truststore.type", "jks")]),
        );
        assert!(
            xml.contains("<name>ssl.client.truststore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
