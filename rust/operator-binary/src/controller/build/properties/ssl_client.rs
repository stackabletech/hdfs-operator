//! Builds the `ssl-client.xml` config file.
//!
//! When HTTPS is enabled the operator injects the truststore location, type and
//! password; user `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

use crate::controller::build::properties::truststore_entries;

/// Renders `ssl-client.xml` for the given HTTPS state and user overrides.
pub fn build(https_enabled: bool, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();
    if https_enabled {
        config.extend(truststore_entries("ssl.client"));
    }
    // Overrides applied last so users win.
    config.extend(overrides);
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::{
        container::{TLS_STORE_DIR, TLS_STORE_PASSWORD},
        properties::test_support::EMPTY_HADOOP_XML,
    };

    #[test]
    fn disabled_https_without_overrides_renders_empty_configuration() {
        assert_eq!(
            build(false, KeyValueConfigOverrides::default()),
            EMPTY_HADOOP_XML
        );
    }

    #[test]
    fn enabled_https_injects_truststore() {
        let xml = build(true, KeyValueConfigOverrides::default());
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
        let xml = build(true, [("ssl.client.truststore.type", "jks")].into());
        assert!(
            xml.contains("<name>ssl.client.truststore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
