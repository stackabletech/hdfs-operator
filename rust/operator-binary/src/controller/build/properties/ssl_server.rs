//! Builds the `ssl-server.xml` config file.
//!
//! When HTTPS is enabled the operator injects the keystore/truststore locations,
//! types and passwords; user `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

use crate::container::{TLS_STORE_DIR, TLS_STORE_PASSWORD};

/// Renders `ssl-server.xml` for the given HTTPS state and user overrides.
pub fn build(https_enabled: bool, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();
    if https_enabled {
        config.extend([
            (
                "ssl.server.truststore.location".to_string(),
                format!("{TLS_STORE_DIR}/truststore.p12"),
            ),
            (
                "ssl.server.truststore.type".to_string(),
                "pkcs12".to_string(),
            ),
            (
                "ssl.server.truststore.password".to_string(),
                TLS_STORE_PASSWORD.to_string(),
            ),
            (
                "ssl.server.keystore.location".to_string(),
                format!("{TLS_STORE_DIR}/keystore.p12"),
            ),
            ("ssl.server.keystore.type".to_string(), "pkcs12".to_string()),
            (
                "ssl.server.keystore.password".to_string(),
                TLS_STORE_PASSWORD.to_string(),
            ),
        ]);
    }
    // Overrides applied last so users win.
    config.extend(overrides);
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_https_without_overrides_renders_empty_configuration() {
        assert_eq!(
            build(false, KeyValueConfigOverrides::default()),
            concat!(
                "<?xml version=\"1.0\"?>\n",
                "<configuration>\n",
                "</configuration>"
            )
        );
    }

    #[test]
    fn enabled_https_injects_keystore_and_truststore() {
        let xml = build(true, KeyValueConfigOverrides::default());
        assert!(
            xml.contains(&format!(
                "<name>ssl.server.keystore.location</name>\n    <value>{TLS_STORE_DIR}/keystore.p12</value>"
            )),
            "{xml}"
        );
        assert!(
            xml.contains(&format!(
                "<name>ssl.server.truststore.password</name>\n    <value>{TLS_STORE_PASSWORD}</value>"
            )),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        let xml = build(true, [("ssl.server.keystore.type", "jks")].into());
        assert!(
            xml.contains("<name>ssl.server.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
