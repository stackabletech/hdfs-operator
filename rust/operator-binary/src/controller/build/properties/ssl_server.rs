//! Builds the `ssl-server.xml` config file.
//!
//! When HTTPS is enabled the operator injects the keystore/truststore locations,
//! types and passwords; user `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

use crate::controller::build::{
    container::{TLS_STORE_DIR, TLS_STORE_PASSWORD},
    properties::{KEYSTORE_TYPE_PKCS12, truststore_entries},
};

/// Renders `ssl-server.xml` for the given HTTPS state and user overrides.
pub fn build(https_enabled: bool, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();
    if https_enabled {
        config.extend(truststore_entries("ssl.server"));
        config.extend([
            (
                "ssl.server.keystore.location".to_string(),
                format!("{TLS_STORE_DIR}/keystore.p12"),
            ),
            (
                "ssl.server.keystore.type".to_string(),
                KEYSTORE_TYPE_PKCS12.to_string(),
            ),
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
    use indoc::{formatdoc, indoc};

    use super::*;
    use crate::controller::build::properties::test_support::EMPTY_HADOOP_XML;

    #[test]
    fn disabled_https_without_overrides_renders_empty_configuration() {
        assert_eq!(
            build(false, KeyValueConfigOverrides::default()),
            EMPTY_HADOOP_XML
        );
    }

    #[test]
    fn enabled_https_injects_keystore_and_truststore() {
        let xml = build(true, KeyValueConfigOverrides::default());
        assert!(
            xml.contains(&formatdoc! {"
                <name>ssl.server.keystore.location</name>
                    <value>{TLS_STORE_DIR}/keystore.p12</value>"}),
            "{xml}"
        );
        assert!(
            xml.contains(&formatdoc! {"
                <name>ssl.server.truststore.password</name>
                    <value>{TLS_STORE_PASSWORD}</value>"}),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        let xml = build(true, [("ssl.server.keystore.type", "jks")].into());
        assert!(
            xml.contains(indoc! {"
                <name>ssl.server.keystore.type</name>
                    <value>jks</value>"}),
            "{xml}"
        );
    }
}
