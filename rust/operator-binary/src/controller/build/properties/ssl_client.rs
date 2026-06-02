//! Builds the `ssl-client.xml` config file.
//!
//! When HTTPS is enabled the operator injects the truststore location, type and
//! password; user `configOverrides` are applied on top.

use std::collections::BTreeMap;

use crate::{
    config::writer::to_hadoop_xml,
    container::{TLS_STORE_DIR, TLS_STORE_PASSWORD},
    controller::build::properties::optional_values,
};

/// Renders `ssl-client.xml` for the given HTTPS state and user overrides.
pub fn build(https_enabled: bool, overrides: &BTreeMap<String, String>) -> String {
    let mut config: BTreeMap<String, Option<String>> = BTreeMap::new();
    if https_enabled {
        config.extend([
            (
                "ssl.client.truststore.location".to_string(),
                Some(format!("{TLS_STORE_DIR}/truststore.p12")),
            ),
            (
                "ssl.client.truststore.type".to_string(),
                Some("pkcs12".to_string()),
            ),
            (
                "ssl.client.truststore.password".to_string(),
                Some(TLS_STORE_PASSWORD.to_string()),
            ),
        ]);
    }
    // Overrides applied last so users win.
    config.extend(optional_values(overrides));
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_https_without_overrides_renders_empty_configuration() {
        assert_eq!(
            build(false, &BTreeMap::new()),
            "<?xml version=\"1.0\"?>\n<configuration>\n</configuration>"
        );
    }

    #[test]
    fn enabled_https_injects_truststore() {
        let xml = build(true, &BTreeMap::new());
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
        let overrides =
            BTreeMap::from([("ssl.client.truststore.type".to_string(), "jks".to_string())]);
        let xml = build(true, &overrides);
        assert!(
            xml.contains("<name>ssl.client.truststore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
