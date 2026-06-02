//! Builds the `security.properties` (JVM security) config file.
//!
//! The operator injects recommended JVM DNS cache TTLs (previously supplied via
//! the product-config `properties.yaml`); user `configOverrides` are applied on
//! top.

use std::collections::BTreeMap;

use crate::{
    config::writer::{PropertiesWriterError, to_java_properties_string},
    controller::build::properties::optional_values,
};

/// Renders `security.properties`: recommended DNS cache TTLs plus user overrides.
pub fn build(overrides: &BTreeMap<String, String>) -> Result<String, PropertiesWriterError> {
    // Recommended JVM DNS cache TTLs. Caching forever (the JVM default for
    // successful lookups) breaks failover when a NameNode's IP changes, so cap
    // both positive and negative caches.
    let mut config: BTreeMap<String, Option<String>> = BTreeMap::from([
        (
            "networkaddress.cache.ttl".to_string(),
            Some("30".to_string()),
        ),
        (
            "networkaddress.cache.negative.ttl".to_string(),
            Some("0".to_string()),
        ),
    ]);
    // Overrides applied last so users win.
    config.extend(optional_values(overrides));
    to_java_properties_string(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_recommended_dns_cache_ttls() {
        assert_eq!(
            build(&BTreeMap::new()).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=30\n"
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        let overrides =
            BTreeMap::from([("networkaddress.cache.ttl".to_string(), "60".to_string())]);
        assert_eq!(
            build(&overrides).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=60\n"
        );
    }

    #[test]
    fn extra_overrides_are_appended() {
        let overrides = BTreeMap::from([("foo.bar".to_string(), "baz".to_string())]);
        assert_eq!(
            build(&overrides).unwrap(),
            "foo.bar=baz\nnetworkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=30\n"
        );
    }
}
