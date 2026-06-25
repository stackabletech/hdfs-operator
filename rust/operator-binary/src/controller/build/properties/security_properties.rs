//! Builds the `security.properties` (JVM security) config file.
//!
//! The operator injects recommended JVM DNS cache TTLs.
//! User `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::{PropertiesWriterError, to_java_properties_string},
    config_overrides::KeyValueConfigOverrides,
};

/// Renders `security.properties`: recommended DNS cache TTLs plus user overrides.
pub fn build(overrides: KeyValueConfigOverrides) -> Result<String, PropertiesWriterError> {
    // Recommended JVM DNS cache TTLs. Caching forever (the JVM default for
    // successful lookups) breaks failover when a NameNode's IP changes, so cap
    // both positive and negative caches.
    let mut config: BTreeMap<String, String> = BTreeMap::from([
        ("networkaddress.cache.ttl".to_string(), "30".to_string()),
        (
            "networkaddress.cache.negative.ttl".to_string(),
            "0".to_string(),
        ),
    ]);
    // Overrides applied last so users win.
    config.extend(overrides);
    to_java_properties_string(config.iter())
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn injects_recommended_dns_cache_ttls() {
        assert_eq!(
            build(KeyValueConfigOverrides::default()).unwrap(),
            indoc! {"
                networkaddress.cache.negative.ttl=0
                networkaddress.cache.ttl=30
            "}
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        assert_eq!(
            build([("networkaddress.cache.ttl", "60")].into()).unwrap(),
            indoc! {"
                networkaddress.cache.negative.ttl=0
                networkaddress.cache.ttl=60
            "}
        );
    }

    #[test]
    fn extra_overrides_are_appended() {
        assert_eq!(
            build([("foo.bar", "baz")].into()).unwrap(),
            indoc! {"
                foo.bar=baz
                networkaddress.cache.negative.ttl=0
                networkaddress.cache.ttl=30
            "}
        );
    }
}
