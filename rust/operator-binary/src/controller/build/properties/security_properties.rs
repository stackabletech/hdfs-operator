//! Builds the `security.properties` (JVM security) config file.
//!
//! The operator sets no defaults here; the file exists purely so users can
//! supply `configOverrides` (e.g. DNS cache TTLs).

use std::collections::BTreeMap;

use crate::{
    config::writer::{PropertiesWriterError, to_java_properties_string},
    controller::build::properties::optional_values,
};

/// Renders `security.properties` from the user-provided overrides only.
pub fn build(overrides: &BTreeMap<String, String>) -> Result<String, PropertiesWriterError> {
    to_java_properties_string(optional_values(overrides).iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_overrides_render_empty_string() {
        assert_eq!(build(&BTreeMap::new()).unwrap(), "");
    }

    #[test]
    fn overrides_are_rendered_sorted() {
        let overrides = BTreeMap::from([
            (
                "networkaddress.cache.negative.ttl".to_string(),
                "0".to_string(),
            ),
            ("networkaddress.cache.ttl".to_string(), "30".to_string()),
        ]);
        assert_eq!(
            build(&overrides).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=30\n"
        );
    }
}
