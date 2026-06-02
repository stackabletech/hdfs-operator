//! Builds the `hadoop-policy.xml` config file.
//!
//! The operator sets no defaults here; the file exists purely so users can
//! supply `configOverrides`.

use std::collections::BTreeMap;

use crate::{config::writer::to_hadoop_xml, controller::build::properties::optional_values};

/// Renders `hadoop-policy.xml` from the user-provided overrides only.
pub fn build(overrides: &BTreeMap<String, String>) -> String {
    to_hadoop_xml(optional_values(overrides).iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_overrides_render_empty_configuration() {
        assert_eq!(
            build(&BTreeMap::new()),
            "<?xml version=\"1.0\"?>\n<configuration>\n</configuration>"
        );
    }

    #[test]
    fn overrides_are_rendered_as_properties() {
        let overrides = BTreeMap::from([(
            "security.client.protocol.acl".to_string(),
            "*".to_string(),
        )]);
        assert_eq!(
            build(&overrides),
            "<?xml version=\"1.0\"?>\n<configuration>\n  \
             <property>\n    <name>security.client.protocol.acl</name>\n    \
             <value>*</value>\n  </property>\n</configuration>"
        );
    }
}
