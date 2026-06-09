//! Builds the `hadoop-policy.xml` config file.
//!
//! The operator sets no defaults here; the file exists purely so users can
//! supply `configOverrides`.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

use crate::controller::build::properties::resolved_overrides;

/// Renders `hadoop-policy.xml` from the user-provided overrides only.
pub fn build(overrides: KeyValueConfigOverrides) -> String {
    let config: BTreeMap<String, String> = resolved_overrides(overrides).collect();
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::config_overrides;

    #[test]
    fn empty_overrides_render_empty_configuration() {
        assert_eq!(
            build(config_overrides(&[])),
            concat!(
                "<?xml version=\"1.0\"?>\n",
                "<configuration>\n",
                "</configuration>"
            )
        );
    }

    #[test]
    fn overrides_are_rendered_as_properties() {
        assert_eq!(
            build(config_overrides(&[("security.client.protocol.acl", "*")])),
            concat!(
                "<?xml version=\"1.0\"?>\n",
                "<configuration>\n",
                "  <property>\n",
                "    <name>security.client.protocol.acl</name>\n",
                "    <value>*</value>\n",
                "  </property>\n",
                "</configuration>"
            )
        );
    }
}
