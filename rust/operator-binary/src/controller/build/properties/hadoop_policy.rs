//! Builds the `hadoop-policy.xml` config file.
//!
//! The operator sets no defaults here; the file exists purely so users can
//! supply `configOverrides`.

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

/// Renders `hadoop-policy.xml` from the user-provided overrides only.
pub fn build(overrides: KeyValueConfigOverrides) -> String {
    to_hadoop_xml(overrides.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::EMPTY_HADOOP_XML;

    #[test]
    fn empty_overrides_render_empty_configuration() {
        assert_eq!(build(KeyValueConfigOverrides::default()), EMPTY_HADOOP_XML);
    }

    #[test]
    fn overrides_are_rendered_as_properties() {
        assert_eq!(
            build([("security.client.protocol.acl", "*")].into()),
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
