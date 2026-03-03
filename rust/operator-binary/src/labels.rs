use std::collections::BTreeMap;

use stackable_operator::kvp::{Label, LabelError, Labels};

/// Add Stackable stack/demo labels to the provided Labels if present in the cluster labels
///
/// These Stackable labels are used, for example, by stackablectl to delete stack/demo resources
/// The "stackable.tech/vendor" label is already added by [`ObjectMetaBuilder::with_recommended_labels()`]
pub(crate) fn add_stackable_labels(
    mut labels: Labels,
    cluster_labels: Option<BTreeMap<String, String>>,
) -> Result<Labels, LabelError> {
    if let Some(cluster_labels) = cluster_labels {
        for (key, value) in cluster_labels {
            match key.as_str() {
                "stackable.tech/stack" | "stackable.tech/demo" => {
                    labels.insert(Label::try_from((key, value))?);
                }
                _ => {}
            }
        }
    }

    Ok(labels)
}
