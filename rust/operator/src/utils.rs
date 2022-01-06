use stackable_operator::kube::{
    core::DynamicObject,
    runtime::{controller::ReconcilerAction, reflector::ObjectRef},
    Resource,
};

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
pub fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> Result<(ObjectRef<DynamicObject>, ReconcilerAction), Box<dyn std::error::Error>> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
}
