use stackable_hdfs_crd::MergedConfig;
use stackable_operator::builder::PodBuilder;

pub fn add_graceful_shutdown_config(
    merged_config: &(dyn MergedConfig + Send + 'static),
    pod_builder: &mut PodBuilder,
) {
    // This must be always set by the merge mechanism, as we provide a default value.
    if let Some(graceful_shutdown_timeout) = merged_config.graceful_shutdown_timeout() {
        pod_builder.termination_grace_period_seconds(graceful_shutdown_timeout.as_secs() as i64);
    }
}
