use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::CommonNodeConfig;
use stackable_operator::builder::PodBuilder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to set terminationGracePeriod"))]
    SetTerminationGracePeriod {
        source: stackable_operator::builder::pod::Error,
    },
}

pub fn add_graceful_shutdown_config(
    merged_config: &CommonNodeConfig,
    pod_builder: &mut PodBuilder,
) -> Result<(), Error> {
    // This must be always set by the merge mechanism, as we provide a default value,
    // users can not disable graceful shutdown.
    if let Some(graceful_shutdown_timeout) = merged_config.graceful_shutdown_timeout {
        pod_builder
            .termination_grace_period(&graceful_shutdown_timeout)
            .context(SetTerminationGracePeriodSnafu)?;
    }

    Ok(())
}
