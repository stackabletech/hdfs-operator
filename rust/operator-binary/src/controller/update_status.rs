//! The update_status step in the HdfsCluster controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    HDFS_OPERATOR_NAME,
    controller::{ValidatedCluster, apply::AppliedResources},
    crd::{HdfsClusterStatus, UpgradeState, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the outcome of the apply step and patches it onto the
/// [`v1alpha1::HdfsCluster`]. Takes [`AppliedResources`] so the type system proves the status
/// derives from applied resources — including whether the role-ordered StatefulSet rollout is
/// still in progress — not merely built ones.
pub async fn update_status(
    client: &Client,
    hdfs: &v1alpha1::HdfsCluster,
    cluster: &ValidatedCluster,
    applied: &AppliedResources,
) -> Result<()> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in &applied.resources.stateful_sets {
        ss_cond_builder.add(stateful_set.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hdfs.spec.cluster_operation);

    let upgrade_state = cluster.status.upgrade_state;

    let status = HdfsClusterStatus {
        conditions: compute_conditions(hdfs, &[&ss_cond_builder, &cluster_operation_cond_builder]),
        // FIXME: We can't currently leave upgrade mode automatically, since we don't know when an upgrade is finalized
        deployed_product_version: Some(
            cluster
                .status
                .deployed_product_version
                .clone()
                // Keep current version if set, otherwise (on initial deploy) fall back
                // to the user's specified version.
                .unwrap_or_else(|| cluster.image.product_version.clone()),
        ),
        upgrade_target_product_version: match upgrade_state {
            // User is upgrading, whatever they're upgrading to is (by definition) the target
            Some(UpgradeState::Upgrading) => Some(cluster.image.product_version.clone()),
            Some(UpgradeState::Downgrading) => {
                if applied.statefulsets_rolled_out {
                    // Downgrade is done, clear
                    tracing::info!("downgrade deployed, clearing upgrade state");
                    None
                } else {
                    // Downgrade is still in progress, preserve the current value
                    cluster.status.upgrade_target_product_version.clone()
                }
            }
            // Upgrade is complete (if any), clear
            None => None,
        },
    };

    client
        .apply_patch_status(HDFS_OPERATOR_NAME, hdfs, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}
