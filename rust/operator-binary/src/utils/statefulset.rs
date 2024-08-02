use std::borrow::Cow;

use snafu::Snafu;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;

#[derive(Debug, Snafu)]
#[snafu(module(outdated_statefulset))]
pub enum OutdatedStatefulSet {
    #[snafu(display("generation {current_generation:?} not yet observed by statefulset controller, last seen was {observed_generation:?}"))]
    NotYetObserved {
        current_generation: Option<i64>,
        observed_generation: Option<i64>,
    },

    #[snafu(display("only {updated_replicas} out of {total_replicas} are updated"))]
    HasOutdatedReplicas {
        total_replicas: i32,
        updated_replicas: i32,
    },
}

/// Checks whether all ReplicaSet replicas are up-to-date according to `sts.spec`
pub fn check_all_replicas_updated(sts: &StatefulSet) -> Result<(), OutdatedStatefulSet> {
    use outdated_statefulset::*;

    let status = sts.status.as_ref().map_or_else(Cow::default, Cow::Borrowed);

    let current_generation = sts.metadata.generation;
    let observed_generation = status.observed_generation;
    if current_generation != observed_generation {
        return NotYetObservedSnafu {
            current_generation,
            observed_generation,
        }
        .fail();
    }

    let total_replicas = status.replicas;
    let updated_replicas = status.updated_replicas.unwrap_or(0);
    if total_replicas != updated_replicas {
        return HasOutdatedReplicasSnafu {
            total_replicas,
            updated_replicas,
        }
        .fail();
    }

    Ok(())
}
