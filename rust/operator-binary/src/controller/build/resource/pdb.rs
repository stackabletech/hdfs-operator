use std::{
    cmp::{max, min},
    str::FromStr,
};

use stackable_operator::{
    commons::pdb::PdbConfig,
    k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::{builder::pdb::pod_disruption_budget_builder_with_role, types::operator::RoleName},
};

use crate::{
    controller::{ValidatedCluster, build, controller_name, operator_name, product_name},
    crd::HdfsNodeRole,
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        HdfsNodeRole::Name => max_unavailable_name_nodes(),
        HdfsNodeRole::Data => max_unavailable_data_nodes(
            build::num_datanodes(cluster),
            cluster.cluster_config.dfs_replication as u16,
        ),
        HdfsNodeRole::Journal => max_unavailable_journal_nodes(),
    });
    let role_name =
        RoleName::from_str(&role.to_string()).expect("a HdfsNodeRole is a valid role name");
    let pdb = pod_disruption_budget_builder_with_role(
        cluster,
        &product_name(),
        &role_name,
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_name_nodes() -> u16 {
    1
}

fn max_unavailable_journal_nodes() -> u16 {
    1
}

fn max_unavailable_data_nodes(num_datanodes: u16, dfs_replication: u16) -> u16 {
    // There must always be a datanode to serve the block.
    // If we would simply subtract one from the `dfs_replication`, we would end up
    // with a single point of failure, so we subtract two instead.
    let max_unavailable = dfs_replication.saturating_sub(2);
    // We need to make sure at least one datanode remains by having at most
    // n - 1 datanodes unavailable. We subtract two to avoid a single point of failure.
    let max_unavailable = min(max_unavailable, num_datanodes.saturating_sub(2));
    // Clamp to at least a single node allowed to be offline, so we don't block Kubernetes nodes from draining.
    max(max_unavailable, 1)
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0, 0, 1)]
    #[case(0, 1, 1)]
    #[case(0, 2, 1)]
    #[case(0, 3, 1)]
    #[case(0, 4, 1)]
    #[case(0, 5, 1)]
    #[case(1, 0, 1)]
    #[case(1, 1, 1)]
    #[case(1, 2, 1)]
    #[case(1, 3, 1)]
    #[case(1, 4, 1)]
    #[case(1, 5, 1)]
    #[case(2, 0, 1)]
    #[case(2, 1, 1)]
    #[case(2, 2, 1)]
    #[case(2, 3, 1)]
    #[case(2, 4, 1)]
    #[case(2, 5, 1)]
    #[case(3, 0, 1)]
    #[case(3, 1, 1)]
    #[case(3, 2, 1)]
    #[case(3, 3, 1)]
    #[case(3, 4, 1)]
    #[case(3, 5, 1)]
    #[case(4, 0, 1)]
    #[case(4, 1, 1)]
    #[case(4, 2, 1)]
    #[case(4, 3, 1)]
    #[case(4, 4, 2)]
    #[case(4, 5, 2)]
    #[case(5, 0, 1)]
    #[case(5, 1, 1)]
    #[case(5, 2, 1)]
    #[case(5, 3, 1)]
    #[case(5, 4, 2)]
    #[case(5, 5, 3)]
    #[case(100, 0, 1)]
    #[case(100, 1, 1)]
    #[case(100, 2, 1)]
    #[case(100, 3, 1)]
    #[case(100, 4, 2)]
    #[case(100, 5, 3)]
    #[case(100, 10, 8)]
    #[case(100, 100, 98)]
    fn test_max_unavailable_data_nodes(
        #[case] num_datanodes: u16,
        #[case] dfs_replication: u16,
        #[case] expected: u16,
    ) {
        let max_unavailable = max_unavailable_data_nodes(num_datanodes, dfs_replication);
        assert_eq!(max_unavailable, expected)
    }
}
