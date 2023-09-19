use std::cmp::{max, min};

use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{constants::APP_NAME, HdfsCluster, HdfsRole};
use stackable_operator::{
    builder::pdb::PdbBuilder, client::Client, cluster_resources::ClusterResources,
    kube::ResourceExt,
};

use crate::{hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER, OPERATOR_NAME};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role group [{role_group}]"))]
    CreatePdb {
        source: stackable_operator::error::Error,
        role_group: String,
    },
    #[snafu(display("Cannot apply role group PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::error::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    let max_unavailable = match role {
        HdfsRole::NameNode => max_unavailable_name_nodes(),
        HdfsRole::DataNode => max_unavailable_data_nodes(
            hdfs.num_datanodes(),
            // FIXME extract to const
            hdfs.spec.cluster_config.dfs_replication as u16,
        ),
        HdfsRole::JournalNode => max_unavailable_journal_nodes(),
    };
    let pdb = PdbBuilder::new_for_role(
        hdfs,
        APP_NAME,
        &role.to_string(),
        OPERATOR_NAME,
        RESOURCE_MANAGER_HDFS_CONTROLLER,
    )
    .with_context(|_| CreatePdbSnafu {
        role_group: role.to_string(),
    })?
    .max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_name_nodes() -> u16 {
    1
}

fn max_unavailable_journal_nodes() -> u16 {
    1
}

fn max_unavailable_data_nodes(num_datanodes: u16, dfs_replication: u16) -> u16 {
    // There must always be a datanode to serve the block.
    let max_unavailable = dfs_replication.saturating_sub(1);
    // There is no point of having a bigger `max_unavailable` than even datanodes in the cluster.
    let max_unavailable = min(max_unavailable, num_datanodes.saturating_sub(1));
    // Clamp to at least a single datanode can be offline, to not render the k8s cluster helpless.
    max(max_unavailable, 1)
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0, 0, 1)]
    #[case(0, 1, 1)]
    #[case(0, 2, 1)]
    #[case(0, 3, 1)]
    #[case(1, 0, 1)]
    #[case(1, 1, 1)]
    #[case(1, 2, 1)]
    #[case(1, 3, 1)]
    #[case(2, 0, 1)]
    #[case(2, 1, 1)]
    #[case(2, 2, 1)]
    #[case(2, 3, 1)] // Attention: This needs to be 1, not 2
    #[case(3, 0, 1)]
    #[case(3, 1, 1)]
    #[case(3, 2, 1)]
    #[case(3, 3, 2)]
    #[case(4, 0, 1)]
    #[case(4, 1, 1)]
    #[case(4, 2, 1)]
    #[case(4, 3, 2)]
    #[case(100, 0, 1)]
    #[case(100, 1, 1)]
    #[case(100, 2, 1)]
    #[case(100, 3, 2)]
    fn test_max_unavailable_data_nodes(
        #[case] num_datanodes: u16,
        #[case] dfs_replication: u16,
        #[case] expected: u16,
    ) {
        let max_unavailable = max_unavailable_data_nodes(num_datanodes, dfs_replication);
        assert_eq!(max_unavailable, expected)
    }
}
