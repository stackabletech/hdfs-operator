use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::ObjectReference,
    kube::runtime::events::{Event, EventType},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{ValidatedCluster, build::total_replicas},
    crd::HdfsNodeRole,
    hdfs_controller::Ctx,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to publish event"))]
    PublishEvent {
        source: stackable_operator::kube::Error,
    },
}

/// Publish a Kubernetes warning event for the `hdfs` cluster resource.
pub async fn publish_warning_event(
    ctx: &Ctx,
    hdfs_object_ref: &ObjectReference,
    action: String,
    reason: String,
    message: String,
) -> Result<(), Error> {
    ctx.event_recorder
        .publish(
            &Event {
                action,
                reason,
                note: Some(message),
                type_: EventType::Warning,
                secondary: None,
            },
            hdfs_object_ref,
        )
        .await
        .context(PublishEventSnafu)
}

pub fn build_invalid_replica_message(
    validated_cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
) -> Option<String> {
    let replicas = match role {
        HdfsNodeRole::Name => total_replicas(&validated_cluster.namenode_role_group_configs),
        HdfsNodeRole::Data => total_replicas(&validated_cluster.datanode_role_group_configs),
        HdfsNodeRole::Journal => total_replicas(&validated_cluster.journalnode_role_group_configs),
    };

    let dfs_replication = validated_cluster.cluster_config.dfs_replication;
    let role_name = role.to_string();
    let min_replicas = role.min_replicas();

    if replicas < min_replicas {
        Some(format!(
            "{role_name}: only has {replicas} replicas configured, it is strongly recommended to use at least [{min_replicas}]"
        ))
    } else if !role.replicas_can_be_even() && replicas.is_multiple_of(2) {
        Some(format!(
            "{role_name}: currently has an even number of replicas [{replicas}], but should always have an odd number to ensure quorum"
        ))
    } else if role.replicas_must_cover_dfs_replication() && replicas < dfs_replication as u16 {
        Some(format!(
            "{role_name}: HDFS replication factor [{dfs_replication}] is configured greater than data node replicas [{replicas}]"
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::deserialize_and_validate_cluster;

    /// A role group with no explicit `replicas` runs one pod — Kubernetes' default for a
    /// `StatefulSet` with `replicas: null` — so it must not be counted as zero. Counting it as
    /// zero produced a warning event telling the user to configure at least one datanode when
    /// they already had one.
    #[test]
    fn an_unset_replica_count_counts_as_one_datanode() {
        let cluster = deserialize_and_validate_cluster(
            "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    dfsReplication: 1
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  journalNodes:
    roleGroups:
      default:
        replicas: 3
  dataNodes:
    roleGroups:
      default: {}
",
        );

        assert_eq!(
            build_invalid_replica_message(&cluster, &HdfsNodeRole::Data),
            None
        );
    }

    /// A role with no role groups at all really does have zero replicas, and still warns.
    #[test]
    fn a_role_without_role_groups_still_warns() {
        let cluster = deserialize_and_validate_cluster(
            "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    dfsReplication: 1
  nameNodes:
    roleGroups: {}
  journalNodes:
    roleGroups:
      default:
        replicas: 3
  dataNodes:
    roleGroups:
      default:
        replicas: 1
",
        );

        assert_eq!(
            build_invalid_replica_message(&cluster, &HdfsNodeRole::Name).as_deref(),
            Some(
                "namenode: only has 0 replicas configured, it is strongly recommended to use at \
                 least [2]"
            )
        );
    }

    /// A `dfsReplication` above the datanode count means HDFS cannot place every replica, so the
    /// user is warned. The gate for this is [`HdfsNodeRole::replicas_must_cover_dfs_replication`], which
    /// is true for datanodes only — the message is about datanodes.
    #[test]
    fn fewer_datanodes_than_the_replication_factor_warns() {
        let cluster = deserialize_and_validate_cluster(
            "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    dfsReplication: 3
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  journalNodes:
    roleGroups:
      default:
        replicas: 3
  dataNodes:
    roleGroups:
      default:
        replicas: 2
",
        );

        assert_eq!(
            build_invalid_replica_message(&cluster, &HdfsNodeRole::Data).as_deref(),
            Some(
                "datanode: HDFS replication factor [3] is configured greater than data node \
                 replicas [2]"
            )
        );
    }

    /// The `dfsReplication` warning is worded in terms of datanode replicas, so only datanodes
    /// are compared against it. A journalnode role group with fewer replicas than
    /// `dfsReplication` is not a misconfiguration and must stay silent.
    #[test]
    fn journalnodes_are_not_compared_to_the_replication_factor() {
        let cluster = deserialize_and_validate_cluster(
            "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    dfsReplication: 5
  nameNodes:
    roleGroups:
      default:
        replicas: 2
  journalNodes:
    roleGroups:
      default:
        replicas: 3
  dataNodes:
    roleGroups:
      default:
        replicas: 5
",
        );

        assert_eq!(
            build_invalid_replica_message(&cluster, &HdfsNodeRole::Journal),
            None
        );
    }
}
