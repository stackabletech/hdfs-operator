use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::ObjectReference,
    kube::runtime::events::{Event, EventType},
    v2::{
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::operator::RoleGroupName,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::ValidatedCluster,
    crd::{HdfsNodeRole, v1alpha1},
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
    } else if !role.replicas_can_be_even() && replicas < dfs_replication as u16 {
        Some(format!(
            "{role_name}: HDFS replication factor [{dfs_replication}] is configured greater than data node replicas [{replicas}]"
        ))
    } else {
        None
    }
}

/// The total number of replicas across the role groups of one role, counting a role group without
/// an explicit replica count as zero.
fn total_replicas<C>(
    role_group_configs: &BTreeMap<
        RoleGroupName,
        RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
) -> u16 {
    role_group_configs
        .values()
        .map(|role_group| role_group.replicas.unwrap_or_default())
        .sum()
}
