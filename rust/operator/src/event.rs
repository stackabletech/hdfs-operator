use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{CONTROLLER_NAME, DEFAULT_DFS_REPLICATION_FACTOR},
    HdfsCluster, HdfsRole,
};
use stackable_operator::{
    client::Client,
    kube::runtime::{
        events::{Event, EventType, Recorder, Reporter},
        reflector::ObjectRef,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Failed to publish event"))]
    PublishEvent {
        source: stackable_operator::kube::Error,
    },
}

/// Publish a Kubernetes event for the `hdfs` cluster resource.
pub async fn publish_event(
    hdfs: &HdfsCluster,
    client: &Client,
    action: &str,
    reason: &str,
    message: &str,
) -> Result<(), Error> {
    let reporter = Reporter {
        controller: CONTROLLER_NAME.into(),
        instance: None,
    };

    let object_ref = ObjectRef::from_obj(hdfs);

    let recorder = Recorder::new(client.as_kube_client(), reporter, object_ref.into());
    recorder
        .publish(Event {
            action: action.into(),
            reason: reason.into(),
            note: Some(message.into()),
            type_: EventType::Warning,
            secondary: None,
        })
        .await
        .context(PublishEventSnafu)
}

pub fn build_invalid_replica_message(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    dfs_replication: Option<u8>,
) -> Option<String> {
    let replicas: u16 = hdfs
        .rolegroup_ref_and_replicas(role)
        .iter()
        .map(|tuple| tuple.1)
        .sum();

    let rn = role.to_string();
    let min_replicas = role.min_replicas();

    if replicas < min_replicas {
        Some(format!("{rn}: only has {replicas} replicas configured, it is strongly recommended to use at least [{min_replicas}]"))
    } else if !role.replicas_can_be_even() && replicas % 2 == 0 {
        Some(format!("{rn}: currently has an even number of replicas [{replicas}], but should always have an odd number to ensure quorum"))
    } else if role.check_valid_dfs_replication() {
        match dfs_replication {
            None => {
                if replicas < u16::from(DEFAULT_DFS_REPLICATION_FACTOR) {
                    Some(format!(
                        "{rn}: HDFS replication factor not set. Using default value of [{DEFAULT_DFS_REPLICATION_FACTOR}] which is greater than data node replicas [{replicas}]"
                    ))
                } else {
                    None
                }
            }
            Some(dfsr) => {
                if replicas < u16::from(dfsr) {
                    Some(format!("{rn}: HDFS replication factor [{dfsr}] is configured greater than data node replicas [{replicas}]"))
                } else {
                    None
                }
            }
        }
    } else {
        None
    }
}
