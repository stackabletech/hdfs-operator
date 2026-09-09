use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    client::Client,
    cluster_resources::ClusterResourceApplyStrategy,
    kube::{
        Resource,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, events::Recorder},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    controller::{
        apply::{self, Applier},
        build::{self},
        update_status::{self, update_status},
    },
    crd::{HdfsNodeRole, v1alpha1},
    event::{build_invalid_replica_message, publish_warning_event},
};

pub const HDFS_CONTROLLER_NAME: &str = "hdfs-controller";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to update the cluster status"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("failed to build Kubernetes resources"))]
    BuildResources {
        source: crate::controller::build::Error,
    },

    #[snafu(display("failed to create cluster event"))]
    FailedToCreateClusterEvent { source: crate::event::Error },

    #[snafu(display("HdfsCluster object is invalid"))]
    InvalidHdfsCluster {
        source: error_boundary::InvalidObject,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub type HdfsOperatorResult<T> = Result<T, Error>;

pub struct Ctx {
    pub client: Client,
    pub event_recorder: Arc<Recorder>,
    pub operator_environment: OperatorEnvironmentOptions,
}

pub async fn reconcile_hdfs(
    hdfs: Arc<DeserializeGuard<v1alpha1::HdfsCluster>>,
    ctx: Arc<Ctx>,
) -> HdfsOperatorResult<Action> {
    tracing::info!("Starting reconcile");

    if hdfs.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

    let hdfs = hdfs
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHdfsClusterSnafu)?;
    let client = &ctx.client;

    let dereferenced = crate::controller::dereference::dereference(client, hdfs)
        .await
        .context(DereferenceSnafu)?;

    let validated_cluster = crate::controller::validate::validate_cluster(
        hdfs,
        &ctx.operator_environment.image_repository,
        dereferenced,
    )
    .context(ValidateSnafu)?;

    // Build every Kubernetes resource up front. This step needs no client: all external
    // references are already dereferenced and validated. The ServiceAccount name is
    // deterministic on the built RBAC object, so the build does not depend on the applied one.
    let resources = build::build(&validated_cluster, &client.kubernetes_cluster_info)
        .context(BuildResourcesSnafu)?;

    // Warn about invalid replica counts. This is validation feedback and independent of the
    // resource application below.
    for role in HdfsNodeRole::iter() {
        if !validated_cluster.role_groups.contains_key(&role) {
            continue;
        }
        if let Some(message) = build_invalid_replica_message(&validated_cluster, &role) {
            publish_warning_event(
                &ctx,
                &hdfs.object_ref(&()),
                "Reconcile".to_owned(),
                "Invalid replicas".to_owned(),
                message,
            )
            .await
            .context(FailedToCreateClusterEventSnafu)?;
        }
    }

    let applied = Applier::new(
        client,
        &validated_cluster,
        ClusterResourceApplyStrategy::from(&hdfs.spec.cluster_operation),
        &hdfs.spec.object_overrides,
    )
    .apply(resources, validated_cluster.status.upgrade_state)
    .await
    .context(ApplyResourcesSnafu)?;

    update_status(client, hdfs, &validated_cluster, &applied)
        .await
        .context(UpdateStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HdfsCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidHdfsCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use stackable_operator::{
        builder::pod::PodBuilder,
        client::Client,
        commons::networking::DomainName,
        kube::{
            Client as KubeClient, Config,
            api::ObjectMeta,
            runtime::{
                controller::Action,
                events::{Recorder, Reporter},
            },
        },
        kvp::Labels,
        utils::cluster_info::KubernetesClusterInfo,
        v2::types::operator::RoleGroupName,
    };

    use super::*;
    use crate::{
        HDFS_FULL_CONTROLLER_NAME,
        controller::build::container::ContainerConfig,
        test_support::{deserialize_cluster, role_group_config, validate_cluster},
    };

    #[test]
    pub fn test_env_overrides() {
        let cr = "
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
  nameNodes:
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    roleGroups:
      default:
        replicas: 1
  dataNodes:
    envOverrides:
      COMMON_VAR: role-value # overridden by role group below
      ROLE_VAR: role-value   # only defined here at role level
    roleGroups:
      default:
        envOverrides:
          COMMON_VAR: group-value # overrides role value
          GROUP_VAR: group-value # only defined here at group level
        replicas: 1
";

        let role = HdfsNodeRole::Data;
        let hdfs = deserialize_cluster(cr);
        let validated_cluster = validate_cluster(&hdfs);
        let role_group_name = RoleGroupName::from_str("default").unwrap();
        let role_group_config = role_group_config(&validated_cluster, &role, &role_group_name);

        let mut pb = PodBuilder::new();
        pb.metadata(ObjectMeta::default());
        ContainerConfig::add_containers_and_volumes(
            &mut pb,
            &validated_cluster,
            &KubernetesClusterInfo {
                cluster_domain: DomainName::try_from("cluster.local").unwrap(),
            },
            &role,
            &role_group_name,
            role_group_config,
            &Labels::new(),
        )
        .unwrap();
        let containers = pb.build().unwrap().spec.unwrap().containers;
        let env_vars = containers
            .iter()
            .find(|c| c.name == role.to_string())
            .unwrap()
            .env
            .clone()
            .unwrap();

        assert_eq!(
            env_vars
                .iter()
                .find(|e| e.name == "COMMON_VAR")
                .unwrap()
                .value,
            Some("group-value".to_string())
        );

        assert_eq!(
            env_vars
                .iter()
                .find(|e| e.name == "ROLE_VAR")
                .unwrap()
                .value,
            Some("role-value".to_string())
        );
        assert_eq!(
            env_vars
                .iter()
                .find(|e| e.name == "GROUP_VAR")
                .unwrap()
                .value,
            Some("group-value".to_string())
        );
    }

    /// The client points at a closed port, so any API call would fail the reconciliation: an `Ok`
    /// proves that a cluster being deleted returns before the reconciler touches the Kubernetes
    /// API, and because the spec is invalid, before the [`DeserializeGuard`] is unwrapped.
    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        let hdfs = serde_yaml::from_str(
            r#"
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  deletionTimestamp: "2026-08-14T12:00:00Z"
spec: {}
"#,
        )
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        let action = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread tokio runtime")
            .block_on(async {
                let kube_client = KubeClient::try_from(Config::new(
                    "http://127.0.0.1:1".parse().expect("valid static URI"),
                ))
                .expect("client from static config");

                let ctx = Arc::new(Ctx {
                    client: Client::new(
                        kube_client.clone(),
                        None,
                        "default".to_owned(),
                        KubernetesClusterInfo {
                            cluster_domain: DomainName::from_str("cluster.local")
                                .expect("valid cluster domain"),
                        },
                    ),
                    event_recorder: Arc::new(Recorder::new(
                        kube_client,
                        Reporter {
                            controller: HDFS_FULL_CONTROLLER_NAME.to_string(),
                            instance: None,
                        },
                    )),
                    operator_environment: OperatorEnvironmentOptions {
                        operator_namespace: "stackable-operators".to_owned(),
                        operator_service_name: "hdfs-operator".to_owned(),
                        image_repository: "oci.stackable.tech/sdp".to_owned(),
                    },
                });

                reconcile_hdfs(Arc::new(hdfs), ctx).await
            })
            .expect("a deleted cluster reconciles without any API call");

        assert_eq!(action, Action::await_change());
    }
}
