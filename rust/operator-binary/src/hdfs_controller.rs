use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    client::Client,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::rbac::build_rbac_resources,
    iter::reverse_if,
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, events::Recorder, reflector::ObjectRef},
    },
    kvp::LabelError,
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::{
        condition::{
            compute_conditions, operations::ClusterOperationsConditionBuilder,
            statefulset::StatefulSetConditionBuilder,
        },
        rollout::check_statefulset_rollout_complete,
    },
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    OPERATOR_NAME,
    controller::{
        build::{
            self,
            resource::discovery::{self, build_discovery_config_map},
        },
        controller_name, operator_name, product_name,
    },
    crd::{HdfsClusterStatus, HdfsNodeRole, UpgradeState, constants::*, v1alpha1},
    event::{build_invalid_replica_message, publish_warning_event},
};

pub const RESOURCE_MANAGER_HDFS_CONTROLLER: &str = "hdfs-operator-hdfs-controller";
pub const HDFS_CONTROLLER_NAME: &str = "hdfs-controller";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("cannot create rolegroup service {name:?}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("cannot create role group config map {name:?}"))]
    ApplyRoleGroupConfigMap {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("cannot create role group stateful set {name:?}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("cannot create discovery config map {name:?}"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to build Kubernetes resources"))]
    BuildResources {
        source: crate::controller::build::Error,
    },

    #[snafu(display("cannot collect discovery configuration"))]
    CollectDiscoveryConfig { source: crate::crd::Error },

    #[snafu(display("cannot build config discovery config map"))]
    BuildDiscoveryConfigMap { source: discovery::Error },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create cluster event"))]
    FailedToCreateClusterEvent { source: crate::event::Error },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to build cluster resources label"))]
    BuildClusterResourcesLabel { source: LabelError },

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

    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &controller_name(),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&hdfs.spec.cluster_operation),
        &hdfs.spec.object_overrides,
    );

    // The service account and rolebinding will be created per cluster
    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hdfs,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildClusterResourcesLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    // Build every (non-discovery) Kubernetes resource up front. This step needs no client: all
    // external references are already dereferenced and validated. The ServiceAccount name is
    // deterministic on the built RBAC object, so the build does not depend on the applied one.
    let resources = build::build(
        &validated_cluster,
        &client.kubernetes_cluster_info,
        &rbac_sa.name_any(),
    )
    .context(BuildResourcesSnafu)?;

    // Apply Services, ConfigMaps and PodDisruptionBudgets first. The StatefulSets are applied
    // afterwards so that every ConfigMap a Pod mounts already exists, which prevents unnecessary
    // Pod restarts. See https://github.com/stackabletech/commons-operator/issues/111 for details.
    for service in resources.services {
        let name = service.name_any();
        cluster_resources
            .add(client, service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu { name })?;
    }
    for config_map in resources.config_maps {
        let name = config_map.name_any();
        cluster_resources
            .add(client, config_map)
            .await
            .with_context(|_| ApplyRoleGroupConfigMapSnafu { name })?;
    }
    for pdb in resources.pod_disruption_budgets {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyPdbSnafu)?;
    }

    let upgrade_state = validated_cluster.status.upgrade_state;

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

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    let mut deploy_done = true;

    // StatefulSets must be rolled out in role order during upgrades (a namenode's version must be
    // >= the datanodes', and so on), with each role finishing its rollout before the next starts.
    // https://hadoop.apache.org/docs/r3.4.0/hadoop-project-dist/hadoop-hdfs/HdfsRollingUpgrade.html#Upgrading_Non-Federated_Clusters
    // The build output is already ordered by role, so it is applied as-is; downgrades have the
    // opposite version relationship and are therefore rolled out in reverse.
    let downgrading = matches!(upgrade_state, Some(UpgradeState::Downgrading));
    if downgrading {
        tracing::info!("HdfsCluster is being downgraded, deploying in reverse order");
    }
    for statefulset in reverse_if(downgrading, resources.stateful_sets.iter()) {
        let name = statefulset.name_any();
        let deployed_statefulset = cluster_resources
            .add(client, statefulset.clone())
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu { name })?;
        ss_cond_builder.add(deployed_statefulset.clone());

        if upgrade_state.is_some() {
            // Ensure each role is fully upgraded before moving on to the next.
            if let Err(reason) = check_statefulset_rollout_complete(&deployed_statefulset) {
                tracing::info!(
                    rolegroup.statefulset = %ObjectRef::from_obj(&deployed_statefulset),
                    reason = &reason as &dyn std::error::Error,
                    "rolegroup is still upgrading, waiting..."
                );
                deploy_done = false;
                break;
            }
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm = build_discovery_config_map(
        &validated_cluster,
        &client.kubernetes_cluster_info,
        &crate::crd::namenode_listener_refs(
            client,
            build::pod_refs(&validated_cluster, &HdfsNodeRole::Name),
        )
        .await
        .context(CollectDiscoveryConfigSnafu)?,
    )
    .context(BuildDiscoveryConfigMapSnafu)?;

    // The discovery CM is linked to the cluster lifecycle via ownerreference.
    // Therefore, must not be added to the "orphaned" cluster resources
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
        .await
        .with_context(|_| ApplyDiscoveryConfigMapSnafu {
            name: discovery_cm.metadata.name.clone().unwrap_or_default(),
        })?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hdfs.spec.cluster_operation);

    let status = HdfsClusterStatus {
        conditions: compute_conditions(hdfs, &[&ss_cond_builder, &cluster_operation_cond_builder]),
        // FIXME: We can't currently leave upgrade mode automatically, since we don't know when an upgrade is finalized
        deployed_product_version: Some(
            validated_cluster
                .status
                .deployed_product_version
                .clone()
                // Keep current version if set, otherwise (on initial deploy) fall back
                // to the user's specified version.
                .unwrap_or_else(|| validated_cluster.image.product_version.clone()),
        ),
        upgrade_target_product_version: match upgrade_state {
            // User is upgrading, whatever they're upgrading to is (by definition) the target
            Some(UpgradeState::Upgrading) => Some(validated_cluster.image.product_version.clone()),
            Some(UpgradeState::Downgrading) => {
                if deploy_done {
                    // Downgrade is done, clear
                    tracing::info!("downgrade deployed, clearing upgrade state");
                    None
                } else {
                    // Downgrade is still in progress, preserve the current value
                    validated_cluster
                        .status
                        .upgrade_target_product_version
                        .clone()
                }
            }
            // Upgrade is complete (if any), clear
            None => None,
        },
    };

    // During upgrades we do partial deployments, we don't want to garbage collect after those
    // since we *will* redeploy (or properly orphan) the remaining resources later.
    if deploy_done {
        cluster_resources
            .delete_orphaned_resources(client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;
    }
    client
        .apply_patch_status(OPERATOR_NAME, hdfs, &status)
        .await
        .context(ApplyStatusSnafu)?;

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
        builder::pod::PodBuilder, commons::networking::DomainName, kube::api::ObjectMeta,
        kvp::Labels, utils::cluster_info::KubernetesClusterInfo,
        v2::types::operator::RoleGroupName,
    };

    use super::*;
    use crate::{
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
}
