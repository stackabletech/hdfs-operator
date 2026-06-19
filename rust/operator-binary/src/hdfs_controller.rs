use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
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
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    OPERATOR_NAME,
    controller::build::{
        self,
        resource::{
            discovery::{self, build_discovery_config_map},
            pdb::build_pdb,
            service::{self, rolegroup_headless_service, rolegroup_metrics_service},
            statefulset::{self, build_rolegroup_statefulset},
        },
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

    #[snafu(display("failed to build the role group ConfigMap"))]
    BuildRoleGroupConfigMap {
        source: crate::controller::build::resource::config_map::Error,
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

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
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

    #[snafu(display("failed to build the role group StatefulSet"))]
    BuildRoleGroupStatefulSet { source: statefulset::Error },

    #[snafu(display("HdfsCluster object is invalid"))]
    InvalidHdfsCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to builds service"))]
    BuildService { source: service::Error },
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

    let hdfs_obj_ref = hdfs.object_ref(&());

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RESOURCE_MANAGER_HDFS_CONTROLLER,
        &hdfs_obj_ref,
        ClusterResourceApplyStrategy::from(&hdfs.spec.cluster_operation),
        &hdfs.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

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

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let upgrade_state = validated_cluster.status.upgrade_state;
    let mut deploy_done = true;

    // Roles must be deployed in order during rolling upgrades,
    // namenode version must be >= datanode version (and so on).
    let roles = reverse_if(
        match upgrade_state {
            // Downgrades have the opposite version relationship, so they need to be rolled out in reverse order.
            Some(UpgradeState::Downgrading) => {
                tracing::info!("HdfsCluster is being downgraded, deploying in reverse order");
                true
            }
            _ => false,
        },
        HdfsNodeRole::iter(),
    );
    'roles: for role in roles {
        let Some(group_config) = validated_cluster.role_groups.get(&role) else {
            tracing::debug!(?role, "role has no configuration, skipping");
            continue;
        };

        if let Some(message) = build_invalid_replica_message(&validated_cluster, &role) {
            publish_warning_event(
                &ctx,
                &hdfs_obj_ref,
                "Reconcile".to_owned(),
                "Invalid replicas".to_owned(),
                message,
            )
            .await
            .context(FailedToCreateClusterEventSnafu)?;
        }

        for (rolegroup_name, validated_cluster_rg_config) in group_config.iter() {
            let rg_service = rolegroup_headless_service(&validated_cluster, &role, rolegroup_name)
                .context(BuildServiceSnafu)?;
            let rg_metrics_service =
                rolegroup_metrics_service(&validated_cluster, &role, rolegroup_name)
                    .context(BuildServiceSnafu)?;

            let rg_configmap =
                crate::controller::build::resource::config_map::build_rolegroup_config_map(
                    &validated_cluster,
                    &client.kubernetes_cluster_info,
                    &role,
                    rolegroup_name,
                )
                .context(BuildRoleGroupConfigMapSnafu)?;

            let rg_statefulset = build_rolegroup_statefulset(
                &validated_cluster,
                &client.kubernetes_cluster_info,
                &role,
                rolegroup_name,
                validated_cluster_rg_config,
                &rbac_sa,
            )
            .context(BuildRoleGroupStatefulSetSnafu)?;

            let rg_service_name = rg_service.name_any();
            let rg_metrics_service_name = rg_metrics_service.name_any();

            cluster_resources
                .add(client, rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    name: rg_service_name,
                })?;
            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    name: rg_metrics_service_name,
                })?;
            let rg_configmap_name = rg_configmap.name_any();
            cluster_resources
                .add(client, rg_configmap.clone())
                .await
                .with_context(|_| ApplyRoleGroupConfigMapSnafu {
                    name: rg_configmap_name,
                })?;

            // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
            // to prevent unnecessary Pod restarts.
            // See https://github.com/stackabletech/commons-operator/issues/111 for details.
            let rg_statefulset_name = rg_statefulset.name_any();
            let deployed_rg_statefulset = cluster_resources
                .add(client, rg_statefulset.clone())
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    name: rg_statefulset_name,
                })?;
            ss_cond_builder.add(deployed_rg_statefulset.clone());

            if upgrade_state.is_some() {
                // When upgrading, ensure that each role is upgraded before moving on to the next as recommended by
                // https://hadoop.apache.org/docs/r3.4.0/hadoop-project-dist/hadoop-hdfs/HdfsRollingUpgrade.html#Upgrading_Non-Federated_Clusters
                if let Err(reason) = check_statefulset_rollout_complete(&deployed_rg_statefulset) {
                    tracing::info!(
                        rolegroup.statefulset = %ObjectRef::from_obj(&deployed_rg_statefulset),
                        reason = &reason as &dyn std::error::Error,
                        "rolegroup is still upgrading, waiting..."
                    );
                    deploy_done = false;
                    break 'roles;
                }
            }
        }

        if let Some(pdb) = build_pdb(&validated_cluster, &role) {
            cluster_resources
                .add(client, pdb)
                .await
                .context(ApplyPdbSnafu)?;
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
