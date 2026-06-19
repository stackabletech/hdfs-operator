use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::{PodBuilder, security::PodSecurityContextBuilder},
    cli::OperatorEnvironmentOptions,
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::rbac::build_rbac_resources,
    iter::reverse_if,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::ServiceAccount,
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, events::Recorder, reflector::ObjectRef},
    },
    kvp::{LabelError, Labels},
    logging::controller::ReconcilerError,
    role_utils::RoleGroupRef,
    shared::time::Duration,
    status::{
        condition::{
            compute_conditions, operations::ClusterOperationsConditionBuilder,
            statefulset::StatefulSetConditionBuilder,
        },
        rollout::check_statefulset_rollout_complete,
    },
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    OPERATOR_NAME,
    controller::{
        ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            container::{self, ContainerConfig},
            graceful_shutdown::{self, add_graceful_shutdown_config},
            resource::{
                discovery::{self, build_discovery_config_map},
                pdb::add_pdbs,
                service::{self, rolegroup_headless_service, rolegroup_metrics_service},
            },
        },
    },
    crd::{
        HdfsClusterStatus, HdfsNodeRole, UpgradeState, UpgradeStateError, constants::*, v1alpha1,
    },
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

    #[snafu(display("invalid upgrade state"))]
    InvalidUpgradeState { source: UpgradeStateError },

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

    #[snafu(display("failed to create container and volume configuration"))]
    FailedToCreateContainerAndVolumeConfiguration {
        source: crate::controller::build::container::Error,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::controller::build::resource::pdb::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: crate::crd::Error },

    #[snafu(display("failed to build cluster resources label"))]
    BuildClusterResourcesLabel { source: LabelError },

    #[snafu(display("failed to build role-group volume claim templates from config"))]
    BuildRoleGroupVolumeClaimTemplates { source: container::Error },

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

    let dfs_replication = hdfs.spec.cluster_config.dfs_replication;
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let upgrade_state = hdfs.upgrade_state().context(InvalidUpgradeStateSnafu)?;
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
        let role_name: &str = role.into();
        let Some(group_config) = validated_cluster.role_groups.get(&role) else {
            tracing::debug!(?role, "role has no configuration, skipping");
            continue;
        };

        if let Some(message) = build_invalid_replica_message(hdfs, &role, dfs_replication) {
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
            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rg_service = rolegroup_headless_service(&validated_cluster, &role, &rolegroup_ref)
                .context(BuildServiceSnafu)?;
            let rg_metrics_service =
                rolegroup_metrics_service(&validated_cluster, &role, &rolegroup_ref)
                    .context(BuildServiceSnafu)?;

            let rg_configmap =
                crate::controller::build::resource::config_map::build_rolegroup_config_map(
                    &validated_cluster,
                    &client.kubernetes_cluster_info,
                    &rolegroup_ref,
                )
                .context(BuildRoleGroupConfigMapSnafu)?;

            let rg_statefulset = rolegroup_statefulset(
                hdfs,
                &validated_cluster,
                &client.kubernetes_cluster_info,
                &role,
                &rolegroup_ref,
                validated_cluster_rg_config,
                &rbac_sa,
            )?;

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

        if let Some(validated_role_config) = validated_cluster.role_configs.get(&role) {
            add_pdbs(
                &validated_role_config.pdb,
                hdfs,
                &role,
                client,
                &mut cluster_resources,
            )
            .await
            .context(FailedToCreatePdbSnafu)?;
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm = build_discovery_config_map(
        &validated_cluster,
        &client.kubernetes_cluster_info,
        &crate::crd::namenode_listener_refs(
            client,
            validated_cluster.pod_refs(&HdfsNodeRole::Name),
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
            hdfs.status
                .as_ref()
                // Keep current version if set
                .and_then(|status| status.deployed_product_version.as_deref())
                // Otherwise (on initial deploy) fall back to user's specified version
                .unwrap_or(hdfs.spec.image.product_version())
                .to_string(),
        ),
        upgrade_target_product_version: match upgrade_state {
            // User is upgrading, whatever they're upgrading to is (by definition) the target
            Some(UpgradeState::Upgrading) => Some(hdfs.spec.image.product_version().to_string()),
            Some(UpgradeState::Downgrading) => {
                if deploy_done {
                    // Downgrade is done, clear
                    tracing::info!("downgrade deployed, clearing upgrade state");
                    None
                } else {
                    // Downgrade is still in progress, preserve the current value
                    hdfs.status
                        .as_ref()
                        .and_then(|status| status.upgrade_target_product_version.clone())
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

fn rolegroup_statefulset(
    hdfs: &v1alpha1::HdfsCluster,
    validated: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role: &HdfsNodeRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
    rolegroup_config: &ValidatedRoleGroupConfig,
    service_account: &ServiceAccount,
) -> HdfsOperatorResult<StatefulSet> {
    tracing::info!("Setting up StatefulSet for {:?}", rolegroup_ref);

    let image = &validated.image;
    let merged_config = &rolegroup_config.config;

    // Pod references for all namenodes across all role groups, needed to wire up the
    // init containers of this role group.
    let namenode_podrefs = validated.pod_refs(&HdfsNodeRole::Name);

    // PodBuilder for StatefulSet Pod template.
    let mut pb = PodBuilder::new();

    let rolegroup_selector_labels: Labels = hdfs
        .rolegroup_selector_labels(rolegroup_ref)
        .context(RoleGroupSelectorLabelsSnafu)?;

    let pb_metadata = ObjectMeta {
        labels: Some(rolegroup_selector_labels.clone().into()),
        ..ObjectMeta::default()
    };

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(image)
        .affinity(&merged_config.affinity)
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    // Adds all containers and volumes to the pod builder
    // We must use the selector labels ("rolegroup_selector_labels") and not the recommended labels
    // for the ephemeral listener volumes created by this function.
    // This is because the recommended set contains a "managed-by" label. This label triggers
    // the cluster resources to "manage" listeners which is wrong and leads to errors.
    // The listeners are managed by the listener-operator.
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        hdfs,
        cluster_info,
        role,
        rolegroup_ref,
        image,
        rolegroup_config,
        hdfs.spec.cluster_config.zookeeper_config_map_name.as_ref(),
        &namenode_podrefs,
        &rolegroup_selector_labels,
    )
    .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    add_graceful_shutdown_config(merged_config, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(rolegroup_config.pod_overrides.clone());

    // The same comment regarding labels is valid here as it is for the ContainerConfig::add_containers_and_volumes() call above.
    let pvcs = ContainerConfig::volume_claim_templates(merged_config, &rolegroup_selector_labels)
        .context(BuildRoleGroupVolumeClaimTemplatesSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: rolegroup_config.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(rolegroup_selector_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(rolegroup_ref.object_name()),
        template: pod_template,

        volume_claim_templates: Some(pvcs),
        ..StatefulSetSpec::default()
    };

    // TODO: The restart-controller is currently not enabled via the label RESTART_CONTROLLER_ENABLED_LABEL.
    // This is due to problems that might appear when restarting pods during the initial formatting of namenodes.
    // See: https://github.com/stackabletech/hdfs-operator/issues/750 (disable restart-controller)
    //      https://github.com/stackabletech/issues/issues/816 (enable restart-controller)
    let metadata = validated.rolegroup_metadata(rolegroup_ref);

    Ok(StatefulSet {
        metadata: metadata.build(),
        spec: Some(statefulset_spec),
        status: None,
    })
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
    use stackable_operator::commons::networking::DomainName;

    use super::*;
    use crate::test_support::{deserialize_cluster, role_group_config, validate_cluster};

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
        let role_group_config = role_group_config(&validated_cluster, &role, "default");

        let rolegroup_ref = hdfs.rolegroup_ref(role.to_string(), "default");
        let resolved_product_image = &validated_cluster.image;

        let mut pb = PodBuilder::new();
        pb.metadata(ObjectMeta::default());
        ContainerConfig::add_containers_and_volumes(
            &mut pb,
            &hdfs,
            &KubernetesClusterInfo {
                cluster_domain: DomainName::try_from("cluster.local").unwrap(),
            },
            &role,
            &rolegroup_ref,
            resolved_product_image,
            role_group_config,
            hdfs.spec.cluster_config.zookeeper_config_map_name.as_ref(),
            &[],
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
