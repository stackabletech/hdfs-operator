//! The apply step in the HdfsCluster controller.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    deep_merger::ObjectOverrides,
    iter::reverse_if,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{ResourceExt, runtime::reflector::ObjectRef},
    status::rollout::check_statefulset_rollout_complete,
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        Applied, KubernetesResources, Prepared, ValidatedCluster, controller_name, operator_name,
        product_name,
    },
    crd::{UpgradeState, constants::FIELD_MANAGER_SCOPE},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply the StatefulSet {name:?}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("cannot create discovery config map {name:?}"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The outcome of the apply step: the applied resources, plus whether every StatefulSet was
/// applied and — during an upgrade or downgrade — fully rolled out.
pub struct AppliedResources {
    pub resources: KubernetesResources<Applied>,
    /// `false` while a rolling upgrade or downgrade is still in progress. The role-ordered
    /// rollout then stopped at the incomplete StatefulSet, so the later ones were not applied
    /// in this run, and the status must keep its upgrade/downgrade state.
    pub statefulsets_rolled_out: bool,
}

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// Unlike its siblings in the other operators, this Applier is HDFS-specific: StatefulSets are
/// rolled out in role order during upgrades (reversed for downgrades), each role gated on the
/// previous one's rollout being complete.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        cluster: &ValidatedCluster,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        let cluster_resources = cluster_resources_new(
            &product_name(),
            &operator_name(),
            &controller_name(),
            &cluster.name,
            &cluster.namespace,
            &cluster.uid,
            apply_strategy,
            object_overrides,
        );

        Applier {
            client,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources and marks them as applied.
    ///
    /// `applied.resources.stateful_sets` contains only the StatefulSets that were actually
    /// applied: during an upgrade or downgrade the role-ordered rollout stops at the first
    /// StatefulSet whose rollout is incomplete (see [`AppliedResources`]).
    pub async fn apply(
        mut self,
        resources: KubernetesResources<Prepared>,
        upgrade_state: Option<UpgradeState>,
    ) -> Result<AppliedResources> {
        // Destructured without `..`, so adding a field to [`KubernetesResources`] fails to
        // compile here instead of silently never being applied.
        //
        // The namenode Listeners are deliberately not part of these resources: this operator
        // never creates them. The listener-operator creates one Listener per namenode pod for
        // the listener volumes declared in the StatefulSets, and this operator only reads them
        // back to build the discovery ConfigMap.
        let KubernetesResources {
            services,
            config_maps,
            pod_disruption_budgets,
            stateful_sets,
            service_accounts,
            role_bindings,
            status: _,
        } = resources;

        // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
        // must exist first, else Pods restart -- commons-operator#111). The ServiceAccount comes
        // first because the Pods reference it at creation time.
        let service_accounts = self.add_resources(service_accounts).await?;
        let role_bindings = self.add_resources(role_bindings).await?;
        let services = self.add_resources(services).await?;
        let config_maps = self.add_resources(config_maps).await?;
        let pod_disruption_budgets = self.add_resources(pod_disruption_budgets).await?;

        // StatefulSets must be rolled out in role order during upgrades (a namenode's version
        // must be >= the datanodes', and so on), with each role finishing its rollout before the
        // next starts.
        // https://hadoop.apache.org/docs/r3.4.0/hadoop-project-dist/hadoop-hdfs/HdfsRollingUpgrade.html#Upgrading_Non-Federated_Clusters
        // The build output is already ordered by role, so it is applied as-is; downgrades have
        // the opposite version relationship and are therefore rolled out in reverse.
        let downgrading = matches!(upgrade_state, Some(UpgradeState::Downgrading));
        if downgrading {
            tracing::info!("HdfsCluster is being downgraded, deploying in reverse order");
        }
        let mut applied_stateful_sets = vec![];
        let mut statefulsets_rolled_out = true;
        for statefulset in reverse_if(downgrading, stateful_sets.into_iter()) {
            let name = statefulset.name_any();
            let applied_statefulset = self
                .cluster_resources
                .add(self.client, statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu { name })?;

            if upgrade_state.is_some()
                && let Err(reason) = check_statefulset_rollout_complete(&applied_statefulset)
            {
                // Ensure each role is fully upgraded before moving on to the next.
                tracing::info!(
                    rolegroup.statefulset = %ObjectRef::from_obj(&applied_statefulset),
                    reason = &reason as &dyn std::error::Error,
                    "rolegroup is still upgrading, waiting..."
                );
                applied_stateful_sets.push(applied_statefulset);
                statefulsets_rolled_out = false;
                break;
            }
            applied_stateful_sets.push(applied_statefulset);
        }

        // During upgrades we do partial deployments; we don't want to garbage collect after
        // those since we *will* redeploy (or properly orphan) the remaining resources later.
        if statefulsets_rolled_out {
            self.cluster_resources
                .delete_orphaned_resources(self.client)
                .await
                .context(DeleteOrphanedResourcesSnafu)?;
        }

        Ok(AppliedResources {
            resources: KubernetesResources {
                stateful_sets: applied_stateful_sets,
                services,
                config_maps,
                pod_disruption_budgets,
                service_accounts,
                role_bindings,
                status: PhantomData,
            },
            statefulsets_rolled_out,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied_resources.push(applied_resource);
        }

        Ok(applied_resources)
    }
}

/// Applies the discovery `ConfigMap` directly, outside the [`ClusterResources`] tracking.
///
/// The CM must stay untracked because it is not emitted on every reconcile run: the namenode
/// `Listener`s are per-pod, so whenever a namenode is scaled up (or a `Listener` briefly has
/// no ingress address), [`crate::crd::namenode_listener_refs`] returns `None` and the CM is
/// skipped for that run. A *tracked* CM would be deleted as an orphan by
/// [`ClusterResources::delete_orphaned_resources`] in every such window and re-created once
/// the addresses are back -- churn for every client watching the discovery CM. Untracked, the
/// existing CM simply stays in place until it can be rebuilt.
///
/// This deliberately differs from Hive and Druid, which do track their discovery CMs: their
/// skip window only opens when a role `Listener` is deleted and re-created, whereas HDFS
/// would hit it on every namenode scale-up.
pub async fn apply_discovery_config_map(client: &Client, discovery_cm: &ConfigMap) -> Result<()> {
    client
        .apply_patch(FIELD_MANAGER_SCOPE, discovery_cm, discovery_cm)
        .await
        .with_context(|_| ApplyDiscoveryConfigMapSnafu {
            name: discovery_cm.metadata.name.clone().unwrap_or_default(),
        })?;
    Ok(())
}
