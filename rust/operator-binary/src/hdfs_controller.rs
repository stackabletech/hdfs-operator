use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use const_format::concatcp;
use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    iter::reverse_if,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, Service, ServiceAccount, ServicePort, ServiceSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, events::Recorder, reflector::ObjectRef},
    },
    kvp::{Label, LabelError, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::{
        condition::{
            compute_conditions, operations::ClusterOperationsConditionBuilder,
            statefulset::StatefulSetConditionBuilder,
        },
        rollout::check_statefulset_rollout_complete,
    },
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    OPERATOR_NAME, build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    container::{self, ContainerConfig, TLS_STORE_DIR, TLS_STORE_PASSWORD},
    crd::{
        AnyNodeConfig, HdfsClusterStatus, HdfsNodeRole, HdfsPodRef, UpgradeState,
        UpgradeStateError, constants::*, v1alpha1,
    },
    discovery::{self, build_discovery_configmap},
    event::{build_invalid_replica_message, publish_warning_event},
    operations::{
        graceful_shutdown::{self, add_graceful_shutdown_config},
        pdb::add_pdbs,
    },
    product_logging::extend_role_group_config_map,
    security::{self, kerberos, opa::HdfsOpaConfig},
};

pub const RESOURCE_MANAGER_HDFS_CONTROLLER: &str = "hdfs-operator-hdfs-controller";
const HDFS_CONTROLLER_NAME: &str = "hdfs-controller";
pub const HDFS_FULL_CONTROLLER_NAME: &str = concatcp!(HDFS_CONTROLLER_NAME, '.', OPERATOR_NAME);

const DOCKER_IMAGE_BASE_NAME: &str = "hadoop";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("invalid role configuration"))]
    InvalidRoleConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product configuration"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
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

    #[snafu(display("no metadata for {obj_ref:?}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        obj_ref: ObjectRef<v1alpha1::HdfsCluster>,
    },

    #[snafu(display("invalid role {role:?}"))]
    InvalidRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("object has no name"))]
    ObjectHasNoName {
        obj_ref: ObjectRef<v1alpha1::HdfsCluster>,
    },

    #[snafu(display("cannot build config map for role {role:?} and role group {role_group:?}"))]
    BuildRoleGroupConfigMap {
        source: stackable_operator::builder::configmap::Error,
        role: String,
        role_group: String,
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

    #[snafu(display("failed to create pod references"))]
    CreatePodReferences { source: crate::crd::Error },

    #[snafu(display("failed to build role properties"))]
    BuildRoleProperties { source: crate::crd::Error },

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name:?}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to merge config"))]
    ConfigMerge { source: crate::crd::Error },

    #[snafu(display("failed to create cluster event"))]
    FailedToCreateClusterEvent { source: crate::event::Error },

    #[snafu(display("failed to create container and volume configuration"))]
    FailedToCreateContainerAndVolumeConfiguration { source: crate::container::Error },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display(
        "failed to serialize {JVM_SECURITY_PROPERTIES_FILE:?} for {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: crate::crd::Error },

    #[snafu(display("failed to build prometheus label"))]
    BuildPrometheusLabel { source: LabelError },

    #[snafu(display("failed to build cluster resources label"))]
    BuildClusterResourcesLabel { source: LabelError },

    #[snafu(display("failed to build role-group selector label"))]
    BuildRoleGroupSelectorLabel { source: LabelError },

    #[snafu(display("failed to build role-group volume claim templates from config"))]
    BuildRoleGroupVolumeClaimTemplates { source: container::Error },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build security config"))]
    BuildSecurityConfig { source: kerberos::Error },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig { source: security::opa::Error },

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
    pub product_config: ProductConfigManager,
    pub event_recorder: Arc<Recorder>,
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

    let resolved_product_image = hdfs
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hdfs,
            hdfs.build_role_properties()
                .context(BuildRolePropertiesSnafu)?,
        )
        .context(InvalidRoleConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let hdfs_obj_ref = hdfs.object_ref(&());
    // A list of all name and journal nodes across all role groups is needed for all ConfigMaps and initialization checks.
    let namenode_podrefs = hdfs
        .pod_refs(&HdfsNodeRole::Name)
        .context(CreatePodReferencesSnafu)?;
    let journalnode_podrefs = hdfs
        .pod_refs(&HdfsNodeRole::Journal)
        .context(CreatePodReferencesSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RESOURCE_MANAGER_HDFS_CONTROLLER,
        &hdfs_obj_ref,
        ClusterResourceApplyStrategy::from(&hdfs.spec.cluster_operation),
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

    let hdfs_opa_config = match &hdfs.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HdfsOpaConfig::from_opa_config(client, hdfs, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

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
        let Some(group_config) = validated_config.get(role_name) else {
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

        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let merged_config = role
                .merged_config(hdfs, rolegroup_name)
                .context(ConfigMergeSnafu)?;

            let env_overrides = rolegroup_config.get(&PropertyNameKind::Env);

            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            // We need to split the creation and the usage of the "metadata" variable in two statements.
            // to avoid the compiler error "E0716 (temporary value dropped while borrowed)".
            let mut metadata = ObjectMetaBuilder::new();
            let metadata = metadata
                .name_and_namespace(hdfs)
                .name(rolegroup_ref.object_name())
                .ownerreference_from_resource(hdfs, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    obj_ref: ObjectRef::from_obj(hdfs),
                })?
                .with_recommended_labels(build_recommended_labels(
                    hdfs,
                    RESOURCE_MANAGER_HDFS_CONTROLLER,
                    &resolved_product_image.app_version_label,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                ))
                .context(ObjectMetaSnafu)?;

            let rg_service = rolegroup_service(hdfs, metadata, &role, &rolegroup_ref)?;

            let rg_configmap = rolegroup_config_map(
                hdfs,
                &client.kubernetes_cluster_info,
                metadata,
                &rolegroup_ref,
                rolegroup_config,
                &namenode_podrefs,
                &journalnode_podrefs,
                &merged_config,
                &hdfs_opa_config,
            )?;

            let rg_statefulset = rolegroup_statefulset(
                hdfs,
                &client.kubernetes_cluster_info,
                metadata,
                &role,
                &rolegroup_ref,
                &resolved_product_image,
                env_overrides,
                &merged_config,
                &namenode_podrefs,
                &rbac_sa,
            )?;

            let rg_service_name = rg_service.name_any();
            cluster_resources
                .add(client, rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    name: rg_service_name,
                })?;
            let rg_configmap_name = rg_configmap.name_any();
            cluster_resources
                .add(client, rg_configmap.clone())
                .await
                .with_context(|_| ApplyRoleGroupConfigMapSnafu {
                    name: rg_configmap_name,
                })?;
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

        let role_config = hdfs.role_config(&role);
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = role_config
        {
            add_pdbs(pdb, hdfs, &role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm = build_discovery_configmap(
        hdfs,
        &client.kubernetes_cluster_info,
        HDFS_CONTROLLER_NAME,
        &hdfs
            .namenode_listener_refs(client)
            .await
            .context(CollectDiscoveryConfigSnafu)?,
        &resolved_product_image,
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

fn rolegroup_service(
    hdfs: &v1alpha1::HdfsCluster,
    metadata: &ObjectMetaBuilder,
    role: &HdfsNodeRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
) -> HdfsOperatorResult<Service> {
    tracing::info!("Setting up Service for {:?}", rolegroup_ref);

    let prometheus_label =
        Label::try_from(("prometheus.io/scrape", "true")).context(BuildPrometheusLabelSnafu)?;
    let mut metadata_with_prometheus_label = metadata.clone();
    metadata_with_prometheus_label.with_label(prometheus_label);

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(
            hdfs.ports(role)
                .into_iter()
                .map(|(name, value)| ServicePort {
                    name: Some(name),
                    port: i32::from(value),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                })
                .collect(),
        ),
        selector: Some(
            hdfs.rolegroup_selector_labels(rolegroup_ref)
                .context(RoleGroupSelectorLabelsSnafu)?
                .into(),
        ),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata: metadata_with_prometheus_label.build(),
        spec: Some(service_spec),
        status: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn rolegroup_config_map(
    hdfs: &v1alpha1::HdfsCluster,
    cluster_info: &KubernetesClusterInfo,
    metadata: &ObjectMetaBuilder,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
    merged_config: &AnyNodeConfig,
    hdfs_opa_config: &Option<HdfsOpaConfig>,
) -> HdfsOperatorResult<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);
    let hdfs_name = hdfs
        .metadata
        .name
        .as_deref()
        .with_context(|| ObjectHasNoNameSnafu {
            obj_ref: ObjectRef::from_obj(hdfs),
        })?;

    let mut hdfs_site_xml = String::new();
    let mut core_site_xml = String::new();
    let mut hadoop_policy_xml = String::new();
    let mut ssl_server_xml = String::new();
    let mut ssl_client_xml = String::new();

    for (property_name_kind, config) in rolegroup_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HDFS_SITE_XML => {
                // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
                // be formatted by the namenode, or used by the other services.
                // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
                //
                // Notes on configuration choices
                // ===============================
                // We used to set `dfs.ha.nn.not-become-active-in-safemode` to true here due to
                // badly worded HDFS documentation:
                // https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
                // This caused a deadlock with no namenode becoming active during a startup after
                // HDFS was completely down for a while.

                let mut hdfs_site = HdfsSiteConfigBuilder::new(hdfs_name.to_string());
                hdfs_site
                    .dfs_namenode_name_dir()
                    .dfs_datanode_data_dir(
                        merged_config
                            .as_datanode()
                            .map(|node| node.resources.storage.clone()),
                    )
                    .dfs_journalnode_edits_dir()
                    .dfs_replication(hdfs.spec.cluster_config.dfs_replication)
                    .dfs_name_services()
                    .dfs_ha_namenodes(namenode_podrefs)
                    .dfs_namenode_shared_edits_dir(cluster_info, journalnode_podrefs)
                    .dfs_namenode_name_dir_ha(namenode_podrefs)
                    .dfs_namenode_rpc_address_ha(cluster_info, namenode_podrefs)
                    .dfs_namenode_http_address_ha(hdfs, cluster_info, namenode_podrefs)
                    .dfs_client_failover_proxy_provider()
                    .security_config(hdfs)
                    .add("dfs.ha.fencing.methods", "shell(/bin/true)")
                    .add("dfs.ha.automatic-failover.enabled", "true")
                    .add("dfs.ha.namenode.id", "${env.POD_NAME}")
                    .add(
                        "dfs.namenode.datanode.registration.unsafe.allow-address-override",
                        "true",
                    )
                    .add("dfs.datanode.registered.hostname", "${env.POD_ADDRESS}")
                    .add("dfs.datanode.registered.port", "${env.DATA_PORT}")
                    .add("dfs.datanode.registered.ipc.port", "${env.IPC_PORT}")
                    // The following two properties are set to "true" because there is a minor chance that data
                    // written to HDFS is not synced to disk even if a block has been closed.
                    // Users in HBase can control this explicitly for the WAL, but for flushes and compactions
                    // I believe they can't as easily (if at all).
                    // In theory, HBase should be able to recover from these failures, but that comes at a cost
                    // and there's always a risk.
                    // Enabling this behavior causes HDFS to sync to disk as soon as possible.
                    .add("dfs.datanode.sync.behind.writes", "true")
                    .add("dfs.datanode.synconclose", "true")
                    // Defaults to 10 since at least 2011.
                    // This controls the concurrent number of client connections (this includes DataNodes)
                    // to the NameNode. Ideally, we'd scale this with the number of DataNodes but this would
                    // lead to restarts of the NameNode.
                    // This should lead to better performance due to more concurrency.
                    .add("dfs.namenode.handler.count", "50")
                    // Defaults to 10 since at least 2012.
                    // This controls the concurrent number of client connections to the DataNodes.
                    // We have no idea how many clients there may be, so it's hard to assign a good default.
                    // Increasing to 50 should lead to better performance due to more concurrency, especially
                    // with use-cases like HBase.
                    .add("dfs.datanode.handler.count", "50")
                    // The following two properties default to 2 and 4 respectively since around 2013.
                    // They control the number of maximum replication "jobs" a NameNode assigns to
                    // a DataNode in a single heartbeat.
                    // Increasing this number will increase network usage during replication events
                    // but can lead to faster recovery.
                    .add("dfs.namenode.replication.max-streams", "4")
                    .add("dfs.namenode.replication.max-streams-hard-limit", "8")
                    // Defaults to 4096 and hasn't changed since at least 2011.
                    // The number of threads used for actual data transfer, so not very CPU heavy
                    // but IO bound. This is why the number is relatively high.
                    // But today's Java and IO should be able to handle more, so bump it to 8192 for
                    // better performance/concurrency.
                    .add("dfs.datanode.max.transfer.threads", "8192");
                if hdfs.has_https_enabled() {
                    hdfs_site.add("dfs.datanode.registered.https.port", "${env.HTTPS_PORT}");
                } else {
                    hdfs_site.add("dfs.datanode.registered.http.port", "${env.HTTP_PORT}");
                }
                if let Some(hdfs_opa_config) = hdfs_opa_config {
                    hdfs_opa_config.add_hdfs_site_config(&mut hdfs_site);
                }
                // the extend with config must come last in order to have overrides working!!!
                hdfs_site_xml = hdfs_site.extend(config).build_as_xml();
            }
            PropertyNameKind::File(file_name) if file_name == CORE_SITE_XML => {
                let mut core_site = CoreSiteConfigBuilder::new(hdfs_name.to_string());
                core_site
                    .fs_default_fs()
                    .ha_zookeeper_quorum()
                    .security_config(hdfs, cluster_info)
                    .context(BuildSecurityConfigSnafu)?
                    .enable_prometheus_endpoint()
                    // The default (4096) hasn't changed since 2009.
                    // Increase to 128k to allow for faster transfers.
                    .add("io.file.buffer.size", "131072");
                if let Some(hdfs_opa_config) = hdfs_opa_config {
                    hdfs_opa_config.add_core_site_config(&mut core_site);
                }
                // the extend with config must come last in order to have overrides working!!!
                core_site_xml = core_site.extend(config).build_as_xml();
            }
            PropertyNameKind::File(file_name) if file_name == HADOOP_POLICY_XML => {
                // We don't add any settings here, the main purpose is to have a configOverride for users.
                let mut config_opts: BTreeMap<String, Option<String>> = BTreeMap::new();
                config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                hadoop_policy_xml = to_hadoop_xml(config_opts.iter());
            }
            PropertyNameKind::File(file_name) if file_name == SSL_SERVER_XML => {
                let mut config_opts = BTreeMap::new();
                if hdfs.has_https_enabled() {
                    config_opts.extend([
                        (
                            "ssl.server.truststore.location".to_string(),
                            Some(format!("{TLS_STORE_DIR}/truststore.p12")),
                        ),
                        (
                            "ssl.server.truststore.type".to_string(),
                            Some("pkcs12".to_string()),
                        ),
                        (
                            "ssl.server.truststore.password".to_string(),
                            Some(TLS_STORE_PASSWORD.to_string()),
                        ),
                        (
                            "ssl.server.keystore.location".to_string(),
                            Some(format!("{TLS_STORE_DIR}/keystore.p12")),
                        ),
                        (
                            "ssl.server.keystore.type".to_string(),
                            Some("pkcs12".to_string()),
                        ),
                        (
                            "ssl.server.keystore.password".to_string(),
                            Some(TLS_STORE_PASSWORD.to_string()),
                        ),
                    ]);
                }
                config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                ssl_server_xml = to_hadoop_xml(config_opts.iter());
            }
            PropertyNameKind::File(file_name) if file_name == SSL_CLIENT_XML => {
                let mut config_opts = BTreeMap::new();
                if hdfs.has_https_enabled() {
                    config_opts.extend([
                        (
                            "ssl.client.truststore.location".to_string(),
                            Some(format!("{TLS_STORE_DIR}/truststore.p12")),
                        ),
                        (
                            "ssl.client.truststore.type".to_string(),
                            Some("pkcs12".to_string()),
                        ),
                        (
                            "ssl.client.truststore.password".to_string(),
                            Some(TLS_STORE_PASSWORD.to_string()),
                        ),
                    ]);
                }
                config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                ssl_client_xml = to_hadoop_xml(config_opts.iter());
            }
            _ => {}
        }
    }

    let mut builder = ConfigMapBuilder::new();

    let jvm_sec_props: BTreeMap<String, Option<String>> = rolegroup_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    builder
        .metadata(metadata.build())
        .add_data(CORE_SITE_XML.to_string(), core_site_xml)
        .add_data(HDFS_SITE_XML.to_string(), hdfs_site_xml)
        .add_data(HADOOP_POLICY_XML.to_string(), hadoop_policy_xml)
        .add_data(SSL_SERVER_XML, ssl_server_xml)
        .add_data(SSL_CLIENT_XML, ssl_client_xml)
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    rolegroup: rolegroup_ref.role_group.clone(),
                }
            })?,
        );

    extend_role_group_config_map(rolegroup_ref, merged_config, &mut builder).context(
        InvalidLoggingConfigSnafu {
            cm_name: rolegroup_ref.object_name(),
        },
    )?;

    builder
        .build()
        .with_context(|_| BuildRoleGroupConfigMapSnafu {
            role: rolegroup_ref.role.clone(),
            role_group: rolegroup_ref.role_group.clone(),
        })
}

#[allow(clippy::too_many_arguments)]
fn rolegroup_statefulset(
    hdfs: &v1alpha1::HdfsCluster,
    cluster_info: &KubernetesClusterInfo,
    metadata: &ObjectMetaBuilder,
    role: &HdfsNodeRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    merged_config: &AnyNodeConfig,
    namenode_podrefs: &[HdfsPodRef],
    service_account: &ServiceAccount,
) -> HdfsOperatorResult<StatefulSet> {
    tracing::info!("Setting up StatefulSet for {:?}", rolegroup_ref);

    let object_name = rolegroup_ref.object_name();
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
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_config.affinity)
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    // Adds all containers and volumes to the pod builder
    // We must use the selector labels ("rolegroup_selector_labels") and not the recommended labels
    // for the ephemeral listener volumes created by this function.
    // This is because the recommended set contains a "managed-by" label. This labels triggers
    // the cluster resources to "manage" listeners which is wrong and leads to errors.
    // The listeners are managed by the listener-operator.
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        hdfs,
        cluster_info,
        role,
        &rolegroup_ref.role_group,
        resolved_product_image,
        merged_config,
        env_overrides,
        &hdfs.spec.cluster_config.zookeeper_config_map_name,
        &object_name,
        namenode_podrefs,
        &rolegroup_selector_labels,
    )
    .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    add_graceful_shutdown_config(merged_config, &mut pb).context(GracefulShutdownSnafu)?;

    let mut pod_template = pb.build_template();
    if let Some(pod_overrides) = hdfs.pod_overrides_for_role(role) {
        pod_template.merge_from(pod_overrides.clone());
    }
    if let Some(pod_overrides) = hdfs.pod_overrides_for_role_group(role, &rolegroup_ref.role_group)
    {
        pod_template.merge_from(pod_overrides.clone());
    }

    // The same comment regarding labels is valid here as it is for the ContainerConfig::add_containers_and_volumes() call above.
    let pvcs = ContainerConfig::volume_claim_templates(merged_config, &rolegroup_selector_labels)
        .context(BuildRoleGroupVolumeClaimTemplatesSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: role
            .role_group_replicas(hdfs, &rolegroup_ref.role_group)
            .map(i32::from),
        selector: LabelSelector {
            match_labels: Some(rolegroup_selector_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(object_name),
        template: pod_template,

        volume_claim_templates: Some(pvcs),
        ..StatefulSetSpec::default()
    };

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

    #[test]
    pub fn test_env_overrides() {
        let cr = "
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
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
        let product_config = "
---
version: 0.1.0
spec:
  units: []
properties: []
";

        let hdfs: v1alpha1::HdfsCluster = serde_yaml::from_str(cr).unwrap();

        let config =
            transform_all_roles_to_config(&hdfs, hdfs.build_role_properties().unwrap()).unwrap();

        let validated_config = validate_all_roles_and_groups_config(
            "3.4.0",
            &config,
            &product_config.parse::<ProductConfigManager>().unwrap(),
            false,
            false,
        )
        .unwrap();

        let role = HdfsNodeRole::Data;
        let rolegroup_config = validated_config
            .get(&role.to_string())
            .unwrap()
            .get("default")
            .unwrap();
        let env_overrides = rolegroup_config.get(&PropertyNameKind::Env);

        let merged_config = role.merged_config(&hdfs, "default").unwrap();
        let resolved_product_image = hdfs.spec.image.resolve(DOCKER_IMAGE_BASE_NAME, "0.0.0-dev");

        let mut pb = PodBuilder::new();
        pb.metadata(ObjectMeta::default());
        ContainerConfig::add_containers_and_volumes(
            &mut pb,
            &hdfs,
            &KubernetesClusterInfo {
                cluster_domain: DomainName::try_from("cluster.local").unwrap(),
            },
            &role,
            "default",
            &resolved_product_image,
            &merged_config,
            env_overrides,
            &hdfs.spec.cluster_config.zookeeper_config_map_name,
            "todo",
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
