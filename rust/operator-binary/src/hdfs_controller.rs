use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use const_format::concatcp;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    cli::OperatorEnvironmentOptions,
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    iter::reverse_if,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, ServiceAccount},
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
    OPERATOR_NAME, build_recommended_labels,
    config::writer::PropertiesWriterError,
    container::{self, ContainerConfig},
    controller::build::properties::{
        ConfigFileName, core_site, hadoop_policy, hdfs_site, security_properties, ssl_client,
        ssl_server,
    },
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
    security::opa::HdfsOpaConfig,
    service::{self, rolegroup_headless_service, rolegroup_metrics_service},
};

pub const RESOURCE_MANAGER_HDFS_CONTROLLER: &str = "hdfs-operator-hdfs-controller";
const HDFS_CONTROLLER_NAME: &str = "hdfs-controller";
pub const HDFS_FULL_CONTROLLER_NAME: &str = concatcp!(HDFS_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub const CONTAINER_IMAGE_BASE_NAME: &str = "hadoop";

/// The validated cluster: proves that config merging and validation succeeded
/// for every role and role group before any resources are created. Placed in the
/// controller so that subsequent steps that reference this struct only depend on
/// the controller.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    pub image: ResolvedProductImage,
    pub role_groups: BTreeMap<HdfsNodeRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HdfsNodeRole, ValidatedRoleConfig>,
    pub hdfs_opa_config: Option<HdfsOpaConfig>,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}

/// Per-rolegroup configuration: the merged CRD config plus the merged
/// (role <- role group) `configOverrides` and `envOverrides`.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: AnyNodeConfig,
    pub config_overrides: v1alpha1::HdfsConfigOverrides,
    pub env_overrides: BTreeMap<String, String>,
}

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

    #[snafu(display("no metadata for {obj_ref:?}"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        obj_ref: ObjectRef<v1alpha1::HdfsCluster>,
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

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name:?}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

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

    #[snafu(display("failed to serialize {} for {rolegroup}", ConfigFileName::Security))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("could not parse HDFS role [{role}]"))]
    UnidentifiedHdfsRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: crate::crd::Error },

    #[snafu(display("failed to build cluster resources label"))]
    BuildClusterResourcesLabel { source: LabelError },

    #[snafu(display("failed to build role-group volume claim templates from config"))]
    BuildRoleGroupVolumeClaimTemplates { source: container::Error },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build core-site.xml"))]
    BuildCoreSiteXml { source: core_site::Error },

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

    let validated = crate::controller::validate::validate_cluster(
        hdfs,
        &ctx.operator_environment.image_repository,
        dereferenced.hdfs_opa_config,
    )
    .context(ValidateSnafu)?;

    let resolved_product_image = &validated.image;

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
        let Some(group_config) = validated.role_groups.get(&role) else {
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

        for (rolegroup_name, validated_rg_config) in group_config.iter() {
            let merged_config = &validated_rg_config.merged_config;

            let env_overrides = &validated_rg_config.env_overrides;

            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rg_service =
                rolegroup_headless_service(hdfs, &role, &rolegroup_ref, resolved_product_image)
                    .context(BuildServiceSnafu)?;
            let rg_metrics_service =
                rolegroup_metrics_service(hdfs, &role, &rolegroup_ref, resolved_product_image)
                    .context(BuildServiceSnafu)?;

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
                .with_recommended_labels(&build_recommended_labels(
                    hdfs,
                    RESOURCE_MANAGER_HDFS_CONTROLLER,
                    &resolved_product_image.app_version_label_value,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                ))
                .context(ObjectMetaSnafu)?;

            let rg_configmap = rolegroup_config_map(
                hdfs,
                &client.kubernetes_cluster_info,
                metadata,
                &rolegroup_ref,
                &validated_rg_config.config_overrides,
                &namenode_podrefs,
                &journalnode_podrefs,
                merged_config,
                &validated.hdfs_opa_config,
            )?;

            let rg_statefulset = rolegroup_statefulset(
                hdfs,
                &client.kubernetes_cluster_info,
                metadata,
                &role,
                &rolegroup_ref,
                resolved_product_image,
                Some(env_overrides),
                merged_config,
                &namenode_podrefs,
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

        if let Some(validated_role_config) = validated.role_configs.get(&role) {
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
    let discovery_cm = build_discovery_configmap(
        hdfs,
        &client.kubernetes_cluster_info,
        HDFS_CONTROLLER_NAME,
        &hdfs
            .namenode_listener_refs(client)
            .await
            .context(CollectDiscoveryConfigSnafu)?,
        resolved_product_image,
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

#[allow(clippy::too_many_arguments)]
fn rolegroup_config_map(
    hdfs: &v1alpha1::HdfsCluster,
    cluster_info: &KubernetesClusterInfo,
    metadata: &ObjectMetaBuilder,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
    config_overrides: &v1alpha1::HdfsConfigOverrides,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
    merged_config: &AnyNodeConfig,
    hdfs_opa_config: &Option<HdfsOpaConfig>,
) -> HdfsOperatorResult<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);

    let role = HdfsNodeRole::from_str(&rolegroup_ref.role).with_context(|_| {
        UnidentifiedHdfsRoleSnafu {
            role: rolegroup_ref.role.clone(),
        }
    })?;
    let hdfs_name = hdfs
        .metadata
        .name
        .as_deref()
        .with_context(|| ObjectHasNoNameSnafu {
            obj_ref: ObjectRef::from_obj(hdfs),
        })?;

    let hdfs_site_xml = hdfs_site::build(
        hdfs,
        hdfs_name,
        cluster_info,
        merged_config,
        namenode_podrefs,
        journalnode_podrefs,
        hdfs_opa_config.as_ref(),
        config_overrides.hdfs_site_xml.clone(),
    );
    let core_site_xml = core_site::build(
        hdfs,
        hdfs_name,
        role,
        cluster_info,
        hdfs_opa_config.as_ref(),
        config_overrides.core_site_xml.clone(),
    )
    .context(BuildCoreSiteXmlSnafu)?;
    let hadoop_policy_xml = hadoop_policy::build(config_overrides.hadoop_policy_xml.clone());
    let ssl_server_xml = ssl_server::build(
        hdfs.has_https_enabled(),
        config_overrides.ssl_server_xml.clone(),
    );
    let ssl_client_xml = ssl_client::build(
        hdfs.has_https_enabled(),
        config_overrides.ssl_client_xml.clone(),
    );

    let mut builder = ConfigMapBuilder::new();
    builder
        .metadata(metadata.build())
        .add_data(ConfigFileName::CoreSite.to_string(), core_site_xml)
        .add_data(ConfigFileName::HdfsSite.to_string(), hdfs_site_xml)
        .add_data(ConfigFileName::HadoopPolicy.to_string(), hadoop_policy_xml)
        .add_data(ConfigFileName::SslServer.to_string(), ssl_server_xml)
        .add_data(ConfigFileName::SslClient.to_string(), ssl_client_xml)
        .add_data(
            ConfigFileName::Security.to_string(),
            security_properties::build(config_overrides.security_properties.clone()).with_context(
                |_| JvmSecurityPropertiesSnafu {
                    rolegroup: rolegroup_ref.role_group.clone(),
                },
            )?,
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
        rolegroup_ref,
        resolved_product_image,
        merged_config,
        env_overrides,
        &hdfs.spec.cluster_config.zookeeper_config_map_name,
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
        service_name: Some(rolegroup_ref.object_name()),
        template: pod_template,

        volume_claim_templates: Some(pvcs),
        ..StatefulSetSpec::default()
    };

    // TODO: The restart-controller is currently not enabled via the label RESTART_CONTROLLER_ENABLED_LABEL.
    // This is due to problems that might appear when restarting pods during the initial formatting of namenodes.
    // See: https://github.com/stackabletech/hdfs-operator/issues/750 (disable restart-controller)
    //      https://github.com/stackabletech/issues/issues/816 (enable restart-controller)
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
    use crate::controller::validate::validate_cluster;

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
        let hdfs: v1alpha1::HdfsCluster = serde_yaml::from_str(cr).unwrap();

        let validated = validate_cluster(&hdfs, "oci.example.org", None).unwrap();

        let role = HdfsNodeRole::Data;
        let validated_rg_config = validated
            .role_groups
            .get(&role)
            .unwrap()
            .get("default")
            .unwrap();
        let rolegroup_ref = hdfs.rolegroup_ref(role.to_string(), "default");
        let env_overrides = &validated_rg_config.env_overrides;
        let merged_config = &validated_rg_config.merged_config;
        let resolved_product_image = &validated.image;

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
            merged_config,
            Some(env_overrides),
            &hdfs.spec.cluster_config.zookeeper_config_map_name,
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
