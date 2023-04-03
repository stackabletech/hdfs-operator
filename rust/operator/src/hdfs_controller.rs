use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    container::ContainerConfig,
    discovery::build_discovery_configmap,
    event::{build_invalid_replica_message, publish_event},
    product_logging::{extend_role_group_config_map, resolve_vector_aggregator_address},
    rbac, OPERATOR_NAME,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::{constants::*, HdfsCluster, HdfsPodRef, HdfsRole, MergedConfig};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder, PodBuilder, PodSecurityContextBuilder},
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, Service, ServicePort, ServiceSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        api::ObjectMeta,
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::role_group_selector_labels,
    logging::controller::ReconcilerError,
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

const RESOURCE_MANAGER_HDFS_CONTROLLER: &str = "hdfs-operator-hdfs-controller";
const HDFS_CONTROLLER: &str = "hdfs-controller";
const DOCKER_IMAGE_BASE_NAME: &str = "hadoop";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Invalid role configuration"))]
    InvalidRoleConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("Invalid product configuration"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Cannot create rolegroup service [{name}]"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Cannot create role group config map [{name}]"))]
    ApplyRoleGroupConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Cannot create role group stateful set [{name}]"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Cannot create discovery config map [{name}]"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("No metadata for [{obj_ref}]"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        obj_ref: ObjectRef<HdfsCluster>,
    },
    #[snafu(display("Invalid role [{role}]"))]
    InvalidRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("Object has no name"))]
    ObjectHasNoName { obj_ref: ObjectRef<HdfsCluster> },
    #[snafu(display("Cannot build config map for role [{role}] and role group [{role_group}]"))]
    BuildRoleGroupConfigMap {
        source: stackable_operator::error::Error,
        role: String,
        role_group: String,
    },
    #[snafu(display("Cannot build config discovery config map"))]
    BuildDiscoveryConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
        name: String,
    },
    #[snafu(display("Failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to create pod references"))]
    CreatePodReferences { source: stackable_hdfs_crd::Error },
    #[snafu(display("Failed to build role properties"))]
    BuildRoleProperties { source: stackable_hdfs_crd::Error },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to merge config"))]
    ConfigMerge { source: stackable_hdfs_crd::Error },
    #[snafu(display("failed to create cluster event"))]
    FailedToCreateClusterEvent { source: crate::event::Error },
    #[snafu(display("failed to create container and volume configuration"))]
    FailedToCreateContainerAndVolumeConfiguration { source: crate::container::Error },
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
}

pub async fn reconcile_hdfs(hdfs: Arc<HdfsCluster>, ctx: Arc<Ctx>) -> HdfsOperatorResult<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;

    let resolved_product_image = hdfs.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let vector_aggregator_address = resolve_vector_aggregator_address(&hdfs, client)
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hdfs.as_ref(),
            hdfs.build_role_properties()
                .context(BuildRolePropertiesSnafu)?,
        )
        .context(InvalidRoleConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    // A list of all name and journal nodes across all role groups is needed for all ConfigMaps and initialization checks.
    let namenode_podrefs = hdfs
        .pod_refs(&HdfsRole::NameNode)
        .context(CreatePodReferencesSnafu)?;
    let journalnode_podrefs = hdfs
        .pod_refs(&HdfsRole::JournalNode)
        .context(CreatePodReferencesSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RESOURCE_MANAGER_HDFS_CONTROLLER,
        &hdfs.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hdfs.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let discovery_cm = build_discovery_configmap(
        &hdfs,
        HDFS_CONTROLLER,
        &namenode_podrefs,
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

    // The service account and rolebinding will be created per cluster and
    // deleted if the cluster is removed.
    // Therefore no cluster / orphaned resources have to be handled here.
    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(hdfs.as_ref(), "hdfs-clusterrole")
        .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
            obj_ref: ObjectRef::from_obj(&*hdfs),
        })?;

    client
        .apply_patch(FIELD_MANAGER_SCOPE, &rbac_sa, &rbac_sa)
        .await
        .with_context(|_| ApplyServiceAccountSnafu {
            name: rbac_sa.name_any(),
        })?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &rbac_rolebinding, &rbac_rolebinding)
        .await
        .with_context(|_| ApplyRoleBindingSnafu {
            name: rbac_rolebinding.name_any(),
        })?;

    let dfs_replication = hdfs.spec.cluster_config.dfs_replication;

    for (role_name, group_config) in validated_config.iter() {
        let role: HdfsRole = HdfsRole::from_str(role_name).with_context(|_| InvalidRoleSnafu {
            role: role_name.to_string(),
        })?;

        if let Some(content) = build_invalid_replica_message(&hdfs, &role, dfs_replication) {
            publish_event(
                &hdfs,
                client,
                "Reconcile",
                "Invalid replicas",
                content.as_ref(),
            )
            .await
            .context(FailedToCreateClusterEventSnafu)?;
        }

        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let merged_config = role
                .merged_config(&hdfs, rolegroup_name)
                .context(ConfigMergeSnafu)?;

            let env_overrides = rolegroup_config.get(&PropertyNameKind::Env);

            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rg_service =
                rolegroup_service(&hdfs, &role, &rolegroup_ref, &resolved_product_image)?;
            let rg_configmap = rolegroup_config_map(
                &hdfs,
                &rolegroup_ref,
                rolegroup_config,
                &namenode_podrefs,
                &journalnode_podrefs,
                &resolved_product_image,
                merged_config.as_ref(),
                vector_aggregator_address.as_deref(),
            )?;

            let rg_statefulset = rolegroup_statefulset(
                &hdfs,
                &role,
                &rolegroup_ref,
                &resolved_product_image,
                env_overrides,
                merged_config.as_ref(),
                &rbac_sa.name_any(),
                &namenode_podrefs,
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
            cluster_resources
                .add(client, rg_statefulset.clone())
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    name: rg_statefulset_name,
                })?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

fn rolegroup_service(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> HdfsOperatorResult<Service> {
    tracing::info!("Setting up Service for {:?}", rolegroup_ref);
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&rolegroup_ref.object_name())
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
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(
                role.ports()
                    .into_iter()
                    .map(|(name, value)| ServicePort {
                        name: Some(name),
                        port: i32::from(value),
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    })
                    .collect(),
            ),
            selector: Some(hdfs.rolegroup_selector_labels(rolegroup_ref)),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn rolegroup_config_map(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
    resolved_product_image: &ResolvedProductImage,
    merged_config: &(dyn MergedConfig + Send + 'static),
    vector_aggregator_address: Option<&str>,
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

    for (property_name_kind, config) in rolegroup_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HDFS_SITE_XML => {
                hdfs_site_xml = HdfsSiteConfigBuilder::new(hdfs_name.to_string())
                    // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
                    // be formatted by the namenode, or used by the other services.
                    // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
                    .dfs_namenode_name_dir()
                    .dfs_datanode_data_dir(merged_config.data_node_resources().map(|r| r.storage))
                    .dfs_journalnode_edits_dir()
                    .dfs_replication(
                        *hdfs
                            .spec
                            .cluster_config
                            .dfs_replication
                            .as_ref()
                            .unwrap_or(&DEFAULT_DFS_REPLICATION_FACTOR),
                    )
                    .dfs_name_services()
                    .dfs_ha_namenodes(namenode_podrefs)
                    .dfs_namenode_shared_edits_dir(journalnode_podrefs)
                    .dfs_namenode_name_dir_ha(namenode_podrefs)
                    .dfs_namenode_rpc_address_ha(namenode_podrefs)
                    .dfs_namenode_http_address_ha(namenode_podrefs)
                    .dfs_client_failover_proxy_provider()
                    .add("dfs.ha.fencing.methods", "shell(/bin/true)")
                    .add("dfs.ha.nn.not-become-active-in-safemode", "true")
                    .add("dfs.ha.automatic-failover.enabled", "true")
                    .add("dfs.ha.namenode.id", "${env.POD_NAME}")
                    // the extend with config must come last in order to have overrides working!!!
                    .extend(config)
                    .build_as_xml();
            }
            PropertyNameKind::File(file_name) if file_name == CORE_SITE_XML => {
                core_site_xml = CoreSiteConfigBuilder::new(hdfs_name.to_string())
                    .fs_default_fs()
                    .ha_zookeeper_quorum()
                    // the extend with config must come last in order to have overrides working!!!
                    .extend(config)
                    .build_as_xml();
            }
            _ => {}
        }
    }

    let mut builder = ConfigMapBuilder::new();

    builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .name(&rolegroup_ref.object_name())
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
                .build(),
        )
        .add_data(CORE_SITE_XML.to_string(), core_site_xml)
        .add_data(HDFS_SITE_XML.to_string(), hdfs_site_xml);

    extend_role_group_config_map(
        rolegroup_ref,
        vector_aggregator_address,
        merged_config,
        &mut builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup_ref.object_name(),
    })?;

    builder
        .build()
        .with_context(|_| BuildRoleGroupConfigMapSnafu {
            role: rolegroup_ref.role.clone(),
            role_group: rolegroup_ref.role_group.clone(),
        })
}

#[allow(clippy::too_many_arguments)]
fn rolegroup_statefulset(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    merged_config: &(dyn MergedConfig + Send + 'static),
    rbac_sa: &str,
    namenode_podrefs: &[HdfsPodRef],
) -> HdfsOperatorResult<StatefulSet> {
    tracing::info!("Setting up StatefulSet for {:?}", rolegroup_ref);

    let object_name = rolegroup_ref.object_name();
    // PodBuilder for StatefulSet Pod template.
    let mut pb = PodBuilder::new();
    pb.metadata(ObjectMeta {
        labels: Some(hdfs.rolegroup_selector_labels(rolegroup_ref)),
        ..ObjectMeta::default()
    })
    .image_pull_secrets_from_product_image(resolved_product_image)
    .affinity(merged_config.affinity())
    .service_account_name(rbac_sa)
    .security_context(
        PodSecurityContextBuilder::new()
            .run_as_user(1000)
            .run_as_group(1000)
            .fs_group(1000)
            .build(),
    );

    // Adds all containers and volumes to the pod builder
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        role,
        resolved_product_image,
        merged_config,
        env_overrides,
        &hdfs.spec.cluster_config.zookeeper_config_map_name,
        &object_name,
        namenode_podrefs,
    )
    .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&rolegroup_ref.object_name())
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
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("OrderedReady".to_string()),
            replicas: Some(role.role_group_replicas(hdfs, &rolegroup_ref.role_group)),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    hdfs,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: object_name,
            template: pb.build_template(),

            volume_claim_templates: ContainerConfig::volume_claim_templates(role, merged_config),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_obj: Arc<HdfsCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
