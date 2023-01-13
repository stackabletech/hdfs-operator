use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsNodeDataDirectory, HdfsSiteConfigBuilder},
    discovery::build_discovery_configmap,
    event::{build_invalid_replica_message, publish_event},
    product_logging::{extend_role_group_config_map, resolve_vector_aggregator_address},
    rbac, OPERATOR_NAME,
};

use crate::container::ContainerConfig;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::{constants::*, HdfsCluster, HdfsPodRef, HdfsRole, MergedConfig};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ObjectMetaBuilder, PodBuilder, PodSecurityContextBuilder, VolumeBuilder,
    },
    client::Client,
    cluster_resources::ClusterResources,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, Service, ServicePort,
                ServiceSpec,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
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
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
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

pub const MAX_LOG_FILES_SIZE_IN_MIB: u32 = 10;
const OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB: u32 = 1;
// We have a maximum of 2 continuous logging files and 2 * 2 (stderr, stdout) for init containers (namenode)
// - name node main container (1)
// - zkfc side container (1)
// - format namenode init container -> stdout + stderr (2) -> small
// - format zookeeper init container -> stdout + stderr (2) -> small
const LOG_VOLUME_SIZE_IN_MIB: u32 =
    6 * (MAX_LOG_FILES_SIZE_IN_MIB + OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB);

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
    #[snafu(display("failed to create (init) container"))]
    FailedToCreateContainer { source: crate::container::Error },
    #[snafu(display("failed to create (init) container"))]
    FailedToCreateContainerConfig { source: crate::container::Error },
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

    let dfs_replication = hdfs.spec.dfs_replication;

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

            cluster_resources
                .add(client, &rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    name: rg_service.metadata.name.clone().unwrap_or_default(),
                })?;
            cluster_resources
                .add(client, &rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigMapSnafu {
                    name: rg_configmap.metadata.name.clone().unwrap_or_default(),
                })?;
            cluster_resources
                .add(client, &rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    name: rg_statefulset.metadata.name.clone().unwrap_or_default(),
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
                hdfs_site_xml = HdfsSiteConfigBuilder::new(
                    hdfs_name.to_string(),
                    HdfsNodeDataDirectory::default(),
                )
                // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
                // be formatted by the namenode, or used by the other services.
                // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
                .dfs_namenode_name_dir()
                .dfs_datanode_data_dir()
                .dfs_journalnode_edits_dir()
                .dfs_replication(
                    *hdfs
                        .spec
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

    let service_name = rolegroup_ref.object_name();
    // PodBuilder for StatefulSet Pod template.
    let mut pb = PodBuilder::new();
    // common pod settings
    pb.metadata(ObjectMeta {
        labels: Some(hdfs.rolegroup_selector_labels(rolegroup_ref)),
        ..ObjectMeta::default()
    })
    .image_pull_secrets_from_product_image(resolved_product_image)
    .add_volume(
        VolumeBuilder::new(ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME)
            .config_map(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            })
            .build(),
    )
    .add_volume(
        VolumeBuilder::new(ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME)
            .empty_dir(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
            })
            .build(),
    )
    .service_account_name(rbac_sa)
    .security_context(
        PodSecurityContextBuilder::new()
            .run_as_user(1000)
            .run_as_group(1000)
            .fs_group(1000)
            .build(),
    );

    if let ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    } = merged_config.hdfs_logging()
    {
        pb.add_volume(
            VolumeBuilder::new(ContainerConfig::HDFS_LOG_VOLUME_MOUNT_NAME)
                .config_map(ConfigMapVolumeSource {
                    name: Some(config_map),
                    ..ConfigMapVolumeSource::default()
                })
                .build(),
        );
    } else {
        pb.add_volume(
            VolumeBuilder::new(ContainerConfig::HDFS_LOG_VOLUME_MOUNT_NAME)
                .config_map(ConfigMapVolumeSource {
                    name: Some(rolegroup_ref.object_name()),
                    ..ConfigMapVolumeSource::default()
                })
                .build(),
        );
    }

    if let Some(zkfc_container_log_config) = merged_config.zkfc_logging() {
        pb.add_volume(
            VolumeBuilder::new(ContainerConfig::ZKFC_CONFIG_VOLUME_MOUNT_NAME)
                .config_map(ConfigMapVolumeSource {
                    name: Some(rolegroup_ref.object_name()),
                    ..ConfigMapVolumeSource::default()
                })
                .build(),
        );

        if let ContainerLogConfig {
            choice:
                Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                    custom: ConfigMapLogConfig { config_map },
                })),
        } = zkfc_container_log_config
        {
            pb.add_volume(
                VolumeBuilder::new(ContainerConfig::ZKFC_LOG_VOLUME_MOUNT_NAME)
                    .config_map(ConfigMapVolumeSource {
                        name: Some(config_map),
                        ..ConfigMapVolumeSource::default()
                    })
                    .build(),
            );
        } else {
            pb.add_volume(
                VolumeBuilder::new(ContainerConfig::ZKFC_LOG_VOLUME_MOUNT_NAME)
                    .config_map(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..ConfigMapVolumeSource::default()
                    })
                    .build(),
            );
        }
    }

    if merged_config.vector_logging_enabled() {
        pb.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            ContainerConfig::HDFS_CONFIG_VOLUME_MOUNT_NAME,
            ContainerConfig::STACKABLE_LOG_VOLUME_MOUNT_NAME,
            Some(&merged_config.vector_logging()),
        ));
    }

    let replicas;
    let zk_config_map_name = &hdfs.spec.zookeeper_config_map_name;

    // HDFS main container
    pb.add_container(
        ContainerConfig::from(role.clone())
            .main_container(
                resolved_product_image,
                zk_config_map_name,
                env_overrides,
                merged_config,
            )
            .context(FailedToCreateContainerSnafu)?,
    );
    // role specific pod settings configured here
    match role {
        HdfsRole::NameNode => {
            let rg = hdfs.namenode_rolegroup(&rolegroup_ref.role_group);
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
            // Format namenode init container
            pb.add_init_container(
                ContainerConfig::try_from(ContainerConfig::FORMAT_NAMENODES_CONTAINER_NAME)
                    .context(FailedToCreateContainerConfigSnafu)?
                    .init_container(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        namenode_podrefs,
                        merged_config,
                    )
                    .context(FailedToCreateContainerSnafu)?,
            );
            // Format ZooKeeper init container
            pb.add_init_container(
                ContainerConfig::try_from(ContainerConfig::FORMAT_ZOOKEEPER_CONTAINER_NAME)
                    .context(FailedToCreateContainerConfigSnafu)?
                    .init_container(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        namenode_podrefs,
                        merged_config,
                    )
                    .context(FailedToCreateContainerSnafu)?,
            );
            // Zookeeper fail over container
            pb.add_container(
                ContainerConfig::try_from(ContainerConfig::ZKFC_CONTAINER_NAME)
                    .context(FailedToCreateContainerConfigSnafu)?
                    .main_container(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        merged_config,
                    )
                    .context(FailedToCreateContainerSnafu)?,
            );
        }
        HdfsRole::DataNode => {
            let rg = hdfs.datanode_rolegroup(&rolegroup_ref.role_group);
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            // Wait for namenode init container
            pb.add_init_container(
                ContainerConfig::try_from(ContainerConfig::WAIT_FOR_NAMENODES_CONTAINER_NAME)
                    .context(FailedToCreateContainerConfigSnafu)?
                    .init_container(
                        resolved_product_image,
                        zk_config_map_name,
                        env_overrides,
                        namenode_podrefs,
                        merged_config,
                    )
                    .context(FailedToCreateContainerSnafu)?,
            );
        }
        HdfsRole::JournalNode => {
            let rg = hdfs.journalnode_rolegroup(&rolegroup_ref.role_group);
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
        }
    }

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
            replicas: Some(i32::from(replicas)),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    hdfs,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name,
            template: pb.build_template(),
            volume_claim_templates: Some(vec![merged_config.resources().storage.data.build_pvc(
                ContainerConfig::DATA_VOLUME_MOUNT_NAME,
                Some(vec!["ReadWriteOnce"]),
            )]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_obj: Arc<HdfsCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
