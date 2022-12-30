use crate::config::{
    CoreSiteConfigBuilder, HdfsNodeDataDirectory, HdfsSiteConfigBuilder, ROOT_DATA_DIR,
};
use crate::discovery::build_discovery_configmap;
use crate::product_logging::{
    extend_role_group_config_map, resolve_vector_aggregator_address, LOG4J_CONFIG_FILE,
};
use crate::{build_recommended_labels, rbac, OPERATOR_NAME};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::{constants::*, HdfsCluster, HdfsPodRef, HdfsRole, MergedConfig};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder,
    },
    client::Client,
    cluster_resources::ClusterResources,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, Container, ContainerPort,
                EmptyDirVolumeSource, EnvVar, EnvVarSource, ObjectFieldSelector, Probe,
                ResourceRequirements, Service, ServicePort, ServiceSpec, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{
        api::ObjectMeta,
        runtime::{
            controller::Action,
            events::{Event, EventType, Recorder, Reporter},
            reflector::ObjectRef,
        },
        Resource, ResourceExt,
    },
    labels::role_group_selector_labels,
    logging::controller::ReconcilerError,
    memory::to_java_heap,
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

pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const MAX_HDFS_LOG_FILES_SIZE_IN_MIB: u32 = 10;

const OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB: u32 = 1;
const LOG_VOLUME_SIZE_IN_MIB: u32 =
    MAX_HDFS_LOG_FILES_SIZE_IN_MIB + OVERFLOW_BUFFER_ON_LOG_VOLUME_IN_MIB;
const HDFS_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

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
    #[snafu(display("Object has no namespace [{obj_ref}]"))]
    ObjectHasNoNamespace { obj_ref: ObjectRef<HdfsCluster> },
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
    #[snafu(display("Object has no associated namespace"))]
    NoNamespace,
    #[snafu(display("Failed to publish event"))]
    PublishEvent {
        source: stackable_operator::kube::Error,
    },
    #[snafu(display("Invalid java heap config for [{role}]"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
        role: String,
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

    let dfs_replication = hdfs.spec.dfs_replication;

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

    for (role_name, group_config) in validated_config.iter() {
        let role: HdfsRole = HdfsRole::from_str(role_name).with_context(|_| InvalidRoleSnafu {
            role: role_name.to_string(),
        })?;

        let role_ports = role.ports();

        if let Some(content) = build_invalid_replica_message(&hdfs, &role, dfs_replication) {
            publish_event(
                &hdfs,
                client,
                "Reconcile",
                "Invalid replicas",
                content.as_ref(),
            )
            .await?;
        }

        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let merged_config = role
                .merged_config(&hdfs, rolegroup_name)
                .context(ConfigMergeSnafu)?;

            let hadoop_container = hdfs_common_container(
                &hdfs,
                role_ports.as_slice(),
                rolegroup_config.get(&PropertyNameKind::Env),
                &resolved_product_image,
            )?;

            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rg_service = rolegroup_service(
                &hdfs,
                &rolegroup_ref,
                role_ports.as_slice(),
                &resolved_product_image,
            )?;
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
                &namenode_podrefs,
                &hadoop_container,
                &rbac_sa.name_any(),
                &resolved_product_image,
                merged_config.as_ref(),
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

pub fn error_policy(_obj: Arc<HdfsCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn rolegroup_service(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_ports: &[(String, i32)],
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
                rolegroup_ports
                    .iter()
                    .map(|(name, value)| ServicePort {
                        name: Some(name.clone()),
                        port: *value,
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
        &merged_config.logging(),
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
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
    rbac_sa: &str,
    resolved_product_image: &ResolvedProductImage,
    merged_config: &(dyn MergedConfig + Send + 'static),
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
    .add_volume(Volume {
        name: "hdfs-config".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(rolegroup_ref.object_name()),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    })
    .add_volume(Volume {
        name: "log".to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
        }),
        ..Volume::default()
    })
    .service_account_name(rbac_sa)
    .security_context(
        PodSecurityContextBuilder::new()
            .run_as_user(1000)
            .run_as_group(1000)
            .fs_group(1000)
            .build(),
    );

    let logging = merged_config.logging();

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = logging.containers.get(&stackable_hdfs_crd::Container::Hdfs)
    {
        pb.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        pb.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    if logging.enable_vector_agent {
        pb.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            "hdfs-config",
            "log",
            merged_config
                .logging()
                .containers
                .get(&stackable_hdfs_crd::Container::Vector),
        ));
    }

    let replicas;

    // role specific pod settings configured here
    match role {
        HdfsRole::DataNode => {
            let rg = hdfs.datanode_rolegroup(rolegroup_ref);
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            for c in datanode_init_containers(namenode_podrefs, hadoop_container)
                .into_iter()
                .flatten()
            {
                pb.add_init_container(c);
            }
            for c in datanode_containers(
                rolegroup_ref,
                hadoop_container,
                &merged_config.resources().into(),
            )? {
                pb.add_container(c);
            }
        }
        HdfsRole::NameNode => {
            let rg = hdfs.namenode_rolegroup(rolegroup_ref);
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
            if let Some(init_containers) =
                namenode_init_containers(namenode_podrefs, hadoop_container)
            {
                for c in init_containers {
                    pb.add_init_container(c);
                }
            }
            for c in namenode_containers(
                rolegroup_ref,
                hadoop_container,
                &merged_config.resources().into(),
            )? {
                pb.add_container(c);
            }
        }
        HdfsRole::JournalNode => {
            let rg = hdfs.journalnode_rolegroup(rolegroup_ref);
            pb.node_selector_opt(rg.and_then(|rg| rg.selector.clone()));
            replicas = rg.and_then(|rg| rg.replicas).unwrap_or_default();
            for c in journalnode_containers(
                rolegroup_ref,
                hadoop_container,
                &merged_config.resources().into(),
            )? {
                pb.add_container(c);
            }
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
            volume_claim_templates: Some(vec![merged_config
                .resources()
                .storage
                .data
                .build_pvc("data", Some(vec!["ReadWriteOnce"]))]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn journalnode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
    resources: &ResourceRequirements,
) -> HdfsOperatorResult<Vec<Container>> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();

    let heap_opts = resources
        .limits
        .as_ref()
        .and_then(|l| l.get("memory"))
        .map(|m| to_java_heap(m, JVM_HEAP_FACTOR))
        .unwrap_or_else(|| Ok("".to_string()))
        .with_context(|_| InvalidJavaHeapConfigSnafu {
            role: HdfsRole::JournalNode.to_string(),
        })?;

    let opts = vec![
        Some(
            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
            DEFAULT_JOURNAL_NODE_METRICS_PORT,
            rolegroup_ref.role,)
        ),
        Some(heap_opts),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(" ")
    .trim()
    .to_string();

    env.push(EnvVar {
        name: "HDFS_JOURNALNODE_OPTS".to_string(),
        value: Some(opts),
        ..EnvVar::default()
    });

    Ok(vec![Container {
        command: Some(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ]),
        args: Some(vec![[
            format!("mkdir -p {}", CONFIG_DIR_NAME),
            format!("cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME}"),
            format!("cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}"),
            format!(
                "{hadoop_home}/bin/hdfs --debug journalnode",
                hadoop_home = HADOOP_HOME
            ),
        ]
        .join(" && ")]),
        readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
        liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
        env: Some(env),
        resources: Some(resources.clone()),
        ..hadoop_container.clone()
    }])
}

fn namenode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
    resources: &ResourceRequirements,
) -> HdfsOperatorResult<Vec<Container>> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();

    let heap_opts = resources
        .limits
        .as_ref()
        .and_then(|l| l.get("memory"))
        .map(|m| to_java_heap(m, JVM_HEAP_FACTOR))
        .unwrap_or_else(|| Ok("".to_string()))
        .with_context(|_| InvalidJavaHeapConfigSnafu {
            role: HdfsRole::NameNode.to_string(),
        })?;

    let opts = vec![
        Some(
            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                DEFAULT_NAME_NODE_METRICS_PORT,
                rolegroup_ref.role,)
        ),
        Some(heap_opts),
        ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(" ")
    .trim()
    .to_string();

    env.push(EnvVar {
        name: "HDFS_NAMENODE_OPTS".to_string(),
        value: Some(opts),
        ..EnvVar::default()
    });

    Ok(vec![
        Container {
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![[
                // We need to collect the tmp config dir and vector configs
                format!("mkdir -p {}", CONFIG_DIR_NAME),
                format!("cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME}"),
                format!("cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}"),
                format!(
                    "{hadoop_home}/bin/hdfs --debug namenode",
                    hadoop_home = HADOOP_HOME
                ),
            ]
            .join(" && ")]),
            env: Some(env),
            readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
            liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
            resources: Some(resources.clone()),
            ..hadoop_container.clone()
        },
        // Note that we don't add the HADOOP_OPTS / HDFS_NAMENODE_OPTS env var to this container (zkfc)
        // Here it would cause an "address already in use" error and prevent the namenode container from starting.
        // Because the jmx exporter is not enabled here, also the readiness probes are not enabled.
        Container {
            name: String::from("zkfc"),
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![[
                // We need to collect the tmp config dir and vector configs
                format!("mkdir -p {CONFIG_DIR_NAME}"),
                format!("cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME}"),
                format!("cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}"),
                format!("{HADOOP_HOME}/bin/hdfs zkfc"),
            ]
            .join(" && ")]),
            ..hadoop_container.clone()
        },
    ])
}

fn datanode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
    resources: &ResourceRequirements,
) -> HdfsOperatorResult<Vec<Container>> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();

    let heap_opts = resources
        .limits
        .as_ref()
        .and_then(|l| l.get("memory"))
        .map(|m| to_java_heap(m, JVM_HEAP_FACTOR))
        .unwrap_or_else(|| Ok("".to_string()))
        .with_context(|_| InvalidJavaHeapConfigSnafu {
            role: HdfsRole::DataNode.to_string(),
        })?;

    let opts = vec![
        Some(
            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                DEFAULT_DATA_NODE_METRICS_PORT,
                rolegroup_ref.role,)),
        Some(heap_opts),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<String>>()
    .join(" ");

    env.push(EnvVar {
        name: "HDFS_DATANODE_OPTS".to_string(),
        value: Some(opts),
        ..EnvVar::default()
    });

    Ok(vec![Container {
        command: Some(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ]),
        args: Some(vec![[
            // We need to collect the tmp config dir and vector configs
            format!("mkdir -p {}", CONFIG_DIR_NAME),
            format!("cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME}"),
            format!("cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}"),
            format!(
                "{hadoop_home}/bin/hdfs --debug datanode",
                hadoop_home = HADOOP_HOME
            ),
        ]
        .join(" && ")]),
        env: Some(env),
        readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_IPC, 10, 10)),
        liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_IPC, 10, 10)),
        resources: Some(resources.clone()),
        ..hadoop_container.clone()
    }])
}

fn datanode_init_containers(
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![Container {
        name: "wait-for-namenodes".to_string(),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "
                echo \"Waiting for namenodes to get ready:\"
                n=0
                while [ ${{n}} -lt 12 ];
                do
                  ALL_NODES_READY=true
                  for id in {pod_names}
                  do
                    echo -n \"Checking pod $id... \"
                    SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                    if [ \"$SERVICE_STATE\" = \"active\" ] || [ \"$SERVICE_STATE\" = \"standby\" ]
                    then
                      echo \"$SERVICE_STATE\"
                    else
                      echo \"not ready\"
                      ALL_NODES_READY=false
                    fi
                  done
                  if [ \"$ALL_NODES_READY\" == \"true\" ]
                  then
                    echo \"All namenodes ready!\"
                    break
                  fi
                  echo \"\"
                  n=$(( n  + 1))
                  sleep 5
                done
            ",
                hadoop_home = HADOOP_HOME,
                pod_names = namenode_podrefs
                    .iter()
                    .map(|pod_ref| pod_ref.pod_name.as_ref())
                    .collect::<Vec<&str>>()
                    .join(" ")
            ),
        ]),
        ..hadoop_container.clone()
    }])
}

fn namenode_init_containers(
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![
    Container {
        name: "format-namenode".to_string(),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            // First step we check for active namenodes. This step should return an active namenode
            // for e.g. scaling. It may fail if the active namenode is restarted and the standby
            // namenode takes over.
            // This is why in the second part we check if the node is formatted already via
            // $NAMENODE_DIR/current/VERSION. Then we dont do anything.
            // If there is no active namenode, the current pod is not formatted we format as
            // active namenode. Otherwise as standby node.
            format!("
                 mkdir -p {CONFIG_DIR_NAME}
                 cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME}
                 cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}
                 echo \"Start formatting namenode $POD_NAME. Checking for active namenodes:\"
                 for id in {pod_names}
                 do
                   echo -n \"Checking pod $id... \"
                   SERVICE_STATE=$({hadoop_home}/bin/hdfs haadmin -getServiceState $id 2>/dev/null)
                   if [ \"$SERVICE_STATE\" == \"active\" ]
                   then
                     ACTIVE_NAMENODE=$id
                     echo \"active\"
                     break
                   fi
                   echo \"\"
                 done

                 set -e
                 if [ ! -f \"{namenode_dir}/current/VERSION\" ]
                 then
                   if [ -z ${{ACTIVE_NAMENODE+x}} ]
                   then
                     echo \"Create pod $POD_NAME as active namenode.\"
                     {hadoop_home}/bin/hdfs namenode -format -noninteractive
                   else
                     echo \"Create pod $POD_NAME as standby namenode.\"
                     {hadoop_home}/bin/hdfs namenode -bootstrapStandby -nonInteractive
                   fi
                 else
                   echo \"Pod $POD_NAME already formatted. Skipping...\"
                 fi",
                hadoop_home = HADOOP_HOME,
                pod_names = namenode_podrefs.iter().map(|pod_ref| pod_ref.pod_name.as_ref()).collect::<Vec<&str>>().join(" "),
                // TODO: What if overridden? We should not default here then!
                namenode_dir = HdfsNodeDataDirectory::default().namenode,
            ),
        ]),
        ..hadoop_container.clone()
    },
    Container {
        name: "format-zk".to_string(),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("
                mkdir -p {CONFIG_DIR_NAME} && cp {TMP_CONFIG_DIR_NAME}/* {CONFIG_DIR_NAME} && cp {HDFS_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME} &&
                test \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && {hadoop_home}/bin/hdfs zkfc -formatZK -nonInteractive || true", hadoop_home = HADOOP_HOME
            )
        ]),
        ..hadoop_container.clone()
    },
    ])
}

/// Creates a probe for [`stackable_operator::k8s_openapi::api::core::v1::TCPSocketAction`]
/// for liveness or readiness probes
fn tcp_socket_action_probe(
    port_name: &str,
    period_seconds: i32,
    initial_delay_seconds: i32,
) -> Probe {
    Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::String(String::from(port_name)),
            ..TCPSocketAction::default()
        }),
        period_seconds: Some(period_seconds),
        initial_delay_seconds: Some(initial_delay_seconds),
        ..Probe::default()
    }
}

/// Build a Container with common HDFS environment variables, ports and volume mounts set.
fn hdfs_common_container(
    hdfs: &HdfsCluster,
    rolegroup_ports: &[(String, i32)],
    env_overrides: Option<&BTreeMap<String, String>>,
    resolved_product_image: &ResolvedProductImage,
) -> HdfsOperatorResult<Container> {
    let mut env: Vec<EnvVar> = env_overrides
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect();

    env.extend(vec![
        EnvVar {
            name: "HADOOP_HOME".to_string(),
            value: Some(String::from(HADOOP_HOME)),
            ..EnvVar::default()
        },
        EnvVar {
            name: "HADOOP_CONF_DIR".to_string(),
            value: Some(String::from(CONFIG_DIR_NAME)),
            ..EnvVar::default()
        },
        EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: String::from("metadata.name"),
                    ..ObjectFieldSelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
        EnvVar {
            name: "ZOOKEEPER".to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: Some(hdfs.spec.zookeeper_config_map_name.clone()),
                    key: "ZOOKEEPER".to_string(),
                    ..ConfigMapKeySelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
    ]);

    Ok(ContainerBuilder::new(HDFS_CONTAINER_NAME)
        .expect("ContainerBuilder not created")
        .image_from_product_image(resolved_product_image)
        .add_env_vars(env)
        .add_volume_mount("data", ROOT_DATA_DIR)
        .add_volume_mount("hdfs-config", TMP_CONFIG_DIR_NAME)
        .add_volume_mount("log-config", HDFS_LOG_CONFIG_TMP_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .add_container_ports(
            rolegroup_ports
                .iter()
                .map(|(name, value)| ContainerPort {
                    name: Some(name.clone()),
                    container_port: *value,
                    protocol: Some("TCP".to_string()),
                    ..ContainerPort::default()
                })
                .collect(),
        )
        .build())
}

/// Publish a Kubernetes event for the `hdfs` cluster resource.
async fn publish_event(
    hdfs: &HdfsCluster,
    client: &Client,
    action: &str,
    reason: &str,
    message: &str,
) -> HdfsOperatorResult<()> {
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

fn build_invalid_replica_message(
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
