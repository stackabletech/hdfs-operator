use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use product_config::{
    types::PropertyNameKind,
    writer::{to_hadoop_xml, to_java_properties_string, PropertiesWriterError},
    ProductConfigManager,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{security::PodSecurityContextBuilder, PodBuilder},
    },
    client::Client,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::ResolvedProductImage,
        rbac::{build_rbac_resources, service_account_name},
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, Service, ServicePort, ServiceSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        api::ObjectMeta,
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, LabelError, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use stackable_hdfs_crd::{
    constants::*, AnyNodeConfig, HdfsCluster, HdfsClusterStatus, HdfsPodRef, HdfsRole,
};

use crate::{
    build_recommended_labels,
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    container::{self, ContainerConfig, TLS_STORE_DIR, TLS_STORE_PASSWORD},
    discovery::{self, build_discovery_configmap},
    event::{build_invalid_replica_message, publish_event},
    operations::{
        graceful_shutdown::{self, add_graceful_shutdown_config},
        pdb::add_pdbs,
    },
    product_logging::{extend_role_group_config_map, resolve_vector_aggregator_address},
    security::{self, kerberos, opa::HdfsOpaConfig},
    OPERATOR_NAME,
};

pub const RESOURCE_MANAGER_HDFS_CONTROLLER: &str = "hdfs-operator-hdfs-controller";
const HDFS_CONTROLLER: &str = "hdfs-controller";
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
        obj_ref: ObjectRef<HdfsCluster>,
    },

    #[snafu(display("invalid role {role:?}"))]
    InvalidRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("object has no name"))]
    ObjectHasNoName { obj_ref: ObjectRef<HdfsCluster> },

    #[snafu(display("cannot build config map for role {role:?} and role group {role_group:?}"))]
    BuildRoleGroupConfigMap {
        source: stackable_operator::builder::configmap::Error,
        role: String,
        role_group: String,
    },

    #[snafu(display("cannot collect discovery configuration"))]
    CollectDiscoveryConfig { source: stackable_hdfs_crd::Error },

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
    CreatePodReferences { source: stackable_hdfs_crd::Error },

    #[snafu(display("failed to build role properties"))]
    BuildRoleProperties { source: stackable_hdfs_crd::Error },

    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name:?}"))]
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
    RoleGroupSelectorLabels { source: stackable_hdfs_crd::Error },

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

    let resolved_product_image = hdfs
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

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

    // The service account and rolebinding will be created per cluster
    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hdfs.as_ref(),
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildClusterResourcesLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let hdfs_opa_config = match &hdfs.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HdfsOpaConfig::from_opa_config(client, &hdfs, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    let dfs_replication = hdfs.spec.cluster_config.dfs_replication;
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

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

            // We need to split the creation and the usage of the "metadata" variable in two statements.
            // to avoid the compiler error "E0716 (temporary value dropped while borrowed)".
            let mut metadata = ObjectMetaBuilder::new();
            let metadata = metadata
                .name_and_namespace(hdfs.as_ref())
                .name(&rolegroup_ref.object_name())
                .ownerreference_from_resource(hdfs.as_ref(), None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    obj_ref: ObjectRef::from_obj(hdfs.as_ref()),
                })?
                .with_recommended_labels(build_recommended_labels(
                    hdfs.as_ref(),
                    RESOURCE_MANAGER_HDFS_CONTROLLER,
                    &resolved_product_image.app_version_label,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                ))
                .context(ObjectMetaSnafu)?;

            let rg_service = rolegroup_service(&hdfs, metadata, &role, &rolegroup_ref)?;

            let rg_configmap = rolegroup_config_map(
                &hdfs,
                metadata,
                &rolegroup_ref,
                rolegroup_config,
                &namenode_podrefs,
                &journalnode_podrefs,
                &merged_config,
                &hdfs_opa_config,
                vector_aggregator_address.as_deref(),
            )?;

            let rg_statefulset = rolegroup_statefulset(
                &hdfs,
                metadata,
                &role,
                &rolegroup_ref,
                &resolved_product_image,
                env_overrides,
                &merged_config,
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
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset.clone())
                    .await
                    .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                        name: rg_statefulset_name,
                    })?,
            );
        }

        let role_config = hdfs.role_config(&role);
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = role_config
        {
            add_pdbs(pdb, &hdfs, &role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm = build_discovery_configmap(
        &hdfs,
        HDFS_CONTROLLER,
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
        conditions: compute_conditions(
            hdfs.as_ref(),
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, &*hdfs, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

fn rolegroup_service(
    hdfs: &HdfsCluster,
    metadata: &ObjectMetaBuilder,
    role: &HdfsRole,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
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
    hdfs: &HdfsCluster,
    metadata: &ObjectMetaBuilder,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
    merged_config: &AnyNodeConfig,
    hdfs_opa_config: &Option<HdfsOpaConfig>,
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
                    .dfs_namenode_shared_edits_dir(journalnode_podrefs)
                    .dfs_namenode_name_dir_ha(namenode_podrefs)
                    .dfs_namenode_rpc_address_ha(namenode_podrefs)
                    .dfs_namenode_http_address_ha(hdfs, namenode_podrefs)
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
                    .add("dfs.datanode.registered.ipc.port", "${env.IPC_PORT}");
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
                    .security_config(hdfs)
                    .context(BuildSecurityConfigSnafu)?;
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
    metadata: &ObjectMetaBuilder,
    role: &HdfsRole,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    resolved_product_image: &ResolvedProductImage,
    env_overrides: Option<&BTreeMap<String, String>>,
    merged_config: &AnyNodeConfig,
    namenode_podrefs: &[HdfsPodRef],
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
        .service_account_name(service_account_name(APP_NAME))
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(HDFS_UID)
                .run_as_group(0)
                .fs_group(1000)
                .build(),
        );

    // Adds all containers and volumes to the pod builder
    // We must use the selector labels ("rolegroup_selector_labels") and not the recommended labels
    // for the ephemeral listener volumes created by this function.
    // This is because the recommended set contains a "managed-by" label. This labels triggers
    // the cluster resources to "manage" listeners which is wrong and leads to errors.
    // The listeners are managed by the listener-operator.
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        hdfs,
        role,
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
        service_name: object_name,
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

pub fn error_policy(_obj: Arc<HdfsCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}

#[cfg(test)]
mod test {
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
    roleGroups:
      default:
        envOverrides:
          MY_ENV: my-value
          HADOOP_HOME: /not/the/default/path
        replicas: 1
";
        let product_config = "
---
version: 0.1.0
spec:
  units: []
properties: []
";

        let hdfs: HdfsCluster = serde_yaml::from_str(cr).unwrap();

        let config =
            transform_all_roles_to_config(&hdfs, hdfs.build_role_properties().unwrap()).unwrap();

        let validated_config = validate_all_roles_and_groups_config(
            "3.4.0",
            &config,
            &ProductConfigManager::from_str(product_config).unwrap(),
            false,
            false,
        )
        .unwrap();

        let role = HdfsRole::DataNode;
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
            &role,
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
        let main_container = containers
            .iter()
            .find(|c| c.name == role.to_string())
            .unwrap();

        assert_eq!(
            main_container
                .env
                .clone()
                .unwrap()
                .into_iter()
                .find(|e| e.name == "MY_ENV")
                .unwrap()
                .value,
            Some("my-value".to_string())
        );

        assert_eq!(
            main_container
                .env
                .clone()
                .unwrap()
                .into_iter()
                .find(|e| e.name == "HADOOP_HOME")
                .unwrap()
                .value,
            Some("/not/the/default/path".to_string())
        );
    }
}
