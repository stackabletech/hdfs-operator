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
    cluster_resources::ClusterResources,
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

pub(crate) const KEYSTORE_DIR_NAME: &str = "/stackable/keystore";

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
    #[snafu(display("Object has no namespace"))]
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
                &role,
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
    role: &HdfsRole,
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
    let hdfs_namespace = hdfs
        .namespace()
        .with_context(|| ObjectHasNoNamespaceSnafu {
            obj_ref: ObjectRef::from_obj(hdfs),
        })?;

    let mut hdfs_site_xml = String::new();
    let mut core_site_xml = String::new();
    let mut ssl_server_xml = String::new();
    let mut ssl_client_xml = String::new();

    for (property_name_kind, config) in rolegroup_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HDFS_SITE_XML => {
                let mut hdfs_site_xml_builder = HdfsSiteConfigBuilder::new(hdfs_name.to_string());
                // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
                // be formatted by the namenode, or used by the other services.
                // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
                hdfs_site_xml_builder
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
                    .dfs_namenode_http_address_ha(hdfs, namenode_podrefs)
                    .dfs_client_failover_proxy_provider()
                    .add("dfs.ha.fencing.methods", "shell(/bin/true)")
                    .add("dfs.ha.nn.not-become-active-in-safemode", "true")
                    .add("dfs.ha.automatic-failover.enabled", "true")
                    .add("dfs.ha.namenode.id", "${env.POD_NAME}");

                if hdfs.has_security_enabled() {
                    hdfs_site_xml_builder
                        .add("dfs.block.access.token.enable", "true")
                        .add("dfs.data.transfer.protection", "authentication")
                        .add("dfs.http.policy", "HTTPS_ONLY")
                        .add("dfs.https.server.keystore.resource", SSL_SERVER_XML)
                        .add("dfs.https.client.keystore.resource", SSL_CLIENT_XML);
                }

                hdfs_site_xml = hdfs_site_xml_builder
                    // the extend with config must come last in order to have overrides working!!!
                    .extend(config)
                    .build_as_xml();
            }
            PropertyNameKind::File(file_name) if file_name == CORE_SITE_XML => {
                let mut core_site_xml_builder = CoreSiteConfigBuilder::new(hdfs_name.to_string());

                core_site_xml_builder.fs_default_fs().ha_zookeeper_quorum();

                if hdfs.has_security_enabled() {
                    // .add("hadoop.security.authentication", "kerberos")
                    // .add("hadoop.security.authorization","true")
                    // .add("hadoop.registry.kerberos.realm","${env.KERBEROS_REALM}")
                    // .add("dfs.web.authentication.kerberos.principal","HTTP/_HOST@${env.KERBEROS_REALM}")
                    // .add("dfs.journalnode.kerberos.internal.spnego.principal","HTTP/_HOST@{env.KERBEROS_REALM}")
                    // .add("dfs.journalnode.kerberos.principal","jn/_HOST@${env.KERBEROS_REALM}")
                    // .add("dfs.journalnode.kerberos.principal.pattern","jn/*.simple-hdfs-journalnode-default.default.svc.cluster.local@${env.KERBEROS_REALM}")
                    // .add("dfs.namenode.kerberos.principal","nn/simple-hdfs-namenode-default.default.svc.cluster.local@${env.KERBEROS_REALM}")
                    // .add("dfs.namenode.kerberos.principal.pattern","nn/simple-hdfs-namenode-default.default.svc.cluster.local@${env.KERBEROS_REALM}")
                    // .add("dfs.datanode.kerberos.principal","dn/_HOST@${env.KERBEROS_REALM}")
                    // .add("dfs.web.authentication.keytab.file","/stackable/kerberos/keytab")
                    // .add("dfs.journalnode.keytab.file","/stackable/kerberos/keytab")
                    // .add("dfs.namenode.keytab.file","/stackable/kerberos/keytab")
                    // .add("dfs.datanode.keytab.file","/stackable/kerberos/keytab")
                    // .add("hadoop.user.group.static.mapping.overrides","dr.who=;nn=;")

                    core_site_xml_builder
                        .add("hadoop.security.authentication", "kerberos")
                        .add("hadoop.security.authorization", "true")
                        // Otherwise we fail with `java.io.IOException: No groups found for user nn`
                        // Default value is `dr.who=`, so we include that here
                        .add("hadoop.user.group.static.mapping.overrides", "dr.who=;nn=;nm=;jn=;")
                        .add("hadoop.registry.kerberos.realm", "${env.KERBEROS_REALM}")
                        .add(
                            "dfs.web.authentication.kerberos.principal",
                            "HTTP/_HOST@${env.KERBEROS_REALM}",
                        )
                        .add(
                            "dfs.web.authentication.keytab.file",
                            "/stackable/kerberos/keytab",
                        )
                        .add(
                            "dfs.journalnode.kerberos.principal.pattern",
                            // jn/hdfs-test-journalnode-default-0.hdfs-test-journalnode-default.test.svc.cluster.local@CLUSTER.LOCAL
                            format!("jn/{hdfs_name}-journalnode-*.{hdfs_name}-journalnode-*.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}").as_str(),
                        )
                        .add(
                            "dfs.namenode.kerberos.principal.pattern",
                            format!("nn/{hdfs_name}-namenode-*.{hdfs_name}-namenode-*.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}").as_str(),
                        );

                    match role {
                        HdfsRole::NameNode => {
                            core_site_xml_builder
                                .add(
                                    "dfs.namenode.kerberos.principal",
                                    "nn/_HOST@${env.KERBEROS_REALM}",
                                )
                                .add("dfs.namenode.keytab.file", "/stackable/kerberos/keytab");
                        }
                        HdfsRole::DataNode => {
                            core_site_xml_builder
                                .add(
                                    "dfs.datanode.kerberos.principal",
                                    "dn/_HOST@${env.KERBEROS_REALM}",
                                )
                                .add("dfs.datanode.keytab.file", "/stackable/kerberos/keytab");
                        }
                        HdfsRole::JournalNode => {
                            core_site_xml_builder
                                .add(
                                    "dfs.journalnode.kerberos.principal",
                                    "jn/_HOST@${env.KERBEROS_REALM}",
                                )
                                .add("dfs.journalnode.keytab.file", "/stackable/kerberos/keytab")
                                .add(
                                    "dfs.journalnode.kerberos.internal.spnego.principal",
                                    "HTTP/_HOST@${env.KERBEROS_REALM}",
                                );
                        }
                    }
                }

                // the extend with config must come last in order to have overrides working!!!
                core_site_xml = core_site_xml_builder.extend(config).build_as_xml();
            }
            // PropertyNameKind::File(file_name) if file_name == HADOOP_POLICY_XML => {
            //     let mut config_opts = BTreeMap::new();
            //     // When a NN connects to a JN, due to some reverse-dns roulette we have a (pretty low) chance of running into the follow error
            //     // (found in the logs of hdfs-journalnode-default-0 container journalnode):
            //     //
            //     // WARN  authorize.ServiceAuthorizationManager (ServiceAuthorizationManager.java:authorize(122)) - Authorization failed for jn/hdfs-journalnode-default-2.hdfs-journalnode-default.kuttl-test-expert-killdeer.svc.cluster.local@CLUSTER.LOCAL (auth:KERBEROS) for protocol=interface org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocol: this service is only accessible by jn/10-244-0-178.hdfs-journalnode-default-2.kuttl-test-expert-killdeer.svc.cluster.local@CLUSTER.LOCAL
            //     // Note: 10.244.0.178 belongs to hdfs-journalnode-default-2 in this case
            //     // So everything is right, but the JN does seem to make a reverse lookup and gets multiple dns names and get's misguided here
            //     //
            //     // An similar error that ocurred as well is
            //     //
            //     // User nn/hdfs-test-namenode-default-0.hdfs-test-namenode-default.test.svc.cluster.local@CLUSTER.LOCAL (auth:KERBEROS) is not authorized for protocol interface org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol: this service is only accessible by nn/10-244-0-65.hdfs-test-namenode-default-0.test.svc.cluster.local@CLUSTER.LOCAL
            //     config_opts
            //         .extend([("security.qjournal.service.protocol.acl".to_string(), Some())]);
            //     config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
            //     ssl_server_xml =
            //         stackable_operator::product_config::writer::to_hadoop_xml(config_opts.iter());
            // }
            PropertyNameKind::File(file_name) if file_name == SSL_SERVER_XML => {
                let mut config_opts = BTreeMap::new();
                config_opts.extend([
                    (
                        "ssl.server.keystore.location".to_string(),
                        Some(format!("{KEYSTORE_DIR_NAME}/keystore.p12")),
                    ),
                    (
                        "ssl.server.keystore.password".to_string(),
                        Some("changeit".to_string()),
                    ),
                    (
                        "ssl.server.keystore.type".to_string(),
                        Some("pkcs12".to_string()),
                    ),
                ]);
                config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                ssl_server_xml =
                    stackable_operator::product_config::writer::to_hadoop_xml(config_opts.iter());
            }
            PropertyNameKind::File(file_name) if file_name == SSL_CLIENT_XML => {
                let mut config_opts = BTreeMap::new();
                config_opts.extend([
                    (
                        "ssl.client.truststore.location".to_string(),
                        Some(format!("{KEYSTORE_DIR_NAME}/truststore.p12")),
                    ),
                    (
                        "ssl.client.truststore.password".to_string(),
                        Some("changeit".to_string()),
                    ),
                    (
                        "ssl.client.truststore.type".to_string(),
                        Some("pkcs12".to_string()),
                    ),
                ]);
                config_opts.extend(config.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                ssl_client_xml =
                    stackable_operator::product_config::writer::to_hadoop_xml(config_opts.iter());
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
        .add_data(HDFS_SITE_XML.to_string(), hdfs_site_xml)
        .add_data(SSL_SERVER_XML, ssl_server_xml)
        .add_data(SSL_CLIENT_XML, ssl_client_xml);

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
        hdfs,
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
