use crate::config::{
    CoreSiteConfigBuilder, HdfsNodeDataDirectory, HdfsSiteConfigBuilder, ROOT_DATA_DIR,
};
use crate::discovery::build_discovery_configmap;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_hdfs_crd::{constants::*, ROLE_PORTS};
use stackable_hdfs_crd::{HdfsCluster, HdfsPodRef, HdfsRole};
use stackable_operator::builder::{ConfigMapBuilder, ObjectMetaBuilder};
use stackable_operator::client::Client;
use stackable_operator::k8s_openapi::api::core::v1::{
    Container, ContainerPort, ObjectFieldSelector, PodSpec, PodTemplateSpec, Probe,
    SecurityContext, TCPSocketAction, VolumeMount,
};
use stackable_operator::k8s_openapi::api::{
    apps::v1::{StatefulSet, StatefulSetSpec},
    core::v1::{
        ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, EnvVar, EnvVarSource,
        PersistentVolumeClaim, PersistentVolumeClaimSpec, ResourceRequirements, Service,
        ServicePort, ServiceSpec, Volume,
    },
};
use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use stackable_operator::k8s_openapi::apimachinery::pkg::{
    api::resource::Quantity, apis::meta::v1::LabelSelector,
};
use stackable_operator::kube::api::ObjectMeta;
use stackable_operator::kube::runtime::controller::{Action, Context};
use stackable_operator::kube::runtime::events::{Event, EventType, Recorder, Reporter};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::ResourceExt;
use stackable_operator::labels::role_group_selector_labels;
use stackable_operator::product_config::{types::PropertyNameKind, ProductConfigManager};
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
};
use stackable_operator::role_utils::RoleGroupRef;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

pub async fn reconcile_hdfs(
    hdfs: Arc<HdfsCluster>,
    ctx: Context<Ctx>,
) -> HdfsOperatorResult<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        hdfs.hdfs_version()?,
        &transform_all_roles_to_config(&*hdfs, hdfs.build_role_properties()?)
            .map_err(|source| Error::InvalidRoleConfig { source })?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|source| Error::InvalidProductConfig { source })?;

    // A list of all name and journal nodes across all role groups is needed for all ConfigMaps and initialization checks.
    let namenode_podrefs = hdfs.pod_refs(&HdfsRole::NameNode)?;
    let journalnode_podrefs = hdfs.pod_refs(&HdfsRole::JournalNode)?;

    let discovery_cm = build_discovery_configmap(&hdfs, &namenode_podrefs).map_err(|e| {
        Error::BuildDiscoveryConfigMap {
            source: e,
            name: hdfs.name(),
        }
    })?;

    client
        .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
        .await
        .map_err(|e| Error::ApplyDiscoveryConfigMap {
            source: e,
            name: discovery_cm.metadata.name.clone().unwrap_or_default(),
        })?;

    let dfs_replication = hdfs.spec.dfs_replication;

    for (role_name, group_config) in validated_config.iter() {
        let role: HdfsRole = serde_yaml::from_str(role_name).unwrap();
        let role_ports = ROLE_PORTS.get(&role).unwrap().as_slice();

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
            let hadoop_container = hdfs_common_container(
                &hdfs,
                role_ports,
                rolegroup_config.get(&PropertyNameKind::Env),
            )?;

            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rg_service = rolegroup_service(&hdfs, &rolegroup_ref, role_ports)?;
            let rg_configmap = rolegroup_config_map(
                &hdfs,
                &rolegroup_ref,
                rolegroup_config,
                &namenode_podrefs,
                &journalnode_podrefs,
            )?;
            let rg_statefulset = rolegroup_statefulset(
                &hdfs,
                &role,
                &rolegroup_ref,
                &namenode_podrefs,
                &hadoop_container,
            )?;

            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
                .await
                .map_err(|e| Error::ApplyRoleGroupService {
                    source: e,
                    name: rg_service.metadata.name.clone().unwrap_or_default(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
                .await
                .map_err(|e| Error::ApplyRoleGroupConfigMap {
                    source: e,
                    name: rg_configmap.metadata.name.clone().unwrap_or_default(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
                .await
                .map_err(|e| Error::ApplyRoleGroupStatefulSet {
                    source: e,
                    name: rg_statefulset.metadata.name.clone().unwrap_or_default(),
                })?;
        }
    }

    Ok(Action::await_change())
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn rolegroup_service(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_ports: &[(String, i32)],
) -> Result<Service, Error> {
    tracing::info!("Setting up Service for {:?}", rolegroup_ref);
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hdfs, None, Some(true))
            .map_err(|source| Error::ObjectMissingMetadataForOwnerRef {
                source,
                obj_ref: ObjectRef::from_obj(hdfs),
            })?
            .with_recommended_labels(
                hdfs,
                APP_NAME,
                hdfs.hdfs_version()?,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
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

fn rolegroup_config_map(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
) -> HdfsOperatorResult<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);

    let mut hdfs_site_xml = String::new();
    let mut core_site_xml = String::new();

    for (property_name_kind, config) in rolegroup_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HDFS_SITE_XML => {
                hdfs_site_xml =
                    HdfsSiteConfigBuilder::new(hdfs.name(), HdfsNodeDataDirectory::default())
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
                core_site_xml = CoreSiteConfigBuilder::new(hdfs.name())
                    .fs_default_fs()
                    .ha_zookeeper_quorum()
                    // the extend with config must come last in order to have overrides working!!!
                    .extend(config)
                    .build_as_xml();
            }
            _ => {}
        }
    }

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .name(&rolegroup_ref.object_name())
                .ownerreference_from_resource(hdfs, None, Some(true))
                .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                    source: e,
                    obj_ref: ObjectRef::from_obj(hdfs),
                })?
                .with_recommended_labels(
                    hdfs,
                    APP_NAME,
                    hdfs.hdfs_version()?,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .build(),
        )
        .add_data(CORE_SITE_XML.to_string(), core_site_xml)
        .add_data(HDFS_SITE_XML.to_string(), hdfs_site_xml)
        .add_data(
            LOG4J_PROPERTIES.to_string(),
            hdfs.spec.log4j.as_ref().unwrap_or(&"".to_string()),
        )
        .build()
        .map_err(|source| Error::BuildRoleGroupConfig {
            source,
            role: rolegroup_ref.role.clone(),
            role_group: rolegroup_ref.role_group.clone(),
        })
}

fn rolegroup_statefulset(
    hdfs: &HdfsCluster,
    role: &HdfsRole,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> HdfsOperatorResult<StatefulSet> {
    tracing::info!("Setting up StatefulSet for {:?}", rolegroup_ref);
    let service_name = rolegroup_ref.object_name();
    let hdfs_image = hdfs.hdfs_image()?;

    let replicas;
    let init_containers;
    let containers;

    match role {
        HdfsRole::DataNode => {
            replicas = hdfs.rolegroup_datanode_replicas(rolegroup_ref)?;
            init_containers =
                datanode_init_containers(&hdfs_image, namenode_podrefs, hadoop_container);
            containers = datanode_containers(rolegroup_ref, hadoop_container);
        }
        HdfsRole::NameNode => {
            replicas = hdfs.rolegroup_namenode_replicas(rolegroup_ref)?;
            init_containers =
                namenode_init_containers(&hdfs_image, namenode_podrefs, hadoop_container);
            containers = namenode_containers(rolegroup_ref, hadoop_container);
        }
        HdfsRole::JournalNode => {
            replicas = hdfs.rolegroup_journalnode_replicas(rolegroup_ref)?;
            init_containers = journalnode_init_containers(hadoop_container);
            containers = journalnode_containers(rolegroup_ref, hadoop_container);
        }
    }

    let template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(hdfs.rolegroup_selector_labels(rolegroup_ref)),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            containers,
            init_containers,
            volumes: Some(vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(rolegroup_ref.object_name()),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            ..PodSpec::default()
        }),
    };
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hdfs, None, Some(true))
            .map_err(|source| Error::ObjectMissingMetadataForOwnerRef {
                source,
                obj_ref: ObjectRef::from_obj(hdfs),
            })?
            .with_recommended_labels(
                hdfs,
                APP_NAME,
                hdfs.hdfs_version()?,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
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
            template,
            volume_claim_templates: Some(vec![local_disk_claim(
                "data",
                Quantity("1Gi".to_string()),
            )]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn journalnode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
) -> Vec<Container> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();

    env.push(EnvVar {
                name: "HADOOP_JOURNALNODE_OPTS".to_string(),
                value: Some(
                    format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                    DEFAULT_JOURNAL_NODE_METRICS_PORT,
                        rolegroup_ref.role,)
                ),
                ..EnvVar::default()
            },);

    vec![Container {
        name: rolegroup_ref.role.clone(),
        args: Some(vec![
            format!("{hadoop_home}/bin/hdfs", hadoop_home = HADOOP_HOME),
            "--debug".to_string(),
            "journalnode".to_string(),
        ]),
        readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
        liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
        env: Some(env),
        ..hadoop_container.clone()
    }]
}

fn namenode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
) -> Vec<Container> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();
    env.push(EnvVar {
                name: "HADOOP_NAMENODE_OPTS".to_string(),
                value: Some(
                    format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                    DEFAULT_NAME_NODE_METRICS_PORT,
                        rolegroup_ref.role,)
                ),
                ..EnvVar::default()
            },);

    vec![
        Container {
            name: rolegroup_ref.role.clone(),
            args: Some(vec![
                format!("{hadoop_home}/bin/hdfs", hadoop_home = HADOOP_HOME),
                "--debug".to_string(),
                "namenode".to_string(),
            ]),
            env: Some(env),
            readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
            liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_RPC, 10, 10)),
            ..hadoop_container.clone()
        },
        // Note that we don't add the HADOOP_OPTS / HADOOP_NAMENODE_OPTS env var to this container (zkfc)
        // Here it would cause an "address already in use" error and prevent the namenode container from starting.
        // Because the jmx exporter is not enabled here, also the readiness probes are not enabled.
        Container {
            name: String::from("zkfc"),
            args: Some(vec![
                format!("{hadoop_home}/bin/hdfs", hadoop_home = HADOOP_HOME),
                "zkfc".to_string(),
            ]),
            ..hadoop_container.clone()
        },
    ]
}

fn datanode_containers(
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    hadoop_container: &Container,
) -> Vec<Container> {
    let mut env: Vec<EnvVar> = hadoop_container.clone().env.unwrap();
    env.push(EnvVar {
                name: "HADOOP_DATANODE_OPTS".to_string(),
                value: Some(
                    format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                    DEFAULT_DATA_NODE_METRICS_PORT,
                        rolegroup_ref.role,)
                ),
                ..EnvVar::default()
            });

    vec![Container {
        name: rolegroup_ref.role.clone(),
        args: Some(vec![
            format!("{hadoop_home}/bin/hdfs", hadoop_home = HADOOP_HOME),
            "--debug".to_string(),
            "datanode".to_string(),
        ]),
        env: Some(env),
        readiness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_IPC, 10, 10)),
        liveness_probe: Some(tcp_socket_action_probe(SERVICE_PORT_NAME_IPC, 10, 10)),
        ..hadoop_container.clone()
    }]
}

fn datanode_init_containers(
    hdfs_image: &str,
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![
    chown_init_container(&HdfsNodeDataDirectory::default().datanode, hadoop_container),
    Container {
        name: "wait-for-namenodes".to_string(),
        image: Some(String::from(hdfs_image)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("
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
            pod_names = namenode_podrefs.iter().map(|pod_ref| pod_ref.pod_name.as_ref()).collect::<Vec<&str>>().join(" ")
            )
        ]),
        ..hadoop_container.clone()
    },])
}

fn journalnode_init_containers(hadoop_container: &Container) -> Option<Vec<Container>> {
    Some(vec![chown_init_container(
        &HdfsNodeDataDirectory::default().journalnode,
        hadoop_container,
    )])
}

fn namenode_init_containers(
    hdfs_image: &str,
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![
    chown_init_container(&HdfsNodeDataDirectory::default().namenode, hadoop_container),
    Container {
        name: "format-namenode".to_string(),
        image: Some(String::from(hdfs_image)),
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
        security_context: Some(SecurityContext {
            run_as_user: Some(1000),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    },
    Container {
        name: "format-zk".to_string(),
        image: Some(String::from(hdfs_image)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!("test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && {hadoop_home}/bin/hdfs zkfc -formatZK -nonInteractive || true", hadoop_home = HADOOP_HOME)
        ]),
        ..hadoop_container.clone()
    },
    ])
}

/// Creates a container that chowns and chmods the provided `node_dir`.
fn chown_init_container(node_dir: &str, hadoop_container: &Container) -> Container {
    Container {
        name: "chown-data".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "mkdir -p {node_dir} && chown -R stackable:stackable {data_dir} && chmod -R a=,u=rwX {data_dir}",
                node_dir = node_dir,
                data_dir = ROOT_DATA_DIR
            ),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    }
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
    Ok(Container {
        image: Some(hdfs.hdfs_image()?),
        env: Some(env),
        volume_mounts: Some(vec![
            VolumeMount {
                mount_path: String::from(ROOT_DATA_DIR),
                name: "data".to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: String::from(CONFIG_DIR_NAME),
                name: "config".to_string(),
                ..VolumeMount::default()
            },
        ]),
        ports: Some(
            rolegroup_ports
                .iter()
                .map(|(name, value)| ContainerPort {
                    name: Some(name.clone()),
                    container_port: *value,
                    protocol: Some("TCP".to_string()),
                    ..ContainerPort::default()
                })
                .collect(),
        ),
        security_context: Some(SecurityContext {
            run_as_user: Some(1000),
            ..SecurityContext::default()
        }),
        ..Container::default()
    })
}

fn local_disk_claim(name: &str, size: Quantity) -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([("storage".to_string(), size)])),
                ..ResourceRequirements::default()
            }),
            ..PersistentVolumeClaimSpec::default()
        }),
        ..PersistentVolumeClaim::default()
    }
}

/// Publish a Kubernetes event for the `hdfs` cluster resource.
async fn publish_event(
    hdfs: &HdfsCluster,
    client: &Client,
    action: &str,
    reason: &str,
    message: &str,
) -> Result<(), Error> {
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
        .map_err(|source| Error::PublishEvent { source })
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
