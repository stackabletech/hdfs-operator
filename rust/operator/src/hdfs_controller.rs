use lazy_static::lazy_static;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_hdfs_crd::{constants::*, ROLE_PORTS};
use stackable_hdfs_crd::{HdfsCluster, HdfsPodRef, HdfsRole};
use stackable_operator::builder::{ConfigMapBuilder, ObjectMetaBuilder};
use stackable_operator::k8s_openapi::api::core::v1::{
    Container, ContainerPort, HTTPGetAction, ObjectFieldSelector, PodSpec, PodTemplateSpec, Probe,
    SecurityContext, VolumeMount,
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
use stackable_operator::kube::runtime::controller::{Context, ReconcilerAction};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::ResourceExt;
use stackable_operator::labels::role_group_selector_labels;
use stackable_operator::product_config::{types::PropertyNameKind, ProductConfigManager};
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
};
use stackable_operator::role_utils::RoleGroupRef;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

lazy_static! {
    /// Liveliness probe used by all master, worker and history containers.
    static ref PROBE: Probe = Probe {
        http_get: Some(HTTPGetAction {
            port: IntOrString::String(String::from(SERVICE_PORT_NAME_METRICS)),
            ..HTTPGetAction::default()
        }),
        period_seconds: Some(10),
        initial_delay_seconds: Some(10),
        ..Probe::default()
    };
}

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

pub async fn reconcile_hdfs(
    hdfs: HdfsCluster,
    ctx: Context<Ctx>,
) -> HdfsOperatorResult<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        hdfs.hdfs_version()?,
        &transform_all_roles_to_config(&hdfs, hdfs.build_role_properties()?)
            .map_err(|source| Error::InvalidRoleConfig { source })?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|source| Error::InvalidProductConfig { source })?;

    // A list of all name and journal nodes across all role groups is needed for all ConfigMaps and initialization checks.
    let namenode_podrefs = hdfs.pod_refs(&HdfsRole::NameNode)?;
    let journalnode_podrefs = hdfs.pod_refs(&HdfsRole::JournalNode)?;

    for (role_name, group_config) in validated_config.iter() {
        let role: HdfsRole = serde_yaml::from_str(role_name).unwrap();
        let role_ports = ROLE_PORTS.get(&role).unwrap().as_slice();
        let hadoop_container = hdfs_common_container(&hdfs, role_ports)?;

        for (rolegroup_name, rolegroup_config) in group_config.iter() {
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
                &journalnode_podrefs,
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

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
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
    _rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
) -> HdfsOperatorResult<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);

    let mut hdfs_site_config = vec![
        // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
        // be formatted by the namenode, or used by the other services.
        // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
        (
            "dfs.namenode.name.dir".to_string(),
            "/data/name".to_string(),
        ),
        (
            "dfs.datanode.data.dir".to_string(),
            "/data/data".to_string(),
        ),
        (
            "dfs.journalnode.edits.dir".to_string(),
            "/data/journal".to_string(),
        ),
        ("dfs.nameservices".to_string(), hdfs.name()),
        (
            format!("dfs.ha.namenodes.{}", hdfs.name()),
            namenode_podrefs
                .iter()
                .map(|nn| nn.pod_name.clone())
                .collect::<Vec<String>>()
                .join(","),
        ),
        (
            "dfs.namenode.shared.edits.dir".to_string(),
            format!(
                "qjournal://{}/{}",
                journalnode_podrefs
                    .iter()
                    .map(|jnid| format!(
                        "{}:{}",
                        jnid.fqdn(),
                        jnid.ports
                            .get(&String::from("dfs.journalnode.rpc-address"))
                            .map_or(8485, |p| *p)
                    ))
                    .collect::<Vec<_>>()
                    .join(";"),
                hdfs.name()
            ),
        ),
        (
            format!("dfs.client.failover.proxy.provider.{}", hdfs.name()),
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider".to_string(),
        ),
        (
            "dfs.ha.fencing.methods".to_string(),
            "shell(/bin/true)".to_string(),
        ),
        (
            "dfs.ha.nn.not-become-active-in-safemode".to_string(),
            "true".to_string(),
        ),
        (
            "dfs.ha.automatic-failover.enabled".to_string(),
            "true".to_string(),
        ),
        (
            "dfs.ha.namenode.id".to_string(),
            "${env.POD_NAME}".to_string(),
        ),
        (
            "dfs.replication".to_string(),
            hdfs.spec.dfs_replication.as_ref().unwrap_or(&3).to_string(),
        ),
    ];
    hdfs_site_config.extend(namenode_podrefs.iter().flat_map(|nnid| {
        [
            (
                format!("dfs.namenode.name.dir.{}.{}", hdfs.name(), nnid.pod_name,),
                "/data/name".to_string(),
            ),
            (
                format!("dfs.namenode.rpc-address.{}.{}", hdfs.name(), nnid.pod_name,),
                format!(
                    "{}:{}",
                    nnid.fqdn(),
                    nnid.ports
                        .get(&String::from("dfs.namenode.rpc-address"))
                        .map_or(DEFAULT_NAME_NODE_RPC_PORT, |p| *p)
                ),
            ),
            (
                format!(
                    "dfs.namenode.http-address.{}.{}",
                    hdfs.name(),
                    nnid.pod_name,
                ),
                format!(
                    "{}:{}",
                    nnid.fqdn(),
                    nnid.ports
                        .get(&String::from("dfs.namenode.http-address"))
                        .map_or(DEFAULT_NAME_NODE_HTTP_PORT, |p| *p)
                ),
            ),
        ]
    }));

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
        .add_data(
            CORE_SITE_XML.to_string(),
            hadoop_config_xml([
                ("fs.defaultFS", format!("hdfs://{}/", hdfs.name())),
                ("ha.zookeeper.quorum", "${env.ZOOKEEPER}".to_string()),
            ]),
        )
        .add_data(
            HDFS_SITE_XML.to_string(),
            hadoop_config_xml(hdfs_site_config),
        )
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
    journalnode_podrefs: &[HdfsPodRef],
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
            init_containers = datanode_init_containers(hdfs, namenode_podrefs, hadoop_container);
            containers = datanode_containers(rolegroup_ref, hadoop_container);
        }
        HdfsRole::NameNode => {
            replicas = hdfs.rolegroup_namenode_replicas(rolegroup_ref)?;
            init_containers =
                namenode_init_containers(&hdfs_image, journalnode_podrefs, hadoop_container);
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
                name: "HADOOP_OPTS".to_string(),
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
            "/stackable/hadoop/bin/hdfs".to_string(),
            "--debug".to_string(),
            "journalnode".to_string(),
        ]),
        readiness_probe: Some(PROBE.clone()),
        liveness_probe: Some(PROBE.clone()),
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
                name: "HADOOP_OPTS".to_string(),
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
                "/stackable/hadoop/bin/hdfs".to_string(),
                "--debug".to_string(),
                "namenode".to_string(),
            ]),
            env: Some(env),
            readiness_probe: Some(PROBE.clone()),
            liveness_probe: Some(PROBE.clone()),
            ..hadoop_container.clone()
        },
        // Note that we don't add the HADOOP_OPTS env var to this container (zkfc)
        // Here it would cause an "address already in use" error and prevent the namenode container from starting.
        // Because the jmx exporter is not enabled here, also the readiness probes are not enabled.
        Container {
            name: String::from("zkfc"),
            args: Some(vec![
                "/stackable/hadoop/bin/hdfs".to_string(),
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
                name: "HADOOP_OPTS".to_string(),
                value: Some(
                    format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/{}.yaml",
                    DEFAULT_DATA_NODE_METRICS_PORT,
                        rolegroup_ref.role,)
                ),
                ..EnvVar::default()
            },);

    vec![Container {
        name: rolegroup_ref.role.clone(),
        args: Some(vec![
            "/stackable/hadoop/bin/hdfs".to_string(),
            "--debug".to_string(),
            "datanode".to_string(),
        ]),
        env: Some(env),
        readiness_probe: Some(PROBE.clone()),
        liveness_probe: Some(PROBE.clone()),
        ..hadoop_container.clone()
    }]
}

fn datanode_init_containers(
    _hdfs: &HdfsCluster,
    namenode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![
    Container {
        name: "chown-data".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            " chown -R stackable:stackable /data && chmod -R a=,u=rwX /data".to_string(),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    },
    Container {
        name: "wait-for-namenodes".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "for jn in {}; do until curl --fail --connect-timeout 2 $jn; do sleep 2; done; done",
                namenode_podrefs
                    .iter()
                    .map(|pr| format!(
                        "http://{}:{}",
                        pr.fqdn(),
                        pr.ports.get(SERVICE_PORT_NAME_METRICS).map_or(DEFAULT_NAME_NODE_METRICS_PORT, |p| *p)
                    ))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
        ]),
        ..hadoop_container.clone()
    },])
}

fn journalnode_init_containers(hadoop_container: &Container) -> Option<Vec<Container>> {
    Some(vec![Container {
        name: "chown-data".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            " chown -R stackable:stackable /data && chmod -R a=,u=rwX /data".to_string(),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    }])
}

fn namenode_init_containers(
    hdfs_image: &str,
    journalnode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> Option<Vec<Container>> {
    Some(vec![
    Container {
        name: "chown-data".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            " chown -R stackable:stackable /data && chmod -R a=,u=rwX /data".to_string(),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    },
    Container {
        name: "wait-for-journals".to_string(),
        image: Some(String::from(TOOLS_IMAGE)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "for jn in {}; do until curl --fail --connect-timeout 2 $jn; do sleep 2; done; done",
                journalnode_podrefs
                    .iter()
                    .map(|pr| format!(
                        "http://{}:{}",
                        pr.fqdn(),
                        pr.ports.get(SERVICE_PORT_NAME_METRICS).map_or(DEFAULT_JOURNAL_NODE_METRICS_PORT, |p| *p)
                    ))
                    .collect::<Vec<String>>()
                    .join(" ")
            ),
        ]),
        ..hadoop_container.clone()
    },
    Container {
        name: "format-namenode".to_string(),
        image: Some(String::from(hdfs_image)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs namenode -format -noninteractive || true"
                .to_string(),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(1000),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    },
        Container {
        name: "format-journals".to_string(),
        image: Some(String::from(hdfs_image)),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "test  \"0\" -ne \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive|| true"
                .to_string(),
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
            "test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs zkfc -formatZK -nonInteractive || true".to_string(),
        ]),
        ..hadoop_container.clone()
    },
    ])
}

/// Build a Container with common HDFS environment variables, ports and volume mounts set.
fn hdfs_common_container(
    hdfs: &HdfsCluster,
    rolegroup_ports: &[(String, i32)],
) -> HdfsOperatorResult<Container> {
    Ok(Container {
        image: Some(hdfs.hdfs_image()?),
        env: Some(vec![
            EnvVar {
                name: "HADOOP_HOME".to_string(),
                value: Some("/stackable/hadoop".to_string()),
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
        ]),
        volume_mounts: Some(vec![
            VolumeMount {
                mount_path: "/data".to_string(),
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

fn hadoop_config_xml<I: IntoIterator<Item = (K, V)>, K: AsRef<str>, V: AsRef<str>>(
    kvs: I,
) -> String {
    use std::fmt::Write;
    let mut xml = "<configuration>\n".to_string();
    for (k, v) in kvs {
        writeln!(
            xml,
            "<property><name>{}</name><value>{}</value></property>",
            k.as_ref(),
            v.as_ref()
        )
        .unwrap();
    }
    xml.push_str("</configuration>");
    xml
}
