use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_hdfs_crd::{HdfsCluster, HdfsPodRef, HdfsRole};
use stackable_operator::builder::{ConfigMapBuilder, ObjectMetaBuilder};
use stackable_operator::k8s_openapi::api::core::v1::{
    Container, ContainerPort, ObjectFieldSelector, PodSpec, PodTemplateSpec, SecurityContext,
    VolumeMount,
};
use stackable_operator::k8s_openapi::api::{
    apps::v1::{StatefulSet, StatefulSetSpec},
    core::v1::{
        ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, EnvVar, EnvVarSource,
        PersistentVolumeClaim, PersistentVolumeClaimSpec, ResourceRequirements, Service,
        ServicePort, ServiceSpec, Volume,
    },
};
use stackable_operator::k8s_openapi::apimachinery::pkg::{
    api::resource::Quantity, apis::meta::v1::LabelSelector,
};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::{
    api::ObjectMeta,
    runtime::controller::{Context, ReconcilerAction},
};
use stackable_operator::labels::role_group_selector_labels;
use stackable_operator::product_config::{types::PropertyNameKind, ProductConfigManager};
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
};
use stackable_operator::role_utils::RoleGroupRef;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

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
        hdfs.version()?,
        &transform_all_roles_to_config(&hdfs, hdfs.build_role_properties()?)
            .map_err(|source| Error::InvalidRoleConfig { source })?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|source| Error::InvalidProductConfig { source })?;

    let namenode_podrefs = hdfs.pods(HdfsRole::NameNode, &validated_config)?;
    let journalnode_podrefs = hdfs.pods(HdfsRole::JournalNode, &validated_config)?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup_ref = hdfs.rolegroup_ref(role_name, rolegroup_name);

            let rolegroup_ports = HdfsCluster::build_ports(&rolegroup_ref, &validated_config)?;
            let rg_service = build_rolegroup_service(&hdfs, &rolegroup_ref, &rolegroup_ports)?;
            let rg_configmap = build_rolegroup_config_map(
                &hdfs,
                &rolegroup_ref,
                rolegroup_config,
                &namenode_podrefs,
                &journalnode_podrefs,
            )?;
            let hadoop_container = hadoop_container(&hdfs, &rolegroup_ref, &rolegroup_ports)?;
            let rg_statefulset = build_rolegroup_statefulset(
                &hdfs,
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

fn build_rolegroup_service(
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
                hdfs.version()?,
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
            selector: Some(hdfs.role_group_selector_labels(rolegroup_ref)),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn build_rolegroup_config_map(
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
        ("dfs.nameservices".to_string(), hdfs.nameservice_id()),
        (
            format!("dfs.ha.namenodes.{}", hdfs.nameservice_id()),
            namenode_podrefs
                .iter()
                .map(|nn| nn.pod_name.clone())
                .collect::<Vec<String>>()
                .join(", "),
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
                            .get(&String::from(DFS_JOURNAL_NODE_RPC_ADDRESS))
                            .map_or(8485, |p| *p)
                    ))
                    .collect::<Vec<_>>()
                    .join(";"),
                hdfs.nameservice_id()
            ),
        ),
        (
            format!(
                "dfs.client.failover.proxy.provider.{}",
                hdfs.nameservice_id()
            ),
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
    ];
    hdfs_site_config.extend(namenode_podrefs.iter().flat_map(|nnid| {
        [
            (
                format!(
                    "dfs.namenode.rpc-address.{}.{}",
                    hdfs.nameservice_id(),
                    nnid.pod_name,
                ),
                format!(
                    "{}:{}",
                    nnid.fqdn(),
                    nnid.ports
                        .get(&String::from(DFS_NAME_NODE_RPC_ADDRESS))
                        .map_or(8020, |p| *p)
                ),
            ),
            (
                format!(
                    "dfs.namenode.http-address.{}.{}",
                    hdfs.nameservice_id(),
                    nnid.pod_name,
                ),
                format!(
                    "{}:{}",
                    nnid.fqdn(),
                    nnid.ports
                        .get(&String::from(DFS_NAME_NODE_HTTP_ADDRESS))
                        .map_or(9870, |p| *p)
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
                    hdfs.version()?,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .build(),
        )
        .add_data(
            CORE_SITE_XML.to_string(),
            hadoop_config_xml([
                ("fs.defaultFS", format!("hdfs://{}/", hdfs.nameservice_id())),
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

fn build_rolegroup_statefulset(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    namenode_podrefs: &[HdfsPodRef],
    journalnode_podrefs: &[HdfsPodRef],
    hadoop_container: &Container,
) -> HdfsOperatorResult<StatefulSet> {
    tracing::info!("Setting up StatefulSet for {:?}", rolegroup_ref);
    let replicas;
    let command;
    let service_name = rolegroup_ref.object_name();

    let mut init_containers = Some(vec![Container {
        name: "chown-data".to_string(),
        image: Some(hdfs.image()?),
        args: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "chown -R stackable:stackable /data && chmod -R a=,u=rwX /data".to_string(),
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..SecurityContext::default()
        }),
        ..hadoop_container.clone()
    }]);

    let mut containers = vec![];

    match serde_yaml::from_str(&rolegroup_ref.role).unwrap() {
        HdfsRole::DataNode => {
            replicas = hdfs.datanode_replicas(Some(rolegroup_ref))?;
            command = vec![
                "/stackable/hadoop/bin/hdfs".to_string(),
                "datanode".to_string(),
            ];
            init_containers
                .get_or_insert_with(Vec::new)
                .extend([Container {
                    name: "wait-for-namenodes".to_string(),
                    image: Some(
                        "docker.stackable.tech/stackable/tools:0.1.0-stackable0".to_string(),
                    ),
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
                                    pr.ports.get(SERVICE_PORT_NAME_HTTP).map_or(9870, |p| *p)
                                ))
                                .collect::<Vec<String>>()
                                .join(" ")
                        ),
                    ]),
                    ..hadoop_container.clone()
                }]);
        }

        HdfsRole::NameNode => {
            replicas = hdfs.namenode_replicas(Some(rolegroup_ref))?;
            command = vec![
                "/stackable/hadoop/bin/hdfs".to_string(),
                "namenode".to_string(),
            ];
            init_containers.get_or_insert_with(Vec::new).extend([
                Container {
                    name: "wait-for-journals".to_string(),
                    image: Some("docker.stackable.tech/stackable/tools:0.1.0-stackable0".to_string()),
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
                                    pr.ports.get(SERVICE_PORT_NAME_HTTP).map_or(8480, |p| *p)
                                ))
                                .collect::<Vec<String>>()
                                .join(" ")
                        ),
                    ]),
                    ..hadoop_container.clone()
                },
                Container {
                    name: "format-namenode".to_string(),
                    image: Some(hdfs.image()?),
                    args: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs namenode -format -noninteractive"
                            .to_string(),
                    ]),
                    ..hadoop_container.clone()
                },
                Container {
                    name: "format-journals".to_string(),
                    image: Some(hdfs.image()?),
                    args: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs namenode -initializeSharedEdits -force"
                            .to_string(),
                    ]),
                    ..hadoop_container.clone()
                },
                Container {
                    name: "format-zk".to_string(),
                    image: Some(hdfs.image()?),
                    args: Some(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        "test  \"0\" -eq \"$(echo $POD_NAME | sed -e 's/.*-//')\" && /stackable/hadoop/bin/hdfs zkfc -formatZK -nonInteractive || true".to_string(),
                    ]),
                    ..hadoop_container.clone()
                },
             ]);
            containers.push(Container {
                name: "zkfc".to_string(),
                args: Some(vec![
                    "/stackable/hadoop/bin/hdfs".to_string(),
                    "zkfc".to_string(),
                ]),
                ..hadoop_container.clone()
            });
        }
        HdfsRole::JournalNode => {
            replicas = hdfs.journalnode_replicas(Some(rolegroup_ref))?;
            command = vec![
                "/stackable/hadoop/bin/hdfs".to_string(),
                "journalnode".to_string(),
            ];
        }
    }

    containers.push(Container {
        name: rolegroup_ref.role.clone(),
        args: Some(command),
        ..hadoop_container.clone()
    });

    let template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(hdfs.role_group_selector_labels(rolegroup_ref)),
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
                hdfs.version()?,
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

fn hadoop_container(
    hdfs: &HdfsCluster,
    _rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_ports: &[(String, i32)],
) -> HdfsOperatorResult<Container> {
    Ok(Container {
        image: Some(hdfs.image()?),
        env: Some(vec![
            EnvVar {
                name: "HADOOP_HOME".to_string(),
                value: Some("/stackable/hadoop".to_string()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "HADOOP_CONF_DIR".to_string(),
                value: Some("/stackable/hadoop/etc/hadoop".to_string()),
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
                mount_path: "/stackable/hadoop/etc/hadoop".to_string(),
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
