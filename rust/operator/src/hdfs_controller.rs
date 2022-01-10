use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_hdfs_crd::{HdfsCluster, HdfsRole};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::k8s_openapi::api::{
    apps::v1::{StatefulSet, StatefulSetSpec},
    core::v1::{
        ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar,
        EnvVarSource, ExecAction, ObjectFieldSelector, PersistentVolumeClaim,
        PersistentVolumeClaimSpec, Probe, ResourceRequirements, Service, ServicePort, ServiceSpec,
        Volume,
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
use stackable_operator::labels::{role_group_selector_labels, role_selector_labels};
use stackable_operator::product_config::{
    types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
};
use stackable_operator::product_config_utils::Configuration;
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
};
use stackable_operator::role_utils::{Role, RoleGroupRef};
use std::borrow::Cow;
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
        &transform_all_roles_to_config(&hdfs, build_role_properties(&hdfs)?)
            .map_err(|source| Error::InvalidRoleConfig { source })?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|source| Error::InvalidProductConfig { source })?;

    let role_namenode_config = validated_config
        .get(&HdfsRole::NameNode.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let ns = hdfs
        .metadata
        .namespace
        .as_deref()
        .ok_or(Error::ObjectHasNoNamespace {
            obj_ref: ObjectRef::from_obj(&hdfs),
        })?;


    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hdfs.rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_rolegroup_service(&hdfs, &rolegroup, rolegroup_config)?;
            let rg_configmap = build_rolegroup_config_map(&hdfs, &rolegroup, rolegroup_config)?;
            let rg_statefulset = build_rolegroup_statefulset(&hdfs, &rolegroup, rolegroup_config)?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
                .await
                .map_err(|e| Error::ApplyRoleGroupService {
                    source: e,
                    name: &rg_service.metadata.name.clone().unwrap_or_default(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
                .await
                .map_err(|e| Error::ApplyRoleGroupConfigMap {
                    source: e,
                    name: &rg_configmap.metadata.name.clone().unwrap_or_default(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
                .await
                .map_err(|e| Error::ApplyRoleGroupStatefulSet {
                    source: e,
                    name: &rg_statefulset.metadata.name.clone().unwrap_or_default(),
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

fn build_role_properties(
    hdfs: &HdfsCluster,
) -> HdfsOperatorResult<
    HashMap<
        String,
        (
            Vec<PropertyNameKind>,
            Role<impl Configuration<Configurable = HdfsCluster>>,
        ),
    >,
> {
    let mut result = HashMap::new();
    let pnk = vec![
        PropertyNameKind::File(HDFS_SITE_XML.to_string()),
        PropertyNameKind::File(CORE_SITE_XML.to_string()),
        PropertyNameKind::Env,
    ];

    if let Some(name_nodes) = &hdfs.spec.name_nodes {
        result.insert(
            HdfsRole::NameNode.to_string(),
            (pnk.clone(), name_nodes.clone().erase()),
        );
    } else {
        return Err(Error::NoNameNodeRole);
    }

    if let Some(data_nodes) = &hdfs.spec.data_nodes {
        result.insert(
            HdfsRole::DataNode.to_string(),
            (pnk.clone(), data_nodes.clone().erase()),
        );
    } else {
        return Err(Error::NoDataNodeRole);
    }

    Ok(result)
}

fn build_rolegroup_service(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service, Error> {
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
                build_ports(hdfs, rolegroup_ref, rolegroup_config)?
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

// TODO: add metrics ports
fn build_ports(
    _hdfs: &HdfsCluster,
    rolegroup: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Vec<(String, i32)>, Error> {
    Ok(match serde_yaml::from_str(&rolegroup.role).unwrap() {
        HdfsRole::Namenode => vec![
            (
                String::from(SERVICE_PORT_NAME_HTTP),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_NAME_NODE_HTTP_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:9870"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("9870"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_NAME_NODE_HTTP_ADDRESS),
                    })?,
            ),
            (
                String::from(SERVICE_PORT_NAME_RPC),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_NAME_NODE_RPC_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:8020"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("8020"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_NAME_NODE_RPC_ADDRESS),
                    })?,
            ),
        ],
        HdfsRole::Datanode => vec![
            (
                String::from(SERVICE_PORT_NAME_DATA),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_DATA_NODE_DATA_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:9866"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("9866"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_DATA_NODE_DATA_ADDRESS),
                    })?,
            ),
            (
                String::from(SERVICE_PORT_NAME_HTTP),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_DATA_NODE_HTTP_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:9864"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("9864"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_DATA_NODE_HTTP_ADDRESS),
                    })?,
            ),
            (
                String::from(SERVICE_PORT_NAME_IPC),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_DATA_NODE_IPC_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:9867"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("9867"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_DATA_NODE_IPC_ADDRESS),
                    })?,
            ),
        ],
        HdfsRole::JournalNode => vec![
            (
                String::from(SERVICE_PORT_NAME_HTTP),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_JOURNAL_NODE_HTTP_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:8480"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("8480"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_JOURNAL_NODE_HTTP_ADDRESS),
                    })?,
            ),
            (
                String::from(SERVICE_PORT_NAME_HTTPS),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_JOURNAL_NODE_HTTPS_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:8481"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("8481"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_JOURNAL_NODE_HTTPS_ADDRESS),
                    })?,
            ),
            (
                String::from(SERVICE_PORT_NAME_RPC),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                    .and_then(|c| c.get(DFS_JOURNAL_NODE_RPC_ADDRESS))
                    .unwrap_or(&String::from("0.0.0.0:8485"))
                    .split(":")
                    .last()
                    .unwrap_or(&String::from("8485"))
                    .parse::<i32>()
                    .map_err(|source| Error::HdfsAddressPortParseError {
                        source,
                        address: String::from(DFS_JOURNAL_NODE_RPC_ADDRESS),
                    })?,
            ),
        ],
    })
}

fn build_rolegroup_config_map(
    hdfs: &HdfsCluster,
    rolegroup: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap, Error> {
    let hdfs_site_config = [
        ("dfs.namenode.name.dir".to_string(), "/data".to_string()),
        ("dfs.datanode.data.dir".to_string(), "/data".to_string()),
        ("dfs.journalnode.edits.dir".to_string(), "/data".to_string()),
        ("dfs.nameservices".to_string(), hdfs.nameservice_id()),
        (
            format!("dfs.ha.namenodes.{}", hdfs.nameservice_id()),
            (0..hdfs.spec.namenode_replicas.unwrap_or(1))
                .map(|i| format!("name-{}", i))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        (
            "dfs.namenode.shared.edits.dir".to_string(),
            format!(
                "qjournal://{}/{}",
                (0..hdfs.spec.journalnode_replicas.unwrap_or(1))
                    .map(journalnode_pod_fqdn)
                    .collect::<Vec<_>>()
                    .join(";"),
                hdfs.nameservice_id()
            ),
        ),
        (
            format!("dfs.client.failover.proxy.provider.{}", hdfs.nameservice_id()),
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
            "ha.zookeeper.quorum".to_string(),
            "${env.ZOOKEEPER_BROKERS}".to_string(),
        ),
        (
            "dfs.block.access.token.enable".to_string(),
            "true".to_string(),
        ),
        // (
        //     "dfs.data.transfer.protection".to_string(),
        //     "authentication".to_string(),
        // ),
        // ("dfs.http.policy".to_string(), "HTTPS_ONLY".to_string()),
        // TODO: "Privileged ports" don't really make sense in K8s, but we ought to sort out TLS anyway
        // (
        //     "ignore.secure.ports.for.testing".to_string(),
        //     "true".to_string(),
        // ),
        // (
        //     "dfs.journalnode.kerberos.principal".to_string(),
        //     format!("jn/{}@{}", namenode_fqdn, kerberos_realm),
        // ),
        // (
        //     "dfs.journalnode.keytab.file".to_string(),
        //     "/kerberos/jn.service.keytab".to_string(),
        // ),
        // (
        //     "dfs.namenode.kerberos.principal".to_string(),
        //     format!("nn/{}@{}", namenode_fqdn, kerberos_realm),
        // ),
        // (
        //     "dfs.namenode.keytab.file".to_string(),
        //     "/kerberos/nn.service.keytab".to_string(),
        // ),
        // (
        //     "dfs.datanode.kerberos.principal".to_string(),
        //     format!("dn/{}@{}", namenode_fqdn, kerberos_realm),
        // ),
        // (
        //     "dfs.datanode.keytab.file".to_string(),
        //     "/kerberos/dn.service.keytab".to_string(),
        // ),
        // JournalNode SPNEGO
        // (
        //     "dfs.web.authentication.kerberos.principal".to_string(),
        //     format!("HTTP/stackable-knode-1.kvm@{}", kerberos_realm),
        //     // format!("HTTP/_HOST@{}", kerberos_realm),
        // ),
        // (
        //     "dfs.web.authentication.kerberos.keytab".to_string(),
        //     "/kerberos/spnego.service.keytab".to_string(),
        // ),
    ]
    .into_iter()
    .chain(
        (0..rolegroup.replicas.map(i32::from).unwrap_or(1)).flat_map(|i| {
            [
                (
                    format!("dfs.namenode.rpc-address.{}.name-{}", hdfs.nameservice_id(), i),
                    format!("{}:8020", namenode_pod_fqdn(i)),
                ),
                (
                    format!("dfs.namenode.http-address.{}.name-{}", hdfs.nameservice_id(), i),
                    format!("{}:9870", namenode_pod_fqdn(i)),
                ),
            ]
        }),
    );

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hdfs)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hdfs, None, Some(true))
                .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                    source: e,
                    obj_ref: ObjectRef::from_obj(hdfs),
                })?
                .with_recommended_labels(
                    hdfs,
                    APP_NAME,
                    hdfs.version()?,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(
            CORE_SITE_XML.to_string(),
            hadoop_config_xml([
                ("fs.defaultFS", format!("hdfs://{}/", name)),
                // ("hadoop.security.authentication", "kerberos".to_string()),
                // ("hadoop.security.authorization", "false".to_string()),
                // JournalNode/WebHDFS SPNEGO
                // ("hadoop.http.authentication.type", "kerberos".to_string()),
                // (
                //     "hadoop.http.authentication.kerberos.principal",
                //     // format!("HTTP/stackable-knode-1.kvm@{}", kerberos_realm),
                //     format!("HTTP/_HOST@{}", kerberos_realm),
                // ),
                // (
                //     "hadoop.http.authentication.kerberos.keytab",
                //     "/kerberos/spnego.service.keytab".to_string(),
                // ),
            ]),
        )
        .add_data(
            HDFS_SITE_XML.to_string(),
            (
                "hdfs-site.xml".to_string(),
                hadoop_config_xml(hdfs_site_config),
            ),
        )
        .build()
        .map_err(|e| Error::BuildRoleGroupConfig {
            source: e,
            rolegroup: rolegroup.clone(),
        })
}

fn build_rolegroup_statefulset(
    hdfs: &HdfsCluster,
    rolegroup_ref: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> HdfsOperatorResult<StatefulSet> {
    match rolegroup_ref.role() {
        HdfsRole::NameNode => todo!(),
        HdfsRole::DataNode => todo!(),
        HdfsRole::JournalNode => {
            let role = hdfs.spec.journal_nodes.as_ref().ok_or(Error::NoJournalNodeRole)?;
            let rolegroup = role
                .role_groups
                .get(&rolegroup_ref.role_group)
                .ok_or(Error::RoleGroupNotFound { rolegroup: rolegroup_ref.role_group.clone()})?;
            Ok(StatefulSet {
                metadata: ObjectMetaBuilder::new()
                    .name_and_namespace(hdfs)
                    .name(&rolegroup_ref.object_name())
                    .ownerreference_from_resource(hdfs, None, Some(true))
                    .map_err(|source| Eror::ObjectMissingMetadataForOwnerRef {
                        source,
                        obj_ref: ObjectRef::from_obj(hdfs),
                    })?
                    .with_recommended_labels(
                        hdfs,
                        APP_NAME,
                        hdfs.version(),
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .build(),
                spec: Some(StatefulSetSpec {
                    pod_management_policy: Some("Parallel".to_string()),
                    replicas: rolegroup.replicas,
                    selector: LabelSelector {
                        match_labels: Some(role_group_selector_labels(
                    hdfs,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                        ..LabelSelector::default()
                    },
                    service_name: journalnode_name.clone(),
                    template: journalnode_pod_template,
                    volume_claim_templates: Some(vec![local_disk_claim(
                        "data",
                        Quantity("1Gi".to_string()),
                    )]),
                    ..StatefulSetSpec::default()
                }),
                status: None,
            })
        }
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

