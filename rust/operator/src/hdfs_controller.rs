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
use std::collections::HashMap;
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

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hdfs.rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_rolegroup_service(&sc, &rolegroup, rolegroup_config)?;
            let rg_configmap = build_rolegroup_config_map(&sc, &rolegroup, rolegroup_config)?;
            let rg_statefulset = build_rolegroup_statefulset(
                &hdfs,
                &default_master_role_ports,
                &rolegroup,
                rolegroup_config,
            )?;
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
    rolegroup: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(hdfs, None, Some(true))
            .map_err(|source| Error::ObjectMissingMetadataForOwnerRef {
                source,
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
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(
                build_ports(hdfs, rolegroup, rolegroup_config)?
                    .iter()
                    .map(|(name, value)| ServicePort {
                        name: Some(name.clone()),
                        port: *value,
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    })
                    .collect(),
            ),
            selector: Some(role_group_selector_labels(
                hdfs,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

// TODO: add metrics ports
fn build_ports(
    _sc: &HdfsCluster,
    rolegroup: &RoleGroupRef<HdfsCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Vec<(String, i32)>, Error> {
    Ok(match serde_yaml::from_str(&rolegroup.role).unwrap() {
        HdfsRole::Namenode => vec![(
            String::from(SERVICE_PORT_NAME_HTTP),
            rolegroup_config
                .get(&PropertyNameKind::File(String::from(HDFS_SITE_XML)))
                .and_then(|c| c.get(DFS_NAME_NODE_HTTP_ADDRESS))
                .unwrap_or(&String::from("0.0.0.0:9870"))
                .split(":")
                .last()
                .unwrap_or(&String::from("9870"))
                .parse::<i32>()
                .map_err(|source| Error::HdfsAddressParseError {
                    source,
                    address: String::from(DFS_NAME_NODE_HTTP_ADDRESS),
                })?,
        )],
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
                    .map_err(|source| Error::HdfsAddressParseError {
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
                    .map_err(|source| Error::HdfsAddressParseError {
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
                    .map_err(|source| Error::HdfsAddressParseError {
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
                    .map_err(|source| Error::HdfsAddressParseError {
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
                    .map_err(|source| Error::HdfsAddressParseError {
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
                    .map_err(|source| Error::HdfsAddressParseError {
                        source,
                        address: String::from(DFS_JOURNAL_NODE_RPC_ADDRESS),
                    })?,
            ),
        ],
    })
}
