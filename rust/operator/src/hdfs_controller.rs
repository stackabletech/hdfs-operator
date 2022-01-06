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
        hdfs_version(&hdfs)?,
        &transform_all_roles_to_config(&hdfs, build_role_properties(&hdfs)?)
            .map_err(|source| Error::InvalidRoleConfig { source })?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|source| Error::InvalidProductConfig { source })?;

    let role_broker_config = validated_config
        .get(&HdfsRole::NameNode.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let broker_role_service = build_broker_role_service(&hdfs)?;
    let broker_role_service = client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &broker_role_service,
            &broker_role_service,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    for (rolegroup_name, rolegroup_config) in role_broker_config.iter() {
        let rolegroup = hdfs.broker_rolegroup_ref(rolegroup_name);

        let rg_service = build_broker_rolegroup_service(&rolegroup, &hdfs)?;
        let rg_configmap = build_broker_rolegroup_config_map(&rolegroup, &hdfs, rolegroup_config)?;
        let rg_statefulset =
            build_broker_rolegroup_statefulset(&rolegroup, &hdfs, rolegroup_config)?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?;
    }

    for discovery_cm in build_discovery_configmaps(client, &hdfs, &hdfs, &broker_role_service)
        .await
        .context(BuildDiscoveryConfigSnafu)?
    {
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
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

pub fn hdfs_version(hdfs: &HdfsCluster) -> HdfsOperatorResult<&str> {
    hdfs.spec
        .version
        .as_deref()
        .ok_or(Error::ObjectHasNoVersion)
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
    }
    else {
        return Err(Error::NoNameNodeRole);
    }

    if let Some(data_nodes) = &hdfs.spec.data_nodes {
        result.insert(
            HdfsRole::DataNode.to_string(),
            (pnk.clone(), data_nodes.clone().erase()),
        );
    }
    else {
        return Err(Error::NoDataNodeRole);
    }

    Ok(result)
}

pub fn build_broker_role_service(hdfs: &HdfsCluster) -> HdfsOperatorResult<Service> {
    let role_name = HdfsRole::NameNode.to_string();
    let role_svc_name = hdfs
        .metadata.name.clone()
        .ok_or(Error::GlobalServiceNameNotFound)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hdfs)
            .name(&role_svc_name)
            .ownerreference_from_resource(hdfs, None, Some(true))
            .map_err(|source| Error::ObjectMissingMetadataForOwnerRef {source})?
            .with_recommended_labels(hdfs, APP_NAME, hdfs_version(hdfs)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("hdfs".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(hdfs, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}