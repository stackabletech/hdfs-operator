//! NodePort controller for exposing individual Pods.
//!
//! For pods with the label `hdfs.stackable.tech/pod-service=true` a NodePort is created that exposes the local node pod.
use std::time::Duration;

use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_operator::{
    k8s_openapi::{
        api::core::v1::{Pod, Service, ServicePort, ServiceSpec},
        apimachinery::pkg::apis::meta::v1::OwnerReference,
    },
    kube::{
        core::ObjectMeta,
        runtime::controller::{Context, ReconcilerAction},
    },
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

pub async fn reconcile_pod(pod: Pod, ctx: Context<Ctx>) -> HdfsOperatorResult<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let name = pod.metadata.name.clone().ok_or(Error::PodHasNoName)?;

    let role = pod
        .metadata
        .labels
        .as_ref()
        .ok_or(Error::PodHasNoLabels { name: name.clone() })?
        .get(&"role".to_string())
        .ok_or(Error::PodHasNoRoleLabel { name: name.clone() })?;

    let ports: Vec<(String, i32)> = pod
        .spec
        .ok_or(Error::PodHasNoSpec { name: name.clone() })?
        .containers
        .iter()
        .filter(|container| container.name == role.clone())
        .flat_map(|c| c.ports.as_ref())
        .flat_map(|cp| cp.iter())
        .map(|cp| (cp.name.clone().unwrap_or_default(), cp.container_port))
        .collect();

    let svc = Service {
        metadata: ObjectMeta {
            namespace: pod.metadata.namespace.clone(),
            name: pod.metadata.name.clone(),
            owner_references: Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: name.clone(),
                uid: pod
                    .metadata
                    .uid
                    .ok_or(Error::PodHasNoUid { name: name.clone() })?,
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("NodePort".to_string()),
            external_traffic_policy: Some("Local".to_string()),
            ports: Some(
                ports
                    .iter()
                    .map(|(name, port)| ServicePort {
                        name: Some(name.clone()),
                        port: *port,
                        ..ServicePort::default()
                    })
                    .collect(),
            ),
            selector: Some([(LABEL_STS_POD_NAME.to_string(), name.clone())].into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    ctx.get_ref()
        .client
        .apply_patch(FIELD_MANAGER_SCOPE_POD, &svc, &svc)
        .await
        .map_err(|source| Error::ApplyPodServiceFailed { source, name })?;
    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
