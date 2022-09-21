//! NodePort controller for exposing individual Pods.
//!
//! For pods with the label `hdfs.stackable.tech/pod-service=true` a NodePort is created that exposes the local node pod.
use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::error::{Error, HdfsOperatorResult};
use stackable_operator::builder::ObjectMetaBuilder;
use stackable_operator::{
    k8s_openapi::api::core::v1::{Pod, Service, ServicePort, ServiceSpec},
    kube::runtime::controller::Action,
};
use std::sync::Arc;
use std::time::Duration;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

const APP_KUBERNETES_LABEL_BASE: &str = "app.kubernetes.io/";

pub async fn reconcile_pod(pod: Arc<Pod>, ctx: Arc<Ctx>) -> HdfsOperatorResult<Action> {
    tracing::info!("Starting reconcile");

    let name = pod.metadata.name.clone().ok_or(Error::PodHasNoName)?;

    let pod_labels = pod
        .metadata
        .labels
        .as_ref()
        .ok_or(Error::PodHasNoLabels { name: name.clone() })?;

    let role = pod_labels
        .get("role")
        .ok_or(Error::PodHasNoRoleLabel { name: name.clone() })?;

    let recommended_labels_from_pod = pod_labels
        .iter()
        .filter(|(key, _)| key.starts_with(APP_KUBERNETES_LABEL_BASE))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    let ports: Vec<(String, i32)> = pod
        .spec
        .as_ref()
        .ok_or(Error::PodHasNoSpec { name: name.clone() })?
        .containers
        .iter()
        .filter(|container| container.name == role.clone())
        .flat_map(|c| c.ports.as_ref())
        .flat_map(|cp| cp.iter())
        .map(|cp| (cp.name.clone().unwrap_or_default(), cp.container_port))
        .collect();

    let svc = Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(pod.as_ref())
            .labels(recommended_labels_from_pod)
            .ownerreference_from_resource(pod.as_ref(), None, None)
            .map_err(|source| Error::PodOwnerReference {
                source,
                name: name.clone(),
            })?
            .build(),
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
    ctx.client
        .apply_patch(FIELD_MANAGER_SCOPE_POD, &svc, &svc)
        .await
        .map_err(|source| Error::ApplyPodServiceFailed { source, name })?;
    Ok(Action::await_change())
}

pub fn error_policy(_error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
