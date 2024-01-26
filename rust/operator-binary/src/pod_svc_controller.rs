//! NodePort controller for exposing individual Pods.
//!
//! For pods with the label `hdfs.stackable.tech/pod-service=true` a NodePort is created that exposes the local node pod.
use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::HdfsRole;
use stackable_operator::{
    builder::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Pod, Service, ServicePort, ServiceSpec},
    kube::runtime::controller::Action,
    kvp::{LabelError, Labels},
    logging::controller::ReconcilerError,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Pod has no name"))]
    PodHasNoName,

    #[snafu(display("Pod [{name}] has no labels"))]
    PodHasNoLabels { name: String },

    #[snafu(display("Pod [{name}] has no spec"))]
    PodHasNoSpec { name: String },

    #[snafu(display("Failed to build owner reference of pod [{name}]"))]
    PodOwnerReference {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[snafu(display("Cannot create pod service [{name}]"))]
    ApplyPodServiceFailed {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[snafu(display("failed to build new labels from pod labels"))]
    NewLabelsFromPodLabels { source: LabelError },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

const APP_KUBERNETES_LABEL_BASE: &str = "app.kubernetes.io/";

pub async fn reconcile_pod(pod: Arc<Pod>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let name = pod.metadata.name.clone().context(PodHasNoNameSnafu)?;

    let pod_labels = Labels::try_from(
        pod.metadata
            .labels
            .as_ref()
            .with_context(|| PodHasNoLabelsSnafu { name: name.clone() })?,
    )
    .context(NewLabelsFromPodLabelsSnafu)?;

    let recommended_labels_from_pod = pod_labels
        .iter()
        .filter(|&label| {
            label
                .key()
                .prefix()
                .map(std::ops::Deref::deref)
                .map(std::ops::Deref::deref)
                == Some(APP_KUBERNETES_LABEL_BASE)
        })
        .cloned()
        .collect();

    let ports: Vec<(String, i32)> = pod
        .spec
        .as_ref()
        .with_context(|| PodHasNoSpecSnafu { name: name.clone() })?
        .containers
        .iter()
        .filter(|container| {
            container.name == HdfsRole::NameNode.to_string()
                || container.name == HdfsRole::DataNode.to_string()
                || container.name == HdfsRole::JournalNode.to_string()
        })
        .flat_map(|c| c.ports.as_ref())
        .flat_map(|cp| cp.iter())
        .map(|cp| (cp.name.clone().unwrap_or_default(), cp.container_port))
        .collect();

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(pod.as_ref())
        .labels(recommended_labels_from_pod)
        .ownerreference_from_resource(pod.as_ref(), None, None)
        .with_context(|_| PodOwnerReferenceSnafu { name: name.clone() })?
        .build();

    let service_spec = ServiceSpec {
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
    };

    let svc = Service {
        metadata,
        spec: Some(service_spec),
        ..Service::default()
    };

    // The pod service is deleted when the corresponding pod is deleted.
    // Therefore no cluster / orphaned resources have to be handled here.
    ctx.client
        .apply_patch(FIELD_MANAGER_SCOPE_POD, &svc, &svc)
        .await
        .with_context(|_| ApplyPodServiceFailedSnafu { name })?;

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<Pod>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}
