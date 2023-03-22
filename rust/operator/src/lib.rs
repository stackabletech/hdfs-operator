mod config;
mod container;
mod discovery;
mod event;
mod hdfs_controller;
mod kerberos;
mod pod_svc_controller;
mod product_logging;
mod rbac;

use std::sync::Arc;

use futures::StreamExt;
use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::client::Client;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::Controller;
use stackable_operator::labels::ObjectLabels;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_operator::namespace::WatchNamespace;
use stackable_operator::product_config::ProductConfigManager;
use tracing::info_span;
use tracing_futures::Instrument;

pub const OPERATOR_NAME: &str = "hdfs.stackable.tech";

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let hdfs_controller = Controller::new(
        namespace.get_api::<HdfsCluster>(&client),
        ListParams::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        ListParams::default(),
    )
    .owns(namespace.get_api::<Service>(&client), ListParams::default())
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        ListParams::default(),
    )
    .shutdown_on_signal()
    .run(
        hdfs_controller::reconcile_hdfs,
        hdfs_controller::error_policy,
        Arc::new(hdfs_controller::Ctx {
            client: client.clone(),
            product_config,
        }),
    )
    .map(|res| report_controller_reconciled(&client, CONTROLLER_NAME, &res))
    .instrument(info_span!("hdfs_controller"));

    let pod_svc_controller = Controller::new(
        namespace.get_api::<Pod>(&client),
        ListParams::default().labels(&format!("{}=true", LABEL_ENABLE)),
    )
    .owns(namespace.get_api::<Pod>(&client), ListParams::default())
    .shutdown_on_signal()
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Arc::new(pod_svc_controller::Ctx {
            client: client.clone(),
        }),
    )
    .map(|res| report_controller_reconciled(&client, &format!("pod-svc.{OPERATOR_NAME}"), &res))
    .instrument(info_span!("pod_svc_controller"));

    futures::stream::select(hdfs_controller, pod_svc_controller)
        .collect::<()>()
        .await;
}

/// Creates recommended `ObjectLabels` to be used in deployed resources
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}
