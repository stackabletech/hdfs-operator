use std::sync::Arc;

use futures::{future, StreamExt};
use futures::FutureExt;
use product_config::ProductConfigManager;
use stackable_hdfs_crd::{constants::*, HdfsCluster};
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Pod, Service},
    },
    kube::runtime::{watcher, Controller,reflector::{self, ObjectRef, Store},},
    labels::ObjectLabels,
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
};
use tracing::info_span;
use tracing::{info, warn, error};
use tracing_futures::Instrument;

mod config;
mod container;
mod discovery;
mod event;
mod hdfs_controller;
mod kerberos;
mod operations;
mod pod_svc_controller;
mod product_logging;

mod built_info {
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

pub const OPERATOR_NAME: &str = "hdfs.stackable.tech";

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let (store, store_w) = reflector::store();

    let reflector = std::pin::pin!(reflector::reflector(
        store_w,
        watcher(
            stackable_operator::kube::Api::<HdfsCluster>::all(client.as_kube_client()),
            watcher::Config::default(),
        ),
    )
    .for_each(|ev| async {
        match ev {
            Ok(watcher::Event::Applied(o)) => {
                info!(object = %ObjectRef::from_obj(&o), "saw updated object")
            }
            Ok(watcher::Event::Deleted(o)) => {
                info!(object = %ObjectRef::from_obj(&o), "saw deleted object")
            }
            Ok(watcher::Event::Restarted(os)) => {
                let objects = os
                    .iter()
                    .map(ObjectRef::from_obj)
                    .map(|o| o.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                info!(objects, "restarted reflector")
            }
            Err(error) => {
                error!(
                    error = &error as &dyn std::error::Error,
                    "failed to update reflector"
                )
            }
        }
       patch_rolebinding(store.clone());
    }).map(Ok));

    let hdfs_controller = Controller::new(
        namespace.get_api::<HdfsCluster>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Service>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        watcher::Config::default(),
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
        watcher::Config::default().labels(&format!("{}=true", LABEL_ENABLE)),
    )
    .owns(
        namespace.get_api::<Pod>(&client),
        watcher::Config::default(),
    )
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

    futures::stream::select(hdfs_controller,  reflector)
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

pub async fn patch_rolebinding(store: Store<HdfsCluster>) {
    for cluster in store.state() {
        warn!(" {:?}", cluster);
    }
}
