mod hdfs_controller;
mod pod_svc_controller;
mod utils;

use futures::StreamExt;
use stackable_hdfs_crd::constants::*;
use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::client::Client;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::Context;
use stackable_operator::kube::runtime::Controller;
use stackable_operator::product_config::ProductConfigManager;
use tracing::info_span;
use tracing_futures::Instrument;
use utils::erase_controller_result_type;

pub async fn create_controller(client: Client, product_config: ProductConfigManager) {
    let hdfs_controller =
        Controller::new(client.get_all_api::<HdfsCluster>(), ListParams::default())
            .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
            .owns(client.get_all_api::<Service>(), ListParams::default())
            .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
            .shutdown_on_signal()
            .run(
                hdfs_controller::reconcile_hdfs,
                hdfs_controller::error_policy,
                Context::new(hdfs_controller::Ctx {
                    client: client.clone(),
                    product_config,
                }),
            )
            .instrument(info_span!("hdfs_controller"));

    /*
    hdfs_controller
        .map(erase_controller_result_type)
        .for_each(|res| async {
            match res {
                Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                Err(err) => {
                    tracing::error!(
                        error = &*err as &dyn std::error::Error,
                        "Failed to reconcile object",
                    )
                }
            }
        })
        .await;
        */

    let pod_svc_controller = Controller::new(
        client.get_all_api::<Pod>(),
        ListParams::default().labels(&format!("{}=true", LABEL_ENABLE)),
    )
    .owns(client.get_all_api::<Pod>(), ListParams::default())
    .shutdown_on_signal()
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Context::new(pod_svc_controller::Ctx { client }),
    )
    .instrument(info_span!("pod_svc_controller"));

    futures::stream::select(
        hdfs_controller.map(erase_controller_result_type),
        pod_svc_controller.map(erase_controller_result_type),
    )
    .for_each(|res| async {
        match res {
            Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
            Err(err) => {
                tracing::error!(error = &*err, "Failed to reconcile object",)
            }
        }
    })
    .await;
}
