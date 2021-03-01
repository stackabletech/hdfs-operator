#![feature(backtrace)]
mod error;

use crate::error::Error;

use async_trait::async_trait;
use kube::api::ListParams;
use kube::Api;

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use stackable_hdfs_crd::{HdfsCluster, HdfsClusterSpec};
use stackable_operator::client::Client;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use std::future::Future;
use std::pin::Pin;

const FINALIZER_NAME: &str = "hdfs.hadoop.stackable.tech/cleanup";

const CLUSTER_NAME_LABEL: &str = "hdfs.hadoop.stackable.tech/cluster-name";

type HdfsReconcileResult = ReconcileResult<error::Error>;

struct HdfsState {
    context: ReconciliationContext<HdfsCluster>,
    hdfs_spec: HdfsClusterSpec,
}

impl HdfsState {
    async fn init_status(&mut self) -> HdfsReconcileResult {
        Ok(ReconcileFunctionAction::Done)
    }
}

impl ReconciliationState for HdfsState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move { self.init_status().await })
    }
}

#[derive(Debug)]
struct HdfsStrategy {}

impl HdfsStrategy {
    pub fn new() -> HdfsStrategy {
        HdfsStrategy {}
    }
}

#[async_trait]
impl ControllerStrategy for HdfsStrategy {
    type Item = HdfsCluster;
    type State = HdfsState;
    type Error = Error;

    fn finalizer_name(&self) -> String {
        FINALIZER_NAME.to_string()
    }

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        Ok(HdfsState {
            hdfs_spec: context.resource.spec.clone(),
            context,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let zk_api: Api<HdfsCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(zk_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = HdfsStrategy::new();

    controller.run(client, strategy).await;
}
