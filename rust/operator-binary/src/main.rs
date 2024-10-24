use std::sync::Arc;

use built_info::PKG_VERSION;
use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use product_config::ProductConfigManager;
use stackable_hdfs_crd::{constants::*, HdfsCluster};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client::{self, Client},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::core::DeserializeGuard,
    kube::{
        api::PartialObjectMeta,
        runtime::{reflector, watcher, Controller},
        Api,
    },
    kvp::ObjectLabels,
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    CustomResourceExt,
};
use tracing::info_span;
use tracing_futures::Instrument;

mod config;
mod container;
mod discovery;
mod event;
mod hdfs_clusterrolebinding_nodes_controller;
mod hdfs_controller;
mod operations;
mod product_logging;
mod security;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const OPERATOR_NAME: &str = "hdfs.stackable.tech";

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => HdfsCluster::print_yaml_schema(PKG_VERSION)?,
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
            cluster_info_opts,
        }) => {
            stackable_operator::logging::initialize_logging(
                "HDFS_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );

            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hdfs-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info_opts)
                    .await?;
            create_controller(client, product_config, watch_namespace).await;
        }
    };

    Ok(())
}

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let (store, store_w) = reflector::store();

    // The topology provider will need to build label information by querying kubernetes nodes and this
    // requires the clusterrole 'hdfs-clusterrole-nodes': this is bound to each deployed HDFS cluster
    // via a patch.
    let reflector = reflector::reflector(
        store_w,
        watcher(
            Api::<PartialObjectMeta<HdfsCluster>>::all(client.as_kube_client()),
            watcher::Config::default(),
        ),
    )
    .then(|ev| {
        hdfs_clusterrolebinding_nodes_controller::reconcile(client.as_kube_client(), &store, ev)
    });

    let hdfs_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<HdfsCluster>>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<DeserializeGuard<Service>>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
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

    futures::stream::select(hdfs_controller, reflector)
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
