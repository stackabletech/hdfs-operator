// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err, clippy::large_enum_variant)]

use std::sync::Arc;

use clap::Parser;
use futures::{FutureExt, StreamExt};
use hdfs_controller::HDFS_FULL_CONTROLLER_NAME;
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    client::{self},
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        Api, ResourceExt,
        api::PartialObjectMeta,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector, watcher,
        },
    },
    kvp::ObjectLabels,
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::crd::{HdfsCluster, HdfsClusterVersion, constants::APP_NAME, v1alpha1};

mod config;
mod container;
mod crd;
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
        Command::Crd => HdfsCluster::merged_crd(HdfsClusterVersion::V1Alpha1)?
            .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?,
        Command::Run(RunArguments {
            product_config,
            watch_namespace,
            operator_environment: _,
            maintenance,
            common,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `HDFS_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `HDFS_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `HDFS_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, maintenance.end_of_support)?
                    .run()
                    .map(anyhow::Ok);

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hdfs-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &common.cluster_info)
                    .await?;

            let (store, store_w) = reflector::store();

            let hdfs_event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: HDFS_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            // The topology provider will need to build label information by querying kubernetes nodes and this
            // requires the clusterrole 'hdfs-clusterrole-nodes': this is bound to each deployed HDFS cluster
            // via a patch.
            let reflector = reflector::reflector(
                store_w,
                watcher(
                    Api::<PartialObjectMeta<v1alpha1::HdfsCluster>>::all(client.as_kube_client()),
                    watcher::Config::default(),
                ),
            )
            .then(|ev| {
                hdfs_clusterrolebinding_nodes_controller::reconcile(
                    client.as_kube_client(),
                    &store,
                    ev,
                )
            })
            .collect::<()>()
            .map(anyhow::Ok);

            let hdfs_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::HdfsCluster>>(&client),
                watcher::Config::default(),
            );
            let config_map_store = hdfs_controller.store();
            let hdfs_controller = hdfs_controller
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                    move |config_map| {
                        config_map_store
                            .state()
                            .into_iter()
                            .filter(move |hdfs| references_config_map(hdfs, &config_map))
                            .map(|hdfs| reflector::ObjectRef::from_obj(&*hdfs))
                    },
                )
                .run(
                    hdfs_controller::reconcile_hdfs,
                    hdfs_controller::error_policy,
                    Arc::new(hdfs_controller::Ctx {
                        client: client.clone(),
                        product_config,
                        event_recorder: hdfs_event_recorder.clone(),
                    }),
                )
                // We can let the reporting happen in the background
                .for_each_concurrent(
                    16, // concurrency limit
                    |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let hdfs_event_recorder = hdfs_event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &hdfs_event_recorder,
                                HDFS_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .instrument(info_span!("hdfs_controller"))
                .map(anyhow::Ok);

            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            futures::try_join!(hdfs_controller, reflector, eos_checker)?;
        }
    };

    Ok(())
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

fn references_config_map(
    hdfs: &DeserializeGuard<v1alpha1::HdfsCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(hdfs) = &hdfs.0 else {
        return false;
    };

    hdfs.spec.cluster_config.zookeeper_config_map_name == config_map.name_any()
        || match &hdfs.spec.cluster_config.authorization {
            Some(hdfs_authorization) => {
                hdfs_authorization.opa.config_map_name == config_map.name_any()
            }
            None => false,
        }
}
