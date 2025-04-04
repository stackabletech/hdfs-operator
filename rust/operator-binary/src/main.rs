use std::{ops::Deref as _, sync::Arc};

use clap::Parser;
use futures::{StreamExt, pin_mut};
use hdfs_controller::HDFS_FULL_CONTROLLER_NAME;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun, RollingPeriod},
    client::{self, Client},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        Api,
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
    namespace::WatchNamespace,
    shared::yaml::SerializeOptions,
};
use stackable_telemetry::{Tracing, tracing::settings::Settings};
use tracing::{info_span, level_filters::LevelFilter};
use tracing_futures::Instrument;

use crate::crd::{HdfsCluster, constants::APP_NAME, v1alpha1};

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
        Command::Crd => HdfsCluster::merged_crd(HdfsCluster::V1Alpha1)?
            .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?,
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            telemetry_arguments,
            cluster_info_opts,
        }) => {
            let _tracing_guard = Tracing::builder()
                // TODO (@Techassi): This should be a constant
                .service_name("hbase-operator")
                .with_console_output((
                    // TODO (@Techassi): Change to CONSOLE_LOG, create constant
                    "HDFS_OPERATOR_LOG",
                    LevelFilter::INFO,
                    !telemetry_arguments.no_console_output,
                ))
                // NOTE (@Techassi): Before stackable-telemetry was used, the log directory was
                // set via an env: `HDFS_OPERATOR_LOG_DIRECTORY`.
                // See: https://github.com/stackabletech/operator-rs/blob/f035997fca85a54238c8de895389cc50b4d421e2/crates/stackable-operator/src/logging/mod.rs#L40
                // Now it will be `ROLLING_LOGS` (or via `--rolling-logs <DIRECTORY>`).
                .with_file_output(telemetry_arguments.rolling_logs.map(|log_directory| {
                    let rotation_period = telemetry_arguments
                        .rolling_logs_period
                        .unwrap_or(RollingPeriod::Hourly)
                        .deref()
                        .clone();

                    Settings::builder()
                        // TODO (@Techassi): Change to CONSOLE_LOG or FILE_LOG, create constant
                        .with_environment_variable("HDFS_OPERATOR_LOG")
                        .with_default_level(LevelFilter::INFO)
                        .file_log_settings_builder(log_directory, "tracing-rs.log")
                        .with_rotation_period(rotation_period)
                        .build()
                }))
                .with_otlp_log_exporter((
                    "OTLP_LOG",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_logs,
                ))
                .with_otlp_trace_exporter((
                    "OTLP_TRACE",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_traces,
                ))
                .build()
                .init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
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

    let hdfs_event_recorder = Arc::new(Recorder::new(client.as_kube_client(), Reporter {
        controller: HDFS_FULL_CONTROLLER_NAME.to_string(),
        instance: None,
    }));

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
        hdfs_clusterrolebinding_nodes_controller::reconcile(client.as_kube_client(), &store, ev)
    })
    .collect::<()>();

    let hdfs_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<v1alpha1::HdfsCluster>>(&client),
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
    .instrument(info_span!("hdfs_controller"));

    pin_mut!(hdfs_controller, reflector);
    // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
    futures::future::select(hdfs_controller, reflector).await;
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
