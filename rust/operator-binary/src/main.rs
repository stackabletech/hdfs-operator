use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use product_config::ProductConfigManager;
use serde_json::json;
use stackable_hdfs_crd::{constants::*, HdfsCluster};
use stackable_operator::k8s_openapi::api::rbac::v1::{ClusterRoleBinding, Subject};
use stackable_operator::kube::api::{PartialObjectMeta, Patch, PatchParams};
use stackable_operator::kube::runtime::reflector;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::Api;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client::{self, Client},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::runtime::{watcher, Controller},
    kvp::ObjectLabels,
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    CustomResourceExt,
};
use tracing::log::warn;
use tracing::{error, info, info_span};
use tracing_futures::Instrument;

mod config;
mod container;
mod discovery;
mod event;
mod hdfs_controller;
mod kerberos;
mod operations;
mod product_logging;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
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
        Command::Crd => HdfsCluster::print_yaml_schema(built_info::CARGO_PKG_VERSION)?,
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
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
                built_info::TARGET_PLATFORM.unwrap_or("unknown target"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hdfs-operator/config-spec/properties.yaml",
            ])?;
            let client = client::create_client(Some(OPERATOR_NAME.to_string())).await?;
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

    // The topology provider will need to get/list pods and nodes to gather and filter label information.
    // Retrieving Node information requires a cluster-role, the binding for which is applied to each
    // HDFS cluster that has been deployed, via a patch.
    let reflector = std::pin::pin!(reflector::reflector(
        store_w,
        watcher(
            Api::<PartialObjectMeta<HdfsCluster>>::all(client.as_kube_client()),
            watcher::Config::default(),
        ),
    )
    .then(|ev| async {
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
        // Build a list of SubjectRef objects for all deployed HdfsClusters
        // To do this we only need the metadata for that, as we only really
        // need name and namespace of the objects
        let subjects: Vec<Subject> = store
            .state()
            .into_iter()
            .map(|object| object.metadata.clone())
            .map(|meta| Subject {
                kind: "ServiceAccount".to_string(),
                name: "hdfs-serviceaccount".to_string(),
                namespace: meta.namespace.clone(),
                ..Subject::default()
            })
            .collect();

        warn!("Patching clusterrolebinding...");
        let patch = Patch::Apply(json!({
            "apiVersion": "rbac.authorization.k8s.io/v1".to_string(),
            "kind": "ClusterRoleBinding".to_string(),
            "metadata": {
                "name": "hdfs-clusterrolebinding-nodes".to_string()
            },
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io".to_string(),
                "kind": "ClusterRole".to_string(),
                "name": "hdfs-clusterrole-nodes".to_string()
            },
            "subjects": subjects
        }));
        warn!("{:?}", &patch);

        let client = client.as_kube_client();
        let api: Api<ClusterRoleBinding> = Api::all(client);
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER_SCOPE.to_string()),
            ..PatchParams::default()
        };
        match api
            .patch("hdfs-clusterrolebinding-nodes", &params, &patch)
            .await
        {
            Ok(_) => warn!("successfully patched!"),
            Err(e) => error!("{}", e),
        }
    }));

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
