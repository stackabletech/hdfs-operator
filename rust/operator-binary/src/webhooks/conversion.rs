use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    kube::{Client, core::crd::MergeError},
    webhook::{
        WebhookServer, WebhookServerError, WebhookServerOptions,
        webhooks::{ConversionWebhook, ConversionWebhookOptions},
    },
};

use crate::crd::{HdfsCluster, HdfsClusterVersion, constants::FIELD_MANAGER};

/// Contains errors which can be encountered when creating the conversion webhook server and the
/// CRD maintainer.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to merge CRD"))]
    MergeCrd { source: MergeError },

    #[snafu(display("failed to create conversion webhook server"))]
    CreateWebhook { source: WebhookServerError },
}

/// Creates and returns a [`WebhookServer`].
pub async fn create_webhook_server(
    operator_environment: &OperatorEnvironmentOptions,
    disable_crd_maintenance: bool,
    client: Client,
) -> Result<WebhookServer, Error> {
    let crds_and_handlers = vec![(
        HdfsCluster::merged_crd(HdfsClusterVersion::V1Alpha1).context(MergeCrdSnafu)?,
        HdfsCluster::try_convert,
    )];

    let conversion_webhook_options = ConversionWebhookOptions {
        disable_crd_maintenance,
        field_manager: FIELD_MANAGER.to_owned(),
    };

    let (conversion_webhook, _initial_reconcile_rx) =
        ConversionWebhook::new(crds_and_handlers, client, conversion_webhook_options);

    let webhook_server_options = WebhookServerOptions {
        socket_addr: WebhookServer::DEFAULT_SOCKET_ADDRESS,
        webhook_namespace: operator_environment.operator_namespace.to_owned(),
        webhook_service_name: operator_environment.operator_service_name.to_owned(),
    };

    WebhookServer::new(vec![Box::new(conversion_webhook)], webhook_server_options)
        .await
        .context(CreateWebhookSnafu)
}
