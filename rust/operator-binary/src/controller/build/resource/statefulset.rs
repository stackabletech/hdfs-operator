//! Builds the rolegroup [`StatefulSet`] for an HDFS role group.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::{PodBuilder, security::PodSecurityContextBuilder},
    k8s_openapi::{
        DeepMerge,
        api::apps::v1::{StatefulSet, StatefulSetSpec},
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::api::ObjectMeta,
};

use crate::controller::build::{
    self, RoleGroupBuilder,
    container::{self, ContainerConfig},
    graceful_shutdown::{self, add_graceful_shutdown_config},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to create container and volume configuration"))]
    FailedToCreateContainerAndVolumeConfiguration { source: container::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },
}

/// Builds the [`StatefulSet`] of one role group.
///
/// Everything about the role group comes from `resolved`, the role and the merged overrides
/// included, so there is nothing here to pair with the wrong role group.
pub(crate) fn build_rolegroup_statefulset(
    builder: &RoleGroupBuilder,
) -> Result<StatefulSet, Error> {
    let RoleGroupBuilder {
        cluster: validated,
        role_group_name,
        resolved,
        ..
    } = builder;
    let role = &builder.role();

    tracing::info!(
        "Setting up StatefulSet for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let image = &validated.image;

    // PodBuilder for StatefulSet Pod template.
    let mut pb = PodBuilder::new();

    let pb_metadata = ObjectMeta {
        labels: Some(resolved.selector_labels.clone().into()),
        ..ObjectMeta::default()
    };

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(image)
        .affinity(&resolved.common.affinity)
        .service_account_name(
            validated
                .cluster_resource_names()
                .service_account_name()
                .to_string(),
        )
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    // Adds all containers and volumes to the pod builder.
    ContainerConfig::add_containers_and_volumes(&mut pb, builder)
        .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    add_graceful_shutdown_config(&resolved.common, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(resolved.merged.pod_overrides.clone());

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: resolved.merged.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(resolved.selector_labels.clone().into()),
            ..LabelSelector::default()
        },
        service_name: Some(
            validated
                .governing_service_name(role, role_group_name)
                .to_string(),
        ),
        template: pod_template,

        volume_claim_templates: Some(resolved.volume_claim_templates.clone()),
        ..StatefulSetSpec::default()
    };

    // TODO: The restart-controller is currently not enabled via the label RESTART_CONTROLLER_ENABLED_LABEL.
    // This is due to problems that might appear when restarting pods during the initial formatting of namenodes.
    // See: https://github.com/stackabletech/hdfs-operator/issues/750 (disable restart-controller)
    //      https://github.com/stackabletech/issues/issues/816 (enable restart-controller)
    let metadata = build::rolegroup_metadata(validated, role, role_group_name);

    Ok(StatefulSet {
        metadata: metadata.build(),
        spec: Some(statefulset_spec),
        status: None,
    })
}
