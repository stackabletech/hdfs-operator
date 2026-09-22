//! Builds the rolegroup [`StatefulSet`] for an HDFS role group.
//!
//! [`common_pod_builder`] opens the pod, the role builder adds the containers its role runs, and
//! [`finish_statefulset`] closes the pod and wraps it in the `StatefulSet`. Both halves need
//! nothing but [`RoleGroupCommon`].

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
    self,
    graceful_shutdown::{self, add_graceful_shutdown_config},
    role_group::RoleGroupCommon,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },
}

/// Opens the role group's pod: everything about it that does not depend on which containers the
/// role runs.
///
/// Infallible: every value it sets is already resolved on [`RoleGroupCommon`].
pub(crate) fn common_pod_builder(common: &RoleGroupCommon) -> PodBuilder {
    let mut pb = PodBuilder::new();

    let pb_metadata = ObjectMeta {
        labels: Some(common.selector_labels.clone().into()),
        ..ObjectMeta::default()
    };

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(&common.cluster.image)
        .affinity(&common.common.affinity)
        .service_account_name(
            common
                .cluster
                .cluster_resource_names()
                .service_account_name()
                .to_string(),
        )
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    pb
}

/// Closes the role group's pod and wraps it in its [`StatefulSet`], once the role builder has
/// added the containers its role runs.
pub(crate) fn finish_statefulset(
    mut pb: PodBuilder,
    common: &RoleGroupCommon,
) -> Result<StatefulSet, Error> {
    let cluster = common.cluster;
    let role = &common.role;
    let role_group_name = &common.role_group_name;

    tracing::info!(
        "Setting up StatefulSet for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    add_graceful_shutdown_config(&common.common, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(common.pod_overrides.clone());

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: common.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(common.selector_labels.clone().into()),
            ..LabelSelector::default()
        },
        service_name: Some(
            cluster
                .governing_service_name(role, role_group_name)
                .to_string(),
        ),
        template: pod_template,

        volume_claim_templates: Some(common.volume_claim_templates.clone()),
        ..StatefulSetSpec::default()
    };

    // TODO: The restart-controller is currently not enabled via the label RESTART_CONTROLLER_ENABLED_LABEL.
    // This is due to problems that might appear when restarting pods during the initial formatting of namenodes.
    // See: https://github.com/stackabletech/hdfs-operator/issues/750 (disable restart-controller)
    //      https://github.com/stackabletech/issues/issues/816 (enable restart-controller)
    let metadata = build::rolegroup_metadata(cluster, role, role_group_name);

    Ok(StatefulSet {
        metadata: metadata.build(),
        spec: Some(statefulset_spec),
        status: None,
    })
}
