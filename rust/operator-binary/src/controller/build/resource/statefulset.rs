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
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            self, ResolvedRoleGroup, RoleGroupResolver,
            container::{self, ContainerConfig},
            graceful_shutdown::{self, add_graceful_shutdown_config},
        },
    },
    crd::v1alpha1,
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
/// Every role-specific value is resolved by the caller into `resolved`. The role comes from the
/// role group's config type, so it cannot disagree with `resolved`.
pub(crate) fn build_rolegroup_statefulset<C: RoleGroupResolver>(
    validated: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rolegroup_config: &RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    resolved: ResolvedRoleGroup,
) -> Result<StatefulSet, Error> {
    let role = &C::ROLE;

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
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        validated,
        cluster_info,
        role_group_name,
        rolegroup_config,
        &resolved,
    )
    .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    add_graceful_shutdown_config(&resolved.common, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(rolegroup_config.pod_overrides.clone());

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: rolegroup_config.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(resolved.selector_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(
            validated
                .governing_service_name(role, role_group_name)
                .to_string(),
        ),
        template: pod_template,

        volume_claim_templates: Some(resolved.volume_claim_templates),
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
