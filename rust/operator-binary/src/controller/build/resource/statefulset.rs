//! Builds the rolegroup [`StatefulSet`] for an HDFS role group.
//!
//! Everything this needs is already resolved on the [`RoleGroupBuilder`], so it reads straight
//! through: open the pod, add the containers and volumes gathered for the role group, close it.

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
    container::{self},
    graceful_shutdown::{self, add_graceful_shutdown_config},
    role_group::RoleGroupBuilder,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add a volume to the Pod"))]
    AddVolume { source: container::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to add a volume to the Pod"))]
    AddPodVolume {
        source: stackable_operator::builder::pod::Error,
    },
}

/// The role group's [`StatefulSet`].
pub(crate) fn build_statefulset(builder: &RoleGroupBuilder) -> Result<StatefulSet, Error> {
    let cluster = builder.cluster;
    let role = &builder.role;
    let role_group_name = &builder.role_group_name;

    tracing::info!(
        "Setting up StatefulSet for role {role} role group {role_group_name}",
        role = role.as_ref()
    );

    let mut pb = PodBuilder::new();
    pb.metadata(ObjectMeta {
        labels: Some(builder.selector_labels.clone().into()),
        ..ObjectMeta::default()
    })
    .image_pull_secrets_from_product_image(&cluster.image)
    .affinity(&builder.common.affinity)
    .service_account_name(
        cluster
            .cluster_resource_names()
            .service_account_name()
            .to_string(),
    )
    .security_context(
        PodSecurityContextBuilder::with_stackable_defaults()
            .fs_group(1000)
            .build(),
    );

    pb.add_volumes(builder.pod_volumes.clone())
        .context(AddPodVolumeSnafu)?;
    for container in &builder.containers {
        pb.add_container(container.clone());
    }
    for init_container in &builder.init_containers {
        pb.add_init_container(init_container.clone());
    }

    add_graceful_shutdown_config(&builder.common, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(builder.pod_overrides.clone());

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: builder.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(builder.selector_labels.clone().into()),
            ..LabelSelector::default()
        },
        service_name: Some(
            cluster
                .governing_service_name(role, role_group_name)
                .to_string(),
        ),
        template: pod_template,

        volume_claim_templates: Some(builder.volume_claim_templates.clone()),
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
