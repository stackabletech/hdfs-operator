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
    kvp::{LabelError, Labels},
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            self,
            container::{self, ContainerConfig},
            graceful_shutdown::{self, add_graceful_shutdown_config},
        },
    },
    crd::HdfsNodeRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: LabelError },

    #[snafu(display("failed to create container and volume configuration"))]
    FailedToCreateContainerAndVolumeConfiguration { source: container::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to build role-group volume claim templates from config"))]
    BuildRoleGroupVolumeClaimTemplates { source: container::Error },
}

pub(crate) fn build_rolegroup_statefulset(
    validated: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
    rolegroup_config: &ValidatedRoleGroupConfig,
) -> Result<StatefulSet, Error> {
    tracing::info!("Setting up StatefulSet for role {role} role group {role_group_name}");

    let image = &validated.image;
    let merged_config = &rolegroup_config.config;

    // PodBuilder for StatefulSet Pod template.
    let mut pb = PodBuilder::new();

    let rolegroup_selector_labels: Labels =
        build::rolegroup_selector_labels(validated, role, role_group_name)
            .context(RoleGroupSelectorLabelsSnafu)?;

    let pb_metadata = ObjectMeta {
        labels: Some(rolegroup_selector_labels.clone().into()),
        ..ObjectMeta::default()
    };

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(image)
        .affinity(&merged_config.affinity)
        .service_account_name(
            validated
                .cluster_resource_names()
                .service_account_name()
                .to_string(),
        )
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    // Adds all containers and volumes to the pod builder
    // We must use the selector labels ("rolegroup_selector_labels") and not the recommended labels
    // for the ephemeral listener volumes created by this function.
    // This is because the recommended set contains a "managed-by" label. This label triggers
    // the cluster resources to "manage" listeners which is wrong and leads to errors.
    // The listeners are managed by the listener-operator.
    ContainerConfig::add_containers_and_volumes(
        &mut pb,
        validated,
        cluster_info,
        role,
        role_group_name,
        rolegroup_config,
        &rolegroup_selector_labels,
    )
    .context(FailedToCreateContainerAndVolumeConfigurationSnafu)?;

    add_graceful_shutdown_config(merged_config, &mut pb).context(GracefulShutdownSnafu)?;

    // The `podOverrides` were already merged (role <- role group) during validation
    // by the local-`framework` `with_validated_config`.
    let mut pod_template = pb.build_template();
    pod_template.merge_from(rolegroup_config.pod_overrides.clone());

    // The same comment regarding labels is valid here as it is for the ContainerConfig::add_containers_and_volumes() call above.
    let pvcs = ContainerConfig::volume_claim_templates(merged_config, &rolegroup_selector_labels)
        .context(BuildRoleGroupVolumeClaimTemplatesSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("OrderedReady".to_string()),
        replicas: rolegroup_config.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(rolegroup_selector_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(
            validated
                .governing_service_name(role, role_group_name)
                .to_string(),
        ),
        template: pod_template,

        volume_claim_templates: Some(pvcs),
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
