//! Gathering one namenode role group.

use snafu::ResultExt;
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use super::{ExtraContainer, RoleGroupBuilder, RoleGroupInputs, common_container_logging};
use crate::{
    controller::{
        NameNodeRoleGroupConfig, ValidatedCluster,
        build::{
            self, Error, RoleGroupSelectorLabelsSnafu, VolumeClaimTemplatesSnafu,
            container::ContainerConfig,
        },
    },
    crd::{HdfsNodeRole, NameNodeContainer},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Name;

/// Gathers one namenode role group.
///
/// Beyond the `hdfs` main container and the Vector sidecar every role runs, namenodes run the
/// ZooKeeper fail-over controller alongside them and two init containers that format the
/// namenodes and ZooKeeper.
///
/// Their listener comes from a persistent volume claim template, for stable per-pod identity, so
/// it is among the volume claim templates rather than a pod-level volume. A pod volume and a
/// claim template of the same name would be rejected at apply time, which is why a role has only
/// ever one of the two.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &NameNodeRoleGroupConfig,
) -> Result<RoleGroupBuilder<'a>, Error> {
    let config = &rg_config.config;

    let selector_labels = build::rolegroup_selector_labels(cluster, &ROLE, role_group_name)
        .context(RoleGroupSelectorLabelsSnafu {
            role: ROLE,
            role_group: role_group_name.clone(),
        })?;

    let volume_claim_templates =
        ContainerConfig::namenode_volume_claim_templates(config, &selector_labels).context(
            VolumeClaimTemplatesSnafu {
                role: ROLE,
                role_group: role_group_name.clone(),
            },
        )?;

    let (hdfs_logging, vector_logging) = common_container_logging(
        &config.logging,
        NameNodeContainer::Hdfs,
        NameNodeContainer::Vector,
    );

    let inputs = RoleGroupInputs {
        cluster,
        cluster_info,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        selector_labels,
        common: config.common.clone(),
        resources: config.resources.clone().into(),
        volume_claim_templates,
        extra_pod_volumes: Vec::new(),
        hdfs_logging,
        vector_logging,
        datanode_storage: None,
        replicas: rg_config.replicas,
        config_overrides: rg_config.config_overrides.clone(),
        env_overrides: rg_config.env_overrides.clone(),
        pod_overrides: rg_config.pod_overrides.clone(),
        jvm_argument_overrides: rg_config
            .product_specific_common_config
            .jvm_argument_overrides
            .clone(),
    };

    RoleGroupBuilder::new(
        inputs,
        vec![
            ExtraContainer::side(
                ContainerConfig::Zkfc,
                config
                    .logging
                    .for_container(&NameNodeContainer::Zkfc)
                    .into_owned(),
            ),
            ExtraContainer::init(
                ContainerConfig::FormatNameNodes,
                config
                    .logging
                    .for_container(&NameNodeContainer::FormatNameNodes)
                    .into_owned(),
            ),
            ExtraContainer::init(
                ContainerConfig::FormatZooKeeper,
                config
                    .logging
                    .for_container(&NameNodeContainer::FormatZooKeeper)
                    .into_owned(),
            ),
        ],
    )
}
