//! Gathering one journalnode role group.

use snafu::ResultExt;
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use super::{RoleGroupBuilder, RoleGroupInputs, common_container_logging};
use crate::{
    controller::{
        JournalNodeRoleGroupConfig, ValidatedCluster,
        build::{self, Error, RoleGroupSelectorLabelsSnafu, container::ContainerConfig},
    },
    crd::{HdfsNodeRole, JournalNodeContainer},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Journal;

/// Gathers one journalnode role group.
///
/// Journalnodes run nothing beyond the `hdfs` main container and the Vector sidecar every role
/// runs, hence the empty container list. They are used only internally by the namenodes, so they
/// have no listener, and they do not configure `dfs.datanode.data.dir`.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &JournalNodeRoleGroupConfig,
) -> Result<RoleGroupBuilder<'a>, Error> {
    let config = &rg_config.config;

    let selector_labels = build::rolegroup_selector_labels(cluster, &ROLE, role_group_name)
        .context(RoleGroupSelectorLabelsSnafu {
            role: ROLE,
            role_group: role_group_name.clone(),
        })?;

    let (hdfs_logging, vector_logging) = common_container_logging(
        &config.logging,
        JournalNodeContainer::Hdfs,
        JournalNodeContainer::Vector,
    );

    let inputs = RoleGroupInputs {
        cluster,
        cluster_info,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        selector_labels,
        common: config.common.clone(),
        resources: config.resources.clone().into(),
        volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(config),
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

    RoleGroupBuilder::new(inputs, Vec::new())
}
