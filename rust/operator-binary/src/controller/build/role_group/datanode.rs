//! Gathering one datanode role group.

use snafu::ResultExt;
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use super::{ExtraContainer, RoleGroupBuilder, RoleGroupInputs, common_container_logging};
use crate::{
    controller::{
        DataNodeRoleGroupConfig, ValidatedCluster,
        build::{
            self, Error, ListenerVolumeSnafu, RoleGroupSelectorLabelsSnafu,
            container::ContainerConfig,
        },
    },
    crd::{DataNodeContainer, HdfsNodeRole},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Data;

/// Gathers one datanode role group.
///
/// Beyond the `hdfs` main container and the Vector sidecar every role runs, datanodes run one
/// init container that waits for the namenodes.
///
/// Datanodes need no stable per-pod identity, so their listener is an ephemeral pod volume rather
/// than a claim template as the namenodes' is. They are also the only role that configures
/// `dfs.datanode.data.dir`; losing that is silent, because the datanodes then fall back to
/// Hadoop's default directory, which is container-local.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &DataNodeRoleGroupConfig,
) -> Result<RoleGroupBuilder<'a>, Error> {
    let config = &rg_config.config;

    let selector_labels = build::rolegroup_selector_labels(cluster, &ROLE, role_group_name)
        .context(RoleGroupSelectorLabelsSnafu {
            role: ROLE,
            role_group: role_group_name.clone(),
        })?;

    let listener_volume = ContainerConfig::datanode_listener_volume(config, &selector_labels)
        .context(ListenerVolumeSnafu {
            role: ROLE,
            role_group: role_group_name.clone(),
        })?;

    let (hdfs_logging, vector_logging) = common_container_logging(
        &config.logging,
        DataNodeContainer::Hdfs,
        DataNodeContainer::Vector,
    );

    let inputs = RoleGroupInputs {
        cluster,
        cluster_info,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        selector_labels,
        common: config.common.clone(),
        resources: config.resources.clone().into(),
        volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(config),
        // First, because the pod's `volumes` are an ordered list: moving the listener volume
        // changes the pod template of every datanode StatefulSet already running, which rolls
        // its pods for no reason.
        extra_pod_volumes: vec![listener_volume],
        hdfs_logging,
        vector_logging,
        datanode_storage: Some(config.resources.storage.clone()),
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
        vec![ExtraContainer::init(
            ContainerConfig::WaitForNameNodes,
            config
                .logging
                .for_container(&DataNodeContainer::WaitForNameNodes)
                .into_owned(),
        )],
    )
}
