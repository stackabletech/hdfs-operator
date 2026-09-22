//! Building the Kubernetes resources of one journalnode role group.

use snafu::ResultExt;
use stackable_operator::{
    builder::pod::PodBuilder,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::RoleGroupName,
};

use super::{RoleGroupCommon, common_container_logging};
use crate::{
    controller::{
        JournalNodeRoleGroupConfig, ValidatedCluster,
        build::{
            self, Error, RoleGroupSelectorLabelsSnafu, container, container::ContainerConfig,
            resource,
        },
    },
    crd::{HdfsNodeRole, JournalNodeContainer},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Journal;

/// One journalnode role group.
///
/// Journalnodes run no containers beyond the `hdfs` main container and the Vector sidecar that
/// every role has, are only used internally by the namenodes so have no listener, and do not
/// configure `dfs.datanode.data.dir`. That is why this struct holds nothing but the common
/// values.
pub(crate) struct JournalNodeRoleGroupBuilder<'a> {
    common: RoleGroupCommon<'a>,
}

impl<'a> JournalNodeRoleGroupBuilder<'a> {
    pub(crate) fn new(
        cluster: &'a ValidatedCluster,
        cluster_info: &'a KubernetesClusterInfo,
        role_group_name: &RoleGroupName,
        rg_config: &JournalNodeRoleGroupConfig,
    ) -> Result<Self, Error> {
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

        Ok(Self {
            common: RoleGroupCommon {
                cluster,
                cluster_info,
                role: ROLE,
                role_group_name: role_group_name.clone(),
                selector_labels,
                common: config.common.clone(),
                resources: config.resources.clone().into(),
                volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(config),
                hdfs_logging,
                vector_logging,
                replicas: rg_config.replicas,
                config_overrides: rg_config.config_overrides.clone(),
                env_overrides: rg_config.env_overrides.clone(),
                pod_overrides: rg_config.pod_overrides.clone(),
                jvm_argument_overrides: rg_config
                    .product_specific_common_config
                    .jvm_argument_overrides
                    .clone(),
            },
        })
    }

    pub(crate) fn build_services(&self) -> Result<Vec<Service>, Error> {
        self.common.build_services()
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        let mut pb = resource::statefulset::common_pod_builder(&self.common);

        self.add_containers(&mut pb)
            .map_err(|source| self.common.container_error(source))?;

        resource::statefulset::finish_statefulset(pb, &self.common)
            .map_err(|source| self.common.stateful_set_error(source))
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        let builder = resource::config_map::common_config_map(&self.common, None)
            .map_err(|source| self.common.config_map_error(source))?;

        resource::config_map::finish_config_map(builder, &self.common)
            .map_err(|source| self.common.config_map_error(source))
    }

    /// The containers a journalnode role group runs: the `hdfs` main container and the Vector
    /// sidecar that every role has, and nothing else.
    fn add_containers(&self, pb: &mut PodBuilder) -> Result<(), container::Error> {
        ContainerConfig::add_hdfs_container_and_common_volumes(pb, &self.common)
    }
}
