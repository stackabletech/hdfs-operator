//! Building the Kubernetes resources of one namenode role group.

use snafu::ResultExt;
use stackable_operator::{
    builder::pod::PodBuilder,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    product_logging::spec::ContainerLogConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::RoleGroupName,
};

use super::{RoleGroupCommon, common_container_logging};
use crate::{
    controller::{
        NameNodeRoleGroupConfig, ValidatedCluster,
        build::{
            self, Error, RoleGroupSelectorLabelsSnafu, VolumeClaimTemplatesSnafu, container,
            container::ContainerConfig, properties::product_logging, resource,
        },
    },
    crd::{HdfsNodeRole, NameNodeContainer},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Name;

/// One namenode role group: everything every role group has, plus the three containers only
/// namenodes run.
///
/// Namenodes get their listener from a persistent volume claim template, for stable per-pod
/// identity, so it is already among [`RoleGroupCommon::volume_claim_templates`] and there is no
/// pod-level listener volume here. A pod volume and a claim template of the same name would be
/// rejected at apply time, which is why only one of the two ever exists for a role.
pub(crate) struct NameNodeRoleGroupBuilder<'a> {
    common: RoleGroupCommon<'a>,
    zkfc_logging: ContainerLogConfig,
    format_namenodes_logging: ContainerLogConfig,
    format_zookeeper_logging: ContainerLogConfig,
}

impl<'a> NameNodeRoleGroupBuilder<'a> {
    pub(crate) fn new(
        cluster: &'a ValidatedCluster,
        cluster_info: &'a KubernetesClusterInfo,
        role_group_name: &RoleGroupName,
        rg_config: &NameNodeRoleGroupConfig,
    ) -> Result<Self, Error> {
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

        Ok(Self {
            common: RoleGroupCommon {
                cluster,
                cluster_info,
                role: ROLE,
                role_group_name: role_group_name.clone(),
                selector_labels,
                common: config.common.clone(),
                resources: config.resources.clone().into(),
                volume_claim_templates,
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
            zkfc_logging: config
                .logging
                .for_container(&NameNodeContainer::Zkfc)
                .into_owned(),
            format_namenodes_logging: config
                .logging
                .for_container(&NameNodeContainer::FormatNameNodes)
                .into_owned(),
            format_zookeeper_logging: config
                .logging
                .for_container(&NameNodeContainer::FormatZooKeeper)
                .into_owned(),
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
        let mut builder = resource::config_map::common_config_map(&self.common, None)
            .map_err(|source| self.common.config_map_error(source))?;

        product_logging::add_log4j_config(&mut builder, &ContainerConfig::Zkfc, &self.zkfc_logging);
        product_logging::add_log4j_config(
            &mut builder,
            &ContainerConfig::FormatNameNodes,
            &self.format_namenodes_logging,
        );
        product_logging::add_log4j_config(
            &mut builder,
            &ContainerConfig::FormatZooKeeper,
            &self.format_zookeeper_logging,
        );

        resource::config_map::finish_config_map(builder, &self.common)
            .map_err(|source| self.common.config_map_error(source))
    }

    /// The containers a namenode role group runs: the `hdfs` main container and the Vector
    /// sidecar that every role has, then the ZooKeeper fail-over controller side container and
    /// the two init containers that format the namenodes and ZooKeeper.
    fn add_containers(&self, pb: &mut PodBuilder) -> Result<(), container::Error> {
        ContainerConfig::add_hdfs_container_and_common_volumes(pb, &self.common)?;

        ContainerConfig::Zkfc.add_as_side_container(pb, &self.common, &self.zkfc_logging)?;
        ContainerConfig::FormatNameNodes.add_as_init_container(
            pb,
            &self.common,
            &self.format_namenodes_logging,
        )?;
        ContainerConfig::FormatZooKeeper.add_as_init_container(
            pb,
            &self.common,
            &self.format_zookeeper_logging,
        )?;

        Ok(())
    }
}
