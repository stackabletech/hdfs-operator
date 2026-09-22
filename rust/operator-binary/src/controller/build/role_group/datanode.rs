//! Building the Kubernetes resources of one datanode role group.

use snafu::ResultExt;
use stackable_operator::{
    builder::pod::PodBuilder,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, Volume},
    },
    product_logging::spec::ContainerLogConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::RoleGroupName,
};

use super::{RoleGroupCommon, common_container_logging};
use crate::{
    controller::{
        DataNodeRoleGroupConfig, ValidatedCluster,
        build::{
            self, AddListenerVolumeSnafu, Error, ListenerVolumeSnafu, RoleGroupSelectorLabelsSnafu,
            container, container::ContainerConfig, properties::product_logging, resource,
        },
    },
    crd::{DataNodeContainer, HdfsNodeRole, storage::DataNodeStorageConfigInnerType},
};

const ROLE: HdfsNodeRole = HdfsNodeRole::Data;

/// One datanode role group: everything every role group has, plus the ephemeral listener volume,
/// the data volume configuration and the one init container only datanodes run.
pub(crate) struct DataNodeRoleGroupBuilder<'a> {
    common: RoleGroupCommon<'a>,
    /// Datanodes need no stable per-pod identity, so their listener is an ephemeral pod volume
    /// rather than a persistent volume claim template as the namenodes' is.
    listener_volume: Volume,
    /// The data volume configuration, which drives `dfs.datanode.data.dir`. Datanodes are the
    /// only role that configures it; losing it is silent, because the datanodes then fall back
    /// to Hadoop's default directory, which is container-local.
    storage: DataNodeStorageConfigInnerType,
    wait_for_namenodes_logging: ContainerLogConfig,
}

impl<'a> DataNodeRoleGroupBuilder<'a> {
    pub(crate) fn new(
        cluster: &'a ValidatedCluster,
        cluster_info: &'a KubernetesClusterInfo,
        role_group_name: &RoleGroupName,
        rg_config: &DataNodeRoleGroupConfig,
    ) -> Result<Self, Error> {
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

        Ok(Self {
            common: RoleGroupCommon {
                cluster,
                cluster_info,
                role: ROLE,
                role_group_name: role_group_name.clone(),
                selector_labels,
                common: config.common.clone(),
                resources: config.resources.clone().into(),
                volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(config),
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
            listener_volume,
            storage: config.resources.storage.clone(),
            wait_for_namenodes_logging: config
                .logging
                .for_container(&DataNodeContainer::WaitForNameNodes)
                .into_owned(),
        })
    }

    pub(crate) fn build_services(&self) -> Result<Vec<Service>, Error> {
        self.common.build_services()
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        let mut pb = resource::statefulset::common_pod_builder(&self.common);

        // Added before the containers because the pod's `volumes` are an ordered list: putting
        // the listener volume anywhere else changes the pod template of every datanode
        // StatefulSet already running, which rolls its pods for no reason.
        pb.add_volume(self.listener_volume.clone())
            .context(AddListenerVolumeSnafu {
                role: ROLE,
                role_group: self.common.role_group_name.clone(),
            })?;

        self.add_containers(&mut pb)
            .map_err(|source| self.common.container_error(source))?;

        resource::statefulset::finish_statefulset(pb, &self.common)
            .map_err(|source| self.common.stateful_set_error(source))
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        let mut builder =
            resource::config_map::common_config_map(&self.common, Some(self.storage.clone()))
                .map_err(|source| self.common.config_map_error(source))?;

        product_logging::add_log4j_config(
            &mut builder,
            &ContainerConfig::WaitForNameNodes,
            &self.wait_for_namenodes_logging,
        );

        resource::config_map::finish_config_map(builder, &self.common)
            .map_err(|source| self.common.config_map_error(source))
    }

    /// The containers a datanode role group runs: the `hdfs` main container and the Vector
    /// sidecar that every role has, then the init container that waits for the namenodes.
    fn add_containers(&self, pb: &mut PodBuilder) -> Result<(), container::Error> {
        ContainerConfig::add_hdfs_container_and_common_volumes(pb, &self.common)?;

        ContainerConfig::WaitForNameNodes.add_as_init_container(
            pb,
            &self.common,
            &self.wait_for_namenodes_logging,
        )?;

        Ok(())
    }
}
