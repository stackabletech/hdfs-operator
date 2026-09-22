//! Building the Kubernetes resources of one role group.
//!
//! There is one builder per role: [`NameNodeRoleGroupBuilder`], [`DataNodeRoleGroupBuilder`] and
//! [`JournalNodeRoleGroupBuilder`]. Each carries a [`RoleGroupCommon`], which holds everything
//! every role group has, and its own role's extras as plain fields. Each lists in its
//! `add_containers` the containers that role runs.
//!
//! A role's extras live on that role's builder only, so a role that has no listener volume has
//! no such field.
//!
//! The work every role shares lives in the helpers the three builders call, in
//! [`super::container`], [`resource::statefulset`] and [`resource::config_map`].

mod datanode;
mod journalnode;
mod namenode;

use std::fmt::Display;

pub(crate) use datanode::DataNodeRoleGroupBuilder;
pub(crate) use journalnode::JournalNodeRoleGroupBuilder;
pub(crate) use namenode::NameNodeRoleGroupBuilder;
use snafu::ResultExt;
use stackable_operator::{
    k8s_openapi::api::core::v1::{
        PersistentVolumeClaim, PodTemplateSpec, ResourceRequirements, Service,
    },
    kvp::Labels,
    product_logging::spec::{ContainerLogConfig, Logging},
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::EnvVarSet, jvm_argument_overrides::JvmArgumentOverrides,
        types::operator::RoleGroupName,
    },
};

use super::{Error, ServiceSnafu, container, resource};
use crate::{
    controller::ValidatedCluster,
    crd::{CommonNodeConfig, HdfsNodeRole, v1alpha1},
};

/// Everything every role group has, whatever its role.
///
/// The three role builders each carry one of these alongside their own role's extras. Every
/// field here is present for every role group, so no `Option` here stands for "this belongs to a
/// different role".
pub(crate) struct RoleGroupCommon<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) cluster_info: &'a KubernetesClusterInfo,
    /// The role this role group belongs to. The shared helpers match on it where a role
    /// genuinely differs, such as the container name and the ports.
    pub(crate) role: HdfsNodeRole,
    pub(crate) role_group_name: RoleGroupName,
    /// The selector labels of the role group's pods, also used as the `StatefulSet` selector and
    /// on the listener volumes.
    ///
    /// We must use the selector labels and not the recommended labels for the listener volumes.
    /// This is because the recommended set contains a "managed-by" label. That label triggers the
    /// cluster resources to "manage" listeners, which is wrong and leads to errors. The listeners
    /// are managed by the listener-operator.
    pub(crate) selector_labels: Labels,
    /// The role group's merged config that is common to every role.
    pub(crate) common: CommonNodeConfig,
    /// The resource requirements of the role group's main and init containers; the ZKFC sidecar
    /// has fixed requirements of its own and ignores this.
    pub(crate) resources: ResourceRequirements,
    /// The `StatefulSet`'s persistent volume claim templates. For namenodes these include the
    /// listener claim template.
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// The log config of the main `hdfs` container, which every role runs.
    pub(crate) hdfs_logging: ContainerLogConfig,
    /// The log config of the Vector sidecar; `None` when the Vector agent is disabled for this
    /// role group.
    pub(crate) vector_logging: Option<ContainerLogConfig>,
    /// The role group's replica count; `None` when unset, which counts as one replica.
    pub(crate) replicas: Option<u16>,
    pub(crate) config_overrides: v1alpha1::HdfsConfigOverrides,
    pub(crate) env_overrides: EnvVarSet,
    pub(crate) pod_overrides: PodTemplateSpec,
    pub(crate) jvm_argument_overrides: JvmArgumentOverrides,
}

impl RoleGroupCommon<'_> {
    /// The name the role group's owned objects share.
    pub(crate) fn object_name(&self) -> String {
        self.cluster
            .role_group_resource_names(&self.role, &self.role_group_name)
            .qualified_role_group_name()
            .to_string()
    }

    /// The headless and metrics `Service`s.
    pub(crate) fn build_services(&self) -> Result<Vec<Service>, Error> {
        let context = || ServiceSnafu {
            role: self.role,
            role_group: self.role_group_name.clone(),
        };

        Ok(vec![
            resource::service::rolegroup_headless_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
            resource::service::rolegroup_metrics_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
        ])
    }

    /// Wraps a container assembly failure with this role group's identity.
    pub(crate) fn container_error(&self, source: container::Error) -> Error {
        Error::Container {
            source,
            role: self.role,
            role_group: self.role_group_name.clone(),
        }
    }

    /// Wraps a `StatefulSet` assembly failure with this role group's identity.
    pub(crate) fn stateful_set_error(&self, source: resource::statefulset::Error) -> Error {
        Error::StatefulSet {
            source,
            role: self.role,
            role_group: self.role_group_name.clone(),
        }
    }

    /// Wraps a `ConfigMap` assembly failure with this role group's identity.
    pub(crate) fn config_map_error(&self, source: resource::config_map::Error) -> Error {
        Error::ConfigMap {
            source,
            role: self.role,
            role_group: self.role_group_name.clone(),
        }
    }
}

/// The log configs of the two containers every role runs: the main `hdfs` container, and the
/// Vector sidecar, which is `None` when the Vector agent is disabled for the role group.
///
/// Each role names these containers with its own enum, so this is generic over that enum rather
/// than repeated once per role.
fn common_container_logging<T>(
    logging: &Logging<T>,
    hdfs: T,
    vector: T,
) -> (ContainerLogConfig, Option<ContainerLogConfig>)
where
    T: Clone + Display + Ord,
{
    (
        logging.for_container(&hdfs).into_owned(),
        logging
            .enable_vector_agent
            .then(|| logging.for_container(&vector).into_owned()),
    )
}
