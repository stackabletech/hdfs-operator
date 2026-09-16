//! Building the Kubernetes resources of one role group.
//!
//! [`RoleGroupBuilder::new`] resolves the role group once; every builder below reads that one
//! object, so none of them takes the role, the config or the resolved values as separate
//! arguments and there is nothing to pair with the wrong role group.

use snafu::ResultExt;
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::operator::RoleGroupName,
    },
};

use super::{
    ConfigMapSnafu, Error, RoleGroupSelectorLabelsSnafu, ServiceSnafu, StatefulSetSnafu,
    resolve::{ResolvedRoleGroup, RoleGroupResolver},
    resource, rolegroup_selector_labels,
};
use crate::{
    controller::ValidatedCluster,
    crd::{HdfsNodeRole, v1alpha1},
};

/// One role group's resolved values plus the context every builder needs.
pub(crate) struct RoleGroupBuilder<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) cluster_info: &'a KubernetesClusterInfo,
    pub(crate) role_group_name: RoleGroupName,
    pub(crate) resolved: ResolvedRoleGroup,
}

impl<'a> RoleGroupBuilder<'a> {
    /// Resolves one role group. `C` is the role group's config type and appears here only: the
    /// builder it returns names no config type, so nothing below this point is generic.
    pub(crate) fn new<C: RoleGroupResolver>(
        cluster: &'a ValidatedCluster,
        cluster_info: &'a KubernetesClusterInfo,
        role_group_name: &RoleGroupName,
        rg_config: &RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    ) -> Result<Self, Error> {
        let selector_labels = rolegroup_selector_labels(cluster, &C::ROLE, role_group_name)
            .context(RoleGroupSelectorLabelsSnafu {
                role: C::ROLE,
                role_group: role_group_name.clone(),
            })?;
        let resolved = C::resolve(rg_config, role_group_name, selector_labels)?;

        Ok(Self {
            cluster,
            cluster_info,
            role_group_name: role_group_name.clone(),
            resolved,
        })
    }

    /// The role this role group belongs to, read back out of the resolved values.
    pub(crate) fn role(&self) -> HdfsNodeRole {
        self.resolved.role.node_role()
    }

    /// The headless and metrics Services. Role-agnostic: neither reads the role config.
    pub(crate) fn build_services(&self) -> Result<Vec<Service>, Error> {
        let role = self.role();
        let context = || ServiceSnafu {
            role,
            role_group: self.role_group_name.clone(),
        };

        Ok(vec![
            resource::service::rolegroup_headless_service(
                self.cluster,
                &role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
            resource::service::rolegroup_metrics_service(
                self.cluster,
                &role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
        ])
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        resource::config_map::build_rolegroup_config_map(self).context(ConfigMapSnafu {
            role: self.role(),
            role_group: self.role_group_name.clone(),
        })
    }

    pub(crate) fn build_stateful_set(&self) -> Result<StatefulSet, Error> {
        resource::statefulset::build_rolegroup_statefulset(self).context(StatefulSetSnafu {
            role: self.role(),
            role_group: self.role_group_name.clone(),
        })
    }
}
