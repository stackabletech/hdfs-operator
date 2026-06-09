//! The validate step in the HdfsCluster controller.
//!
//! Synchronously merges and validates the cluster spec into the typed [`ValidatedCluster`]
//! consumed by `controller::build::*`. Config fragments are merged and validated via
//! [`HdfsNodeRole::merged_config`], and the per-file `configOverrides` / `envOverrides`
//! are merged here (role group wins).

use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::merge::Merge,
    kube::ResourceExt,
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup},
    v2::types::{kubernetes::NamespaceName, operator::ClusterName},
};
use strum::IntoEnumIterator;

use crate::{
    crd::{HdfsNodeRole, v1alpha1},
    hdfs_controller::{
        CONTAINER_IMAGE_BASE_NAME, ValidatedCluster, ValidatedClusterConfig, ValidatedRoleConfig,
        ValidatedRoleGroupConfig,
    },
    security::opa::HdfsOpaConfig,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid cluster name"))]
    InvalidClusterName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("the HdfsCluster has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid cluster namespace"))]
    InvalidNamespace {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },
}

pub fn validate_cluster(
    hdfs: &v1alpha1::HdfsCluster,
    image_repository: &str,
    hdfs_opa_config: Option<HdfsOpaConfig>,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = hdfs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    for hdfs_role in HdfsNodeRole::iter() {
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = hdfs.role_config(&hdfs_role)
        {
            role_configs.insert(hdfs_role, ValidatedRoleConfig { pdb: pdb.clone() });
        }

        let group_configs = match hdfs_role {
            HdfsNodeRole::Name => {
                validate_role_group_configs(hdfs, hdfs_role, hdfs.spec.name_nodes.as_ref())?
            }
            HdfsNodeRole::Data => {
                validate_role_group_configs(hdfs, hdfs_role, hdfs.spec.data_nodes.as_ref())?
            }
            HdfsNodeRole::Journal => {
                validate_role_group_configs(hdfs, hdfs_role, hdfs.spec.journal_nodes.as_ref())?
            }
        };

        role_groups.insert(hdfs_role, group_configs);
    }

    let namespace = hdfs.namespace().context(ObjectHasNoNamespaceSnafu)?;
    let namespace = NamespaceName::from_str(&namespace).context(InvalidNamespaceSnafu)?;

    Ok(ValidatedCluster {
        name: ClusterName::from_str(&hdfs.name_any()).context(InvalidClusterNameSnafu)?,
        namespace,
        image: resolved_product_image,
        cluster_config: ValidatedClusterConfig::resolve(hdfs, hdfs_opa_config),
        role_groups,
        role_configs,
    })
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<C>(
    hdfs: &v1alpha1::HdfsCluster,
    hdfs_role: HdfsNodeRole,
    role: Option<&Role<C, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>>,
) -> Result<BTreeMap<String, ValidatedRoleGroupConfig>, Error> {
    let Some(role) = role else {
        return Ok(BTreeMap::new());
    };

    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let validated =
                validate_role_group_config(hdfs, hdfs_role, role, role_group_name, role_group)?;
            Ok((role_group_name.clone(), validated))
        })
        .collect()
}

/// Validates a single role group into a [`ValidatedRoleGroupConfig`]: merges and
/// validates the CRD config via [`HdfsNodeRole::merged_config`] and merges the
/// role-level and role-group-level `configOverrides` and `envOverrides` (the role
/// group wins).
fn validate_role_group_config<C>(
    hdfs: &v1alpha1::HdfsCluster,
    hdfs_role: HdfsNodeRole,
    role: &Role<C, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    role_group_name: &str,
    role_group: &RoleGroup<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
) -> Result<ValidatedRoleGroupConfig, Error> {
    let config = hdfs_role
        .merged_config(hdfs, role_group_name)
        .context(FailedToResolveConfigSnafu)?;

    let mut config_overrides = role_group.config.config_overrides.clone();
    config_overrides.merge(&role.config.config_overrides);

    let mut env_overrides = BTreeMap::new();
    env_overrides.extend(role.config.env_overrides.clone());
    env_overrides.extend(role_group.config.env_overrides.clone());

    Ok(ValidatedRoleGroupConfig {
        replicas: role_group.replicas.unwrap_or_default(),
        config,
        config_overrides,
        env_overrides,
    })
}
