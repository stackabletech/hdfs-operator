//! The validate step in the HdfsCluster controller.
//!
//! Synchronously merges and validates the cluster spec into the typed
//! [`ValidatedCluster`], consumed by `controller::build::*`. This replaces the
//! product-config `transform_all_roles_to_config` /
//! `validate_all_roles_and_groups_config` steps: config fragments are merged and
//! validated via [`HdfsNodeRole::merged_config`], and the per-file
//! `configOverrides` / `envOverrides` are merged here (role group wins).

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::merge::Merge,
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role},
};
use strum::IntoEnumIterator;

use crate::{
    crd::{HdfsNodeRole, v1alpha1},
    hdfs_controller::{
        CONTAINER_IMAGE_BASE_NAME, ValidatedCluster, ValidatedRoleConfig, ValidatedRoleGroupConfig,
    },
    security::opa::HdfsOpaConfig,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
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

        let role_group_overrides = match hdfs_role {
            HdfsNodeRole::Name => collect_role_group_overrides(hdfs.spec.name_nodes.as_ref()),
            HdfsNodeRole::Data => collect_role_group_overrides(hdfs.spec.data_nodes.as_ref()),
            HdfsNodeRole::Journal => collect_role_group_overrides(hdfs.spec.journal_nodes.as_ref()),
        };

        let mut group_configs = BTreeMap::new();
        for (role_group_name, config_overrides, mut env_overrides) in role_group_overrides {
            let merged_config = hdfs_role
                .merged_config(hdfs, &role_group_name)
                .context(FailedToResolveConfigSnafu)?;

            // Rack awareness topology labels, namenode only. Previously injected
            // via the product-config `Configuration::compute_env`.
            if hdfs_role == HdfsNodeRole::Name {
                if let Some(rack_awareness) = hdfs.rackawareness_config() {
                    env_overrides.insert("TOPOLOGY_LABELS".to_string(), rack_awareness);
                }
            }

            group_configs.insert(
                role_group_name,
                ValidatedRoleGroupConfig {
                    merged_config,
                    config_overrides,
                    env_overrides,
                },
            );
        }

        role_groups.insert(hdfs_role, group_configs);
    }

    Ok(ValidatedCluster {
        image: resolved_product_image,
        role_groups,
        role_configs,
        hdfs_opa_config,
    })
}

/// Merges the role-level and role-group-level `configOverrides` and `envOverrides`
/// for every role group of a role (the role group wins). Replaces the
/// product-config `transform_all_roles_to_config` step.
fn collect_role_group_overrides<C>(
    role: Option<&Role<C, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>>,
) -> Vec<(String, v1alpha1::HdfsConfigOverrides, BTreeMap<String, String>)> {
    let Some(role) = role else {
        return Vec::new();
    };

    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let mut config_overrides = role_group.config.config_overrides.clone();
            config_overrides.merge(&role.config.config_overrides);

            let mut env_overrides = BTreeMap::new();
            env_overrides.extend(
                role.config
                    .env_overrides
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            env_overrides.extend(
                role_group
                    .config
                    .env_overrides
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            );

            (role_group_name.clone(), config_overrides, env_overrides)
        })
        .collect()
}
