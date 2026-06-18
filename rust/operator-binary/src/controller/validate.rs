//! The validate step in the HdfsCluster controller.

use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::{fragment::FromFragment, merge::Merge},
    role_utils::{GenericRoleConfig, Role},
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        role_utils::{JavaCommonConfig, with_validated_config},
    },
};
use strum::IntoEnumIterator;

use crate::{
    controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedRoleConfig, ValidatedRoleGroupConfig,
        dereference::DereferencedObjects,
    },
    crd::{
        AnyNodeConfig, DataNodeConfigFragment, HdfsNodeRole, JournalNodeConfigFragment,
        NameNodeConfigFragment, v1alpha1,
    },
};

const CONTAINER_IMAGE_BASE_NAME: &str = "hadoop";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to get the cluster name"))]
    GetClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the cluster namespace"))]
    GetClusterNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the cluster uid"))]
    GetClusterUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("invalid environment variable override name"))]
    ParseEnvVarName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to merge and validate the role group config"))]
    ValidateRoleGroupConfig {
        source: stackable_operator::config::fragment::ValidationError,
    },
}

pub fn validate_cluster(
    hdfs: &v1alpha1::HdfsCluster,
    image_repository: &str,
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    let image: product_image_selection::ResolvedProductImage = hdfs
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
    let cluster_name = get_cluster_name(hdfs).context(GetClusterNameSnafu)?;

    for hdfs_role in HdfsNodeRole::iter() {
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = hdfs.role_config(&hdfs_role)
        {
            role_configs.insert(hdfs_role, ValidatedRoleConfig { pdb: pdb.clone() });
        }

        let group_configs = match hdfs_role {
            HdfsNodeRole::Name => validate_role_group_configs(
                hdfs.spec.name_nodes.as_ref(),
                NameNodeConfigFragment::default_config(cluster_name.as_ref(), &hdfs_role),
                AnyNodeConfig::Name,
            )?,
            HdfsNodeRole::Data => validate_role_group_configs(
                hdfs.spec.data_nodes.as_ref(),
                DataNodeConfigFragment::default_config(cluster_name.as_ref(), &hdfs_role),
                AnyNodeConfig::Data,
            )?,
            HdfsNodeRole::Journal => validate_role_group_configs(
                hdfs.spec.journal_nodes.as_ref(),
                JournalNodeConfigFragment::default_config(cluster_name.as_ref(), &hdfs_role),
                AnyNodeConfig::Journal,
            )?,
        };

        role_groups.insert(hdfs_role, group_configs);
    }

    let namespace = get_namespace(hdfs).context(GetClusterNamespaceSnafu)?;
    let uid = get_uid(hdfs).context(GetClusterUidSnafu)?;

    Ok(ValidatedCluster::new(
        cluster_name,
        namespace,
        uid,
        image,
        ValidatedClusterConfig::resolve(hdfs, dereferenced_objects.hdfs_opa_config),
        role_groups,
        role_configs,
    ))
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`], which folds the CRD config fragment (default <-
/// role <- role group) plus the `configOverrides`, `envOverrides`, `cliOverrides`
/// and `podOverrides` (role group wins) into a single
/// [`RoleGroupConfig`](stackable_operator::v2::role_utils::RoleGroupConfig). The
/// concrete per-role validated config is wrapped into [`AnyNodeConfig`] via `wrap`.
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<Config, ValidatedConfig>(
    role: Option<&Role<Config, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>>,
    default_config: Config,
    wrap: fn(ValidatedConfig) -> AnyNodeConfig,
) -> Result<BTreeMap<String, ValidatedRoleGroupConfig>, Error>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
{
    let Some(role) = role else {
        return Ok(BTreeMap::new());
    };

    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let validated = with_validated_config::<
                ValidatedConfig,
                JavaCommonConfig,
                Config,
                GenericRoleConfig,
                v1alpha1::HdfsConfigOverrides,
            >(role_group, role, &default_config)
            .context(ValidateRoleGroupConfigSnafu)?;

            let mut env_overrides = EnvVarSet::new();
            for (env_var_name, env_var_value) in validated.config.env_overrides {
                env_overrides = env_overrides.with_value(
                    &EnvVarName::from_str(&env_var_name).context(ParseEnvVarNameSnafu)?,
                    env_var_value,
                );
            }

            // Re-wrap the per-role validated config into the role-agnostic
            // `AnyNodeConfig`; the merged overrides carry over unchanged.
            let validated = ValidatedRoleGroupConfig {
                replicas: validated.replicas,
                config: wrap(validated.config.config),
                config_overrides: validated.config.config_overrides,
                env_overrides,
                cli_overrides: validated.config.cli_overrides,
                pod_overrides: validated.config.pod_overrides,
                product_specific_common_config: validated.config.product_specific_common_config,
            };
            Ok((role_group_name.clone(), validated))
        })
        .collect()
}
