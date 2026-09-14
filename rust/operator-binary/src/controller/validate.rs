//! The validate step in the HdfsCluster controller.

use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::{fragment::FromFragment, merge::Merge},
    role_utils::GenericRoleConfig,
    v2::{
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        role_utils::{JavaCommonConfig, Role, RoleGroupConfig, with_validated_config},
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedClusterStatus, ValidatedRole,
        ValidatedRoleConfig, dereference::DereferencedObjects,
    },
    crd::{
        DataNodeConfigFragment, HdfsNodeRole, JournalNodeConfigFragment, NameNodeConfigFragment,
        UpgradeStateError, v1alpha1,
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

    #[snafu(display("invalid role group name {role_group:?}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to merge and validate the role group config"))]
    ValidateRoleGroupConfig {
        source: stackable_operator::config::fragment::ValidationError,
    },

    #[snafu(display("invalid upgrade state"))]
    UpgradeState { source: UpgradeStateError },
}

pub fn validate_cluster(
    hdfs: &v1alpha1::HdfsCluster,
    image_repository: &str,
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    // Destructured without `..`, so adding a field to [`DereferencedObjects`] fails to
    // compile here instead of silently never being validated.
    let DereferencedObjects {
        hdfs_opa_config,
        namenode_listeners,
        discovery_config_map,
    } = dereferenced_objects;

    let image: product_image_selection::ResolvedProductImage = hdfs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let validated_role_config = |role: HdfsNodeRole| {
        hdfs.role_config(&role).map(
            |GenericRoleConfig {
                 pod_disruption_budget,
             }| ValidatedRoleConfig {
                pdb: pod_disruption_budget.clone(),
            },
        )
    };

    let cluster_name = get_cluster_name(hdfs).context(GetClusterNameSnafu)?;

    // Validated in `HdfsNodeRole` declaration order, because the first role that fails is the
    // error the user sees: reordering these three statements changes which misconfiguration gets
    // reported when more than one role is wrong.
    let journalnode_role_group_configs = validate_role_group_configs(
        hdfs.spec.journal_nodes.as_ref(),
        JournalNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Journal),
    )?;
    let namenode_role_group_configs = validate_role_group_configs(
        hdfs.spec.name_nodes.as_ref(),
        NameNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Name),
    )?;
    let datanode_role_group_configs = validate_role_group_configs(
        hdfs.spec.data_nodes.as_ref(),
        DataNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Data),
    )?;

    let namespace = get_namespace(hdfs).context(GetClusterNamespaceSnafu)?;
    let uid = get_uid(hdfs).context(GetClusterUidSnafu)?;
    let status = ValidatedClusterStatus {
        upgrade_state: hdfs.upgrade_state().context(UpgradeStateSnafu)?,
        deployed_product_version: hdfs
            .status
            .as_ref()
            .and_then(|status| status.deployed_product_version.clone()),
        upgrade_target_product_version: hdfs
            .status
            .as_ref()
            .and_then(|status| status.upgrade_target_product_version.clone()),
    };

    Ok(ValidatedCluster::new(
        cluster_name,
        namespace,
        uid,
        image,
        ValidatedClusterConfig::resolve(hdfs, hdfs_opa_config),
        ValidatedRole {
            role_groups: namenode_role_group_configs,
            config: validated_role_config(HdfsNodeRole::Name),
        },
        ValidatedRole {
            role_groups: datanode_role_group_configs,
            config: validated_role_config(HdfsNodeRole::Data),
        },
        ValidatedRole {
            role_groups: journalnode_role_group_configs,
            config: validated_role_config(HdfsNodeRole::Journal),
        },
        namenode_listeners,
        discovery_config_map,
        status,
    ))
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`], which folds the CRD config fragment (default <-
/// role <- role group) plus the `configOverrides`, `envOverrides`, `cliOverrides`
/// and `podOverrides` (role group wins) into a single
/// [`RoleGroupConfig`].
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<Config, ValidatedConfig>(
    role: Option<&Role<Config, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>>,
    default_config: Config,
) -> Result<
    BTreeMap<
        RoleGroupName,
        RoleGroupConfig<ValidatedConfig, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
    Error,
>
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

            // Flatten the nested config into a single `RoleGroupConfig`; the merged overrides
            // carry over unchanged.
            let validated = RoleGroupConfig {
                replicas: validated.replicas,
                config: validated.config.config,
                config_overrides: validated.config.config_overrides,
                env_overrides: validated.config.env_overrides.into(),
                cli_overrides: validated.config.cli_overrides,
                pod_overrides: validated.config.pod_overrides,
                product_specific_common_config: validated.config.product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role_group: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
        })
        .collect()
}
