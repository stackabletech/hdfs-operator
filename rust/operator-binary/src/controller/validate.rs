//! The validate step in the HdfsCluster controller.

use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::{fragment::FromFragment, merge::Merge},
    crd::containerdebug,
    role_utils::GenericRoleConfig,
    v2::{
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        role_utils::{JavaCommonConfig, Role, RoleGroupConfig, with_validated_config},
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedClusterStatus,
        ValidatedCommonNodeConfig, ValidatedDataNodeConfig, ValidatedJournalNodeConfig,
        ValidatedNameNodeConfig, ValidatedRoleConfig, dereference::DereferencedObjects,
    },
    crd::{
        CommonNodeConfig, DataNodeConfig, DataNodeConfigFragment, HdfsNodeRole, JournalNodeConfig,
        JournalNodeConfigFragment, NameNodeConfig, NameNodeConfigFragment, UpgradeStateError,
        v1alpha1,
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

    #[snafu(display("invalid containerdebug config for role {role} role group {role_group}", role = role.as_ref()))]
    InvalidContainerdebugConfig {
        source: containerdebug::Error,
        role: HdfsNodeRole,
        role_group: String,
    },
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
            &crate::built_info::PKG_VERSION_SEMVER,
        )
        .context(ResolveProductImageSnafu)?;

    let cluster_name = get_cluster_name(hdfs).context(GetClusterNameSnafu)?;

    // The first failure propagates, so this order decides which misconfiguration the user is told
    // about when more than one role is wrong. It is `HdfsNodeRole`'s variant order.
    let journalnode_role_group_configs = validate_role_group_configs(
        HdfsNodeRole::Journal,
        hdfs.spec.journal_nodes.as_ref(),
        JournalNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Journal),
        validated_journalnode_config,
    )?;
    let namenode_role_group_configs = validate_role_group_configs(
        HdfsNodeRole::Name,
        hdfs.spec.name_nodes.as_ref(),
        NameNodeConfigFragment::default_config(
            cluster_name.as_ref(),
            &HdfsNodeRole::Name,
            hdfs.spec
                .cluster_config
                .authorization
                .as_ref()
                .map(|authorization| &authorization.opa),
        ),
        validated_namenode_config,
    )?;
    let datanode_role_group_configs = validate_role_group_configs(
        HdfsNodeRole::Data,
        hdfs.spec.data_nodes.as_ref(),
        DataNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Data),
        validated_datanode_config,
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

    // The three role-level configs share one type, so naming each field is what stops a role
    // being given another role's PodDisruptionBudget.
    Ok(ValidatedCluster {
        metadata: ValidatedCluster::object_meta(&cluster_name, &namespace, &uid),
        product_version: ValidatedCluster::product_version(&image),
        name: cluster_name,
        namespace,
        uid,
        cluster_config: ValidatedClusterConfig::resolve(hdfs, hdfs_opa_config),
        image,
        namenode_config: validated_role_config(hdfs, HdfsNodeRole::Name),
        namenode_role_group_configs,
        datanode_config: validated_role_config(hdfs, HdfsNodeRole::Data),
        datanode_role_group_configs,
        journalnode_config: validated_role_config(hdfs, HdfsNodeRole::Journal),
        journalnode_role_group_configs,
        namenode_listeners,
        discovery_config_map,
        status,
    })
}

/// The validated role-level config of one role, or `None` if the role is absent from the spec.
///
/// [`GenericRoleConfig`] is destructured without `..`, so a field added to it upstream fails to
/// compile here instead of being silently left unvalidated.
fn validated_role_config(
    hdfs: &v1alpha1::HdfsCluster,
    role: HdfsNodeRole,
) -> Option<ValidatedRoleConfig> {
    hdfs.role_config(&role).map(
        |GenericRoleConfig {
             pod_disruption_budget,
         }| ValidatedRoleConfig {
            pdb: pod_disruption_budget.clone(),
        },
    )
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`], which folds the CRD config fragment (default <-
/// role <- role group) plus the `configOverrides`, `envOverrides`, `cliOverrides`
/// and `podOverrides` (role group wins) into a single
/// [`RoleGroupConfig`]. Its merged config is then turned into the role's validated config by
/// `validated_config`, which is what the build step gets to see.
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<Fragment, Config, ValidatedConfig>(
    hdfs_role: HdfsNodeRole,
    role: Option<
        &Role<Fragment, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    >,
    default_config: Fragment,
    validated_config: impl Fn(Config, HdfsNodeRole, &str) -> Result<ValidatedConfig, Error>,
) -> Result<
    BTreeMap<
        RoleGroupName,
        RoleGroupConfig<ValidatedConfig, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    >,
    Error,
>
where
    Fragment: Clone + Merge,
    Config: FromFragment<Fragment = Fragment>,
{
    let Some(role) = role else {
        return Ok(BTreeMap::new());
    };

    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = with_validated_config::<
                Config,
                JavaCommonConfig,
                Fragment,
                GenericRoleConfig,
                v1alpha1::HdfsConfigOverrides,
            >(role_group, role, &default_config)
            .context(ValidateRoleGroupConfigSnafu)?;

            // The overrides carry over unchanged.
            let validated = RoleGroupConfig {
                replicas: merged.replicas,
                config: validated_config(merged.config.config, hdfs_role, role_group_name)?,
                config_overrides: merged.config.config_overrides,
                env_overrides: merged.config.env_overrides.into(),
                cli_overrides: merged.config.cli_overrides,
                pod_overrides: merged.config.pod_overrides,
                product_specific_common_config: merged.config.product_specific_common_config,
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

/// Turns a namenode role group's merged [`NameNodeConfig`] into its [`ValidatedNameNodeConfig`].
///
/// Destructured without `..`, so a field added to [`NameNodeConfig`] fails to compile here
/// instead of silently never reaching the build step.
fn validated_namenode_config(
    config: NameNodeConfig,
    role: HdfsNodeRole,
    role_group_name: &str,
) -> Result<ValidatedNameNodeConfig, Error> {
    let NameNodeConfig {
        resources,
        logging,
        listener_class,
        common,
    } = config;

    Ok(ValidatedNameNodeConfig {
        resources,
        logging,
        listener_class,
        common: validated_common_config(common, role, role_group_name)?,
    })
}

/// Turns a datanode role group's merged [`DataNodeConfig`] into its [`ValidatedDataNodeConfig`].
///
/// Destructured without `..`, so a field added to [`DataNodeConfig`] fails to compile here
/// instead of silently never reaching the build step.
fn validated_datanode_config(
    config: DataNodeConfig,
    role: HdfsNodeRole,
    role_group_name: &str,
) -> Result<ValidatedDataNodeConfig, Error> {
    let DataNodeConfig {
        resources,
        logging,
        listener_class,
        common,
    } = config;

    Ok(ValidatedDataNodeConfig {
        resources,
        logging,
        listener_class,
        common: validated_common_config(common, role, role_group_name)?,
    })
}

/// Turns a journalnode role group's merged [`JournalNodeConfig`] into its
/// [`ValidatedJournalNodeConfig`].
///
/// Destructured without `..`, so a field added to [`JournalNodeConfig`] fails to compile here
/// instead of silently never reaching the build step.
fn validated_journalnode_config(
    config: JournalNodeConfig,
    role: HdfsNodeRole,
    role_group_name: &str,
) -> Result<ValidatedJournalNodeConfig, Error> {
    let JournalNodeConfig {
        resources,
        logging,
        common,
    } = config;

    Ok(ValidatedJournalNodeConfig {
        resources,
        logging,
        common: validated_common_config(common, role, role_group_name)?,
    })
}

/// Turns the merged [`CommonNodeConfig`] every role shares into its [`ValidatedCommonNodeConfig`].
///
/// The `containerdebug` config is the only part that can fail: an interval of zero would make
/// `containerdebug` collect in a tight loop, so it is rejected here rather than in the build step,
/// where the role group's objects would already be half built.
///
/// Destructured without `..`, so a field added to [`CommonNodeConfig`] fails to compile here
/// instead of silently never reaching the build step.
fn validated_common_config(
    config: CommonNodeConfig,
    role: HdfsNodeRole,
    role_group_name: &str,
) -> Result<ValidatedCommonNodeConfig, Error> {
    let CommonNodeConfig {
        affinity,
        containerdebug,
        graceful_shutdown_timeout,
        requested_secret_lifetime,
    } = config;

    Ok(ValidatedCommonNodeConfig {
        affinity,
        containerdebug: containerdebug
            .validate()
            .context(InvalidContainerdebugConfigSnafu {
                role,
                role_group: role_group_name.to_owned(),
            })?,
        graceful_shutdown_timeout,
        requested_secret_lifetime,
    })
}

#[cfg(test)]
mod tests {
    use crate::test_support::{deserialize_cluster, try_validate_cluster};

    /// The `containerdebug` config is validated per role group, so an invalid one fails the
    /// validate step and with it the reconciliation -- before any Kubernetes object is built,
    /// let alone applied. The error names the role and role group it came from, which the
    /// merged config alone no longer tells the user.
    #[test]
    fn zero_containerdebug_interval_fails_validation() {
        let hdfs = deserialize_cluster(
            r#"
---
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: hdfs
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 3.4.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
  nameNodes:
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    config:
      containerdebug:
        interval: 0s
    roleGroups:
      default:
        replicas: 1
  dataNodes:
    roleGroups:
      default:
        replicas: 1
"#,
        );

        let Err(error) = try_validate_cluster(&hdfs) else {
            panic!("the validation should fail");
        };

        assert!(
            snafu::Report::from_error(&error)
                .to_string()
                .contains("invalid containerdebug config for role journalnode role group default"),
            "{error:?}"
        );
    }
}
