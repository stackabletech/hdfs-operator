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

    #[snafu(display("invalid config for role {role} role group {role_group}", role = role.as_ref()))]
    InvalidConfig {
        source: ConfigError,
        role: HdfsNodeRole,
        role_group: String,
    },
}

/// Why a role group's merged config could not be turned into its validated config.
///
/// Carries no role or role group: the caller adds them (see [`Error::InvalidConfig`]), as only it
/// knows which role group the config was merged for.
#[derive(Snafu, Debug)]
pub enum ConfigError {
    #[snafu(display("invalid containerdebug config"))]
    InvalidContainerdebugConfig { source: containerdebug::Error },
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
    let journalnode_role_group_configs = validate_role_group_configs::<_, JournalNodeConfig>(
        HdfsNodeRole::Journal,
        hdfs.spec.journal_nodes.as_ref(),
        JournalNodeConfigFragment::default_config(cluster_name.as_ref(), &HdfsNodeRole::Journal),
    )?;
    let namenode_role_group_configs = validate_role_group_configs::<_, NameNodeConfig>(
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
    )?;
    let datanode_role_group_configs = validate_role_group_configs::<_, DataNodeConfig>(
        HdfsNodeRole::Data,
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

/// The validated role groups of one role, keyed by role group name.
type ValidatedRoleGroupConfigs<Config> = BTreeMap<
    RoleGroupName,
    RoleGroupConfig<Config, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
>;

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`], which folds the CRD config fragment (default <-
/// role <- role group) plus the `configOverrides`, `envOverrides`, `cliOverrides`
/// and `podOverrides` (role group wins) into a single
/// [`RoleGroupConfig`]. Its merged config is then turned into the role's validated config by
/// [`IntoValidated::validate`], which is what the build step gets to see.
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<Fragment, Config>(
    hdfs_role: HdfsNodeRole,
    role: Option<
        &Role<Fragment, v1alpha1::HdfsConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    >,
    default_config: Fragment,
) -> Result<ValidatedRoleGroupConfigs<Config::Validated>, Error>
where
    Fragment: Clone + Merge,
    Config: FromFragment<Fragment = Fragment> + IntoValidated,
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
            let validated =
                RoleGroupConfig {
                    replicas: merged.replicas,
                    config: merged.config.config.validate().with_context(|_| {
                        InvalidConfigSnafu {
                            role: hdfs_role,
                            role_group: role_group_name.clone(),
                        }
                    })?,
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

/// Turns a role group's merged config into the validated config the build step works with.
///
/// Every impl destructures the merged config without `..`, so a field added to the CRD struct
/// fails to compile there instead of silently never reaching the build step.
// FIXME: `IntoValidated` should be a trait coming from operator-rs.
trait IntoValidated {
    /// The validated config the build step gets to see.
    type Validated;
    type Error;

    /// Consumes the merged config, as nothing needs it once it is validated.
    fn validate(self) -> Result<Self::Validated, Self::Error>;
}

impl IntoValidated for NameNodeConfig {
    type Validated = ValidatedNameNodeConfig;
    type Error = ConfigError;

    fn validate(self) -> Result<Self::Validated, ConfigError> {
        let NameNodeConfig {
            resources,
            logging,
            listener_class,
            common,
        } = self;

        Ok(ValidatedNameNodeConfig {
            resources,
            logging,
            listener_class,
            common: common.validate()?,
        })
    }
}

impl IntoValidated for DataNodeConfig {
    type Validated = ValidatedDataNodeConfig;
    type Error = ConfigError;

    fn validate(self) -> Result<Self::Validated, ConfigError> {
        let DataNodeConfig {
            resources,
            logging,
            listener_class,
            common,
        } = self;

        Ok(ValidatedDataNodeConfig {
            resources,
            logging,
            listener_class,
            common: common.validate()?,
        })
    }
}

impl IntoValidated for JournalNodeConfig {
    type Validated = ValidatedJournalNodeConfig;
    type Error = ConfigError;

    fn validate(self) -> Result<Self::Validated, ConfigError> {
        let JournalNodeConfig {
            resources,
            logging,
            common,
        } = self;

        Ok(ValidatedJournalNodeConfig {
            resources,
            logging,
            common: common.validate()?,
        })
    }
}

/// The settings every role shares.
///
/// The `containerdebug` config is the only part that can fail: an interval of zero would make
/// `containerdebug` collect in a tight loop. Rejecting it here, rather than in the build step,
/// means the build step can only ever be handed a validated `containerdebug` config.
impl IntoValidated for CommonNodeConfig {
    type Validated = ValidatedCommonNodeConfig;
    type Error = ConfigError;

    fn validate(self) -> Result<Self::Validated, ConfigError> {
        let CommonNodeConfig {
            affinity,
            containerdebug,
            graceful_shutdown_timeout,
            requested_secret_lifetime,
        } = self;

        Ok(ValidatedCommonNodeConfig {
            affinity,
            containerdebug: containerdebug
                .validate()
                .context(InvalidContainerdebugConfigSnafu)?,
            graceful_shutdown_timeout,
            requested_secret_lifetime,
        })
    }
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

        let chain: Vec<String> =
            std::iter::successors(Some(&error as &dyn std::error::Error), |error| {
                error.source()
            })
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            chain,
            [
                "invalid config for role journalnode role group default",
                "invalid containerdebug config",
                "the containerdebug interval must not be zero",
            ]
        );
    }
}
