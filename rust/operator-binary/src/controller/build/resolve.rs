//! Resolving one role group into everything the shared builders need.
//!
//! One [`RoleGroupResolver`] impl per role config type, so a role's resolution is written once.
//! [`RoleGroupResolver::resolve`] takes the whole [`RoleGroupConfig`] and returns a
//! [`ResolvedRoleGroup`] that no longer mentions the config type, so the builders take one
//! non-generic argument and read the role back out of it.

use std::fmt::Display;

use snafu::ResultExt;
use stackable_operator::{
    k8s_openapi::api::core::v1::{
        PersistentVolumeClaim, PodTemplateSpec, ResourceRequirements, Volume,
    },
    kvp::Labels,
    product_logging::spec::{ContainerLogConfig, Logging},
    v2::{
        builder::pod::container::EnvVarSet,
        jvm_argument_overrides::JvmArgumentOverrides,
        role_utils::{JavaCommonConfig, RoleGroupConfig},
        types::operator::RoleGroupName,
    },
};

use super::{Error, ListenerVolumeSnafu, VolumeClaimTemplatesSnafu, container::ContainerConfig};
use crate::crd::{
    CommonNodeConfig, DataNodeConfig, DataNodeContainer, HdfsNodeRole, JournalNodeConfig,
    JournalNodeContainer, NameNodeConfig, NameNodeContainer,
    storage::DataNodeStorageConfigInnerType, v1alpha1,
};

/// The role group's merged values that the builders use verbatim: its replica count and the
/// override sets. Nothing here depends on the role, which is why it is carried alongside the
/// resolved values rather than among them.
///
/// `cli_overrides` is deliberately absent: it is merged during validation but no builder reads it.
pub struct MergedRoleGroupConfig {
    pub replicas: Option<u16>,
    pub config_overrides: v1alpha1::HdfsConfigOverrides,
    pub env_overrides: EnvVarSet,
    pub pod_overrides: PodTemplateSpec,
    pub jvm_argument_overrides: JvmArgumentOverrides,
}

impl MergedRoleGroupConfig {
    fn of<C>(
        rg_config: &RoleGroupConfig<C, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
    ) -> Self {
        Self {
            replicas: rg_config.replicas,
            config_overrides: rg_config.config_overrides.clone(),
            env_overrides: rg_config.env_overrides.clone(),
            pod_overrides: rg_config.pod_overrides.clone(),
            jvm_argument_overrides: rg_config
                .product_specific_common_config
                .jvm_argument_overrides
                .clone(),
        }
    }
}

/// The log config of the two containers every role has. Containers only one role runs carry theirs
/// in [`RoleSpecificValues`], which is the single place the role is decided.
#[derive(Debug)]
pub struct RoleGroupLogging {
    /// The main `hdfs` container, which every role has.
    pub hdfs: ContainerLogConfig,
    /// The Vector sidecar; `None` when the Vector agent is disabled for this role group.
    pub vector: Option<ContainerLogConfig>,
}

/// The values the shared builders cannot derive themselves, resolved by
/// [`RoleGroupResolver::resolve`], which knows the role.
///
/// The builders take this and nothing else about the role group, so there is no second argument to
/// pair with the wrong one. The role comes from [`RoleSpecificValues::node_role`].
pub struct ResolvedRoleGroup {
    /// The selector labels of the role group's pods, also used as the `StatefulSet` selector and
    /// on its listener volume.
    ///
    /// We must use the selector labels and not the recommended labels for the listener volumes.
    /// This is because the recommended set contains a "managed-by" label. That label triggers the
    /// cluster resources to "manage" listeners, which is wrong and leads to errors. The listeners
    /// are managed by the listener-operator.
    pub selector_labels: Labels,
    /// The role group's merged config that is common to every role.
    pub common: CommonNodeConfig,
    /// The resource requirements of the role group's main and init containers; the ZKFC sidecar
    /// has fixed requirements of its own and ignores this.
    pub resources: ResourceRequirements,
    /// The `StatefulSet`'s persistent volume claim templates.
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// The values that exist for this role only.
    pub role: RoleSpecificValues,
    /// The log config of each of the role group's containers.
    pub logging: RoleGroupLogging,
    /// The role group's replica count and overrides, carried through unchanged.
    pub merged: MergedRoleGroupConfig,
}

/// Everything that exists for one role only: the containers that role runs, their log configs, and
/// its storage and listener arrangements.
///
/// An enum rather than `Option` fields, so consumers are exhaustive and three silent failures do
/// not compile: a datanode without its storage drops `dfs.datanode.data.dir` and sends its blocks
/// to container-local storage; a namenode with a pod-level listener volume collides with the
/// identically named claim template and is rejected at apply time; a container without its log
/// config falls back to Hadoop's built-in logging, uncollected by Vector.
pub enum RoleSpecificValues {
    /// Journalnodes run no role-specific container, have no listener and no role-specific
    /// storage configuration.
    Journal,
    /// Namenodes run the `zkfc` side container and the `format-namenodes` and `format-zookeeper`
    /// init containers. They get their listener from a volume claim template in
    /// [`ResolvedRoleGroup::volume_claim_templates`], for stable per-pod identity, so they have
    /// no pod-level listener volume.
    Name {
        zkfc: ContainerLogConfig,
        format_namenodes: ContainerLogConfig,
        format_zookeeper: ContainerLogConfig,
    },
    /// Datanodes run the `wait-for-namenodes` init container. They need no stable per-pod
    /// identity, so their listener is an ephemeral pod volume, and they are the only role that
    /// configures `dfs.datanode.data.dir`.
    Data {
        listener_volume: Volume,
        storage: DataNodeStorageConfigInnerType,
        wait_for_namenodes: ContainerLogConfig,
    },
}

impl RoleSpecificValues {
    /// The role these values belong to.
    pub fn node_role(&self) -> HdfsNodeRole {
        match self {
            Self::Journal => HdfsNodeRole::Journal,
            Self::Name { .. } => HdfsNodeRole::Name,
            Self::Data { .. } => HdfsNodeRole::Data,
        }
    }

    /// The role group's ephemeral listener volume; only datanodes have one.
    pub fn listener_volume(&self) -> Option<&Volume> {
        match self {
            Self::Data {
                listener_volume, ..
            } => Some(listener_volume),
            Self::Journal | Self::Name { .. } => None,
        }
    }

    /// The datanode data volume configuration, which drives `dfs.datanode.data.dir`; `None` for
    /// the other roles.
    pub fn datanode_storage(&self) -> Option<&DataNodeStorageConfigInnerType> {
        match self {
            Self::Data { storage, .. } => Some(storage),
            Self::Journal | Self::Name { .. } => None,
        }
    }
}

/// The log config of the two containers every role has: the main `hdfs` container, and the Vector
/// sidecar, which is `None` when the Vector agent is disabled for the role group.
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

/// How to resolve one role group's role-specific values, implemented once per role config type.
///
/// The trait supplies the role and the single role-dependent step, which is what lets
/// [`build_role`](super::build_role) be written once. The shared builders read [`Self::ROLE`]
/// instead of taking a role parameter a caller could pair with the wrong config.
pub(crate) trait RoleGroupResolver: Sized {
    /// The role whose config this is.
    const ROLE: HdfsNodeRole;

    /// Resolves everything the shared builders cannot derive themselves. Takes the selector
    /// labels because two of the three roles need them to build their listener.
    fn resolve(
        rg_config: &RoleGroupConfig<Self, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error>;
}

impl RoleGroupResolver for JournalNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Journal;

    fn resolve(
        rg_config: &RoleGroupConfig<Self, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
        _role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        let config = &rg_config.config;
        let (hdfs, vector) = common_container_logging(
            &config.logging,
            JournalNodeContainer::Hdfs,
            JournalNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(config),
            role: RoleSpecificValues::Journal,
            logging: RoleGroupLogging { hdfs, vector },
            merged: MergedRoleGroupConfig::of(rg_config),
        })
    }
}

impl RoleGroupResolver for NameNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Name;

    fn resolve(
        rg_config: &RoleGroupConfig<Self, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        let config = &rg_config.config;
        // Namenodes get their listener from a persistent volume claim template, for stable
        // per-pod identity, rather than from an ephemeral volume.
        let volume_claim_templates =
            ContainerConfig::namenode_volume_claim_templates(config, &selector_labels).context(
                VolumeClaimTemplatesSnafu {
                    role: Self::ROLE,
                    role_group: role_group_name.clone(),
                },
            )?;

        let (hdfs, vector) = common_container_logging(
            &config.logging,
            NameNodeContainer::Hdfs,
            NameNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates,
            role: RoleSpecificValues::Name {
                zkfc: config
                    .logging
                    .for_container(&NameNodeContainer::Zkfc)
                    .into_owned(),
                format_namenodes: config
                    .logging
                    .for_container(&NameNodeContainer::FormatNameNodes)
                    .into_owned(),
                format_zookeeper: config
                    .logging
                    .for_container(&NameNodeContainer::FormatZooKeeper)
                    .into_owned(),
            },
            logging: RoleGroupLogging { hdfs, vector },
            merged: MergedRoleGroupConfig::of(rg_config),
        })
    }
}

impl RoleGroupResolver for DataNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Data;

    fn resolve(
        rg_config: &RoleGroupConfig<Self, JavaCommonConfig, v1alpha1::HdfsConfigOverrides>,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup, Error> {
        let config = &rg_config.config;
        // Datanodes use an ephemeral listener volume, since they need no stable per-pod identity.
        let listener_volume = ContainerConfig::datanode_listener_volume(config, &selector_labels)
            .context(ListenerVolumeSnafu {
            role: Self::ROLE,
            role_group: role_group_name.clone(),
        })?;

        let (hdfs, vector) = common_container_logging(
            &config.logging,
            DataNodeContainer::Hdfs,
            DataNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: config.common.clone(),
            resources: config.resources.clone().into(),
            volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(config),
            role: RoleSpecificValues::Data {
                listener_volume,
                storage: config.resources.storage.clone(),
                wait_for_namenodes: config
                    .logging
                    .for_container(&DataNodeContainer::WaitForNameNodes)
                    .into_owned(),
            },
            logging: RoleGroupLogging { hdfs, vector },
            merged: MergedRoleGroupConfig::of(rg_config),
        })
    }
}
