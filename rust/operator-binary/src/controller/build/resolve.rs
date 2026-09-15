//! Resolving one role group into the values the shared builders cannot derive themselves.
//!
//! One [`RoleGroupResolver`] impl per role config type, so a role's resolution is written once.

use std::{fmt::Display, marker::PhantomData};

use snafu::ResultExt;
use stackable_operator::{
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, ResourceRequirements, Volume},
    kvp::Labels,
    product_logging::spec::{ContainerLogConfig, Logging},
    v2::types::operator::RoleGroupName,
};

use super::{Error, ListenerVolumeSnafu, VolumeClaimTemplatesSnafu, container::ContainerConfig};
use crate::crd::{
    CommonNodeConfig, DataNodeConfig, DataNodeContainer, HdfsNodeRole, JournalNodeConfig,
    JournalNodeContainer, NameNodeConfig, NameNodeContainer,
    storage::DataNodeStorageConfigInnerType,
};

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
/// Every builder takes `RoleGroupConfig<C, ..>` and `ResolvedRoleGroup<C>` together, so one role's
/// overrides and replica count cannot be paired with another role's resolved values: both are the
/// same `C` or they do not compile.
pub struct ResolvedRoleGroup<C> {
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
    /// Ties the bundle to its config type. Private, so [`RoleGroupResolver::resolve`] is the only
    /// constructor outside this module — a struct literal elsewhere is `E0451`. `fn() -> C` rather
    /// than `C`, so the bundle does not read as owning one.
    _config: PhantomData<fn() -> C>,
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
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup<Self>, Error>;
}

impl RoleGroupResolver for JournalNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Journal;

    fn resolve(
        &self,
        _role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup<Self>, Error> {
        let (hdfs, vector) = common_container_logging(
            &self.logging,
            JournalNodeContainer::Hdfs,
            JournalNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates: ContainerConfig::journalnode_volume_claim_templates(self),
            role: RoleSpecificValues::Journal,
            logging: RoleGroupLogging { hdfs, vector },
            _config: PhantomData,
        })
    }
}

impl RoleGroupResolver for NameNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Name;

    fn resolve(
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup<Self>, Error> {
        // Namenodes get their listener from a persistent volume claim template, for stable
        // per-pod identity, rather than from an ephemeral volume.
        let volume_claim_templates =
            ContainerConfig::namenode_volume_claim_templates(self, &selector_labels).context(
                VolumeClaimTemplatesSnafu {
                    role: Self::ROLE,
                    role_group: role_group_name.clone(),
                },
            )?;

        let (hdfs, vector) = common_container_logging(
            &self.logging,
            NameNodeContainer::Hdfs,
            NameNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates,
            role: RoleSpecificValues::Name {
                zkfc: self
                    .logging
                    .for_container(&NameNodeContainer::Zkfc)
                    .into_owned(),
                format_namenodes: self
                    .logging
                    .for_container(&NameNodeContainer::FormatNameNodes)
                    .into_owned(),
                format_zookeeper: self
                    .logging
                    .for_container(&NameNodeContainer::FormatZooKeeper)
                    .into_owned(),
            },
            logging: RoleGroupLogging { hdfs, vector },
            _config: PhantomData,
        })
    }
}

impl RoleGroupResolver for DataNodeConfig {
    const ROLE: HdfsNodeRole = HdfsNodeRole::Data;

    fn resolve(
        &self,
        role_group_name: &RoleGroupName,
        selector_labels: Labels,
    ) -> Result<ResolvedRoleGroup<Self>, Error> {
        // Datanodes use an ephemeral listener volume, since they need no stable per-pod identity.
        let listener_volume = ContainerConfig::datanode_listener_volume(self, &selector_labels)
            .context(ListenerVolumeSnafu {
                role: Self::ROLE,
                role_group: role_group_name.clone(),
            })?;

        let (hdfs, vector) = common_container_logging(
            &self.logging,
            DataNodeContainer::Hdfs,
            DataNodeContainer::Vector,
        );

        Ok(ResolvedRoleGroup {
            selector_labels,
            common: self.common.clone(),
            resources: self.resources.clone().into(),
            volume_claim_templates: ContainerConfig::datanode_volume_claim_templates(self),
            role: RoleSpecificValues::Data {
                listener_volume,
                storage: self.resources.storage.clone(),
                wait_for_namenodes: self
                    .logging
                    .for_container(&DataNodeContainer::WaitForNameNodes)
                    .into_owned(),
            },
            logging: RoleGroupLogging { hdfs, vector },
            _config: PhantomData,
        })
    }
}
