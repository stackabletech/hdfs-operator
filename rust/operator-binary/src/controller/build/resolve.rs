//! Resolving one role group's role-specific values, once per role.
//!
//! These types live in their own module so that [`ResolvedRoleGroup`]'s private `_config` field is
//! private to *them*: the shared builders in [`super::container`] and [`super::resource`] are
//! siblings of this module rather than descendants, so [`RoleGroupResolver::resolve`] is the only
//! way any of them can obtain a bundle.

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

/// The log configuration of the two containers every role has, resolved during the build step by
/// code that knows the role, so the shared builders never see a role-specific `Logging<C>`.
///
/// The containers only one role runs carry their log config in [`RoleSpecificValues`], so that
/// "the `zkfc` log config exists exactly when this is a namenode" is one fact, not two that have
/// to agree.
#[derive(Debug)]
pub struct RoleGroupLogging {
    /// The main `hdfs` container, which every role has.
    pub hdfs: ContainerLogConfig,
    /// The Vector sidecar; `None` when the Vector agent is disabled for this role group.
    pub vector: Option<ContainerLogConfig>,
}

/// Everything about one role group that the shared builders below cannot derive themselves: the
/// values resolved from its role-specific config, plus the selector labels, which the build loop
/// already needs for the listener volume and the PVC templates.
///
/// Resolving these in the build loop, which knows the role, is what lets the builders be generic
/// over the role group's config type.
///
/// `C` is the role group's config type, the same one [`RoleGroupResolver`] is implemented on.
/// Every builder takes `RoleGroupConfig<C, ..>` and `ResolvedRoleGroup<C>` together, so one role's
/// overrides and replica count cannot be paired with another role's resolved values: the two
/// parameters are the same `C` or they do not compile. The private `_config` field makes
/// [`RoleGroupResolver::resolve`] the only constructor outside this module — a struct literal
/// elsewhere is rejected with `E0451` — so a `C` can never disagree with the values filled in
/// beside it.
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
    /// Ties this bundle to the role group's config type; see the struct documentation. The
    /// `fn() -> C` spelling marks the relationship without claiming this struct owns a `C`.
    _config: PhantomData<fn() -> C>,
}

/// Everything that exists for one role only: the containers that role runs, their log configs,
/// and its storage and listener arrangements.
///
/// One enum rather than several `Option` fields, so the compiler checks the pairing at every
/// construction site and every consumer is exhaustive. A datanode without its storage
/// configuration does not compile — that would silently drop `dfs.datanode.data.dir` and send the
/// datanodes' blocks to container-local storage. Neither does a namenode with a pod-level
/// listener volume, which would collide with the identically named volume claim template and be
/// rejected at apply time. And neither does a container without its log config, which would leave
/// `log4j.properties` out of both the `ConfigMap` and the `cp` in the container args, so the
/// container logs with Hadoop's built-in defaults and Vector collects nothing for it.
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
/// This is what lets [`build_role`](super::build_role) be written once: the trait supplies the role and the single
/// role-dependent step, and everything else about building a role group is identical across the
/// three roles.
///
/// [`Self::ROLE`] is the single source of truth for the role in the shared builders: they take the
/// role group's config type and read the role from it, rather than taking the role as a second
/// parameter a caller could pair with the wrong config. See [`ResolvedRoleGroup`] for what the
/// shared `C` guarantees.
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
