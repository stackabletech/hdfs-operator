use crate::HdfsCluster;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object has no version")]
    ObjectHasNoVersion { obj_ref: ObjectRef<HdfsCluster> },
    #[error("no namenode role defined")]
    NoNameNodeRole,
    #[error("no datanode role defined")]
    NoDataNodeRole,
    #[error("no journalnode role defined")]
    NoJournalNodeRole,
    #[error("invalid role configuration: {source}")]
    InvalidRoleConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[error("invalid product configuration: {source}")]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[error("no service name")]
    GlobalServiceNameNotFound,

    #[error("Cannot create rolegroup service {name}. Caused by: {source}")]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("Cannot create pod service [{name}]. Caused by: {source}")]
    ApplyPodServiceFailed {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("Cannot create role group config map {name}. Caused by: {source}")]
    ApplyRoleGroupConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("Cannot create role group stateful set {name}. Caused by: {source}")]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("No metadata for [{obj_ref}]. Caused by: {source}")]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        obj_ref: ObjectRef<HdfsCluster>,
    },

    #[error("HdfsAddress is missing.")]
    HdfsAddressMissingError,

    #[error("Cannot parse address port [{address}], Caused by: {source}")]
    HdfsAddressPortParseError {
        source: <i32 as FromStr>::Err,
        address: String,
    },

    #[error("Cannot parse port [{address}]")]
    HdfsAddressParseError { address: String },

    #[error("Cannot find rolegroup [{rolegroup}]")]
    RoleGroupNotFound { rolegroup: String },

    #[error("Object has no namespace [{obj_ref}]")]
    ObjectHasNoNamespace { obj_ref: ObjectRef<HdfsCluster> },

    #[error(
        "Cannot build config for role [{role}] and rolegroup [{role_group}]. Caused by: {source}"
    )]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        role: String,
        role_group: String,
    },

    #[error("Pod has no name")]
    PodHasNoName,
    #[error("Pod [{name}] has no uid")]
    PodHasNoUid { name: String, },
    #[error("Pod [{name}] has no labels")]
    PodHasNoLabels { name: String, },
    #[error("Pod [{name}] has no [role] label")]
    PodHasNoRoleLabel { name: String, },
    #[error("Pod [{name}] has no spec")]
    PodHasNoSpec { name: String, },
    #[error("Pod [{name}] has no container named [{role}]")]
    PodHasNoContainer { name: String, role: String, },

    #[error("Container [{name}] of pod [{pod}] has no ports.")]
    ContainerHasNoPorts { name: String, pod: String,  },
}

pub type HdfsOperatorResult<T> = std::result::Result<T, Error>;
