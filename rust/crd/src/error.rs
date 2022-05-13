use crate::HdfsCluster;
use stackable_operator::{
    kube::runtime::reflector::ObjectRef, logging::controller::ReconcilerError,
};
use std::str::FromStr;
use strum::EnumDiscriminants;

#[derive(Debug, thiserror::Error, EnumDiscriminants)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
pub enum Error {
    #[error("object has no version")]
    ObjectHasNoVersion { obj_ref: ObjectRef<HdfsCluster> },
    #[error("missing node role {role}")]
    MissingNodeRole { role: String },
    #[error("invalid role configuration")]
    InvalidRoleConfig {
        #[source]
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[error("invalid product configuration")]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },

    #[error("cannot create rolegroup service {name}")]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create namenode service {name}")]
    ApplyNameNodeService {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create pod service {name}")]
    ApplyPodServiceFailed {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create role service account {name}")]
    ApplyRoleServiceAccount {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot apply rolebinding {name} to role service account")]
    ApplyRoleRoleBinding {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create role group config map {name}")]
    ApplyRoleGroupConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create role group stateful set {name}")]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("cannot create discovery config map {name}")]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("no metadata for {obj_ref}")]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        obj_ref: ObjectRef<HdfsCluster>,
    },

    #[error("cannot parse address port {address:?}")]
    HdfsAddressPortParseError {
        source: <i32 as FromStr>::Err,
        address: String,
    },

    #[error("cannot parse port {address:?}")]
    HdfsAddressParseError { address: String },

    #[error("cannot find rolegroup {rolegroup}")]
    RoleGroupNotFound { rolegroup: String },

    #[error("object {obj_ref} has no namespace")]
    ObjectHasNoNamespace { obj_ref: ObjectRef<HdfsCluster> },

    #[error("cannot build config for role {role} and rolegroup {role_group}")]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        role: String,
        role_group: String,
    },

    #[error("cannot build config discovery config map {name}")]
    BuildDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("Pod has no name")]
    PodHasNoName,
    #[error("Pod {name} has no uid")]
    PodHasNoUid { name: String },
    #[error("Pod {name} has no labels")]
    PodHasNoLabels { name: String },
    #[error("Pod {name} has no [role] label")]
    PodHasNoRoleLabel { name: String },
    #[error("Pod {name} has no spec")]
    PodHasNoSpec { name: String },
    #[error("Pod {name} has no container named {role}")]
    PodHasNoContainer { name: String, role: String },

    #[error("object has no associated namespace.")]
    NoNamespaceContext,

    #[error("group {group} of role {role} not in validated config.")]
    RolegroupNotInValidatedConfig { group: String, role: String },

    #[error("failed to publish event")]
    PublishEvent {
        source: stackable_operator::kube::Error,
    },
}
pub type HdfsOperatorResult<T> = std::result::Result<T, Error>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
