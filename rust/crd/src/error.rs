use crate::HdfsCluster;
use stackable_operator::config::fragment::ValidationError;
use stackable_operator::{
    kube::runtime::reflector::ObjectRef, logging::controller::ReconcilerError,
};
use std::str::FromStr;
use strum::EnumDiscriminants;

#[derive(Debug, thiserror::Error, EnumDiscriminants)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
pub enum Error {
    #[error("Object has no version")]
    ObjectHasNoVersion { obj_ref: ObjectRef<HdfsCluster> },
    #[error("Missing node role {role}")]
    MissingNodeRole { role: String },
    #[error("Invalid role configuration: {source}")]
    InvalidRoleConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[error("Invalid product configuration: {source}")]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },

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

    #[error("Cannot create discovery config map {name}. Caused by: {source}")]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("No metadata for [{obj_ref}]. Caused by: {source}")]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        obj_ref: ObjectRef<HdfsCluster>,
    },

    #[error("Cannot parse address port [{address}], Caused by: {source}")]
    HdfsAddressPortParseError {
        source: <i32 as FromStr>::Err,
        address: String,
    },

    #[error("Cannot parse port [{address}]")]
    HdfsAddressParseError { address: String },

    #[error("Cannot find role [{role}]")]
    RoleNotFound { role: String },

    #[error("Cannot find rolegroup [{rolegroup}]")]
    RoleGroupNotFound { rolegroup: String },

    #[error("Object has no name")]
    ObjectHasNoName,
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

    #[error("Cannot build config discovery config map. Caused by: {source}")]
    BuildDiscoveryConfigMap {
        source: stackable_operator::error::Error,
    },

    #[error("Pod has no name")]
    PodHasNoName,
    #[error("Pod [{name}] has no uid")]
    PodHasNoUid { name: String },
    #[error("Pod [{name}] has no labels")]
    PodHasNoLabels { name: String },
    #[error("Pod [{name}] has no [role] label")]
    PodHasNoRoleLabel { name: String },
    #[error("Pod [{name}] has no spec")]
    PodHasNoSpec { name: String },
    #[error("Pod [{name}] has no container named [{role}]")]
    PodHasNoContainer { name: String, role: String },
    #[error("Failed to build ownerreference of pod [{name}]")]
    PodOwnerReference {
        source: stackable_operator::error::Error,
        name: String,
    },

    #[error("Object has no associated namespace.")]
    NoNamespaceContext,

    #[error("Group [{group}] of role [{role}] not in validated config.")]
    RolegroupNotInValidatedConfig { group: String, role: String },

    #[error("Failed to publish event")]
    PublishEvent {
        source: stackable_operator::kube::Error,
    },

    #[error("Name node Java heap config: {source}")]
    NamenodeJavaHeapConfig {
        source: stackable_operator::error::Error,
    },

    #[error("Data node Java heap config: {source}")]
    DatanodeJavaHeapConfig {
        source: stackable_operator::error::Error,
    },

    #[error("Journal node Java heap config: {source}")]
    JournalnodeJavaHeapConfig {
        source: stackable_operator::error::Error,
    },

    #[error("failed to patch service account: {source}")]
    ApplyServiceAccount {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[error("failed to patch role binding: {source}")]
    ApplyRoleBinding {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[error("failed to create cluster resources")]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[error("failed to delete orphaned resources")]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[error("fragment validation failure")]
    FragmentValidationFailure { source: ValidationError },
}
pub type HdfsOperatorResult<T> = std::result::Result<T, Error>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
