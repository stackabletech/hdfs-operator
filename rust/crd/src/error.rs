use crate::HdfsCluster;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
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

    /*
    #[error("No pods are found for Hdfs cluster [{namespace}/{name}]. Please check the Hdfs custom resource and Hdfs Operator for errors.")]
    NoHdfsPodsAvailableForConnectionInfo { namespace: String, name: String },

    #[error(
        "No [{ipc_port}] container port found in: [{pods:?}]. Maybe no name_node up and running yet?"
    )]
    NoIpcContainerPortFound { ipc_port: String, pods: Vec<Pod> },

    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Got object with no name from Kubernetes, this should not happen, please open a ticket for this with the reference: [{reference}]")]
    ObjectWithoutName { reference: String },

    #[error("Operator Framework reported error: {source}")]
    OperatorFrameworkError {
        #[from]
        source: stackable_operator::error::Error,
    },

    #[error("Pod [{pod}] is missing the following required labels: [{labels:?}]")]
    PodMissingLabels { pod: String, labels: Vec<String> },

    #[error("Pod has no hostname assignment, this is most probably a transitive failure and should be retried: [{pod}]")]
    PodWithoutHostname { pod: String },
    */
}

pub type HdfsOperatorResult<T> = std::result::Result<T, Error>;
