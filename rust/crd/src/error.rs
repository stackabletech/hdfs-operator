use k8s_openapi::api::core::v1::Pod;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HdfsAddress is missing.")]
    HdfsAddressMissingError,

    #[error("HdfsAddress [{address}] not parseable")]
    HdfsAddressParseError { address: String },

    #[error("HdfsAddress port [{port}] not parseable: {reason}")]
    HdfsAddressPortParseError { port: String, reason: String },

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
}

pub type HdfsOperatorResult<T> = std::result::Result<T, Error>;
