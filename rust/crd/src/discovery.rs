use crate::error::Error::{
    NoHdfsPodsAvailableForConnectionInfo, ObjectWithoutName, OperatorFrameworkError,
    PodWithoutHostname,
};
use crate::error::HdfsOperatorResult;
use crate::{HdfsCluster, HdfsRole, APP_NAME, IPC_PORT, MANAGED_BY};

use crate::discovery::TicketReferences::ErrHdfsPodWithoutName;
use serde::{Deserialize, Serialize};
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use stackable_operator::labels::{
    APP_COMPONENT_LABEL, APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL,
};
use stackable_operator::schemars::{self, JsonSchema};
use std::collections::BTreeMap;
use strum_macros::Display;
use tracing::{debug, warn};

#[derive(Display)]
pub enum TicketReferences {
    ErrHdfsPodWithoutName,
}

/// Contains all necessary information to identify a Stackable managed HDFS
/// cluster and build a connection string for it.
/// The main purpose for this struct is for other operators that need to reference a
/// HDFS ensemble to use in their CRDs.
/// This has the benefit of keeping references to HDFS ensembles consistent
/// throughout the entire stack.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
pub struct HdfsReference {
    pub namespace: String,
    pub name: String,
    pub root: Option<String>,
}

/// Contains all necessary information to establish a connection with a HDFS cluster.
/// Other operators using this crate will interact with this struct.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
pub struct HdfsConnectionInformation {
    pub node_name: String,
    pub port: String,
    pub root: Option<String>,
}

impl HdfsConnectionInformation {
    /// Returns a full qualified connection string for a name node as defined by HDFS.
    /// This has the form `hdfs://host:port[/root]`
    /// For example:
    ///  - hdfs://server1:9000/hbase
    pub fn full_connection_string(&self) -> String {
        let con = self.connection_string();
        if let Some(root) = &self.root {
            return format!("{}{}", con, root);
        }
        con
    }

    /// Returns a connection string (without root) for a name node as defined by HDFS.
    /// This has the form `hdfs://host:port`
    /// For example:
    ///  - hdfs://server1:9000
    pub fn connection_string(&self) -> String {
        format!("hdfs://{}:{}", self.node_name, self.port)
    }
}

/// Returns connection information for a HDFS Cluster custom resource.
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `hdfs_reference` - The HdfsReference in the custom resource
///
pub async fn get_hdfs_connection_info(
    client: &Client,
    hdfs_reference: &HdfsReference,
) -> HdfsOperatorResult<Option<HdfsConnectionInformation>> {
    check_hdfs_reference(client, &hdfs_reference.name, &hdfs_reference.namespace).await?;

    let hdfs_pods = client
        .list_with_label_selector(None, &get_match_labels(&hdfs_reference.name))
        .await?;

    // No HDFS pods means empty connect string. We throw an error indicating to check the
    // HDFS custom resource or the HDFS operator for errors.
    if hdfs_pods.is_empty() {
        return Err(NoHdfsPodsAvailableForConnectionInfo {
            namespace: hdfs_reference.namespace.clone(),
            name: hdfs_reference.name.clone(),
        });
    }

    get_hdfs_connection_string_from_pods(&hdfs_pods, hdfs_reference.root.as_deref())
}

/// Builds the actual connection string after all necessary information has been retrieved.
/// Takes a list of pods belonging to this cluster from which the hostnames are retrieved.
/// Checks the 'IPC' container port instead of the cluster spec to retrieve the correct port.
///
/// WARNING: For now this only works with one name_node.
///
/// # Arguments
///
/// * `hdfs_pods` - All pods belonging to the cluster
/// * `root` - The additional root (e.g. "/hbase") appended to the connection string
///
pub fn get_hdfs_connection_string_from_pods(
    hdfs_pods: &[Pod],
    root: Option<&str>,
) -> HdfsOperatorResult<Option<HdfsConnectionInformation>> {
    let name_node_str = &HdfsRole::NameNode.to_string();
    let clean_chroot = pad_and_check_chroot(root)?;

    // filter for name nodes
    let filtered_pods: Vec<&Pod> = hdfs_pods
        .iter()
        .filter(|pod| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(APP_COMPONENT_LABEL))
                == Some(name_node_str)
        })
        .collect();

    if filtered_pods.len() > 1 {
        warn!("Retrieved more than one name_node pod. This is not supported and may lead to untested side effects. \
           Please specify only one name_node in the custom resource via 'replicas=1'.");
    }

    for pod in &filtered_pods {
        let pod_name = match &pod.metadata.name {
            None => {
                return Err(ObjectWithoutName {
                    reference: ErrHdfsPodWithoutName.to_string(),
                })
            }
            Some(pod_name) => pod_name.clone(),
        };

        let node_name = match pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) {
            None => {
                debug!("Pod [{:?}] is does not have node_name set, might not be scheduled yet, aborting.. ",
                       pod_name);
                return Err(PodWithoutHostname { pod: pod_name });
            }
            Some(node_name) => node_name,
        };

        if let Some(port) = extract_container_port(pod, APP_NAME, IPC_PORT) {
            return Ok(Some(HdfsConnectionInformation {
                node_name,
                port,
                root: clean_chroot,
            }));
        }
    }

    Ok(None)
}

/// Build a Labelselector that applies only to name node pods belonging to the cluster instance
/// referenced by `name`.
///
/// # Arguments
///
/// * `name` - The name of the HDFS cluster
///
fn get_match_labels(name: &str) -> LabelSelector {
    let mut match_labels = BTreeMap::new();
    match_labels.insert(String::from(APP_NAME_LABEL), String::from(APP_NAME));
    match_labels.insert(String::from(APP_MANAGED_BY_LABEL), String::from(MANAGED_BY));
    match_labels.insert(String::from(APP_INSTANCE_LABEL), name.to_string());
    match_labels.insert(
        String::from(APP_COMPONENT_LABEL),
        HdfsRole::NameNode.to_string(),
    );

    LabelSelector {
        match_labels: Some(match_labels),
        ..Default::default()
    }
}

/// Check in kubernetes, whether the HDFS object referenced by `hdfs_name` and `hdfs_namespace`
/// exists. If it exists the object will be returned.
///
/// # Arguments
///
/// * `client` - A [`stackable_operator::client::Client`] used to access the Kubernetes cluster
/// * `hdfs_name` - The name of HDFS cluster
/// * `hdfs_namespace` - The namespace of the HDFS cluster
///
async fn check_hdfs_reference(
    client: &Client,
    hdfs_name: &str,
    hdfs_namespace: &str,
) -> HdfsOperatorResult<HdfsCluster> {
    debug!(
        "Checking HdfsReference if [{}] exists in namespace [{}].",
        hdfs_name, hdfs_namespace
    );
    let hdfs_cluster: OperatorResult<HdfsCluster> =
        client.get(hdfs_name, Some(hdfs_namespace)).await;

    hdfs_cluster.map_err(|err| {
        warn!(?err, "Referencing a HDFS cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                hdfs_namespace,
                hdfs_name
        );
        OperatorFrameworkError {source: err}
    })
}

/// Left pads the chroot string with a / if necessary - mostly for convenience, so users do not
/// need to specify the / when entering the chroot string in their config.
/// Checks if the result is a valid HDFS path.
///
/// # Arguments
///
/// * `root` - The root (e.g 'hbase' or '/hbase')
///
fn pad_and_check_chroot(root: Option<&str>) -> HdfsOperatorResult<Option<String>> {
    // Left pad the root with a / if needed
    // Sadly this requires copying the reference once,
    // but I know of no way to avoid that
    let padded_root = match root {
        None => return Ok(None),
        Some(root) => {
            if root.starts_with('/') {
                root.to_string()
            } else {
                format!("/{}", root)
            }
        }
    };
    Ok(Some(padded_root))
}

/// Extract the container port `port_name` from a container with name `container_name`.
/// Returns None if not the port or container are not available.
///
/// # Arguments
///
/// * `pod` - The pod to extract the container port from
/// * `container_name` - The name of the container to search for.
/// * `port_name` - The name of the container port.
///
fn extract_container_port(pod: &Pod, container_name: &str, port_name: &str) -> Option<String> {
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if container.name != container_name {
                continue;
            }

            if let Some(port) = container.ports.as_ref().and_then(|ports| {
                ports
                    .iter()
                    .find(|port| port.name == Some(port_name.to_string()))
            }) {
                return Some(port.container_port.to_string());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use rstest::rstest;
    use stackable_operator::k8s_openapi;

    #[test]
    fn get_labels_from_name() {
        let test_name = "testcluster";
        let selector = get_match_labels(test_name);

        assert!(selector.match_expressions.is_none());
        assert_eq!(selector.match_labels.as_ref().unwrap().len(), 4);
        assert_eq!(
            selector
                .match_labels
                .as_ref()
                .unwrap()
                .get(APP_NAME_LABEL)
                .unwrap(),
            APP_NAME
        );
        assert_eq!(
            selector
                .match_labels
                .as_ref()
                .unwrap()
                .get(APP_MANAGED_BY_LABEL)
                .unwrap(),
            MANAGED_BY
        );
        assert_eq!(
            selector
                .match_labels
                .as_ref()
                .unwrap()
                .get(APP_INSTANCE_LABEL)
                .unwrap(),
            test_name
        );
        assert_eq!(
            selector
                .match_labels
                .as_ref()
                .unwrap()
                .get(APP_COMPONENT_LABEL)
                .unwrap(),
            &HdfsRole::NameNode.to_string()
        );
    }

    #[rstest]
    #[case(Some("test"), Some("/test"))]
    #[case(Some("/test"), Some("/test"))]
    fn pad_root(#[case] input: Option<&str>, #[case] expected_output: Option<&str>) {
        assert_eq!(
            expected_output,
            pad_and_check_chroot(input)
                .expect("should not fail")
                .as_deref()
        );
    }

    #[rstest]
    #[case::single_pod_with_root(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
              app.kubernetes.io/component: namenode 
          spec:
            nodeName: worker-1.stackable.tech
            containers:
              - name: hdfs
                ports:
                  - containerPort: 9555
                    name: ipc
    "},
    Some("/dev"),
    "hdfs://worker-1.stackable.tech:9555/dev"
    )]
    #[case::single_pod_without_root(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
              app.kubernetes.io/component: namenode 
          spec:
            nodeName: worker-1.stackable.tech
            containers:
              - name: hdfs
                ports:
                  - containerPort: 9555
                    name: ipc
    "},
    None,
    "hdfs://worker-1.stackable.tech:9555"
    )]
    fn get_connection_string(
        #[case] hdfs_pods: &str,
        #[case] root: Option<&str>,
        #[case] expected_result: &str,
    ) {
        let pods = parse_pod_list_from_yaml(hdfs_pods);

        let conn_string = get_hdfs_connection_string_from_pods(pods.as_slice(), root)
            .expect("should not fail")
            .unwrap();
        assert_eq!(expected_result, conn_string.full_connection_string());
    }

    #[rstest]
    #[case::missing_hostname(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
              app.kubernetes.io/component: namenode 
          spec:
            containers:
              - name: hdfs
                ports:
                  - containerPort: 9555
                    name: ipc
    "},
    Some("/prod"),
    )]
    fn get_connection_string_should_fail(#[case] hdfs_pods: &str, #[case] root: Option<&str>) {
        let pods = parse_pod_list_from_yaml(hdfs_pods);
        let conn_string = get_hdfs_connection_string_from_pods(pods.as_slice(), root);
        assert!(conn_string.is_err())
    }

    #[rstest]
    #[case::missing_mandatory_label(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/instance: test
          spec:
            nodeName: worker-1.stackable.tech
            containers:
              - name: hdfs
                ports:
                  - containerPort: 9555
                    name: ipc
    "},
    Some("/prod"),
    )]
    #[case::missing_container(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
              app.kubernetes.io/component: namenode 
          spec:
            nodeName: worker-1.stackable.tech
            containers: []
    "},
    Some("/prod"),
    )]
    #[case::missing_correct_container_port(
    indoc! {"
        - apiVersion: v1
          kind: Pod
          metadata:
            name: test
            labels:
              app.kubernetes.io/name: hdfs
              app.kubernetes.io/role-group: default
              app.kubernetes.io/instance: test
              app.kubernetes.io/component: namenode 
          spec:
            nodeName: worker-1.stackable.tech
            containers: 
              - name: hdfs
                ports:
                  - containerPort: 9555
                    name: abc
    "},
    Some("/prod"),
    )]
    fn get_connection_string_should_be_none(#[case] hdfs_pods: &str, #[case] root: Option<&str>) {
        let pods = parse_pod_list_from_yaml(hdfs_pods);
        let conn_string = get_hdfs_connection_string_from_pods(pods.as_slice(), root);
        assert!(conn_string.unwrap().is_none())
    }

    fn parse_pod_list_from_yaml(pod_config: &str) -> Vec<Pod> {
        let kube_pods: Vec<k8s_openapi::api::core::v1::Pod> =
            serde_yaml::from_str(pod_config).unwrap();
        kube_pods.iter().map(|pod| pod.to_owned()).collect()
    }
}
