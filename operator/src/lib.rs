#![feature(backtrace)]
mod error;
mod hdfs;

use crate::error::Error;

use async_trait::async_trait;
use kube::api::ListParams;
use kube::Api;

use crate::error::Error::NodeTypeParseError;
use crate::hdfs::{HdfsClusterBuilder, HdfsClusterDefinition};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Node, Pod, PodSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_hdfs_crd::{
    HdfsCluster, HdfsClusterSpec, HdfsConfig, HdfsVersion, SelectorAndConfig,
};
use stackable_operator::client::Client;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

const FINALIZER_NAME: &str = "hdfs.hadoop.stackable.tech/cleanup";

const CLUSTER_NAME_LABEL: &str = "hdfs.hadoop.stackable.tech/cluster-name";

const NODE_GROUP_LABEL: &str = "hdfs.hadoop.stackable.tech/node-group-name";

const NODE_TYPE_LABEL: &str = "hdfs.hadoop.stackable.tech/node-type";

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum HdfsNodeType {
    NameNode,
    DataNode,
    JournalNode,
}

impl Display for HdfsNodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Into<BTreeMap<String, Option<String>>> for HdfsNodeType {
    fn into(self) -> BTreeMap<String, Option<String>> {
        vec![(String::from(NODE_TYPE_LABEL), Some(self.to_string()))]
            .into_iter()
            .collect()
    }
}

impl FromStr for HdfsNodeType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NameNode" => Ok(Self::NameNode),
            "DataNode" => Ok(Self::DataNode),
            "JournalNode" => Ok(Self::JournalNode),
            _ => Err(NodeTypeParseError(String::from(s))),
        }
    }
}

type HdfsReconcileResult = ReconcileResult<error::Error>;

struct HdfsState {
    context: ReconciliationContext<HdfsCluster>,
    hdfs_spec: HdfsClusterSpec,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<HdfsNodeType, HashMap<String, Vec<Node>>>,
    cluster_builder: HdfsClusterBuilder,
    cluster_definition: Option<HdfsClusterDefinition>,
}

impl HdfsState {
    async fn init_status(&mut self) -> HdfsReconcileResult {
        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn create_missing_pods(&mut self) -> HdfsReconcileResult {
        for node_type in [
            HdfsNodeType::DataNode,
            HdfsNodeType::NameNode,
            HdfsNodeType::JournalNode,
        ]
        .iter()
        {
            if let Some(nodes_for_role) = self.eligible_nodes.get(node_type) {
                for (role_group, nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        node_type, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        nodes.len(),
                        nodes
                            .iter()
                            .map(|node| node.metadata.name.clone().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.clone().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "labels: [{:?}]",
                        get_node_and_group_labels(role_group, node_type)
                    );
                    let nodes_that_need_pods =
                        stackable_operator::reconcile::find_nodes_that_need_pods(
                            nodes,
                            &self.existing_pods,
                            &get_node_and_group_labels(role_group, node_type),
                        )
                        .await;

                    for node in nodes_that_need_pods {
                        debug!(
                            "Creating pod on node [{}] for [{}] role and group [{}]",
                            node.metadata
                                .name
                                .clone()
                                .unwrap_or_else(|| String::from("<no node name found>")),
                            node_type,
                            role_group
                        );
                        self.create_pod(
                            node.metadata.name.as_ref().unwrap(),
                            node_type.clone(),
                            &role_group,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }

    async fn wait_for_terminating_pods(&self) -> HdfsReconcileResult {
        match self
            .existing_pods
            .iter()
            .any(|pod| stackable_operator::finalizer::has_deletion_stamp(pod))
        {
            true => {
                info!("Found terminating pods, requeuing to await termination!");
                Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(5)))
            }
            false => {
                debug!("No terminating pods found, continueing");
                Ok(ReconcileFunctionAction::Continue)
            }
        }
    }

    pub async fn delete_illegal_pods(&self) -> HdfsReconcileResult {
        let mut mandatory_labels = BTreeMap::new();
        let allowed_roles = [
            HdfsNodeType::DataNode,
            HdfsNodeType::NameNode,
            HdfsNodeType::JournalNode,
        ]
        .iter()
        .map(|role| role.to_string())
        .collect::<Vec<String>>();
        mandatory_labels.insert(String::from(NODE_TYPE_LABEL), Some(allowed_roles));
        mandatory_labels.insert(String::from(NODE_GROUP_LABEL), None);
        mandatory_labels.insert(String::from(CLUSTER_NAME_LABEL), None);

        let deleted_pods = self
            .context
            .delete_illegal_pods(&self.existing_pods, &mandatory_labels)
            .await?;
        if !deleted_pods.is_empty() {
            warn!("Found [{}] illegal pods that were missing mandatory labels, the following pods were deleted: [{}]", deleted_pods.len(), deleted_pods.iter().map(|pod| pod.metadata.name.clone().unwrap_or_default()).collect::<Vec<_>>().join(","));
            Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(5)))
        } else {
            debug!("No illegal pods found.");
            Ok(ReconcileFunctionAction::Continue)
        }
    }

    async fn delete_excess_pods(&self) -> HdfsReconcileResult {
        let mut eligible_nodes_map = vec![];
        debug!(
            "Looking for excess pods that need to be deleted for cluster [{}]",
            self.context.name()
        );
        for node_type in [
            HdfsNodeType::DataNode,
            HdfsNodeType::NameNode,
            HdfsNodeType::JournalNode,
        ]
        .iter()
        {
            if let Some(eligible_nodes_for_role) = self.eligible_nodes.get(node_type) {
                for (group_name, eligible_nodes) in eligible_nodes_for_role {
                    // Create labels to identify eligible nodes
                    trace!(
                        "Adding [{}] nodes to eligible node list for role [{}] and group [{}].",
                        eligible_nodes.len(),
                        node_type,
                        group_name
                    );
                    eligible_nodes_map.push((
                        eligible_nodes.as_slice(),
                        get_node_and_group_labels(group_name, node_type),
                    ))
                }
            }
        }
        let excess_pods = stackable_operator::reconcile::find_excess_pods(
            eligible_nodes_map.as_slice(),
            &self.existing_pods,
        );
        info!(
            "Found [{}] excess pods for [{}]",
            excess_pods.len(),
            self.context.name()
        );
        for pod in excess_pods {
            self.context.client.delete(pod).await?;
        }

        Ok(ReconcileFunctionAction::Done)
    }

    async fn create_pod(
        &self,
        node_name: &str,
        node_type: HdfsNodeType,
        node_group: &str,
    ) -> Result<Pod, Error> {
        let pod = self.build_pod(node_name, node_type, node_group)?;
        Ok(self.context.client.create(&pod).await?)
    }

    fn build_pod(
        &self,
        node_name: &str,
        node_type: HdfsNodeType,
        node_group: &str,
    ) -> Result<Pod, Error> {
        let (containers, volumes) = self.build_containers(&node_name, &node_type, &node_group);

        Ok(Pod {
            metadata: metadata::build_metadata(
                self.get_pod_name(&node_name, &node_type.to_string()),
                Some(self.build_labels(&node_type, &node_group)),
                &self.context.resource,
                true,
            )?,
            spec: Some(PodSpec {
                node_name: Some(node_name.to_string()),
                tolerations: Some(stackable_operator::krustlet::create_tolerations()),
                containers,
                volumes: Some(volumes),
                ..PodSpec::default()
            }),

            ..Pod::default()
        })
    }

    fn build_containers(
        &self,
        node_name: &str,
        node_type: &HdfsNodeType,
        node_group: &str,
    ) -> (Vec<Container>, Vec<Volume>) {
        let version = self.context.resource.spec.version.clone();
        let image_name = format!(
            "stackable/hadoop:{}",
            serde_json::json!(version).as_str().expect("This should not fail as it comes from an enum, if this fails please file a bug report")
        );

        let command = if node_type == &HdfsNodeType::DataNode {
            vec!["{{packageroot}}/hadoop-3.2.2/bin/hdfs", "--config {{configroot/conf}}", "datanode"]
        } else if node_type == &HdfsNodeType::NameNode {
            vec!["{{packageroot}}/hadoop-3.2.2/bin/hdfs", "--config {{configroot/conf}}", "namenode"]
        } else if node_type == &HdfsNodeType::JournalNode {
            vec!["{{packageroot}}/hadoop-3.2.2/bin/hdfs", "--config {{configroot/conf}}", "journalnode"]
        } else {
            error!("Got wrong node type, this should not happen, please report this as a bug in our issue tracker!");
            panic!("shit went wrong")
        }.into_iter().map(|part| String::from(part)).collect::<Vec<_>>();

        let containers = vec![Container {
            image: Some(image_name),
            name: "hdfs".to_string(),
            command: Some(command),
            volume_mounts: Some(vec![
                // One mount for the config directory, this will be relative to the extracted package
                VolumeMount {
                    mount_path: "conf".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                },
                // We need a second mount for the data directory
                // because we need to write the myid file into the data directory
                VolumeMount {
                    mount_path: "/tmp/hdfs".to_string(), // TODO: Make configurable
                    name: "data-volume".to_string(),
                    ..VolumeMount::default()
                },
            ]),
            ..Container::default()
        }];

        let cm_name_prefix = format!(
            "hdfs-{}",
            self.get_pod_name(node_name, &node_type.to_string())
        );
        let volumes = vec![
            Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
            Volume {
                name: "data-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-data", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
        ];

        (containers, volumes)
    }

    fn build_labels(&self, node_type: &HdfsNodeType, node_group: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(CLUSTER_NAME_LABEL.to_string(), self.context.name());
        labels.insert(NODE_TYPE_LABEL.to_string(), node_type.to_string());
        labels.insert(NODE_GROUP_LABEL.to_string(), node_group.to_string());

        labels
    }

    /// All pod names follow a simple pattern: <name of ZooKeeperCluster object>-<Node name>
    fn get_pod_name(&self, node_name: &str, role: &str) -> String {
        format!(
            "{}-{}-{}",
            self.context.name(),
            node_name,
            role.to_lowercase()
        )
    }
}

impl ReconciliationState for HdfsState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("=================== Start Reconciliation ===================");
        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.wait_for_terminating_pods())
                .await?
                .then(self.delete_illegal_pods())
                .await?
                .then(self.create_missing_pods())
                .await?
                .then(self.delete_excess_pods())
                .await
        })
    }
}

#[derive(Debug)]
struct HdfsStrategy {}

impl HdfsStrategy {
    pub fn new() -> HdfsStrategy {
        HdfsStrategy {}
    }
}

#[async_trait]
impl ControllerStrategy for HdfsStrategy {
    type Item = HdfsCluster;
    type State = HdfsState;
    type Error = Error;

    fn finalizer_name(&self) -> String {
        FINALIZER_NAME.to_string()
    }

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let cluster_spec: HdfsClusterSpec = context.resource.spec.clone();

        let mut group_definitions: HashMap<HdfsNodeType, HashMap<String, LabelSelector>> =
            HashMap::new();
        group_definitions.insert(
            HdfsNodeType::DataNode,
            cluster_spec
                .datanode
                .selectors
                .iter()
                .map(|(group_name, selector_config)| {
                    (
                        group_name.clone(),
                        selector_config.clone().selector.unwrap(),
                    )
                })
                .collect(),
        );
        group_definitions.insert(
            HdfsNodeType::NameNode,
            cluster_spec
                .namenode
                .selectors
                .iter()
                .map(|(group_name, selector_config)| {
                    (
                        group_name.clone(),
                        selector_config.clone().selector.unwrap(),
                    )
                })
                .collect(),
        );

        // Retrieve eligible nodes for all node types
        let mut eligible_nodes = HashMap::new();
        for node_type in [
            HdfsNodeType::DataNode,
            HdfsNodeType::NameNode,
            HdfsNodeType::JournalNode,
        ]
        .iter()
        {
            if let Some(groups) = group_definitions.get(node_type) {
                eligible_nodes.insert(
                    node_type.clone(),
                    context.find_nodes_that_fit_selectors(groups).await?,
                );
            }
        }

        let existing_pods = context.list_pods().await?;

        Ok(HdfsState {
            hdfs_spec: context.resource.spec.clone(),
            existing_pods,
            eligible_nodes,
            context,
            cluster_builder: HdfsClusterBuilder::new(HdfsVersion::v3_2_2),
            cluster_definition: None,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let hdfs_api: Api<HdfsCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(hdfs_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = HdfsStrategy::new();

    controller.run(client, strategy).await;
}

fn get_node_and_group_labels(
    group_name: &str,
    node_type: &HdfsNodeType,
) -> BTreeMap<String, Option<String>> {
    let mut node_labels = BTreeMap::new();
    node_labels.insert(String::from(NODE_TYPE_LABEL), Some(node_type.to_string()));
    node_labels.insert(
        String::from(NODE_GROUP_LABEL),
        Some(String::from(group_name)),
    );
    node_labels
}
