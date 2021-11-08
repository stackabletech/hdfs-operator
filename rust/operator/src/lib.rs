mod error;
use crate::error::Error;

use async_trait::async_trait;
use stackable_hdfs_crd::commands::{Format, Restart, Start, Stop};
use stackable_hdfs_crd::discovery::{
    get_hdfs_connection_string_from_pods, HdfsConnectionInformation,
};
use stackable_hdfs_crd::{
    HdfsAddress, HdfsCluster, HdfsClusterSpec, HdfsRole, HdfsVersion, APP_NAME, CONFIG_DIR_NAME,
    CORE_SITE_XML, DATA_PORT, DFS_DATA_NODE_ADDRESS, DFS_DATA_NODE_HTTP_ADDRESS,
    DFS_DATA_NODE_IPC_ADDRESS, DFS_NAME_NODE_HTTP_ADDRESS, FS_DEFAULT, HDFS_SITE_XML, HTTP_PORT,
    IPC_PORT, METRICS_PORT, METRICS_PORT_PROPERTY,
};
use stackable_operator::builder::{
    ContainerBuilder, ObjectMetaBuilder, PodBuilder, PodSecurityContextBuilder, VolumeBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::command::materialize_command;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::identity::{LabeledPodIdentityFactory, PodIdentity, PodToNodeMapping};
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Pod};
use stackable_operator::kube::api::{ListParams, ResourceExt};
use stackable_operator::kube::Api;
use stackable_operator::labels;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels,
};
use stackable_operator::name_utils;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils;
use stackable_operator::role_utils::{
    get_role_and_group_labels, list_eligible_nodes_for_role_and_group, EligibleNodesForRoleAndGroup,
};
use stackable_operator::scheduler::{
    K8SUnboundedHistory, RoleGroupEligibleNodes, ScheduleStrategy, Scheduler, StickyScheduler,
};
use stackable_operator::status::HasClusterExecutionStatus;
use stackable_operator::status::{init_status, ClusterExecutionStatus};
use stackable_operator::versioning::{finalize_versioning, init_versioning};
use stackable_operator::{configmap, product_config};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::error;
use tracing::{debug, info, trace, warn};

const FINALIZER_NAME: &str = "hdfs.stackable.tech/cleanup";
const ID_LABEL: &str = "hdfs.stackable.tech/id";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";

const CM_TYPE_CONFIG: &str = "config";

const LOG_4J: &str = "log4j.properties";

type HdfsReconcileResult = ReconcileResult<error::Error>;

struct HdfsState {
    context: ReconciliationContext<HdfsCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
    hdfs_info: Option<HdfsConnectionInformation>,
}

impl HdfsState {
    async fn get_hdfs_connection_information(&mut self) -> HdfsReconcileResult {
        let hdfs_info = get_hdfs_connection_string_from_pods(&self.existing_pods, None)?;

        debug!("Received HDFS connection string: [{:?}]", hdfs_info);

        self.hdfs_info = hdfs_info;

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Required labels for pods. Pods without any of these will deleted and/or replaced.
    pub fn get_required_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = HdfsRole::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(labels::APP_COMPONENT_LABEL.to_string(), Some(roles));
        mandatory_labels.insert(
            labels::APP_INSTANCE_LABEL.to_string(),
            Some(vec![self.context.name()]),
        );
        mandatory_labels.insert(
            labels::APP_VERSION_LABEL.to_string(),
            Some(vec![self.context.resource.spec.version.to_string()]),
        );
        mandatory_labels.insert(ID_LABEL.to_string(), None);

        mandatory_labels
    }

    /// Will initialize the status object if it's never been set.
    async fn init_status(&mut self) -> HdfsReconcileResult {
        // init status with default values if not available yet.
        self.context.resource = init_status(&self.context.client, &self.context.resource).await?;

        let spec_version = self.context.resource.spec.version.clone();

        self.context.resource =
            init_versioning(&self.context.client, &self.context.resource, spec_version).await?;

        // set the cluster status to running
        if self.context.resource.cluster_execution_status().is_none() {
            self.context
                .client
                .merge_patch_status(
                    &self.context.resource,
                    &self
                        .context
                        .resource
                        .cluster_execution_status_patch(&ClusterExecutionStatus::Running),
                )
                .await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn create_missing_pods(&mut self) -> HdfsReconcileResult {
        trace!(target: "create_missing_pods","Starting `create_missing_pods`");

        // The iteration happens in two stages here, to accommodate the way our operators think
        // about roles and role groups.
        // The hierarchy is:
        // - Roles (NameNode, DataNode)
        //   - Role groups (user defined)
        for role in HdfsRole::iter() {
            let role_str = &role.to_string();
            if let Some(nodes_for_role) = self.eligible_nodes.get(role_str) {
                for (role_group, eligible_nodes) in nodes_for_role {
                    debug!( target: "create_missing_pods",
                        "Identify missing pods for [{}] role and group [{}]",
                        role_str, role_group
                    );
                    trace!( target: "create_missing_pods",
                        "candidate_nodes[{}]: [{:?}]",
                        eligible_nodes.nodes.len(),
                        eligible_nodes
                            .nodes
                            .iter()
                            .map(|node| node.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(target: "create_missing_pods",
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(target: "create_missing_pods",
                        "labels: [{:?}]",
                        get_role_and_group_labels(role_str, role_group)
                    );
                    let mut history = match self
                        .context
                        .resource
                        .status
                        .as_ref()
                        .and_then(|status| status.history.as_ref())
                    {
                        Some(simple_history) => {
                            // we clone here because we cannot access mut self because we need it later
                            // to create config maps and pods. The `status` history will be out of sync
                            // with the cloned `simple_history` until the next reconcile.
                            // The `status` history should not be used after this method to avoid side
                            // effects.
                            K8SUnboundedHistory::new(&self.context.client, simple_history.clone())
                        }
                        None => K8SUnboundedHistory::new(
                            &self.context.client,
                            PodToNodeMapping::default(),
                        ),
                    };

                    let mut sticky_scheduler =
                        StickyScheduler::new(&mut history, ScheduleStrategy::GroupAntiAffinity);

                    let pod_id_factory = LabeledPodIdentityFactory::new(
                        APP_NAME,
                        &self.context.name(),
                        &self.eligible_nodes,
                        ID_LABEL,
                        1,
                    );

                    trace!("pod_id_factory: {:?}", pod_id_factory.as_ref());

                    let state = sticky_scheduler.schedule(
                        &pod_id_factory,
                        &RoleGroupEligibleNodes::from(&self.eligible_nodes),
                        &self.existing_pods,
                    )?;

                    let mapping = state.remaining_mapping().filter(
                        APP_NAME,
                        &self.context.name(),
                        role_str,
                        role_group,
                    );

                    if let Some((pod_id, node_id)) = mapping.iter().next() {
                        // now we have a node that needs a pod -> get validated config
                        let validated_config = config_for_role_and_group(
                            pod_id.role(),
                            pod_id.group(),
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(pod_id, &role, validated_config, &state.mapping())
                            .await?;

                        self.create_pod(
                            pod_id,
                            &role,
                            &node_id.name,
                            &config_maps,
                            validated_config,
                        )
                        .await?;

                        history.save(&self.context.resource).await?;

                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        finalize_versioning(&self.context.client, &self.context.resource).await?;

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Creates the config maps required for a hdfs instance (or role, role_group combination):
    /// * The 'core-site.xml' properties file
    /// * The 'hdfs-site.xml' properties file
    /// * The 'log4j.properties' file
    ///
    /// Returns a map with a 'type' identifier (e.g. config) as key and the corresponding
    /// ConfigMap as value. This is required to set the volume mounts in the pod later on.
    ///
    /// # Arguments
    ///
    /// - `pod_id` - The `PodIdentity` containing app, instance, role, group names and the id.
    /// - `validated_config` - The validated product config.
    /// - `id_mapping` - All id to node mappings required to create config maps
    ///
    async fn create_config_maps(
        &self,
        pod_id: &PodIdentity,
        role: &HdfsRole,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
        id_mapping: &PodToNodeMapping,
    ) -> Result<HashMap<&'static str, ConfigMap>, Error> {
        let mut config_maps = HashMap::new();
        let mut config_maps_data = BTreeMap::new();

        let log4j_config = include_str!("log4j.properties");
        config_maps_data.insert(LOG_4J.to_string(), log4j_config.to_string());

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(file_name)
                    if file_name == CORE_SITE_XML && role == &HdfsRole::NameNode =>
                {
                    let mut data = BTreeMap::new();
                    for (property_name, property_value) in config {
                        if property_name == FS_DEFAULT {
                            // retrieve node where name node will be scheduled
                            match id_mapping.get(pod_id).map(|node| &node.name) {
                                Some(node_name) => {
                                    data.insert(
                                        property_name.to_string(),
                                        Some(format!(
                                            "{}:{}",
                                            node_name,
                                            HdfsAddress::try_from(property_value)?.port.to_string()
                                        )),
                                    );
                                }
                                // TODO: throw error?
                                None => {
                                    warn!(
                                        "No node mapping or node name found for pod id [{:?}]",
                                        pod_id
                                    );
                                }
                            }
                            continue;
                        }

                        data.insert(property_name.to_string(), Some(property_value.to_string()));
                    }

                    config_maps_data.insert(
                        file_name.clone(),
                        product_config::writer::to_hadoop_xml(data.iter()),
                    );
                }
                PropertyNameKind::File(file_name)
                    if file_name == CORE_SITE_XML && role == &HdfsRole::DataNode =>
                {
                    if let Some(info) = &self.hdfs_info {
                        let mut data = BTreeMap::new();
                        data.insert(
                            FS_DEFAULT.to_string(),
                            Some(info.connection_string().clone()),
                        );
                        config_maps_data.insert(
                            file_name.clone(),
                            product_config::writer::to_hadoop_xml(data.iter()),
                        );
                    } else {
                        warn!("No HDFS name_node connection string found. Maybe no name_node up and running yet?");
                    }
                }
                PropertyNameKind::File(file_name) if file_name == HDFS_SITE_XML => {
                    let mut data = BTreeMap::new();
                    for (property_name, property_value) in config {
                        data.insert(property_name.to_string(), Some(property_value.to_string()));
                    }
                    config_maps_data.insert(
                        file_name.clone(),
                        product_config::writer::to_hadoop_xml(data.iter()),
                    );
                }
                _ => {}
            }
        }

        let mut cm_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );

        cm_labels.insert(
            configmap::CONFIGMAP_TYPE_LABEL.to_string(),
            CM_TYPE_CONFIG.to_string(),
        );

        let cm_conf_name = name_utils::build_resource_name(
            pod_id.app(),
            &self.context.name(),
            pod_id.role(),
            Some(pod_id.group()),
            None,
            Some(CM_TYPE_CONFIG),
        )?;

        let cm_config = configmap::build_config_map(
            &self.context.resource,
            &cm_conf_name,
            &self.context.namespace(),
            cm_labels,
            config_maps_data,
        )?;

        config_maps.insert(
            CM_TYPE_CONFIG,
            configmap::create_config_map(&self.context.client, cm_config).await?,
        );

        trace!("config_maps to be returned: {:?}", config_maps);
        Ok(config_maps)
    }

    /// Creates the pod required for the HDFS instance.
    ///
    /// # Arguments
    ///
    /// - `role` - spark role.
    /// - `group` - The role group.
    /// - `node_name` - The node name for this pod.
    /// - `config_maps` - The config maps and respective types required for this pod.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_pod(
        &self,
        pod_id: &PodIdentity,
        role: &HdfsRole,
        node_name: &str,
        config_maps: &HashMap<&'static str, ConfigMap>,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<Pod, error::Error> {
        let mut metrics_port: Option<String> = None;
        let mut ipc_port: Option<String> = None;
        let mut http_port: Option<String> = None;
        let mut data_port: Option<String> = None;
        //let mut data_dir: Option<&String> = None;

        let spec: &HdfsClusterSpec = &self.context.resource.spec;
        let version: &HdfsVersion = &spec.version;

        let mut cb = ContainerBuilder::new(APP_NAME);

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(file_name)
                    if file_name == CORE_SITE_XML && role == &HdfsRole::NameNode =>
                {
                    ipc_port = HdfsAddress::port(config.get(FS_DEFAULT))?;
                }
                PropertyNameKind::File(file_name)
                    if file_name == HDFS_SITE_XML && role == &HdfsRole::NameNode =>
                {
                    http_port = HdfsAddress::port(config.get(DFS_NAME_NODE_HTTP_ADDRESS))?;
                    //data_dir = config.get(DFS_NAME_NODE_NAME_DIR);
                }
                PropertyNameKind::File(file_name)
                    if file_name == HDFS_SITE_XML && role == &HdfsRole::DataNode =>
                {
                    ipc_port = HdfsAddress::port(config.get(DFS_DATA_NODE_IPC_ADDRESS))?;
                    http_port = HdfsAddress::port(config.get(DFS_DATA_NODE_HTTP_ADDRESS))?;
                    data_port = HdfsAddress::port(config.get(DFS_DATA_NODE_ADDRESS))?;
                    //data_dir = config.get(DFS_DATA_NODE_DATA_DIR);
                }
                PropertyNameKind::Env => {
                    for (property_name, property_value) in config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for ENV... skipping");
                            continue;
                        }
                        // if a metrics port is provided (for now by user, it is not required in
                        // product config to be able to not configure any monitoring / metrics)
                        if property_name == METRICS_PORT_PROPERTY {
                            metrics_port = Some(property_value.to_string());
                            match role {
                                HdfsRole::NameNode => {
                                    cb.add_env_var(
                                        "HDFS_NAMENODE_OPTS".to_string(),
                                        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/namenode.yaml",
                                                property_value)
                                    );
                                }
                                HdfsRole::DataNode => {
                                    cb.add_env_var(
                                        "HDFS_DATANODE_OPTS".to_string(),
                                        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/datanode.yaml",
                                                property_value)
                                    );
                                }
                            }
                            continue;
                        }

                        cb.add_env_var(property_name, property_value);
                    }
                }
                _ => {}
            }
        }

        cb.image(format!(
            "docker.stackable.tech/stackable/hadoop:{}-0.1",
            version.to_string()
        ));
        cb.command(role.get_command(spec.auto_format_fs.unwrap_or(true)));

        let pod_name = name_utils::build_resource_name(
            pod_id.app(),
            &self.context.name(),
            pod_id.role(),
            Some(pod_id.group()),
            Some(node_name),
            None,
        )?;

        let mut recommended_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );
        recommended_labels.insert(ID_LABEL.to_string(), pod_id.id().to_string());

        let mut pod_builder = PodBuilder::new();

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CM_TYPE_CONFIG) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                cb.add_volume_mount("config", CONFIG_DIR_NAME);
                pod_builder.add_volume(VolumeBuilder::new("config").with_config_map(name).build());
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: CM_TYPE_CONFIG,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: CM_TYPE_CONFIG,
                pod_name,
            });
        }

        // TODO: if we create this it will be under root and we cannot access via stackable user
        //   HDFS will create it itself under stackable user
        // One mount for data_dir
        // if let Some(dir) = data_dir {
        //     cb.add_volume_mount("data", dir);
        //     pod_builder.add_volume(
        //         VolumeBuilder::new("data")
        //             .with_empty_dir(Some(""), None)
        //             .build(),
        //     );
        // }

        let mut annotations = BTreeMap::new();
        // only add metrics container port and annotation if available
        if let Some(metrics_port) = metrics_port {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            cb.add_container_port(METRICS_PORT, metrics_port.parse()?);
        }

        // add ipc port if available
        if let Some(ipc_port) = ipc_port {
            cb.add_container_port(IPC_PORT, ipc_port.parse()?);
        }

        // add http port if available
        if let Some(http_port) = http_port {
            cb.add_container_port(HTTP_PORT, http_port.parse()?);
        }

        // add data port if available
        if let Some(data_port) = data_port {
            cb.add_container_port(DATA_PORT, data_port.parse()?);
        }

        let pod = pod_builder
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(recommended_labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_container(cb.build())
            .node_name(node_name)
            // TODO: first iteration we are using host network
            .host_network(true)
            .security_context(
                // in the docker file we use stackable:stackable (1000:1000)
                PodSecurityContextBuilder::new()
                    .run_as_user(1000)
                    .run_as_group(1000)
                    .fs_group(1000)
                    .run_as_non_root()
                    .build(),
            )
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        for pod in &self.existing_pods {
            self.context.client.delete(pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    pub async fn process_command(&mut self) -> HdfsReconcileResult {
        match self.context.retrieve_current_command().await? {
            // if there is no new command and the execution status is stopped we stop the
            // reconcile loop here.
            None => match self.context.resource.cluster_execution_status() {
                Some(execution_status) if execution_status == ClusterExecutionStatus::Stopped => {
                    Ok(ReconcileFunctionAction::Done)
                }
                _ => Ok(ReconcileFunctionAction::Continue),
            },
            Some(command_ref) => match command_ref.kind.as_str() {
                "Restart" => {
                    info!("Restarting cluster [{:?}]", command_ref);
                    let mut restart_command: Restart =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_restart(&mut restart_command).await?)
                }
                "Start" => {
                    info!("Starting cluster [{:?}]", command_ref);
                    let mut start_command: Start =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_start(&mut start_command).await?)
                }
                "Stop" => {
                    info!("Stopping cluster [{:?}]", command_ref);
                    let mut stop_command: Stop =
                        materialize_command(&self.context.client, &command_ref).await?;

                    Ok(self.context.default_stop(&mut stop_command).await?)
                }
                "Format" => {
                    info!(
                        "Formatting filesystem on name node according to command [{:?}]",
                        command_ref
                    );
                    let _format_command: Format =
                        materialize_command(&self.context.client, &command_ref).await?;

                    Ok(ReconcileFunctionAction::Done)
                }
                _ => {
                    error!("Got unknown type of command: [{:?}]", command_ref);
                    Ok(ReconcileFunctionAction::Done)
                }
            },
        }
    }
}

impl ReconciliationState for HdfsState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");

        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.context.handle_deletion(
                    Box::pin(self.delete_all_pods()),
                    FINALIZER_NAME,
                    true,
                ))
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_required_labels(),
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(
                    self.context
                        .wait_for_terminating_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(
                    self.context
                        .wait_for_running_and_ready_pods(&self.existing_pods),
                )
                .await?
                .then(self.process_command())
                .await?
                .then(self.context.delete_excess_pods(
                    list_eligible_nodes_for_role_and_group(&self.eligible_nodes).as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.get_hdfs_connection_information())
                .await?
                .then(self.create_missing_pods())
                .await
        })
    }
}

struct HdfsStrategy {
    config: Arc<ProductConfigManager>,
}

impl HdfsStrategy {
    pub fn new(config: ProductConfigManager) -> HdfsStrategy {
        HdfsStrategy {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl ControllerStrategy for HdfsStrategy {
    type Item = HdfsCluster;
    type State = HdfsState;
    type Error = Error;

    /// Init the Hdfs state. Store all available pods owned by this cluster for later processing.
    /// Retrieve nodes that fit selectors and store them for later processing:
    /// HdfsRole (we only have 'server') -> role group -> list of nodes.
    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context
            .list_owned(build_common_labels_for_all_managed_resources(
                APP_NAME,
                &context.resource.name(),
            ))
            .await?;
        trace!(
            "{}: Found [{}] pods",
            context.log_name(),
            existing_pods.len()
        );

        let hdfs_spec: HdfsClusterSpec = context.resource.spec.clone();

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            HdfsRole::NameNode.to_string(),
            role_utils::find_nodes_that_fit_selectors(&context.client, None, &hdfs_spec.name_nodes)
                .await?,
        );

        eligible_nodes.insert(
            HdfsRole::DataNode.to_string(),
            role_utils::find_nodes_that_fit_selectors(&context.client, None, &hdfs_spec.data_nodes)
                .await?,
        );

        trace!("Eligible Nodes: {:?}", eligible_nodes);

        let mut roles = HashMap::new();

        roles.insert(
            HdfsRole::NameNode.to_string(),
            (
                vec![
                    PropertyNameKind::File(HDFS_SITE_XML.to_string()),
                    PropertyNameKind::File(CORE_SITE_XML.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.name_nodes.clone().into(),
            ),
        );

        roles.insert(
            HdfsRole::DataNode.to_string(),
            (
                vec![
                    PropertyNameKind::File(HDFS_SITE_XML.to_string()),
                    PropertyNameKind::File(CORE_SITE_XML.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.data_nodes.clone().into(),
            ),
        );

        let role_config = transform_all_roles_to_config(&context.resource, roles);
        let validated_role_config = validate_all_roles_and_groups_config(
            &context.resource.spec.version.to_string(),
            &role_config,
            &self.config,
            false,
            false,
        )?;

        Ok(HdfsState {
            context,
            existing_pods,
            eligible_nodes,
            validated_role_config,
            hdfs_info: None,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client, product_config_path: &str) -> OperatorResult<()> {
    let api: Api<HdfsCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();
    let cmd_restart_api: Api<Restart> = client.get_all_api();
    let cmd_start_api: Api<Start> = client.get_all_api();
    let cmd_stop_api: Api<Stop> = client.get_all_api();

    let controller = Controller::new(api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(cmd_restart_api, ListParams::default())
        .owns(cmd_start_api, ListParams::default())
        .owns(cmd_stop_api, ListParams::default());

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = HdfsStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;

    Ok(())
}
