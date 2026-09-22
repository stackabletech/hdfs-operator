//! Building the Kubernetes resources of one role group.
//!
//! Two phases, so neither has to reach into the other. First each role's module ([`namenode`],
//! [`datanode`], [`journalnode`]) gathers its [`RoleGroupInputs`] and names the containers only
//! that role runs; [`RoleGroupBuilder::new`] turns those into finished containers, pod volumes
//! and rendered `ConfigMap` entries. Then [`RoleGroupBuilder`]'s `build_*` methods emit the
//! Kubernetes objects, reading straight through fields that are already resolved.
//!
//! The emit phase is identical for every role, so it lives here once. Everything that differs
//! between roles happens in the three role modules' `build` functions.

mod datanode;
mod journalnode;
mod namenode;

use std::fmt::Display;

pub(crate) use datanode::build as build_datanode_role_group;
pub(crate) use journalnode::build as build_journalnode_role_group;
pub(crate) use namenode::build as build_namenode_role_group;
use snafu::ResultExt;
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{
            ConfigMap, Container, PersistentVolumeClaim, PodTemplateSpec, ResourceRequirements,
            Service, Volume,
        },
    },
    kvp::Labels,
    product_logging::spec::{ContainerLogConfig, Logging},
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::EnvVarSet, jvm_argument_overrides::JvmArgumentOverrides,
        types::operator::RoleGroupName,
    },
};

use super::{
    Error, ServiceSnafu, container::ContainerConfig, properties::product_logging, resource,
};
use crate::{
    controller::ValidatedCluster,
    crd::{CommonNodeConfig, HdfsNodeRole, storage::DataNodeStorageConfigInnerType, v1alpha1},
};

/// A container only one role runs, named by that role's module together with the log config that
/// belongs to it.
pub(crate) struct ExtraContainer {
    config: ContainerConfig,
    logging: ContainerLogConfig,
    init: bool,
}

impl ExtraContainer {
    /// A container running alongside the main one for the pod's whole lifetime.
    pub(crate) fn side(config: ContainerConfig, logging: ContainerLogConfig) -> Self {
        Self {
            config,
            logging,
            init: false,
        }
    }

    /// A container running to completion before the main one starts.
    pub(crate) fn init(config: ContainerConfig, logging: ContainerLogConfig) -> Self {
        Self {
            config,
            logging,
            init: true,
        }
    }
}

/// One role group's validated configuration, as its role's module reads it off the CRD.
///
/// [`RoleGroupBuilder::new`] consumes this: every field is either resolved into the builder's
/// finished containers and `ConfigMap` entries, or carried over to the emit phase.
pub(crate) struct RoleGroupInputs<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) cluster_info: &'a KubernetesClusterInfo,
    pub(crate) role: HdfsNodeRole,
    pub(crate) role_group_name: RoleGroupName,
    /// The selector labels of the role group's pods, also used as the `StatefulSet` selector and
    /// on the listener volumes.
    ///
    /// We must use the selector labels and not the recommended labels for the listener volumes.
    /// This is because the recommended set contains a "managed-by" label. That label triggers the
    /// cluster resources to "manage" listeners, which is wrong and leads to errors. The listeners
    /// are managed by the listener-operator.
    pub(crate) selector_labels: Labels,
    /// The role group's merged config that is common to every role.
    pub(crate) common: CommonNodeConfig,
    /// The resource requirements of the role group's main and init containers; the ZKFC sidecar
    /// has fixed requirements of its own and ignores this.
    pub(crate) resources: ResourceRequirements,
    /// The `StatefulSet`'s persistent volume claim templates. For namenodes these include the
    /// listener claim template.
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// Pod-level volumes beyond those the containers bring with them. Only datanodes have one,
    /// their ephemeral listener volume.
    pub(crate) extra_pod_volumes: Vec<Volume>,
    /// The log config of the main `hdfs` container, which every role runs.
    pub(crate) hdfs_logging: ContainerLogConfig,
    /// The log config of the Vector sidecar; `None` when the Vector agent is disabled for this
    /// role group.
    pub(crate) vector_logging: Option<ContainerLogConfig>,
    /// The data volume configuration behind `dfs.datanode.data.dir`; `Some` only for datanodes,
    /// the one role that configures it.
    pub(crate) datanode_storage: Option<DataNodeStorageConfigInnerType>,
    /// The role group's replica count; `None` when unset, which counts as one replica.
    pub(crate) replicas: Option<u16>,
    pub(crate) config_overrides: v1alpha1::HdfsConfigOverrides,
    pub(crate) env_overrides: EnvVarSet,
    pub(crate) pod_overrides: PodTemplateSpec,
    pub(crate) jvm_argument_overrides: JvmArgumentOverrides,
}

impl RoleGroupInputs<'_> {
    /// The name the role group's owned objects share.
    pub(crate) fn object_name(&self) -> String {
        self.cluster
            .role_group_resource_names(&self.role, &self.role_group_name)
            .qualified_role_group_name()
            .to_string()
    }
}

/// Everything one role group's Kubernetes objects are built from, already resolved.
///
/// These fields are outputs rather than configuration: the containers are built and the
/// `ConfigMap` entries rendered. The `build_*` methods therefore read straight through, and
/// nothing about which role this is reaches them.
pub(crate) struct RoleGroupBuilder<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) role: HdfsNodeRole,
    pub(crate) role_group_name: RoleGroupName,
    pub(crate) selector_labels: Labels,
    /// Carried for the pod's affinity and its graceful shutdown timeout.
    pub(crate) common: CommonNodeConfig,
    pub(crate) replicas: Option<u16>,
    pub(crate) pod_overrides: PodTemplateSpec,
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    /// The main `hdfs` container, the Vector sidecar when it is enabled, and the role's own side
    /// containers, in the order the role named them.
    pub(crate) containers: Vec<Container>,
    /// The role's init containers, in the order the role named them.
    pub(crate) init_containers: Vec<Container>,
    /// Every pod-level volume the containers and the role need.
    pub(crate) pod_volumes: Vec<Volume>,
    /// The rendered `ConfigMap` entries, as (file name, content).
    pub(crate) config_map_data: Vec<(String, String)>,
}

impl<'a> RoleGroupBuilder<'a> {
    /// Resolves one role group: builds the containers every role runs plus the `extra_containers`
    /// this role named, renders its `ConfigMap` entries and collects its pod volumes.
    pub(crate) fn new(
        inputs: RoleGroupInputs<'a>,
        extra_containers: Vec<ExtraContainer>,
    ) -> Result<Self, Error> {
        let role = inputs.role;
        let role_group_name = inputs.role_group_name.clone();

        let container_error = |source| Error::Container {
            source,
            role,
            role_group: role_group_name.clone(),
        };

        // The role's own volumes come first: the pod's `volumes` are an ordered list, and
        // reordering them changes the pod template of every StatefulSet already running.
        let mut pod_volumes = inputs.extra_pod_volumes.clone();
        let (mut containers, container_volumes) =
            ContainerConfig::common_containers_and_volumes(&inputs).map_err(container_error)?;
        pod_volumes.extend(container_volumes);

        let mut config_map_data =
            resource::config_map::common_config_map_data(&inputs).map_err(|source| {
                Error::ConfigMap {
                    source,
                    role,
                    role_group: role_group_name.clone(),
                }
            })?;

        let mut init_containers = Vec::new();
        for extra in &extra_containers {
            let (container, volumes) = extra
                .config
                .build_container(&inputs, &extra.logging, extra.init)
                .map_err(container_error)?;

            if extra.init {
                init_containers.push(container);
            } else {
                containers.push(container);
            }
            pod_volumes.extend(volumes);
            config_map_data.extend(product_logging::log4j_config(&extra.config, &extra.logging));
        }

        Ok(Self {
            cluster: inputs.cluster,
            role,
            role_group_name: inputs.role_group_name,
            selector_labels: inputs.selector_labels,
            common: inputs.common,
            replicas: inputs.replicas,
            pod_overrides: inputs.pod_overrides,
            volume_claim_templates: inputs.volume_claim_templates,
            containers,
            init_containers,
            pod_volumes,
            config_map_data,
        })
    }

    /// The headless and metrics `Service`s.
    pub(crate) fn build_services(&self) -> Result<Vec<Service>, Error> {
        let context = || ServiceSnafu {
            role: self.role,
            role_group: self.role_group_name.clone(),
        };

        Ok(vec![
            resource::service::rolegroup_headless_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
            resource::service::rolegroup_metrics_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            )
            .with_context(|_| context())?,
        ])
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        resource::config_map::build_config_map(self).map_err(|source| Error::ConfigMap {
            source,
            role: self.role,
            role_group: self.role_group_name.clone(),
        })
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        resource::statefulset::build_statefulset(self).map_err(|source| Error::StatefulSet {
            source,
            role: self.role,
            role_group: self.role_group_name.clone(),
        })
    }
}

/// The log configs of the two containers every role runs: the main `hdfs` container, and the
/// Vector sidecar, which is `None` when the Vector agent is disabled for the role group.
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
