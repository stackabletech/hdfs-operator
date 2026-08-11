use snafu::{ResultExt, Snafu};
use stackable_operator::{
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::api::ListParams,
    v2::controller_utils::{get_cluster_name, get_namespace},
};

use crate::{
    controller::build::{opa::HdfsOpaConfig, resource::discovery::discovery_config_map_name},
    crd::{is_namenode_listener, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig {
        source: crate::controller::build::opa::Error,
    },

    #[snafu(display("failed to get the cluster name"))]
    GetClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the cluster namespace"))]
    GetClusterNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to list the namenode Listeners"))]
    ListNamenodeListeners {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to get the discovery ConfigMap {config_map_name}"))]
    GetDiscoveryConfigMap {
        source: stackable_operator::client::Error,
        config_map_name: String,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub hdfs_opa_config: Option<HdfsOpaConfig>,
    /// The namenode pod `Listener`s as currently stored in the cluster, fetched because the
    /// discovery `ConfigMap` is built from their ingress addresses. Unlike
    /// [`Self::hdfs_opa_config`] they are not referenced from the spec: the listener-operator
    /// creates them for the namenode listener volumes, so they can be missing or still
    /// address-less around the first reconcile runs.
    pub namenode_listeners: Vec<Listener>,
    /// The discovery `ConfigMap` as currently stored in the cluster. Like the namenode
    /// `Listener`s it is not referenced from the spec but created by this operator itself:
    /// `None` until the first successful build. Fetched so that the build step can re-emit
    /// the existing contents unchanged while no fresh ConfigMap can be built (not every
    /// namenode `Listener` has an ingress address then), instead of letting the apply step
    /// delete it as an orphan.
    pub discovery_config_map: Option<ConfigMap>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hdfs: &v1alpha1::HdfsCluster,
) -> Result<DereferencedObjects, Error> {
    let hdfs_opa_config = match &hdfs.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HdfsOpaConfig::from_opa_config(client, hdfs, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    let cluster_name = get_cluster_name(hdfs).context(GetClusterNameSnafu)?;
    let namespace = get_namespace(hdfs).context(GetClusterNamespaceSnafu)?;
    let namenode_listeners = client
        .list::<Listener>(namespace.as_ref(), &ListParams::default())
        .await
        .context(ListNamenodeListenersSnafu)?
        .into_iter()
        .filter(|listener| {
            listener
                .metadata
                .name
                .as_deref()
                .is_some_and(|listener_name| {
                    is_namenode_listener(listener_name, cluster_name.as_ref())
                })
        })
        .collect();

    let config_map_name = discovery_config_map_name(&cluster_name);
    let discovery_config_map = client
        .get_opt::<ConfigMap>(&config_map_name, namespace.as_ref())
        .await
        .context(GetDiscoveryConfigMapSnafu { config_map_name })?;

    Ok(DereferencedObjects {
        hdfs_opa_config,
        namenode_listeners,
        discovery_config_map,
    })
}
