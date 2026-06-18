use std::collections::{BTreeMap, HashMap};

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    kube::{Resource, api::ObjectMeta},
    role_utils::RoleGroupRef,
    v2::{
        role_utils::RoleGroupConfig,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

use crate::{
    build_recommended_labels,
    controller::build::opa::HdfsOpaConfig,
    crd::{AnyNodeConfig, HdfsNodeRole, HdfsPodRef, security::AuthenticationConfig, v1alpha1},
    hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER,
};

pub mod build;
pub mod dereference;
pub mod validate;

/// The [`RoleGroupConfig`] specialised for HDFS: the validated config is the
/// per-role [`AnyNodeConfig`],
pub type ValidatedRoleGroupConfig = RoleGroupConfig<
    AnyNodeConfig,
    stackable_operator::v2::role_utils::JavaCommonConfig,
    v1alpha1::HdfsConfigOverrides,
>;

/// The validated cluster: proves that config merging and validation succeeded
/// for every role and role group before any resources are created. Placed in the
/// controller so that subsequent steps that reference this struct only depend on
/// the controller.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    /// The cluster's object metadata (name, namespace and uid). Kept private and only
    /// exposed via the [`Resource`] implementation so this type can act as the owner
    /// when building owned objects.
    metadata: ObjectMeta,
    /// The logical (and Kubernetes object) name of the cluster.
    pub name: ClusterName,
    /// The cluster namespace, used to build kerberos principals.
    pub namespace: NamespaceName,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_groups: BTreeMap<HdfsNodeRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HdfsNodeRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_groups: BTreeMap<HdfsNodeRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
        role_configs: BTreeMap<HdfsNodeRole, ValidatedRoleConfig>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                // The uid is required so this type can produce valid owner references
                // (Kubernetes rejects owner references without a uid).
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            image,
            cluster_config,
            role_groups,
            role_configs,
        }
    }

    /// Builds the [`HdfsPodRef`]s expected for every pod of the given `role`, across
    /// all of its role groups.
    ///
    /// These pod refs can only access HDFS from inside the Kubernetes cluster (they
    /// use the cluster-internal headless service DNS names). For downstream clients,
    /// the listener-based refs collected during reconciliation are used instead.
    ///
    /// This is infallible: all required information (namespace, replicas and ports)
    /// is already resolved on `self` during validation.
    pub fn pod_refs(&self, role: &HdfsNodeRole) -> Vec<HdfsPodRef> {
        let ports: HashMap<String, u16> =
            crate::crd::role_data_ports(role, self.cluster_config.authentication.is_some())
                .into_iter()
                .collect();

        self.role_groups
            .get(role)
            .into_iter()
            .flatten()
            .flat_map(|(role_group_name, role_group)| {
                let object_name = format!("{}-{role}-{role_group_name}", self.name);
                let ports = ports.clone();
                (0..role_group.replicas.unwrap_or(1)).map(move |i| HdfsPodRef {
                    namespace: self.namespace.to_string(),
                    role_group_service_name: object_name.clone(),
                    pod_name: format!("{object_name}-{i}"),
                    ports: ports.clone(),
                    fqdn_override: None,
                })
            })
            .collect()
    }

    /// Builds the common [`ObjectMetaBuilder`] shared by a role group's owned resources
    /// (the ConfigMap and the StatefulSet): name, namespace, owner reference and the
    /// recommended labels, all derived from this validated cluster.
    ///
    /// This is infallible: a [`ValidatedCluster`] always carries a name, namespace and
    /// uid, and its fail-safe typed values always produce valid label values, so neither
    /// the owner reference nor the recommended labels can fail to build.
    pub fn rolegroup_metadata(
        &self,
        rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
    ) -> ObjectMetaBuilder {
        let mut metadata = ObjectMetaBuilder::new();
        metadata
            .name_and_namespace(self)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(self, None, Some(true))
            .expect(
                "the owner reference is valid because the ValidatedCluster has an \
                api_version, kind, name and uid",
            )
            .with_recommended_labels(&build_recommended_labels(
                self,
                RESOURCE_MANAGER_HDFS_CONTROLLER,
                &self.image.app_version_label_value,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .expect(
                "the recommended labels are valid because the ValidatedCluster uses \
                fail-safe typed values",
            );
        metadata
    }
}

/// Lets [`ValidatedCluster`] be used as the owner [`Resource`] (e.g. in
/// [`ObjectMetaBuilder::ownerreference_from_resource`]). The kind/group/version/plural
/// are delegated to [`v1alpha1::HdfsCluster`] so the generated owner references are
/// identical to the ones built from the raw cluster object.
impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::HdfsCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::HdfsCluster as Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HdfsCluster::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HdfsCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HdfsCluster::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HdfsCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// Cluster-wide settings resolved once during validation, so the build steps no
/// longer need the raw `HdfsCluster` to render config.
#[derive(Clone, Debug)]
pub struct ValidatedClusterConfig {
    /// The authentication config, if configured. Its presence enables both Kerberos
    /// and HTTPS; it also carries the TLS and Kerberos secret class names.
    pub authentication: Option<AuthenticationConfig>,
    /// The resolved OPA authorization config, if authorization is configured.
    pub authorization: Option<HdfsOpaConfig>,
    /// The replication factor.
    pub dfs_replication: u8,
    pub rack_awareness: Option<String>,
}

impl ValidatedClusterConfig {
    pub fn resolve(
        hdfs: &v1alpha1::HdfsCluster,
        authorization: Option<HdfsOpaConfig>,
    ) -> ValidatedClusterConfig {
        ValidatedClusterConfig {
            authentication: hdfs.authentication_config().cloned(),
            authorization,
            dfs_replication: hdfs.spec.cluster_config.dfs_replication,
            rack_awareness: hdfs.rackawareness_config(),
        }
    }
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}
