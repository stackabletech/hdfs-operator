use std::{collections::BTreeMap, str::FromStr};

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{Resource, api::ObjectMeta},
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label::recommended_labels,
        role_group_utils::ResourceNames,
        role_utils::{self, RoleGroupConfig},
        types::{
            kubernetes::{ConfigMapName, NamespaceName, ServiceName, Uid},
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion,
                RoleGroupName, RoleName,
            },
        },
    },
};

use crate::{
    OPERATOR_NAME,
    controller::build::opa::HdfsOpaConfig,
    crd::{
        AnyNodeConfig, HdfsNodeRole, UpgradeState, constants::APP_NAME,
        security::AuthenticationConfig, v1alpha1,
    },
    hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER,
};

pub mod build;
pub mod dereference;
pub mod validate;

/// Every Kubernetes resource produced by the build step.
///
/// The resources are flat, unordered collections. The reconcile step re-groups the
/// StatefulSets by role to preserve HDFS's ordered, rollout-gated deployment during
/// upgrades. The discovery `ConfigMap` is not part of this set: it depends on a live
/// Kubernetes client (to resolve listener addresses) and is therefore built and applied
/// separately in the reconcile step.
pub struct KubernetesResources {
    pub services: Vec<Service>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub stateful_sets: Vec<StatefulSet>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
}

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
    /// The cluster's Kubernetes UID, used to build owner references.
    pub uid: Uid,
    pub product_version: ProductVersion,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_groups: BTreeMap<HdfsNodeRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HdfsNodeRole, ValidatedRoleConfig>,
    /// The validated view of the cluster's current status, resolved once during
    /// validation.
    pub status: ValidatedClusterStatus,
}

impl ValidatedCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_groups: BTreeMap<HdfsNodeRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
        role_configs: BTreeMap<HdfsNodeRole, ValidatedRoleConfig>,
        status: ValidatedClusterStatus,
    ) -> Self {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a valid
        // `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
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
            uid,
            image,
            product_version,
            cluster_config,
            role_groups,
            role_configs,
            status,
        }
    }

    /// Whether HTTPS is enabled, derived from the validated authentication settings.
    pub fn has_https_enabled(&self) -> bool {
        self.cluster_config.authentication.is_some()
    }

    /// Whether Kerberos is enabled, derived from the validated authentication settings.
    pub fn has_kerberos_enabled(&self) -> bool {
        self.cluster_config.authentication.is_some()
    }

    /// The validated authentication config, if authentication is enabled.
    pub fn authentication_config(&self) -> Option<&AuthenticationConfig> {
        self.cluster_config.authentication.as_ref()
    }

    /// The resolved rack awareness label list, if rack awareness is configured.
    pub fn rackawareness_config(&self) -> Option<String> {
        self.cluster_config.rack_awareness.clone()
    }

    /// The type-safe role name for an HDFS role (`namenode`/`datanode`/`journalnode`).
    pub(crate) fn role_name(role: &HdfsNodeRole) -> RoleName {
        RoleName::from_str(&role.to_string()).expect("a HdfsNodeRole is a valid role name")
    }

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn rbac_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: product_name(),
        }
    }

    /// Type-safe names for the resources of the given role group.
    pub(crate) fn resource_names(
        &self,
        role: &HdfsNodeRole,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(role),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Recommended labels for a resource that is not tied to a concrete [`HdfsNodeRole`], using a free-form role/role-group label value.
    pub fn recommended_labels_for(
        &self,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_with(&self.product_version, role_name, role_group_name)
    }

    fn recommended_labels_with(
        &self,
        product_version: &ProductVersion,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            role_name,
            role_group_name,
        )
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(
        &self,
        role: &HdfsNodeRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_for(&Self::role_name(role), role_group_name)
    }

    /// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, the resource `name`, an owner
    /// reference back to this cluster, and the given recommended `labels`.
    pub(crate) fn object_meta(&self, name: impl Into<String>, labels: Labels) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(labels);
        builder
    }

    /// The name of a role group's governing headless Service.
    ///
    /// Used as the headless Service's own name, as the StatefulSet's (immutable) `serviceName`,
    /// and for the pod DNS references derived from them; the three reference each other, so they
    /// must be derived here and nowhere else.
    //
    // TODO: The v2 `ResourceNames::headless_service_name()` would add a `-headless` suffix, but we
    // deliberately keep the un-suffixed name so the StatefulSet's (immutable) `serviceName` and the
    // pod DNS names stay unchanged for existing clusters. A decision is needed on whether to adopt
    // the suffixed name (requires StatefulSet recreation on upgrade).
    pub(crate) fn governing_service_name(
        &self,
        role: &HdfsNodeRole,
        role_group_name: &RoleGroupName,
    ) -> ServiceName {
        ServiceName::from_str(
            self.resource_names(role, role_group_name)
                .qualified_role_group_name()
                .as_ref(),
        )
        .expect("a qualified role group name is a valid Service name")
    }
}

/// The product name (`hdfs`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'hdfs' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(RESOURCE_MANAGER_HDFS_CONTROLLER)
        .expect("the controller name is a valid label value")
}

/// Lets [`ValidatedCluster`] be used as the owner [`Resource`]. The kind/group/version/plural
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

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

/// The validated view of the cluster's current status, resolved once during
/// validation from the (mutable) [`v1alpha1::HdfsCluster::status`] so the controller
/// reasons about upgrades from a typed snapshot rather than re-reading the status.
#[derive(Clone, Debug)]
pub struct ValidatedClusterStatus {
    /// The current upgrade state (`None` unless the cluster is mid up/downgrade).
    pub upgrade_state: Option<UpgradeState>,
    /// The product version currently deployed (during upgrades this is the *old*
    /// version), or `None` on a fresh install.
    pub deployed_product_version: Option<String>,
    /// The product version currently being upgraded to, otherwise `None`.
    pub upgrade_target_product_version: Option<String>,
}

/// The validated logging configuration for the cluster.
#[derive(Clone, Debug)]
pub struct ValidatedLogging {
    /// The name of the Vector aggregator discovery `ConfigMap`, if log aggregation
    /// is configured.
    pub vector_aggregator_config_map_name: Option<ConfigMapName>,
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
    /// The validated logging configuration.
    pub logging: ValidatedLogging,
    /// The name of the ZooKeeper discovery `ConfigMap`.
    pub zookeeper_config_map_name: ConfigMapName,
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
            zookeeper_config_map_name: hdfs.spec.cluster_config.zookeeper_config_map_name.clone(),
            logging: ValidatedLogging {
                vector_aggregator_config_map_name: hdfs
                    .spec
                    .cluster_config
                    .vector_aggregator_config_map_name
                    .clone(),
            },
        }
    }
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}
