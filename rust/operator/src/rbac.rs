use stackable_operator::builder::ObjectMetaBuilder;
use stackable_operator::k8s_openapi::api::core::v1::ServiceAccount;
use stackable_operator::k8s_openapi::api::rbac::v1::{RoleBinding, RoleRef, Subject};
use stackable_operator::kube::{Resource, ResourceExt};

/// Used as runAsUser in the pod security context. This is specified in the Hadoop image file
pub const HDFS_UID: i64 = 1000;

/// Build RBAC objects for the product workloads.
/// The `rbac_prefix` is meant to be the product name, for example: zookeeper, airflow, etc.
/// and it is a assumed that a ClusterRole named `{rbac_prefix}-clusterrole` exists.
pub fn build_rbac_resources<T: Resource>(
    resource: &T,
    rbac_prefix: &str,
) -> (ServiceAccount, RoleBinding) {
    let sa_name = format!("{rbac_prefix}-sa");
    let service_account = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(resource)
            .name(sa_name.clone())
            .build(),
        ..ServiceAccount::default()
    };

    let role_binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(resource)
            .name(format!("{rbac_prefix}-rolebinding"))
            .build(),
        role_ref: RoleRef {
            kind: "ClusterRole".to_string(),
            name: format!("{rbac_prefix}-clusterrole"),
            api_group: "rbac.authorization.k8s.io".to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: sa_name,
            namespace: resource.namespace(),
            ..Subject::default()
        }]),
    };

    (service_account, role_binding)
}
