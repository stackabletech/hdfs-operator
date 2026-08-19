//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    v2::rbac,
};

use crate::controller::{ValidatedCluster, build::recommended_labels_for_cluster_resources};

/// Builds the [`ServiceAccount`] that the role-group Pods run under.
pub fn build_service_account(cluster: &ValidatedCluster) -> ServiceAccount {
    rbac::build_service_account(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}

/// Builds the [`RoleBinding`] that binds the [`ServiceAccount`] from [`build_service_account`] to
/// the operator-deployed ClusterRole.
pub fn build_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    rbac::build_role_binding(
        cluster,
        &cluster.cluster_resource_names(),
        recommended_labels_for_cluster_resources(cluster),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        controller::build::properties::test_support::MINIMAL_HDFS_YAML,
        test_support::{app_version_label, deserialize_and_validate_cluster},
    };

    /// The cluster name is deliberately different from the product name (`hdfs`) so that
    /// swapped `name`/`instance` label values cannot pass unnoticed.
    fn swap_guard_cluster() -> crate::controller::ValidatedCluster {
        deserialize_and_validate_cluster(&MINIMAL_HDFS_YAML.replace("name: hdfs", "name: my-hdfs"))
    }

    #[test]
    fn test_service_account() {
        let service_account = build_service_account(&swap_guard_cluster());

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "my-hdfs",
                        "app.kubernetes.io/managed-by": "hdfs.stackable.tech_hdfs-controller",
                        "app.kubernetes.io/name": "hdfs",
                        "app.kubernetes.io/version": app_version_label("3.4.0"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-hdfs-serviceaccount",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "hdfs.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "HdfsCluster",
                            "name": "my-hdfs",
                            "uid": "c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f"
                        }
                    ]
                }
            }),
            serde_json::to_value(service_account).expect("must be serializable")
        );
    }

    #[test]
    fn test_role_binding() {
        let role_binding = build_role_binding(&swap_guard_cluster());

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "my-hdfs",
                        "app.kubernetes.io/managed-by": "hdfs.stackable.tech_hdfs-controller",
                        "app.kubernetes.io/name": "hdfs",
                        "app.kubernetes.io/version": app_version_label("3.4.0"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-hdfs-rolebinding",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "hdfs.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "HdfsCluster",
                            "name": "my-hdfs",
                            "uid": "c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f"
                        }
                    ]
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "hdfs-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "my-hdfs-serviceaccount",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(role_binding).expect("must be serializable")
        );
    }
}
