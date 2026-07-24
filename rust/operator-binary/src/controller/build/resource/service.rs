//! Builds the per-rolegroup `Service`s for the HdfsCluster: a headless `Service`
//! backing the `StatefulSet` pod DNS, and a metrics `Service` carrying the
//! Prometheus scrape labels/annotations.
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::LabelError,
    v2::{
        builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{ValidatedCluster, build},
    crd::HdfsNodeRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: LabelError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Service> {
    tracing::info!("Setting up headless Service for role {role} role group {role_group_name}");

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(
            build::headless_service_ports(cluster, role)
                .into_iter()
                .map(|(name, value)| ServicePort {
                    name: Some(name),
                    port: i32::from(value),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                })
                .collect(),
        ),
        selector: Some(
            build::rolegroup_selector_labels(cluster, role, role_group_name)
                .context(RoleGroupSelectorLabelsSnafu)?
                .into(),
        ),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata: build::object_meta(
            cluster,
            cluster
                .governing_service_name(role, role_group_name)
                .to_string(),
            cluster.recommended_labels(role, role_group_name),
        )
        .build(),
        spec: Some(service_spec),
        status: None,
    })
}

pub(crate) fn rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Service> {
    tracing::info!("Setting up metrics Service for role {role} role group {role_group_name}");

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(
            build::metrics_service_ports(cluster, role)
                .into_iter()
                .map(|(name, value)| ServicePort {
                    name: Some(name),
                    port: i32::from(value),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                })
                .collect(),
        ),
        selector: Some(
            build::rolegroup_selector_labels(cluster, role, role_group_name)
                .context(RoleGroupSelectorLabelsSnafu)?
                .into(),
        ),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata: build::object_meta(
            cluster,
            cluster
                .role_group_resource_names(role, role_group_name)
                .metrics_service_name()
                .to_string(),
            cluster.recommended_labels(role, role_group_name),
        )
        .with_labels(prometheus_labels(&Scraping::Enabled))
        .with_annotations(prometheus_annotations(
            &Scraping::Enabled,
            if cluster.has_https_enabled() {
                &Scheme::Https
            } else {
                &Scheme::Http
            },
            "/prom",
            &build::native_metrics_port(cluster, role),
        ))
        .build(),
        spec: Some(service_spec),
        status: None,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::{
        controller::build::properties::test_support::validated_cluster, crd::HdfsNodeRole,
    };

    #[test]
    fn test_rolegroup_metrics_service() {
        let cluster = validated_cluster();
        let role = &HdfsNodeRole::Name;
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");

        let service =
            rolegroup_metrics_service(&cluster, role, &role_group_name).expect("should not fail");

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    // Every metrics Service must carry the Prometheus scrape label and the
                    // `prometheus.io/path|port|scheme|scrape` annotations, or Prometheus stops
                    // discovering the endpoints.
                    "annotations": {
                        "prometheus.io/path": "/prom",
                        "prometheus.io/port": "9870",
                        "prometheus.io/scheme": "http",
                        "prometheus.io/scrape": "true"
                    },
                    "labels": {
                        "app.kubernetes.io/component": "namenode",
                        "app.kubernetes.io/instance": "hdfs",
                        "app.kubernetes.io/managed-by": "hdfs.stackable.tech_hdfs-operator-hdfs-controller",
                        "app.kubernetes.io/name": "hdfs",
                        "app.kubernetes.io/role-group": "default",
                        "app.kubernetes.io/version": "3.4.0-stackable0.0.0-dev",
                        "prometheus.io/scrape": "true",
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "hdfs-namenode-default-metrics",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "hdfs.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "HdfsCluster",
                            "name": "hdfs",
                            "uid": "c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f"
                        }
                    ]
                },
                "spec": {
                    "clusterIP": "None",
                    "ports": [
                        {
                            "name": "metrics",
                            "port": 9870,
                            "protocol": "TCP"
                        },
                        {
                            "name": "jmx-metrics",
                            "port": 8183,
                            "protocol": "TCP"
                        }
                    ],
                    "publishNotReadyAddresses": true,
                    "selector": {
                        "app.kubernetes.io/component": "namenode",
                        "app.kubernetes.io/instance": "hdfs",
                        "app.kubernetes.io/name": "hdfs",
                        "app.kubernetes.io/role-group": "default",
                        "group": "default",
                        "role": "namenode"
                    },
                    "type": "ClusterIP"
                }
            }),
            serde_json::to_value(service).expect("must be serializable")
        );
    }
}
