use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Label, LabelError},
    role_utils::RoleGroupRef,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    build_recommended_labels,
    controller::ValidatedCluster,
    crd::{HdfsNodeRole, v1alpha1},
    hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build prometheus label"))]
    BuildPrometheusLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build roleGroup selector labels"))]
    RoleGroupSelectorLabels { source: crate::crd::Error },
}

pub(crate) fn rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
) -> Result<Service, Error> {
    tracing::info!("Setting up Service for {:?}", rolegroup_ref);

    let mut metadata_builder = ObjectMetaBuilder::new();
    metadata_builder
        .name_and_namespace(cluster)
        .name(rolegroup_ref.object_name())
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_recommended_labels(&build_recommended_labels(
            cluster,
            RESOURCE_MANAGER_HDFS_CONTROLLER,
            &cluster.image.app_version_label_value,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?;

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(
            cluster
                .headless_service_ports(role)
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
            cluster
                .rolegroup_selector_labels(rolegroup_ref)
                .context(RoleGroupSelectorLabelsSnafu)?
                .into(),
        ),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata: metadata_builder.build(),
        spec: Some(service_spec),
        status: None,
    })
}

pub(crate) fn rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HdfsCluster>,
) -> Result<Service, Error> {
    tracing::info!("Setting up metrics Service for {:?}", rolegroup_ref);

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(
            cluster
                .metrics_service_ports(role)
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
            cluster
                .rolegroup_selector_labels(rolegroup_ref)
                .context(RoleGroupSelectorLabelsSnafu)?
                .into(),
        ),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(rolegroup_ref.rolegroup_metrics_service_name())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_recommended_labels(&build_recommended_labels(
                cluster,
                RESOURCE_MANAGER_HDFS_CONTROLLER,
                &cluster.image.app_version_label_value,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .context(ObjectMetaSnafu)?
            .with_label(
                Label::try_from(("prometheus.io/scrape", "true"))
                    .context(BuildPrometheusLabelSnafu)?,
            )
            .with_annotations(
                Annotations::try_from([
                    ("prometheus.io/path".to_owned(), "/prom".to_owned()),
                    (
                        "prometheus.io/port".to_owned(),
                        cluster.native_metrics_port(role).to_string(),
                    ),
                    (
                        "prometheus.io/scheme".to_owned(),
                        if cluster.has_https_enabled() {
                            "https".to_owned()
                        } else {
                            "http".to_owned()
                        },
                    ),
                    ("prometheus.io/scrape".to_owned(), "true".to_owned()),
                ])
                .expect("should be valid annotations"),
            )
            .build(),
        spec: Some(service_spec),
        status: None,
    })
}
