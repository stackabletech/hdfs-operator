use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::LabelError,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
        },
        types::operator::RoleGroupName,
    },
};

use crate::{
    build_recommended_labels,
    controller::{ValidatedCluster, build},
    crd::HdfsNodeRole,
    hdfs_controller::RESOURCE_MANAGER_HDFS_CONTROLLER,
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

pub(crate) fn rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Service, Error> {
    tracing::info!("Setting up headless Service for role {role} role group {role_group_name}");

    let resource_names = cluster.resource_names(role, role_group_name);
    let role_name = role.to_string();
    // TODO: The v2 `ResourceNames::headless_service_name()` would add a `-headless` suffix, but
    // we deliberately keep the un-suffixed name here so the StatefulSet's (immutable) `serviceName`
    // and the pod DNS names stay unchanged for existing clusters. A decision is needed on whether
    // to adopt the suffixed name (requires StatefulSet recreation on upgrade).
    let mut metadata_builder = ObjectMetaBuilder::new();
    metadata_builder
        .name_and_namespace(cluster)
        .name(resource_names.qualified_role_group_name())
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_recommended_labels(&build_recommended_labels(
            cluster,
            RESOURCE_MANAGER_HDFS_CONTROLLER,
            &cluster.image.app_version_label_value,
            &role_name,
            role_group_name.as_ref(),
        ))
        .context(ObjectMetaSnafu)?;

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
        metadata: metadata_builder.build(),
        spec: Some(service_spec),
        status: None,
    })
}

pub(crate) fn rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> Result<Service, Error> {
    tracing::info!("Setting up metrics Service for role {role} role group {role_group_name}");

    let resource_names = cluster.resource_names(role, role_group_name);
    let role_name = role.to_string();

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
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(resource_names.metrics_service_name())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_recommended_labels(&build_recommended_labels(
                cluster,
                RESOURCE_MANAGER_HDFS_CONTROLLER,
                &cluster.image.app_version_label_value,
                &role_name,
                role_group_name.as_ref(),
            ))
            .context(ObjectMetaSnafu)?
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
