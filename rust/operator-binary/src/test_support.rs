use std::str::FromStr;

use stackable_operator::v2::types::operator::RoleGroupName;

use crate::{
    controller::{ValidatedCluster, ValidatedRoleGroupConfig, validate},
    crd::{AnyNodeConfig, DataNodeConfig, HdfsNodeRole, v1alpha1},
};

/// The expected `app.kubernetes.io/version` label value for the given product version.
///
/// The `-stackable` suffix carries the operator's own version, which is `0.0.0-dev` on main
/// but rewritten by the release process — so tests must derive it rather than hardcode it,
/// or they fail on release branches.
pub fn app_version_label(product_version: &str) -> String {
    format!(
        "{product_version}-stackable{}",
        crate::built_info::PKG_VERSION
    )
}

pub fn deserialize_cluster(spec: &str) -> v1alpha1::HdfsCluster {
    let deserializer = serde_yaml::Deserializer::from_str(spec);
    serde_yaml::with::singleton_map_recursive::deserialize(deserializer).expect("")
}

pub fn validate_cluster(hdfs: &v1alpha1::HdfsCluster) -> ValidatedCluster {
    validate::validate_cluster(
        hdfs,
        "oci.example.org",
        crate::controller::dereference::DereferencedObjects {
            hdfs_opa_config: None,
            namenode_listeners: vec![],
            discovery_config_map: None,
        },
    )
    .expect("cluster spec should be valid")
}

pub fn deserialize_and_validate_cluster(spec: &str) -> ValidatedCluster {
    validate_cluster(&deserialize_cluster(spec))
}

/// Parses a role group name for use in tests, panicking if it is invalid.
pub fn role_group_name(name: &str) -> RoleGroupName {
    RoleGroupName::from_str(name).expect("role group name should be valid")
}

pub fn role_group_config<'a>(
    validated_cluster: &'a ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> &'a ValidatedRoleGroupConfig {
    validated_cluster
        .role_groups
        .get(role)
        .expect("role should be defined")
        .get(role_group_name)
        .expect("role group should be defined")
}

pub fn anynode_config<'a>(
    validated_cluster: &'a ValidatedCluster,
    role: &HdfsNodeRole,
    role_group_name: &RoleGroupName,
) -> &'a AnyNodeConfig {
    &role_group_config(validated_cluster, role, role_group_name).config
}

pub fn datanode_config<'a>(
    validated_cluster: &'a ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> &'a DataNodeConfig {
    anynode_config(validated_cluster, &HdfsNodeRole::Data, role_group_name)
        .as_datanode()
        .expect("should be a DataNode")
}

/// A namenode pod `Listener` with a single ingress address, shaped as the dereference step
/// fetches it from the cluster. `name` must follow the `listener-<pod name>` convention
/// (see `crate::crd::pod_listener_name`) for `namenode_listener_refs` to find it.
pub fn namenode_listener(
    name: &str,
    address: &str,
    port: i32,
) -> stackable_operator::crd::listener::v1alpha1::Listener {
    use stackable_operator::crd::listener::v1alpha1 as listener;

    let mut namenode_listener = listener::Listener::new(name, listener::ListenerSpec::default());
    namenode_listener.status = Some(listener::ListenerStatus {
        service_name: None,
        ingress_addresses: Some(vec![listener::ListenerIngress {
            address: address.to_owned(),
            address_type: listener::AddressType::Hostname,
            ports: std::collections::BTreeMap::from([("rpc".to_string(), port)]),
        }]),
        node_ports: None,
    });
    namenode_listener
}
