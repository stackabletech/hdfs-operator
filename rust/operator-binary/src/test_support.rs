use std::str::FromStr;

use stackable_operator::v2::types::operator::RoleGroupName;

use crate::{
    controller::{ValidatedCluster, ValidatedRoleGroupConfig, validate},
    crd::{AnyNodeConfig, DataNodeConfig, HdfsNodeRole, v1alpha1},
};

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
