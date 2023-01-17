use std::collections::BTreeMap;

use crate::constants::*;
use serde::{Deserialize, Serialize};
use stackable_operator::config::merge::{Atomic, Merge};
use stackable_operator::{
    commons::resources::PvcConfig,
    config::fragment::Fragment,
    k8s_openapi::api::core::v1::PersistentVolumeClaim,
    schemars::{self, JsonSchema},
};

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct HdfsStorageConfig {
    #[fragment_attrs(serde(default))]
    pub data: PvcConfig,
}

// We can't use a struct with a BTreeMap attribute that is #[fragment_attrs(serde(flatten))],
// as all of the `storage` additionalProperties will be missing in the generated CRD for some reasons.
//
// This will result in errors such as
// Warning: unknown field "spec.dataNodes.config.resources.storage.data"
// Warning: unknown field "spec.dataNodes.config.resources.storage.hdd"
// Warning: unknown field "spec.dataNodes.config.resources.storage.ssd"
// making it impossible to configure multiple pvcs
//
// So we define a type for the inner BTreeMap and a struct with some helper functions.
pub type DataNodeStorageConfigInnerType = BTreeMap<String, DataNodePvc>;

/// Use this struct to call functions on the `DataNodeStorageConfigInnerType` type.
pub struct DataNodeStorageConfig {
    pub pvcs: DataNodeStorageConfigInnerType,
}

impl DataNodeStorageConfig {
    /// Builds a list of pvcs with the length being `self.number_of_data_pvcs`.
    /// The spec - such as size, storageClass or selector - is used from the regular `PvcConfig` struct used for the `data` attribute.
    pub fn build_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let mut pvcs = vec![];

        for (pvc_name_prefix, pvc) in &self.pvcs {
            let disk_pvc_template = pvc
                .pvc
                .build_pvc(pvc_name_prefix, Some(vec!["ReadWriteOnce"]));

            pvcs.extend(
                Self::pvc_names(pvc_name_prefix, pvc.count)
                    .into_iter()
                    .map(|pvc_name| {
                        let mut pvc = disk_pvc_template.clone();
                        pvc.metadata.name = Some(pvc_name);
                        pvc
                    }),
            )
        }
        pvcs
    }

    /// Returns the a String to be put into `dfs.datanode.data.dir`.
    /// The config will include the HDFS storage type.
    pub fn get_datanode_data_dir(&self) -> String {
        self.pvcs
            .iter()
            .flat_map(|(pvc_name_prefix, pvc)| {
                Self::pvc_names(pvc_name_prefix, pvc.count)
                    .into_iter()
                    .map(|pvc_name| {
                        let storage_type = pvc.hdfs_storage_type.as_hdfs_config_literal();
                        format!(
                            "[{storage_type}]{DATANODE_ROOT_DATA_DIR_PREFIX}{pvc_name}{DATANODE_ROOT_DATA_DIR_SUFFIX}"
                        )
                    })
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns a list with the names of the pvcs to be used for nodes.
    /// The pvc names will be prefixed with the name given in the `pvc_name_prefix` argument.
    ///
    /// There are two options when it comes to naming:
    /// 1. Name the pvcs `data-0, data-1, data-2, ... , data-{n-1}`
    /// ** Good, because consistent naming
    /// ** Bad, because existing deployments (using release 22.11 or earlier) will need to migrate their data by renaming their pvcs
    ///
    /// 2. Name the pvcs `data, data-1, data-2, ... , data-{n-1}`
    /// ** Good, if nodes only have a single pvc (which probably most of the deployments will have) they name of the pvc will be consistent with the name of all the other pvcs out there
    /// ** It is important that the first pvc will be called `data` (without suffix), regardless of the number of pvcs to be used. This is needed as nodes should be able to alter the number of pvcs attached and the use-case number of pvcs 1 -> 2 should be supported without renaming pvcs.
    ///
    /// This function uses the 2. option.
    pub fn pvc_names(pvc_name_prefix: &str, number_of_pvcs: u16) -> Vec<String> {
        (0..number_of_pvcs)
            .map(|pvc_index| {
                if pvc_index == 0 {
                    pvc_name_prefix.to_string()
                } else {
                    format!("{pvc_name_prefix}-{pvc_index}")
                }
            })
            .collect()
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct DataNodePvc {
    #[fragment_attrs(serde(default, flatten))]
    pub pvc: PvcConfig,

    #[serde(default = "default_number_of_datanode_pvcs")]
    pub count: u16,

    #[fragment_attrs(serde(default))]
    pub hdfs_storage_type: HdfsStorageType,
}

fn default_number_of_datanode_pvcs() -> u16 {
    1
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum HdfsStorageType {
    Archive,
    Disk,
    #[serde(rename = "SSD")]
    Ssd,
    #[serde(rename = "RAMDisk")]
    RamDisk,
}

impl Atomic for HdfsStorageType {}

impl Default for HdfsStorageType {
    fn default() -> Self {
        Self::Disk
    }
}

impl HdfsStorageType {
    pub fn as_hdfs_config_literal(&self) -> &str {
        match self {
            HdfsStorageType::Archive => "ARCHIVE",
            HdfsStorageType::Disk => "DISK",
            HdfsStorageType::Ssd => "SSD",
            HdfsStorageType::RamDisk => "RAM_DISK",
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use stackable_operator::{
        commons::resources::PvcConfig,
        k8s_openapi::{
            api::core::v1::{PersistentVolumeClaimSpec, ResourceRequirements},
            apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
        },
    };

    use super::{DataNodePvc, DataNodeStorageConfig, HdfsStorageType};

    #[test]
    pub fn test_datanode_storage_defaults() {
        let data_node_storage = DataNodeStorageConfig {
            pvcs: BTreeMap::from([(
                "data".to_string(),
                DataNodePvc {
                    pvc: PvcConfig {
                        capacity: Some(Quantity("5Gi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                    count: 1,
                    hdfs_storage_type: HdfsStorageType::default(),
                },
            )]),
        };

        let pvcs = data_node_storage.build_pvcs();
        let datanode_data_dir = data_node_storage.get_datanode_data_dir();

        assert_eq!(pvcs.len(), 1);
        assert_eq!(pvcs[0].metadata.name, Some("data".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("5Gi".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: None,
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(datanode_data_dir, "[DISK]/stackable/data/data/datanode");
    }

    #[test]
    pub fn test_datanode_storage_multiple_storage_types() {
        let data_node_storage = DataNodeStorageConfig {
            pvcs: BTreeMap::from([
                (
                    "hdd".to_string(),
                    DataNodePvc {
                        pvc: PvcConfig {
                            capacity: Some(Quantity("12Ti".to_owned())),
                            storage_class: Some("hdd-storage-class".to_string()),
                            selectors: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([(
                                    "foo".to_string(),
                                    "bar".to_string(),
                                )])),
                            }),
                        },
                        count: 8,
                        hdfs_storage_type: HdfsStorageType::Disk,
                    },
                ),
                (
                    "ssd".to_string(),
                    DataNodePvc {
                        pvc: PvcConfig {
                            capacity: Some(Quantity("2Ti".to_owned())),
                            storage_class: Some("premium-ssd".to_string()),
                            selectors: None,
                        },
                        count: 4,
                        hdfs_storage_type: HdfsStorageType::Ssd,
                    },
                ),
            ]),
        };
        let pvcs = data_node_storage.build_pvcs();
        let datanode_data_dir = data_node_storage.get_datanode_data_dir();

        assert_eq!(pvcs.len(), 8 + 4);
        assert_eq!(pvcs[0].metadata.name, Some("hdd".to_string()));
        assert_eq!(
            pvcs[0].spec,
            Some(PersistentVolumeClaimSpec {
                resources: Some(ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("12Ti".to_string())
                    )])),
                    ..ResourceRequirements::default()
                }),
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: Some("hdd-storage-class".to_string()),
                selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(BTreeMap::from([("foo".to_string(), "bar".to_string())]))
                }),
                ..PersistentVolumeClaimSpec::default()
            })
        );
        assert_eq!(datanode_data_dir, "[DISK]/stackable/data/hdd/datanode,[DISK]/stackable/data/hdd-1/datanode,[DISK]/stackable/data/hdd-2/datanode,[DISK]/stackable/data/hdd-3/datanode,[DISK]/stackable/data/hdd-4/datanode,[DISK]/stackable/data/hdd-5/datanode,[DISK]/stackable/data/hdd-6/datanode,[DISK]/stackable/data/hdd-7/datanode,[SSD]/stackable/data/ssd/datanode,[SSD]/stackable/data/ssd-1/datanode,[SSD]/stackable/data/ssd-2/datanode,[SSD]/stackable/data/ssd-3/datanode")
    }
}
