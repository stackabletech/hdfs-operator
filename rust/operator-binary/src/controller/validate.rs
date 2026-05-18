use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::GenericRoleConfig,
};

use crate::{
    crd::{AnyNodeConfig, HdfsNodeRole, v1alpha1},
    hdfs_controller::{CONTAINER_IMAGE_BASE_NAME, ValidatedCluster},
    security::opa::HdfsOpaConfig,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid role properties"))]
    RoleProperties { source: crate::crd::Error },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product configuration"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("could not parse HDFS role [{role}]"))]
    UnidentifiedHdfsRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}

/// Per-rolegroup configuration: the merged CRD config plus the product-config properties.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: AnyNodeConfig,
    pub product_config_properties: HashMap<PropertyNameKind, BTreeMap<String, String>>,
}

pub fn validate_cluster(
    hdfs: &v1alpha1::HdfsCluster,
    image_repository: &str,
    product_config_manager: &ProductConfigManager,
    hdfs_opa_config: Option<HdfsOpaConfig>,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = hdfs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let roles = hdfs
        .build_role_properties()
        .context(RolePropertiesSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(hdfs, &roles).context(GenerateProductConfigSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    for (role_name, group_config) in validated_config.iter() {
        let hdfs_role = HdfsNodeRole::from_str(role_name).context(UnidentifiedHdfsRoleSnafu {
            role: role_name.to_string(),
        })?;

        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = hdfs.role_config(&hdfs_role)
        {
            role_configs.insert(hdfs_role, ValidatedRoleConfig { pdb: pdb.clone() });
        }

        let mut group_configs = BTreeMap::new();
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let merged_config = hdfs_role
                .merged_config(hdfs, rolegroup_name)
                .context(FailedToResolveConfigSnafu)?;

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    product_config_properties: rolegroup_config.clone(),
                },
            );
        }

        role_groups.insert(hdfs_role, group_configs);
    }

    Ok(ValidatedCluster {
        image: resolved_product_image,
        role_groups,
        role_configs,
        hdfs_opa_config,
    })
}
