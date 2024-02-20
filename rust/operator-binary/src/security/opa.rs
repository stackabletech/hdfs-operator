use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{security::HdfsAuthorization, HdfsCluster};
use stackable_operator::{
    client::Client,
    commons::{opa::OpaApiVersion, product_image_selection::ResolvedProductImage},
};

use crate::config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "HDFS 2.2.x does not support OPA authorization (you are using {hdfs_version})"
    ))]
    OPANotSupported { hdfs_version: String },

    #[snafu(display("failed to construct OPA endpoint URL for authorizer"))]
    ConstructOpaEndpointForAuthorizer {
        source: stackable_operator::error::Error,
    },

    #[snafu(display("failed to construct OPA endpoint URL for group mapper"))]
    ConstructOpaEndpointForGroupMapper {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct HdfsOpaConfig {
    authorization_connection_string: String,
    group_mapper_connection_string: Option<String>,
}

impl HdfsOpaConfig {
    pub async fn from_opa_config(
        client: &Client,
        hdfs: &HdfsCluster,
        resolved_product_image: &ResolvedProductImage,
        authorization_config: &HdfsAuthorization,
    ) -> Result<Self> {
        if resolved_product_image.product_version.starts_with("2.2.") {
            OPANotSupportedSnafu {
                hdfs_version: resolved_product_image.product_version.clone(),
            }
            .fail()?;
        }

        let authorization_connection_string = authorization_config
            .opa_authorization
            .full_document_url_from_config_map(client, hdfs, Some("allow"), OpaApiVersion::V1)
            .await
            .context(ConstructOpaEndpointForAuthorizerSnafu)?;
        let group_mapper_connection_string = match &authorization_config.opa_group_mapping {
            Some(group_mapping) => Some(
                group_mapping
                    .full_document_url_from_config_map(
                        client,
                        hdfs,
                        Some("groups"),
                        OpaApiVersion::V1,
                    )
                    .await
                    .context(ConstructOpaEndpointForGroupMapperSnafu)?,
            ),
            None => None,
        };

        Ok(HdfsOpaConfig {
            authorization_connection_string,
            group_mapper_connection_string,
        })
    }

    pub fn add_hdfs_site_config(&self, config: &mut HdfsSiteConfigBuilder) {
        config.add(
            "dfs.namenode.inode.attributes.provider.class",
            "tech.stackable.hadoop.StackableAuthorizer",
        );
    }

    pub fn add_core_site_config(&self, config: &mut CoreSiteConfigBuilder) {
        config.add(
            "hadoop.security.authorization.opa.policy.url",
            &self.authorization_connection_string,
        );
        if let Some(group_mapper_connection_string) = &self.group_mapper_connection_string {
            config.add(
                "hadoop.security.group.mapping",
                "tech.stackable.hadoop.StackableGroupMapper",
            );
            config.add(
                "hadoop.security.group.mapping.opa.policy.url",
                group_mapper_connection_string,
            );
        }
    }
}
