use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{security::AuthorizationConfig, HdfsCluster};
use stackable_operator::{client::Client, commons::opa::OpaApiVersion};

use crate::config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct OPA endpoint URL for authorizer"))]
    ConstructOpaEndpointForAuthorizer {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct HdfsOpaConfig {
    authorization_connection_string: String,
}

impl HdfsOpaConfig {
    pub async fn from_opa_config(
        client: &Client,
        hdfs: &HdfsCluster,
        authorization_config: &AuthorizationConfig,
    ) -> Result<Self> {
        let authorization_connection_string = authorization_config
            .opa
            .full_document_url_from_config_map(client, hdfs, Some("allow"), OpaApiVersion::V1)
            .await
            .context(ConstructOpaEndpointForAuthorizerSnafu)?;

        Ok(HdfsOpaConfig {
            authorization_connection_string,
        })
    }

    /// Add all the needed configurations to `hdfs-site.xml`
    pub fn add_hdfs_site_config(&self, config: &mut HdfsSiteConfigBuilder) {
        config.add(
            "dfs.namenode.inode.attributes.provider.class",
            "tech.stackable.hadoop.StackableAuthorizer",
        );
    }

    /// Add all the needed configurations to `core-site.xml`
    pub fn add_core_site_config(&self, config: &mut CoreSiteConfigBuilder) {
        config.add(
            "hadoop.security.authorization.opa.policy.url",
            &self.authorization_connection_string,
        );
    }
}
