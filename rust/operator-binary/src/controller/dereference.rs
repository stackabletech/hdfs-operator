use snafu::{ResultExt, Snafu};

use crate::{controller::build::opa::HdfsOpaConfig, crd::v1alpha1};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig {
        source: crate::controller::build::opa::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub hdfs_opa_config: Option<HdfsOpaConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hdfs: &v1alpha1::HdfsCluster,
) -> Result<DereferencedObjects, Error> {
    let hdfs_opa_config = match &hdfs.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HdfsOpaConfig::from_opa_config(client, hdfs, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    Ok(DereferencedObjects { hdfs_opa_config })
}
