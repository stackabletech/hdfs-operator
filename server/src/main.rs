use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::{client, error};

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("HDFS_OPERATOR_LOG");

    let client = client::create_client(Some("hdfs.stackable.tech".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<HdfsCluster>(client.clone()).await?;

    stackable_hdfs_operator::create_controller(client).await;
    Ok(())
}
