use stackable_operator::{client, error};

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("HDFS_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache Hadoop HDFS");
    let client = client::create_client(Some("hdfs.stackable.tech".to_string())).await?;
    stackable_hdfs_operator::create_controller(client).await;
    Ok(())
}
