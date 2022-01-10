use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    HdfsCluster::write_yaml_schema("../../deploy/crd/hdfscluster.crd.yaml")?;

    Ok(())
}
