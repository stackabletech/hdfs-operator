use stackable_hdfs_crd::commands::{Format, Restart, Start, Stop};
use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    HdfsCluster::write_yaml_schema("../../deploy/crd/hdfscluster.crd.yaml")?;
    Restart::write_yaml_schema("../../deploy/crd/restart.crd.yaml")?;
    Start::write_yaml_schema("../../deploy/crd/start.crd.yaml")?;
    Stop::write_yaml_schema("../../deploy/crd/stop.crd.yaml")?;
    Format::write_yaml_schema("../../deploy/crd/format.crd.yaml")?;

    Ok(())
}
