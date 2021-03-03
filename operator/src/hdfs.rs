use crate::error::Error;
use k8s_openapi::api::core::v1::{Node, Pod};
use stackable_hdfs_crd::HdfsVersion;

pub struct HdfsClusterBuilder {
    nodes_for_namenodes: Option<Vec<Node>>,
    nodes_for_datanodes: Option<Vec<Node>>,
    version: HdfsVersion,
}

impl HdfsClusterBuilder {
    pub fn new(version: HdfsVersion) -> Self {
        Self {
            nodes_for_namenodes: None,
            nodes_for_datanodes: None,
            version,
        }
    }

    pub fn set_namenode_nodes(&mut self, nodes: &Vec<Node>) {
        self.nodes_for_namenodes = Some(nodes.clone());
    }

    pub fn set_datanode_nodes(&mut self, nodes: &Vec<Node>) {
        self.nodes_for_datanodes = Some(nodes.clone());
    }

    pub fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    pub fn build(&self) -> Result<HdfsClusterDefinition, Error> {
        self.validate().map(|_| HdfsClusterDefinition {
            namenodes: self.nodes_for_namenodes.clone().unwrap_or_default(),
            datanodes: self.nodes_for_datanodes.clone().unwrap_or_default(),
            version: self.version.clone(),
        })
    }
}

pub struct HdfsClusterDefinition {
    namenodes: Vec<Node>,
    datanodes: Vec<Node>,
    version: HdfsVersion,
}

impl HdfsClusterDefinition {
    pub fn get_namenode_pod(node: &Node) -> Result<Pod, Error> {
        Ok(Pod {
            metadata: Default::default(),
            spec: None,
            status: None,
        })
    }
}
