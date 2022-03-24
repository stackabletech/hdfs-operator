use stackable_hdfs_crd::constants::{
    DEFAULT_JOURNAL_NODE_RPC_PORT, DEFAULT_NAME_NODE_HTTP_PORT, DEFAULT_NAME_NODE_RPC_PORT,
};
use stackable_hdfs_crd::HdfsPodRef;
use std::collections::BTreeMap;

// hdfs-site.xml
pub const DFS_NAMENODE_NAME_DIR: &str = "dfs.namenode.name.dir";
pub const DFS_NAMENODE_SHARED_EDITS_DIR: &str = "dfs.namenode.shared.edits.dir";
pub const DFS_NAMENODE_RPC_ADDRESS: &str = "dfs.namenode.rpc-address";
pub const DFS_NAMENODE_HTTP_ADDRESS: &str = "dfs.namenode.http-address";
pub const DFS_DATANODE_DATA_DIR: &str = "dfs.datanode.data.dir";
pub const DFS_JOURNALNODE_EDITS_DIR: &str = "dfs.journalnode.edits.dir";
pub const DFS_JOURNALNODE_RPC_ADDRESS: &str = "dfs.journalnode.rpc-address";
pub const DFS_REPLICATION: &str = "dfs.replication";
pub const DFS_NAME_SERVICES: &str = "dfs.nameservices";

// core-site.xml
pub const FS_DEFAULT_FS: &str = "fs.defaultFS";
pub const HA_ZOOKEEPER_QUORUM: &str = "ha.zookeeper.quorum";

#[derive(Clone, Default)]
pub struct HdfsSiteConfigBuilder {
    config: BTreeMap<String, String>,
}

impl HdfsSiteConfigBuilder {
    pub fn new() -> Self {
        HdfsSiteConfigBuilder {
            config: BTreeMap::new(),
        }
    }

    pub fn dfs_namenode_name_dir(&mut self, dir: impl Into<String>) -> &mut Self {
        self.config.insert(DFS_NAMENODE_NAME_DIR.to_string(), dir);
        self
    }

    pub fn dfs_datanode_data_dir(&mut self, dir: impl Into<String>) -> &mut Self {
        self.config.insert(DFS_DATANODE_DATA_DIR.to_string(), dir);
        self
    }

    pub fn dfs_journalnode_edits_dir(&mut self, dir: impl Into<String>) -> &mut Self {
        self.config
            .insert(DFS_JOURNALNODE_EDITS_DIR.to_string(), dir);
        self
    }

    pub fn dfs_name_services(&mut self, logical_name: impl Into<String>) -> &mut Self {
        self.config
            .insert(DFS_NAME_SERVICES.to_string(), logical_name);
        self
    }

    pub fn dfs_ha_namenodes(
        &mut self,
        logical_name: &str,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        self.config.insert(
            format!("dfs.ha.namenodes.{}", logical_name),
            namenode_podrefs
                .iter()
                .map(|nn| nn.pod_name.clone())
                .collect::<Vec<String>>()
                .join(","),
        );
        self
    }

    pub fn dfs_namenode_shared_edits_dir(
        &mut self,
        journalnode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        self.config.insert(
            DFS_NAMENODE_SHARED_EDITS_DIR.to_string(),
            format!(
                "qjournal://{}/{}",
                journalnode_podrefs
                    .iter()
                    .map(|jnid| format!(
                        "{}:{}",
                        jnid.fqdn(),
                        jnid.ports
                            .get(&String::from(DFS_JOURNALNODE_RPC_ADDRESS))
                            .map_or(DEFAULT_JOURNAL_NODE_RPC_PORT, |p| *p)
                    ))
                    .collect::<Vec<_>>()
                    .join(";"),
                hdfs.name()
            ),
        );
        self
    }

    // This required? https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
    pub fn dfs_namenode_name_dir_ha(
        &mut self,
        logical_name: &str,
        dir: impl Into<String>,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        for nn in namenode_podrefs {
            self.config.insert(
                format!("{}.{}.{}", DFS_NAMENODE_NAME_DIR, logical_name, nn.pod_name),
                dir.to_string(),
            );
        }
        self
    }

    pub fn dfs_namenode_rpc_address_ha(
        &mut self,
        logical_name: &str,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        self.dfs_namenode_address_ha(
            logical_name,
            namenode_podrefs,
            DFS_NAMENODE_RPC_ADDRESS,
            DEFAULT_NAME_NODE_RPC_PORT,
        );
        self
    }

    pub fn dfs_namenode_http_address_ha(
        &mut self,
        logical_name: &str,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        self.dfs_namenode_address_ha(
            logical_name,
            namenode_podrefs,
            DFS_NAMENODE_HTTP_ADDRESS,
            DEFAULT_NAME_NODE_HTTP_PORT,
        );
        self
    }

    fn dfs_namenode_address_ha(
        &mut self,
        logical_name: &str,
        namenode_podrefs: &[HdfsPodRef],
        address: &str,
        default_port: i32,
    ) -> &mut Self {
        for nn in namenode_podrefs {
            self.config.insert(
                format!("{}.{}.{}", address, logical_name, nn.pod_name),
                format!(
                    "{}:{}",
                    nn.fqdn(),
                    nn.ports.get(address).map_or(default_port, |p| *p)
                ),
            );
        }
        self
    }
}

#[derive(Clone, Default)]
pub struct CoreSiteConfigBuilder {
    config: BTreeMap<String, String>,
}

impl CoreSiteConfigBuilder {
    pub fn new() -> Self {
        CoreSiteConfigBuilder {
            config: BTreeMap::new(),
        }
    }

    pub fn fs_default_fs(&mut self, logical_name: &str) -> &mut Self {
        self.config.insert(
            FS_DEFAULT_FS.to_string(),
            format!("hdfs://{}/", logical_name),
        );
        self
    }

    pub fn ha_zookeeper_quorum(&mut self) -> &mut Self {
        self.config.insert(
            HA_ZOOKEEPER_QUORUM.to_string(),
            "${env.ZOOKEEPER}".to_string(),
        );
        self
    }
}
