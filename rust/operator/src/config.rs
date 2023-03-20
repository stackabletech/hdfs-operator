use stackable_hdfs_crd::constants::{
    DEFAULT_JOURNAL_NODE_RPC_PORT, DEFAULT_NAME_NODE_HTTPS_PORT, DEFAULT_NAME_NODE_HTTP_PORT,
    DEFAULT_NAME_NODE_RPC_PORT, DFS_DATANODE_DATA_DIR, DFS_HA_NAMENODES, DFS_JOURNALNODE_EDITS_DIR,
    DFS_JOURNALNODE_RPC_ADDRESS, DFS_NAMENODE_HTTPS_ADDRESS, DFS_NAMENODE_HTTP_ADDRESS,
    DFS_NAMENODE_NAME_DIR, DFS_NAMENODE_RPC_ADDRESS, DFS_NAMENODE_SHARED_EDITS_DIR,
    DFS_NAME_SERVICES, DFS_REPLICATION, FS_DEFAULT_FS, HADOOP_SECURITY_AUTHENTICATION,
    HA_ZOOKEEPER_QUORUM, JOURNALNODE_ROOT_DATA_DIR, NAMENODE_ROOT_DATA_DIR,
};
use stackable_hdfs_crd::storage::{DataNodeStorageConfig, DataNodeStorageConfigInnerType};
use stackable_hdfs_crd::{HdfsCluster, HdfsPodRef};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct HdfsSiteConfigBuilder {
    config: BTreeMap<String, String>,
    logical_name: String,
}

impl HdfsSiteConfigBuilder {
    pub fn new(logical_name: String) -> Self {
        HdfsSiteConfigBuilder {
            config: BTreeMap::new(),
            logical_name,
        }
    }

    pub fn add(&mut self, property: &str, value: &str) -> &mut Self {
        self.config.insert(property.to_string(), value.to_string());
        self
    }

    pub fn extend(&mut self, properties: &BTreeMap<String, String>) -> &mut Self {
        self.config.extend(properties.clone());
        self
    }

    pub fn dfs_namenode_name_dir(&mut self) -> &mut Self {
        self.config.insert(
            DFS_NAMENODE_NAME_DIR.to_string(),
            NAMENODE_ROOT_DATA_DIR.to_string(),
        );
        self
    }

    pub fn dfs_datanode_data_dir(
        &mut self,
        datanode_storage: Option<DataNodeStorageConfigInnerType>,
    ) -> &mut Self {
        if let Some(datanode_storage) = datanode_storage {
            let datanode_storage = DataNodeStorageConfig {
                pvcs: datanode_storage,
            };
            self.config.insert(
                DFS_DATANODE_DATA_DIR.to_string(),
                datanode_storage.get_datanode_data_dir(),
            );
        }
        self
    }

    pub fn dfs_journalnode_edits_dir(&mut self) -> &mut Self {
        self.config.insert(
            DFS_JOURNALNODE_EDITS_DIR.to_string(),
            JOURNALNODE_ROOT_DATA_DIR.to_string(),
        );
        self
    }

    pub fn dfs_name_services(&mut self) -> &mut Self {
        self.config
            .insert(DFS_NAME_SERVICES.to_string(), self.logical_name.clone());
        self
    }

    pub fn dfs_replication(&mut self, replication: u8) -> &mut Self {
        self.config
            .insert(DFS_REPLICATION.to_string(), replication.to_string());
        self
    }

    pub fn dfs_ha_namenodes(&mut self, namenode_podrefs: &[HdfsPodRef]) -> &mut Self {
        self.config.insert(
            format!("{}.{}", DFS_HA_NAMENODES, self.logical_name),
            namenode_podrefs
                .iter()
                .map(|nn| nn.pod_name.clone())
                .collect::<Vec<String>>()
                .join(","),
        );
        self
    }

    pub fn dfs_client_failover_proxy_provider(&mut self) -> &mut Self {
        self.config.insert(
            format!("dfs.client.failover.proxy.provider.{}", self.logical_name),
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider".to_string(),
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
                self.logical_name
            ),
        );
        self
    }

    pub fn dfs_namenode_name_dir_ha(&mut self, namenode_podrefs: &[HdfsPodRef]) -> &mut Self {
        for nn in namenode_podrefs {
            self.config.insert(
                format!(
                    "{}.{}.{}",
                    DFS_NAMENODE_NAME_DIR, self.logical_name, nn.pod_name
                ),
                NAMENODE_ROOT_DATA_DIR.to_string(),
            );
        }
        self
    }

    pub fn dfs_namenode_rpc_address_ha(&mut self, namenode_podrefs: &[HdfsPodRef]) -> &mut Self {
        self.dfs_namenode_address_ha(
            namenode_podrefs,
            DFS_NAMENODE_RPC_ADDRESS,
            DEFAULT_NAME_NODE_RPC_PORT,
        );
        self
    }

    pub fn dfs_namenode_http_address_ha(
        &mut self,
        hdfs: &HdfsCluster,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        if hdfs.has_https_enabled() {
            self.dfs_namenode_address_ha(
                namenode_podrefs,
                DFS_NAMENODE_HTTPS_ADDRESS,
                DEFAULT_NAME_NODE_HTTPS_PORT,
            );
        } else {
            self.dfs_namenode_address_ha(
                namenode_podrefs,
                DFS_NAMENODE_HTTP_ADDRESS,
                DEFAULT_NAME_NODE_HTTP_PORT,
            );
        }
        self
    }

    fn dfs_namenode_address_ha(
        &mut self,
        namenode_podrefs: &[HdfsPodRef],
        address: &str,
        default_port: u16,
    ) -> &mut Self {
        for nn in namenode_podrefs {
            self.config.insert(
                format!("{}.{}.{}", address, self.logical_name, nn.pod_name),
                format!(
                    "{}:{}",
                    nn.fqdn(),
                    nn.ports.get(address).map_or(default_port, |p| *p)
                ),
            );
        }
        self
    }

    pub fn build_as_xml(&self) -> String {
        let transformed_config = transform_for_product_config(&self.config);

        stackable_operator::product_config::writer::to_hadoop_xml(transformed_config.iter())
    }
}

#[derive(Clone, Default)]
pub struct CoreSiteConfigBuilder {
    config: BTreeMap<String, String>,
    logical_name: String,
}

impl CoreSiteConfigBuilder {
    pub fn new(logical_name: String) -> Self {
        CoreSiteConfigBuilder {
            config: BTreeMap::new(),
            logical_name,
        }
    }

    pub fn add(&mut self, property: &str, value: &str) -> &mut Self {
        self.config.insert(property.to_string(), value.to_string());
        self
    }

    pub fn extend(&mut self, properties: &BTreeMap<String, String>) -> &mut Self {
        self.config.extend(properties.clone());
        self
    }

    pub fn fs_default_fs(&mut self) -> &mut Self {
        self.config.insert(
            FS_DEFAULT_FS.to_string(),
            format!("hdfs://{}/", self.logical_name),
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

    pub fn hadoop_security_authentication(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.config.insert(
                HADOOP_SECURITY_AUTHENTICATION.to_string(),
                "kerberos".to_string(),
            );
        }
        self
    }

    pub fn build_as_xml(&self) -> String {
        let transformed_config = transform_for_product_config(&self.config);
        stackable_operator::product_config::writer::to_hadoop_xml(transformed_config.iter())
    }
}

fn transform_for_product_config(
    config: &BTreeMap<String, String>,
) -> BTreeMap<String, Option<String>> {
    config
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect::<BTreeMap<String, Option<String>>>()
}
