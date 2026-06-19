//! Builds the `hdfs-site.xml` config file.

use std::collections::BTreeMap;

use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
        types::common::Port,
    },
};

use crate::{
    controller::{ValidatedCluster, build},
    crd::{
        AnyNodeConfig, HdfsNodeRole, HdfsPodRef,
        constants::{
            DEFAULT_JOURNAL_NODE_RPC_PORT, DEFAULT_NAME_NODE_HTTP_PORT,
            DEFAULT_NAME_NODE_HTTPS_PORT, DEFAULT_NAME_NODE_RPC_PORT, DFS_DATANODE_DATA_DIR,
            DFS_HA_NAMENODES, DFS_JOURNALNODE_EDITS_DIR, DFS_JOURNALNODE_RPC_ADDRESS,
            DFS_NAME_SERVICES, DFS_NAMENODE_HTTP_ADDRESS, DFS_NAMENODE_HTTPS_ADDRESS,
            DFS_NAMENODE_NAME_DIR, DFS_NAMENODE_RPC_ADDRESS, DFS_NAMENODE_SHARED_EDITS_DIR,
            DFS_REPLICATION, JOURNALNODE_ROOT_DATA_DIR, NAMENODE_ROOT_DATA_DIR,
            SERVICE_PORT_NAME_HTTP, SERVICE_PORT_NAME_HTTPS, SERVICE_PORT_NAME_RPC,
        },
        storage::{DataNodeStorageConfig, DataNodeStorageConfigInnerType},
    },
};

/// Renders `hdfs-site.xml`: operator defaults, HA wiring derived from the pod
/// refs, kerberos/OPA security config, with user `configOverrides` applied last.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    merged_config: &AnyNodeConfig,
    overrides: KeyValueConfigOverrides,
) -> String {
    let cluster_config = &cluster.cluster_config;
    let namenode_podrefs = build::pod_refs(cluster, &HdfsNodeRole::Name);
    let journalnode_podrefs = build::pod_refs(cluster, &HdfsNodeRole::Journal);
    // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
    // be formatted by the namenode, or used by the other services.
    // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
    //
    // Notes on configuration choices
    // ===============================
    // We used to set `dfs.ha.nn.not-become-active-in-safemode` to true here due to
    // badly worded HDFS documentation:
    // https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
    // This caused a deadlock with no namenode becoming active during a startup after
    // HDFS was completely down for a while.
    let mut hdfs_site = HdfsSiteConfigBuilder::new(cluster.name.as_ref().to_owned());
    hdfs_site
        .dfs_namenode_name_dir()
        .dfs_datanode_data_dir(
            merged_config
                .as_datanode()
                .map(|node| node.resources.storage.clone()),
        )
        .dfs_journalnode_edits_dir()
        .dfs_replication(cluster_config.dfs_replication)
        .dfs_name_services()
        .dfs_ha_namenodes(&namenode_podrefs)
        .dfs_namenode_shared_edits_dir(cluster_info, &journalnode_podrefs)
        .dfs_namenode_name_dir_ha(&namenode_podrefs)
        .dfs_namenode_rpc_address_ha(cluster_info, &namenode_podrefs)
        .dfs_namenode_http_address_ha(
            cluster_config.authentication.is_some(),
            cluster_info,
            &namenode_podrefs,
        )
        .dfs_client_failover_proxy_provider()
        .security_config(cluster_config.authentication.is_some())
        .add("dfs.ha.fencing.methods", "shell(/bin/true)")
        .add("dfs.ha.automatic-failover.enabled", "true")
        .add("dfs.ha.namenode.id", "${env.POD_NAME}")
        .add(
            "dfs.namenode.datanode.registration.unsafe.allow-address-override",
            "true",
        )
        .add("dfs.datanode.registered.hostname", "${env.POD_ADDRESS}")
        .add("dfs.datanode.registered.port", "${env.DATA_PORT}")
        .add("dfs.datanode.registered.ipc.port", "${env.IPC_PORT}")
        // The following two properties are set to "true" because there is a minor chance that data
        // written to HDFS is not synced to disk even if a block has been closed.
        // Users in HBase can control this explicitly for the WAL, but for flushes and compactions
        // I believe they can't as easily (if at all).
        // In theory, HBase should be able to recover from these failures, but that comes at a cost
        // and there's always a risk.
        // Enabling this behavior causes HDFS to sync to disk as soon as possible.
        .add("dfs.datanode.sync.behind.writes", "true")
        .add("dfs.datanode.synconclose", "true")
        // Defaults to 10 since at least 2011.
        // This controls the concurrent number of client connections (this includes DataNodes)
        // to the NameNode. Ideally, we'd scale this with the number of DataNodes but this would
        // lead to restarts of the NameNode.
        // This should lead to better performance due to more concurrency.
        .add("dfs.namenode.handler.count", "50")
        // Defaults to 10 since at least 2012.
        // This controls the concurrent number of client connections to the DataNodes.
        // We have no idea how many clients there may be, so it's hard to assign a good default.
        // Increasing to 50 should lead to better performance due to more concurrency, especially
        // with use-cases like HBase.
        .add("dfs.datanode.handler.count", "50")
        // The following two properties default to 2 and 4 respectively since around 2013.
        // They control the number of maximum replication "jobs" a NameNode assigns to
        // a DataNode in a single heartbeat.
        // Increasing this number will increase network usage during replication events
        // but can lead to faster recovery.
        .add("dfs.namenode.replication.max-streams", "4")
        .add("dfs.namenode.replication.max-streams-hard-limit", "8")
        // Defaults to 4096 and hasn't changed since at least 2011.
        // The number of threads used for actual data transfer, so not very CPU heavy
        // but IO bound. This is why the number is relatively high.
        // But today's Java and IO should be able to handle more, so bump it to 8192 for
        // better performance/concurrency.
        .add("dfs.datanode.max.transfer.threads", "8192");
    if cluster_config.authentication.is_some() {
        hdfs_site.add("dfs.datanode.registered.https.port", "${env.HTTPS_PORT}");
    } else {
        hdfs_site.add("dfs.datanode.registered.http.port", "${env.HTTP_PORT}");
    }
    if let Some(opa_config) = &cluster_config.authorization {
        opa_config.add_hdfs_site_config(&mut hdfs_site);
    }
    // the extend with config must come last in order to have overrides working!!!
    hdfs_site.extend(overrides).build_as_xml()
}

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

    pub fn add(&mut self, property: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.config.insert(property.into(), value.into());
        self
    }

    pub fn extend(&mut self, properties: impl IntoIterator<Item = (String, String)>) -> &mut Self {
        self.config.extend(properties);
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
        cluster_info: &KubernetesClusterInfo,
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
                        jnid.fqdn(cluster_info),
                        jnid.ports
                            .get(&String::from(DFS_JOURNALNODE_RPC_ADDRESS))
                            .map_or(DEFAULT_JOURNAL_NODE_RPC_PORT, |p| p.clone())
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

    pub fn dfs_namenode_rpc_address_ha(
        &mut self,
        cluster_info: &KubernetesClusterInfo,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        self.dfs_namenode_address_ha(
            cluster_info,
            namenode_podrefs,
            DFS_NAMENODE_RPC_ADDRESS,
            SERVICE_PORT_NAME_RPC,
            DEFAULT_NAME_NODE_RPC_PORT,
        );
        self
    }

    pub fn dfs_namenode_http_address_ha(
        &mut self,
        https_enabled: bool,
        cluster_info: &KubernetesClusterInfo,
        namenode_podrefs: &[HdfsPodRef],
    ) -> &mut Self {
        if https_enabled {
            self.dfs_namenode_address_ha(
                cluster_info,
                namenode_podrefs,
                DFS_NAMENODE_HTTPS_ADDRESS,
                SERVICE_PORT_NAME_HTTPS,
                DEFAULT_NAME_NODE_HTTPS_PORT,
            );
        } else {
            self.dfs_namenode_address_ha(
                cluster_info,
                namenode_podrefs,
                DFS_NAMENODE_HTTP_ADDRESS,
                SERVICE_PORT_NAME_HTTP,
                DEFAULT_NAME_NODE_HTTP_PORT,
            );
        }
        self
    }

    fn dfs_namenode_address_ha(
        &mut self,
        cluster_info: &KubernetesClusterInfo,
        namenode_podrefs: &[HdfsPodRef],
        address: &str,
        port_name: &str,
        default_port: Port,
    ) -> &mut Self {
        for nn in namenode_podrefs {
            self.config.insert(
                format!(
                    "{address}.{logical_name}.{pod_name}",
                    logical_name = self.logical_name,
                    pod_name = nn.pod_name,
                ),
                format!(
                    "{fqdn}:{port}",
                    fqdn = nn.fqdn(cluster_info),
                    port = nn
                        .ports
                        .get(port_name)
                        .map_or(default_port.clone(), |p| p.clone())
                ),
            );
        }
        self
    }

    pub fn build_as_xml(&self) -> String {
        to_hadoop_xml(self.config.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::build::properties::test_support::{cluster_info, validated_cluster},
        crd::HdfsNodeRole,
        test_support::anynode_config,
    };

    fn namenode_merged_config(validated_cluster: &ValidatedCluster) -> &AnyNodeConfig {
        anynode_config(validated_cluster, &HdfsNodeRole::Name, "default")
    }

    #[test]
    fn renders_operator_defaults() {
        let validated_cluster = validated_cluster();
        let merged = namenode_merged_config(&validated_cluster);
        let xml = build(
            &validated_cluster,
            &cluster_info(),
            merged,
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains("<name>dfs.replication</name>\n    <value>3</value>"),
            "{xml}"
        );
        assert!(
            xml.contains("<name>dfs.datanode.max.transfer.threads</name>\n    <value>8192</value>"),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_defaults() {
        let validated_cluster = validated_cluster();
        let merged = namenode_merged_config(&validated_cluster);
        let xml = build(
            &validated_cluster,
            &cluster_info(),
            merged,
            [("dfs.replication", "5")].into(),
        );
        assert!(
            xml.contains("<name>dfs.replication</name>\n    <value>5</value>"),
            "{xml}"
        );
    }
}
