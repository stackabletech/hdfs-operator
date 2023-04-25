pub const DEFAULT_DFS_REPLICATION_FACTOR: u8 = 3;

pub const CONTROLLER_NAME: &str = "hdfsclusters.hdfs.stackable.tech";

pub const FIELD_MANAGER_SCOPE: &str = "hdfscluster";
pub const FIELD_MANAGER_SCOPE_POD: &str = "pod-service";

pub const APP_NAME: &str = "hdfs";

pub const HDFS_SITE_XML: &str = "hdfs-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";
pub const LOG4J_PROPERTIES: &str = "log4j.properties";

pub const SERVICE_PORT_NAME_RPC: &str = "rpc";
pub const SERVICE_PORT_NAME_IPC: &str = "ipc";
pub const SERVICE_PORT_NAME_HTTP: &str = "http";
pub const SERVICE_PORT_NAME_HTTPS: &str = "https";
pub const SERVICE_PORT_NAME_DATA: &str = "data";
pub const SERVICE_PORT_NAME_METRICS: &str = "metrics";

pub const DEFAULT_NAME_NODE_METRICS_PORT: u16 = 8183;
pub const DEFAULT_NAME_NODE_HTTP_PORT: u16 = 9870;
pub const DEFAULT_NAME_NODE_RPC_PORT: u16 = 8020;

pub const DEFAULT_DATA_NODE_METRICS_PORT: u16 = 8082;
pub const DEFAULT_DATA_NODE_HTTP_PORT: u16 = 9864;
pub const DEFAULT_DATA_NODE_DATA_PORT: u16 = 9866;
pub const DEFAULT_DATA_NODE_IPC_PORT: u16 = 9867;

pub const DEFAULT_JOURNAL_NODE_METRICS_PORT: u16 = 8081;
pub const DEFAULT_JOURNAL_NODE_HTTP_PORT: u16 = 8480;
pub const DEFAULT_JOURNAL_NODE_HTTPS_PORT: u16 = 8481;
pub const DEFAULT_JOURNAL_NODE_RPC_PORT: u16 = 8485;

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
pub const DFS_HA_NAMENODES: &str = "dfs.ha.namenodes";

// core-site.xml
pub const FS_DEFAULT_FS: &str = "fs.defaultFS";
pub const HA_ZOOKEEPER_QUORUM: &str = "ha.zookeeper.quorum";

pub const STACKABLE_ROOT_DATA_DIR: &str = "/stackable/data";
pub const NAMENODE_ROOT_DATA_DIR: &str = "/stackable/data/namenode";
pub const JOURNALNODE_ROOT_DATA_DIR: &str = "/stackable/data/journalnode";

// Will end up with something like `/stackable/data/<pvc-name>/datanode` e.g. `/stackable/data/data/datanode` and `/stackable/data/data-1/datanode` etc.
// We need one additional level because we don't want users to call their pvc e.g. `hadoop`
// ending up with a location of `/stackable/hadoop/data`
pub const DATANODE_ROOT_DATA_DIR_PREFIX: &str = "/stackable/data/";
pub const DATANODE_ROOT_DATA_DIR_SUFFIX: &str = "/datanode";

pub const STACKABLE_LISTENER_DIR: &str = "/stackable/listener";
