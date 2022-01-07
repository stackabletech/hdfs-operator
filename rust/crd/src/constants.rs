
pub const FIELD_MANAGER_SCOPE: &str = "hdfscluster";

pub const APP_NAME: &str = "hdfs";
pub const MANAGED_BY: &str = "hdfs-operator";

pub const DFS_NAME_NODE_NAME_DIR: &str = "dfs.namenode.name.dir";
pub const DFS_DATA_NODE_DATA_DIR: &str = "dfs.datanode.data.dir";
pub const DFS_REPLICATION: &str = "dfs.replication";

pub const FS_DEFAULT: &str = "fs.defaultFS";

// Properties below are from https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
pub const DFS_NAME_NODE_HTTP_ADDRESS: &str = "dfs.namenode.http-address"; // 0.0.0.0:50070	The address and the base port where the dfs namenode web ui will listen on. 
pub const DFS_DATA_NODE_IPC_ADDRESS: &str = "dfs.datanode.ipc.address"; // 0.0.0.0:50020	The datanode ipc server address and port. 
pub const DFS_DATA_NODE_HTTP_ADDRESS: &str = "dfs.datanode.http.address"; // 0.0.0.0:50090	The secondary namenode http server address and port. 
pub const DFS_DATA_NODE_DATA_ADDRESS: &str = "dfs.datanode.address"; // 0.0.0.0:50010	The datanode server address and port for data transfer. 
pub const DFS_JOURNAL_NODE_RPC_ADDRESS: &str  = "dfs.journalnode.rpc-address"; //	0.0.0.0:8485	The JournalNode RPC server address and port.
pub const DFS_JOURNAL_NODE_HTTP_ADDRESS: &str  = "dfs.journalnode.http-address"; //	0.0.0.0:8480	The address and port the JournalNode HTTP server listens on. If the port is 0 then the server will start on a free port.
pub const DFS_JOURNAL_NODE_HTTPS_ADDRESS: &str  = "dfs.journalnode.https-address"; //	0.0.0.0:8481

pub const METRICS_PORT_PROPERTY: &str = "metricsPort";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";
pub const CONFIG_DIR_NAME: &str = "/stackable/conf";

pub const HDFS_SITE_XML: &str = "hdfs-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";

pub const SERVICE_PORT_NAME_RPC: &str = "rpc";
pub const SERVICE_PORT_NAME_IPC: &str = "ipc";
pub const SERVICE_PORT_NAME_HTTP: &str = "http";
pub const SERVICE_PORT_NAME_HTTPS: &str = "https";
pub const SERVICE_PORT_NAME_DATA: &str = "data";
pub const SERVICE_PORT_NAME_METRICS: &str = "metrics";
