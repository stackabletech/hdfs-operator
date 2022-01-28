pub const FIELD_MANAGER_SCOPE: &str = "hdfscluster";
pub const FIELD_MANAGER_SCOPE_POD: &str = "pod-service";

pub const APP_NAME: &str = "hdfs";

pub const TOOLS_IMAGE: &str = "docker.stackable.tech/stackable/tools:0.1.0-stackable0";

pub const LABEL_ENABLE: &str = "hdfs.stackable.tech/pod-service";
pub const LABEL_STS_POD_NAME: &str = "statefulset.kubernetes.io/pod-name";

pub const DFS_NAME_NODE_NAME_DIR: &str = "dfs.namenode.name.dir";
pub const DFS_DATA_NODE_DATA_DIR: &str = "dfs.datanode.data.dir";
pub const DFS_REPLICATION: &str = "dfs.replication";

pub const FS_DEFAULT: &str = "fs.defaultFS";

// Properties below are from https://hadoop.apache.org/docs/r3.3.1/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
pub const DFS_NAME_NODE_HTTP_ADDRESS: &str = "dfs.namenode.http-address"; // 0.0.0.0:9870	The address and the base port where the dfs namenode web ui will listen on.
pub const DFS_NAME_NODE_RPC_ADDRESS: &str = "dfs.namenode.rpc-address"; // RPC address that handles all clients requests. In the case of HA/Federation where multiple namenodes exist, the name service id is added to the name e.g. dfs.namenode.rpc-address.ns1 dfs.namenode.rpc-address.EXAMPLENAMESERVICE The value of this property will take the form of nn-host1:rpc-port. The NameNode's default RPC port is 8020.
pub const DFS_DATA_NODE_IPC_ADDRESS: &str = "dfs.datanode.ipc.address"; // 0.0.0.0:9867	The datanode ipc server address and port.
pub const DFS_DATA_NODE_HTTP_ADDRESS: &str = "dfs.datanode.http.address"; // 0.0.0.0:9864	The datanode http server address and port.
pub const DFS_DATA_NODE_HTTPS_ADDRESS: &str = "dfs.datanode.https.address"; // 0.0.0.0:9865	The datanode secure http server address and port.
pub const DFS_DATA_NODE_DATA_ADDRESS: &str = "dfs.datanode.address"; // 0.0.0.0:9866	The datanode server address and port for data transfer.
pub const DFS_JOURNAL_NODE_RPC_ADDRESS: &str = "dfs.journalnode.rpc-address"; //	0.0.0.0:8485	The JournalNode RPC server address and port.
pub const DFS_JOURNAL_NODE_HTTP_ADDRESS: &str = "dfs.journalnode.http-address"; //	0.0.0.0:8480	The address and port the JournalNode HTTP server listens on. If the port is 0 then the server will start on a free port.
pub const DFS_JOURNAL_NODE_HTTPS_ADDRESS: &str = "dfs.journalnode.https-address"; // 0.0.0.0:8481	The address and port the JournalNode HTTPS server listens on. If the port is 0 then the server will start on a free port.

pub const METRICS_PORT_PROPERTY: &str = "metricsPort";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";
pub const CONFIG_DIR_NAME: &str = "/stackable/hadoop/etc/hadoop";

pub const HDFS_SITE_XML: &str = "hdfs-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";
pub const LOG4J_PROPERTIES: &str = "log4j.properties";

pub const SERVICE_PORT_NAME_RPC: &str = "rpc";
pub const SERVICE_PORT_NAME_IPC: &str = "ipc";
pub const SERVICE_PORT_NAME_HTTP: &str = "http";
pub const SERVICE_PORT_NAME_HTTPS: &str = "https";
pub const SERVICE_PORT_NAME_DATA: &str = "data";
pub const SERVICE_PORT_NAME_METRICS: &str = "metrics";

pub const DEFAULT_NAME_NODE_METRICS_PORT: i32 = 8183;
pub const DEFAULT_JOURNAL_NODE_METRICS_PORT: i32 = 8081;
pub const DEFAULT_DATA_NODE_METRICS_PORT: i32 = 8082;
