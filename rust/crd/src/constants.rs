pub const FIELD_MANAGER_SCOPE: &str = "hdfscluster";
pub const FIELD_MANAGER_SCOPE_POD: &str = "pod-service";

pub const APP_NAME: &str = "hdfs";

pub const TOOLS_IMAGE: &str = "docker.stackable.tech/stackable/tools:0.1.0-stackable0";

pub const LABEL_ENABLE: &str = "hdfs.stackable.tech/pod-service";
pub const LABEL_STS_POD_NAME: &str = "statefulset.kubernetes.io/pod-name";

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
pub const DEFAULT_NAME_NODE_HTTP_PORT: i32 = 9870;
pub const DEFAULT_NAME_NODE_RPC_PORT: i32 = 8020;

pub const DEFAULT_DATA_NODE_METRICS_PORT: i32 = 8082;
pub const DEFAULT_DATA_NODE_HTTP_PORT: i32 = 9864;
pub const DEFAULT_DATA_NODE_DATA_PORT: i32 = 9866;
pub const DEFAULT_DATA_NODE_IPC_PORT: i32 = 9867;

pub const DEFAULT_JOURNAL_NODE_METRICS_PORT: i32 = 8081;
pub const DEFAULT_JOURNAL_NODE_HTTP_PORT: i32 = 8480;
pub const DEFAULT_JOURNAL_NODE_HTTPS_PORT: i32 = 8481;
pub const DEFAULT_JOURNAL_NODE_RPC_PORT: i32 = 8485;
