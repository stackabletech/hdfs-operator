use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityConfig {
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_kerberos_tls_secret_class")]
    pub tls_secret_class: String,
    /// Kerberos configuration
    pub kerberos: KerberosConfig,
    /// Configures how communication between hdfs nodes as well as between hdfs clients and cluster are secured.
    /// Possible values are:
    ///
    /// Authentication:
    /// Establishes mutual authentication between the client and the server.
    /// Sets `hadoop.rpc.protection` to `authentication`, `hadoop.data.transfer.protection` to `authentication` and `dfs.encrypt.data.transfer` to `false`.
    ///
    /// Integrity:
    /// In addition to authentication, it guarantees that a man-in-the-middle cannot tamper with messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `integrity`, `hadoop.data.transfer.protection` to `integrity` and `dfs.encrypt.data.transfer` to `false`.
    ///
    /// Privacy:
    /// In addition to the features offered by authentication and integrity, it also fully encrypts the messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `privacy`, `hadoop.data.transfer.protection` to `privacy` and `dfs.encrypt.data.transfer` to `true`.
    ///
    /// Defaults to privacy for best security
    #[serde(default)]
    pub wire_encryption: WireEncryption,
}

fn default_kerberos_tls_secret_class() -> String {
    "tls".to_string()
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HDFS services.
    pub kerberos_secret_class: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum WireEncryption {
    /// Establishes mutual authentication between the client and the server.
    /// Sets `hadoop.rpc.protection` to `authentication`, `hadoop.data.transfer.protection` to `authentication` and `dfs.encrypt.data.transfer` to `false`.
    Authentication,
    /// In addition to authentication, it guarantees that a man-in-the-middle cannot tamper with messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `integrity`, `hadoop.data.transfer.protection` to `integrity` and `dfs.encrypt.data.transfer` to `false`.
    Integrity,
    /// In addition to the features offered by authentication and integrity, it also fully encrypts the messages exchanged between the client and the server.
    /// Sets `hadoop.rpc.protection` to `privacy`, `hadoop.data.transfer.protection` to `privacy` and `dfs.encrypt.data.transfer` to `true`.
    #[default]
    Privacy,
}
