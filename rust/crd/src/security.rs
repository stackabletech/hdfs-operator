use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::opa::OpaConfig,
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationConfig {
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_tls_secret_class")]
    pub tls_secret_class: String,
    /// Kerberos configuration.
    pub kerberos: KerberosConfig,
}

fn default_tls_secret_class() -> String {
    "tls".to_string()
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HDFS services.
    pub secret_class: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationConfig {
    // No doc - it's in the struct.
    pub opa: OpaConfig,
}
