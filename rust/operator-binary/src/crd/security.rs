use std::str::FromStr;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::opa::OpaConfig,
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationConfig {
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_tls_secret_class")]
    pub tls_secret_class: SecretClassName,
    /// Kerberos configuration.
    pub kerberos: KerberosConfig,
}

fn default_tls_secret_class() -> SecretClassName {
    SecretClassName::from_str("tls").expect("\"tls\" should be a valid SecretClassName")
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HDFS services.
    pub secret_class: SecretClassName,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationConfig {
    // No doc - it's in the struct.
    pub opa: OpaConfig,
}
