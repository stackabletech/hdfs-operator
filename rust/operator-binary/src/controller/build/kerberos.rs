use stackable_operator::utils::cluster_info::KubernetesClusterInfo;

use crate::controller::build::properties::{
    ConfigFileName, core_site::CoreSiteConfigBuilder, hdfs_site::HdfsSiteConfigBuilder,
};

pub const KERBEROS_CONTAINER_PATH: &str = "/stackable/kerberos";

/// Hadoop wire-encryption protection level requiring authentication, integrity and
/// confidentiality. Used for both `dfs.data.transfer.protection` and `hadoop.rpc.protection`.
const WIRE_ENCRYPTION_PRIVACY: &str = "privacy";

/// The cluster-wide security settings the `security_config` builders need,
/// resolved from the `HdfsCluster` so the builders don't depend on the raw CRD.
pub struct KerberosConfig<'a> {
    pub cluster_name: &'a str,
    pub cluster_namespace: &'a str,
    /// Whether an `authentication` config is set (gates the core-site security config).
    pub authentication_enabled: bool,
    /// Whether kerberos is enabled (gates the discovery security config).
    pub kerberos_enabled: bool,
    pub authorization_enabled: bool,
}

impl HdfsSiteConfigBuilder {
    pub fn security_config(&mut self, kerberos_enabled: bool) -> &mut Self {
        if kerberos_enabled {
            self.add("dfs.block.access.token.enable", "true")
                .add("dfs.http.policy", "HTTPS_ONLY")
                .add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true")
                .add(
                    "dfs.https.server.keystore.resource",
                    ConfigFileName::SslServer.to_string(),
                )
                .add(
                    "dfs.https.client.keystore.resource",
                    ConfigFileName::SslClient.to_string(),
                );
            self.add_wire_encryption_settings();
        }
        self
    }

    pub fn security_discovery_config(&mut self, kerberos_enabled: bool) -> &mut Self {
        if kerberos_enabled {
            // We want e.g. hbase to automatically renew the Kerberos tickets.
            // This shouldn't harm any other consumers.
            self.add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
            self.add_wire_encryption_settings();
        }
        self
    }

    fn add_wire_encryption_settings(&mut self) -> &mut Self {
        self.add("dfs.data.transfer.protection", WIRE_ENCRYPTION_PRIVACY);
        self.add("dfs.encrypt.data.transfer", "true");
        self.add(
            "dfs.encrypt.data.transfer.cipher.suite",
            "AES/CTR/NoPadding",
        );
        self
    }
}

impl CoreSiteConfigBuilder {
    pub fn security_config(
        &mut self,
        kerberos: &KerberosConfig,
        cluster_info: &KubernetesClusterInfo,
    ) -> &mut Self {
        if kerberos.authentication_enabled {
            let principal_host_part = principal_host_part(
                kerberos.cluster_name,
                kerberos.cluster_namespace,
                cluster_info,
            );

            self.add("hadoop.security.authentication", "kerberos")
                // Not adding hadoop.registry.kerberos.realm, as it seems to not be used by our customers
                // and would need text-replacement of the env var anyway.
                // .add("hadoop.registry.kerberos.realm", "${env.KERBEROS_REALM}")
                .add(
                    "dfs.journalnode.kerberos.principal",
                    format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.journalnode.kerberos.internal.spnego.principal",
                    format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal",
                    format!("nn/{principal_host_part}"),
                )
                .add(
                    "dfs.datanode.kerberos.principal",
                    format!("dn/{principal_host_part}"),
                )
                .add(
                    "dfs.web.authentication.kerberos.principal",
                    format!("HTTP/{principal_host_part}"),
                )
                .add(
                    "dfs.journalnode.keytab.file",
                    format!("{KERBEROS_CONTAINER_PATH}/keytab"),
                )
                .add(
                    "dfs.namenode.keytab.file",
                    format!("{KERBEROS_CONTAINER_PATH}/keytab"),
                )
                .add(
                    "dfs.datanode.keytab.file",
                    format!("{KERBEROS_CONTAINER_PATH}/keytab"),
                )
                .add(
                    "dfs.journalnode.kerberos.principal.pattern",
                    format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal.pattern",
                    format!("nn/{principal_host_part}"),
                );

            if !kerberos.authorization_enabled {
                // In case *no* OPA authorizer is used, we got the following error message:
                // java.io.IOException: No groups found for user nn
                // In case the OPA authorizer is used everything seems to be fine.
                // The default value is `dr.who=`, so we include that here.
                self.add(
                    "hadoop.user.group.static.mapping.overrides",
                    "dr.who=;nn=;nm=;jn=;",
                );
            }

            self.add_wire_encryption_settings();
        }
        self
    }

    pub fn security_discovery_config(
        &mut self,
        kerberos: &KerberosConfig,
        cluster_info: &KubernetesClusterInfo,
    ) -> &mut Self {
        if kerberos.kerberos_enabled {
            let principal_host_part = principal_host_part(
                kerberos.cluster_name,
                kerberos.cluster_namespace,
                cluster_info,
            );

            self.add("hadoop.security.authentication", "kerberos")
                .add(
                    "dfs.journalnode.kerberos.principal",
                    format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal",
                    format!("nn/{principal_host_part}"),
                )
                .add(
                    "dfs.datanode.kerberos.principal",
                    format!("dn/{principal_host_part}"),
                );
            self.add_wire_encryption_settings();
        }
        self
    }

    fn add_wire_encryption_settings(&mut self) -> &mut Self {
        self.add("hadoop.rpc.protection", WIRE_ENCRYPTION_PRIVACY);
        self
    }
}

/// For a long time we tried using `_HOST` in principals, e.g. `jn/_HOST@REALM.COM`.
/// Turns out there are a lot of code paths that check the principal of the requester using a reverse lookup of the incoming IP address
/// and getting a different hostname than the principal has.
/// What ultimately killed this approach was
///
/// ```text
/// 2023-05-30 09:23:01,745 ERROR namenode.EditLogInputStream (EditLogFileInputStream.java:nextOpImpl(220)) - caught exception initializing https://hdfs-journalnode-default-1.hdfs-journalnode-default.kuttl-test-fine-rat.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A595659877%3A1685437352616%3ACID-90c52400-5b07-49bf-bdbe-3469bbdc5ebb&inProgressOk=true
/// org.apache.hadoop.hdfs.server.common.HttpGetFailedException: Fetch of https://hdfs-journalnode-default-1.hdfs-journalnode-default.kuttl-test-fine-rat.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A595659877%3A1685437352616%3ACID-90c52400-5b07-49bf-bdbe-3469bbdc5ebb&inProgressOk=true failed with status code 403
/// Response message:
/// Only Namenode and another JournalNode may access this servlet
/// ```
///
/// After we have switched to using the following principals everything worked without problems
fn principal_host_part(
    cluster_name: &str,
    cluster_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> String {
    let cluster_domain = &cluster_info.cluster_domain;
    format!("{cluster_name}.{cluster_namespace}.svc.{cluster_domain}@${{env.KERBEROS_REALM}}")
}
