use stackable_hdfs_crd::{
    constants::{HADOOP_SECURITY_AUTHENTICATION, SSL_CLIENT_XML, SSL_SERVER_XML},
    security::{SecurityConfig, WireEncryption},
    HdfsCluster, HdfsRole,
};

use crate::config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder};

impl HdfsSiteConfigBuilder {
    pub fn security_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if let Some(security_config) = hdfs.security_config() {
            self.add("dfs.block.access.token.enable", "true")
                .add("dfs.http.policy", "HTTPS_ONLY")
                .add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true")
                .add("dfs.https.server.keystore.resource", SSL_SERVER_XML)
                .add("dfs.https.client.keystore.resource", SSL_CLIENT_XML);
            self.add_wire_encryption_settings(security_config);
        }
        self
    }

    pub fn security_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if let Some(security_config) = hdfs.security_config() {
            // We want e.g. hbase to automatically renew the Kerberos tickets.
            // This shouldn't harm any other consumers.
            self.add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
            self.add_wire_encryption_settings(security_config);
        }
        self
    }

    fn add_wire_encryption_settings(&mut self, security_config: &SecurityConfig) -> &mut Self {
        match security_config.wire_encryption {
            WireEncryption::Authentication => {
                self.add("dfs.data.transfer.protection", "authentication");
                self.add("dfs.encrypt.data.transfer", "false");
            }
            WireEncryption::Integrity => {
                self.add("dfs.data.transfer.protection", "integrity");
                self.add("dfs.encrypt.data.transfer", "false");
            }
            WireEncryption::Privacy => {
                self.add("dfs.data.transfer.protection", "privacy");
                self.add("dfs.encrypt.data.transfer", "true");
            }
        }
        self
    }
}

impl CoreSiteConfigBuilder {
    pub fn kerberos_config(
        &mut self,
        hdfs: &HdfsCluster,
        role: &HdfsRole,
        hdfs_name: &str,
        hdfs_namespace: &str,
    ) -> &mut Self {
        if let Some(security_config) = hdfs.security_config() {
            self
                .add("hadoop.security.authentication", "kerberos")
                .add("hadoop.security.authorization", "true")
                // Otherwise we fail with `java.io.IOException: No groups found for user nn`
                // Default value is `dr.who=`, so we include that here
                .add("hadoop.user.group.static.mapping.overrides", "dr.who=;nn=;nm=;jn=;")
                .add("hadoop.registry.kerberos.realm", "${env.KERBEROS_REALM}")
                .add(
                    "dfs.web.authentication.kerberos.principal",
                    "HTTP/_HOST@${env.KERBEROS_REALM}",
                )
                .add(
                    "dfs.web.authentication.keytab.file",
                    "/stackable/kerberos/keytab",
                )
                .add(
                    "dfs.journalnode.kerberos.principal.pattern",
                    // E.g. jn/hdfs-test-journalnode-default-0.hdfs-test-journalnode-default.test.svc.cluster.local@CLUSTER.LOCAL
                    format!("jn/{hdfs_name}-journalnode-*.{hdfs_name}-journalnode-*.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}").as_str(),
                )
                .add(
                    "dfs.namenode.kerberos.principal.pattern",
                    format!("nn/{hdfs_name}-namenode-*.{hdfs_name}-namenode-*.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}").as_str(),
                );

            match role {
                HdfsRole::NameNode => {
                    self.add(
                        "dfs.namenode.kerberos.principal",
                        "nn/_HOST@${env.KERBEROS_REALM}",
                    )
                    .add("dfs.namenode.keytab.file", "/stackable/kerberos/keytab");
                }
                HdfsRole::DataNode => {
                    self.add(
                        "dfs.datanode.kerberos.principal",
                        "dn/_HOST@${env.KERBEROS_REALM}",
                    )
                    .add("dfs.datanode.keytab.file", "/stackable/kerberos/keytab");
                }
                HdfsRole::JournalNode => {
                    self.add(
                        "dfs.journalnode.kerberos.principal",
                        "jn/_HOST@${env.KERBEROS_REALM}",
                    )
                    .add("dfs.journalnode.keytab.file", "/stackable/kerberos/keytab")
                    .add(
                        "dfs.journalnode.kerberos.internal.spnego.principal",
                        "HTTP/_HOST@${env.KERBEROS_REALM}",
                    );
                }
            }

            self.add_wire_encryption_settings(security_config);
        }
        self
    }

    pub fn security_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if let Some(security_config) = hdfs.security_config() {
            self.add(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            self.add_wire_encryption_settings(security_config);
        }
        self
    }

    fn add_wire_encryption_settings(&mut self, security_config: &SecurityConfig) -> &mut Self {
        match security_config.wire_encryption {
            WireEncryption::Authentication => {
                self.add("hadoop.rpc.protection", "authentication");
            }
            WireEncryption::Integrity => {
                self.add("hadoop.rpc.protection", "integrity");
            }
            WireEncryption::Privacy => {
                self.add("hadoop.rpc.protection", "privacy");
            }
        }
        self
    }
}
