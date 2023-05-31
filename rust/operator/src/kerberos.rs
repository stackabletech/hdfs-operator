use stackable_hdfs_crd::{
    constants::{HADOOP_SECURITY_AUTHENTICATION, SSL_CLIENT_XML, SSL_SERVER_XML},
    HdfsCluster,
};

use crate::config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder};

impl HdfsSiteConfigBuilder {
    pub fn security_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.add("dfs.block.access.token.enable", "true")
                .add("dfs.http.policy", "HTTPS_ONLY")
                .add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true")
                .add("dfs.https.server.keystore.resource", SSL_SERVER_XML)
                .add("dfs.https.client.keystore.resource", SSL_CLIENT_XML);
            self.add_wire_encryption_settings();
        }
        self
    }

    pub fn security_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            // We want e.g. hbase to automatically renew the Kerberos tickets.
            // This shouldn't harm any other consumers.
            self.add("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
            self.add_wire_encryption_settings();
        }
        self
    }

    fn add_wire_encryption_settings(&mut self) -> &mut Self {
        self.add("dfs.data.transfer.protection", "privacy");
        self.add("dfs.encrypt.data.transfer", "true");
        self
    }
}

impl CoreSiteConfigBuilder {
    pub fn security_config(
        &mut self,
        hdfs: &HdfsCluster,
        hdfs_name: &str,
        hdfs_namespace: &str,
    ) -> &mut Self {
        if hdfs.authentication_config().is_some() {
            // For a long time we tried using `_HOST` in principals, e.g. `jn/_HOST@REALM.COM`.
            // Turns out there are a lot of code paths that check the principal of the requester using a reverse lookup of the incoming IP address
            // and getting a different hostname than the principal has.
            // What ultimately killed this approach was
            //
            // 2023-05-30 09:23:01,745 ERROR namenode.EditLogInputStream (EditLogFileInputStream.java:nextOpImpl(220)) - caught exception initializing https://hdfs-journalnode-default-1.hdfs-journalnode-default.kuttl-test-fine-rat.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A595659877%3A1685437352616%3ACID-90c52400-5b07-49bf-bdbe-3469bbdc5ebb&inProgressOk=true
            // org.apache.hadoop.hdfs.server.common.HttpGetFailedException: Fetch of https://hdfs-journalnode-default-1.hdfs-journalnode-default.kuttl-test-fine-rat.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A595659877%3A1685437352616%3ACID-90c52400-5b07-49bf-bdbe-3469bbdc5ebb&inProgressOk=true failed with status code 403
            // Response message:
            // Only Namenode and another JournalNode may access this servlet
            //
            // After we have switched to using the following principals everything worked without problems

            let principal_host_part =
                format!("{hdfs_name}.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}");
            self.add("hadoop.security.authentication", "kerberos")
                .add("hadoop.registry.kerberos.realm", "${env.KERBEROS_REALM}")
                .add(
                    "dfs.journalnode.kerberos.principal",
                    &format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.journalnode.kerberos.internal.spnego.principal",
                    &format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal",
                    &format!("nn/{principal_host_part}"),
                )
                .add(
                    "dfs.datanode.kerberos.principal",
                    &format!("dn/{principal_host_part}"),
                )
                .add(
                    "dfs.web.authentication.kerberos.principal",
                    &format!("HTTP/{principal_host_part}"),
                )
                .add("dfs.journalnode.keytab.file", "/stackable/kerberos/keytab")
                .add("dfs.namenode.keytab.file", "/stackable/kerberos/keytab")
                .add("dfs.datanode.keytab.file", "/stackable/kerberos/keytab")
                .add(
                    "dfs.journalnode.kerberos.principal.pattern",
                    &format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal.pattern",
                    &format!("nn/{principal_host_part}"),
                )
                // Otherwise we fail with `java.io.IOException: No groups found for user nn`
                // Default value is `dr.who=`, so we include that here
                .add(
                    "hadoop.user.group.static.mapping.overrides",
                    "dr.who=;nn=;nm=;jn=;",
                );

            self.add_wire_encryption_settings();
        }
        self
    }

    pub fn security_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.add(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            self.add_wire_encryption_settings();
        }
        self
    }

    fn add_wire_encryption_settings(&mut self) -> &mut Self {
        self.add("hadoop.rpc.protection", "authentication");
        self
    }
}
