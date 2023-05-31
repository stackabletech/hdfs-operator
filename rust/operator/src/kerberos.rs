use stackable_hdfs_crd::{
    constants::{HADOOP_SECURITY_AUTHENTICATION, SSL_CLIENT_XML, SSL_SERVER_XML},
    HdfsCluster,
};
use stackable_operator::commons::product_image_selection::ResolvedProductImage;

use crate::{
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    hdfs_controller::Error,
};

pub fn check_if_supported(resolved_product_image: &ResolvedProductImage) -> Result<(), Error> {
    if resolved_product_image.product_version.starts_with("3.2.") {
        Err(Error::KerberosNotSupported {})
    } else {
        Ok(())
    }
}

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

// IMPORTANT: We only support Kerberos for HDFS >= 3.3.x
// With HDFS 3.2.2 we got weird errors, which *might* be caused by DNS lookup issues
//
// 2023-05-31 12:34:18,319 ERROR namenode.EditLogInputStream (EditLogFileInputStream.java:nextOpImpl(220)) - caught exception initializing https://hdfs-journalnode-default-2.hdfs-journalnode-default.kuttl-test-nice-eft.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A1740831343%3A1685535647411%3ACID-5bb822a0-549e-41ce-9997-ee657b6fc23f&inProgressOk=true
// java.io.IOException: org.apache.hadoop.security.authentication.client.AuthenticationException: Error while authenticating with endpoint: https://hdfs-journalnode-default-2.hdfs-journalnode-default.kuttl-test-nice-eft.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A1740831343%3A1685535647411%3ACID-5bb822a0-549e-41ce-9997-ee657b6fc23f&inProgressOk=true
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream$URLLog$1.run(EditLogFileInputStream.java:482)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream$URLLog$1.run(EditLogFileInputStream.java:474)
//         at java.base/java.security.AccessController.doPrivileged(Native Method)
//         at java.base/javax.security.auth.Subject.doAs(Subject.java:423)
//         at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1762)
//         at org.apache.hadoop.security.SecurityUtil.doAsUser(SecurityUtil.java:535)
//         at org.apache.hadoop.security.SecurityUtil.doAsCurrentUser(SecurityUtil.java:529)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream$URLLog.getInputStream(EditLogFileInputStream.java:473)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream.init(EditLogFileInputStream.java:157)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream.nextOpImpl(EditLogFileInputStream.java:218)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream.nextOp(EditLogFileInputStream.java:276)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogInputStream.readOp(EditLogInputStream.java:85)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogInputStream.skipUntil(EditLogInputStream.java:151)
//         at org.apache.hadoop.hdfs.server.namenode.RedundantEditLogInputStream.nextOp(RedundantEditLogInputStream.java:190)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogInputStream.readOp(EditLogInputStream.java:85)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogInputStream.skipUntil(EditLogInputStream.java:151)
//         at org.apache.hadoop.hdfs.server.namenode.RedundantEditLogInputStream.nextOp(RedundantEditLogInputStream.java:190)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogInputStream.readOp(EditLogInputStream.java:85)
//         at org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.loadEditRecords(FSEditLogLoader.java:243)
//         at org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.loadFSEdits(FSEditLogLoader.java:182)
//         at org.apache.hadoop.hdfs.server.namenode.FSImage.loadEdits(FSImage.java:914)
//         at org.apache.hadoop.hdfs.server.namenode.FSImage.loadFSImage(FSImage.java:761)
//         at org.apache.hadoop.hdfs.server.namenode.FSImage.recoverTransitionRead(FSImage.java:338)
//         at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.loadFSImage(FSNamesystem.java:1135)
//         at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.loadFromDisk(FSNamesystem.java:750)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.loadNamesystem(NameNode.java:658)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.initialize(NameNode.java:734)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.<init>(NameNode.java:977)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.<init>(NameNode.java:950)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.createNameNode(NameNode.java:1716)
//         at org.apache.hadoop.hdfs.server.namenode.NameNode.main(NameNode.java:1783)
// Caused by: org.apache.hadoop.security.authentication.client.AuthenticationException: Error while authenticating with endpoint: https://hdfs-journalnode-default-2.hdfs-journalnode-default.kuttl-test-nice-eft.svc.cluster.local:8481/getJournal?jid=hdfs&segmentTxId=1&storageInfo=-65%3A1740831343%3A1685535647411%3ACID-5bb822a0-549e-41ce-9997-ee657b6fc23f&inProgressOk=true
//         at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
//         at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
//         at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
//         at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator.wrapExceptionWithMessage(KerberosAuthenticator.java:232)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator.authenticate(KerberosAuthenticator.java:219)
//         at org.apache.hadoop.security.authentication.client.AuthenticatedURL.openConnection(AuthenticatedURL.java:348)
//         at org.apache.hadoop.hdfs.web.URLConnectionFactory.openConnection(URLConnectionFactory.java:186)
//         at org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream$URLLog$1.run(EditLogFileInputStream.java:480)
//         ... 30 more
// Caused by: org.apache.hadoop.security.authentication.client.AuthenticationException: GSSException: No valid credentials provided (Mechanism level: Server not found in Kerberos database (7) - LOOKING_UP_SERVER)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator.doSpnegoSequence(KerberosAuthenticator.java:360)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator.authenticate(KerberosAuthenticator.java:204)
//         ... 33 more
// Caused by: GSSException: No valid credentials provided (Mechanism level: Server not found in Kerberos database (7) - LOOKING_UP_SERVER)
//         at java.security.jgss/sun.security.jgss.krb5.Krb5Context.initSecContext(Krb5Context.java:773)
//         at java.security.jgss/sun.security.jgss.GSSContextImpl.initSecContext(GSSContextImpl.java:266)
//         at java.security.jgss/sun.security.jgss.GSSContextImpl.initSecContext(GSSContextImpl.java:196)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator$1.run(KerberosAuthenticator.java:336)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator$1.run(KerberosAuthenticator.java:310)
//         at java.base/java.security.AccessController.doPrivileged(Native Method)
//         at java.base/javax.security.auth.Subject.doAs(Subject.java:423)
//         at org.apache.hadoop.security.authentication.client.KerberosAuthenticator.doSpnegoSequence(KerberosAuthenticator.java:310)
//         ... 34 more
// Caused by: KrbException: Server not found in Kerberos database (7) - LOOKING_UP_SERVER
//         at java.security.jgss/sun.security.krb5.KrbTgsRep.<init>(KrbTgsRep.java:73)
//         at java.security.jgss/sun.security.krb5.KrbTgsReq.getReply(KrbTgsReq.java:226)
//         at java.security.jgss/sun.security.krb5.KrbTgsReq.sendAndGetCreds(KrbTgsReq.java:237)
//         at java.security.jgss/sun.security.krb5.internal.CredentialsUtil.serviceCredsSingle(CredentialsUtil.java:477)
//         at java.security.jgss/sun.security.krb5.internal.CredentialsUtil.serviceCreds(CredentialsUtil.java:340)
//         at java.security.jgss/sun.security.krb5.internal.CredentialsUtil.serviceCreds(CredentialsUtil.java:314)
//         at java.security.jgss/sun.security.krb5.internal.CredentialsUtil.acquireServiceCreds(CredentialsUtil.java:169)
//         at java.security.jgss/sun.security.krb5.Credentials.acquireServiceCreds(Credentials.java:490)
//         at java.security.jgss/sun.security.jgss.krb5.Krb5Context.initSecContext(Krb5Context.java:697)
//         ... 41 more
// Caused by: KrbException: Identifier doesn't match expected value (906)
//         at java.security.jgss/sun.security.krb5.internal.KDCRep.init(KDCRep.java:140)
//         at java.security.jgss/sun.security.krb5.internal.TGSRep.init(TGSRep.java:65)
//         at java.security.jgss/sun.security.krb5.internal.TGSRep.<init>(TGSRep.java:60)
//         at java.security.jgss/sun.security.krb5.KrbTgsRep.<init>(KrbTgsRep.java:55)
//         ... 49 more
