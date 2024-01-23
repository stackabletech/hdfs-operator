use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{
    constants::{SSL_CLIENT_XML, SSL_SERVER_XML},
    HdfsCluster,
};
use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    kube::{runtime::reflector::ObjectRef, ResourceExt},
};

use crate::config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Object has no namespace"))]
    ObjectHasNoNamespace {
        source: stackable_hdfs_crd::Error,
        obj_ref: ObjectRef<HdfsCluster>,
    },
}

/// We only support Kerberos for HDFS >= 3.3.x
/// With HDFS 3.2.2 we got weird errors, which *might* be caused by DNS lookup issues
/// The Stacktrace is documented in rust/operator/src/kerberos_hdfs_3.2_stacktrace.txt
pub fn is_supported(resolved_product_image: &ResolvedProductImage) -> bool {
    resolved_product_image.product_version.starts_with("3.2.")
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
    pub fn security_config(&mut self, hdfs: &HdfsCluster) -> Result<&mut Self> {
        if hdfs.authentication_config().is_some() {
            let principal_host_part = principal_host_part(hdfs)?;

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
                .add("dfs.journalnode.keytab.file", "/stackable/kerberos/keytab")
                .add("dfs.namenode.keytab.file", "/stackable/kerberos/keytab")
                .add("dfs.datanode.keytab.file", "/stackable/kerberos/keytab")
                .add(
                    "dfs.journalnode.kerberos.principal.pattern",
                    format!("jn/{principal_host_part}"),
                )
                .add(
                    "dfs.namenode.kerberos.principal.pattern",
                    format!("nn/{principal_host_part}"),
                )
                // Otherwise we fail with `java.io.IOException: No groups found for user nn`
                // Default value is `dr.who=`, so we include that here
                .add(
                    "hadoop.user.group.static.mapping.overrides",
                    "dr.who=;nn=;nm=;jn=;",
                );

            self.add_wire_encryption_settings();
        }
        Ok(self)
    }

    pub fn security_discovery_config(&mut self, hdfs: &HdfsCluster) -> Result<&mut Self> {
        if hdfs.has_kerberos_enabled() {
            let principal_host_part = principal_host_part(hdfs)?;

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
        Ok(self)
    }

    fn add_wire_encryption_settings(&mut self) -> &mut Self {
        self.add("hadoop.rpc.protection", "privacy");
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
fn principal_host_part(hdfs: &HdfsCluster) -> Result<String> {
    let hdfs_name = hdfs.name_any();
    let hdfs_namespace = hdfs
        .namespace_or_error()
        .with_context(|_| ObjectHasNoNamespaceSnafu {
            obj_ref: ObjectRef::from_obj(hdfs),
        })?;
    Ok(format!(
        "{hdfs_name}.{hdfs_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}"
    ))
}
