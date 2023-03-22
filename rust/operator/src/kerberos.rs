use indoc::formatdoc;
use stackable_hdfs_crd::{
    constants::{HADOOP_SECURITY_AUTHENTICATION, SSL_CLIENT_XML, SSL_SERVER_XML},
    HdfsCluster, HdfsRole,
};
use stackable_operator::{
    builder::{ContainerBuilder, PodBuilder, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
    commons::product_image_selection::ResolvedProductImage,
};

use crate::{
    config::{CoreSiteConfigBuilder, HdfsSiteConfigBuilder},
    hdfs_controller::KEYSTORE_DIR_NAME,
};

impl HdfsSiteConfigBuilder {
    pub fn kerberos_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.add("dfs.block.access.token.enable", "true")
                .add("dfs.data.transfer.protection", "authentication")
                .add("dfs.http.policy", "HTTPS_ONLY")
                .add("dfs.https.server.keystore.resource", SSL_SERVER_XML)
                .add("dfs.https.client.keystore.resource", SSL_CLIENT_XML);
        }
        self
    }

    pub fn kerberos_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.add("dfs.data.transfer.protection", "authentication");
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
        if hdfs.has_kerberos_enabled() {
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
        }
        self
    }

    pub fn kerberos_discovery_config(&mut self, hdfs: &HdfsCluster) -> &mut Self {
        if hdfs.has_kerberos_enabled() {
            self.add(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        }
        self
    }
}

pub fn create_tls_cert_bundle_init_container_and_volumes(
    pb: &mut PodBuilder,
    https_secret_class: &str,
    resolved_product_image: &ResolvedProductImage,
) {
    pb.add_volume(
        VolumeBuilder::new("tls")
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(https_secret_class)
                    .with_pod_scope()
                    .with_node_scope()
                    .build(),
            )
            .build(),
    );

    pb.add_volume(
        VolumeBuilder::new("keystore")
            .with_empty_dir(Option::<String>::None, None)
            .build(),
    );

    let create_tls_cert_bundle_init_container =
        ContainerBuilder::new("create-tls-cert-bundle")
            .unwrap()
            .image_from_product_image(resolved_product_image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![formatdoc!(
                    r###"
                    echo "Cleaning up truststore - just in case"
                    rm -f {KEYSTORE_DIR_NAME}/truststore.p12
                    echo "Creating truststore"
                    keytool -importcert -file /stackable/tls/ca.crt -keystore {KEYSTORE_DIR_NAME}/truststore.p12 -storetype pkcs12 -noprompt -alias ca_cert -storepass changeit
                    echo "Creating certificate chain"
                    cat /stackable/tls/ca.crt /stackable/tls/tls.crt > {KEYSTORE_DIR_NAME}/chain.crt
                    echo "Cleaning up keystore - just in case"
                    rm -f {KEYSTORE_DIR_NAME}/keystore.p12
                    echo "Creating keystore"
                    openssl pkcs12 -export -in {KEYSTORE_DIR_NAME}/chain.crt -inkey /stackable/tls/tls.key -out {KEYSTORE_DIR_NAME}/keystore.p12 --passout pass:changeit"###
                )])
                // Only this init container needs the actual cert (from tls volume) to create the truststore + keystore from
                .add_volume_mount("tls", "/stackable/tls")
                .add_volume_mount("keystore", KEYSTORE_DIR_NAME)
            .build();
    pb.add_init_container(create_tls_cert_bundle_init_container);
}
