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
            "dfs.encrypt.data.transfer.cipher.suites",
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
        role_group_service_name: &str,
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
                )
                // The following are required to secure WebHDFS
                .add("hadoop.http.authentication.type", "kerberos")
                .add(
                    "hadoop.http.authentication.simple.anonymous.allowed",
                    "false",
                )
                // SPNEGO principal for this pod's own FQDN. A single named principal is read
                // through the JDK; `*` would make Hadoop parse the keytab with Kerby, which
                // fails on the hole secret-operator leaves where it removes its dummy entry.
                .add(
                    "hadoop.http.authentication.kerberos.principal",
                    format!(
                        "HTTP/${{env.POD_NAME}}.{role_group_service_name}.{namespace}.svc.{cluster_domain}@${{env.KERBEROS_REALM}}",
                        namespace = kerberos.cluster_namespace,
                        cluster_domain = cluster_info.cluster_domain,
                    ),
                )
                .add(
                    "hadoop.http.authentication.kerberos.keytab",
                    format!("{KERBEROS_CONTAINER_PATH}/keytab"),
                )
                // DataNodes build their HTTP server without `DFSUtil.getHttpServerTemplate`, so they
                // only get an authentication filter if one is listed here. NameNodes and JournalNodes
                // drop `AuthenticationFilterInitializer` from this list and install `AuthFilterInitializer`.
                .add(
                    "hadoop.http.filter.initializers",
                    "org.apache.hadoop.http.lib.StaticUserWebFilter,org.apache.hadoop.security.AuthenticationFilterInitializer",
                )
                // Prometheus cannot do SPNEGO, so the metrics endpoint skips authentication.
                .add("hadoop.http.authentication.kerberos.endpoint.whitelist", "/prom")
                // NameNodes fetch edits from JournalNodes over SPNEGO, which yields the caller's short
                // name. `GetJournalEditServlet.isValidRequestor` only accepts a short name equal to the
                // JournalNode's own, so the JournalNode principal maps to the NameNode's short name.
                .add(
                    "hadoop.security.auth_to_local",
                    format!(
                        "RULE:[2:$1/$2@$0](jn/{host}@${{env.KERBEROS_REALM}})s/.*/nn/\nDEFAULT",
                        host = format!("{cluster}.{namespace}.svc.{cluster_domain}",
                            cluster = kerberos.cluster_name,
                            namespace = kerberos.cluster_namespace,
                            cluster_domain = cluster_info.cluster_domain,
                        ).replace('.', "\\."),
                    ),
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

#[cfg(test)]
mod tests {
    use indoc::formatdoc;
    use rstest::rstest;

    use super::*;
    use crate::controller::build::properties::test_support::{cluster_info, hadoop_property_value};

    /// Renders the core-site security config of the `hdfs` cluster in the `default` namespace
    /// for a pod of the role group Service `hdfs-namenode-default`.
    fn render(authentication_enabled: bool) -> String {
        let kerberos = KerberosConfig {
            cluster_name: "hdfs",
            cluster_namespace: "default",
            authentication_enabled,
            kerberos_enabled: authentication_enabled,
            authorization_enabled: false,
        };
        CoreSiteConfigBuilder::new("hdfs".to_owned())
            .security_config(&kerberos, &cluster_info(), "hdfs-namenode-default")
            .build_as_xml()
    }

    fn property(name: &str, value: &str) -> String {
        formatdoc! {"
            <name>{name}</name>
                <value>{value}</value>"}
    }

    /// Hadoop's HTTP clients request a ticket for `HTTP/<host in URL>`, and daemons address
    /// each other by pod FQDN, so each pod must accept its own FQDN principal.
    #[test]
    fn http_principal_is_the_pods_fqdn() {
        let xml = render(true);

        let expected = property(
            "hadoop.http.authentication.kerberos.principal",
            "HTTP/${env.POD_NAME}.hdfs-namenode-default.default.svc.cluster.local@${env.KERBEROS_REALM}",
        );
        assert!(xml.contains(&expected), "{xml}");
    }

    #[rstest]
    #[case::kerberos_type("hadoop.http.authentication.type", "kerberos")]
    #[case::no_anonymous_access("hadoop.http.authentication.simple.anonymous.allowed", "false")]
    #[case::keytab(
        "hadoop.http.authentication.kerberos.keytab",
        "/stackable/kerberos/keytab"
    )]
    #[case::datanode_filter(
        "hadoop.http.filter.initializers",
        "org.apache.hadoop.http.lib.StaticUserWebFilter,org.apache.hadoop.security.AuthenticationFilterInitializer"
    )]
    #[case::unauthenticated_metrics(
        "hadoop.http.authentication.kerberos.endpoint.whitelist",
        "/prom"
    )]
    fn http_endpoints_require_authentication(#[case] name: &str, #[case] value: &str) {
        let xml = render(true);

        assert!(xml.contains(&property(name, value)), "{xml}");
    }

    /// `GetJournalEditServlet` only accepts a requester whose short name equals the
    /// JournalNode's own, so the JournalNode service principal must map to `nn`, while
    /// `DEFAULT` keeps every other principal's mapping. The XML writer encodes the newline
    /// between the two rules as `&#xA;`.
    #[test]
    fn journalnode_principal_maps_to_namenode_user() {
        let xml = render(true);

        let expected = property(
            "hadoop.security.auth_to_local",
            r"RULE:[2:$1/$2@$0](jn/hdfs\.default\.svc\.cluster\.local@${env.KERBEROS_REALM})s/.*/nn/&#xA;DEFAULT",
        );
        assert!(xml.contains(&expected), "{xml}");
    }

    /// The JournalNode logs in as `dfs.journalnode.kerberos.principal`, so the `auth_to_local`
    /// rule must match exactly that principal, with every dot of the host escaped so that the
    /// regular expression matches nothing else.
    #[test]
    fn auth_to_local_rule_matches_the_journalnode_principal() {
        let xml = render(true);
        let journalnode_principal =
            hadoop_property_value(&xml, "dfs.journalnode.kerberos.principal")
                .expect("the JournalNode principal should be set");
        let rule_pattern = hadoop_property_value(&xml, "hadoop.security.auth_to_local")
            .and_then(|rules| rules.strip_prefix("RULE:[2:$1/$2@$0]("))
            .and_then(|rule| rule.split_once(")s/"))
            .map(|(pattern, _)| pattern)
            .expect("the first auth_to_local rule should map a two-component principal");

        assert_eq!(rule_pattern.replace(r"\.", "."), journalnode_principal);
        let host_pattern = rule_pattern.replace("${env.KERBEROS_REALM}", "");
        assert!(
            !host_pattern.replace(r"\.", "").contains('.'),
            "unescaped dot in {rule_pattern:?}"
        );
    }

    #[rstest]
    #[case::http_authentication("hadoop.http.authentication.")]
    #[case::http_filter("hadoop.http.filter.initializers")]
    #[case::principal_mapping("hadoop.security.auth_to_local")]
    fn no_security_settings_without_authentication(#[case] name_prefix: &str) {
        let xml = render(false);

        assert!(!xml.contains(&format!("<name>{name_prefix}")), "{xml}");
    }
}
