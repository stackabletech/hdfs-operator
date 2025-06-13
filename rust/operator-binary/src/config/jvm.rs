use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::core::v1::ResourceRequirements,
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::JvmArgumentOverrides,
};

use crate::{
    crd::{HdfsNodeRole, constants::JVM_SECURITY_PROPERTIES_FILE, v1alpha1},
    security::kerberos::KERBEROS_CONTAINER_PATH,
};

const JVM_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid java heap config for {role:?}"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::memory::Error,
        role: String,
    },

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: crate::crd::Error },
}

// All init or sidecar containers must have access to the following settings.
// As the Prometheus metric emitter is not part of this config it's safe to use for hdfs cli tools as well.
// This will not only enable the init containers to work, but also the user to run e.g.
// `bin/hdfs dfs -ls /` without getting `Caused by: java.lang.IllegalArgumentException: KrbException: Cannot locate default realm`
// because the `-Djava.security.krb5.conf` setting is missing
pub fn construct_global_jvm_args(kerberos_enabled: bool) -> String {
    let mut jvm_args = Vec::new();

    if kerberos_enabled {
        jvm_args.push(format!(
            "-Djava.security.krb5.conf={KERBEROS_CONTAINER_PATH}/krb5.conf"
        ));
    }

    // We do *not* add user overrides to the global JVM args, but only the role specific JVM arguments.
    // This allows users to configure stuff for the server (probably what they want to do), without
    // also influencing e.g. startup scripts.
    //
    // However, this is just an assumption. If it is wrong users can still envOverride the global
    // JVM args.
    //
    // Please feel absolutely free to change this behavior!
    jvm_args.join(" ")
}

pub fn construct_role_specific_jvm_args(
    hdfs: &v1alpha1::HdfsCluster,
    hdfs_role: &HdfsNodeRole,
    role_group: &str,
    kerberos_enabled: bool,
    resources: Option<&ResourceRequirements>,
    config_dir: &str,
    metrics_port: u16,
) -> Result<String, Error> {
    let mut jvm_args = Vec::new();

    if let Some(memory_limit) = resources.and_then(|r| r.limits.as_ref()?.get("memory")) {
        let memory_limit = MemoryQuantity::try_from(memory_limit).with_context(|_| {
            InvalidJavaHeapConfigSnafu {
                role: hdfs_role.to_string(),
            }
        })?;
        let heap = memory_limit.scale_to(BinaryMultiple::Mebi) * JVM_HEAP_FACTOR;
        let heap = heap
            .format_for_java()
            .with_context(|_| InvalidJavaHeapConfigSnafu {
                role: hdfs_role.to_string(),
            })?;

        jvm_args.push(format!("-Xms{heap}"));
        jvm_args.push(format!("-Xmx{heap}"));
    }

    jvm_args.extend([
        format!("-Djava.security.properties={config_dir}/{JVM_SECURITY_PROPERTIES_FILE}"),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={metrics_port}:/stackable/jmx/{hdfs_role}.yaml")
    ]);
    if kerberos_enabled {
        jvm_args.push(format!(
            "-Djava.security.krb5.conf={KERBEROS_CONTAINER_PATH}/krb5.conf"
        ));
    }

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged_jvm_args = hdfs
        .get_merged_jvm_argument_overrides(hdfs_role, role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;

    Ok(merged_jvm_args
        .effective_jvm_config_after_merging()
        .join(" "))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{container::ContainerConfig, crd::constants::DEFAULT_NAME_NODE_METRICS_PORT};

    #[test]
    fn test_global_jvm_args() {
        assert_eq!(construct_global_jvm_args(false), "");
        assert_eq!(
            construct_global_jvm_args(true),
            format!("-Djava.security.krb5.conf={KERBEROS_CONTAINER_PATH}/krb5.conf")
        );
    }

    #[test]
    fn test_jvm_config_defaults_without_kerberos() {
        let input = r#"
        apiVersion: hdfs.stackable.tech/v1alpha1
        kind: HdfsCluster
        metadata:
          name: hdfs
        spec:
          image:
            productVersion: 3.4.1
          clusterConfig:
            zookeeperConfigMapName: hdfs-zk
          nameNodes:
            roleGroups:
              default:
                replicas: 1
        "#;
        let jvm_config = construct_test_role_specific_jvm_args(input, false);

        assert_eq!(
            jvm_config,
            "-Xms819m \
            -Xmx819m \
            -Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=8183:/stackable/jmx/namenode.yaml"
        );
    }

    #[test]
    fn test_jvm_config_jvm_argument_overrides() {
        let input = r#"
        apiVersion: hdfs.stackable.tech/v1alpha1
        kind: HdfsCluster
        metadata:
          name: hdfs
        spec:
          image:
            productVersion: 3.4.1
          clusterConfig:
            zookeeperConfigMapName: hdfs-zk
          nameNodes:
            config:
              resources:
                memory:
                  limit: 42Gi
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  # We need more memory!
                  removeRegex:
                    - -Xmx.*
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Xmx40000m
                    - -Dhttps.proxyPort=1234
        "#;
        let jvm_config = construct_test_role_specific_jvm_args(input, true);

        assert_eq!(
            jvm_config,
            format!(
                "-Xms34406m \
                -Djava.security.properties=/stackable/config/security.properties \
                -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=8183:/stackable/jmx/namenode.yaml \
                -Djava.security.krb5.conf={KERBEROS_CONTAINER_PATH}/krb5.conf \
                -Dhttps.proxyHost=proxy.my.corp \
                -Djava.net.preferIPv4Stack=true \
                -Xmx40000m \
                -Dhttps.proxyPort=1234"
            )
        );
    }

    fn construct_test_role_specific_jvm_args(hdfs_cluster: &str, kerberos_enabled: bool) -> String {
        let hdfs: v1alpha1::HdfsCluster =
            serde_yaml::from_str(hdfs_cluster).expect("illegal test input");

        let role = HdfsNodeRole::Name;
        let merged_config = role.merged_config(&hdfs, "default").unwrap();
        let container_config = ContainerConfig::from(role);
        let resources = container_config.resources(&merged_config);

        construct_role_specific_jvm_args(
            &hdfs,
            &role,
            "default",
            kerberos_enabled,
            resources.as_ref(),
            "/stackable/config",
            DEFAULT_NAME_NODE_METRICS_PORT,
        )
        .unwrap()
    }
}
