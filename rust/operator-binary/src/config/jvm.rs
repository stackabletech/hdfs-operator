use snafu::{ResultExt, Snafu};
use stackable_hdfs_crd::{constants::JVM_SECURITY_PROPERTIES_FILE, HdfsRole};
use stackable_operator::{
    k8s_openapi::api::core::v1::ResourceRequirements,
    memory::{BinaryMultiple, MemoryQuantity},
};

const JVM_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid java heap config for {role:?}"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::memory::Error,
        role: String,
    },
}

// All init or sidecar containers must have access to the following settings.
// As the Prometheus metric emitter is not part of this config it's safe to use for hdfs cli tools as well.
// This will not only enable the init containers to work, but also the user to run e.g.
// `bin/hdfs dfs -ls /` without getting `Caused by: java.lang.IllegalArgumentException: KrbException: Cannot locate default realm`
// because the `-Djava.security.krb5.conf` setting is missing
pub fn construct_global_jvm_args(kerberos_enabled: bool) -> String {
    let mut jvm_args = Vec::new();

    if kerberos_enabled {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_owned());
    }

    // TODO: Handle user input
    jvm_args.join(" ")
}

pub fn construct_role_specific_jvm_args(
    role: &HdfsRole,
    kerberos_enabled: bool,
    resources: Option<&ResourceRequirements>,
    config_dir: &str,
    metrics_port: u16,
) -> Result<String, Error> {
    let mut jvm_args = Vec::new();

    if let Some(memory_limit) = resources.and_then(|r| r.limits.as_ref()?.get("memory")) {
        let memory_limit = MemoryQuantity::try_from(memory_limit).with_context(|_| {
            InvalidJavaHeapConfigSnafu {
                role: role.to_string(),
            }
        })?;
        jvm_args.push(format!(
            "-Xmx{}",
            (memory_limit * JVM_HEAP_FACTOR)
                .scale_to(BinaryMultiple::Kibi)
                .format_for_java()
                .with_context(|_| {
                    InvalidJavaHeapConfigSnafu {
                        role: role.to_string(),
                    }
                })?
        ));
    }

    jvm_args.extend([format!(
            "-Djava.security.properties={config_dir}/{JVM_SECURITY_PROPERTIES_FILE}",
        ),format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={metrics_port}:/stackable/jmx/{role}.yaml",
        )]);

    if kerberos_enabled {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_string());
    }

    // TODO: Handle user input
    Ok(jvm_args.join(" "))
}
