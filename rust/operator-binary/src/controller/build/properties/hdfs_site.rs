//! Builds the `hdfs-site.xml` config file.

use std::collections::BTreeMap;

use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::config_overrides::KeyValueConfigOverrides,
};

use crate::{
    config::HdfsSiteConfigBuilder,
    controller::{ValidatedCluster, build::properties::resolved_overrides},
    crd::{AnyNodeConfig, HdfsNodeRole},
};

/// Renders `hdfs-site.xml`: operator defaults, HA wiring derived from the pod
/// refs, kerberos/OPA security config, with user `configOverrides` applied last.
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    merged_config: &AnyNodeConfig,
    overrides: KeyValueConfigOverrides,
) -> String {
    let cluster_config = &cluster.cluster_config;
    let namenode_podrefs = cluster.pod_refs(&HdfsNodeRole::Name);
    let journalnode_podrefs = cluster.pod_refs(&HdfsNodeRole::Journal);
    // IMPORTANT: these folders must be under the volume mount point, otherwise they will not
    // be formatted by the namenode, or used by the other services.
    // See also: https://github.com/apache-spark-on-k8s/kubernetes-HDFS/commit/aef9586ecc8551ca0f0a468c3b917d8c38f494a0
    //
    // Notes on configuration choices
    // ===============================
    // We used to set `dfs.ha.nn.not-become-active-in-safemode` to true here due to
    // badly worded HDFS documentation:
    // https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
    // This caused a deadlock with no namenode becoming active during a startup after
    // HDFS was completely down for a while.
    let mut hdfs_site = HdfsSiteConfigBuilder::new(cluster.name.as_ref().to_owned());
    hdfs_site
        .dfs_namenode_name_dir()
        .dfs_datanode_data_dir(
            merged_config
                .as_datanode()
                .map(|node| node.resources.storage.clone()),
        )
        .dfs_journalnode_edits_dir()
        .dfs_replication(cluster_config.dfs_replication)
        .dfs_name_services()
        .dfs_ha_namenodes(&namenode_podrefs)
        .dfs_namenode_shared_edits_dir(cluster_info, &journalnode_podrefs)
        .dfs_namenode_name_dir_ha(&namenode_podrefs)
        .dfs_namenode_rpc_address_ha(cluster_info, &namenode_podrefs)
        .dfs_namenode_http_address_ha(
            cluster_config.authentication.is_some(),
            cluster_info,
            &namenode_podrefs,
        )
        .dfs_client_failover_proxy_provider()
        .security_config(cluster_config.authentication.is_some())
        .add("dfs.ha.fencing.methods", "shell(/bin/true)")
        .add("dfs.ha.automatic-failover.enabled", "true")
        .add("dfs.ha.namenode.id", "${env.POD_NAME}")
        .add(
            "dfs.namenode.datanode.registration.unsafe.allow-address-override",
            "true",
        )
        .add("dfs.datanode.registered.hostname", "${env.POD_ADDRESS}")
        .add("dfs.datanode.registered.port", "${env.DATA_PORT}")
        .add("dfs.datanode.registered.ipc.port", "${env.IPC_PORT}")
        // The following two properties are set to "true" because there is a minor chance that data
        // written to HDFS is not synced to disk even if a block has been closed.
        // Users in HBase can control this explicitly for the WAL, but for flushes and compactions
        // I believe they can't as easily (if at all).
        // In theory, HBase should be able to recover from these failures, but that comes at a cost
        // and there's always a risk.
        // Enabling this behavior causes HDFS to sync to disk as soon as possible.
        .add("dfs.datanode.sync.behind.writes", "true")
        .add("dfs.datanode.synconclose", "true")
        // Defaults to 10 since at least 2011.
        // This controls the concurrent number of client connections (this includes DataNodes)
        // to the NameNode. Ideally, we'd scale this with the number of DataNodes but this would
        // lead to restarts of the NameNode.
        // This should lead to better performance due to more concurrency.
        .add("dfs.namenode.handler.count", "50")
        // Defaults to 10 since at least 2012.
        // This controls the concurrent number of client connections to the DataNodes.
        // We have no idea how many clients there may be, so it's hard to assign a good default.
        // Increasing to 50 should lead to better performance due to more concurrency, especially
        // with use-cases like HBase.
        .add("dfs.datanode.handler.count", "50")
        // The following two properties default to 2 and 4 respectively since around 2013.
        // They control the number of maximum replication "jobs" a NameNode assigns to
        // a DataNode in a single heartbeat.
        // Increasing this number will increase network usage during replication events
        // but can lead to faster recovery.
        .add("dfs.namenode.replication.max-streams", "4")
        .add("dfs.namenode.replication.max-streams-hard-limit", "8")
        // Defaults to 4096 and hasn't changed since at least 2011.
        // The number of threads used for actual data transfer, so not very CPU heavy
        // but IO bound. This is why the number is relatively high.
        // But today's Java and IO should be able to handle more, so bump it to 8192 for
        // better performance/concurrency.
        .add("dfs.datanode.max.transfer.threads", "8192");
    if cluster_config.authentication.is_some() {
        hdfs_site.add("dfs.datanode.registered.https.port", "${env.HTTPS_PORT}");
    } else {
        hdfs_site.add("dfs.datanode.registered.http.port", "${env.HTTP_PORT}");
    }
    if let Some(opa_config) = &cluster_config.authorization {
        opa_config.add_hdfs_site_config(&mut hdfs_site);
    }
    // the extend with config must come last in order to have overrides working!!!
    let overrides: BTreeMap<String, String> = resolved_overrides(overrides).collect();
    hdfs_site.extend(&overrides).build_as_xml()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::build::properties::test_support::{
            cluster_info, config_overrides, minimal_hdfs, validated_cluster,
        },
        crd::{HdfsNodeRole, v1alpha1},
    };

    fn namenode_merged_config(hdfs: &v1alpha1::HdfsCluster) -> AnyNodeConfig {
        HdfsNodeRole::Name
            .merged_config(hdfs, "default")
            .expect("merged config for the minimal namenode group")
    }

    #[test]
    fn renders_operator_defaults() {
        let merged = namenode_merged_config(&minimal_hdfs());
        let xml = build(
            &validated_cluster(),
            &cluster_info(),
            &merged,
            config_overrides(&[]),
        );
        assert!(
            xml.contains("<name>dfs.replication</name>\n    <value>3</value>"),
            "{xml}"
        );
        assert!(
            xml.contains("<name>dfs.datanode.max.transfer.threads</name>\n    <value>8192</value>"),
            "{xml}"
        );
    }

    #[test]
    fn user_overrides_win_over_defaults() {
        let merged = namenode_merged_config(&minimal_hdfs());
        let xml = build(
            &validated_cluster(),
            &cluster_info(),
            &merged,
            config_overrides(&[("dfs.replication", "5")]),
        );
        assert!(
            xml.contains("<name>dfs.replication</name>\n    <value>5</value>"),
            "{xml}"
        );
    }
}
