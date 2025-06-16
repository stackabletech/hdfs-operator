# Fetch metrics from the built-in Prometheus endpoint of HDFS components.

import logging
import sys

import requests


def check_metrics(
    namespace: str, role: str, port: int, expected_metrics: list[str]
) -> None:
    response: requests.Response = requests.get(
        f"http://hdfs-{role}-default-0.hdfs-{role}-default.{namespace}.svc.cluster.local:{port}/prom",
        timeout=10,
    )
    assert response.ok, "Requesting metrics failed"

    # Split the response into lines to check for metric names at the beginning of each line.
    # This is a bit slower than using a regex but it allows to use special characters like "{}" in metric names
    # without needing to escape them.
    response_lines = response.text.splitlines()
    for metric in expected_metrics:
        # Use any() with a generator to stop early if the metric is found.
        assert any((line.startswith(metric) for line in response_lines)) is True, (
            f"Metric '{metric}' not found for {role}"
        )


def check_namenode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-namenode-default-0"}',
        # Counter suffixed with "_total"
        # The metric attributes can change so we remove them from the expected metric.
        # The full name looks like: 'fs_namesystem_files_total{context="dfs",enabledecpolicies="RS-6-3-1024k",hastate="active",totalsynctimes="4 7 ",hostname="hdfs-namenode-default-0"}',
        "fs_namesystem_files_total",
        # Metric suffixed with "_created"
        'namenode_files_created{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-0"}',
        # Boolean metric
        # 'hadoop_namenode_security_enabled{kind="NameNodeStatus",role="NameNode",service="HDFS"}',
        # Non-special metric
        'namenode_files_deleted{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-0"}',
    ]

    check_metrics(namespace, "namenode", 9870, expected_metrics)


def check_datanode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-datanode-default-0"}',
        # Kind "FSDatasetState" suffixed with "_total"
        # 'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total{context="FSDatasetState",storageinfo="FSDataset{dirpath=\'[/stackable/data/hdd/datanode,/stackable/data/hdd-1/datanode, /stackable/data/ssd/datanode]\'}",hostname="hdfs-datanode-default-0"}',
        "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total",
        # Kind "FSDatasetState"
        # 'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_capacity{context="FSDatasetState",storageinfo="FSDataset{dirpath=\'[/stackable/data/hdd/datanode, /stackable/data/hdd-1/datanode, /stackable/data/ssd/datanode]\'}",hostname="hdfs-datanode-default-0"}',
        "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_capacity",
        # Kind "DataNodeActivity" suffixed with "_info"
        'datanode_blocks_get_local_path_info{sessionid="null",context="dfs",hostname="hdfs-datanode-default-0"}',
        # Kind "DataNodeActivity"
        'datanode_blocks_read{sessionid="null",context="dfs",hostname="hdfs-datanode-default-0"}',
        # Counter suffixed with "_total"
        # 'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total{context="FSDatasetState",storageinfo="FSDataset{dirpath=\'[/stackable/data/hdd/datanode,/stackable/data/hdd-1/datanode, /stackable/data/ssd/datanode]\'}",hostname="hdfs-datanode-default-0"}',
        "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total",
        # Boolean metric
        #'hadoop_datanode_security_enabled{kind="DataNodeInfo",role="DataNode",service="HDFS"}',
        # Non-special metric
        'jvm_metrics_gc_count{context="jvm",processname="DataNode",sessionid="null",hostname="hdfs-datanode-default-0"}',
    ]

    check_metrics(namespace, "datanode", 9864, expected_metrics)


def check_journalnode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-journalnode-default-0"}',
        # Non-special metric
        'journal_node_bytes_written{context="dfs",journalid="hdfs",hostname="hdfs-journalnode-default-0"}',
        # There is no boolean metric in JournalNode.
    ]

    check_metrics(namespace, "journalnode", 8480, expected_metrics)


if __name__ == "__main__":
    namespace_arg: str = sys.argv[1]
    product_version_arg: str = sys.argv[2]

    logging.basicConfig(
        level="DEBUG",
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    check_namenode_metrics(namespace_arg, product_version_arg)
    check_datanode_metrics(namespace_arg, product_version_arg)
    check_journalnode_metrics(namespace_arg, product_version_arg)

    print("All expected metrics found")
