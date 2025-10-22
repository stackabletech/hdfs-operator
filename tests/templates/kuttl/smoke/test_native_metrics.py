# Every rule in the JMX configuration is covered by one expected metric.

import re
import sys
import logging

import requests


def check_metrics(
    namespace: str, role: str, port: int, expected_metrics: list[str]
) -> None:
    response: requests.Response = requests.get(
        f"http://hdfs-{role}-default-metrics.{namespace}.svc.cluster.local:{port}/prom",
        timeout=10,
    )
    assert response.ok, "Requesting metrics failed"

    for metric in expected_metrics:
        assert re.search(f"^{metric}", response.text, re.MULTILINE) is not None, (
            f"Metric '{metric}' not found for {role}"
        )


def check_namenode_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = [
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-namenode-default-',
        'namenode_total_file_ops{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default',
        'namenode_files_created{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-',
        'namenode_files_deleted{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-',
    ]

    check_metrics(namespace, "namenode", 9870, expected_metrics)


def check_datanode_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = [
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-datanode-default-0"}',
        'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_capacity{context="FSDatasetState",storageinfo="FSDataset{dirpath=\'[/stackable/data/data/datanode]\'}",hostname="hdfs-datanode-default-0"}',
        'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total{context="FSDatasetState",storageinfo="FSDataset{dirpath=\'[/stackable/data/data/datanode]\'}",hostname="hdfs-datanode-default-0"}',
        'datanode_blocks_get_local_path_info{sessionid="null",context="dfs",hostname="hdfs-datanode-default-0"}',
        'datanode_blocks_read{sessionid="null",context="dfs",hostname="hdfs-datanode-default-0"}',
        'jvm_metrics_gc_count{context="jvm",processname="DataNode",sessionid="null",hostname="hdfs-datanode-default-0"}',
    ]

    check_metrics(namespace, "datanode", 9864, expected_metrics)


def check_journalnode_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = [
        'metrics_system_num_active_sources{context="metricssystem",hostname="hdfs-journalnode-default-0"}',
        'journal_node_bytes_written{context="dfs",journalid="hdfs",hostname="hdfs-journalnode-default-0"}',
    ]

    check_metrics(namespace, "journalnode", 8480, expected_metrics)


if __name__ == "__main__":
    namespace_arg: str = sys.argv[1]

    logging.basicConfig(
        level="DEBUG",
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    check_namenode_metrics(namespace_arg)
    check_datanode_metrics(namespace_arg)
    check_journalnode_metrics(namespace_arg)

    print("All expected metrics found")
