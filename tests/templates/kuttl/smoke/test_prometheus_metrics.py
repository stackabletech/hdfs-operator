# Native Prometheus metrics test
# We use a raw string for "expected_metrics" but still have to escape special regex characters "[", "]", "{" and "}"
# that we expect to be in the metrics string.

import re
import sys
import logging

import requests


def check_metrics(
    namespace: str, role: str, port: int, expected_metrics: list[str]
) -> None:
    response = requests.get(
        f"http://hdfs-{role}-default-metrics.{namespace}.svc.cluster.local:{port}/prom",
        timeout=10,
    )
    assert response.ok, "Requesting metrics failed for {role}."

    for metric in expected_metrics:
        regex = re.compile(metric, re.MULTILINE)
        assert regex.search(response.text) is not None, (
            f"Metric '{metric}' not found for {role}"
        )


def check_namenode_metrics(namespace: str) -> None:
    expected_metrics = [
        r'metrics_system_num_active_sources\{context="metricssystem",hostname="hdfs-namenode-default-\d+"\}',
        r'namenode_total_file_ops\{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-\d+"\}',
        r'namenode_files_created\{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-\d+"\}',
        r'namenode_files_deleted\{processname="NameNode",sessionid="null",context="dfs",hostname="hdfs-namenode-default-\d+"\}',
    ]

    check_metrics(namespace, "namenode", 9870, expected_metrics)


def check_datanode_metrics(
    namespace: str,
    datanode_pvc_arg: str
) -> None:
    expected_metrics: list[str] = [
        r'metrics_system_num_active_sources\{context="metricssystem",hostname="hdfs-datanode-default-\d+',
        r'datanode_blocks_get_local_path_info\{sessionid="null",context="dfs",hostname="hdfs-datanode-default-\d+"\}',
        r'datanode_blocks_read\{sessionid="null",context="dfs",hostname="hdfs-datanode-default-\d+"\}',
        r'jvm_metrics_gc_count\{context="jvm",processname="DataNode",sessionid="null",hostname="hdfs-datanode-default-\d+"\}',
    ]

    # metrics change depending on datanode pvcs 
    if datanode_pvc_arg == "default":
        expected_metrics.extend([
            r'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_capacity\{context="FSDatasetState",storageinfo="FSDataset\{dirpath=\'\[/stackable/data/data/datanode]\'\}",hostname="hdfs-datanode-default-\d+"\}',
            r'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total\{context="FSDatasetState",storageinfo="FSDataset\{dirpath=\'\[/stackable/data/data/datanode]\'\}",hostname="hdfs-datanode-default-\d+"\}',
        ])
    else:
        expected_metrics.extend([
            r'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_capacity\{context="FSDatasetState",storageinfo="FSDataset\{dirpath=\'\[/stackable/data/hdd/datanode, /stackable/data/hdd-1/datanode, /stackable/data/ssd/datanode]\'\}",hostname="hdfs-datanode-default-\d+"\}',
            r'org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_fs_dataset_impl_estimated_capacity_lost_total\{context="FSDatasetState",storageinfo="FSDataset\{dirpath=\'\[/stackable/data/hdd/datanode, /stackable/data/hdd-1/datanode, /stackable/data/ssd/datanode]\'\}",hostname="hdfs-datanode-default-\d+"\}',
        ])        

    check_metrics(namespace, "datanode", 9864, expected_metrics)


def check_journalnode_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = [
        r'metrics_system_num_active_sources\{context="metricssystem",hostname="hdfs-journalnode-default-\d+"\}',
        r'journal_node_bytes_written\{context="dfs",journalid="hdfs",hostname="hdfs-journalnode-default-\d+"\}',
    ]

    check_metrics(namespace, "journalnode", 8480, expected_metrics)


if __name__ == "__main__":
    namespace_arg: str = sys.argv[1]
    datanode_pvc_arg: str = sys.argv[2]

    logging.basicConfig(
        level="DEBUG",
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    check_namenode_metrics(namespace_arg)
    check_datanode_metrics(namespace_arg, datanode_pvc_arg)
    check_journalnode_metrics(namespace_arg)

    print("All expected native metrics found")
