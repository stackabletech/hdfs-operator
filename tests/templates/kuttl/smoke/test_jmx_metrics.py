# Every rule in the JMX configuration is covered by one expected metric.

import re
import sys
import logging

import requests


def check_metrics(
    namespace: str, role: str, port: int, expected_metrics: list[str]
) -> None:
    response: requests.Response = requests.get(
        f"http://hdfs-{role}-default-metrics.{namespace}.svc.cluster.local:{port}/metrics",
        timeout=10,
    )
    assert response.ok, "Requesting metrics failed"

    for metric in expected_metrics:
        assert re.search(f"^{metric}", response.text, re.MULTILINE) is not None, (
            f"Metric '{metric}' not found for {role}"
        )


def check_namenode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'hadoop_namenode_num_active_sources{kind="MetricsSystem",role="NameNode",service="HDFS",sub="Stats"}',
        # Attribute "Total"
        'hadoop_namenode_total{kind="NameNodeInfo",role="NameNode",service="HDFS"}',
        # Counter suffixed with "_total"
        'hadoop_namenode_files_total{kind="FSNamesystem",role="NameNode",service="HDFS"}',
        # Metric suffixed with "_created"
        'hadoop_namenode_files_created_{kind="NameNodeActivity",role="NameNode",service="HDFS"}',
        # Boolean metric
        'hadoop_namenode_security_enabled{kind="NameNodeStatus",role="NameNode",service="HDFS"}',
        # Non-special metric
        'hadoop_namenode_files_deleted{kind="NameNodeActivity",role="NameNode",service="HDFS"}',
    ]

    if product_version in ["3.3.4", "3.3.6"]:
        # Log counters were removed in 3.4.0 (HADOOP-17524).
        expected_metrics.extend(
            [
                # Metric suffixed with "_info"
                'hadoop_namenode_log_info_{kind="JvmMetrics",role="NameNode",service="HDFS"}',
            ]
        )

    check_metrics(namespace, "namenode", 8183, expected_metrics)


def check_datanode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'hadoop_datanode_num_active_sources{kind="MetricsSystem",role="DataNode",service="HDFS",sub="Stats"}',
        # Kind "FSDatasetState" suffixed with "_total"
        'hadoop_datanode_estimated_capacity_lost_total{fsdatasetid=".+",kind="FSDatasetState",role="DataNode",service="HDFS"}',
        # Kind "FSDatasetState"
        'hadoop_datanode_capacity{fsdatasetid=".+",kind="FSDatasetState",role="DataNode",service="HDFS"}',
        # Kind "DataNodeActivity" suffixed with "_info"
        'hadoop_datanode_blocks_get_local_path_info_{host="hdfs-datanode-default-0\\.hdfs-datanode-default-headless\\..+\\.svc\\.cluster\\.local",kind="DataNodeActivity",port="9866",role="DataNode",service="HDFS"}',
        # Kind "DataNodeActivity"
        'hadoop_datanode_blocks_read{host="hdfs-datanode-default-0\\.hdfs-datanode-default-headless\\..+\\.svc\\.cluster\\.local",kind="DataNodeActivity",port="9866",role="DataNode",service="HDFS"}',
        # Counter suffixed with "_total"
        'hadoop_datanode_estimated_capacity_lost_total{kind="FSDatasetState",role="DataNode",service="HDFS"}',
        # Boolean metric
        'hadoop_datanode_security_enabled{kind="DataNodeInfo",role="DataNode",service="HDFS"}',
        # Non-special metric
        'hadoop_datanode_gc_count{kind="JvmMetrics",role="DataNode",service="HDFS"}',
    ]

    if product_version in ["3.3.4", "3.3.6"]:
        # Log counters were removed in 3.4.0 (HADOOP-17524).
        expected_metrics.extend(
            [
                # Metric suffixed with "_info"
                'hadoop_datanode_log_info_{kind="JvmMetrics",role="DataNode",service="HDFS"}',
            ]
        )

    check_metrics(namespace, "datanode", 8082, expected_metrics)


def check_journalnode_metrics(
    namespace: str,
    product_version: str,
) -> None:
    expected_metrics: list[str] = [
        # Kind "MetricsSystem"
        'hadoop_journalnode_num_active_sources{kind="MetricsSystem",role="JournalNode",service="HDFS",sub="Stats"}',
        # Non-special metric
        'hadoop_journalnode_bytes_written{kind="Journal-hdfs",role="JournalNode",service="HDFS"}',
        # There is no boolean metric in JournalNode.
    ]

    if product_version in ["3.3.4", "3.3.6"]:
        # Log counters were removed in 3.4.0 (HADOOP-17524).
        expected_metrics.extend(
            [
                # Metric suffixed with "_info"
                'hadoop_journalnode_log_info_{kind="JvmMetrics",role="JournalNode",service="HDFS"}',
            ]
        )

    check_metrics(namespace, "journalnode", 8081, expected_metrics)


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
