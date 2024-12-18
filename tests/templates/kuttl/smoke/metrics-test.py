from requests import Response
import re
import requests
import sys
import logging


def check_metrics(
        namespace: str,
        role: str,
        port: int,
        expected_metrics: list[str]
    ) -> None:
    response: Response = requests.get(
        f'http://hdfs-{role}-default-0.hdfs-{role}-default.{namespace}.svc.cluster.local:{port}/metrics'
    )
    assert response.ok, "Requesting metrics failed"

    for metric in expected_metrics:
        assert re.search(f'^{metric}', response.text, re.MULTILINE) is not None, \
                f"Metric '{metric}' not found for {role}"


def check_namenode_metrics(
        namespace: str,
    ) -> None:
    expected_metrics = [
      # Kind "MetricsSystem"
      'hadoop_namenode_num_active_sources{kind="MetricsSystem",role="NameNode",service="HDFS",sub="Stats"}',
      # Attribute "Total"
      'hadoop_namenode_total{kind="NameNodeInfo",role="NameNode",service="HDFS"}',
      # Counter suffixed with "_total"
      'hadoop_namenode_files_total{kind="FSNamesystem",role="NameNode",service="HDFS"}',
      # Metric suffixed with "_created"
      'hadoop_namenode_files_created_{kind="NameNodeActivity",role="NameNode",service="HDFS"}',
      # Metric suffixed with "_info"
      'hadoop_namenode_log_info_{kind="JvmMetrics",role="NameNode",service="HDFS"}',
      # Non-special metric
      'hadoop_namenode_files_deleted{kind="NameNodeActivity",role="NameNode",service="HDFS"}',
    ]
    check_metrics(namespace, 'namenode', 8183, expected_metrics)


def check_datanode_metrics(
        namespace: str,
    ) -> None:
    expected_metrics = [
      # Kind "MetricsSystem"
      'hadoop_datanode_num_active_sources{kind="MetricsSystem",role="DataNode",service="HDFS",sub="Stats"}',
      # Kind "FSDatasetState" suffixed with "_total"
      'hadoop_datanode_estimated_capacity_lost_total{fsdatasetid=".+",kind="FSDatasetState",role="DataNode",service="HDFS"}',
      # Kind "FSDatasetState"
      'hadoop_datanode_capacity{fsdatasetid=".+",kind="FSDatasetState",role="DataNode",service="HDFS"}',
      # Kind "DataNodeActivity" suffixed with "_info"
      'hadoop_datanode_blocks_get_local_path_info_{host="hdfs-datanode-default-0\\.hdfs-datanode-default\\..+\\.svc\\.cluster\\.local",kind="DataNodeActivity",port="9866",role="DataNode",service="HDFS"}',
      # Kind "DataNodeActivity"
      'hadoop_datanode_blocks_read{host="hdfs-datanode-default-0\\.hdfs-datanode-default\\..+\\.svc\\.cluster\\.local",kind="DataNodeActivity",port="9866",role="DataNode",service="HDFS"}',
      # Counter suffixed with "_total"
      'hadoop_datanode_estimated_capacity_lost_total{kind="FSDatasetState",role="DataNode",service="HDFS"}',
      # Metric suffixed with "_info"
      'hadoop_datanode_log_info_{kind="JvmMetrics",role="DataNode",service="HDFS"}',
      # Non-special metric
      'hadoop_datanode_gc_count{kind="JvmMetrics",role="DataNode",service="HDFS"}',
    ]
    check_metrics(namespace, 'datanode', 8082, expected_metrics)


def check_journalnode_metrics(
        namespace: str,
    ) -> None:
    expected_metrics = [
      # Kind "MetricsSystem"
      'hadoop_journalnode_num_active_sources{kind="MetricsSystem",role="JournalNode",service="HDFS",sub="Stats"}',
      # Metric suffixed with "_info"
      'hadoop_journalnode_log_info_{kind="JvmMetrics",role="JournalNode",service="HDFS"}',
      # Non-special metric
      'hadoop_journalnode_bytes_written{kind="Journal-hdfs",role="JournalNode",service="HDFS"}',
    ]
    check_metrics(namespace, 'journalnode', 8081, expected_metrics)


if __name__ == "__main__":
    namespace: str = sys.argv[1]

    log_level = "DEBUG"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    check_namenode_metrics(namespace)
    check_datanode_metrics(namespace)
    check_journalnode_metrics(namespace)

    print("All expected metrics found")
