#!/usr/bin/env bash
set -euo pipefail

mkdir -p etc
kubectl -n "$NAMESPACE" get cm/hdfs -o jsonpath='{.data.core-site\.xml}' > etc/core-site.xml
kubectl -n "$NAMESPACE" get cm/hdfs -o jsonpath='{.data.hdfs-site\.xml}' > etc/hdfs-site.xml

# Run a "vanilla"/upstream container outside of k8s to ensure that the client doesn't rely on any stackableisms or running inside of k8s.
docker run --rm -it --volume (pwd)/etc:/opt/hadoop/etc:ro --volume (pwd):/data:ro apache/hadoop:"{{ test_scenario['values']['hadoop-external-client-docker-image'] }}" hdfs dfs -put /data/testdata.txt /
