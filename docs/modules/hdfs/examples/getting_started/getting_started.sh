#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operators, create a superset instance and briefly open a port
# forward and connect to the superset instance to make sure it is up and running.
# No running processes are left behind (i.e. the port-forwarding is closed at the end)

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Adding 'stackable-stable' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-stable https://repo.stackable.tech/repository/helm-stable/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait zookeeper-operator stackable-stable/zookeeper-operator --version 23.11.0
helm install --wait hdfs-operator stackable-stable/hdfs-operator --version 23.11.0
helm install --wait commons-operator stackable-stable/commons-operator --version 23.11.0
helm install --wait secret-operator stackable-stable/secret-operator --version 23.11.0
helm install --wait listener-operator stackable-stable/listener-operator --version 23.11.0
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=23.11.0 \
  secret=23.11.0 \
  listener=23.11.0 \
  zookeeper=23.11.0 \
  hdfs=23.11.0
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating Zookeeper cluster"
# tag::install-zk[]
kubectl apply -f zk.yaml
# end::install-zk[]

echo "Creating ZNode"
# tag::install-zk[]
kubectl apply -f znode.yaml
# end::install-zk[]



for (( i=1; i<=15; i++ ))
do
  echo "Waiting for ZookeeperCluster to appear ..."
  if eval kubectl get statefulset simple-zk-server-default; then
    break
  fi

  sleep 1
done

echo "Awaiting Zookeeper rollout finish"
# tag::watch-zk-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-zk-server-default
# end::watch-zk-rollout[]

echo "Creating HDFS cluster"
# tag::install-hdfs[]
kubectl apply -f hdfs.yaml
# end::install-hdfs[]

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for HdfsCluster to appear ..."
  if eval kubectl get statefulset simple-hdfs-datanode-default; then
    break
  fi

  sleep 1
done

echo "Awaiting HDFS rollout finish"
# tag::watch-hdfs-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-hdfs-datanode-default
kubectl rollout status --watch --timeout=5m statefulset/simple-hdfs-namenode-default
kubectl rollout status --watch --timeout=5m statefulset/simple-hdfs-journalnode-default
# end::watch-hdfs-rollout[]

echo "Creating Helper"
# tag::install-webhdfs[]
kubectl apply -f webhdfs.yaml
# end::install-webhdfs[]

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for Webhdfs helper to appear ..."
  if eval kubectl get statefulset webhdfs; then
    break
  fi

  sleep 1
done

echo "Awaiting helper rollout finish"
# tag::watch-helper-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/webhdfs
# end::watch-helper-rollout[]

file_status() {
  # tag::file-status[]
  kubectl exec -n default webhdfs-0 -- curl -s -XGET "http://simple-hdfs-namenode-default-0:9870/webhdfs/v1/?op=LISTSTATUS"
  # end::file-status[]
}

echo "Confirm that HDFS is empty..."
status=$(file_status | jq -r '.FileStatuses.FileStatus')

if [ "$status" == "[]" ]; then
  echo "As expected, HDFS is empty"
else
  echo "Detected status: $status"
  exit 1
fi

echo "Copy test file"
# tag::copy-file[]
kubectl cp -n default ./testdata.txt webhdfs-0:/tmp
# end::copy-file[]

create_file() {
  # tag::create-file[]
  kubectl exec -n default webhdfs-0 -- \
  curl -s -XPUT -T /tmp/testdata.txt "http://simple-hdfs-namenode-default-0:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=CREATE&noredirect=true"
  # end::create-file[]
}

location=$(create_file | jq -r '.Location')

echo "Redirect location: $location"

# tag::create-file-redirected[]
kubectl exec -n default webhdfs-0 -- curl -s -XPUT -T /tmp/testdata.txt "$location"
# end::create-file-redirected[]

echo "Confirm that HDFS is *not* empty..."
found_file=$(file_status | jq -r '.FileStatuses.FileStatus[0].pathSuffix')
echo "Created file: $found_file with status $(file_status)"

echo "Delete file"
delete_file() {
  # tag::delete-file[]
  kubectl exec -n default webhdfs-0 -- curl -s -XDELETE "http://simple-hdfs-namenode-default-0:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=DELETE"
  # end::delete-file[]
}

deleted=$(delete_file | jq -r '.boolean')

if [ "$deleted" == "true" ]; then
  echo "File was deleted!"
else
  echo "Detected status: $deleted"
  exit 1
fi

