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
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait zookeeper-operator stackable-dev/zookeeper-operator --version 0.0.0-dev
helm install --wait hdfs-operator stackable-dev/hdfs-operator --version 0.0.0-dev
helm install --wait commons-operator stackable-dev/commons-operator --version 0.0.0-dev
helm install --wait secret-operator stackable-dev/secret-operator --version 0.0.0-dev
helm install --wait listener-operator stackable-dev/listener-operator --version 0.0.0-dev
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.0.0-dev \
  secret=0.0.0-dev \
  listener=0.0.0-dev \
  zookeeper=0.0.0-dev \
  hdfs=0.0.0-dev
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
set -x
file_status() {
  # tag::file-status[]
  kubectl exec -n default webhdfs-0 -- curl -s -XGET "http://listener-simple-hdfs-namenode-default-0:9870/webhdfs/v1/?op=LISTSTATUS"
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
  curl -s -XPUT -T /tmp/testdata.txt "http://listener-simple-hdfs-namenode-default-0:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=CREATE&noredirect=true"
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
  kubectl exec -n default webhdfs-0 -- curl -s -XDELETE "http://listener-simple-hdfs-namenode-default-0:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=DELETE"
  # end::delete-file[]
}

deleted=$(delete_file | jq -r '.boolean')

if [ "$deleted" == "true" ]; then
  echo "File was deleted!"
else
  echo "Detected status: $deleted"
  exit 1
fi

