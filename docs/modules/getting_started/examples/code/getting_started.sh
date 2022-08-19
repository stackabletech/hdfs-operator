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

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait zookeeper-operator stackable-dev/zookeeper-operator --version 0.11.0-nightly
helm install --wait hdfs-operator stackable-dev/hdfs-operator --version 0.5.0-nightly
helm install --wait commons-operator stackable-dev/commons-operator --version 0.3.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.6.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.3.0-nightly \
  secret=0.6.0-nightly \
  zookeeper=0.11.0-nightly \
  hdfs=0.5.0-nightly
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

sleep 5

echo "Awaiting Zookeeper rollout finish"
# tag::watch-zk-rollout[]
kubectl rollout status --watch statefulset/simple-zk-server-default
# end::watch-zk-rollout[]

echo "Creating HDFS cluster"
# tag::install-hdfs[]
kubectl apply -f hdfs.yaml
# end::install-hdfs[]

sleep 5

echo "Awaiting HDFS rollout finish"
# tag::watch-hdfs-rollout[]
kubectl rollout status --watch statefulset/simple-hdfs-datanode-default
kubectl rollout status --watch statefulset/simple-hdfs-namenode-default
kubectl rollout status --watch statefulset/simple-hdfs-journalnode-default
# end::watch-hdfs-rollout[]

echo "Creating Helper"
# tag::install-webhdfs[]
kubectl apply -f webhdfs.yaml
# end::install-webhdfs[]

sleep 5

echo "Awaiting helper rollout finish"
# tag::watch-helper-rollout[]
kubectl rollout status --watch statefulset/webhdfs
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

