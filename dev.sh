#!/bin/env sh

#cargo build
# this is because target is in .gitignore
cp target/debug/stackable-hdfs-operator .
docker build -t docker.stackable.tech/stackable/hdfs-operator:0.3.0-nightly -f docker/Dockerfile.devel .
rm stackable-hdfs-operator
kind load docker-image docker.stackable.tech/stackable/hdfs-operator:0.3.0-nightly --name hdfs
