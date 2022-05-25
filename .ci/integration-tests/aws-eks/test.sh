#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/hdfs-operator.git
(cd hdfs-operator/ && ./scripts/run_tests.sh)
exit_code=$?
./operator-logs.sh hdfs > /target/hdfs-operator.log
exit $exit_code
