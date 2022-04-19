# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- The possibility to specify `configOverrides` and `envOverrides` ([#122]).
- Reconciliation errors are now reported as Kubernetes events ([#130]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#134]).
- Config builder for `hdfs-site.xml` and `core-site.xml` ([#150]).
- Discovery configmap that exposes the namenode services for clients to connect ([#150]).
- Documented service discovery for namenodes ([#150]).
- Publish warning events when role replicas don't meet certain minimum requirements ([#162]).
- PVCs can now be configured for data storage ([#164]).

### Changed

- `operator-rs` `0.10.0` -> `0.15.0` ([#130], [#134], [#148]).
- `HADOOP_OPTS` for jmx exporter specified to `HADOOP_NAMENODE_OPTS`, `HADOOP_DATANODE_OPTS` and `HADOOP_JOURNALNODE_OPTS` to fix cli tool ([#148]).

[#122]: https://github.com/stackabletech/hdfs-operator/pull/122
[#130]: https://github.com/stackabletech/hdfs-operator/pull/130
[#134]: https://github.com/stackabletech/hdfs-operator/pull/134
[#148]: https://github.com/stackabletech/hdfs-operator/pull/148
[#150]: https://github.com/stackabletech/hdfs-operator/pull/150
[#162]: https://github.com/stackabletech/hdfs-operator/pull/162
[#164]: https://github.com/stackabletech/hdfs-operator/pull/164

## [0.3.0] - 2022-02-14

### Added

- Monitoring scraping label `prometheus.io/scrape: true` ([#104]).

### Changed

- Complete rewrite to use `StatefulSet`s, `hostPath` volumes and the Kubernetes overlay network. ([#68])
- `operator-rs` `0.9.0` → `0.10.0` ([#104]).

[#68]: https://github.com/stackabletech/hdfs-operator/pull/68
[#104]: https://github.com/stackabletech/hdfs-operator/pull/104

## [0.2.0] - 2021-11-12

- `operator-rs` `0.3.0` → `0.4.0` ([#20]).
- Adapted pod image and container command to docker image ([#20]).
- Adapted documentation to represent new workflow with docker images ([#20]). 

[#20]: https://github.com/stackabletech/hdfs-operator/pull/20

## [0.1.0] - 2021-10-27

### Changed
- Switched to operator-rs tag 0.3.0 ([#13])

[#13]: https://github.com/stackabletech/hdfs-operator/pull/13
