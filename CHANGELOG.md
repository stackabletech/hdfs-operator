# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- The possibility to specifiy `configOverrides` and `envOverrides` ([#122]).
- Reconciliation errors are now reported as Kubernetes events ([#130]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#134]).

### Changed

- `operator-rs` `0.10.0` -> `0.13.0` ([#130],[#134]).

[#122]: https://github.com/stackabletech/hdfs-operator/pull/122
[#130]: https://github.com/stackabletech/hdfs-operator/pull/130
[#134]: https://github.com/stackabletech/hdfs-operator/pull/134

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
