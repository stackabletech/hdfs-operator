# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Default stackableVersion to operator version ([#381]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#384]).
- Support PodDisruptionBudgets ([#394]).
- Added support for 3.2.4, 3.3.6 ([#409]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#378], [#409]).
- Let secret-operator handle certificate conversion ([#392]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#381], [#394], [#404], [#405], [#409]).

### Fixed

- Don't default roleGroup replicas to zero when not specified ([#402]).

### Removed

- Removed support for 3.3.1, 3.3.3 ([#409]).

[#378]: https://github.com/stackabletech/hdfs-operator/pull/378
[#381]: https://github.com/stackabletech/hdfs-operator/pull/381
[#384]: https://github.com/stackabletech/hdfs-operator/pull/384
[#392]: https://github.com/stackabletech/hdfs-operator/pull/392
[#394]: https://github.com/stackabletech/hdfs-operator/pull/394
[#402]: https://github.com/stackabletech/hdfs-operator/pull/402
[#404]: https://github.com/stackabletech/hdfs-operator/pull/404
[#405]: https://github.com/stackabletech/hdfs-operator/pull/405
[#409]: https://github.com/stackabletech/hdfs-operator/pull/409

## [23.7.0] - 2023-07-14

### Added

- Add support for enabling secure mode with Kerberos ([#334]).
- Generate OLM bundle for Release 23.4.0 ([#350]).
- Missing CRD defaults for `status.conditions` field ([#354]).
- Set explicit resources on all containers ([#359]).
- Support podOverrides ([#368]).

### Changed

- Operator-rs: `0.40.2` -> `0.44.0` ([#349], [#372]).
- Use 0.0.0-dev product images for testing ([#351])
- Use testing-tools 0.2.0 ([#351])
- Run as root group ([#353]).
- Added kuttl test suites ([#364])
- Increase the size limit of the log volumes ([#372])

[#334]: https://github.com/stackabletech/hdfs-operator/pull/334
[#349]: https://github.com/stackabletech/hdfs-operator/pull/349
[#350]: https://github.com/stackabletech/hdfs-operator/pull/350
[#351]: https://github.com/stackabletech/hdfs-operator/pull/351
[#353]: https://github.com/stackabletech/hdfs-operator/pull/353
[#354]: https://github.com/stackabletech/hdfs-operator/pull/354
[#359]: https://github.com/stackabletech/hdfs-operator/pull/359
[#364]: https://github.com/stackabletech/hdfs-operator/pull/364
[#368]: https://github.com/stackabletech/hdfs-operator/pull/368
[#372]: https://github.com/stackabletech/hdfs-operator/pull/372

## [23.4.0] - 2023-04-17

### Added

- Deploy default and support custom affinities ([#319]).
- Added OLM bundle files ([#328]).
- Extend cluster resources for status and cluster operation (paused, stopped) ([#337]).
- Cluster status conditions ([#339]).

### Changed

- [Breaking] Moved top level config option to `clusterConfig` ([#326]).
- [BREAKING] Support specifying Service type.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` ([#340]).
- `operator-rs` `0.36.0` -> `0.40.2` ([#326], [#337], [#341], [#342]).
- Use `build_rbac_resources` from operator-rs ([#342]).

### Fixed

- Avoid empty log events dated to 1970-01-01 and improve the precision of the
  log event timestamps ([#341]).

### Removed

- Removed the `--debug` flag for HDFS container start up ([#332]).

[#319]: https://github.com/stackabletech/hdfs-operator/pull/319
[#326]: https://github.com/stackabletech/hdfs-operator/pull/326
[#328]: https://github.com/stackabletech/hdfs-operator/pull/328
[#332]: https://github.com/stackabletech/hdfs-operator/pull/332
[#337]: https://github.com/stackabletech/hdfs-operator/pull/337
[#339]: https://github.com/stackabletech/hdfs-operator/pull/339
[#340]: https://github.com/stackabletech/hdfs-operator/pull/340
[#341]: https://github.com/stackabletech/hdfs-operator/pull/341
[#342]: https://github.com/stackabletech/hdfs-operator/pull/342

## [23.1.0] - 2023-01-23

### Added

- Log aggregation added ([#290]).
- Support for multiple storage directories ([#296]).

### Changed

- [BREAKING] Use Product image selection instead of version. `spec.version` has been replaced by `spec.image` ([#281]).
- Updated stackable image versions ([#271]).
- Fix the previously ignored node selector on role groups ([#286]).
- `operator-rs` `0.25.2` -> `0.30.2` ([#276], [#286], [#290]).
- Replaced `thiserror` with `snafu` ([#290]).

[#271]: https://github.com/stackabletech/hdfs-operator/pull/271
[#276]: https://github.com/stackabletech/hdfs-operator/pull/276
[#281]: https://github.com/stackabletech/hdfs-operator/pull/281
[#286]: https://github.com/stackabletech/hdfs-operator/pull/286
[#290]: https://github.com/stackabletech/hdfs-operator/pull/290
[#296]: https://github.com/stackabletech/hdfs-operator/pull/296

## [0.6.0] - 2022-11-07

### Added

- Orphaned resources are deleted ([#249])
- Support Hadoop 3.3.4 ([#250])

### Changed

- `operator-rs` `0.24.0` -> `0.25.2` ([#249]).

[#249]: https://github.com/stackabletech/hdfs-operator/pull/249
[#250]: https://github.com/stackabletech/hdfs-operator/pull/250

### Fixed

- Set specified resource request and limit on namenode main container ([#259]).

[#259]: https://github.com/stackabletech/hdfs-operator/pull/259

## [0.5.0] - 2022-09-06

### Changed

- Include chart name when installing with a custom release name ([#205]).
- Added OpenShift compatibility ([#225]).
- Add recommended labels to NodePort services ([#240]).

[#205]: https://github.com/stackabletech/hdfs-operator/pull/205
[#225]: https://github.com/stackabletech/hdfs-operator/pull/225
[#240]: https://github.com/stackabletech/hdfs-operator/pull/240

## [0.4.0] - 2022-06-30

### Added

- The possibility to specify `configOverrides` and `envOverrides` ([#122]).
- Reconciliation errors are now reported as Kubernetes events ([#130]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify a single namespace to watch ([#134]).
- Config builder for `hdfs-site.xml` and `core-site.xml` ([#150]).
- Discovery configmap that exposes the namenode services for clients to connect ([#150]).
- Documented service discovery for namenodes ([#150]).
- Publish warning events when role replicas don't meet certain minimum requirements ([#162]).
- PVCs for data storage, cpu and memory limits are now configurable ([#164]).
- Fix environment variable names according to <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html#Configuring_Environment_of_Hadoop_Daemons> ([#164]).

### Changed

- `operator-rs` `0.10.0` -> `0.15.0` ([#130], [#134], [#148]).
- `HADOOP_OPTS` for jmx exporter specified to `HADOOP_NAMENODE_OPTS`, `HADOOP_DATANODE_OPTS` and `HADOOP_JOURNALNODE_OPTS` to fix cli tool ([#148]).
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 3.5.8` becomes (for example) `version: 3.5.8-stackable0.1.0` ([#180])

[#122]: https://github.com/stackabletech/hdfs-operator/pull/122
[#130]: https://github.com/stackabletech/hdfs-operator/pull/130
[#134]: https://github.com/stackabletech/hdfs-operator/pull/134
[#148]: https://github.com/stackabletech/hdfs-operator/pull/148
[#150]: https://github.com/stackabletech/hdfs-operator/pull/150
[#162]: https://github.com/stackabletech/hdfs-operator/pull/162
[#164]: https://github.com/stackabletech/hdfs-operator/pull/164
[#180]: https://github.com/stackabletech/hdfs-operator/pull/180

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
