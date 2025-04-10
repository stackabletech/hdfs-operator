# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- BREAKING: Replace stackable-operator `initialize_logging` with stackable-telemetry `Tracing` ([#661], [#668]).
  - The console log level was set by `HDFS_OPERATOR_LOG`, and is now set by `CONSOLE_LOG`.
  - The file log level was set by `HDFS_OPERATOR_LOG`, and is now set by `FILE_LOG`.
  - The file log directory was set by `HDFS_OPERATOR_LOG_DIRECTORY`, and is now set
    by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
  - Replace stackable-operator `print_startup_string` with `tracing::info!` with fields.
- BREAKING: Inject the vector aggregator address into the vector config using the env var `VECTOR_AGGREGATOR_ADDRESS` instead
    of having the operator write it to the vector config ([#671]).

### Fixed

- Use `json` file extension for log files ([#667]).
- Fix a bug where changes to ConfigMaps that are referenced in the HdfsCluster spec didn't trigger a reconciliation ([#671]).

[#661]: https://github.com/stackabletech/hdfs-operator/pull/661
[#671]: https://github.com/stackabletech/hdfs-operator/pull/671
[#667]: https://github.com/stackabletech/hdfs-operator/pull/667
[#668]: https://github.com/stackabletech/hdfs-operator/pull/668

## [25.3.0] - 2025-03-21

### Added

- The lifetime of auto generated TLS certificates is now configurable with the role and roleGroup
  config property `requestedSecretLifetime`. This helps reducing frequent Pod restarts ([#619]).
- Run a `containerdebug` process in the background of each HDFS container to collect debugging information ([#629]).
- Support configuring JVM arguments ([#636]).
- Aggregate emitted Kubernetes events on the CustomResources ([#643]).
- Add support for version `3.4.1` ([#656]).

### Changed

- Bump `stackable-operator` to 0.87.0 and `stackable-versioned` to 0.6.0 ([#655]).
- Switch the WebUI liveness probe from `httpGet` to checking the tcp socket.
  This helps with setups where configOverrides are used to enable security on the HTTP interfaces.
  As this results in `401` HTTP responses (instead of `200`), this previously failed the liveness checks.
- Set the JVM argument `-Xms` in addition to `-Xmx` (with the same value). This ensure consistent JVM configs across our products ([#636]).
- Default to OCI for image metadata and product image selection ([#640]).

[#619]: https://github.com/stackabletech/hdfs-operator/pull/619
[#629]: https://github.com/stackabletech/hdfs-operator/pull/629
[#636]: https://github.com/stackabletech/hdfs-operator/pull/636
[#640]: https://github.com/stackabletech/hdfs-operator/pull/640
[#643]: https://github.com/stackabletech/hdfs-operator/pull/643
[#655]: https://github.com/stackabletech/hdfs-operator/pull/655
[#656]: https://github.com/stackabletech/hdfs-operator/pull/656

## [24.11.1] - 2025-01-10

### Fixed

- BREAKING: Use distinct ServiceAccounts for the Stacklets, so that multiple Stacklets can be
  deployed in one namespace. Existing Stacklets will use the newly created ServiceAccounts after
  restart ([#616]).

[#616]: https://github.com/stackabletech/hdfs-operator/pull/616

## [24.11.0] - 2024-11-18

### Added

- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#591]).

### Changed

- Reduce CRD size from `1.4MB` to `136KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#574]):
  - `podOverrides`
  - `affinity`

### Fixed

- An invalid `HdfsCluster` doesn't cause the operator to stop functioning ([#594]).

[#574]: https://github.com/stackabletech/hdfs-operator/pull/574
[#591]: https://github.com/stackabletech/hdfs-operator/pull/591
[#594]: https://github.com/stackabletech/hdfs-operator/pull/594

## [24.7.0] - 2024-07-24

### Added

- Add experimental support for version `3.4.0` ([#545], [#557]). We do NOT support upgrading from 3.3 to 3.4 yet!

### Changed

- Bump `stackable-operator` from `0.64.0` to `0.70.0` ([#546]).
- Bump `product-config` from `0.6.0` to `0.7.0` ([#546]).
- Bump other dependencies ([#549]).

### Fixed

- Revert changing the getting started script to use the listener class `cluster-internal` ([#492]) ([#493]).
- Fix HDFS pods crashing on launch when any port names contain dashes ([#517]).
- Add labels to ephemeral (listener) volumes. These allow `stackablectl stack list` to display datanode endpoints ([#534])
- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#536]).

[#493]: https://github.com/stackabletech/hdfs-operator/pull/493
[#517]: https://github.com/stackabletech/hdfs-operator/pull/517
[#534]: https://github.com/stackabletech/hdfs-operator/pull/534
[#536]: https://github.com/stackabletech/hdfs-operator/pull/536
[#545]: https://github.com/stackabletech/hdfs-operator/pull/545
[#546]: https://github.com/stackabletech/hdfs-operator/pull/546
[#549]: https://github.com/stackabletech/hdfs-operator/pull/549
[#557]: https://github.com/stackabletech/hdfs-operator/pull/557

## [24.3.0] - 2024-03-20

### Added

- Added rack awareness support via topology provider implementation ([#429], [#495]).
- More CRD documentation ([#433]).
- Support for exposing HDFS clusters to clients outside of Kubernetes ([#450]).
- Helm: support labels in values.yaml ([#460]).
- Add support for OPA authorizer ([#474]).

### Changed

- Use new label builders ([#454]).
- Change the liveness probes to use the web UI port and to fail after
  one minute ([#491]).
- Update the getting started script to use the listener class `cluster-internal` ([#492]).

### Removed

- [BREAKING] `.spec.clusterConfig.listenerClass` has been split to `.spec.nameNodes.config.listenerClass` and `.spec.dataNodes.config.listenerClass`, migration will be required when using `external-unstable` ([#450], [#462]).
- [BREAKING] Removed legacy node selector on roleGroups ([#454]).
- Change default value of `dfs.ha.nn.not-become-active-in-safemode` from `true` to `false` ([#458]).
- Removed support for Hadoop `3.2` ([#475]).

### Fixed

- Include hdfs principals `dfs.journalnode.kerberos.principal`, `dfs.namenode.kerberos.principal`
  and `dfs.datanode.kerberos.principal` in the discovery ConfigMap in case Kerberos is enabled ([#451]).
- User provided env overrides now work as expected ([#499]).

[#429]: https://github.com/stackabletech/hdfs-operator/pull/429
[#450]: https://github.com/stackabletech/hdfs-operator/pull/450
[#451]: https://github.com/stackabletech/hdfs-operator/pull/451
[#454]: https://github.com/stackabletech/hdfs-operator/pull/454
[#458]: https://github.com/stackabletech/hdfs-operator/pull/458
[#460]: https://github.com/stackabletech/hdfs-operator/pull/460
[#462]: https://github.com/stackabletech/hdfs-operator/pull/462
[#474]: https://github.com/stackabletech/hdfs-operator/pull/474
[#475]: https://github.com/stackabletech/hdfs-operator/pull/475
[#491]: https://github.com/stackabletech/hdfs-operator/pull/491
[#492]: https://github.com/stackabletech/hdfs-operator/pull/492
[#495]: https://github.com/stackabletech/hdfs-operator/pull/495
[#499]: https://github.com/stackabletech/hdfs-operator/pull/499

## [23.11.0] - 2023-11-24

### Added

- Default stackableVersion to operator version ([#381]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#384]).
- Support PodDisruptionBudgets ([#394]).
- Support graceful shutdown ([#407]).
- Added support for 3.2.4, 3.3.6 ([#409]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#378], [#409]).
- Let secret-operator handle certificate conversion ([#392]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#381], [#394], [#404], [#405], [#409]).
- Consolidate Rust workspace members ([#425]).

### Fixed

- Don't default roleGroup replicas to zero when not specified ([#402]).
- [BREAKING] Removed field `autoFormatFs`, which was never read ([#422]).

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
[#407]: https://github.com/stackabletech/hdfs-operator/pull/407
[#409]: https://github.com/stackabletech/hdfs-operator/pull/409
[#422]: https://github.com/stackabletech/hdfs-operator/pull/422
[#425]: https://github.com/stackabletech/hdfs-operator/pull/425

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
