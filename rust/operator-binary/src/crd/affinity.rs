use stackable_operator::{
    commons::{
        affinity::{
            StackableAffinityFragment, affinity_between_cluster_pods, affinity_between_role_pods,
        },
        opa::OpaConfig,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{HdfsNodeRole, constants::APP_NAME};

/// `opa_config` is only passed for roles that send authorization requests to OPA.
pub fn get_affinity(
    cluster_name: &str,
    role: &HdfsNodeRole,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    let mut pod_affinities = vec![affinity_between_cluster_pods(APP_NAME, cluster_name, 20)];
    if let Some(opa_config) = opa_config {
        pod_affinities.push(affinity_between_role_pods(
            "opa",
            &opa_config.config_map_name, // The discovery cm has the same name as the OpaCluster itself
            "server",
            50,
        ));
    }

    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(pod_affinities),
            required_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70),
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use rstest::rstest;
    use stackable_operator::{
        commons::affinity::StackableAffinity,
        k8s_openapi::{
            api::core::v1::{
                PodAffinity, PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use crate::{
        crd::HdfsNodeRole,
        test_support::{anynode_config, deserialize_and_validate_cluster, role_group_name},
    };

    #[rstest]
    #[case(HdfsNodeRole::Journal)]
    #[case(HdfsNodeRole::Name)]
    #[case(HdfsNodeRole::Data)]
    fn test_affinity_defaults(#[case] role: HdfsNodeRole) {
        let input = r#"
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: simple-hdfs
  namespace: test
  uid: 8047b73b-db0f-4281-811f-de59105ae6bf
spec:
  image:
    productVersion: 3.5.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
    authorization:
      opa:
        configMapName: simple-opa
        package: hdfs
  journalNodes:
    roleGroups:
      default:
        replicas: 1
  nameNodes:
    roleGroups:
      default:
        replicas: 1
  dataNodes:
    roleGroups:
      default:
        replicas: 1
        "#;

        let validated_cluster = deserialize_and_validate_cluster(input);
        let merged_config = anynode_config(&validated_cluster, &role, &role_group_name("default"));

        let mut expected_pod_affinities = vec![WeightedPodAffinityTerm {
            pod_affinity_term: PodAffinityTerm {
                label_selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(BTreeMap::from([
                        ("app.kubernetes.io/name".to_string(), "hdfs".to_string()),
                        (
                            "app.kubernetes.io/instance".to_string(),
                            "simple-hdfs".to_string(),
                        ),
                    ])),
                }),
                namespace_selector: None,
                namespaces: None,
                topology_key: "kubernetes.io/hostname".to_string(),
                ..PodAffinityTerm::default()
            },
            weight: 20,
        }];
        // Only the NameNode is configured with the OPA authorizer.
        if role == HdfsNodeRole::Name {
            expected_pod_affinities.push(WeightedPodAffinityTerm {
                pod_affinity_term: PodAffinityTerm {
                    label_selector: Some(LabelSelector {
                        match_expressions: None,
                        match_labels: Some(BTreeMap::from([
                            ("app.kubernetes.io/name".to_string(), "opa".to_string()),
                            (
                                "app.kubernetes.io/instance".to_string(),
                                "simple-opa".to_string(),
                            ),
                            (
                                "app.kubernetes.io/component".to_string(),
                                "server".to_string(),
                            ),
                        ])),
                    }),
                    namespace_selector: None,
                    namespaces: None,
                    topology_key: "kubernetes.io/hostname".to_string(),
                    ..PodAffinityTerm::default()
                },
                weight: 50,
            });
        }

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: Some(PodAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(
                        expected_pod_affinities
                    ),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: Some(PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_expressions: None,
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "hdfs".to_string(),),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "simple-hdfs".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            role.to_string(),
                                        )
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                                ..PodAffinityTerm::default()
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: None,
                node_selector: None,
            }
        );
    }
}
