use stackable_operator::{
    commons::affinity::{
        affinity_between_cluster_pods, affinity_between_role_pods, StackableAffinityFragment,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::{HdfsRole, APP_NAME};

pub fn get_affinity(cluster_name: &str, role: &HdfsRole) -> StackableAffinityFragment {
    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_cluster_pods(APP_NAME, cluster_name, 20),
            ]),
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
    use rstest::rstest;
    use std::collections::BTreeMap;

    use crate::{HdfsCluster, HdfsRole};
    use stackable_operator::{
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, PodAffinity,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    #[rstest]
    #[case(HdfsRole::JournalNode)]
    #[case(HdfsRole::NameNode)]
    #[case(HdfsRole::DataNode)]
    fn test_affinity_defaults(#[case] role: HdfsRole) {
        let input = r#"
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: simple-hdfs
spec:
  image:
    productVersion: 3.3.4
    stackableVersion: 0.2.0
  clusterConfig:
    zookeeperConfigMapName: hdfs-zk
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
        let hdfs: HdfsCluster = serde_yaml::from_str(input).unwrap();
        let merged_config = role.merged_config(&hdfs, "default").unwrap();

        assert_eq!(
            merged_config.affinity(),
            &StackableAffinity {
                pod_affinity: Some(PodAffinity {
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
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 20
                        }
                    ]),
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

    #[test]
    fn test_affinity_legacy_node_selector() {
        let input = r#"
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: simple-hdfs
spec:
  image:
    productVersion: 3.3.4
    stackableVersion: 0.2.0
  clusterConfig:    
    zookeeperConfigMapName: hdfs-zk
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
        selector:
          matchLabels:
            disktype: ssd
          matchExpressions:
            - key: topology.kubernetes.io/zone
              operator: In
              values:
                - antarctica-east1
                - antarctica-west1
        "#;
        let hdfs: HdfsCluster = serde_yaml::from_str(input).unwrap();
        let merged_config = HdfsRole::DataNode.merged_config(&hdfs, "default").unwrap();

        assert_eq!(
            merged_config.affinity(),
            &StackableAffinity {
                pod_affinity: Some(PodAffinity {
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
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 20
                        }
                    ]),
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
                                            "datanode".to_string(),
                                        )
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: Some(NodeAffinity {
                    preferred_during_scheduling_ignored_during_execution: None,
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: "topology.kubernetes.io/zone".to_string(),
                                operator: "In".to_string(),
                                values: Some(vec![
                                    "antarctica-east1".to_string(),
                                    "antarctica-west1".to_string()
                                ]),
                            }]),
                            match_fields: None,
                        }]
                    }),
                }),
                node_selector: Some(StackableNodeSelector {
                    node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())])
                }),
            }
        );
    }
}
