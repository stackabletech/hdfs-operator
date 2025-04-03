use serde_json::json;
use stackable_operator::{
    commons::rbac::build_rbac_resources,
    k8s_openapi::api::rbac::v1::{ClusterRoleBinding, Subject},
    kube::{
        Api, Client, ResourceExt,
        api::{Patch, PatchParams},
        core::PartialObjectMeta,
        runtime::{
            reflector::{ObjectRef, Store},
            watcher,
        },
    },
    kvp::Labels,
};
use tracing::{error, info};

use crate::crd::{
    constants::{APP_NAME, FIELD_MANAGER_SCOPE},
    v1alpha1,
};

pub async fn reconcile(
    client: Client,
    store: &Store<PartialObjectMeta<v1alpha1::HdfsCluster>>,
    ev: watcher::Result<watcher::Event<PartialObjectMeta<v1alpha1::HdfsCluster>>>,
) {
    match ev {
        Ok(watcher::Event::Apply(o)) => {
            info!(object = %ObjectRef::from_obj(&o), "saw updated object")
        }
        Ok(watcher::Event::Delete(o)) => {
            info!(object = %ObjectRef::from_obj(&o), "saw deleted object")
        }
        Ok(watcher::Event::InitApply(o)) => {
            info!(object = %ObjectRef::from_obj(&o), "restarted reflector")
        }
        Ok(watcher::Event::Init) | Ok(watcher::Event::InitDone) => {}
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to update reflector"
            )
        }
    }

    // Build a list of SubjectRef objects for all deployed HdfsClusters.
    // To do this we only need the metadata for that, as we only really
    // need name and namespace of the objects
    let subjects: Vec<Subject> = store
        .state()
        .into_iter()
        .filter_map(|object| {
            // The call to 'build_rbac_resources' can fail, so we
            // use filter_map here, log an error for any failures and keep
            // going with all the non-broken elements
            // Usually we'd rather opt for failing completely here, but in this specific instance
            // this could mean that one broken cluster somewhere could impact other working clusters
            // within the namespace, so we opted for doing everything we can here, instead of failing
            // completely.
            match build_rbac_resources(&*object, APP_NAME, Labels::default()) {
                Ok((service_account, _role_binding)) => {
                    Some((object.metadata.clone(), service_account.name_any()))
                }
                Err(e) => {
                    error!(
                        ?object,
                        error = &e as &dyn std::error::Error,
                        "Failed to build serviceAccount name for hdfs cluster"
                    );
                    None
                }
            }
        })
        .flat_map(|(meta, sa_name)| {
            let mut result = vec![
                Subject {
                    kind: "ServiceAccount".to_string(),
                    name: sa_name,
                    namespace: meta.namespace.clone(),
                    ..Subject::default()
                },
                // This extra Serviceaccount is being written for legacy/compatibility purposes
                // to ensure that running clusters don't lose access to anything during an upgrade
                // of the Stackable operators, this code can be removed in later releases
                // The value is hardcoded here, as we have removed access to the private fns that
                // would have built it, since this is a known target though, and will be removed soon
                // this should not be an issue.
                Subject {
                    kind: "ServiceAccount".to_string(),
                    name: "hdfs-serviceaccount".to_string(),
                    namespace: meta.namespace.clone(),
                    ..Subject::default()
                },
            ];
            // If a cluster is called hdfs this would result in the same subject
            // being written twicex.
            // Since we know this vec only contains two elements we can use dedup for
            // simply removing this duplicate.
            result.dedup();
            result
        })
        .collect();

    let patch = Patch::Apply(json!({
        "apiVersion": "rbac.authorization.k8s.io/v1".to_string(),
        "kind": "ClusterRoleBinding".to_string(),
        "metadata": {
            "name": "hdfs-clusterrolebinding-nodes".to_string()
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io".to_string(),
            "kind": "ClusterRole".to_string(),
            "name": "hdfs-clusterrole-nodes".to_string()
        },
        "subjects": subjects
    }));

    let api: Api<ClusterRoleBinding> = Api::all(client);
    let params = PatchParams::apply(FIELD_MANAGER_SCOPE);
    match api
        .patch("hdfs-clusterrolebinding-nodes", &params, &patch)
        .await
    {
        Ok(_) => info!(
            "ClusterRoleBinding has been successfully patched: {:?}",
            &patch
        ),
        Err(e) => error!("{}", e),
    }
}
