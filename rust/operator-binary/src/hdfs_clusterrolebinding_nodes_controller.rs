use serde_json::json;
use stackable_hdfs_crd::{
    constants::{APP_NAME, FIELD_MANAGER_SCOPE},
    HdfsCluster,
};
use stackable_operator::{
    commons::rbac::service_account_name,
    k8s_openapi::api::rbac::v1::{ClusterRoleBinding, Subject},
    kube::{
        api::{Patch, PatchParams},
        core::{DeserializeGuard, PartialObjectMeta},
        runtime::{
            reflector::{ObjectRef, Store},
            watcher,
        },
        Api, Client,
    },
};
use tracing::{error, info};

pub async fn reconcile(
    client: Client,
    store: &Store<PartialObjectMeta<DeserializeGuard<HdfsCluster>>>,
    ev: watcher::Result<watcher::Event<PartialObjectMeta<DeserializeGuard<HdfsCluster>>>>,
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
        .map(|object| object.metadata.clone())
        .map(|meta| Subject {
            kind: "ServiceAccount".to_string(),
            name: service_account_name(APP_NAME),
            namespace: meta.namespace.clone(),
            ..Subject::default()
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
