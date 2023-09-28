use k8s_openapi::api::core::v1::Service;
use kube::api::{DeleteParams, PostParams};
use kube::api::{Patch, PatchParams};
use kube::{client::Client, runtime::controller::Action, Api};
use kube::{Resource, ResourceExt};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::error::{Error, Result};
use tokio::time::Duration;

use crate::controller::{ContextData, SparkApplicationAction};
use crd::spark_application::SparkSession;

pub async fn reconcile(resource: Arc<SparkSession>, context: Arc<ContextData>) -> Result<Action> {
    let client: Client = context.client.clone();
    let name = resource.name_any(); // Name of the resource is used to name the subresources as well.
    let namespace: String = match resource.namespace() {
        None => {
            // If there is no namespace to deploy to defined, reconciliation ends with an error immediately.
            return Err(Error::ResourceNamespaceNotExists { name: name });
        }
        Some(namespace) => namespace,
    };

    return match determine_action(&resource) {
        SparkApplicationAction::Create => {
            add_finalizer(client.clone(), &name, &namespace).await?;
            deploy(client, &name, &namespace, &resource).await?;
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        SparkApplicationAction::Delete => {
            delete(client.clone(), &name, &namespace).await?;
            delete_finalizers(client, &name, &namespace).await?;
            Ok(Action::await_change()) // Makes no sense to delete after a successful delete, as the resource is gone
        }
        // The resource is already in desired state, do nothing and re-check after 10 seconds
        // SparkApplicationAction::NoOp => Ok(Action::requeue(Duration::from_secs(10))),
        SparkApplicationAction::NoOp => Ok(Action::await_change()),
    };
}

fn determine_action(resource: &SparkSession) -> SparkApplicationAction {
    return if resource.meta().deletion_timestamp.is_some() {
        SparkApplicationAction::Delete
    } else if resource
        .meta()
        .finalizers
        .as_ref()
        .map_or(true, |finalizers| finalizers.is_empty())
    {
        SparkApplicationAction::Create
    } else {
        SparkApplicationAction::NoOp
    };
}

pub fn on_error(resource: Arc<SparkSession>, error: &Error, _context: Arc<ContextData>) -> Action {
    tracing::error!("Reconciliation error:\n{:?}.\n{:?}", error, resource);
    Action::requeue(Duration::from_secs(5))
}

// -------------------

/// Creates a new deployment of `n` pods with the `inanimate/echo-server:latest` docker image inside,
/// where `n` is the number of `replicas` given.
///
/// # Arguments
/// - `client` - A Kubernetes client to create the deployment with.
/// - `name` - Name of the deployment to be created
/// - `replicas` - Number of pod replicas for the Deployment to contain
/// - `namespace` - Namespace to create the Kubernetes Deployment in.
///
/// Note: It is assumed the resource does not already exists for simplicity. Returns an `Error` if it does.
pub async fn deploy(
    client: Client,
    name: &str,
    namespace: &str,
    resource: &SparkSession,
) -> Result<crd::sko_spark_application::SparkApplication> {
    let appl = resource
        .sko_application(&client, namespace)
        .await
        .map_err(|e| Error::FailedBuildSKOApplication {
            name: name.to_string(),
            source: e
        })?;

    // Create the deployment defined above
    let deployment_api: Api<crd::sko_spark_application::SparkApplication> =
        Api::namespaced(client.clone(), namespace);
    let ret = deployment_api
        .create(&PostParams::default(), &appl)
        .await
        .map_err(|e| {            
            tracing::error!("Failed to create SKO resource: {:?}", e);
            Error::FailedDeploySKOResource { name: name.to_string(),}
        })?;

    // create the services
    let svc_api: Api<Service> = Api::namespaced(client, namespace);
    let svc = resource
        .hive_server2_thrift_service()
        .map_err(|_| Error::FailedResolveHS2ThriftService {
            name: name.to_string(),
        })?;
    let _hs2_thrift_svc = svc_api
        .create(
            &PostParams::default(),
            &svc,
        )
        .await.map_err(|e| {
            tracing::error!("Failed to create HS2 thrift service: {:?}", e);
            Error::FailedCreateService { name: svc.name_any() }
        })?;
    let svc = resource
        .hive_server2_ui_service()
        .map_err(|_| Error::FailedResolveHS2UIService {
            name: name.to_string(),
        })?;

    let _hs2_ui_svc = svc_api
        .create(
            &PostParams::default(),
            &svc,
        )
        .await.map_err(|e| {
            tracing::error!("Failed to create HS2 UI service: {:?}", e);
            Error::FailedCreateService { name: svc.name_any() }
        })?;
    Ok(ret)
}

/// Deletes an existing SparkApplication.
///
/// # Arguments:
/// - `client` - A Kubernetes client to delete the SparkApplication with
/// - `name` - Name of the SparkApplication to delete
/// - `namespace` - Namespace the existing SparkApplication resides in
///
/// Note: It is assumed the deployment exists for simplicity. Otherwise returns an Error.
pub async fn delete(client: Client, name: &str, namespace: &str) -> Result<(), Error> {
    let api: Api<crd::spark_application::SparkSession> = Api::namespaced(client, namespace);
    api.delete(name, &DeleteParams::default()).await.map_err(|_| {
        Error::FailedDeleteSKOResource { name: name.to_string() }
    })?;
    Ok(())
}

// ---------------------
// finalizers
// TODO: move functions out of this module
//  and put them in a separate module with generic functions

/// Adds a finalizer record into an SparkSession kind of resource. If the finalizer already exists,
/// this action has no effect.
///
/// # Arguments:
/// - `client` - Kubernetes client to modify the resource with.
/// - `name` - Name of the resource to modify. Existence is not verified
/// - `namespace` - Namespace where the resource with given `name` resides.
///
/// Note: Does not check for resource's existence for simplicity.
pub async fn add_finalizer(client: Client, name: &str, namespace: &str) -> Result<SparkSession> {
    let api: Api<SparkSession> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["spark.bytenative.com/finalizer"]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch)
        .await
        .map_err(|_| Error::FailedPatchResource {
            name: name.to_string(),
        })
}

/// Removes all finalizers from an `SparkApplication` resource. If there are no finalizers already, this
/// action has no effect.
///
/// # Arguments:
/// - `client` - Kubernetes client to modify the `SparkApplication` resource with.
/// - `name` - Name of the `SparkApplication` resource to modify. Existence is not verified
/// - `namespace` - Namespace where the `SparkApplication` resource with given `name` resides.
///
/// Note: Does not check for resource's existence for simplicity.
pub async fn delete_finalizers(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<SparkSession> {
    let api: Api<SparkSession> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    api.patch(name, &PatchParams::default(), &patch)
        .await
        .map_err(|_| Error::FailedPatchResource {
            name: name.to_string(),
        })
}
