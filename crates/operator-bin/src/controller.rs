use kube::{client::Client, Api};
use kube::api::PatchParams;
use kube::Resource;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::{ Result, Error };

/// Action to be taken upon an specific resource during reconciliation
pub(crate) enum SparkApplicationAction {
    /// Create the subresources, this includes spawning `n` pods with service
    Create,
    /// Delete all subresources created in the `Create` phase
    Delete,
    /// This resource is in desired state and requires no actions to be taken
    NoOp,
}

/// Context injected with each `reconcile` and `on_error` method invocation.
pub struct ContextData {
    /// Kubernetes client to make Kubernetes API requests with. Required for K8S resource management.
    pub client: Client,
}

impl ContextData {
    /// Constructs a new instance of ContextData.
    ///
    /// # Arguments:
    /// - `client`: A Kubernetes client to make Kubernetes REST API requests with. Resources
    /// will be created and deleted with this client.
    pub fn new(client: Client) -> Self {
        ContextData { client }
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum OwnerType {
    SparkJob,
    SparkSession,
    SparkScheduledJob,
    ScheduledSparkApplication,
    Other
}

impl From<&String> for OwnerType {
    fn from(string: &String) -> Self {
        match string.as_str() {
            "SparkJob" => OwnerType::SparkJob,
            "SparkSession" => OwnerType::SparkSession,
            // TODO: make sure it is not a standalone ScheduledSparkApplication
            "SparkScheduledJob" => OwnerType::SparkScheduledJob,
            "ScheduledSparkApplication" => OwnerType::ScheduledSparkApplication,
            _ => OwnerType::Other,
        }
    }
}

pub(crate) fn determine_spark_app<T>(resource: &T, name: &String) -> Result<OwnerType>
where T: Resource
{
    let owner_refs = resource.meta()
        .owner_references
        .clone()
        .ok_or(Error::FailedResolveOwnerReferences { name: name.clone() })?;

    let owner_types: Vec<OwnerType> = owner_refs.iter()
        .map(|r| {
            OwnerType::from(&r.kind)
    }).collect();

    let mut owner_type = OwnerType::Other;
    // check owner type, and make sure only owner_type is assigned once
    if owner_type == OwnerType::Other && owner_types.iter().filter(|t| *t == &OwnerType::SparkJob).count() > 0 {
        owner_type = OwnerType::SparkJob;
    } 
    if owner_type == OwnerType::Other && owner_types.iter().filter(|t| *t == &OwnerType::SparkSession).count() > 0 {
        assert_eq!(owner_type, OwnerType::Other);
        owner_type = OwnerType::SparkSession;
    }
    if owner_type == OwnerType::Other && owner_types.iter().filter(|t| *t == &OwnerType::SparkScheduledJob).count() > 0 {
        assert_eq!(owner_type, OwnerType::Other);
        owner_type = OwnerType::SparkScheduledJob;
    }

    tracing::debug!("determine_spark_app - sko name: {}, owner type: {:?}", name, owner_type);
    Ok(owner_type)
}


pub(crate) async fn apply_status<T, K, F>(
    resource: &T,
    client: &Client,
    status_fn: F
) -> Result<()>
where
    T: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    K: Clone,
    K: DeserializeOwned,
    K: for<'a> Deserialize<'a>,
    K: std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
    F: FnOnce(&T, &String) -> Result<serde_json::Value>
{
    let res_name = resource
        .meta()
        .name
        .as_ref()
        .ok_or(Error::ResourceNameNotExists)?;

    let job_name = resource
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get(&String::from("app.kubernetes.io/instance")))
        .ok_or(Error::FailedResolveInstance { name: res_name.clone() })?;

    let job_api = Api::<K>::namespaced(
        client.clone(),
        resource
            .meta()
            .namespace
            .as_ref()
            .ok_or(Error::ResourceNamespaceNotExists { name: res_name.clone() })?,
    );

    let _job = job_api.get(job_name).await.map_err(|_| {
        Error::SparkJobOrSessionNotExists{ name: res_name.clone() }
    })?;

    let data = status_fn(resource, &res_name)?;
    tracing::info!("Update spark job [{job_name}] status to [{:?}]", data);
    job_api
        .patch_status(
            &job_name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(data),
        )
        .await.map_err(|_| {
            Error::FailedPatchResource { name: res_name.clone() }
        })?;

    Ok(())
}