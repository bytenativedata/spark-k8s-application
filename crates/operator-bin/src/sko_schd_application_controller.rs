use kube::{runtime::controller::Action, ResourceExt};
use std::sync::Arc;

use crate::error::{Error, Result};

use tokio::time::Duration;

use crd::sko_spark_application::ScheduledSparkApplication as SKOScheduledSparkApplication;

use crate::controller::{apply_status, determine_spark_app, ContextData, OwnerType};

pub async fn reconcile(
    resource: Arc<SKOScheduledSparkApplication>,
    context: Arc<ContextData>,
) -> Result<Action> {
    let name = resource.name_any();

    match determine_spark_app::<SKOScheduledSparkApplication>(&resource, &name)? {
        OwnerType::SparkJob | OwnerType::SparkSession => {
            // should not happend here, ignored
        }
        OwnerType::SparkScheduledJob => {
            // TODO: make sure it's from SparkScheduledJob, or a standalone SKO Scheduled Spark Application
            // with label `app.kubernetes.io/role-group`
            apply_status::<
                SKOScheduledSparkApplication,
                crd::spark_application::SparkScheduledJob,
                _,
            >(&resource, &context.client, status_json)
            .await?;
            // get the owner of SKOScheduledSparkApplication, and make sure it's owner type
        }
        OwnerType::ScheduledSparkApplication => {
            // should not happend here, ignored
        }
        OwnerType::Other => {
            // TODO: do something ...
        }
    }

    // Ok(Action::requeue(Duration::from_secs(10)))
    Ok(Action::await_change())
}

pub fn on_error(
    resource: Arc<SKOScheduledSparkApplication>,
    error: &Error,
    _context: Arc<ContextData>,
) -> Action {
    tracing::error!("Reconciliation error:\n{:?}.\n{:?}", error, resource);
    Action::requeue(Duration::from_secs(5))
}

// TODO: apply sko state to spark job/session state
// fill job status with all the sko status struct?
fn status_json(
    resource: &SKOScheduledSparkApplication,
    res_name: &String,
) -> Result<serde_json::Value> {
    let state = resource
        .status
        .as_ref()
        .and_then(|s| s.schedule_state.as_ref());
    let data = match state {
        Some(state) => {    
            serde_json::json!({
            "status": {
                "phase": state.clone()
            }})
        }
        None => {
            tracing::warn!("Failed to resolve the state of spark-on-k8s-operator applicaition [{res_name}]");
            serde_json::json!({
            "status": {
                "phase": "Unknown"
            }})
        }
    };
    Ok(data)
}
