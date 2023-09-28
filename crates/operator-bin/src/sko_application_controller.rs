use crd::spark_application::{SparkJob, SparkSession};
use kube::{runtime::controller::Action, ResourceExt};
use std::sync::Arc;
use tokio::time::Duration;

use crate::controller::*;
use crate::error::{Error, Result};
use crd::sko_spark_application::SparkApplication as SKOSparkApplication;

pub async fn reconcile(
    resource: Arc<SKOSparkApplication>,
    context: Arc<ContextData>,
) -> Result<Action> {
    let name = resource.name_any();

    match determine_spark_app::<SKOSparkApplication>(&resource, &name)? {
        OwnerType::SparkJob => {
            apply_status::<SKOSparkApplication, SparkJob, _>(
                &resource,
                &context.client,
                status_json,
            )
            .await?;
        }
        OwnerType::SparkSession => {
            apply_status::<SKOSparkApplication, SparkSession, _>(
                &resource,
                &context.client,
                status_json,
            )
            .await?;
        }
        OwnerType::SparkScheduledJob => {
            // should not happend here, ignored
        }
        OwnerType::ScheduledSparkApplication => {
            // TODO: if the SparkScheduledJob would to check the runs of each Spark Application shcheduled by itself,
            // we can trace the runs here. or just ignore it
            tracing::info!("SKO Spark Application {} is from a SKO Scheduled Spark Application and would be ignored", name);
        }
        OwnerType::Other => {
            // TODO: do something ...
        }
    }

    // Ok(Action::requeue(Duration::from_secs(10)))
    Ok(Action::await_change())
}

pub fn on_error(
    resource: Arc<SKOSparkApplication>,
    error: &Error,
    _context: Arc<ContextData>,
) -> Action {
    tracing::error!(
        "Reconciliation error:\n{:?}.\n{:?}",
        error,
        resource.name_any()
    );
    Action::requeue(Duration::from_secs(5))
}

// TODO: apply sko state to spark job/session state
// fill job status with all the sko status struct?
fn status_json(resource: &SKOSparkApplication, res_name: &String) -> Result<serde_json::Value> {
    let state = resource
        .status
        .as_ref()
        .and_then(|s| s.app_state.as_ref());
    let data = match state {
        Some(state) => {
            if state.error_message.is_some() {
                tracing::warn!(
                    "SKO Spark Application {} with State [{}]\r\n  Error: {}",
                    res_name,
                    state.state,
                    state.error_message.as_ref().unwrap()
                );
            } else {
                tracing::debug!(
                    "SKO Spark Application {} with State [{}]",
                    res_name,
                    state.state,
                );
            };
        
            serde_json::json!({
            "status": {
                "phase": state.state.clone()
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
