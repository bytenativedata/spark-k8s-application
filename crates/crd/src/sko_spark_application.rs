#[allow(unused_imports, unused_import_braces)]
use std::collections::HashMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use crate::spark_application::{
    ImagePullPolicy,
    Dependencies,
    RestartPolicy,
    MonitoringSpec,
    BatchSchedulerConfiguration,
    SparkUIConfiguration,
    DynamicAllocation
};

use crate::spark_application::{ DriverSpec, ExecutorSpec };

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "sparkoperator.k8s.io",
    version = "v1beta2",
    kind = "SparkApplication",
    shortname = "sparkapp",
    status = "SparkApplicationStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SKOSparkApplicationSpec {
    #[serde(default, rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub spark_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_user: Option<String>,
	
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<ImagePullPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_secrets: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_application_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python_verison: Option<String>,
	// Arguments is a list of arguments to be passed to the application.
	// +optional
	pub arguments: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hadoop_conf: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_config_map: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hadoop_config_map: Option<String>,

    // Volumes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<k8s_openapi::api::core::v1::Volume>>,
    // Driver DriverSpec
    pub driver: DriverSpec,
    // Executor ExecutorSpec    
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ExecutorSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<Dependencies>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<RestartPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_retries: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_interval: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_overhead_factor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitoring: Option<MonitoringSpec>,
	/// BatchScheduler configures which batch scheduler will be used for scheduling
	/// +optional    
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_scheduler: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_scheduler_options: Option<BatchSchedulerConfiguration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_live_seconds: Option<i64>,
    #[serde(default, rename = "sparkUIOptions", skip_serializing_if = "Option::is_none")]
    pub spark_uioptions: Option<SparkUIConfiguration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_allocation: Option<DynamicAllocation>,
}

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "sparkoperator.k8s.io",
    version = "v1beta2",
    kind = "ScheduledSparkApplication",
    shortname = "scheduledsparkapp",
    status = "ScheduledSparkApplicationStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SKOScheduledSparkApplicationSpec {
	// Schedule is a cron schedule on which the application should run.
	pub schedule: String,
	// Template is a template from which SparkApplication instances can be created.
	pub template: SKOSparkApplicationSpec,
	// Suspend is a flag telling the controller to suspend subsequent runs of the application if set to true.
	// +optional
	// Defaults to false.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub suspend: Option<bool>,
	// ConcurrencyPolicy is the policy governing concurrent SparkApplication runs.
    // values: Allow, Forbid, Replace
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency_policy: Option<String>,
	// SuccessfulRunHistoryLimit is the number of past successful runs of the application to keep.
	// +optional
	// Defaults to 1.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub successful_run_history_limit: Option<i32>,
	// FailedRunHistoryLimit is the number of past failed runs of the application to keep.
	// +optional
	// Defaults to 1.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub failed_run_history_limit: Option<i32>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
	// SparkApplicationID is set by the spark-distribution(via spark.app.id config) on the driver and executor pods
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_application_id: Option<String>,
	// SubmissionID is a unique ID of the current submission of the application.
    #[serde(default, rename="submissionID", skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
	// LastSubmissionAttemptTime is the time for the last application submission attempt.
	// +nullable
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_submission_attempt_time: Option<String>,
	// CompletionTime is the time when the application runs to completion if it does.
	// +nullable
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_time: Option<String>,
	// DriverInfo has information about the driver.
    #[serde(default)]
    pub driver_info: DriverInfo,
	// AppState tells the overall application state.
    #[serde(default, rename="applicationState", skip_serializing_if = "Option::is_none")]
    pub app_state: Option<ApplicationState>,
	// ExecutorState records the state of executors by executor Pod names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_state: Option<HashMap<String, String>>,
	// ExecutionAttempts is the total number of attempts to run a submitted application to completion.
	// Incremented upon each attempted run of the application and reset upon invalidation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_attempts: Option<i32>,
	// SubmissionAttempts is the total number of attempts to submit an application to run.
	// Incremented upon each attempted submission of the application and reset upon invalidation and rerun.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_attempts: Option<i32>,
}


#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationState {
	pub state: String,
	pub error_message: Option<String>
}


#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
// DriverInfo captures information about the driver.
pub struct DriverInfo {
    #[serde(default, rename = "webUIServiceName", skip_serializing_if = "Option::is_none")]
	pub web_uiservice_name: Option<String>,
	// UI Details for the UI created via ClusterIP service accessible from within the cluster.
    #[serde(default, rename = "webUIPort", skip_serializing_if = "Option::is_none")]
	pub web_uiport: Option<i32>,
    #[serde(default, rename = "webUIAddress", skip_serializing_if = "Option::is_none")]
	pub web_uiaddress: Option<String>,
	// Ingress Details if an ingress for the UI was created.
    #[serde(default, rename = "webUIIngressName", skip_serializing_if = "Option::is_none")]
	pub web_uiingress_name: Option<String>,
    #[serde(default, rename = "webUIIngressAddress", skip_serializing_if = "Option::is_none")]
	pub web_uiingress_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
	pub pod_name: Option<String>,
}


#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledSparkApplicationStatus {
	// LastRun is the time when the last run of the application started.
	// +nullable
    #[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_run: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
	// NextRun is the time when the next run of the application will start.
	// +nullable
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub next_run: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
	// LastRunName is the name of the SparkApplication for the most recent run of the application.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_run_name: Option<String>,
	// PastSuccessfulRunNames keeps the names of SparkApplications for past successful runs.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub past_successful_run_names: Option<Vec<String>>, 
	// PastFailedRunNames keeps the names of SparkApplications for past failed runs.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub past_failed_run_names:  Option<Vec<String>>, 
	// ScheduleState is the current scheduling state of the application.
    // values: FailedValidation, Scheduled
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schedule_state: Option<String>,
	// Reason tells why the ScheduledSparkApplication is in the particular ScheduleState.
	reason: Option<String>,
}