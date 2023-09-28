use std::collections::BTreeMap;
#[allow(unused_imports)]
use std::collections::HashMap;
use std::ops::Deref;

use k8s_openapi::api::core::v1::{ConfigMap, Service, EnvVar, EnvFromSource};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::core::ObjectMeta;
use kube::{Client, CustomResource, ResourceExt, Api};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_valid::Validate;
use crate::metadata::ObjectMetaBuilder;
use crate::metadata::ObjectLabels;
use strum::{Display, EnumString};

use crate::s3::{S3ConnectionDef, S3ConnectionSpec};
use crate::sko_spark_application::{
    SKOScheduledSparkApplicationSpec, SKOSparkApplicationSpec,
    ScheduledSparkApplication as SKOScheduledSparkApplication,
    SparkApplication as SKOSparkApplication,
};
use crate::{Error, Result};
use crate::{
    SparkApplicationStatus, SparkCatalogDef, SparkCatalogSpec, SparkEnvSetDef, SparkEnvSetSpec,
};

#[derive(Clone, CustomResource, Default, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkTemplate",
    shortname = "sct",
    plural = "sparktemplates",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkSpec {
    #[serde(default)]
    pub spark_version: String,

    // Mode is the deployment mode of the Spark application.
    // +kubebuilder:validation:Enum={cluster,client}
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_user: Option<String>,

    // Image is the container image for the driver, executor, and init-container. Any custom container images for the
    // driver, executor, or init-container takes precedence over this.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<ImagePullPolicy>,
    // ImagePullSecrets is the list of image-pull secrets.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_secrets: Option<Vec<String>>,

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
    #[serde(
        default,
        rename = "sparkUIOptions",
        skip_serializing_if = "Option::is_none"
    )]
    pub spark_uioptions: Option<SparkUIConfiguration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_allocation: Option<DynamicAllocation>,

    // ----------------------------------------------------------------------
    // extra to make it work
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_connection: Option<crate::s3::S3ConnectionDef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalogs: Option<Vec<crate::SparkCatalogDef>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_sets: Option<Vec<crate::SparkEnvSetDef>>,
}

impl SparkSpec {
    pub async fn merge_template_opt(&self, client: &Client, namespace: &str, template: Option<&String>) -> Result<Self> {
        if let Some(template) = template {
            let st_api = Api::<SparkTemplate>::namespaced(client.clone(), namespace);
            let st = st_api.get(template)
                .await.map_err(|_| {
                    Error::MissingSparkTemplate { name: template.to_string() }
                })?;
            let mut st_value = serde_json::to_value::<SparkSpec>(st.spec)
                .map_err(|e| {
                    Error::FailedSerializeObjectToJson { internal: e }
                })?;
            let self_value = serde_json::to_value::<SparkSpec>(self.clone())
            .map_err(|e| {
                Error::FailedDeserializeObjectFromJson { internal: e }
            })?;
            json_patch::merge(&mut st_value, &self_value);
            Ok(serde_json::from_value::<SparkSpec>(st_value)
            .map_err(|e| {
                Error::FailedMergeObjects { internal: e }
            })?)
        } else {
            Ok(self.clone())
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverSpec {
    // NOTE: ignored SparkPodSpec items from SKO, and use PodTemplateSpec instead
    // and would build SparkPodSpec from PodOverrides
    // TODO:
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // pub pod_overrides: Option<k8s_openapi::api::core::v1::PodTemplateSpec>,
    // PodName is the name of the driver pod that the user creates. This is used for the
    // in-cluster client mode in which the user creates a client pod where the driver of
    // the user application runs. It's an error to set this field if Mode is not
    // in-cluster-client.
    // +optional
    // +kubebuilder:validation:Pattern=[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*
    pub pod_name: Option<String>, // *string `json:"podName,omitempty"`
    // CoreRequest is the physical CPU core request for the driver.
    // Maps to `spark.kubernetes.driver.request.cores` that is available since Spark 3.0.
    // +optional
    pub core_request: Option<String>, // *string `json:"coreRequest,omitempty"`
    // JavaOptions is a string of extra JVM options to pass to the driver. For instance,
    // GC settings or other logging.
    // +optional
    pub java_options: Option<String>, // *string `json:"javaOptions,omitempty"`
    // Lifecycle for running preStop or postStart commands
    // +optional
    pub lifecycle: Option<k8s_openapi::api::core::v1::Lifecycle>, // *apiv1.Lifecycle `json:"lifecycle,omitempty"`
    // KubernetesMaster is the URL of the Kubernetes master used by the driver to manage executor pods and
    // other Kubernetes resources. Default to https://kubernetes.default.svc.
    // +optional
    pub kubernetes_master: Option<String>, // *string `json:"kubernetesMaster,omitempty"`
    // ServiceAnnotations defines the annotations to be added to the Kubernetes headless service used by
    // executors to connect to the driver.
    // +optional
    pub service_annotations: Option<HashMap<String, String>>, // map[string]string `json:"serviceAnnotations,omitempty"`
    // Ports settings for the pods, following the Kubernetes specifications.
    // +optional
    pub ports: Option<Vec<Port>>,

    // COPY THESE TO ExecutorSpec
    // NOTE: this is from GO version of SparkPodSpec inline, should be sync with ExecutorSpec

    // ServiceAccount is the name of the custom Kubernetes service account used by the pod.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,

    /// Cores maps to `spark.driver.cores` or `spark.executor.cores` for the driver and executors, respectively.
    /// +optional
    /// +kubebuilder:validation:Minimum=1
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<i32>,
    /// CoreLimit specifies a hard limit on CPU cores for the pod.
    /// Optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_limit: Option<String>,
    /// Memory is the amount of memory to request for the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    /// MemoryOverhead is the amount of off-heap memory to allocate in cluster mode, in MiB unless otherwise specified.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_overhead: Option<String>,
    /// GPU specifies GPU requirement for the pod.
    /// +optional
    // #[serde(default, rename = "gpu", skip_serializing_if = "Option::is_none")]
    // GPU *GPUSpec `json:"gpu,omitempty"`
    /// Image is the container image to use. Overrides Spec.Image if set.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Image *string `json:"image,omitempty"`
    /// ConfigMaps carries information of other ConfigMaps to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config_maps: Option<Vec<NamePath>>,
    /// Secrets carries information of secrets to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    secrets: Option<Vec<SecretInfo>>,
    /// Env carries the environment variables to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    env: Option<Vec<EnvVar>>,

    /// EnvVars carries the environment variables to add to the pod.
    /// Deprecated. Consider using `env` instead.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // EnvVars map[string]string `json:"envVars,omitempty"`

    /// EnvFrom is a list of sources to populate environment variables in the container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    env_from: Option<Vec<EnvFromSource>>,

    /// EnvSecretKeyRefs holds a mapping from environment variable names to SecretKeyRefs.
    /// Deprecated. Consider using `env` instead.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // EnvSecretKeyRefs map[string]NameKey `json:"envSecretKeyRefs,omitempty"`

    /// Labels are the Kubernetes labels to be added to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,
    /// Annotations are the Kubernetes annotations to be added to the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Annotations map[string]string `json:"annotations,omitempty"`
    /// VolumeMounts specifies the volumes listed in ".spec.volumes" to mount into the main container's filesystem.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<k8s_openapi::api::core::v1::VolumeMount>>,
    /// Affinity specifies the affinity/anti-affinity settings for the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Affinity *apiv1.Affinity `json:"affinity,omitempty"`
    /// Tolerations specifies the tolerations listed in ".spec.tolerations" to be applied to the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Tolerations []apiv1.Toleration `json:"tolerations,omitempty"`
    /// PodSecurityContext specifies the PodSecurityContext to apply.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // PodSecurityContext *apiv1.PodSecurityContext `json:"podSecurityContext,omitempty"`
    /// SecurityContext specifies the container's SecurityContext to apply.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // SecurityContext *apiv1.SecurityContext `json:"securityContext,omitempty"`
    /// SchedulerName specifies the scheduler that will be used for scheduling
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // SchedulerName *string `json:"schedulerName,omitempty"`
    /// Sidecars is a list of sidecar containers that run along side the main Spark container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecars: Option<Vec<k8s_openapi::api::core::v1::Container>>,
    /// InitContainers is a list of init-containers that run to completion before the main Spark container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_containers: Option<Vec<k8s_openapi::api::core::v1::Container>>,
    /// HostNetwork indicates whether to request host networking for the pod or not.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_network: Option<bool>,
    /// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
    /// This field is mutually exclusive with nodeSelector at SparkApplication level (which will be deprecated).
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,
    /// DnsConfig dns settings for the pod, following the Kubernetes specifications.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // DNSConfig *apiv1.PodDNSConfig `json:"dnsConfig,omitempty"`
    /// Termination grace period seconds for the pod
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
    /// HostAliases settings for the pod, following the Kubernetes specifications.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // HostAliases []apiv1.HostAlias `json:"hostAliases,omitempty"`
    /// ShareProcessNamespace settings for the pod, following the Kubernetes specifications.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_process_namespace: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSpec {
    // NOTE: ignored SparkPodSpec items from SKO, and use PodTemplateSpec instead
    // and would build SparkPodSpec from PodOverrides
    // TODO:
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // pub pod_overrides: Option<k8s_openapi::api::core::v1::PodTemplateSpec>,
    // Instances is the number of executor instances.
    // +optional
    // +kubebuilder:validation:Minimum=1
    pub instances: Option<i32>,
    // CoreRequest is the physical CPU core request for the executors.
    // Maps to `spark.kubernetes.executor.request.cores` that is available since Spark 2.4.
    // +optional
    pub core_request: Option<String>,
    // JavaOptions is a string of extra JVM options to pass to the executors. For instance,
    // GC settings or other logging.
    // +optional
    pub java_options: Option<String>,
    // DeleteOnTermination specify whether executor pods should be deleted in case of failure or normal termination.
    // Maps to `spark.kubernetes.executor.deleteOnTermination` that is available since Spark 3.0.
    // +optional
    pub delete_on_termination: Option<bool>,
    // Ports settings for the pods, following the Kubernetes specifications.
    // +optional
    pub ports: Option<Vec<Port>>,

    // COPIED FROM DriverSpec
    // NOTE: this is from GO version of SparkPodSpec inline, should be sync with ExecutorSpec

    // ServiceAccount is the name of the custom Kubernetes service account used by the pod.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,

    /// Cores maps to `spark.driver.cores` or `spark.executor.cores` for the driver and executors, respectively.
    /// +optional
    /// +kubebuilder:validation:Minimum=1
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<i32>,
    /// CoreLimit specifies a hard limit on CPU cores for the pod.
    /// Optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_limit: Option<String>,
    /// Memory is the amount of memory to request for the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    /// MemoryOverhead is the amount of off-heap memory to allocate in cluster mode, in MiB unless otherwise specified.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_overhead: Option<String>,
    /// GPU specifies GPU requirement for the pod.
    /// +optional
    // #[serde(default, rename = "gpu", skip_serializing_if = "Option::is_none")]
    // GPU *GPUSpec `json:"gpu,omitempty"`
    /// Image is the container image to use. Overrides Spec.Image if set.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Image *string `json:"image,omitempty"`
    /// ConfigMaps carries information of other ConfigMaps to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config_maps: Option<Vec<NamePath>>,
    /// Secrets carries information of secrets to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    secrets: Option<Vec<SecretInfo>>,
    /// Env carries the environment variables to add to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    env: Option<Vec<EnvVar>>,
    /// EnvVars carries the environment variables to add to the pod.
    /// Deprecated. Consider using `env` instead.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // EnvVars map[string]string `json:"envVars,omitempty"`

    /// EnvFrom is a list of sources to populate environment variables in the container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    env_from: Option<Vec<EnvFromSource>>,

    /// EnvSecretKeyRefs holds a mapping from environment variable names to SecretKeyRefs.
    /// Deprecated. Consider using `env` instead.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // EnvSecretKeyRefs map[string]NameKey `json:"envSecretKeyRefs,omitempty"`
    /// Labels are the Kubernetes labels to be added to the pod.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,
    /// Annotations are the Kubernetes annotations to be added to the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Annotations map[string]string `json:"annotations,omitempty"`
    /// VolumeMounts specifies the volumes listed in ".spec.volumes" to mount into the main container's filesystem.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<k8s_openapi::api::core::v1::VolumeMount>>,
    /// Affinity specifies the affinity/anti-affinity settings for the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Affinity *apiv1.Affinity `json:"affinity,omitempty"`
    /// Tolerations specifies the tolerations listed in ".spec.tolerations" to be applied to the pod.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // Tolerations []apiv1.Toleration `json:"tolerations,omitempty"`
    /// PodSecurityContext specifies the PodSecurityContext to apply.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // PodSecurityContext *apiv1.PodSecurityContext `json:"podSecurityContext,omitempty"`
    /// SecurityContext specifies the container's SecurityContext to apply.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // SecurityContext *apiv1.SecurityContext `json:"securityContext,omitempty"`
    /// SchedulerName specifies the scheduler that will be used for scheduling
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // SchedulerName *string `json:"schedulerName,omitempty"`
    /// Sidecars is a list of sidecar containers that run along side the main Spark container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecars: Option<Vec<k8s_openapi::api::core::v1::Container>>,
    /// InitContainers is a list of init-containers that run to completion before the main Spark container.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_containers: Option<Vec<k8s_openapi::api::core::v1::Container>>,
    /// HostNetwork indicates whether to request host networking for the pod or not.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_network: Option<bool>,
    /// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
    /// This field is mutually exclusive with nodeSelector at SparkApplication level (which will be deprecated).
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,
    /// DnsConfig dns settings for the pod, following the Kubernetes specifications.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // DNSConfig *apiv1.PodDNSConfig `json:"dnsConfig,omitempty"`
    /// Termination grace period seconds for the pod
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
    /// HostAliases settings for the pod, following the Kubernetes specifications.
    /// +optional
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // HostAliases []apiv1.HostAlias `json:"hostAliases,omitempty"`
    /// ShareProcessNamespace settings for the pod, following the Kubernetes specifications.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_process_namespace: Option<bool>,
}

// SecretInfo captures information of a secret.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretInfo {
	pub name: String,
	pub path: String,
    /// allowed values: Generic, GCPServiceAccount, HadoopDelegationToken
	/// GenericType is for secrets that needs no special handling.
	/// HadoopDelegationTokenSecret is for secrets from an Hadoop delegation token that needs the
	/// environment variable HADOOP_TOKEN_FILE_LOCATION.
	/// GCPServiceAccountSecret is for secrets from a GCP service account Json key file that needs
	/// the environment variable GOOGLE_APPLICATION_CREDENTIALS.
    #[serde(rename = "secretType")]
	pub typ: String,
}

// NamePath is a pair of a name and a path to which the named objects should be mounted to.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamePath {
    name: String,
    path: String,
}

// Port represents the port definition in the pods objects.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Port {
    name: String,
    protocol: String,
    container_port: i32,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartPolicy {
    /// Type specifies the RestartPolicyType.
    /// +kubebuilder:validation:Enum={Never,Always,OnFailure}  
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub typ: Option<String>,

    // OnSubmissionFailureRetries is the number of times to retry submitting an application before giving up.
    // This is best effort and actual retry attempts can be >= the value specified due to caching.
    // These are required if RestartPolicy is OnFailure.
    // +kubebuilder:validation:Minimum=0
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_submission_failure_retries: Option<i32>,

    // OnFailureRetries the number of times to retry running an application before giving up.
    // +kubebuilder:validation:Minimum=0
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure_retries: Option<i32>,

    // OnSubmissionFailureRetryInterval is the interval in seconds between retries on failed submissions.
    // +kubebuilder:validation:Minimum=1
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_submission_failure_retry_interval: Option<i64>,

    // OnFailureRetryInterval is the interval in seconds between retries on failed runs.
    // +kubebuilder:validation:Minimum=1
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure_retry_interval: Option<i64>,
}

// TODO: move jars, files, py_files out, and to JobSpec for according job types
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Dependencies {
    // Jars is a list of JAR files the Spark application depends on.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jars: Option<Vec<String>>, // []string `json:"jars,omitempty"`
    // Files is a list of files the Spark application depends on.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<String>>, // []string `json:"files,omitempty"`
    // PyFiles is a list of Python files the Spark application depends on.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub py_files: Option<Vec<String>>, // []string `json:"pyFiles,omitempty"`
    // Packages is a list of maven coordinates of jars to include on the driver and executor
    // classpaths. This will search the local maven repo, then maven central and any additional
    // remote repositories given by the "repositories" option.
    // Each package should be of the form "groupId:artifactId:version".
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packages: Option<Vec<String>>, // []string `json:"packages,omitempty"`
    // ExcludePackages is a list of "groupId:artifactId", to exclude while resolving the
    // dependencies provided in Packages to avoid dependency conflicts.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_packages: Option<Vec<String>>,
    // Repositories is a list of additional remote repositories to search for the maven coordinate
    // given with the "packages" option.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repositories: Option<Vec<String>>,
}

impl Dependencies {
    pub fn extend(&mut self, other: Dependencies) {
        self.jars
            .get_or_insert(Vec::new())
            .extend(other.jars.unwrap_or_default());
        self.files
            .get_or_insert(Vec::new())
            .extend(other.files.unwrap_or_default());
        self.py_files
            .get_or_insert(Vec::new())
            .extend(other.py_files.unwrap_or_default());
        self.packages
            .get_or_insert(Vec::new())
            .extend(other.packages.unwrap_or_default());
        self.exclude_packages
            .get_or_insert(Vec::new())
            .extend(other.exclude_packages.unwrap_or_default());
        self.repositories
            .get_or_insert(Vec::new())
            .extend(other.repositories.unwrap_or_default());
    }

    pub fn extend_jars(&mut self, jars: Vec<String>) {
        self.jars.get_or_insert(Vec::new()).extend(jars);
    }
}

/// MonitoringSpec defines the monitoring specification.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringSpec {
    // ExposeDriverMetrics specifies whether to expose metrics on the driver.
    pub expose_driver_metrics: bool,
    // ExposeExecutorMetrics specifies whether to expose metrics on the executors.
    pub expose_executor_metrics: bool,
    // MetricsProperties is the content of a custom metrics.properties for configuring the Spark metric system.
    // +optional
    // If not specified, the content in spark-docker/conf/metrics.properties will be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_properties: Option<String>,
    // MetricsPropertiesFile is the container local path of file metrics.properties for configuring
    //the Spark metric system. If not specified, value /etc/metrics/conf/metrics.properties will be used.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_properties_file: Option<String>,
    // Prometheus is for configuring the Prometheus JMX exporter.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prometheus: Option<PrometheusSpec>,
}

// PrometheusSpec defines the Prometheus specification when Prometheus is to be used for
// collecting and exposing metrics.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusSpec {
    // JmxExporterJar is the path to the Prometheus JMX exporter jar in the container.
    pub jmx_exporter_jar: String,
    // Port is the port of the HTTP server run by the Prometheus JMX exporter.
    // If not specified, 8090 will be used as the default.
    // +kubebuilder:validation:Minimum=1024
    // +kubebuilder:validation:Maximum=49151
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    // PortName is the port name of prometheus JMX exporter port.
    // If not specified, jmx-exporter will be used as the default.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_name: Option<String>,
    // ConfigFile is the path to the custom Prometheus configuration file provided in the Spark image.
    // ConfigFile takes precedence over Configuration, which is shown below.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_file: Option<String>,
    // Configuration is the content of the Prometheus configuration needed by the Prometheus JMX exporter.
    // If not specified, the content in spark-docker/conf/prometheus.yaml will be used.
    // Configuration has no effect if ConfigFile is set.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchSchedulerConfiguration {
    /// Queue stands for the resource queue which the application belongs to, it's being used in Volcano batch scheduler.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// PriorityClassName stands for the name of k8s PriorityClass resource, it's being used in Volcano batch scheduler.
    /// +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,
    /// Resources stands for the resource list custom request for. Usually it is used to define the lower-bound limit.
    /// If specified, volcano scheduler will consider it as the resources requested.
    /// +optional
    // apiv1.ResourceList `json:"resources,omitempty"`
    // TODO: determine the right type of ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkUIConfiguration {
    // ServicePort allows configuring the port at service level that might be different from the targetPort.
    // TargetPort should be the same as the one defined in spark.ui.port
    pub service_port: i32,
    // ServicePortName allows configuring the name of the service port.
    // This may be useful for sidecar proxies like Envoy injected by Istio which require specific ports names to treat traffic as proper HTTP.
    // Defaults to spark-driver-ui-port.
    pub service_port_name: String,
    // ServiceType allows configuring the type of the service. Defaults to ClusterIP.
    pub service_type: String,
    // ServiceAnnotations is a map of key,value pairs of annotations that might be added to the service object.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_annotations: Option<HashMap<String, String>>,
    // IngressAnnotations is a map of key,value pairs of annotations that might be added to the ingress object. i.e. specify nginx as ingress.class
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_annotations: Option<HashMap<String, String>>,
    // TlsHosts is useful If we need to declare SSL certificates to the ingress object
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_tls: Option<k8s_openapi::api::networking::v1::IngressTLS>,
}

/// Hiveserver2Thrift
/// add to driver.ports
/// add to spark configs
/// deploy a service
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkHiveserver2ServiceConfiguration {
    // ServicePort allows configuring the port at service level that might be different from the targetPort.
    // TargetPort should be the same as the one defined in spark.hiveserver2.port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_port: Option<i32>,
    // ServicePortName allows configuring the name of the service port.
    // This may be useful for sidecar proxies like Envoy injected by Istio which require specific ports names to treat traffic as proper HTTP.
    // Defaults to driver-thrift-port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_port_name: Option<String>,
    // ServiceType allows configuring the type of the service. Defaults to ClusterIP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_type: Option<ServiceType>,
    // ServiceAnnotations is a map of key,value pairs of annotations that might be added to the service object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_annotations: Option<HashMap<String, String>>,
    // IngressAnnotations is a map of key,value pairs of annotations that might be added to the ingress object. i.e. specify nginx as ingress.class
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_annotations: Option<HashMap<String, String>>,
    // TlsHosts is useful If we need to declare SSL certificates to the ingress object
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_tls: Option<k8s_openapi::api::networking::v1::IngressTLS>,
}

// DynamicAllocation contains configuration options for dynamic allocation.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicAllocation {
    // Enabled controls whether dynamic allocation is enabled or not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    // InitialExecutors is the initial number of executors to request. If .spec.executor.instances
    // is also set, the initial number of executors is set to the bigger of that and this option.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_executors: Option<i32>,
    // MinExecutors is the lower bound for the number of executors if dynamic allocation is enabled.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_executors: Option<i32>,
    // MaxExecutors is the upper bound for the number of executors if dynamic allocation is enabled.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_executors: Option<i32>,
    // ShuffleTrackingTimeout controls the timeout in milliseconds for executors that are holding
    // shuffle data if shuffle tracking is enabled (true by default if dynamic allocation is enabled).
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shuffle_tracking_timeout: Option<i32>,
}

#[derive(
    Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Display, EnumString,
)]
pub enum SparkJobType {
    #[default]
    SqlJob,
    SqlFileJob,
    // TODO: remove JarJob or Jave & Scala
    JarJob,
    JavaJob,
    ScalaJob,
    PythonJob,
    RJob,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    #[serde(rename = "type")]
    pub typ: SparkJobType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jar: Option<JarJobSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub python: Option<PythonJobSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r: Option<RJobSpec>,
}

impl JobSpec {
    // should ONLY ref to self.spec.spark
    pub fn sql_config_map(&self, metadata: ObjectMeta) -> Result<ConfigMap> {
        Ok(ConfigMap {
            metadata,
            binary_data: None,
            data: Some(BTreeMap::from([(
                crate::constants::SQL_FILE_LOCAL_FILE_NAME.to_string(),
                self.sql
                    .clone()
                    .ok_or(Error::MissingJobField {
                        job_type: SparkJobType::SqlJob,
                        field_name: "sql".to_string(),
                    })?
                    .to_string(),
            )])),
            immutable: Some(true),
        })
    }

    pub(crate) fn sql_config_map_name(&self, app_name: &String) -> String {
        // add config map for driver
        format!(
            "{}{}",
            crate::constants::SQL_FILE_CONFIG_MAP_PREFIX.to_string(),
            common::utils::repair_resource_name(&app_name)
        )
    }

    pub(crate) fn populate_sko_fields(
        &self,
        app_name: &String,
        sko: &mut SKOSparkApplicationSpec,
    ) -> Result<()> {
        // assumed the sko.typ is assigned in sko_spec_default
        match self.typ {
            SparkJobType::JarJob | SparkJobType::JavaJob | SparkJobType::ScalaJob => {
                let jar = self.jar.clone().ok_or(Error::MissingJobField {
                    job_type: self.typ.clone(),
                    field_name: "jar".to_string(),
                })?;
                let main_application_file = Some(jar.main_application_file);
                let main_class = jar.main_class;
                sko.main_application_file = main_application_file;
                sko.main_class = main_class;
            }
            SparkJobType::SqlJob => {
                sko.main_application_file =
                    Some(crate::constants::SPARK_MAIN_APPLICATION_FILE.to_owned());
                sko.main_class = Some(crate::constants::SPARK_SQL_MAIN_CLASS.to_owned());

                // run sql with a volume mount, and add the local sql file
                let sql_file = format!(
                    "-f{}/{}",
                    crate::constants::SQL_FILE_LOCAL_DIR_NAME,
                    crate::constants::SQL_FILE_LOCAL_FILE_NAME
                );
                sko.arguments.get_or_insert(vec![]).push(sql_file);

                // add volume mount for sql config map
                sko.driver.config_maps.get_or_insert(vec![]).push(NamePath {
                    name: self.sql_config_map_name(app_name),
                    path: crate::constants::SQL_FILE_LOCAL_DIR_NAME.to_string(),
                });
            }
            SparkJobType::SqlFileJob => {
                sko.main_application_file =
                    Some(crate::constants::SPARK_MAIN_APPLICATION_FILE.to_owned());
                sko.main_class = Some(crate::constants::SPARK_SQL_MAIN_CLASS.to_owned());
                // append a argument for the sql file
                sko.arguments.get_or_insert(vec![]).push(format!(
                    "-f{}",
                    self.sql_file.clone().ok_or(Error::MissingJobField {
                        job_type: SparkJobType::SqlFileJob,
                        field_name: "sqlFile".to_string()
                    })?
                ));
            }
            SparkJobType::PythonJob => {
                let python = self.python.clone().ok_or(Error::MissingJobField {
                    job_type: SparkJobType::PythonJob,
                    field_name: "python file".to_string(),
                })?;
                sko.main_application_file = Some(python.application_file);
                sko.python_verison = python.python_version;
            }
            SparkJobType::RJob => {
                let r = self.r.clone().ok_or(Error::MissingJobField {
                    job_type: SparkJobType::RJob,
                    field_name: "r file".to_string(),
                })?;
                sko.main_application_file = Some(r.application_file);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlJobSpec {
    #[serde(default)]
    pub statement: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlFileJobSpec {
    #[serde(default)]
    pub file: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JarJobSpec {
    // MainFile is the path to a bundled JAR, Python, or R file of the application.
    // +optional
    #[serde(default)]
    pub main_application_file: String,
    // MainClass is the fully-qualified main class of the Spark application.
    // This only applies to Java/Scala Spark applications.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_class: Option<String>,
    // Arguments is a list of arguments to be passed to the application.
    // +optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PythonJobSpec {
    // This sets the major Python version of the docker
    // image used to run the driver and executor containers. Can either be 2 or 3, default 2.
    // +optional
    // +kubebuilder:validation:Enum={"2","3"}
    #[serde(default, skip_serializing_if = "Option::is_none")]
    python_version: Option<String>,
    /// would be set to mainApplicationFile
    application_file: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq, Validate)]
#[serde(rename_all = "camelCase")]
pub struct RJobSpec {
    /// would be set to mainApplicationFile
    application_file: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSpec {
    /*
    concurrencyPolicy:                                                                                                                                              │
      type: string                                                                                                                                                  │
    failedRunHistoryLimit:                                                                                                                                          │
      format: int32                                                                                                                                                 │
      type: integer                                                                                                                                                 │
    schedule:                                                                                                                                                       │
      type: string                                                                                                                                                  │
    successfulRunHistoryLimit:                                                                                                                                      │
      format: int32                                                                                                                                                 │
      type: integer                                                                                                                                                 │
    suspend:                                                                                                                                                        │
      type: boolean
    */
    #[serde(default)]
    pub schedule: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_run_history_limit: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub successful_run_history_limit: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspend: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hive_server2_thrift_options: Option<SparkHiveserver2ServiceConfiguration>,
    #[serde(
        default,
        rename = "hiveServer2UIOptions",
        skip_serializing_if = "Option::is_none"
    )]
    pub hive_server2_ui_options: Option<SparkHiveserver2ServiceConfiguration>,
}

async fn sko_spec_default_from_template(
    spark: &SparkSpec,
    typ: &SparkJobType,
    client: &Client,
    namespace: &str,
    template: Option<&String>
) -> Result<SKOSparkApplicationSpec> {
    let merged = spark.merge_template_opt(client, namespace, template).await?;
    let SparkSpec {
        spark_version,
        mode,
        proxy_user,
        image,
        image_pull_policy,
        image_pull_secrets,
        spark_conf,
        hadoop_conf,
        spark_config_map,
        hadoop_config_map,
        volumes,
        driver,
        executor,
        deps,
        restart_policy,
        node_selector,
        failure_retries,
        retry_interval,
        memory_overhead_factor,
        monitoring,
        batch_scheduler,
        batch_scheduler_options,
        time_to_live_seconds,
        spark_uioptions,
        dynamic_allocation,
        s3_connection,
        catalogs,
        env_sets,
    } = merged;
    
    // driver and executor
    let mut driver = sko_spec_driver(driver)?;
    let mut executor = executor.unwrap_or_default();
    // intial configs with ...
    let mut conf = spark_conf.unwrap_or_default();
    // initial deps
    let mut deps = deps.unwrap_or_default();
    // s3 conf
    if let Some(s3_connection) = s3_connection {
        let spec = match s3_connection {
            S3ConnectionDef::Inline(inline) => inline,
            S3ConnectionDef::Reference(resource_name) => {
                S3ConnectionSpec::get(resource_name.as_str(), client, namespace).await?
            }
        };
        conf.extend(spec.spark_configs());

        if let Some(credentials) = spec.credentials {
            // add credentials env-var for driver and executor
            if let Some(env_vars) = credentials.env_vars() {
                driver.env.get_or_insert(vec![]).extend(env_vars.clone());
                executor.env.get_or_insert(vec![]).extend(env_vars);
            }
            // add credentials volume and volume mount fro driver and executor
            // if let Some(secret) = credentials.secret_mount() {
            //     driver.secrets.get_or_insert(vec![]).push(secret.clone());
            //     executor.secrets.get_or_insert(vec![]).push(secret);
            // }       
        }
    }
    // catalog conf
    // make the key unique
    if let Some(catalogs) = catalogs {
        for catalog in catalogs {
            let spec = match catalog {
                SparkCatalogDef::Inline(inline) => inline,
                SparkCatalogDef::Reference(resource_name) => {
                    SparkCatalogSpec::get(resource_name.as_str(), client, namespace).await?
                }
            };
            conf.extend(spec.spark_configs());
            spec.jars.map(|r| deps.extend_jars(r));
        }
    };

    // envset conf
    if let Some(env_sets) = env_sets {
        for env_set in env_sets {
            let spec = match env_set {
                SparkEnvSetDef::Inline(inline) => inline,
                SparkEnvSetDef::Reference(resource_name) => {
                    SparkEnvSetSpec::get(resource_name.as_str(), client, namespace).await?
                }
            };
            conf.extend(spec.configs.unwrap_or_default());
            deps.extend(spec.deps.unwrap_or_default());
        }
    }

    Ok(SKOSparkApplicationSpec {
        typ: sko_application_type(typ),
        spark_version: spark_version,
        mode: mode,
        proxy_user: proxy_user,
        image: image,
        image_pull_policy: image_pull_policy,
        image_pull_secrets: image_pull_secrets,
        main_application_file: None,
        main_class: None,
        python_verison: None,
        arguments: None,

        spark_conf: Some(conf),
        hadoop_conf: hadoop_conf,
        spark_config_map: spark_config_map,
        hadoop_config_map: hadoop_config_map,
        volumes: volumes,
        driver: driver,
        executor: Some(executor),
        deps: Some(deps),
        restart_policy: restart_policy,
        node_selector: node_selector,
        failure_retries: failure_retries,
        retry_interval: retry_interval,
        memory_overhead_factor: memory_overhead_factor,
        monitoring: monitoring,
        batch_scheduler: batch_scheduler,
        batch_scheduler_options: batch_scheduler_options,
        time_to_live_seconds: time_to_live_seconds,
        spark_uioptions: spark_uioptions,
        dynamic_allocation: dynamic_allocation,
    })
}

// should ONLY ref to self.spec.spark
async fn sko_spec_default(
    spark: &SparkSpec,
    typ: &SparkJobType,
    client: &Client,
    namespace: &str,
) -> Result<SKOSparkApplicationSpec> {
    // driver and executor
    let mut driver = sko_spec_driver(spark.driver.clone())?;
    let mut executor = spark.executor.clone().unwrap_or_default();
    // intial configs with ...
    let mut conf = spark.spark_conf.clone().unwrap_or_default();
    // initial deps
    let mut deps = spark.deps.clone().unwrap_or_default();
    // s3 conf
    if let Some(s3_connection) = &spark.s3_connection {
        let spec = match s3_connection {
            S3ConnectionDef::Inline(inline) => inline.clone(),
            S3ConnectionDef::Reference(resource_name) => {
                S3ConnectionSpec::get(resource_name.as_str(), client, namespace).await?
            }
        };
        // add credentials env-var for driver and executor
        if let Some(credentials) = &spec.credentials {
            if let Some(env_vars) = credentials.env_vars() {
                driver.env.get_or_insert(vec![]).extend(env_vars.clone());
                executor.env.get_or_insert(vec![]).extend(env_vars);
            }    
        }
        
        conf.extend(spec.spark_configs());

        // add credentials volume and volume mount fro driver and executor
        if let Some(credentials) = &spec.credentials {
            if let Some(secret) = credentials.secret_mount() {
                driver.secrets.get_or_insert(vec![]).push(secret.clone());
                executor.secrets.get_or_insert(vec![]).push(secret);
            }                
        }
    }
    // catalog conf
    // make the key unique
    if let Some(catalogs) = &spark.catalogs {
        for catalog in catalogs {
            let spec = match catalog {
                SparkCatalogDef::Inline(inline) => inline.clone(),
                SparkCatalogDef::Reference(resource_name) => {
                    SparkCatalogSpec::get(resource_name.as_str(), client, namespace).await?
                }
            };
            conf.extend(spec.spark_configs());
            spec.jars.as_ref().map(|r| deps.extend_jars(r.clone()));
        }
    };

    // envset conf
    if let Some(env_sets) = &spark.env_sets {
        for env_set in env_sets {
            let spec = match env_set {
                SparkEnvSetDef::Inline(inline) => inline.clone(),
                SparkEnvSetDef::Reference(resource_name) => {
                    SparkEnvSetSpec::get(resource_name.as_str(), client, namespace).await?
                }
            };
            conf.extend(spec.configs.clone().unwrap_or_default());
            deps.extend(spec.deps.clone().unwrap_or_default());
        }
    }

    Ok(SKOSparkApplicationSpec {
        typ: sko_application_type(typ),
        spark_version: spark.spark_version.clone(),
        mode: spark.mode.clone(),
        proxy_user: spark.proxy_user.clone(),
        image: spark.image.clone(),
        image_pull_policy: spark.image_pull_policy.clone(),
        image_pull_secrets: spark.image_pull_secrets.clone(),
        main_application_file: None,
        main_class: None,
        python_verison: None,
        arguments: None,

        spark_conf: Some(conf),
        hadoop_conf: spark.hadoop_conf.clone(),
        spark_config_map: spark.spark_config_map.clone(),
        hadoop_config_map: spark.hadoop_config_map.clone(),
        volumes: spark.volumes.clone(),
        driver: driver,
        executor: Some(executor),
        deps: Some(deps),
        restart_policy: spark.restart_policy.clone(),
        node_selector: spark.node_selector.clone(),
        failure_retries: spark.failure_retries.clone(),
        retry_interval: spark.retry_interval.clone(),
        memory_overhead_factor: spark.memory_overhead_factor.clone(),
        monitoring: spark.monitoring.clone(),
        batch_scheduler: spark.batch_scheduler.clone(),
        batch_scheduler_options: spark.batch_scheduler_options.clone(),
        time_to_live_seconds: spark.time_to_live_seconds.clone(),
        spark_uioptions: spark.spark_uioptions.clone(),
        dynamic_allocation: spark.dynamic_allocation.clone(),
    })
}

fn sko_spec_driver(mut driver: DriverSpec) -> Result<DriverSpec> {
    if driver.service_account.is_none() {
        driver.service_account = Some(String::from(crate::constants::SKO_DEFAULT_SERVICE_ACCOUNT))
    }
    Ok(driver)
}

fn sko_application_type(typ: &SparkJobType) -> String {
    match typ {
        SparkJobType::SqlJob
        | SparkJobType::SqlFileJob
        | SparkJobType::JarJob
        | SparkJobType::JavaJob => crate::constants::SKO_APPLICATION_TYPE_JAVA.to_owned(),
        SparkJobType::ScalaJob => crate::constants::SKO_APPLICATION_TYPE_SCALA.to_owned(),
        SparkJobType::PythonJob => crate::constants::SKO_APPLICATION_TYPE_PYTHON.to_owned(),
        SparkJobType::RJob => crate::constants::SKO_APPLICATION_TYPE_R.to_owned(),
    }
}
trait Sparkable {
    fn spark(&self) -> &SparkSpec;
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkJob",
    shortname = "scj",
    status = "SparkApplicationStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkJobSpec {
    pub spark_template: Option<String>,
    pub spark: SparkSpec,
    pub job: JobSpec,
}

impl Sparkable for SparkJob {
    fn spark(&self) -> &SparkSpec {
        &self.spec.spark
    }
}

impl SparkJob {
    fn build_recommended_labels<'a>(&'a self, role: &'a str) -> ObjectLabels<Self> {
        ObjectLabels {
            owner: self,
            app_name: crate::constants::APP_NAME,
            // TODO: not this version
            app_version: &self.spec.spark.spark_version.deref(),
            operator_name: crate::constants::OPERATOR_NAME,
            controller_name: crate::constants::CONTROLLER_NAME_JOB,
            role,
            role_group: crate::constants::CONTROLLER_NAME_JOB,
        }
    }

    fn sko_meta_named(&self, name: String) -> Result<ObjectMeta> {
        Ok(ObjectMetaBuilder::new()
            .name(common::utils::repair_resource_name(&name))
            // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
            // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
            .ownerreference_from_resource(self, None, None)
            .map_err(|_| Error::FailedBuildOwnerReference { name: name })?
            .with_recommended_labels(
                self.build_recommended_labels(crate::constants::RESOURCE_ROLE_SKO),
            )
            .build())
    }

    fn sko_meta(&self) -> Result<ObjectMeta> {
        let name = self.name_any();
        Ok(self.sko_meta_named(name)?)
    }

    async fn sko_spec(&self, client: &Client, namespace: &str) -> Result<SKOSparkApplicationSpec> {
        // let mut sko = sko_spec_default(self.spark(), &self.spec.job.typ, client, namespace).await?;
        let mut sko = sko_spec_default_from_template(
            self.spark(), 
            &self.spec.job.typ, 
            client, 
            namespace, 
            self.spec.spark_template.as_ref()
        ).await?;

        let name = self.name_any();
        self.spec.job.populate_sko_fields(&name, &mut sko)?;
        Ok(sko)
    }

    pub async fn sko_application(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<SKOSparkApplication> {
        let appl = SKOSparkApplication {
            metadata: self.sko_meta()?,
            spec: self.sko_spec(client, namespace).await?,
            status: Option::None,
        };
        Ok(appl)
    }

    pub fn sql_config_map(&self) -> Result<Option<ConfigMap>> {
        match self.spec.job.typ {
            SparkJobType::SqlJob => {
                let app_name = self.name_any();
                let cm_name = self.spec.job.sql_config_map_name(&app_name);
                Ok(Some(
                    self.spec
                        .job
                        .sql_config_map(self.sko_meta_named(cm_name)?)?,
                ))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkScheduledJob",
    shortname = "ssj",
    status = "SparkApplicationStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkScheduledJobSpec {
    pub spark_template: Option<String>,
    pub spark: SparkSpec,
    pub job: JobSpec,
    pub schedule: ScheduleSpec,
}

impl Sparkable for SparkScheduledJob {
    fn spark(&self) -> &SparkSpec {
        &self.spec.spark
    }
}

impl SparkScheduledJob {
    fn build_recommended_labels<'a>(&'a self, role: &'a str) -> ObjectLabels<Self> {
        ObjectLabels {
            owner: self,
            app_name: crate::constants::APP_NAME,
            // TODO: not this version
            app_version: &self.spec.spark.spark_version.deref(),
            operator_name: crate::constants::OPERATOR_NAME,
            controller_name: crate::constants::CONTROLLER_NAME_SCHD_JOB,
            role,
            role_group: crate::constants::CONTROLLER_NAME_SCHD_JOB,
        }
    }

    fn sko_meta_named(&self, name: String) -> Result<ObjectMeta> {
        Ok(ObjectMetaBuilder::new()
            .name(common::utils::repair_resource_name(&name))
            // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
            // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
            .ownerreference_from_resource(self, None, None)
            .map_err(|_| Error::FailedBuildOwnerReference { name: name })?
            .with_recommended_labels(
                self.build_recommended_labels(crate::constants::RESOURCE_ROLE_SKO),
            )
            .build())
    }

    fn sko_meta(&self) -> Result<ObjectMeta> {
        let name = self.name_any();
        Ok(self.sko_meta_named(name)?)
    }

    async fn sko_spec(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<SKOScheduledSparkApplicationSpec> {
        // let mut templete =
        //     sko_spec_default(self.spark(), &SparkJobType::JavaJob, client, namespace).await?;
        let mut templete = sko_spec_default_from_template(
            self.spark(), 
            &self.spec.job.typ, 
            client, 
            namespace, 
            self.spec.spark_template.as_ref()
        ).await?;

        let name = self.name_any();
        self.spec.job.populate_sko_fields(&name, &mut templete)?;
        Ok(SKOScheduledSparkApplicationSpec {
            schedule: self.spec.schedule.schedule.clone(),
            template: templete,
            suspend: self.spec.schedule.suspend,
            concurrency_policy: self.spec.schedule.concurrency_policy.clone(),
            successful_run_history_limit: self.spec.schedule.successful_run_history_limit,
            failed_run_history_limit: self.spec.schedule.failed_run_history_limit,
        })
    }

    pub async fn sko_application(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<SKOScheduledSparkApplication> {
        let appl = SKOScheduledSparkApplication {
            metadata: self.sko_meta()?,
            spec: self.sko_spec(client, namespace).await?,
            status: Option::None,
        };
        Ok(appl)
    }

    pub fn sql_config_map(&self) -> Result<Option<ConfigMap>> {
        match self.spec.job.typ {
            SparkJobType::SqlJob => {
                let app_name = self.name_any();
                // add some to avoid name collisions with SparkJob with same name
                let cm_name = self
                    .spec
                    .job
                    .sql_config_map_name(&format!("{}-schd", app_name));
                Ok(Some(
                    self.spec
                        .job
                        .sql_config_map(self.sko_meta_named(cm_name)?)?,
                ))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkSession",
    shortname = "scs",
    status = "SparkApplicationStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkSessionSpec {
    pub spark_template: Option<String>,
    pub spark: SparkSpec,
    pub session: SessionSpec,
}

impl Sparkable for SparkSession {
    fn spark(&self) -> &SparkSpec {
        &self.spec.spark
    }
}

impl SparkSession {
    fn build_recommended_labels<'a>(&'a self, role: &'a str) -> ObjectLabels<Self> {
        ObjectLabels {
            owner: self,
            app_name: crate::constants::APP_NAME,
            // TODO: not this version
            app_version: &self.spec.spark.spark_version.deref(),
            operator_name: crate::constants::OPERATOR_NAME,
            controller_name: crate::constants::CONTROLLER_NAME_SESSION,
            role,
            role_group: crate::constants::CONTROLLER_NAME_SESSION,
        }
    }

    fn sko_meta_named(&self, name: String) -> Result<ObjectMeta> {
        Ok(ObjectMetaBuilder::new()
            .name(common::utils::repair_resource_name(&name))
            // this reference is not pointing to a controller but only provides a UID that can used to clean up resources
            // cleanly (specifically driver pods and related config maps) when the spark application is deleted.
            .ownerreference_from_resource(self, None, None)
            .map_err(|_| Error::FailedBuildOwnerReference { name: name })?
            .with_recommended_labels(
                self.build_recommended_labels(crate::constants::RESOURCE_ROLE_SKO),
            )
            .build())
    }

    fn sko_meta(&self) -> Result<ObjectMeta> {
        // re-name the child resource
        // let name = self.name_any();
        // let new_name = format!("{}-{}", name, common::utils::generate_random_string(4).to_lowercase());
        let new_name = self.name_any();
        Ok(self.sko_meta_named(new_name)?)
    }

    async fn sko_spec(&self, client: &Client, namespace: &str) -> Result<SKOSparkApplicationSpec> {
        let main_application_file = Some(crate::constants::SPARK_MAIN_APPLICATION_FILE.to_owned());
        let main_class = Some(crate::constants::SPARK_SESSION_MAIN_CLASS.to_owned());

        let thrift_conf = self
            .spec
            .session
            .hive_server2_thrift_options
            .clone()
            .unwrap_or_default();
        let ui_conf = self
            .spec
            .session
            .hive_server2_ui_options
            .clone()
            .unwrap_or_default();
        let hs2_thrift_port = Port {
            name: thrift_conf
                .service_port_name
                .clone()
                .unwrap_or("hs2-thrift-port".to_string()),
            protocol: "TCP".to_string(),
            container_port: crate::constants::HIVE_SERVER2_THRIFT_DEFAULT_PORT,
        };
        let hs2_ui_port = Port {
            name: ui_conf
                .service_port_name
                .clone()
                .unwrap_or("hs2-ui-port".to_string()),
            protocol: "TCP".to_string(),
            container_port: crate::constants::HIVE_SERVER2_UI_DEFAULT_PORT,
        };

        // let mut sko = sko_spec_default(self.spark(), &SparkJobType::JavaJob, client, namespace).await?;
        let mut sko = sko_spec_default_from_template(
            self.spark(), 
            &SparkJobType::JavaJob, 
            client, 
            namespace, 
            self.spec.spark_template.as_ref()
        ).await?;

        sko.main_application_file = main_application_file;
        sko.main_class = main_class;

        // add ports
        let driver_ports = sko.driver.ports.get_or_insert(vec![]);
        driver_ports.extend(vec![hs2_thrift_port, hs2_ui_port]);

        // add spark configs
        let spark_conf = sko.spark_conf.get_or_insert(HashMap::new());
        spark_conf.extend(self.spark_hs2_configs()?);

        Ok(sko)
    }

    pub async fn sko_application(
        &self,
        client: &Client,
        namespace: &str,
    ) -> Result<SKOSparkApplication> {
        // TODO: validate session ...
        let appl = SKOSparkApplication {
            metadata: self.sko_meta()?,
            spec: self.sko_spec(client, namespace).await?,
            status: Option::None,
        };
        Ok(appl)
    }

    fn spark_hs2_configs(&self) -> Result<HashMap<String, String>> {
        // TODO: more details
        Ok(HashMap::from([
            (
                crate::constants::SPARK_HIVE_SERVER2_WEBUI_HOST.to_string(),
                "0.0.0.0".to_string(),
            ),
            (
                crate::constants::SPARK_HIVE_SERVER2_WEBUI_PORT.to_string(),
                crate::constants::HIVE_SERVER2_UI_DEFAULT_PORT.to_string(),
            ),
            (
                crate::constants::SPARK_HIVE_SERVER2_THRIFT_BIND_HOST.to_string(),
                "0.0.0.0".to_string(),
            ),
            (
                crate::constants::SPARK_HIVE_SERVER2_THRIFT_PORT.to_string(),
                crate::constants::HIVE_SERVER2_THRIFT_DEFAULT_PORT.to_string(),
            ),
            (
                crate::constants::SPARK_HIVE_SERVER2_ENABLE_DOAS.to_string(),
                "false".to_string(),
            ),
        ]))
    }

    pub fn hive_server2_thrift_service(&self) -> Result<Service> {
        let servicec_config = self
            .spec
            .session
            .hive_server2_thrift_options
            .clone()
            .unwrap_or_default();
        let name = self.name_any();
        Ok(Service {
            metadata: self.sko_meta_named(format!(
                "{}-hs2-thrift-svc",
                common::utils::repair_resource_name(&name)
            ))?,
            spec: Option::Some(k8s_openapi::api::core::v1::ServiceSpec {
                // ExternalName, ClusterIP, NodePort, and LoadBalancer
                type_: Some(
                    servicec_config
                        .service_type
                        .clone()
                        .unwrap_or_default()
                        .to_string(),
                ),
                ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
                    name: servicec_config
                        .service_port_name
                        .clone()
                        .or(Some("hs2-thrift-port".to_string())),
                    app_protocol: None,
                    node_port: match servicec_config.service_type {
                        Some(ServiceType::NodePort) => servicec_config.service_port.or(Some(
                            crate::constants::HIVE_SERVER2_THRIFT_DEFAULT_NODE_PORT,
                        )),
                        _ => None,
                    },
                    port: servicec_config
                        .service_port
                        .unwrap_or(crate::constants::HIVE_SERVER2_THRIFT_DEFAULT_NODE_PORT),
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(
                        crate::constants::HIVE_SERVER2_THRIFT_DEFAULT_PORT,
                    )),
                }]),
                // TODO: selector
                selector: Some(<BTreeMap<String, String>>::from([
                    ("spark-role".to_string(), "driver".to_string()),
                    ("sparkoperator.k8s.io/app-name".to_string(), name),
                ])),
                ..k8s_openapi::api::core::v1::ServiceSpec::default()
            }),
            status: Option::None,
        })
    }

    pub fn hive_server2_ui_service(&self) -> Result<Service> {
        let servicec_config = self
            .spec
            .session
            .hive_server2_ui_options
            .clone()
            .unwrap_or_default();
        let name = self.name_any();
        Ok(Service {
            metadata: self.sko_meta_named(format!(
                "{}-hs2-ui-svc",
                common::utils::repair_resource_name(&name)
            ))?,
            spec: Option::Some(k8s_openapi::api::core::v1::ServiceSpec {
                // ExternalName, ClusterIP, NodePort, and LoadBalancer
                type_: Some(
                    servicec_config
                        .service_type
                        .clone()
                        .unwrap_or_default()
                        .to_string(),
                ),
                ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
                    name: servicec_config
                        .service_port_name
                        .clone()
                        .or(Some("hs2-ui-port".to_string())),
                    app_protocol: None,
                    node_port: match servicec_config.service_type {
                        Some(ServiceType::NodePort) => servicec_config
                            .service_port
                            .or(Some(crate::constants::HIVE_SERVER2_UI_DEFAULT_NODE_PORT)),
                        _ => None,
                    },
                    port: servicec_config
                        .service_port
                        .unwrap_or(crate::constants::HIVE_SERVER2_UI_DEFAULT_NODE_PORT),
                    protocol: Some("TCP".to_string()),
                    target_port: Some(IntOrString::Int(
                        crate::constants::HIVE_SERVER2_UI_DEFAULT_PORT,
                    )),
                }]),
                selector: Some(<BTreeMap<String, String>>::from([
                    ("spark-role".to_string(), "driver".to_string()),
                    ("sparkoperator.k8s.io/app-name".to_string(), name),
                ])),
                ..k8s_openapi::api::core::v1::ServiceSpec::default()
            }),
            status: Option::None,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Display, EnumString)]
pub enum ImagePullPolicy {
    Always,
    IfNotPresent,
    Never,
}

#[derive(
    Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Display, EnumString,
)]
pub enum ServiceType {
    ClusterIP,
    #[default]
    NodePort,
    LoadBalancer,
    ExternalName,
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use kube::core::ObjectMeta;

    use super::*;

    #[test]
    fn test_ser_spark_job() {
        let job = SparkJob {
            metadata: ObjectMeta {
                name: Some("test_name".to_string()),
                namespace: Some("test_ns".to_string()),
                /* TODO: More fields */
                ..ObjectMeta::default()
            },
            spec: SparkJobSpec {
                spark: SparkSpec {
                    spark_version: "3.1.1".to_string(),
                    ..SparkSpec::default()
                },
                job: JobSpec {
                    typ: SparkJobType::SqlJob,
                    sql: Some("SELECT 1".to_string()),
                    ..JobSpec::default()
                },
                spark_template: None
            },
            status: Option::None,
        };

        let yml = serde_yaml::to_string(&job).unwrap();

        println!("{}", yml);
    }

    #[test]
    fn test_deser_spark_job() {
        let job = serde_yaml::from_str::<SparkJob>(
            "
        apiVersion: spark.bytenative.com/v1
        kind: SparkJob
        metadata:
          name: test_name
          namespace: test_ns
        spec:
          spark:
            sparkVersion: 3.1.1
          job:
            type: SqlJob
            sql: SELECT 1
        ",
        )
        .unwrap();

        assert_eq!(
            job.spec.job,
            JobSpec {
                typ: SparkJobType::SqlJob,
                sql: Some("SELECT 1".to_string()),
                ..JobSpec::default()
            }
        )
    }

    #[test]
    fn test_sko_migrated_sample() {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open("spark-job-example.yaml")
            .expect("Couldn't open file");
        let job = serde_yaml::from_reader::<File, SparkJob>(f).unwrap();
        println!("{:?}", serde_yaml::to_string(&job).unwrap());
        assert!(job.spec.spark.volumes.is_some());
    }
}
