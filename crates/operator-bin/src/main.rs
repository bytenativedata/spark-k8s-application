mod controller;
mod error;
mod sko_application_controller;
mod sko_schd_application_controller;
mod spark_job_controller;
mod spark_schd_job_controller;
mod spark_session_controller;
use std::sync::Arc;

// TODO: move common functions into this module, and change the name
use clap::{crate_description, crate_version, Args, Parser};
use crd::constants;
use futures::StreamExt;
use k8s_openapi::NamespaceResourceScope;
use kube::runtime::Controller;
use kube::runtime::controller::{Action, Error as KubeError};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher::Config;
use kube::{Resource, Api, Client};

use crate::controller::ContextData;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
    // pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub enum Command<
        Run: Args = ProductOperatorRun,
        Print: Args = CrdParams
        > {
    /// Print CRD objects
    Crd(Print),
    /// Print generated SKO objects
    Sko(SkoParams),
    /// Run operator
    Run(Run),
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct ProductOperatorRun {
    /// Provides the path to a product-config file
    #[arg(long, short = 'p', value_name = "FILE", default_value = "", env)]
    pub product_config: common::ProductConfigPath,
    /// Provides a specific namespace to watch (instead of watching all namespaces)
    #[arg(long, env, default_value = "")]
    pub watch_namespace: common::WatchNamespace,
    /// Tracing log collector system
    #[arg(long, env, default_value_t, value_enum)]
    pub tracing_target: common::logging::TracingTarget,
    /// Log level
    #[arg(long, default_value = "INFO")]
    pub log_level: String,
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct SkoParams {
    /// Provides a specific object type to handle
    #[arg(long = "TYPE", short = 't', value_enum)]
    pub typ: crd::ObjectType,
    /// Provides the path to a job or session yaml
    #[arg(long, short = 'f')]
    pub file: String,
}

#[derive(clap::Parser, Debug, PartialEq, Eq)]
#[command(long_about = "")]
pub struct CrdParams {
    /// Provides the path to a product-config file
    #[arg(long, short = 'f', value_name = "FILE", default_value = "", env)]
    pub file: String,
}

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

fn get_api<K>(client: Client, namespace: Option<&String>) -> Api<K>
    where
        <K as Resource>::DynamicType: Default,
        K: Resource<Scope = NamespaceResourceScope>,
    {
    if let Some(ns) = namespace {
        Api::namespaced(client, ns)
    } else {
        Api::all(client)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd(print) => {
            if print.file.is_empty() {
                crd::print_yaml_schema::<crd::spark_application::SparkJob>()?;
                crd::print_yaml_schema::<crd::spark_application::SparkScheduledJob>()?;
                crd::print_yaml_schema::<crd::spark_application::SparkSession>()?;
            } else {
                crd::serialize_crds_to_file(print.file.as_str())?;
            }
            return Ok(());
        }
        
        Command::Sko(sko) => {
            let kube_client = kube::client::Client::try_default()
                .await
                .expect("Failed to create kube client");
            crd::print_sko_object_from(sko.typ, sko.file, &kube_client).await?;
            return Ok(());
        }

        Command::Run(ProductOperatorRun {
            product_config: _,
            watch_namespace,
            tracing_target: _,
            log_level
        }) => {
            common::logging::initialize_logging(
                constants::OPERATOR_LOG_ENV,
                constants::OPERATOR_NAME,
                common::logging::TracingTarget::None,
                log_level.as_str()
            );
            common::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET_PLATFORM.unwrap_or("unknown target platform"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            
            let mut namespace: Option<String> = None;
            if let common::WatchNamespace::One(ns) = watch_namespace {
                namespace = Some(ns);
            }
            let namespace = namespace.as_ref();

            let kube_client = kube::client::Client::try_default()
                .await
                .expect("Failed to create kube client");

            // Preparation of resources used by the `kube_runtime::Controller`
            let spark_job_crd_api: Api<crd::spark_application::SparkJob> = 
                get_api(kube_client.clone(), namespace);
            let spark_schd_job_crd_api: Api<crd::spark_application::SparkScheduledJob> = 
                get_api(kube_client.clone(), namespace);
            let spark_session_crd_api: Api<crd::spark_application::SparkSession> = 
                get_api(kube_client.clone(), namespace);
            let sko_app_crd_api: Api<crd::sko_spark_application::SparkApplication> = 
                get_api(kube_client.clone(), namespace);
            let sko_schd_app_crd_api: Api<crd::sko_spark_application::ScheduledSparkApplication> = 
                get_api(kube_client.clone(), namespace);

            // The controller comes from the `kube_runtime` crate and manages the reconciliation process.
            // It requires the following information:
            // - `kube::Api<T>` this controller "owns". In this case, `T = SparkApplication`, as this controller owns the `SparkApplication` resource,
            // - `kube::runtime::watcher::Config` can be adjusted for precise filtering of `SparkApplication` resources before the actual reconciliation, e.g. by label,
            // - `reconcile` function with reconciliation logic to be called each time a resource of `SparkApplication` kind is created/updated/deleted,
            // - `on_error` function to call whenever reconciliation fails.

            let scj_controller = Controller::new(spark_job_crd_api.clone(), Config::default())
                .run(
                    spark_job_controller::reconcile,
                    spark_job_controller::on_error,
                    Arc::new(ContextData::new(kube_client.clone())),
                )
                .map(|reconciliation_result| {
                    match reconciliation_result {
                        Ok(resource) => {
                            tracing::info!("Reconciliation successful. Resource: {:?}", resource);
                        }
                        Err(reconciliation_err) => {
                            tracing::error!("Reconciliation error: {:?}", reconciliation_err)
                        }
                    };
                });

            let ssj_controler = Controller::new(spark_schd_job_crd_api.clone(), Config::default())
                .run(
                    spark_schd_job_controller::reconcile,
                    spark_schd_job_controller::on_error,
                    Arc::new(ContextData::new(kube_client.clone())),
                )
                .map(|reconciliation_result| {
                    match reconciliation_result {
                        Ok(resource) => {
                            tracing::info!("Reconciliation successful. Resource: {:?}", resource);
                        }
                        Err(reconciliation_err) => {
                            tracing::error!("Reconciliation error: {:?}", reconciliation_err)
                        }
                    };
                });
            // scs_controller
            let scs_controller = Controller::new(spark_session_crd_api.clone(), Config::default())
                .run(
                    spark_session_controller::reconcile,
                    spark_session_controller::on_error,
                    Arc::new(ContextData::new(kube_client.clone())),
                )
                .map(|reconciliation_result| {
                    match reconciliation_result {
                        Ok(resource) => {
                            tracing::info!("Reconciliation successful. Resource: {:?}", resource);
                        }
                        Err(reconciliation_err) => {
                            tracing::error!("Reconciliation error: {:?}", reconciliation_err)
                        }
                    };
                });

            let sko_app_controller = Controller::new(sko_app_crd_api.clone(), Config::default())
                .run(
                    sko_application_controller::reconcile,
                    sko_application_controller::on_error,
                    Arc::new(ContextData::new(kube_client.clone())),
                )
                .map(|reconciliation_result| {
                    match reconciliation_result {
                        Ok(resource) => {
                            tracing::info!("Reconciliation successful. Resource: {:?}", resource);
                        }
                        Err(reconciliation_err) => {
                            tracing::error!("Reconciliation error: {:?}", reconciliation_err)
                        }
                    };
                });

            let sko_schd_app_controller = Controller::new(sko_schd_app_crd_api.clone(), Config::default())
                .run(
                    sko_schd_application_controller::reconcile,
                    sko_schd_application_controller::on_error,
                    Arc::new(ContextData::new(kube_client.clone())),
                )
                .map(|reconciliation_result| {
                    match reconciliation_result {
                        Ok(resource) => {
                            tracing::info!("Reconciliation successful. Resource: {:?}", resource);
                        }
                        Err(reconciliation_err) => {
                            tracing::error!("Reconciliation error: {:?}", reconciliation_err)
                        }
                    };
                });

            futures::stream::select(
                futures::stream::select(
                    futures::stream::select(scj_controller, ssj_controler),
                    scs_controller,
                ),
                futures::stream::select(sko_app_controller, sko_schd_app_controller),
            )
            .collect::<()>()
            .await;

            Ok(())
        }
    }
}

pub fn report_controller_reconciled<K, ReconcileErr, QueueErr>(
    result: &Result<(ObjectRef<K>, Action), KubeError<ReconcileErr, QueueErr>>,
) where
    K: kube::Resource,
{
    match result {
        Ok((obj, _)) => {
            println!(
                "Reconciliation successful. Resource: {:?}:{:?}",
                obj.name, obj.namespace
            );
        }
        Err(err) => match err {
            KubeError::ObjectNotFound(source) => {
                tracing::error!("Reconciliation error: {:?}", source);
            }
            KubeError::ReconcilerFailed(_, obj) => {
                tracing::error!("Reconciliation error: reconciler for object {:?} failed", obj);
            }
            KubeError::QueueError(_) => {
                tracing::error!("Reconciliation error: event queue error");
            }
            KubeError::RunnerError(source) => {
                tracing::error!("Reconciliation error: {:?}", source);
            } 
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// test connection to k3s
    #[tokio::test]
    async fn test_connection() {
        // TODO: initialize kube client
        let kube_client = kube::client::Client::try_default()
            .await
            .expect("Failed to create client");

        println!("{:?}", kube_client.apiserver_version().await.unwrap());

        // test k3s connection with pods api
        let pods: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(kube_client.clone(), "kube-system");
        let p = pods
            .list(&kube::api::ListParams::default())
            .await
            .expect("Failed to get pod");
        println!("{:?}", p);
    }

    /// test connection to k3s
    #[tokio::test]
    async fn test_sko_deploy() {
        let kube_client = kube::client::Client::try_default()
            .await
            .expect("Failed to create client");

        let apps: Api<crd::sko_spark_application::SparkApplication> =
            Api::namespaced(kube_client.clone(), "sparkjob");
        let p = apps
            .list(&kube::api::ListParams::default())
            .await
            .expect("Failed to get sko spark apps");
        println!("{:?}", p);
    }
}
