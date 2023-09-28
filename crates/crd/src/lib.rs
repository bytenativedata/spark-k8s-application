use std::collections::HashMap;

use k8s_openapi::Resource;
use kube::{Api, Client, CustomResource, CustomResourceExt, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod metadata;
pub mod constants;
pub mod s3;
pub mod sko_spark_application;
pub mod spark_application;

// error definitions for crd
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Missing S3 connection [{name}]")]
    MissingS3Connection { name: String },

    #[error("Missing S3 bucket [{name}]")]
    MissingS3Bucket { name: String },

    #[error("Missing Spark Catalog [{name}]")]
    MissingSparkCatalog { name: String },

    #[error("Missing Spark EnvSet [{name}]")]
    MissingSparEnvSet { name: String },

    #[error("Missing Spark template [{name}]")]
    MissingSparkTemplate { name: String },

    #[error("Failed to serialize resource [{internal}]")]
    FailedSerializeResource { internal: String },

    #[error("Failed to build owner reference [{name}]")]
    FailedBuildOwnerReference { name: String },

    #[error("For {job_type} job, the {field_name} field should be specified")]
    MissingJobField {
        job_type: SparkJobType,
        field_name: String
    },
    
    #[error("Object is missing key: {key}")]
    MissingObjectKey { key: &'static str },
    
    #[error("Failed to serialize object to json with internal error: \n {internal}")]
    FailedSerializeObjectToJson { internal: serde_json::error::Error },
    
    #[error("Failed to deserialize object from json with internal error: \n {internal}")]
    FailedDeserializeObjectFromJson { internal: serde_json::error::Error },

    #[error("Failed to deserialize object from yaml with internal error: \n {internal}")]
    FailedDeserializeObjectFromYaml { internal: serde_yaml::Error },

    #[error("Failed to merge json objects with internal error: \n {internal}")]
    FailedMergeObjects { internal: serde_json::error::Error },
    
    #[error("Resource namespace not exists [{name}]")]
    ResourceNamespaceNotExists { name: String },
}


#[derive(Debug, Clone, clap::ValueEnum, PartialEq, Eq)]
pub enum ObjectType {
    Job,
    Session,
    ScheduledJob
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    pub phase: String,
}

// --------------------

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkCatalog",
    plural = "sparkcatalogs",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkCatalogSpec {
    pub name: String,
    /// in case `spark_catalog_`, not provided
    pub impl_class: Option<String>,
    /// the jars to be added for this catalog
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jars: Option<Vec<String>>,
    /// add the prefix to the catalog name, and then apply to the spark application
    /// for example: spark_catalog_prefix spark.sql.catalog.{name}.{key}: {value}
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_configs: Option<HashMap<String, String>>,
    /// directly applied to the spark application
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_configs: Option<HashMap<String, String>>,
}

impl SparkCatalogSpec {
    /// Convenience function to retrieve the spec of a S3 bucket resource from the K8S API service.
    pub async fn get(
        resource_name: &str,
        client: &Client,
        namespace: &str,
    ) -> Result<SparkCatalogSpec> {
        let catalog_api = Api::<SparkCatalog>::namespaced(client.clone(), namespace);
        let catalog =
            catalog_api
                .get(resource_name)
                .await
                .map_err(|_| Error::MissingSparkCatalog {
                    name: resource_name.to_string(),
                })?;

        Ok(catalog.spec)
    }

    pub fn spark_configs(&self) -> HashMap<String, String> {
        let name = self.name.as_str();

        let mut all_configs = self
            .catalog_configs
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|(k, v)| (format!("spark.sql.catalog.{}.{}", name, k), v.clone()))
            .collect::<HashMap<String, String>>();

        all_configs.extend([(
            format!("spark.sql.catalog.{}", name),
            self.impl_class.clone().unwrap_or_default(),
        )]);
        all_configs.extend(self.spark_configs.clone().unwrap_or_default());
        // Ok(ret)
        all_configs
    }
}

/// Operators are expected to define fields for this type in order to work with Spark catalog.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SparkCatalogDef {
    Inline(SparkCatalogSpec),
    Reference(String),
}

impl SparkCatalogDef {
    /// Return an [SparkCatalogSpec]
    pub async fn resolve(&self, client: &Client, namespace: &str) -> Result<SparkCatalogSpec> {
        match self {
            SparkCatalogDef::Inline(spec) => Ok(spec.clone()),
            SparkCatalogDef::Reference(resource_name) => {
                SparkCatalogSpec::get(resource_name, client, namespace).await
            }
        }
    }
}

/// Struct to define a `Set` of configs and deps related to a spark case or application
/// for example: mysql-8
///     deps.jars: ["mysql-connector-java-8.0.20.jar"]
///     configs: {}
/// These could be reused and be merge to current spark job/session by define:
///     spec.spark.env_sets: [mysql-8]
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[kube(
    group = "spark.bytenative.com",
    version = "v1",
    kind = "SparkEnvSet",
    plural = "sparkenvsets",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkEnvSetSpec {
    /// directly applied to the spark application
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configs: Option<HashMap<String, String>>,
    /// the deps including jars, files ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<spark_application::Dependencies>,
}

impl SparkEnvSetSpec {
    /// Convenience function to retrieve the spec of a S3 bucket resource from the K8S API service.
    pub async fn get(
        resource_name: &str,
        client: &Client,
        namespace: &str,
    ) -> Result<SparkEnvSetSpec> {
        let resource_api = Api::<SparkEnvSet>::namespaced(client.clone(), namespace);
        let resource =
            resource_api
                .get(resource_name)
                .await
                .map_err(|_| Error::MissingSparEnvSet {
                    name: resource_name.to_string(),
                })?;

        Ok(resource.spec)
    }
}

/// Operators are expected to define fields for this type in order to work with Spark envset.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SparkEnvSetDef {
    Inline(SparkEnvSetSpec),
    Reference(String),
}

impl SparkEnvSetDef {
    /// resolve the spec
    pub async fn resolve(&self, client: &Client, namespace: &str) -> Result<SparkEnvSetSpec> {
        match self {
            SparkEnvSetDef::Inline(spec) => Ok(spec.clone()),
            SparkEnvSetDef::Reference(resource_name) => {
                SparkEnvSetSpec::get(resource_name, client, namespace).await
            }
        }
    }
}

// -------------------------
// Generated the CRD specification to the YAML


use serde_yaml::Mapping;
use spark_application::SparkJobType;
#[allow(dead_code)]
fn remove_description_fileds(v: &serde_yaml::Value) -> Option<serde_yaml::Value> {
    match v {
        serde_yaml::Value::Mapping(m) => {
            let mut clone = Mapping::new();
            for (key, val) in m.iter() {
                if key.is_string() && key.as_str() == Some("description") {
                    // ignore `description`
                    // println!("trim desc key with Value: {:?}", val);
                } else {
                    remove_description_fileds(val).and_then(|m| {
                        clone.insert(key.clone(), m);
                        Some(())
                    });
                }
            }
            Some(serde_yaml::Value::Mapping(clone))
        }
        serde_yaml::Value::Sequence(s) => {
            let mut clone = serde_yaml::Sequence::new();
            s.iter().for_each(|val| {
                remove_description_fileds(val).and_then(|m| {
                    clone.push(m);
                    Some(())
                });
            });
            Some(serde_yaml::Value::Sequence(clone))
        }
        serde_yaml::Value::Tagged(s) => {
            // TODO: trim description if meet
            let clone = s.clone();
            Some(serde_yaml::Value::Tagged(clone))
        }
        _ => Some(v.clone()),
    }
}

#[allow(dead_code)]
pub(crate) fn serialize_crd_to_file<T: CustomResourceExt>(file: &str) {
    let crd = T::crd();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(file)
        .expect("Couldn't open file");
    let value = serde_yaml::to_value(crd).unwrap();
    let value = remove_description_fileds(&value);
    serde_yaml::to_writer(f, &value).unwrap();
}

pub(crate) fn serialize_crd_to_string<T: CustomResourceExt>() -> Result<String>{
    let crd = T::crd();
    let value = serde_yaml::to_value(crd).unwrap();
    let value = remove_description_fileds(&value);
    let string = serde_yaml::to_string(&value).map_err(|e| {
        Error::FailedSerializeResource { internal: e.to_string() }
    })?;
    Ok(string)
}

pub fn serialize_crds_to_file(file: &str) -> Result<(), Error> {
    let scj = serialize_crd_to_string::<crate::spark_application::SparkJob>()?;
    let ssj = serialize_crd_to_string::<crate::spark_application::SparkScheduledJob>()?;
    let scs = serialize_crd_to_string::<crate::spark_application::SparkSession>()?;
    let sct = serialize_crd_to_string::<crate::spark_application::SparkTemplate>()?;
    let s3c = serialize_crd_to_string::<crate::s3::S3Connection>()?;
    let s3b = serialize_crd_to_string::<crate::s3::S3Bucket>()?;
    let scl = serialize_crd_to_string::<SparkCatalog>()?;
    let ses = serialize_crd_to_string::<SparkEnvSet>()?;

    let contents = format!("
---
{}

---
{}

---
{}

---
{}

---
{}

---
{}

---
{}

---
{}

    ", scj, ssj, scs, sct, s3c, s3b, scl, ses);
    std::fs::write(file, contents).unwrap_or_else(|e| {
        println!("Write CRDs Error {:?}", e);
    });

    Ok(())
}

pub fn print_yaml_schema<T: CustomResourceExt>() -> Result<()> {
    let string = serialize_crd_to_string::<T>()?;
    
    println!("---");
    println!("{string}");
    Ok(())
}

fn resource_from_yaml_file<K>(file: String) -> Result<K> 
where 
    K: kube::Resource,
    K: serde::de::DeserializeOwned
{
    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(file)
        .expect("Couldn't open file");
    
    let mut resource: K = 
    serde_yaml::from_reader(f)
        .map_err(|e| {
            Error::FailedDeserializeObjectFromYaml { internal: e }
        })?;
    let name = resource.name_any();
    let namespace: String = resource.namespace().ok_or(Error::ResourceNamespaceNotExists { name })?;
    // make up resource with a fake uid to pass build-owner-reference
    resource.meta_mut().uid = Some("fake-uid".to_string());
    Ok(resource)
}

/// generate SKO application from a specific type of Job/Session and input file, then print ...
pub async fn print_sko_object_from(typ: ObjectType, file: String, client: &Client) -> Result<()> {
    let yaml = match typ {
        ObjectType::Job => {
            let resource = resource_from_yaml_file::<crate::spark_application::SparkJob>(file)?;
            let name = resource.name_any();
            let namespace: String = resource.namespace().ok_or(Error::ResourceNamespaceNotExists { name })?;
            let sko = resource.sko_application(client, namespace.as_str()).await?;
            serde_yaml::to_string(&sko).map_err(|e| {
                Error::FailedSerializeResource { internal: e.to_string() }
            })?
        }
        ObjectType::Session => {
            let resource = resource_from_yaml_file::<crate::spark_application::SparkSession>(file)?;
            let name = resource.name_any();
            let namespace: String = resource.namespace().ok_or(Error::ResourceNamespaceNotExists { name })?;
            let sko = resource.sko_application(client, namespace.as_str()).await?;
            serde_yaml::to_string(&sko).map_err(|e| {
                Error::FailedSerializeResource { internal: e.to_string() }
            })?          
        }
        ObjectType::ScheduledJob => {
            let resource = resource_from_yaml_file::<crate::spark_application::SparkScheduledJob>(file)?;
            let name = resource.name_any();
            let namespace: String = resource.namespace().ok_or(Error::ResourceNamespaceNotExists { name })?;
            let sko = resource.sko_application(client, namespace.as_str()).await?;
            serde_yaml::to_string(&sko).map_err(|e| {
                Error::FailedSerializeResource { internal: e.to_string() }
            })?
        }
    };
    println!("---");
    println!("{}", yaml);          
    Ok(())
}

#[cfg(test)]
mod tests {
    use schemars::{JsonSchema, gen::SchemaGenerator};

    #[test]
    fn generate_schema() {
        let mut gen = SchemaGenerator::default();
        let s = gen.into_root_schema_for::<crate::spark_application::SparkJob>();
        // crate::spark_application::SparkJob::json_schema(&mut gen);
        // println!("--- \n definitions: \n{:?}", s.definitions);
        // println!("--- \n meta_schema: \n{:?}", s.meta_schema.unwrap_or(String::default()));
        println!("--- \n schema: \n{:?}", s.schema);
    }
}