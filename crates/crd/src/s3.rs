use std::collections::HashMap;

use k8s_openapi::api::core::v1::{
    KeyToPath, SecretVolumeSource, Volume, EnvVar, EnvVarSource, SecretKeySelector,
};
use kube::{Api, Client, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::{ Display, EnumString };

use crate::{Error, Result};

/// S3 bucket specification containing only the bucket name and an inlined or
/// referenced connection specification.
#[derive(
    Clone, CustomResource, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize,
)]
#[kube(
    group = "s3.bytenative.com",
    version = "v1alpha1",
    kind = "S3Bucket",
    plural = "s3buckets",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct S3BucketSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<S3ConnectionDef>,
}

impl S3BucketSpec {
    /// Convenience function to retrieve the spec of a S3 bucket resource from the K8S API service.
    pub async fn get(
        resource_name: &str,
        client: &Client,
        namespace: &str,
    ) -> Result<S3BucketSpec> {
        let s3bucket_api = Api::<S3Bucket>::namespaced(client.clone(), namespace);
        let s3bucket = s3bucket_api.get(resource_name)
            .await.map_err(|_| {
                Error::MissingS3Bucket { name: resource_name.to_string() }
            })?;

        Ok(s3bucket.spec)
    }

    /// Map &self to an [InlinedS3BucketSpec] by obtaining connection spec from the K8S API service if necessary
    pub async fn inlined(&self, client: &Client, namespace: &str) -> Result<InlinedS3BucketSpec> {
        match self.connection.as_ref() {
            Some(connection_def) => Ok(InlinedS3BucketSpec {
                connection: Some(connection_def.resolve(client, namespace).await?),
                bucket_name: self.bucket_name.clone(),
            }),
            None => Ok(InlinedS3BucketSpec {
                bucket_name: self.bucket_name.clone(),
                connection: None,
            }),
        }
    }
}

/// Convenience struct with the connection spec inlined.
pub struct InlinedS3BucketSpec {
    pub bucket_name: Option<String>,
    pub connection: Option<S3ConnectionSpec>,
}

impl InlinedS3BucketSpec {
    /// Build the endpoint URL from [S3ConnectionSpec::host] and [S3ConnectionSpec::port] and the S3 implementation to use
    pub fn endpoint(&self) -> Option<String> {
        self.connection
            .as_ref()
            .and_then(|connection| connection.endpoint())
    }
}

/// Operators are expected to define fields for this type in order to work with S3 buckets.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum S3BucketDef {
    Inline(S3BucketSpec),
    Reference(String),
}

impl S3BucketDef {
    /// Returns an [InlinedS3BucketSpec].
    pub async fn resolve(&self, client: &Client, namespace: &str) -> Result<InlinedS3BucketSpec> {
        match self {
            S3BucketDef::Inline(s3_bucket) => s3_bucket.inlined(client, namespace).await,
            S3BucketDef::Reference(s3_bucket) => {
                S3BucketSpec::get(s3_bucket.as_str(), client, namespace)
                    .await?
                    .inlined(client, namespace)
                    .await
            }
        }
    }
}

/// Operators are expected to define fields for this type in order to work with S3 connections.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum S3ConnectionDef {
    Inline(S3ConnectionSpec),
    Reference(String),
}

impl S3ConnectionDef {
    /// Returns an [S3ConnectionSpec].
    pub async fn resolve(&self, client: &Client, namespace: &str) -> Result<S3ConnectionSpec> {
        match self {
            S3ConnectionDef::Inline(s3_connection_spec) => Ok(s3_connection_spec.clone()),
            S3ConnectionDef::Reference(s3_conn_reference) => {
                S3ConnectionSpec::get(s3_conn_reference, client, namespace).await
            }
        }
    }
}

/// Operators are expected to define fields for this type in order to work with S3 connections.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineS3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum S3Credentials {
    Secret(String),
    Inline(InlineS3Credentials),
    Anonymous(String),
}

use crate::constants::{S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SECRET_DIR_NAME};
impl S3Credentials {
    pub fn spark_configs(&self) -> HashMap<String, String> {
        match self {
            S3Credentials::Secret(secret) => {
                let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret}");
                HashMap::from([
                    (
                        "spark.hadoop.fs.s3a.aws.credentials.provider".to_string(),
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string(),
                    ),
                    (
                        "spark.hadoop.fs.s3a.access.key".to_string(),
                        format!("${{cat {secret_dir}/{S3_ACCESS_KEY_ID}}}"),
                    ),
                    (
                        "spark.hadoop.fs.s3a.secret.key".to_string(),
                        format!("${{cat {secret_dir}/{S3_SECRET_ACCESS_KEY}}}"),
                    ),
                ])
            }
            S3Credentials::Inline(inline) => {
                HashMap::from([
                    (
                        "spark.hadoop.fs.s3a.aws.credentials.provider".to_string(),
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string(),
                    ),
                    (
                        "spark.hadoop.fs.s3a.access.key".to_string(),
                        inline.access_key.clone(),
                    ),
                    (
                        "spark.hadoop.fs.s3a.secret.key".to_string(),
                        inline.secret_key.clone(),
                    ),
                ])
            }
            S3Credentials::Anonymous(_) => Self::spark_configs_anonymous(),
        }
    }
    
    pub fn env_vars(&self) -> Option<Vec<EnvVar>> {
        match self {
            S3Credentials::Secret(secret) => {
                Some(vec![
                    EnvVar {
                        name: "AWS_ACCESS_KEY_ID".to_string(),
                        value_from: Some(EnvVarSource { 
                            secret_key_ref: Some(SecretKeySelector {
                                key: S3_ACCESS_KEY_ID.to_string(),
                                name: Some(secret.clone()), 
                                optional: Some(true) }),
                            ..Default::default()
                        }),
                        value: None
                    },
                    EnvVar {
                        name: "AWS_SECRET_ACCESS_KEY".to_string(),
                        value_from: Some(EnvVarSource { 
                            secret_key_ref: Some(SecretKeySelector {
                                key: S3_SECRET_ACCESS_KEY.to_string(),
                                name: Some(secret.clone()), 
                                optional: Some(true) }),
                            ..Default::default()
                        }),
                        value: None
                    },
                ])
            }
            S3Credentials::Inline(inline) => {
                let InlineS3Credentials { access_key, secret_key } = inline.clone();
                Some(vec![
                    EnvVar {
                        name: "AWS_ACCESS_KEY_ID".to_string(),
                        value_from: None,
                        value: Some(access_key)
                    },
                    EnvVar {
                        name: "AWS_SECRET_ACCESS_KEY".to_string(),
                        value_from: None,
                        value: Some(secret_key)
                    },
                ])
            }
            S3Credentials::Anonymous(_) => None
        }
    }

    pub fn spark_configs_anonymous() -> HashMap<String, String> {
        HashMap::from([(
            "spark.hadoop.fs.s3a.aws.credentials.provider".to_string(),
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string(),
        )])
    }

    pub fn secret_volume(&self, volume_name: &str) -> Option<Volume> {
        Some(k8s_openapi::api::core::v1::Volume {
            name: volume_name.to_string(),
            // TODO: check make a SecretOperatorVolumeSource
            secret: Some(SecretVolumeSource {
                secret_name: Some(volume_name.to_string()),
                default_mode: Some(0o420),
                items: Some(vec![
                    KeyToPath {
                        key: S3_ACCESS_KEY_ID.to_string(),
                        path: format!("{S3_SECRET_DIR_NAME}/{S3_ACCESS_KEY_ID}"),
                        mode: Some(0o420),
                    },
                    KeyToPath {
                        key: S3_SECRET_ACCESS_KEY.to_string(),
                        path: format!("{S3_SECRET_DIR_NAME}/{S3_SECRET_ACCESS_KEY}"),
                        mode: Some(0o420),
                    },
                ]),
                optional: Some(true),
            }),
            ..Volume::default()
        })
    }

    pub fn secret_mount(&self) -> Option<crate::spark_application::SecretInfo> {
        match self {
            S3Credentials::Secret(name) => {
                Some(crate::spark_application::SecretInfo {
                    name: name.clone(),
                    path: S3_SECRET_DIR_NAME.to_string(),
                    typ: "Generic".to_string(),
                })
            }
            _ => None
        }
    }

    pub fn volume_mounts(&self) -> Option<k8s_openapi::api::core::v1::VolumeMount> {
        match self {
            S3Credentials::Secret(secret) => {
                let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret}");
                Some(k8s_openapi::api::core::v1::VolumeMount {
                    name: secret.to_string(),
                    mount_path: secret_dir,
                    read_only: Some(true),
                    ..k8s_openapi::api::core::v1::VolumeMount::default()
                })
            }
            _ => None,
        }
    }
}

/// S3 connection definition as CRD.
#[derive(
    CustomResource, Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize,
)]
#[kube(
    group = "s3.bytenative.com",
    version = "v1alpha1",
    kind = "S3Connection",
    plural = "s3connections",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct S3ConnectionSpec {
    /// Hostname of the S3 server without any protocol or port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Port the S3 server listens on.
    /// If not specified the products will determine the port to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Which access style to use.
    /// Defaults to virtual hosted-style as most of the data products out there.
    /// Have a look at the official documentation on <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_style: Option<S3AccessStyle>,
    /// If the S3 uses authentication you have to specify you S3 credentials.
    /// In the most cases a SecretClass providing `accessKey` and `secretKey` is sufficient.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_inline: Option<InlineS3Credentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<S3Credentials>,
    /// If you want to use TLS when talking to S3 you can enable TLS encrypted communication with this setting.
    /// the String value is the name of secret or `None` that means no TLS validation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    // pub tls: Option<Tls>,
    pub tls: Option<String>,
}

impl S3ConnectionSpec {
    /// Convenience function to retrieve the spec of a S3 connection resource from the K8S API service.
    pub async fn get(
        resource_name: &str,
        client: &Client,
        namespace: &str,
    ) -> Result<S3ConnectionSpec> {
        let s3conn_api = Api::<S3Connection>::namespaced(client.clone(), namespace);
        let s3conn = s3conn_api.get(resource_name).await.map_err(|_| {
            Error::MissingS3Connection { name: resource_name.to_string() }
        })?;

        Ok(s3conn.spec)
    }

    /// Build the endpoint URL from this connection
    pub fn endpoint(&self) -> Option<String> {
        let protocol = match self.tls.as_ref() {
            Some(_tls) => "https",
            _ => "http",
        };
        self.host.as_ref().map(|h| match self.port {
            Some(p) => format!("{protocol}://{h}:{p}"),
            None => format!("{protocol}://{h}"),
        })
    }

    pub fn spark_configs(&self) -> HashMap<String, String> {
        let ret = HashMap::from([
            (
                "spark.hadoop.fs.s3a.endpoint".to_string(),
                self.endpoint().unwrap(),
            ),
            (
                "spark.hadoop.fs.s3a.impl".to_string(),
                "org.apache.hadoop.fs.s3a.S3AFileSystem".to_string(),
            ),
            (
                "spark.hadoop.fs.s3a.path.style.access".to_string(),
                (self.access_style == Some(S3AccessStyle::Path)).to_string(),
            ),
        ]);
        // TODO: how to use it with secret volume
        /*
        let ext = self.credentials.as_ref().map_or_else(
            S3Credentials::spark_configs_anonymous, |c| c.spark_configs(),
        );
        ret.extend(ext);
        */
        ret
    }
}

#[derive(
    Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Display, EnumString,
)]
pub enum S3AccessStyle {
    /// Use path-style access as described in <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#path-style-access>
    Path,
    /// Use as virtual hosted-style access as described in <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#virtual-hosted-style-access>
    #[default]
    VirtualHosted,
}

#[cfg(test)]
mod test {
    use std::str;

    use crate::s3::{S3AccessStyle, S3ConnectionDef};
    use crate::s3::{S3BucketSpec, S3ConnectionSpec};
    use serde_yaml;

    #[test]
    fn test_ser_inline() {
        let bucket = S3BucketSpec {
            bucket_name: Some("test-bucket-name".to_owned()),
            connection: Some(S3ConnectionDef::Inline(S3ConnectionSpec {
                host: Some("host".to_owned()),
                port: Some(8080),
                credentials: None,
                access_style: Some(S3AccessStyle::VirtualHosted),
                tls: None,
                ..Default::default()
            })),
        };

        let actual_yaml = serde_yaml::to_string(&bucket).expect("Yaml serialization Bucket");
        let actual_yaml = str::from_utf8(actual_yaml.as_bytes()).expect("UTF-8 encoded document");

        let expected_yaml = "---
bucketName: test-bucket-name
connection:
  inline:
    host: host
    port: 8080
    accessStyle: VirtualHosted
";

        assert_eq!(expected_yaml, actual_yaml)
    }
}
