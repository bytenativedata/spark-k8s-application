// error definitions for crd
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to resolve OwnerReferences for resource [{name}]")]
    FailedResolveOwnerReferences { name: String },
    
    #[error("Failed to resolve Instance for resource [{name}]")]
    FailedResolveInstance { name: String },
        
    #[error("Failed to resolve the state of spark-on-k8s-operator applicaition [{name}]")]
    FailedResolveSKOResourceState { name: String },
    
    #[error("Resource name not exists")]
    ResourceNameNotExists,
    
    #[error("Resource namespace not exists [{name}]")]
    ResourceNamespaceNotExists { name: String },
    
    #[error("Spark Job or Session resource not exists [{name}]")]
    SparkJobOrSessionNotExists { name: String }, 
    
    #[error("Failed to patch Spark Job or Session resource [{name}]")]
    FailedPatchResource { name: String },
    
    #[error("Failed to deploy a SKO Resource [{name}]")]
    FailedDeploySKOResource { name: String },
    
    #[error("Failed to build a SKO Application from Spark Job or Session [{name}]: {source}")]
    FailedBuildSKOApplication { name: String, source: crd::Error },

    #[error("Failed to delete a SKO Resource [{name}]")]
    FailedDeleteSKOResource { name: String },
    
    #[error("Failed to create Service [{name}]")]
    FailedCreateService { name: String },
        
    #[error("Failed to create ConfigMap [{name}]")]
    FailedDeployConfigMap { name: String },

    #[error("Failed to resolve HS2 UI Service for Spark Session [{name}]")]
    FailedResolveHS2UIService { name: String },
    
    #[error("Failed to resolve HS2 Thrift Service for Spark Session [{name}]")]
    FailedResolveHS2ThriftService { name: String },
    
    #[error("Error from Crd: [{source}]")]
    CrdError { source: crd::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;