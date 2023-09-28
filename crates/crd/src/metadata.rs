use crate::{Error, Result};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::{Resource, ResourceExt};
use std::collections::BTreeMap;
use tracing::warn;

#[derive(Clone, Default)]
pub struct ObjectMetaBuilder {
    name: Option<String>,
    generate_name: Option<String>,
    namespace: Option<String>,
    ownerreference: Option<OwnerReference>,
    labels: Option<BTreeMap<String, String>>,
    annotations: Option<BTreeMap<String, String>>,
}

impl ObjectMetaBuilder {
    pub fn new() -> ObjectMetaBuilder {
        ObjectMetaBuilder::default()
    }

    /// This sets the name and namespace from a given resource
    pub fn name_and_namespace<T: Resource>(&mut self, resource: &T) -> &mut Self {
        self.name = Some(resource.name_any());
        self.namespace = resource.namespace();
        self
    }

    pub fn name_opt(&mut self, name: impl Into<Option<String>>) -> &mut Self {
        self.name = name.into();
        self
    }

    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn generate_name(&mut self, generate_name: impl Into<String>) -> &mut Self {
        self.generate_name = Some(generate_name.into());
        self
    }

    pub fn generate_name_opt(&mut self, generate_name: impl Into<Option<String>>) -> &mut Self {
        self.generate_name = generate_name.into();
        self
    }

    pub fn namespace_opt(&mut self, namespace: impl Into<Option<String>>) -> &mut Self {
        self.namespace = namespace.into();
        self
    }

    pub fn namespace(&mut self, namespace: impl Into<String>) -> &mut Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn ownerreference(&mut self, ownerreference: OwnerReference) -> &mut Self {
        self.ownerreference = Some(ownerreference);
        self
    }

    pub fn ownerreference_opt(&mut self, ownerreference: Option<OwnerReference>) -> &mut Self {
        self.ownerreference = ownerreference;
        self
    }

    /// This can be used to set the `OwnerReference` to the provided resource.
    pub fn ownerreference_from_resource<T: Resource<DynamicType = ()>>(
        &mut self,
        resource: &T,
        block_owner_deletion: Option<bool>,
        controller: Option<bool>,
    ) -> Result<&mut Self> {
        self.ownerreference = Some(
            OwnerReferenceBuilder::new()
                .initialize_from_resource(resource)
                .block_owner_deletion_opt(block_owner_deletion)
                .controller_opt(controller)
                .build()?,
        );
        Ok(self)
    }

    /// This adds a single annotation to the existing annotations.
    /// It'll override an annotation with the same key.
    pub fn with_annotation(
        &mut self,
        annotation_key: impl Into<String>,
        annotation_value: impl Into<String>,
    ) -> &mut Self {
        self.annotations
            .get_or_insert_with(BTreeMap::new)
            .insert(annotation_key.into(), annotation_value.into());
        self
    }

    /// This adds multiple annotations to the existing annotations.
    /// Any existing annotation with a key that is contained in `annotations` will be overwritten
    pub fn with_annotations(&mut self, annotations: BTreeMap<String, String>) -> &mut Self {
        self.annotations
            .get_or_insert_with(BTreeMap::new)
            .extend(annotations);
        self
    }

    /// This will replace all existing annotations
    pub fn annotations(&mut self, annotations: BTreeMap<String, String>) -> &mut Self {
        self.annotations = Some(annotations);
        self
    }

    /// This adds a single label to the existing labels.
    /// It'll override a label with the same key.
    pub fn with_label(
        &mut self,
        label_key: impl Into<String>,
        label_value: impl Into<String>,
    ) -> &mut Self {
        self.labels
            .get_or_insert_with(BTreeMap::new)
            .insert(label_key.into(), label_value.into());
        self
    }

    /// This adds multiple labels to the existing labels.
    /// Any existing label with a key that is contained in `labels` will be overwritten
    pub fn with_labels(&mut self, labels: BTreeMap<String, String>) -> &mut Self {
        self.labels.get_or_insert_with(BTreeMap::new).extend(labels);
        self
    }

    /// This will replace all existing labels
    pub fn labels(&mut self, labels: BTreeMap<String, String>) -> &mut Self {
        self.labels = Some(labels);
        self
    }

    pub fn with_recommended_labels<T: Resource>(
        &mut self,
        object_labels: ObjectLabels<T>,
    ) -> &mut Self {
        let recommended_labels = get_recommended_labels(object_labels);
        self.labels
            .get_or_insert_with(BTreeMap::new)
            .extend(recommended_labels);
        self
    }

    pub fn build(&self) -> ObjectMeta {
        // if 'generate_name' and 'name' are set, Kubernetes will prioritize the 'name' field and
        // 'generate_name' has no impact.
        if let (Some(name), Some(generate_name)) = (&self.name, &self.generate_name) {
            warn!(
                "ObjectMeta has a 'name' [{}] and 'generate_name' [{}] field set. Kubernetes \
		         will prioritize the 'name' field over 'generate_name'.",
                name, generate_name
            );
        }

        ObjectMeta {
            generate_name: self.generate_name.clone(),
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            owner_references: self
                .ownerreference
                .as_ref()
                .map(|ownerreference| vec![ownerreference.clone()]),
            labels: self.labels.clone(),
            annotations: self.annotations.clone(),
            ..ObjectMeta::default()
        }
    }
}

#[derive(Clone, Default)]
pub struct OwnerReferenceBuilder {
    api_version: Option<String>,
    block_owner_deletion: Option<bool>,
    controller: Option<bool>,
    kind: Option<String>,
    name: Option<String>,
    uid: Option<String>,
}

impl OwnerReferenceBuilder {
    pub fn new() -> OwnerReferenceBuilder {
        OwnerReferenceBuilder::default()
    }

    pub fn api_version(&mut self, api_version: impl Into<String>) -> &mut Self {
        self.api_version = Some(api_version.into());
        self
    }

    pub fn api_version_opt(&mut self, api_version: impl Into<Option<String>>) -> &mut Self {
        self.api_version = api_version.into();
        self
    }

    pub fn block_owner_deletion(&mut self, block_owner_deletion: bool) -> &mut Self {
        self.block_owner_deletion = Some(block_owner_deletion);
        self
    }

    pub fn block_owner_deletion_opt(&mut self, block_owner_deletion: Option<bool>) -> &mut Self {
        self.block_owner_deletion = block_owner_deletion;
        self
    }

    pub fn controller(&mut self, controller: bool) -> &mut Self {
        self.controller = Some(controller);
        self
    }

    pub fn controller_opt(&mut self, controller: Option<bool>) -> &mut Self {
        self.controller = controller;
        self
    }

    pub fn kind(&mut self, kind: impl Into<String>) -> &mut Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn kind_opt(&mut self, kind: impl Into<Option<String>>) -> &mut Self {
        self.kind = kind.into();
        self
    }

    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn name_opt(&mut self, name: impl Into<Option<String>>) -> &mut Self {
        self.name = name.into();
        self
    }

    pub fn uid(&mut self, uid: impl Into<String>) -> &mut Self {
        self.uid = Some(uid.into());
        self
    }

    pub fn uid_opt(&mut self, uid: impl Into<Option<String>>) -> &mut Self {
        self.uid = uid.into();
        self
    }

    pub fn initialize_from_resource<T: Resource<DynamicType = ()>>(
        &mut self,
        resource: &T,
    ) -> &mut Self {
        self.api_version(T::api_version(&()))
            .kind(T::kind(&()))
            .name(resource.name_any())
            .uid_opt(resource.meta().uid.clone());
        self
    }

    pub fn build(&self) -> Result<OwnerReference> {
        Ok(OwnerReference {
            api_version: match self.api_version {
                None => return Err(Error::MissingObjectKey { key: "api_version" }),
                Some(ref api_version) => api_version.clone(),
            },
            block_owner_deletion: self.block_owner_deletion,
            controller: self.controller,
            kind: match self.kind {
                None => return Err(Error::MissingObjectKey { key: "kind" }),
                Some(ref kind) => kind.clone(),
            },
            name: match self.name {
                None => return Err(Error::MissingObjectKey { key: "name" }),
                Some(ref name) => name.clone(),
            },
            uid: match self.uid {
                None => return Err(Error::MissingObjectKey { key: "uid" }),
                Some(ref uid) => uid.clone(),
            },
        })
    }
}

// ----

const _APP_KUBERNETES_LABEL_BASE: &str = "app.kubernetes.io/";

/// The name of the application 
pub const APP_NAME_LABEL: &str = concat!("app.kubernetes.io/", "name");
/// A unique name identifying the instance of an application 
pub const APP_INSTANCE_LABEL: &str = concat!("app.kubernetes.io/", "instance");
/// The current version of the application
pub const APP_VERSION_LABEL: &str = concat!("app.kubernetes.io/", "version");
/// The component within the architecture 
pub const APP_COMPONENT_LABEL: &str = concat!("app.kubernetes.io/", "component");
/// The name of a higher level application this one is part 
pub const APP_PART_OF_LABEL: &str = concat!("app.kubernetes.io/", "part-of");
/// The tool being used to manage the operation of an application
pub const APP_MANAGED_BY_LABEL: &str = concat!("app.kubernetes.io/", "managed-by");
pub const APP_ROLE_GROUP_LABEL: &str = concat!("app.kubernetes.io/", "role-group");

#[derive(Debug, Clone, Copy)]
pub struct ObjectLabels<'a, T> {
    pub owner: &'a T,
    pub app_name: &'a str,
    pub app_version: &'a str,
    /// The DNS-style name of the operator managing the object
    pub operator_name: &'a str,
    pub controller_name: &'a str,
    pub role: &'a str,
    pub role_group: &'a str,
}

pub fn get_recommended_labels<T>(
    ObjectLabels {
        owner,
        app_name,
        app_version,
        operator_name,
        controller_name,
        role,
        role_group,
    }: ObjectLabels<T>,
) -> BTreeMap<String, String>
where
    T: Resource,
{
    let mut labels = role_group_selector_labels(owner, app_name, role, role_group);

    labels.insert(APP_VERSION_LABEL.to_string(), app_version.to_string());
    labels.insert(
        APP_MANAGED_BY_LABEL.to_string(),
        format!("{}_{}", operator_name, controller_name),
    );

    labels
}

pub fn role_group_selector_labels<T: Resource>(
    owner: &T,
    app_name: &str,
    role: &str,
    role_group: &str,
) -> BTreeMap<String, String> {
    let mut labels = role_selector_labels(owner, app_name, role);
    labels.insert(APP_ROLE_GROUP_LABEL.to_string(), role_group.to_string());
    labels
}


pub fn role_selector_labels<T: Resource>(
    owner: &T,
    app_name: &str,
    role: &str,
) -> BTreeMap<String, String> {
    let mut labels = build_common_labels_for_all_managed_resources(app_name, &owner.name_any());
    labels.insert(APP_COMPONENT_LABEL.to_string(), role.to_string());
    labels
}

pub fn build_common_labels_for_all_managed_resources(
    app_name: &str,
    owner_name: &str,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(APP_NAME_LABEL.to_string(), app_name.to_string());
    labels.insert(APP_INSTANCE_LABEL.to_string(), owner_name.to_string());
    labels
}
