//! Resource attributes and detection for OpenTelemetry.
//!
//! This module provides utilities for creating and managing resource attributes
//! that describe the service and environment.

use opentelemetry::KeyValue;
use opentelemetry_sdk::resource::{
    EnvResourceDetector, Resource,
    SdkProvidedResourceDetector,
};
use opentelemetry_semantic_conventions::resource::{
    DEPLOYMENT_ENVIRONMENT, HOST_ARCH, HOST_NAME, OS_TYPE, PROCESS_COMMAND_ARGS,
    PROCESS_EXECUTABLE_NAME, PROCESS_EXECUTABLE_PATH, PROCESS_OWNER, PROCESS_PID,
    PROCESS_RUNTIME_DESCRIPTION, PROCESS_RUNTIME_NAME, PROCESS_RUNTIME_VERSION,
    SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_VERSION,
};
use std::collections::HashMap;
use uuid::Uuid;

/// Resource builder for creating OpenTelemetry resources with semantic conventions.
#[derive(Debug, Clone)]
pub struct ResourceBuilder {
    service_name: Option<String>,
    service_version: Option<String>,
    service_instance_id: Option<String>,
    deployment_environment: Option<String>,
    additional_attributes: HashMap<String, String>,
}

impl ResourceBuilder {
    /// Creates a new resource builder.
    pub fn new() -> Self {
        Self {
            service_name: None,
            service_version: None,
            service_instance_id: None,
            deployment_environment: None,
            additional_attributes: HashMap::new(),
        }
    }

    /// Sets the service name.
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Sets the service version.
    pub fn with_service_version(mut self, version: impl Into<String>) -> Self {
        self.service_version = Some(version.into());
        self
    }

    /// Sets the service instance ID.
    pub fn with_service_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.service_instance_id = Some(instance_id.into());
        self
    }

    /// Sets the deployment environment (e.g., "production", "staging", "development").
    pub fn with_deployment_environment(mut self, env: impl Into<String>) -> Self {
        self.deployment_environment = Some(env.into());
        self
    }

    /// Adds a custom attribute.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.additional_attributes.insert(key.into(), value.into());
        self
    }

    /// Adds multiple custom attributes.
    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.additional_attributes.extend(attributes);
        self
    }

    /// Builds the resource with automatic detection of system resources.
    pub fn build(self) -> Resource {
        let mut key_values = Vec::new();

        // Service attributes
        if let Some(name) = self.service_name {
            key_values.push(KeyValue::new(SERVICE_NAME, name));
        }

        if let Some(version) = self.service_version {
            key_values.push(KeyValue::new(SERVICE_VERSION, version));
        }

        // Generate instance ID if not provided
        let instance_id = self
            .service_instance_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        key_values.push(KeyValue::new(SERVICE_INSTANCE_ID, instance_id));

        if let Some(env) = self.deployment_environment {
            key_values.push(KeyValue::new(DEPLOYMENT_ENVIRONMENT, env));
        }

        // Add custom attributes
        for (key, value) in self.additional_attributes {
            key_values.push(KeyValue::new(key, value));
        }

        // Create base resource
        let base_resource = Resource::new(key_values);

        // Merge with detected resources (Note: ResourceDetector API changed in 0.24)
        // For now, just return base resource without auto-detection
        // TODO: Update to use new opentelemetry_sdk 0.24 resource detection API
        base_resource
    }

    /// Builds the resource without automatic detection (manual attributes only).
    pub fn build_without_detection(self) -> Resource {
        let mut key_values = Vec::new();

        if let Some(name) = self.service_name {
            key_values.push(KeyValue::new(SERVICE_NAME, name));
        }

        if let Some(version) = self.service_version {
            key_values.push(KeyValue::new(SERVICE_VERSION, version));
        }

        let instance_id = self
            .service_instance_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        key_values.push(KeyValue::new(SERVICE_INSTANCE_ID, instance_id));

        if let Some(env) = self.deployment_environment {
            key_values.push(KeyValue::new(DEPLOYMENT_ENVIRONMENT, env));
        }

        for (key, value) in self.additional_attributes {
            key_values.push(KeyValue::new(key, value));
        }

        Resource::new(key_values)
    }
}

impl Default for ResourceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a default resource for the stream processor service.
pub fn create_processor_resource(
    service_name: Option<String>,
    service_version: Option<String>,
    environment: Option<String>,
) -> Resource {
    let mut builder = ResourceBuilder::new();

    if let Some(name) = service_name {
        builder = builder.with_service_name(name);
    } else {
        builder = builder.with_service_name("llm-optimizer-processor");
    }

    if let Some(version) = service_version {
        builder = builder.with_service_version(version);
    } else {
        builder = builder.with_service_version(env!("CARGO_PKG_VERSION"));
    }

    if let Some(env) = environment {
        builder = builder.with_deployment_environment(env);
    }

    // Add processor-specific attributes
    builder = builder.with_attribute("service.component", "stream-processor");

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_builder_basic() {
        let resource = ResourceBuilder::new()
            .with_service_name("test-service")
            .with_service_version("1.0.0")
            .with_deployment_environment("test")
            .build_without_detection();

        // Verify service name is set
        let attributes: Vec<_> = resource.iter().collect();
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == SERVICE_NAME && v.as_str() == "test-service"));
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == SERVICE_VERSION && v.as_str() == "1.0.0"));
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == DEPLOYMENT_ENVIRONMENT && v.as_str() == "test"));
    }

    #[test]
    fn test_resource_builder_custom_attributes() {
        let resource = ResourceBuilder::new()
            .with_service_name("test")
            .with_attribute("custom.key", "custom.value")
            .build_without_detection();

        let attributes: Vec<_> = resource.iter().collect();
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == "custom.key" && v.as_str() == "custom.value"));
    }

    #[test]
    fn test_resource_builder_auto_instance_id() {
        let resource = ResourceBuilder::new()
            .with_service_name("test")
            .build_without_detection();

        let attributes: Vec<_> = resource.iter().collect();
        // Instance ID should be auto-generated
        assert!(attributes
            .iter()
            .any(|(k, _v)| k.as_str() == SERVICE_INSTANCE_ID));
    }

    #[test]
    fn test_create_processor_resource_defaults() {
        let resource = create_processor_resource(None, None, None);

        let attributes: Vec<_> = resource.iter().collect();
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == SERVICE_NAME && v.as_str() == "llm-optimizer-processor"));
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == "service.component"
                && v.as_str() == "stream-processor"));
    }

    #[test]
    fn test_create_processor_resource_custom() {
        let resource = create_processor_resource(
            Some("custom-processor".to_string()),
            Some("2.0.0".to_string()),
            Some("production".to_string()),
        );

        let attributes: Vec<_> = resource.iter().collect();
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == SERVICE_NAME && v.as_str() == "custom-processor"));
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == SERVICE_VERSION && v.as_str() == "2.0.0"));
        assert!(attributes
            .iter()
            .any(|(k, v)| k.as_str() == DEPLOYMENT_ENVIRONMENT && v.as_str() == "production"));
    }
}
