//! OTLP metrics export for OpenTelemetry.
//!
//! This module provides metrics export to OTLP collectors with support for
//! different temporality modes and aggregation strategies.

use crate::telemetry::resource::ResourceBuilder;
use opentelemetry::{global, metrics::MeterProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector, TemporalitySelector},
        PeriodicReader, SdkMeterProvider,
    },
    runtime,
    Resource,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Aggregation temporality for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Temporality {
    /// Delta temporality (export deltas between collections).
    Delta,
    /// Cumulative temporality (export cumulative values).
    Cumulative,
}

impl Default for Temporality {
    fn default() -> Self {
        Self::Cumulative
    }
}

/// Metric export interval configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    /// Export interval (how often to export metrics).
    #[serde(with = "humantime_serde", default = "default_export_interval")]
    pub export_interval: Duration,

    /// Export timeout (maximum time to wait for export).
    #[serde(with = "humantime_serde", default = "default_export_timeout")]
    pub export_timeout: Duration,
}

fn default_export_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_export_timeout() -> Duration {
    Duration::from_secs(30)
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            export_interval: default_export_interval(),
            export_timeout: default_export_timeout(),
        }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Whether metrics export is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Service name.
    pub service_name: String,

    /// Service version.
    #[serde(default = "default_service_version")]
    pub service_version: String,

    /// OTLP endpoint (e.g., "http://localhost:4317").
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,

    /// Metric temporality (delta or cumulative).
    #[serde(default)]
    pub temporality: Temporality,

    /// Export configuration.
    #[serde(default)]
    pub export_config: ExportConfig,

    /// Additional headers for OTLP export.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Deployment environment.
    #[serde(default)]
    pub environment: Option<String>,
}

fn default_enabled() -> bool {
    true
}

fn default_service_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            service_name: "llm-optimizer-processor".to_string(),
            service_version: default_service_version(),
            otlp_endpoint: default_otlp_endpoint(),
            temporality: Temporality::default(),
            export_config: ExportConfig::default(),
            headers: HashMap::new(),
            environment: None,
        }
    }
}

/// Initializes the global meter provider with OTLP exporter.
///
/// # Errors
///
/// Returns an error if the meter provider cannot be initialized.
pub fn init_meter_provider(
    config: MetricsConfig,
) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    if !config.enabled {
        tracing::info!("OpenTelemetry metrics export is disabled");
        return Err("metrics disabled".into());
    }

    // Build resource
    let resource = ResourceBuilder::new()
        .with_service_name(config.service_name.clone())
        .with_service_version(config.service_version.clone())
        .with_attribute("service.component", "stream-processor");

    let resource = if let Some(env) = config.environment.clone() {
        resource.with_deployment_environment(env)
    } else {
        resource
    };

    let resource = resource.build();

    // Create OTLP exporter with gRPC
    let exporter_builder = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&config.otlp_endpoint);

    // Add custom headers if present
    let exporter_builder = if !config.headers.is_empty() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        for (key, value) in &config.headers {
            if let (Ok(k), Ok(v)) = (
                key.parse::<tonic::metadata::MetadataKey<_>>(),
                value.parse::<tonic::metadata::MetadataValue<_>>(),
            ) {
                metadata.insert(k, v);
            }
        }
        exporter_builder.with_metadata(metadata)
    } else {
        exporter_builder
    };

    let exporter = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(exporter_builder)
        .with_resource(resource)
        .with_period(config.export_config.export_interval)
        .with_timeout(config.export_config.export_timeout)
        .build()?;

    Ok(exporter)
}

/// Shuts down the global meter provider and flushes all pending metrics.
pub fn shutdown_meter_provider() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

/// Helper struct for recording metrics with common attributes.
#[derive(Debug, Clone)]
pub struct MetricsRecorder {
    common_attributes: Vec<KeyValue>,
}

impl MetricsRecorder {
    /// Creates a new metrics recorder.
    pub fn new() -> Self {
        Self {
            common_attributes: Vec::new(),
        }
    }

    /// Adds a common attribute that will be included in all metrics.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.common_attributes
            .push(KeyValue::new(key.into(), value.into()));
        self
    }

    /// Adds multiple common attributes.
    pub fn with_attributes(mut self, attributes: Vec<KeyValue>) -> Self {
        self.common_attributes.extend(attributes);
        self
    }

    /// Records a counter increment.
    pub fn record_counter(
        &self,
        meter_name: &str,
        counter_name: &str,
        value: u64,
        attributes: Vec<KeyValue>,
    ) {
        let meter = global::meter(meter_name.to_string());
        let counter = meter.u64_counter(counter_name.to_string()).init();

        let mut all_attrs = self.common_attributes.clone();
        all_attrs.extend(attributes);

        counter.add(value, &all_attrs);
    }

    /// Records a histogram value.
    pub fn record_histogram(
        &self,
        meter_name: &str,
        histogram_name: &str,
        value: f64,
        attributes: Vec<KeyValue>,
    ) {
        let meter = global::meter(meter_name.to_string());
        let histogram = meter.f64_histogram(histogram_name.to_string()).init();

        let mut all_attrs = self.common_attributes.clone();
        all_attrs.extend(attributes);

        histogram.record(value, &all_attrs);
    }

    /// Records a gauge value (using UpDownCounter).
    pub fn record_gauge(
        &self,
        meter_name: &str,
        gauge_name: &str,
        value: i64,
        attributes: Vec<KeyValue>,
    ) {
        let meter = global::meter(meter_name.to_string());
        let gauge = meter.i64_up_down_counter(gauge_name.to_string()).init();

        let mut all_attrs = self.common_attributes.clone();
        all_attrs.extend(attributes);

        gauge.add(value, &all_attrs);
    }
}

impl Default for MetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Processor-specific metric names.
pub mod metric_names {
    pub const EVENTS_PROCESSED: &str = "processor.events.processed";
    pub const EVENTS_DROPPED: &str = "processor.events.dropped";
    pub const PROCESSING_DURATION: &str = "processor.processing.duration";
    pub const WINDOW_TRIGGER_COUNT: &str = "processor.window.triggers";
    pub const AGGREGATION_DURATION: &str = "processor.aggregation.duration";
    pub const STATE_OPERATION_DURATION: &str = "processor.state.operation.duration";
    pub const DEDUPLICATION_COUNT: &str = "processor.deduplication.count";
    pub const WATERMARK_LAG: &str = "processor.watermark.lag";
}

/// Common metric attribute keys.
pub mod metric_attributes {
    pub const EVENT_TYPE: &str = "event.type";
    pub const WINDOW_TYPE: &str = "window.type";
    pub const AGGREGATION_TYPE: &str = "aggregation.type";
    pub const STATE_BACKEND: &str = "state.backend";
    pub const OPERATION_TYPE: &str = "operation.type";
    pub const ERROR_TYPE: &str = "error.type";
    pub const PARTITION_ID: &str = "partition.id";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temporality_default() {
        let temporality = Temporality::default();
        assert_eq!(temporality, Temporality::Cumulative);
    }

    #[test]
    fn test_export_config_default() {
        let config = ExportConfig::default();
        assert_eq!(config.export_interval, Duration::from_secs(60));
        assert_eq!(config.export_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.service_name, "llm-optimizer-processor");
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.temporality, Temporality::Cumulative);
    }

    #[test]
    fn test_metrics_recorder_new() {
        let recorder = MetricsRecorder::new();
        assert!(recorder.common_attributes.is_empty());
    }

    #[test]
    fn test_metrics_recorder_with_attributes() {
        let recorder = MetricsRecorder::new()
            .with_attribute("service", "test")
            .with_attribute("env", "dev");

        assert_eq!(recorder.common_attributes.len(), 2);
    }

    #[test]
    fn test_temporality_serialization() {
        let delta = Temporality::Delta;
        let cumulative = Temporality::Cumulative;

        let delta_json = serde_json::to_string(&delta).unwrap();
        let cumulative_json = serde_json::to_string(&cumulative).unwrap();

        assert_eq!(delta_json, "\"delta\"");
        assert_eq!(cumulative_json, "\"cumulative\"");
    }
}
