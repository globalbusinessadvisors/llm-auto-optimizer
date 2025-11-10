//! Distributed tracing with OpenTelemetry.
//!
//! This module provides trace provider initialization, span instrumentation,
//! and helpers for distributed tracing.

use crate::telemetry::resource::ResourceBuilder;
use opentelemetry::{
    global,
    trace::{SpanKind, Status, TraceError, Tracer, TracerProvider as _},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    runtime,
    trace::{
        BatchConfig, BatchSpanProcessor, RandomIdGenerator, Sampler, SpanLimits,
        TracerProvider,
    },
    Resource,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// OTLP protocol type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    /// gRPC protocol
    Grpc,
    /// HTTP protocol
    Http,
}

impl Default for OtlpProtocol {
    fn default() -> Self {
        Self::Grpc
    }
}

/// Sampling strategy for traces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SamplingStrategy {
    /// Always sample (sample all traces).
    AlwaysOn,
    /// Never sample (drop all traces).
    AlwaysOff,
    /// Sample based on trace ID ratio.
    TraceIdRatio { ratio: f64 },
    /// Parent-based sampling (follow parent's sampling decision).
    ParentBased {
        #[serde(flatten)]
        root: Box<SamplingStrategy>,
    },
}

impl Default for SamplingStrategy {
    fn default() -> Self {
        Self::ParentBased {
            root: Box::new(Self::TraceIdRatio { ratio: 1.0 }),
        }
    }
}

impl SamplingStrategy {
    /// Converts the strategy to an OpenTelemetry sampler.
    pub fn to_sampler(&self) -> Sampler {
        match self {
            Self::AlwaysOn => Sampler::AlwaysOn,
            Self::AlwaysOff => Sampler::AlwaysOff,
            Self::TraceIdRatio { ratio } => Sampler::TraceIdRatioBased(*ratio),
            Self::ParentBased { root } => Sampler::ParentBased(Box::new(root.to_sampler())),
        }
    }
}

/// Batch configuration for span export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfiguration {
    /// Maximum queue size for batching.
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
    /// Maximum batch size.
    #[serde(default = "default_max_export_batch_size")]
    pub max_export_batch_size: usize,
    /// Maximum time to wait before exporting.
    #[serde(with = "humantime_serde", default = "default_scheduled_delay")]
    pub scheduled_delay: Duration,
    /// Maximum time to wait for export to complete.
    #[serde(with = "humantime_serde", default = "default_export_timeout")]
    pub export_timeout: Duration,
}

fn default_max_queue_size() -> usize {
    2048
}

fn default_max_export_batch_size() -> usize {
    512
}

fn default_scheduled_delay() -> Duration {
    Duration::from_secs(5)
}

fn default_export_timeout() -> Duration {
    Duration::from_secs(30)
}

impl Default for BatchConfiguration {
    fn default() -> Self {
        Self {
            max_queue_size: default_max_queue_size(),
            max_export_batch_size: default_max_export_batch_size(),
            scheduled_delay: default_scheduled_delay(),
            export_timeout: default_export_timeout(),
        }
    }
}

impl From<BatchConfiguration> for BatchConfig {
    fn from(_config: BatchConfiguration) -> Self {
        // Note: BatchConfig API changed in opentelemetry_sdk 0.24
        // TODO: Update to use new BatchConfigBuilder API
        BatchConfig::default()
    }
}

/// Trace provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceConfig {
    /// Whether tracing is enabled.
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

    /// OTLP protocol (grpc or http).
    #[serde(default)]
    pub otlp_protocol: OtlpProtocol,

    /// Sampling strategy.
    #[serde(default)]
    pub sampling: SamplingStrategy,

    /// Batch configuration.
    #[serde(default)]
    pub batch_config: BatchConfiguration,

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

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            service_name: "llm-optimizer-processor".to_string(),
            service_version: default_service_version(),
            otlp_endpoint: default_otlp_endpoint(),
            otlp_protocol: OtlpProtocol::default(),
            sampling: SamplingStrategy::default(),
            batch_config: BatchConfiguration::default(),
            headers: HashMap::new(),
            environment: None,
        }
    }
}

/// Initializes the global trace provider with OTLP exporter.
///
/// # Errors
///
/// Returns an error if the trace provider cannot be initialized.
pub fn init_tracer_provider(config: TraceConfig) -> Result<TracerProvider, TraceError> {
    if !config.enabled {
        tracing::info!("OpenTelemetry tracing is disabled");
        return Err(TraceError::Other("tracing disabled".into()));
    }

    // Set up trace context propagator
    global::set_text_map_propagator(TraceContextPropagator::new());

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

    // Create OTLP exporter
    let exporter = match config.otlp_protocol {
        OtlpProtocol::Grpc => {
            let mut exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.otlp_endpoint);

            // Add custom headers
            if !config.headers.is_empty() {
                let mut metadata = tonic::metadata::MetadataMap::new();
                for (key, value) in &config.headers {
                    if let (Ok(k), Ok(v)) = (
                        key.parse::<tonic::metadata::MetadataKey<_>>(),
                        value.parse::<tonic::metadata::MetadataValue<_>>(),
                    ) {
                        metadata.insert(k, v);
                    }
                }
                exporter = exporter.with_metadata(metadata);
            }

            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(exporter)
                .with_trace_config(
                    opentelemetry_sdk::trace::Config::default()
                        .with_sampler(config.sampling.to_sampler())
                        .with_id_generator(RandomIdGenerator::default())
                        .with_resource(resource),
                )
                .with_batch_config(config.batch_config.into())
                .install_batch(runtime::Tokio)?
        }
        OtlpProtocol::Http => {
            // Note: HTTP exporter API changed in opentelemetry-otlp 0.17
            // For now, falling back to gRPC. TODO: Implement HTTP support with new API
            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(&config.otlp_endpoint),
                )
                .with_trace_config(
                    opentelemetry_sdk::trace::Config::default()
                        .with_sampler(config.sampling.to_sampler())
                        .with_id_generator(RandomIdGenerator::default())
                        .with_resource(resource),
                )
                .with_batch_config(config.batch_config.into())
                .install_batch(runtime::Tokio)?
        }
    };

    Ok(exporter)
}

/// Shuts down the global trace provider and flushes all pending spans.
pub async fn shutdown_tracer_provider() {
    global::shutdown_tracer_provider();
}

/// Span builder helper for creating instrumented spans.
pub struct SpanBuilder {
    name: String,
    kind: SpanKind,
    attributes: Vec<KeyValue>,
}

impl SpanBuilder {
    /// Creates a new span builder with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SpanKind::Internal,
            attributes: Vec::new(),
        }
    }

    /// Sets the span kind.
    pub fn with_kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    /// Adds an attribute to the span.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes
            .push(KeyValue::new(key.into(), value.into()));
        self
    }

    /// Adds multiple attributes to the span.
    pub fn with_attributes(mut self, attributes: Vec<KeyValue>) -> Self {
        self.attributes.extend(attributes);
        self
    }

    /// Starts the span using the global tracer.
    pub fn start(self) -> impl opentelemetry::trace::Span {
        let tracer = global::tracer("llm-optimizer-processor");
        let span = tracer
            .span_builder(self.name)
            .with_kind(self.kind)
            .with_attributes(self.attributes)
            .start(&tracer);
        span
    }

    /// Starts the span with a custom tracer.
    pub fn start_with_tracer<T: Tracer>(
        self,
        tracer: &T,
    ) -> T::Span {
        tracer
            .span_builder(self.name)
            .with_kind(self.kind)
            .with_attributes(self.attributes)
            .start(tracer)
    }
}

/// Helper macro for creating instrumented spans.
///
/// # Example
///
/// ```ignore
/// use crate::telemetry::span;
///
/// let span = span!("process_event", kind = SpanKind::Internal, "event.type" => "metric");
/// ```
#[macro_export]
macro_rules! span {
    ($name:expr) => {
        $crate::telemetry::SpanBuilder::new($name).start()
    };
    ($name:expr, kind = $kind:expr) => {
        $crate::telemetry::SpanBuilder::new($name)
            .with_kind($kind)
            .start()
    };
    ($name:expr, $($key:expr => $value:expr),+ $(,)?) => {
        {
            let mut builder = $crate::telemetry::SpanBuilder::new($name);
            $(
                builder = builder.with_attribute($key, $value);
            )+
            builder.start()
        }
    };
    ($name:expr, kind = $kind:expr, $($key:expr => $value:expr),+ $(,)?) => {
        {
            let mut builder = $crate::telemetry::SpanBuilder::new($name)
                .with_kind($kind);
            $(
                builder = builder.with_attribute($key, $value);
            )+
            builder.start()
        }
    };
}

/// Records an exception on a span.
pub fn record_exception<T: opentelemetry::trace::Span>(
    span: &mut T,
    error: &dyn std::error::Error,
) {
    span.record_error(error);
    span.set_status(Status::error(error.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampling_strategy_default() {
        let strategy = SamplingStrategy::default();
        let sampler = strategy.to_sampler();
        // Default should be parent-based with always-on root
        assert!(matches!(sampler, Sampler::ParentBased(_)));
    }

    #[test]
    fn test_sampling_strategy_always_on() {
        let strategy = SamplingStrategy::AlwaysOn;
        let sampler = strategy.to_sampler();
        assert!(matches!(sampler, Sampler::AlwaysOn));
    }

    #[test]
    fn test_sampling_strategy_trace_id_ratio() {
        let strategy = SamplingStrategy::TraceIdRatio { ratio: 0.5 };
        let sampler = strategy.to_sampler();
        assert!(matches!(sampler, Sampler::TraceIdRatioBased(_)));
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfiguration::default();
        assert_eq!(config.max_queue_size, 2048);
        assert_eq!(config.max_export_batch_size, 512);
    }

    #[test]
    fn test_trace_config_default() {
        let config = TraceConfig::default();
        assert!(config.enabled);
        assert_eq!(config.service_name, "llm-optimizer-processor");
        assert_eq!(config.otlp_endpoint, "http://localhost:4317");
        assert_eq!(config.otlp_protocol, OtlpProtocol::Grpc);
    }

    #[test]
    fn test_span_builder() {
        let builder = SpanBuilder::new("test-span")
            .with_kind(SpanKind::Server)
            .with_attribute("test.key", "test.value");

        assert_eq!(builder.name, "test-span");
        assert_eq!(builder.kind, SpanKind::Server);
        assert_eq!(builder.attributes.len(), 1);
    }

    #[test]
    fn test_otlp_protocol_serialization() {
        let grpc = OtlpProtocol::Grpc;
        let http = OtlpProtocol::Http;

        let grpc_json = serde_json::to_string(&grpc).unwrap();
        let http_json = serde_json::to_string(&http).unwrap();

        assert_eq!(grpc_json, "\"grpc\"");
        assert_eq!(http_json, "\"http\"");
    }
}
