//! Stream processor for LLM Auto-Optimizer
//!
//! This crate provides stream processing capabilities for handling events,
//! metrics, and feedback in the optimizer system.

pub mod actuator;
pub mod aggregation;
pub mod analyzer;
pub mod config;
pub mod core;
pub mod decision;
pub mod deduplication;
pub mod error;
pub mod kafka;
pub mod metrics;
pub mod normalization;
pub mod pipeline;
pub mod state;
pub mod storage;
pub mod telemetry;
pub mod watermark;
pub mod window;
pub mod stream_processor;

// Re-export commonly used types
pub use core::{
    CloudEventKeyExtractor, CloudEventTimeExtractor, EventKey, EventTimeExtractor,
    FeedbackEventKeyExtractor, FeedbackEventTimeExtractor, KeyExtractor,
    MetricPointKeyExtractor, MetricPointTimeExtractor, ProcessorEvent,
};

pub use window::{
    Window, WindowBounds, WindowType, WindowAssigner, WindowTrigger, TriggerResult, TriggerContext,
    WindowMerger, TumblingWindowAssigner, SlidingWindowAssigner, SessionWindowAssigner,
    OnWatermarkTrigger, ProcessingTimeTrigger, CountTrigger, CompositeTrigger, CompositeMode,
    ImmediateTrigger, NeverTrigger, WindowManager, WindowMetadata, WindowStats,
};

pub use stream_processor::{
    StreamProcessor, StreamProcessorBuilder, StreamProcessorConfig, WindowResult, ProcessorStats,
};

pub use error::{
    ProcessorError, WindowError, AggregationError, StateError, WatermarkError,
    Result as ProcessorResult,
};

pub use config::{
    ProcessorConfig, WindowConfig, WindowType as ConfigWindowType,
    WatermarkConfig, WatermarkStrategy, StateConfig, StateBackend,
    AggregationConfig, AggregationType,
    DeduplicationStrategy,
};

pub use normalization::{
    TimeSeriesNormalizer, NormalizerBuilder, NormalizationConfig, FillStrategy,
    TimeSeriesEvent, NormalizationStats, NormalizationError, NormalizationResult,
};

pub use pipeline::{
    StreamPipelineBuilder, StreamPipeline, PipelineConfig,
    StreamExecutor, ExecutorStats,
    StreamOperator, MapOperator, FilterOperator, FlatMapOperator,
    KeyByOperator, WindowOperator, AggregateOperator, DeduplicationOperator,
    OperatorContext, DeduplicationStats as OperatorDeduplicationStats,
};

pub use kafka::{
    // Offset management
    OffsetManager, OffsetStore, InMemoryOffsetStore, StateBackendOffsetStore,
    TopicPartition, OffsetInfo, OffsetCommitStrategy, OffsetResetStrategy,
    OffsetStats,
    // Source (consumer)
    CommitStrategy, KafkaSource, KafkaSourceConfig, KafkaSourceMetrics,
    SourceMessage,
    // Sink (producer)
    BincodeSerializer, DeliveryGuarantee, JsonSerializer, KafkaSink, KafkaSinkConfig,
    MessageSerializer, PartitionStrategy, SinkMessage, SinkMetrics,
};

pub use deduplication::{
    DeduplicationConfig, DeduplicationConfigBuilder, DeduplicationStats,
    EventDeduplicator, EventIdExtractor,
};

pub use metrics::{
    LabelSet, LabelValue, MetricLabels,
    ProcessorMetrics, MetricsSnapshot,
    MetricsRegistry, METRICS_REGISTRY,
    MetricsServer, MetricsServerConfig, HealthStatus,
    MetricsError, Result as MetricsResult,
};

pub use telemetry::{
    // Configuration
    TelemetryConfig, TraceConfig, MetricsConfig as TelemetryMetricsConfig,
    BatchConfiguration, ExportConfig, OtlpProtocol, SamplingStrategy, Temporality,
    // Initialization
    init_telemetry, init_tracer_provider, init_meter_provider,
    shutdown_tracer_provider, shutdown_meter_provider, TelemetryProviders,
    // Tracing
    SpanBuilder, record_exception,
    // Context propagation
    HeaderCarrier, BaggageItem, context_with_span, extract_context,
    extract_from_headers, inject_context, inject_into_headers,
    get_baggage, propagate_context, with_baggage, with_context,
    // Metrics
    MetricsRecorder, metric_names, metric_attributes,
    // Resources
    ResourceBuilder, create_processor_resource,
    // Re-exports
    global as otel_global, Context as OtelContext, KeyValue as OtelKeyValue,
    Span as OtelSpan, SpanKind, Status as OtelStatus, Tracer as OtelTracer,
};

pub use analyzer::{
    // Core traits and types
    Analyzer, AnalyzerConfig, AnalyzerConfigBuilder, AnalyzerError, AnalyzerResult, AnalyzerState,
    AnalyzerEvent, AnalysisReport, AnalyzerStats, Insight, Recommendation, Alert,
    // Analyzer implementations
    PerformanceAnalyzer, PerformanceAnalyzerConfig,
    CostAnalyzer, CostAnalyzerConfig, ModelPricing,
    QualityAnalyzer, QualityAnalyzerConfig,
    PatternAnalyzer, PatternAnalyzerConfig, PatternType,
    AnomalyAnalyzer, AnomalyAnalyzerConfig, AnomalyType,
    // Common types
    Severity, Confidence, Priority, InsightCategory,
};

pub use decision::{
    // Core traits
    DecisionEngine, OptimizationStrategy, DecisionValidator, DecisionExecutor,
    // Decision Coordinator
    DecisionCoordinator,
    // Strategies (all 5 optimization strategies)
    ModelSelectionStrategy, CachingStrategy, RateLimitingStrategy,
    BatchingStrategy, PromptOptimizationStrategy,
    // Configuration
    DecisionEngineConfig, StrategyConfigs,
    ModelSelectionConfig, CachingConfig, RateLimitingConfig,
    BatchingConfig, PromptOptimizationConfig, SafetyConfig, MonitoringConfig,
    // Types
    Decision, DecisionInput, DecisionOutcome, DecisionCriteria, DecisionStats,
    DecisionType, ExpectedImpact, ActualImpact, ConfigChange, ConfigType,
    RollbackPlan, RollbackCondition, SafetyCheck, SystemMetrics, ComparisonOperator,
    // Error types
    DecisionError, DecisionResult, DecisionState,
};

pub use actuator::{
    // Core traits
    Actuator, CanaryDeployment, ConfigurationManager, HealthMonitor, TrafficSplitter,
    // Main Coordinator
    ActuatorCoordinator,
    // Component Implementations
    CanaryDeploymentEngine, ConfigurationManagerImpl, ProductionHealthMonitor,
    RollbackEngine,
    // Configuration Manager
    ConfigurationBackend, InMemoryBackend, AuditLogEntry, ConfigurationDiff, DiffType,
    // Rollback Engine
    RollbackMode, RollbackStatus, RollbackTrigger, RollbackHistoryEntry,
    NotificationHandler, NotificationSeverity,
    // Configuration
    ActuatorConfig, ConfigurationManagementConfig, HealthMonitoringConfig, RollbackConfig,
    // Health Monitor
    HealthCheckType, HealthMonitorConfig,
    // Types
    ActuatorStats, CanaryConfig, CanaryPhase, ConfigurationSnapshot,
    DeploymentMetrics, DeploymentRequest, DeploymentStatus, DeploymentStrategy,
    HealthCheckResult, RollbackReason, RollbackRequest, RollbackResult,
    StatisticalAnalysis, SuccessCriteria, TrafficSplitStrategy,
    // Error types
    ActuatorError, ActuatorResult, ActuatorState, DeploymentState,
};

pub use storage::{
    // Core traits
    Storage, CacheStorage, KeyValueStorage, RelationalStorage, PubSubStorage,
    Migrator, Backup, Subscriber,
    // Storage Manager
    StorageManager, RoutingStrategy, ManagerState, ManagerStats,
    // Backend Implementations
    PostgreSQLStorage, RedisStorage, SledStorage,
    // Configuration
    StorageConfig, PostgreSQLConfig, RedisConfig, SledConfig,
    PoolConfig, RetryConfig, MonitoringConfig as StorageMonitoringConfig,
    SSLMode, SledMode,
    // Types
    StorageBackend, StorageEntry, StorageStats, StorageHealth, StorageMetadata,
    Query, Filter, Operator, Sort, SortDirection, Value,
    Transaction, TransactionState, Operation, Batch,
    CacheStats, BackupMetadata, Migration, SqlParam, SqlResult,
    // Error types
    StorageError, StorageResult, ErrorCategory,
};
