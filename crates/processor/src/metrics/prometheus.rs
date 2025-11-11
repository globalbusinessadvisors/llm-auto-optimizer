//! Prometheus metrics definitions for the Stream Processor
//!
//! This module defines all metrics exported by the processor, following
//! Prometheus naming conventions and best practices.

use super::labels::{
    BackendLabel, CacheLayerLabel, LabelNames, OperationLabel, ResultLabel, StrategyLabel,
    WindowTypeLabel,
};
use super::registry::MetricsRegistry;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

/// Comprehensive metrics for the Stream Processor
pub struct ProcessorMetrics {
    // === Stream Processing Metrics ===
    /// Total number of events received by the processor
    pub events_received_total: Counter<u64, AtomicU64>,

    /// Total number of events processed, labeled by result (success/error/dropped/filtered)
    pub events_processed_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Duration of event processing in seconds
    pub event_processing_duration_seconds: Family<Vec<(String, String)>, Histogram>,

    /// Current pipeline lag in seconds
    pub pipeline_lag_seconds: Gauge<f64, AtomicU64>,

    /// Number of backpressure events (queued events)
    pub backpressure_events: Gauge<i64, AtomicI64>,

    // === Window Metrics ===
    /// Total number of windows created
    pub windows_created_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Total number of windows triggered
    pub windows_triggered_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Number of currently active windows
    pub windows_active: Family<Vec<(String, String)>, Gauge<i64, AtomicI64>>,

    /// Distribution of window sizes (number of events)
    pub window_size_events: Family<Vec<(String, String)>, Histogram>,

    /// Total number of late events (arriving after watermark)
    pub late_events_total: Counter<u64, AtomicU64>,

    // === Aggregation Metrics ===
    /// Total number of aggregations computed
    pub aggregations_computed_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Duration of aggregation computation in seconds
    pub aggregation_duration_seconds: Family<Vec<(String, String)>, Histogram>,

    /// Total number of aggregation errors
    pub aggregation_errors_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    // === State Backend Metrics ===
    /// Total number of state operations
    pub state_operations_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Duration of state operations in seconds
    pub state_operation_duration_seconds: Family<Vec<(String, String)>, Histogram>,

    /// Total number of state cache hits
    pub state_cache_hits_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Total number of state cache misses
    pub state_cache_misses_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Current size of state in bytes
    pub state_size_bytes: Family<Vec<(String, String)>, Gauge<i64, AtomicI64>>,

    /// Total number of state evictions
    pub state_evictions_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    // === Deduplication Metrics ===
    /// Total number of events checked for deduplication
    pub dedup_events_checked_total: Counter<u64, AtomicU64>,

    /// Total number of duplicate events found
    pub dedup_duplicates_found_total: Counter<u64, AtomicU64>,

    /// Current size of deduplication cache
    pub dedup_cache_size: Gauge<i64, AtomicI64>,

    /// Total number of deduplication cache evictions
    pub dedup_cache_evictions_total: Counter<u64, AtomicU64>,

    // === Normalization Metrics ===
    /// Total number of events normalized
    pub normalization_events_total: Counter<u64, AtomicU64>,

    /// Total number of fill operations performed
    pub normalization_fill_operations_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Total number of gaps filled in time series
    pub normalization_gaps_filled_total: Counter<u64, AtomicU64>,

    /// Total number of outliers detected
    pub normalization_outliers_detected_total: Counter<u64, AtomicU64>,

    // === Watermark Metrics ===
    /// Current watermark lag in seconds
    pub watermark_lag_seconds: Gauge<f64, AtomicU64>,

    /// Total number of watermark updates
    pub watermark_updates_total: Counter<u64, AtomicU64>,

    /// Total number of out-of-order events
    pub out_of_order_events_total: Counter<u64, AtomicU64>,

    // === Kafka Metrics ===
    /// Total number of messages consumed from Kafka
    pub kafka_messages_consumed_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Total number of messages produced to Kafka
    pub kafka_messages_produced_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Kafka consumer lag (messages)
    pub kafka_consumer_lag: Family<Vec<(String, String)>, Gauge<i64, AtomicI64>>,

    /// Kafka offset commit errors
    pub kafka_offset_commit_errors_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    // === Pipeline Metrics ===
    /// Total number of pipeline errors
    pub pipeline_errors_total: Family<Vec<(String, String)>, Counter<u64, AtomicU64>>,

    /// Number of active pipeline operators
    pub pipeline_operators_active: Gauge<i64, AtomicI64>,

    /// Duration of operator execution in seconds
    pub operator_duration_seconds: Family<Vec<(String, String)>, Histogram>,
}

impl ProcessorMetrics {
    /// Create a new ProcessorMetrics instance and register all metrics
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Self {
            // Stream Processing
            events_received_total: Counter::default(),
            events_processed_total: Family::default(),
            event_processing_duration_seconds: Family::new_with_constructor(Self::duration_histogram),
            pipeline_lag_seconds: Gauge::default(),
            backpressure_events: Gauge::default(),

            // Windows
            windows_created_total: Family::default(),
            windows_triggered_total: Family::default(),
            windows_active: Family::default(),
            window_size_events: Family::new_with_constructor(Self::size_histogram),
            late_events_total: Counter::default(),

            // Aggregations
            aggregations_computed_total: Family::default(),
            aggregation_duration_seconds: Family::new_with_constructor(Self::duration_histogram),
            aggregation_errors_total: Family::default(),

            // State Backend
            state_operations_total: Family::default(),
            state_operation_duration_seconds: Family::new_with_constructor(Self::duration_histogram),
            state_cache_hits_total: Family::default(),
            state_cache_misses_total: Family::default(),
            state_size_bytes: Family::default(),
            state_evictions_total: Family::default(),

            // Deduplication
            dedup_events_checked_total: Counter::default(),
            dedup_duplicates_found_total: Counter::default(),
            dedup_cache_size: Gauge::default(),
            dedup_cache_evictions_total: Counter::default(),

            // Normalization
            normalization_events_total: Counter::default(),
            normalization_fill_operations_total: Family::default(),
            normalization_gaps_filled_total: Counter::default(),
            normalization_outliers_detected_total: Counter::default(),

            // Watermarks
            watermark_lag_seconds: Gauge::default(),
            watermark_updates_total: Counter::default(),
            out_of_order_events_total: Counter::default(),

            // Kafka
            kafka_messages_consumed_total: Family::default(),
            kafka_messages_produced_total: Family::default(),
            kafka_consumer_lag: Family::default(),
            kafka_offset_commit_errors_total: Family::default(),

            // Pipeline
            pipeline_errors_total: Family::default(),
            pipeline_operators_active: Gauge::default(),
            operator_duration_seconds: Family::new_with_constructor(Self::duration_histogram),
        };

        // Register all metrics with help text and units
        registry.register(
            "processor_events_received",
            "Total number of events received by the processor",
            metrics.events_received_total.clone(),
        );

        registry.register(
            "processor_events_processed",
            "Total number of events processed by result",
            metrics.events_processed_total.clone(),
        );

        registry.register(
            "processor_event_processing_duration_seconds",
            "Duration of event processing",
            metrics.event_processing_duration_seconds.clone(),
        );

        registry.register(
            "processor_pipeline_lag_seconds",
            "Current pipeline lag",
            metrics.pipeline_lag_seconds.clone(),
        );

        registry.register(
            "processor_backpressure_events",
            "Number of events waiting due to backpressure",
            metrics.backpressure_events.clone(),
        );

        registry.register(
            "processor_windows_created",
            "Total number of windows created by type",
            metrics.windows_created_total.clone(),
        );

        registry.register(
            "processor_windows_triggered",
            "Total number of windows triggered by type",
            metrics.windows_triggered_total.clone(),
        );

        registry.register(
            "processor_windows_active",
            "Number of currently active windows by type",
            metrics.windows_active.clone(),
        );

        registry.register(
            "processor_window_size_events",
            "Distribution of window sizes in number of events",
            metrics.window_size_events.clone(),
        );

        registry.register(
            "processor_late_events",
            "Total number of late events arriving after watermark",
            metrics.late_events_total.clone(),
        );

        registry.register(
            "processor_aggregations_computed",
            "Total number of aggregations computed by type",
            metrics.aggregations_computed_total.clone(),
        );

        registry.register(
            "processor_aggregation_duration_seconds",
            "Duration of aggregation computation",
            metrics.aggregation_duration_seconds.clone(),
        );

        registry.register(
            "processor_aggregation_errors",
            "Total number of aggregation errors by type",
            metrics.aggregation_errors_total.clone(),
        );

        registry.register(
            "processor_state_operations",
            "Total number of state operations by operation and backend",
            metrics.state_operations_total.clone(),
        );

        registry.register(
            "processor_state_operation_duration_seconds",
            "Duration of state operations",
            metrics.state_operation_duration_seconds.clone(),
        );

        registry.register(
            "processor_state_cache_hits",
            "Total number of state cache hits by layer",
            metrics.state_cache_hits_total.clone(),
        );

        registry.register(
            "processor_state_cache_misses",
            "Total number of state cache misses by layer",
            metrics.state_cache_misses_total.clone(),
        );

        registry.register(
            "processor_state_size_bytes",
            "Current size of state in bytes by backend",
            metrics.state_size_bytes.clone(),
        );

        registry.register(
            "processor_state_evictions",
            "Total number of state evictions by backend",
            metrics.state_evictions_total.clone(),
        );

        registry.register(
            "processor_dedup_events_checked",
            "Total number of events checked for deduplication",
            metrics.dedup_events_checked_total.clone(),
        );

        registry.register(
            "processor_dedup_duplicates_found",
            "Total number of duplicate events found",
            metrics.dedup_duplicates_found_total.clone(),
        );

        registry.register(
            "processor_dedup_cache_size",
            "Current size of deduplication cache",
            metrics.dedup_cache_size.clone(),
        );

        registry.register(
            "processor_dedup_cache_evictions",
            "Total number of deduplication cache evictions",
            metrics.dedup_cache_evictions_total.clone(),
        );

        registry.register(
            "processor_normalization_events",
            "Total number of events normalized",
            metrics.normalization_events_total.clone(),
        );

        registry.register(
            "processor_normalization_fill_operations",
            "Total number of fill operations by strategy",
            metrics.normalization_fill_operations_total.clone(),
        );

        registry.register(
            "processor_normalization_gaps_filled",
            "Total number of gaps filled in time series",
            metrics.normalization_gaps_filled_total.clone(),
        );

        registry.register(
            "processor_normalization_outliers_detected",
            "Total number of outliers detected",
            metrics.normalization_outliers_detected_total.clone(),
        );

        registry.register(
            "processor_watermark_lag_seconds",
            "Current watermark lag",
            metrics.watermark_lag_seconds.clone(),
        );

        registry.register(
            "processor_watermark_updates",
            "Total number of watermark updates",
            metrics.watermark_updates_total.clone(),
        );

        registry.register(
            "processor_out_of_order_events",
            "Total number of out-of-order events",
            metrics.out_of_order_events_total.clone(),
        );

        registry.register(
            "processor_kafka_messages_consumed",
            "Total number of messages consumed from Kafka",
            metrics.kafka_messages_consumed_total.clone(),
        );

        registry.register(
            "processor_kafka_messages_produced",
            "Total number of messages produced to Kafka",
            metrics.kafka_messages_produced_total.clone(),
        );

        registry.register(
            "processor_kafka_consumer_lag",
            "Kafka consumer lag in messages",
            metrics.kafka_consumer_lag.clone(),
        );

        registry.register(
            "processor_kafka_offset_commit_errors",
            "Kafka offset commit errors",
            metrics.kafka_offset_commit_errors_total.clone(),
        );

        registry.register(
            "processor_pipeline_errors",
            "Total number of pipeline errors by type",
            metrics.pipeline_errors_total.clone(),
        );

        registry.register(
            "processor_pipeline_operators_active",
            "Number of active pipeline operators",
            metrics.pipeline_operators_active.clone(),
        );

        registry.register(
            "processor_operator_duration_seconds",
            "Duration of operator execution",
            metrics.operator_duration_seconds.clone(),
        );

        metrics
    }

    /// Create a new instance registered with the global registry
    pub fn global() -> Arc<Self> {
        let registry = MetricsRegistry::global();
        let binding = registry.registry();
        let mut reg = binding.write();
        Arc::new(Self::new(&mut reg))
    }

    // === Convenience methods for common metric operations ===

    /// Record an event being received
    #[inline]
    pub fn record_event_received(&self) {
        self.events_received_total.inc();
    }

    /// Record an event being processed
    #[inline]
    pub fn record_event_processed(&self, result: ResultLabel) {
        self.events_processed_total
            .get_or_create(&vec![(LabelNames::RESULT.to_string(), result.to_string())])
            .inc();
    }

    /// Record event processing duration
    #[inline]
    pub fn record_event_processing_duration(&self, duration_secs: f64, result: ResultLabel) {
        self.event_processing_duration_seconds
            .get_or_create(&vec![(LabelNames::RESULT.to_string(), result.to_string())])
            .observe(duration_secs);
    }

    /// Update pipeline lag
    #[inline]
    pub fn set_pipeline_lag(&self, lag_secs: f64) {
        self.pipeline_lag_seconds.set(lag_secs);
    }

    /// Update backpressure events count
    #[inline]
    pub fn set_backpressure_events(&self, count: i64) {
        self.backpressure_events.set(count);
    }

    /// Record window creation
    #[inline]
    pub fn record_window_created(&self, window_type: WindowTypeLabel) {
        self.windows_created_total
            .get_or_create(&vec![(
                LabelNames::WINDOW_TYPE.to_string(),
                window_type.to_string(),
            )])
            .inc();
    }

    /// Record window trigger
    #[inline]
    pub fn record_window_triggered(&self, window_type: WindowTypeLabel) {
        self.windows_triggered_total
            .get_or_create(&vec![(
                LabelNames::WINDOW_TYPE.to_string(),
                window_type.to_string(),
            )])
            .inc();
    }

    /// Update active windows count
    #[inline]
    pub fn set_windows_active(&self, window_type: WindowTypeLabel, count: i64) {
        self.windows_active
            .get_or_create(&vec![(
                LabelNames::WINDOW_TYPE.to_string(),
                window_type.to_string(),
            )])
            .set(count);
    }

    /// Record window size
    #[inline]
    pub fn record_window_size(&self, window_type: WindowTypeLabel, size: f64) {
        self.window_size_events
            .get_or_create(&vec![(
                LabelNames::WINDOW_TYPE.to_string(),
                window_type.to_string(),
            )])
            .observe(size);
    }

    /// Record late event
    #[inline]
    pub fn record_late_event(&self) {
        self.late_events_total.inc();
    }

    /// Record state operation
    #[inline]
    pub fn record_state_operation(
        &self,
        operation: OperationLabel,
        backend: BackendLabel,
        duration_secs: f64,
    ) {
        let labels = vec![
            (LabelNames::OPERATION.to_string(), operation.to_string()),
            (LabelNames::BACKEND.to_string(), backend.to_string()),
        ];

        self.state_operations_total.get_or_create(&labels).inc();
        self.state_operation_duration_seconds
            .get_or_create(&labels)
            .observe(duration_secs);
    }

    /// Record state cache hit
    #[inline]
    pub fn record_state_cache_hit(&self, layer: CacheLayerLabel) {
        self.state_cache_hits_total
            .get_or_create(&vec![(LabelNames::LAYER.to_string(), layer.to_string())])
            .inc();
    }

    /// Record state cache miss
    #[inline]
    pub fn record_state_cache_miss(&self, layer: CacheLayerLabel) {
        self.state_cache_misses_total
            .get_or_create(&vec![(LabelNames::LAYER.to_string(), layer.to_string())])
            .inc();
    }

    /// Update state size
    #[inline]
    pub fn set_state_size_bytes(&self, backend: BackendLabel, size: i64) {
        self.state_size_bytes
            .get_or_create(&vec![(LabelNames::BACKEND.to_string(), backend.to_string())])
            .set(size);
    }

    /// Record deduplication check
    #[inline]
    pub fn record_dedup_check(&self, is_duplicate: bool) {
        self.dedup_events_checked_total.inc();
        if is_duplicate {
            self.dedup_duplicates_found_total.inc();
        }
    }

    /// Update deduplication cache size
    #[inline]
    pub fn set_dedup_cache_size(&self, size: i64) {
        self.dedup_cache_size.set(size);
    }

    /// Record normalization fill operation
    #[inline]
    pub fn record_normalization_fill(&self, strategy: StrategyLabel) {
        self.normalization_fill_operations_total
            .get_or_create(&vec![(LabelNames::STRATEGY.to_string(), strategy.to_string())])
            .inc();
    }

    /// Update watermark lag
    #[inline]
    pub fn set_watermark_lag(&self, lag_secs: f64) {
        self.watermark_lag_seconds.set(lag_secs);
    }

    /// Record watermark update
    #[inline]
    pub fn record_watermark_update(&self) {
        self.watermark_updates_total.inc();
    }

    /// Record out-of-order event
    #[inline]
    pub fn record_out_of_order_event(&self) {
        self.out_of_order_events_total.inc();
    }

    /// Helper function to create duration histograms (1ms to 30s)
    fn duration_histogram() -> Histogram {
        Histogram::new(exponential_buckets(0.001, 2.0, 16))
    }

    /// Helper function to create size histograms (1 to 10000 events)
    fn size_histogram() -> Histogram {
        Histogram::new(exponential_buckets(1.0, 2.0, 14))
    }
}

/// Snapshot of metrics for testing and debugging
#[derive(Debug, Default)]
pub struct MetricsSnapshot {
    pub events_received: u64,
    pub events_processed_success: u64,
    pub events_processed_error: u64,
    pub windows_created: u64,
    pub duplicates_found: u64,
}

impl MetricsSnapshot {
    /// Create a snapshot from the current metrics
    pub fn capture(metrics: &ProcessorMetrics) -> Self {
        Self {
            events_received: metrics.events_received_total.get(),
            events_processed_success: metrics
                .events_processed_total
                .get_or_create(&vec![(
                    LabelNames::RESULT.to_string(),
                    ResultLabel::Success.to_string(),
                )])
                .get(),
            events_processed_error: metrics
                .events_processed_total
                .get_or_create(&vec![(
                    LabelNames::RESULT.to_string(),
                    ResultLabel::Error.to_string(),
                )])
                .get(),
            windows_created: metrics
                .windows_created_total
                .get_or_create(&vec![(
                    LabelNames::WINDOW_TYPE.to_string(),
                    WindowTypeLabel::Tumbling.to_string(),
                )])
                .get(),
            duplicates_found: metrics.dedup_duplicates_found_total.get(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let mut registry = Registry::default();
        let metrics = ProcessorMetrics::new(&mut registry);

        // Test basic operations
        metrics.record_event_received();
        assert_eq!(metrics.events_received_total.get(), 1);

        metrics.record_event_processed(ResultLabel::Success);
        assert_eq!(
            metrics
                .events_processed_total
                .get_or_create(&vec![(
                    LabelNames::RESULT.to_string(),
                    ResultLabel::Success.to_string()
                )])
                .get(),
            1
        );
    }

    #[test]
    fn test_window_metrics() {
        let mut registry = Registry::default();
        let metrics = ProcessorMetrics::new(&mut registry);

        metrics.record_window_created(WindowTypeLabel::Tumbling);
        metrics.record_window_triggered(WindowTypeLabel::Tumbling);
        metrics.set_windows_active(WindowTypeLabel::Tumbling, 5);

        assert_eq!(
            metrics
                .windows_active
                .get_or_create(&vec![(
                    LabelNames::WINDOW_TYPE.to_string(),
                    WindowTypeLabel::Tumbling.to_string()
                )])
                .get(),
            5
        );
    }

    #[test]
    fn test_deduplication_metrics() {
        let mut registry = Registry::default();
        let metrics = ProcessorMetrics::new(&mut registry);

        metrics.record_dedup_check(false);
        metrics.record_dedup_check(true);
        metrics.record_dedup_check(true);

        assert_eq!(metrics.dedup_events_checked_total.get(), 3);
        assert_eq!(metrics.dedup_duplicates_found_total.get(), 2);
    }

    #[test]
    fn test_state_metrics() {
        let mut registry = Registry::default();
        let metrics = ProcessorMetrics::new(&mut registry);

        metrics.record_state_operation(OperationLabel::Get, BackendLabel::Redis, 0.001);
        metrics.record_state_cache_hit(CacheLayerLabel::L1);
        metrics.record_state_cache_miss(CacheLayerLabel::L2);

        assert_eq!(
            metrics
                .state_cache_hits_total
                .get_or_create(&vec![(
                    LabelNames::LAYER.to_string(),
                    CacheLayerLabel::L1.to_string()
                )])
                .get(),
            1
        );
    }

    #[test]
    fn test_metrics_snapshot() {
        let mut registry = Registry::default();
        let metrics = ProcessorMetrics::new(&mut registry);

        metrics.record_event_received();
        metrics.record_event_processed(ResultLabel::Success);
        metrics.record_dedup_check(true);

        let snapshot = MetricsSnapshot::capture(&metrics);
        assert_eq!(snapshot.events_received, 1);
        assert_eq!(snapshot.events_processed_success, 1);
        assert_eq!(snapshot.duplicates_found, 1);
    }
}
