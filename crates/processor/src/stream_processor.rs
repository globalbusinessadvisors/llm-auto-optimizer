//! High-level stream processor for event processing with windowing and aggregation
//!
//! This module provides the `StreamProcessor` which coordinates:
//! - Event ingestion and time extraction
//! - Window assignment and management
//! - Watermark generation and propagation
//! - Aggregation computation per window
//! - Result emission
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │   Events    │
//! └──────┬──────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Time Extraction    │
//! └──────┬──────────────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │ Window Assignment   │ ◄─── WindowManager
//! └──────┬──────────────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │   Aggregation       │ ◄─── Aggregators
//! └──────┬──────────────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │ Watermark Check     │
//! └──────┬──────────────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │  Trigger & Emit     │
//! └──────┬──────────────┘
//!        │
//!        ▼
//! ┌─────────────────────┐
//! │    Results          │
//! └─────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust
//! use processor::{
//!     StreamProcessor, StreamProcessorBuilder,
//!     TumblingWindowAssigner, OnWatermarkTrigger,
//! };
//! use processor::aggregation::CompositeAggregator;
//! use chrono::Duration;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Build a stream processor for latency metrics
//! let processor = StreamProcessorBuilder::new()
//!     .with_tumbling_window(Duration::minutes(5))
//!     .with_watermark_trigger()
//!     .with_aggregation(|_window_id| {
//!         let mut agg = CompositeAggregator::new();
//!         agg.add_count("request_count")
//!            .add_avg("avg_latency")
//!            .add_p50("p50_latency")
//!            .add_p95("p95_latency")
//!            .add_p99("p99_latency")
//!            .add_max("max_latency");
//!         agg
//!     })
//!     .build();
//!
//! // Process events
//! // processor.process_event(event).await?;
//! # Ok(())
//! # }
//! ```

use crate::aggregation::{Aggregator, CompositeAggregator, AggregationResult};
use crate::core::{EventTimeExtractor, KeyExtractor, ProcessorEvent};
use crate::error::{ProcessorError, WindowError};
use crate::watermark::WatermarkGenerator;
use crate::window::{
    Window, WindowAssigner, WindowManager, WindowTrigger, WindowMetadata,
    TumblingWindowAssigner, SlidingWindowAssigner, SessionWindowAssigner,
    OnWatermarkTrigger, ProcessingTimeTrigger, CountTrigger,
};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, trace, warn};

/// Result emitted by the stream processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    /// Window that produced this result
    pub window: Window,
    /// Aggregation key
    pub key: String,
    /// Aggregated results
    pub results: AggregationResult,
    /// Number of events processed
    pub event_count: u64,
    /// Processing timestamp
    pub processed_at: DateTime<Utc>,
    /// Watermark at the time of processing
    pub watermark: Option<DateTime<Utc>>,
}

/// Configuration for the stream processor
#[derive(Debug, Clone)]
pub struct StreamProcessorConfig {
    /// Allow processing of late events
    pub allow_late_events: bool,
    /// Maximum lateness threshold
    pub late_event_threshold: Duration,
    /// Watermark generation interval
    pub watermark_interval: Duration,
    /// Window cleanup retention duration
    pub window_retention: Duration,
    /// Maximum number of concurrent windows per key
    pub max_windows_per_key: usize,
    /// Buffer size for result emission
    pub result_buffer_size: usize,
}

impl Default for StreamProcessorConfig {
    fn default() -> Self {
        Self {
            allow_late_events: true,
            late_event_threshold: Duration::minutes(5),
            watermark_interval: Duration::seconds(1),
            window_retention: Duration::hours(1),
            max_windows_per_key: 1000,
            result_buffer_size: 1000,
        }
    }
}

/// Statistics for the stream processor
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessorStats {
    /// Total events processed
    pub events_processed: u64,
    /// Total events dropped (late or errors)
    pub events_dropped: u64,
    /// Total windows created
    pub windows_created: u64,
    /// Total windows fired
    pub windows_fired: u64,
    /// Current active windows
    pub active_windows: usize,
    /// Current watermark
    pub current_watermark: Option<DateTime<Utc>>,
    /// Processing latency in milliseconds (p50, p95, p99)
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    /// Last update timestamp
    pub last_update: DateTime<Utc>,
}

/// Per-key aggregation state
struct KeyState<A: Aggregator> {
    /// Aggregators for each active window
    aggregators: HashMap<String, A>,
    /// Last seen event time
    last_event_time: Option<DateTime<Utc>>,
}

impl<A: Aggregator> KeyState<A> {
    fn new() -> Self {
        Self {
            aggregators: HashMap::new(),
            last_event_time: None,
        }
    }
}

/// Stream processor for windowed aggregations
pub struct StreamProcessor<T, A, W, Tr>
where
    A: Aggregator + Clone + Send + Sync + 'static,
    W: WindowAssigner + Clone + Send + Sync + 'static,
    Tr: WindowTrigger + Clone + Send + Sync + 'static,
{
    /// Configuration
    config: StreamProcessorConfig,
    /// Window manager
    window_manager: Arc<RwLock<WindowManager<W, Tr>>>,
    /// Watermark generator
    watermark_generator: Arc<RwLock<dyn WatermarkGenerator>>,
    /// Aggregators per key per window
    aggregators: Arc<DashMap<String, KeyState<A>>>,
    /// Result sender
    result_tx: mpsc::Sender<WindowResult>,
    /// Result receiver
    result_rx: Option<mpsc::Receiver<WindowResult>>,
    /// Statistics
    stats: Arc<RwLock<ProcessorStats>>,
    /// Aggregator factory
    aggregator_factory: Arc<dyn Fn(&str) -> A + Send + Sync>,
    /// Phantom data for event type
    _phantom: PhantomData<T>,
}

impl<T, A, W, Tr> StreamProcessor<T, A, W, Tr>
where
    A: Aggregator<Input = f64> + Clone + Send + Sync + 'static,
    W: WindowAssigner + Clone + Send + Sync + 'static,
    Tr: WindowTrigger + Clone + Send + Sync + 'static,
{
    /// Create a new stream processor
    pub fn new<F>(
        config: StreamProcessorConfig,
        assigner: W,
        trigger: Tr,
        aggregator_factory: F,
    ) -> Self
    where
        F: Fn(&str) -> A + Send + Sync + 'static,
    {
        let window_manager = WindowManager::with_late_events(
            assigner,
            trigger,
            config.allow_late_events,
            config.late_event_threshold,
        );

        let (result_tx, result_rx) = mpsc::channel(config.result_buffer_size);

        Self {
            config,
            window_manager: Arc::new(RwLock::new(window_manager)),
            watermark_generator: Arc::new(RwLock::new(
                WatermarkGenerator::bounded_out_of_orderness(
                    config.late_event_threshold,
                )
            )),
            aggregators: Arc::new(DashMap::new()),
            result_tx,
            result_rx: Some(result_rx),
            stats: Arc::new(RwLock::new(ProcessorStats::default())),
            aggregator_factory: Arc::new(aggregator_factory),
            _phantom: PhantomData,
        }
    }

    /// Process a single event
    ///
    /// This is the main entry point for event processing. It:
    /// 1. Assigns the event to windows
    /// 2. Updates aggregators for each window
    /// 3. Updates watermark
    /// 4. Checks for windows ready to fire
    /// 5. Emits results for completed windows
    pub async fn process_event(
        &self,
        key: String,
        event_time: DateTime<Utc>,
        value: f64,
    ) -> Result<(), ProcessorError> {
        let start_time = Utc::now();

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.events_processed += 1;
            stats.last_update = Utc::now();
        }

        // Assign event to windows
        let windows = {
            let mut manager = self.window_manager.write().await;
            match manager.assign_event(event_time) {
                Ok(windows) => windows,
                Err(e) => {
                    warn!(
                        error = %e,
                        event_time = %event_time,
                        "Failed to assign event to windows"
                    );
                    let mut stats = self.stats.write().await;
                    stats.events_dropped += 1;
                    return Err(e);
                }
            }
        };

        if windows.is_empty() {
            // Event was dropped (too late)
            let mut stats = self.stats.write().await;
            stats.events_dropped += 1;
            return Ok(());
        }

        trace!(
            key = %key,
            event_time = %event_time,
            window_count = windows.len(),
            "Processing event"
        );

        // Update aggregators for each window
        let mut key_state = self.aggregators
            .entry(key.clone())
            .or_insert_with(KeyState::new);

        for window in &windows {
            let aggregator = key_state.aggregators
                .entry(window.id.clone())
                .or_insert_with(|| (self.aggregator_factory)(&key));

            if let Err(e) = aggregator.update(value) {
                error!(
                    window_id = %window.id,
                    error = %e,
                    "Failed to update aggregator"
                );
                return Err(ProcessorError::Aggregation(
                    crate::error::AggregationError::CorruptedState {
                        aggregation_type: "unknown".to_string(),
                        details: e.to_string(),
                    }
                ));
            }
        }

        key_state.last_event_time = Some(event_time);
        drop(key_state);

        // Update watermark
        let watermark = {
            let mut wm_gen = self.watermark_generator.write().await;
            wm_gen.on_event(event_time.timestamp_millis(), 0);
            wm_gen.current_watermark()
        };

        // Check for windows ready to fire
        let ready_windows = {
            let mut manager = self.window_manager.write().await;
            manager.advance_watermark(watermark.to_datetime())?
        };

        // Process ready windows
        for window_id in ready_windows {
            self.fire_window(&key, &window_id, watermark.to_datetime()).await?;
        }

        // Update latency statistics
        let latency = (Utc::now() - start_time).num_milliseconds() as f64;
        self.update_latency_stats(latency).await;

        Ok(())
    }

    /// Fire a window and emit results
    async fn fire_window(
        &self,
        key: &str,
        window_id: &str,
        watermark: DateTime<Utc>,
    ) -> Result<(), ProcessorError> {
        // Get window metadata
        let metadata = {
            let manager = self.window_manager.read().await;
            manager.get_window(window_id)
        };

        let metadata = match metadata {
            Some(m) => m,
            None => {
                warn!(window_id = %window_id, "Window not found");
                return Ok(());
            }
        };

        // Get and remove aggregator
        let results = {
            let mut key_state = match self.aggregators.get_mut(key) {
                Some(state) => state,
                None => {
                    warn!(key = %key, window_id = %window_id, "Key state not found");
                    return Ok(());
                }
            };

            let aggregator = match key_state.aggregators.remove(window_id) {
                Some(agg) => agg,
                None => {
                    warn!(key = %key, window_id = %window_id, "Aggregator not found");
                    return Ok(());
                }
            };

            // Finalize aggregation based on type
            // For CompositeAggregator, use finalize directly
            // For other aggregators, we need to handle differently
            if let Some(composite) = (&aggregator as &dyn std::any::Any).downcast_ref::<CompositeAggregator>() {
                composite.finalize().map_err(|e| {
                    ProcessorError::Aggregation(crate::error::AggregationError::CorruptedState {
                        aggregation_type: "composite".to_string(),
                        details: e.to_string(),
                    })
                })?
            } else {
                // For simple aggregators, create a result with single value
                let value = aggregator.finalize().map_err(|e| {
                    ProcessorError::Aggregation(crate::error::AggregationError::CorruptedState {
                        aggregation_type: "simple".to_string(),
                        details: e.to_string(),
                    })
                })?;

                let mut result = AggregationResult::new();
                result.set_value("value", value);
                result.set_count("count", aggregator.count());
                result
            }
        };

        // Create window result
        let window_result = WindowResult {
            window: metadata.window.clone(),
            key: key.to_string(),
            results,
            event_count: metadata.stats.event_count,
            processed_at: Utc::now(),
            watermark: Some(watermark),
        };

        debug!(
            window_id = %window_id,
            key = %key,
            event_count = metadata.stats.event_count,
            "Window fired"
        );

        // Emit result
        if let Err(e) = self.result_tx.send(window_result).await {
            error!(error = %e, "Failed to send window result");
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.windows_fired += 1;
            stats.current_watermark = Some(watermark);
        }

        Ok(())
    }

    /// Update latency statistics
    async fn update_latency_stats(&self, latency_ms: f64) {
        // Simple exponential moving average for now
        // In production, you'd want more sophisticated percentile tracking
        let mut stats = self.stats.write().await;

        if stats.latency_p50_ms == 0.0 {
            stats.latency_p50_ms = latency_ms;
            stats.latency_p95_ms = latency_ms;
            stats.latency_p99_ms = latency_ms;
        } else {
            // EMA with different smoothing factors
            stats.latency_p50_ms = 0.9 * stats.latency_p50_ms + 0.1 * latency_ms;
            stats.latency_p95_ms = 0.95 * stats.latency_p95_ms + 0.05 * latency_ms.max(stats.latency_p95_ms);
            stats.latency_p99_ms = 0.99 * stats.latency_p99_ms + 0.01 * latency_ms.max(stats.latency_p99_ms);
        }
    }

    /// Get a stream of window results
    pub fn results(&mut self) -> Option<impl Stream<Item = WindowResult>> {
        self.result_rx.take().map(ReceiverStream::new)
    }

    /// Get current statistics
    pub async fn stats(&self) -> ProcessorStats {
        let mut stats = self.stats.read().await.clone();

        // Update active windows count
        let manager = self.window_manager.read().await;
        stats.active_windows = manager.active_window_count();
        stats.current_watermark = manager.current_watermark();

        stats
    }

    /// Trigger cleanup of old windows
    pub async fn cleanup(&self) -> Result<usize, ProcessorError> {
        let mut manager = self.window_manager.write().await;
        let removed = manager.cleanup_old_windows(self.config.window_retention);

        if removed > 0 {
            info!(removed, "Cleaned up old windows");
        }

        Ok(removed)
    }

    /// Reset the processor (for testing)
    pub async fn reset(&self) {
        let mut manager = self.window_manager.write().await;
        manager.reset();

        let mut wm_gen = self.watermark_generator.write().await;
        *wm_gen = WatermarkGenerator::bounded_out_of_orderness(
            self.config.late_event_threshold
        );

        self.aggregators.clear();

        let mut stats = self.stats.write().await;
        *stats = ProcessorStats::default();

        info!("Stream processor reset");
    }
}

/// Builder for creating stream processors
pub struct StreamProcessorBuilder<T, A> {
    config: StreamProcessorConfig,
    _phantom: PhantomData<(T, A)>,
}

impl<T, A> Default for StreamProcessorBuilder<T, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, A> StreamProcessorBuilder<T, A>
where
    A: Aggregator<Input = f64> + Clone + Send + Sync + 'static,
{
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: StreamProcessorConfig::default(),
            _phantom: PhantomData,
        }
    }

    /// Set configuration
    pub fn with_config(mut self, config: StreamProcessorConfig) -> Self {
        self.config = config;
        self
    }

    /// Allow late events
    pub fn allow_late_events(mut self, allow: bool, threshold: Duration) -> Self {
        self.config.allow_late_events = allow;
        self.config.late_event_threshold = threshold;
        self
    }

    /// Set watermark interval
    pub fn watermark_interval(mut self, interval: Duration) -> Self {
        self.config.watermark_interval = interval;
        self
    }

    /// Build with tumbling windows
    pub fn build_tumbling<F>(
        self,
        window_size: Duration,
        aggregator_factory: F,
    ) -> StreamProcessor<T, A, TumblingWindowAssigner, OnWatermarkTrigger>
    where
        F: Fn(&str) -> A + Send + Sync + 'static,
    {
        let assigner = TumblingWindowAssigner::new(window_size);
        let trigger = OnWatermarkTrigger::new();

        StreamProcessor::new(self.config, assigner, trigger, aggregator_factory)
    }

    /// Build with sliding windows
    pub fn build_sliding<F>(
        self,
        window_size: Duration,
        slide: Duration,
        aggregator_factory: F,
    ) -> StreamProcessor<T, A, SlidingWindowAssigner, OnWatermarkTrigger>
    where
        F: Fn(&str) -> A + Send + Sync + 'static,
    {
        let assigner = SlidingWindowAssigner::new(window_size, slide);
        let trigger = OnWatermarkTrigger::new();

        StreamProcessor::new(self.config, assigner, trigger, aggregator_factory)
    }

    /// Build with session windows
    pub fn build_session<F>(
        self,
        gap: Duration,
        aggregator_factory: F,
    ) -> StreamProcessor<T, A, SessionWindowAssigner, OnWatermarkTrigger>
    where
        F: Fn(&str) -> A + Send + Sync + 'static,
    {
        let assigner = SessionWindowAssigner::new(gap);
        let trigger = OnWatermarkTrigger::new();

        StreamProcessor::new(self.config, assigner, trigger, aggregator_factory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::AverageAggregator;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_stream_processor_basic() {
        let processor = StreamProcessorBuilder::new()
            .build_tumbling(
                Duration::seconds(1),
                |_key| AverageAggregator::new(),
            );

        // Process events
        let base_time = Utc::now();
        processor.process_event("key1".to_string(), base_time, 10.0).await.unwrap();
        processor.process_event("key1".to_string(), base_time + Duration::milliseconds(500), 20.0).await.unwrap();

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 2);
    }

    #[tokio::test]
    async fn test_stream_processor_windowing() {
        let mut processor = StreamProcessorBuilder::new()
            .build_tumbling(
                Duration::milliseconds(1000),
                |_key| AverageAggregator::new(),
            );

        let mut results_stream = processor.results().unwrap();

        // Process events in first window
        let base_time = Utc::now();
        processor.process_event("key1".to_string(), base_time, 10.0).await.unwrap();
        processor.process_event("key1".to_string(), base_time + Duration::milliseconds(500), 20.0).await.unwrap();

        // Process event in second window to trigger watermark
        processor.process_event(
            "key1".to_string(),
            base_time + Duration::milliseconds(2000),
            30.0
        ).await.unwrap();

        // Should have fired first window
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            results_stream.next()
        ).await.ok();
    }

    #[tokio::test]
    async fn test_stream_processor_multiple_keys() {
        let processor = StreamProcessorBuilder::new()
            .build_tumbling(
                Duration::seconds(1),
                |_key| AverageAggregator::new(),
            );

        let base_time = Utc::now();
        processor.process_event("key1".to_string(), base_time, 10.0).await.unwrap();
        processor.process_event("key2".to_string(), base_time, 20.0).await.unwrap();
        processor.process_event("key1".to_string(), base_time + Duration::milliseconds(500), 30.0).await.unwrap();

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 3);
    }

    #[tokio::test]
    async fn test_stream_processor_cleanup() {
        let processor = StreamProcessorBuilder::new()
            .build_tumbling(
                Duration::milliseconds(100),
                |_key| AverageAggregator::new(),
            );

        let base_time = Utc::now();
        processor.process_event("key1".to_string(), base_time, 10.0).await.unwrap();

        // Trigger window
        processor.process_event(
            "key1".to_string(),
            base_time + Duration::seconds(1),
            20.0
        ).await.unwrap();

        // Cleanup
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let removed = processor.cleanup().await.unwrap();
        assert!(removed > 0);
    }
}
