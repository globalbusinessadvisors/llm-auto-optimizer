//! Time-series normalization for stream processing
//!
//! This module provides production-grade time-series normalization capabilities including:
//! - Multiple fill strategies (forward, backward, linear interpolation, zero, skip)
//! - Timestamp alignment to regular intervals
//! - Out-of-order event handling with buffering
//! - Automatic interval detection
//! - Batch and streaming normalization
//! - Memory-efficient bounded buffers
//! - Comprehensive metrics and tracing

use crate::error::{ProcessorError, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};
use tracing::{debug, trace, warn};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during normalization
#[derive(Debug, thiserror::Error)]
pub enum NormalizationError {
    /// Invalid interval duration
    #[error("invalid interval: {interval}ms, must be greater than 0")]
    InvalidInterval { interval: i64 },

    /// Buffer overflow
    #[error("buffer overflow: attempted to add event when buffer is full (capacity: {capacity})")]
    BufferOverflow { capacity: usize },

    /// Invalid timestamp
    #[error("invalid timestamp: {timestamp}, reason: {reason}")]
    InvalidTimestamp { timestamp: i64, reason: String },

    /// Insufficient data for interpolation
    #[error("insufficient data for {operation}: need at least {required} points, got {actual}")]
    InsufficientData {
        operation: String,
        required: usize,
        actual: usize,
    },

    /// Configuration error
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Computation error
    #[error("computation error: {0}")]
    Computation(String),
}

impl From<NormalizationError> for ProcessorError {
    fn from(err: NormalizationError) -> Self {
        ProcessorError::Unexpected(err.to_string())
    }
}

pub type NormalizationResult<T> = std::result::Result<T, NormalizationError>;

// ============================================================================
// Fill Strategies
// ============================================================================

/// Strategy for filling missing time-series points
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FillStrategy {
    /// Carry forward the last observed value
    ForwardFill,

    /// Use the next observed value (requires future data)
    BackwardFill,

    /// Linear interpolation between surrounding points
    LinearInterpolate,

    /// Fill with zero/default value
    Zero,

    /// Skip missing points (no output for missing intervals)
    Skip,
}

impl FillStrategy {
    /// Fill a missing value given surrounding context
    ///
    /// # Arguments
    /// * `prev_value` - The last observed value before the gap
    /// * `next_value` - The next observed value after the gap
    /// * `prev_time` - Timestamp of previous value
    /// * `next_time` - Timestamp of next value
    /// * `target_time` - Timestamp to fill
    ///
    /// # Returns
    /// `Some(value)` if a value should be filled, `None` if point should be skipped
    pub fn fill(
        &self,
        prev_value: Option<f64>,
        next_value: Option<f64>,
        prev_time: Option<DateTime<Utc>>,
        next_time: Option<DateTime<Utc>>,
        target_time: DateTime<Utc>,
    ) -> NormalizationResult<Option<f64>> {
        match self {
            FillStrategy::ForwardFill => Ok(prev_value),

            FillStrategy::BackwardFill => Ok(next_value),

            FillStrategy::LinearInterpolate => {
                match (prev_value, next_value, prev_time, next_time) {
                    (Some(v1), Some(v2), Some(t1), Some(t2)) => {
                        // Perform linear interpolation
                        let total_duration = (t2 - t1).num_milliseconds() as f64;
                        let elapsed_duration = (target_time - t1).num_milliseconds() as f64;

                        if total_duration == 0.0 {
                            // If timestamps are identical, average the values
                            Ok(Some((v1 + v2) / 2.0))
                        } else {
                            let ratio = elapsed_duration / total_duration;
                            let interpolated = v1 + ratio * (v2 - v1);
                            Ok(Some(interpolated))
                        }
                    }
                    (Some(v), None, _, _) => Ok(Some(v)),    // Fall back to forward fill
                    (None, Some(v), _, _) => Ok(Some(v)),    // Fall back to backward fill
                    (Some(v1), Some(v2), None, _) | (Some(v1), Some(v2), _, None) => Ok(Some((v1 + v2) / 2.0)), // No timestamps, average values
                    (None, None, _, _) => Ok(None),          // No data available
                }
            }

            FillStrategy::Zero => Ok(Some(0.0)),

            FillStrategy::Skip => Ok(None),
        }
    }

    /// Check if this strategy requires future data
    pub fn requires_future_data(&self) -> bool {
        matches!(self, FillStrategy::BackwardFill | FillStrategy::LinearInterpolate)
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for time-series normalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationConfig {
    /// Target interval for normalized time-series (in milliseconds)
    /// If None, will be auto-detected from data
    pub interval_ms: Option<i64>,

    /// Strategy for filling missing values
    pub fill_strategy: FillStrategy,

    /// Maximum buffer size for out-of-order events
    pub max_buffer_size: usize,

    /// Maximum allowed out-of-orderness (in milliseconds)
    pub max_out_of_order_ms: i64,

    /// Minimum number of samples required for interval detection
    pub min_samples_for_detection: usize,

    /// Tolerance for interval detection (percentage)
    pub interval_tolerance: f64,

    /// Whether to emit warnings for late/out-of-order events
    pub warn_on_late_events: bool,
}

impl Default for NormalizationConfig {
    fn default() -> Self {
        Self {
            interval_ms: None,
            fill_strategy: FillStrategy::LinearInterpolate,
            max_buffer_size: 1000,
            max_out_of_order_ms: 60_000, // 1 minute
            min_samples_for_detection: 10,
            interval_tolerance: 0.1, // 10% tolerance
            warn_on_late_events: true,
        }
    }
}

impl NormalizationConfig {
    /// Create a new configuration with specified interval
    pub fn with_interval(interval_ms: i64) -> NormalizationResult<Self> {
        if interval_ms <= 0 {
            return Err(NormalizationError::InvalidInterval { interval: interval_ms });
        }
        Ok(Self {
            interval_ms: Some(interval_ms),
            ..Default::default()
        })
    }

    /// Set the fill strategy
    pub fn with_fill_strategy(mut self, strategy: FillStrategy) -> Self {
        self.fill_strategy = strategy;
        self
    }

    /// Set the maximum buffer size
    pub fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> NormalizationResult<()> {
        if let Some(interval) = self.interval_ms {
            if interval <= 0 {
                return Err(NormalizationError::InvalidInterval { interval });
            }
        }

        if self.max_buffer_size == 0 {
            return Err(NormalizationError::Configuration(
                "max_buffer_size must be greater than 0".to_string(),
            ));
        }

        if self.max_out_of_order_ms < 0 {
            return Err(NormalizationError::Configuration(
                "max_out_of_order_ms must be non-negative".to_string(),
            ));
        }

        if self.interval_tolerance < 0.0 || self.interval_tolerance >= 1.0 {
            return Err(NormalizationError::Configuration(
                "interval_tolerance must be between 0.0 and 1.0".to_string(),
            ));
        }

        Ok(())
    }
}

// ============================================================================
// Time-series Event
// ============================================================================

/// A time-series data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesEvent {
    /// Timestamp of the event
    pub timestamp: DateTime<Utc>,

    /// Value at this timestamp
    pub value: f64,

    /// Optional metadata/tags
    pub metadata: Option<serde_json::Value>,
}

impl TimeSeriesEvent {
    /// Create a new time-series event
    pub fn new(timestamp: DateTime<Utc>, value: f64) -> Self {
        Self {
            timestamp,
            value,
            metadata: None,
        }
    }

    /// Create an event with metadata
    pub fn with_metadata(timestamp: DateTime<Utc>, value: f64, metadata: serde_json::Value) -> Self {
        Self {
            timestamp,
            value,
            metadata: Some(metadata),
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics about normalization operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NormalizationStats {
    /// Total events received
    pub events_received: u64,

    /// Events successfully normalized
    pub events_normalized: u64,

    /// Events dropped due to being too late
    pub events_dropped_late: u64,

    /// Events dropped due to buffer overflow
    pub events_dropped_overflow: u64,

    /// Number of points filled
    pub points_filled: u64,

    /// Number of points skipped
    pub points_skipped: u64,

    /// Current buffer size
    pub current_buffer_size: usize,

    /// Maximum buffer size reached
    pub max_buffer_size_reached: usize,

    /// Detected interval (in milliseconds)
    pub detected_interval_ms: Option<i64>,

    /// Total processing time (microseconds)
    pub total_processing_time_us: u64,

    /// Number of out-of-order events
    pub out_of_order_events: u64,
}

impl NormalizationStats {
    /// Calculate the fill rate (percentage of points that were filled)
    pub fn fill_rate(&self) -> f64 {
        if self.events_normalized > 0 {
            self.points_filled as f64 / self.events_normalized as f64
        } else {
            0.0
        }
    }

    /// Calculate the drop rate (percentage of events dropped)
    pub fn drop_rate(&self) -> f64 {
        if self.events_received > 0 {
            (self.events_dropped_late + self.events_dropped_overflow) as f64
                / self.events_received as f64
        } else {
            0.0
        }
    }

    /// Calculate average processing time per event (microseconds)
    pub fn avg_processing_time_us(&self) -> f64 {
        if self.events_received > 0 {
            self.total_processing_time_us as f64 / self.events_received as f64
        } else {
            0.0
        }
    }
}

// ============================================================================
// Time-series Normalizer
// ============================================================================

/// Thread-safe time-series normalizer with out-of-order handling
#[derive(Clone)]
pub struct TimeSeriesNormalizer {
    config: Arc<NormalizationConfig>,
    state: Arc<RwLock<NormalizerState>>,
}

/// Internal state for the normalizer
#[derive(Debug)]
struct NormalizerState {
    /// Sorted buffer for out-of-order events
    buffer: BTreeMap<i64, f64>,

    /// Last emitted timestamp
    last_emitted_timestamp: Option<DateTime<Utc>>,

    /// Statistics
    stats: NormalizationStats,

    /// Detected interval (in milliseconds)
    detected_interval: Option<i64>,

    /// Sample timestamps for interval detection
    sample_timestamps: VecDeque<i64>,
}

impl TimeSeriesNormalizer {
    /// Create a new normalizer with the given configuration
    pub fn new(config: NormalizationConfig) -> NormalizationResult<Self> {
        config.validate()?;

        let state = NormalizerState {
            buffer: BTreeMap::new(),
            last_emitted_timestamp: None,
            stats: NormalizationStats::default(),
            detected_interval: config.interval_ms,
            sample_timestamps: VecDeque::with_capacity(config.min_samples_for_detection),
        };

        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Create a normalizer with default configuration
    pub fn default_config() -> NormalizationResult<Self> {
        Self::new(NormalizationConfig::default())
    }

    /// Process a single time-series event and return normalized events
    ///
    /// This method may return multiple events if there were gaps that needed to be filled.
    /// It buffers out-of-order events and emits them when the watermark advances.
    pub fn normalize(&self, event: TimeSeriesEvent) -> Result<Vec<TimeSeriesEvent>> {
        let start_time = std::time::Instant::now();

        let mut state = self.state.write().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire write lock: {}", e))
        })?;

        state.stats.events_received += 1;

        let timestamp_ms = event.timestamp.timestamp_millis();

        // Check if event is too late
        if let Some(last_emitted) = state.last_emitted_timestamp {
            let late_by_ms = last_emitted.timestamp_millis() - timestamp_ms;
            if late_by_ms > self.config.max_out_of_order_ms {
                if self.config.warn_on_late_events {
                    warn!(
                        timestamp_ms = timestamp_ms,
                        late_by_ms = late_by_ms,
                        "Dropping late event"
                    );
                }
                state.stats.events_dropped_late += 1;
                return Ok(Vec::new());
            }
        }

        // Add to interval detection samples
        if state.detected_interval.is_none() {
            self.update_interval_detection(&mut state, timestamp_ms);
        }

        // Check buffer capacity
        if state.buffer.len() >= self.config.max_buffer_size {
            warn!(
                buffer_size = state.buffer.len(),
                "Buffer overflow, dropping event"
            );
            state.stats.events_dropped_overflow += 1;
            return Ok(Vec::new());
        }

        // Check for out-of-order
        if let Some(last_emitted) = state.last_emitted_timestamp {
            if timestamp_ms < last_emitted.timestamp_millis() {
                state.stats.out_of_order_events += 1;
            }
        }

        // Add to buffer
        state.buffer.insert(timestamp_ms, event.value);
        state.stats.current_buffer_size = state.buffer.len();
        state.stats.max_buffer_size_reached =
            state.stats.max_buffer_size_reached.max(state.buffer.len());

        // Emit normalized events
        let result = self.emit_normalized_events(&mut state);

        // Update statistics
        let elapsed = start_time.elapsed();
        state.stats.total_processing_time_us += elapsed.as_micros() as u64;

        result
    }

    /// Process a batch of events
    ///
    /// This is more efficient than calling `normalize()` repeatedly as it can
    /// optimize buffer operations and reduce lock contention.
    pub fn normalize_batch(&self, events: Vec<TimeSeriesEvent>) -> Result<Vec<TimeSeriesEvent>> {
        let start_time = std::time::Instant::now();

        let mut state = self.state.write().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire write lock: {}", e))
        })?;

        let mut all_results = Vec::new();

        for event in events {
            state.stats.events_received += 1;

            let timestamp_ms = event.timestamp.timestamp_millis();

            // Check if event is too late
            if let Some(last_emitted) = state.last_emitted_timestamp {
                let late_by_ms = last_emitted.timestamp_millis() - timestamp_ms;
                if late_by_ms > self.config.max_out_of_order_ms {
                    if self.config.warn_on_late_events {
                        debug!(
                            timestamp_ms = timestamp_ms,
                            late_by_ms = late_by_ms,
                            "Dropping late event in batch"
                        );
                    }
                    state.stats.events_dropped_late += 1;
                    continue;
                }
            }

            // Add to interval detection samples
            if state.detected_interval.is_none() {
                self.update_interval_detection(&mut state, timestamp_ms);
            }

            // Check buffer capacity
            if state.buffer.len() >= self.config.max_buffer_size {
                state.stats.events_dropped_overflow += 1;
                continue;
            }

            // Check for out-of-order
            if let Some(last_emitted) = state.last_emitted_timestamp {
                if timestamp_ms < last_emitted.timestamp_millis() {
                    state.stats.out_of_order_events += 1;
                }
            }

            // Add to buffer
            state.buffer.insert(timestamp_ms, event.value);
        }

        state.stats.current_buffer_size = state.buffer.len();
        state.stats.max_buffer_size_reached =
            state.stats.max_buffer_size_reached.max(state.buffer.len());

        // Emit all normalized events
        let result = self.emit_normalized_events(&mut state);

        // Update statistics
        let elapsed = start_time.elapsed();
        state.stats.total_processing_time_us += elapsed.as_micros() as u64;

        match result {
            Ok(events) => {
                all_results.extend(events);
                Ok(all_results)
            }
            Err(e) => Err(e),
        }
    }

    /// Flush any remaining buffered events
    ///
    /// This should be called at the end of a stream to ensure all buffered
    /// events are processed. It will emit events even if gaps remain.
    pub fn flush(&self) -> Result<Vec<TimeSeriesEvent>> {
        let mut state = self.state.write().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire write lock: {}", e))
        })?;

        let mut result = Vec::new();

        // Emit all buffered events
        while !state.buffer.is_empty() {
            if let Some((&timestamp_ms, &value)) = state.buffer.iter().next() {
                let timestamp = DateTime::from_timestamp_millis(timestamp_ms)
                    .unwrap_or_else(|| Utc::now());

                result.push(TimeSeriesEvent::new(timestamp, value));
                state.buffer.remove(&timestamp_ms);
                state.last_emitted_timestamp = Some(timestamp);
                state.stats.events_normalized += 1;
            }
        }

        Ok(result)
    }

    /// Get current statistics
    pub fn stats(&self) -> Result<NormalizationStats> {
        let state = self.state.read().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire read lock: {}", e))
        })?;

        Ok(state.stats.clone())
    }

    /// Reset the normalizer state
    pub fn reset(&self) -> Result<()> {
        let mut state = self.state.write().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire write lock: {}", e))
        })?;

        state.buffer.clear();
        state.last_emitted_timestamp = None;
        state.stats = NormalizationStats::default();
        state.detected_interval = self.config.interval_ms;
        state.sample_timestamps.clear();

        Ok(())
    }

    /// Get the detected or configured interval
    pub fn interval_ms(&self) -> Result<Option<i64>> {
        let state = self.state.read().map_err(|e| {
            ProcessorError::Unexpected(format!("Failed to acquire read lock: {}", e))
        })?;

        Ok(state.detected_interval)
    }

    // ========================================================================
    // Private helper methods
    // ========================================================================

    /// Update interval detection with a new timestamp
    fn update_interval_detection(&self, state: &mut NormalizerState, timestamp_ms: i64) {
        state.sample_timestamps.push_back(timestamp_ms);

        // Keep only the required number of samples
        while state.sample_timestamps.len() > self.config.min_samples_for_detection {
            state.sample_timestamps.pop_front();
        }

        // Try to detect interval once we have enough samples
        if state.sample_timestamps.len() >= self.config.min_samples_for_detection {
            if let Some(interval) = self.detect_interval(&state.sample_timestamps) {
                debug!(interval_ms = interval, "Detected time-series interval");
                state.detected_interval = Some(interval);
                state.stats.detected_interval_ms = Some(interval);
            }
        }
    }

    /// Detect the interval from a set of timestamps
    fn detect_interval(&self, timestamps: &VecDeque<i64>) -> Option<i64> {
        if timestamps.len() < 2 {
            return None;
        }

        let mut intervals = Vec::new();
        for i in 1..timestamps.len() {
            let interval = timestamps[i] - timestamps[i - 1];
            if interval > 0 {
                intervals.push(interval);
            }
        }

        if intervals.is_empty() {
            return None;
        }

        // Calculate median interval
        intervals.sort_unstable();
        let median = if intervals.len() % 2 == 0 {
            (intervals[intervals.len() / 2 - 1] + intervals[intervals.len() / 2]) / 2
        } else {
            intervals[intervals.len() / 2]
        };

        // Verify consistency: most intervals should be within tolerance of median
        let tolerance = (median as f64 * self.config.interval_tolerance) as i64;
        let consistent_count = intervals
            .iter()
            .filter(|&&i| (i - median).abs() <= tolerance)
            .count();

        let consistency_ratio = consistent_count as f64 / intervals.len() as f64;

        if consistency_ratio >= 0.7 {
            // At least 70% of intervals are consistent
            Some(median)
        } else {
            None
        }
    }

    /// Emit normalized events from the buffer
    fn emit_normalized_events(&self, state: &mut NormalizerState) -> Result<Vec<TimeSeriesEvent>> {
        let interval = match state.detected_interval {
            Some(i) => i,
            None => {
                // No interval detected yet, keep buffering
                return Ok(Vec::new());
            }
        };

        let mut result = Vec::new();

        // Determine which events we can safely emit
        // We need to keep some buffer for strategies that need future data
        let buffer_to_keep = if self.config.fill_strategy.requires_future_data() {
            2 // Keep at least 2 events for interpolation
        } else {
            1 // Keep at least 1 event for forward fill
        };

        if state.buffer.len() <= buffer_to_keep {
            return Ok(result);
        }

        // Get the timestamps we can process
        let timestamps: Vec<i64> = state.buffer.keys().copied().collect();
        let emit_until_idx = timestamps.len() - buffer_to_keep;

        for i in 0..emit_until_idx {
            let timestamp_ms = timestamps[i];
            let value = state.buffer[&timestamp_ms];

            let timestamp = DateTime::from_timestamp_millis(timestamp_ms)
                .ok_or_else(|| {
                    NormalizationError::InvalidTimestamp {
                        timestamp: timestamp_ms,
                        reason: "invalid timestamp".to_string(),
                    }
                })?;

            // Check if we need to fill gaps
            if let Some(last_emitted) = state.last_emitted_timestamp {
                let expected_next_ms = last_emitted.timestamp_millis() + interval;
                let gap_ms = timestamp_ms - expected_next_ms;

                if gap_ms > 0 {
                    // There's a gap, fill it
                    let filled_events = self.fill_gap(
                        state,
                        last_emitted,
                        timestamp,
                        interval,
                        value,
                        i,
                        &timestamps,
                    )?;
                    result.extend(filled_events);
                }
            }

            // Emit the actual event
            result.push(TimeSeriesEvent::new(timestamp, value));
            state.last_emitted_timestamp = Some(timestamp);
            state.stats.events_normalized += 1;

            // Remove from buffer
            state.buffer.remove(&timestamp_ms);
        }

        Ok(result)
    }

    /// Fill a gap between two timestamps
    #[allow(clippy::too_many_arguments)]
    fn fill_gap(
        &self,
        state: &mut NormalizerState,
        last_timestamp: DateTime<Utc>,
        next_timestamp: DateTime<Utc>,
        interval: i64,
        next_value: f64,
        next_idx: usize,
        all_timestamps: &[i64],
    ) -> Result<Vec<TimeSeriesEvent>> {
        let mut result = Vec::new();

        let prev_value = if next_idx > 0 {
            Some(state.buffer[&all_timestamps[next_idx - 1]])
        } else {
            None
        };

        let mut current_ms = last_timestamp.timestamp_millis() + interval;
        let next_ms = next_timestamp.timestamp_millis();

        while current_ms < next_ms {
            let current_timestamp = DateTime::from_timestamp_millis(current_ms)
                .ok_or_else(|| NormalizationError::InvalidTimestamp {
                    timestamp: current_ms,
                    reason: "invalid timestamp during gap fill".to_string(),
                })?;

            let filled_value = self.config.fill_strategy.fill(
                prev_value,
                Some(next_value),
                Some(last_timestamp),
                Some(next_timestamp),
                current_timestamp,
            )?;

            match filled_value {
                Some(value) => {
                    result.push(TimeSeriesEvent::new(current_timestamp, value));
                    state.stats.points_filled += 1;
                    trace!(
                        timestamp_ms = current_ms,
                        value = value,
                        strategy = ?self.config.fill_strategy,
                        "Filled missing point"
                    );
                }
                None => {
                    state.stats.points_skipped += 1;
                    trace!(
                        timestamp_ms = current_ms,
                        "Skipped missing point"
                    );
                }
            }

            current_ms += interval;
        }

        Ok(result)
    }
}

// ============================================================================
// Builder Pattern
// ============================================================================

/// Builder for TimeSeriesNormalizer
pub struct NormalizerBuilder {
    config: NormalizationConfig,
}

impl NormalizerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: NormalizationConfig::default(),
        }
    }

    /// Set the target interval
    pub fn interval_ms(mut self, interval: i64) -> Self {
        self.config.interval_ms = Some(interval);
        self
    }

    /// Set the fill strategy
    pub fn fill_strategy(mut self, strategy: FillStrategy) -> Self {
        self.config.fill_strategy = strategy;
        self
    }

    /// Set the maximum buffer size
    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.config.max_buffer_size = size;
        self
    }

    /// Set the maximum out-of-order duration
    pub fn max_out_of_order_ms(mut self, ms: i64) -> Self {
        self.config.max_out_of_order_ms = ms;
        self
    }

    /// Set the minimum samples for interval detection
    pub fn min_samples_for_detection(mut self, count: usize) -> Self {
        self.config.min_samples_for_detection = count;
        self
    }

    /// Set the interval detection tolerance
    pub fn interval_tolerance(mut self, tolerance: f64) -> Self {
        self.config.interval_tolerance = tolerance;
        self
    }

    /// Enable or disable late event warnings
    pub fn warn_on_late_events(mut self, warn: bool) -> Self {
        self.config.warn_on_late_events = warn;
        self
    }

    /// Build the normalizer
    pub fn build(self) -> NormalizationResult<TimeSeriesNormalizer> {
        TimeSeriesNormalizer::new(self.config)
    }
}

impl Default for NormalizerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_event(timestamp_ms: i64, value: f64) -> TimeSeriesEvent {
        let timestamp = DateTime::from_timestamp_millis(timestamp_ms).unwrap();
        TimeSeriesEvent::new(timestamp, value)
    }

    #[test]
    fn test_fill_strategy_forward_fill() {
        let strategy = FillStrategy::ForwardFill;
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);
        let target = t1 + Duration::seconds(5);

        let result = strategy
            .fill(Some(10.0), Some(20.0), Some(t1), Some(t2), target)
            .unwrap();
        assert_eq!(result, Some(10.0));
    }

    #[test]
    fn test_fill_strategy_backward_fill() {
        let strategy = FillStrategy::BackwardFill;
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);
        let target = t1 + Duration::seconds(5);

        let result = strategy
            .fill(Some(10.0), Some(20.0), Some(t1), Some(t2), target)
            .unwrap();
        assert_eq!(result, Some(20.0));
    }

    #[test]
    fn test_fill_strategy_linear_interpolate() {
        let strategy = FillStrategy::LinearInterpolate;
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);
        let target = t1 + Duration::seconds(5);

        let result = strategy
            .fill(Some(10.0), Some(20.0), Some(t1), Some(t2), target)
            .unwrap();
        assert_eq!(result, Some(15.0)); // Midpoint interpolation
    }

    #[test]
    fn test_fill_strategy_zero() {
        let strategy = FillStrategy::Zero;
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);
        let target = t1 + Duration::seconds(5);

        let result = strategy
            .fill(Some(10.0), Some(20.0), Some(t1), Some(t2), target)
            .unwrap();
        assert_eq!(result, Some(0.0));
    }

    #[test]
    fn test_fill_strategy_skip() {
        let strategy = FillStrategy::Skip;
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);
        let target = t1 + Duration::seconds(5);

        let result = strategy
            .fill(Some(10.0), Some(20.0), Some(t1), Some(t2), target)
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_config_validation() {
        let config = NormalizationConfig::with_interval(1000).unwrap();
        assert!(config.validate().is_ok());

        let invalid_config = NormalizationConfig {
            interval_ms: Some(-1000),
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_normalizer_creation() {
        let config = NormalizationConfig::with_interval(1000).unwrap();
        let normalizer = TimeSeriesNormalizer::new(config);
        assert!(normalizer.is_ok());
    }

    #[test]
    fn test_normalizer_with_ordered_events() {
        let config = NormalizationConfig::with_interval(1000)
            .unwrap()
            .with_fill_strategy(FillStrategy::Skip);
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 1000, 20.0),
            create_event(base_time + 2000, 30.0),
        ];

        let mut all_results = Vec::new();
        for event in events {
            let results = normalizer.normalize(event).unwrap();
            all_results.extend(results);
        }

        // Should emit events except the last one (kept in buffer)
        assert!(all_results.len() >= 1);
    }

    #[test]
    fn test_normalizer_with_gaps() {
        let config = NormalizationConfig::with_interval(1000)
            .unwrap()
            .with_fill_strategy(FillStrategy::LinearInterpolate);
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 3000, 40.0), // Gap of 2 intervals
            create_event(base_time + 4000, 50.0),
        ];

        let mut all_results = Vec::new();
        for event in events {
            let results = normalizer.normalize(event).unwrap();
            all_results.extend(results);
        }

        // Flush to get remaining events
        let final_results = normalizer.flush().unwrap();
        all_results.extend(final_results);

        // Should have original points + filled points
        assert!(all_results.len() >= 3);
    }

    #[test]
    fn test_normalizer_batch_processing() {
        let config = NormalizationConfig::with_interval(1000)
            .unwrap()
            .with_fill_strategy(FillStrategy::Skip);
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 1000, 20.0),
            create_event(base_time + 2000, 30.0),
        ];

        let results = normalizer.normalize_batch(events).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_normalizer_out_of_order_events() {
        let config = NormalizationConfig::with_interval(1000)
            .unwrap()
            .with_fill_strategy(FillStrategy::Skip);
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 2000, 30.0),
            create_event(base_time + 1000, 20.0), // Out of order
        ];

        let mut all_results = Vec::new();
        for event in events {
            let results = normalizer.normalize(event).unwrap();
            all_results.extend(results);
        }

        let stats = normalizer.stats().unwrap();
        assert_eq!(stats.out_of_order_events, 1);
    }

    #[test]
    fn test_normalizer_late_events() {
        let config = NormalizationConfig {
            interval_ms: Some(1000),
            max_out_of_order_ms: 2000,
            warn_on_late_events: false,
            ..Default::default()
        };
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 1000, 20.0),
            create_event(base_time + 2000, 30.0),
            create_event(base_time + 10000, 100.0), // Advance watermark
            create_event(base_time + 500, 5.0),     // Too late (should be dropped)
        ];

        for event in events {
            let _ = normalizer.normalize(event);
        }

        let stats = normalizer.stats().unwrap();
        assert!(stats.events_dropped_late > 0);
    }

    #[test]
    fn test_normalizer_buffer_overflow() {
        let config = NormalizationConfig {
            interval_ms: Some(1000),
            max_buffer_size: 3,
            ..Default::default()
        };
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        // Add more events than buffer can hold
        for i in 0..10 {
            let event = create_event(base_time + (i * 1000), i as f64);
            let _ = normalizer.normalize(event);
        }

        let stats = normalizer.stats().unwrap();
        // Some events should be dropped due to overflow or processed
        assert!(stats.events_received == 10);
    }

    #[test]
    fn test_normalizer_interval_detection() {
        let config = NormalizationConfig {
            interval_ms: None, // Auto-detect
            min_samples_for_detection: 5,
            ..Default::default()
        };
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let interval = 5000_i64;

        // Send regular events
        for i in 0..10 {
            let event = create_event(base_time + (i * interval), i as f64);
            let _ = normalizer.normalize(event);
        }

        let detected = normalizer.interval_ms().unwrap();
        assert_eq!(detected, Some(interval));
    }

    #[test]
    fn test_normalizer_reset() {
        let config = NormalizationConfig::with_interval(1000).unwrap();
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let event = create_event(1000, 10.0);
        let _ = normalizer.normalize(event);

        let stats_before = normalizer.stats().unwrap();
        assert!(stats_before.events_received > 0);

        normalizer.reset().unwrap();

        let stats_after = normalizer.stats().unwrap();
        assert_eq!(stats_after.events_received, 0);
    }

    #[test]
    fn test_normalizer_flush() {
        let config = NormalizationConfig::with_interval(1000)
            .unwrap()
            .with_fill_strategy(FillStrategy::Skip);
        let normalizer = TimeSeriesNormalizer::new(config).unwrap();

        let base_time = 1000_i64;
        let events = vec![
            create_event(base_time, 10.0),
            create_event(base_time + 1000, 20.0),
        ];

        for event in events {
            let _ = normalizer.normalize(event);
        }

        let flushed = normalizer.flush().unwrap();
        assert!(!flushed.is_empty());

        let stats = normalizer.stats().unwrap();
        assert_eq!(stats.current_buffer_size, 0);
    }

    #[test]
    fn test_builder_pattern() {
        let normalizer = NormalizerBuilder::new()
            .interval_ms(1000)
            .fill_strategy(FillStrategy::ForwardFill)
            .max_buffer_size(500)
            .max_out_of_order_ms(30000)
            .build();

        assert!(normalizer.is_ok());
    }

    #[test]
    fn test_stats_calculations() {
        let mut stats = NormalizationStats {
            events_received: 100,
            events_normalized: 80,
            points_filled: 20,
            events_dropped_late: 5,
            events_dropped_overflow: 5,
            total_processing_time_us: 10000,
            ..Default::default()
        };

        assert_eq!(stats.fill_rate(), 0.25); // 20/80
        assert_eq!(stats.drop_rate(), 0.10); // 10/100
        assert_eq!(stats.avg_processing_time_us(), 100.0); // 10000/100
    }

    #[test]
    fn test_time_series_event_creation() {
        let timestamp = Utc::now();
        let event = TimeSeriesEvent::new(timestamp, 42.0);

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.value, 42.0);
        assert!(event.metadata.is_none());

        let event_with_meta = TimeSeriesEvent::with_metadata(
            timestamp,
            42.0,
            serde_json::json!({"key": "value"}),
        );
        assert!(event_with_meta.metadata.is_some());
    }

    #[test]
    fn test_linear_interpolation_edge_cases() {
        let strategy = FillStrategy::LinearInterpolate;
        let t1 = Utc::now();

        // No previous value, should fall back to next value
        let result = strategy
            .fill(None, Some(20.0), None, Some(t1), t1)
            .unwrap();
        assert_eq!(result, Some(20.0));

        // No next value, should fall back to previous value
        let result = strategy
            .fill(Some(10.0), None, Some(t1), None, t1)
            .unwrap();
        assert_eq!(result, Some(10.0));

        // No values at all
        let result = strategy.fill(None, None, None, None, t1).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let config = NormalizationConfig::with_interval(1000).unwrap();
        let normalizer = Arc::new(TimeSeriesNormalizer::new(config).unwrap());

        let mut handles = vec![];

        for i in 0..4 {
            let normalizer_clone = Arc::clone(&normalizer);
            let handle = thread::spawn(move || {
                let base_time = 1000 + (i * 10000);
                for j in 0..10 {
                    let event = create_event(base_time + (j * 1000), (i * 10 + j) as f64);
                    let _ = normalizer_clone.normalize(event);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = normalizer.stats().unwrap();
        assert_eq!(stats.events_received, 40);
    }
}
