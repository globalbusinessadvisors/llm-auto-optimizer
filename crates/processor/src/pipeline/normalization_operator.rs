//! Normalization operator for time-series data alignment
//!
//! This module provides the `NormalizationOperator` which aligns events to fixed intervals,
//! fills missing data points, and handles out-of-order events.

use crate::config::{AlignmentStrategy, FillStrategy, NormalizationConfig};
use crate::error::Result;
use crate::pipeline::operator::{OperatorContext, StreamOperator};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};

/// Event with timestamp for normalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedEvent {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

/// Statistics for normalization operations
#[derive(Debug, Clone, Default)]
pub struct NormalizationStats {
    /// Total events processed
    pub events_processed: u64,
    /// Events aligned to intervals
    pub events_aligned: u64,
    /// Missing intervals filled
    pub intervals_filled: u64,
    /// Outliers detected and dropped
    pub outliers_dropped: u64,
    /// Out-of-order events reordered
    pub out_of_order_events: u64,
    /// Events dropped (buffer overflow or other reasons)
    pub events_dropped: u64,
}

/// Normalization operator for time-series data
///
/// This operator normalizes time-series data by:
/// - Aligning event timestamps to fixed intervals
/// - Buffering and reordering out-of-order events
/// - Filling missing data points using various strategies
/// - Detecting and removing outliers
/// - Emitting aligned events at consistent intervals
///
/// # Example
///
/// ```rust
/// use processor::pipeline::operator::NormalizationOperator;
/// use processor::config::{NormalizationConfig, FillStrategy, AlignmentStrategy};
/// use std::time::Duration;
///
/// let config = NormalizationConfig::new()
///     .enabled()
///     .with_interval(Duration::from_secs(5))
///     .with_fill_strategy(FillStrategy::LinearInterpolation)
///     .with_alignment(AlignmentStrategy::Start);
///
/// let operator = NormalizationOperator::new("normalization", config);
/// ```
#[derive(Debug, Clone)]
pub struct NormalizationOperator {
    name: String,
    config: NormalizationConfig,
    // Buffer for out-of-order events (sorted by timestamp)
    buffer: Arc<RwLock<BTreeMap<i64, Vec<f64>>>>,
    // Last emitted timestamp
    last_emitted: Arc<RwLock<Option<DateTime<Utc>>>>,
    // Statistics
    stats: Arc<RwLock<NormalizationStats>>,
    // Recent values for outlier detection
    recent_values: Arc<RwLock<VecDeque<f64>>>,
}

impl NormalizationOperator {
    /// Create a new normalization operator
    pub fn new<S: Into<String>>(name: S, config: NormalizationConfig) -> Self {
        Self {
            name: name.into(),
            config,
            buffer: Arc::new(RwLock::new(BTreeMap::new())),
            last_emitted: Arc::new(RwLock::new(None)),
            stats: Arc::new(RwLock::new(NormalizationStats::default())),
            recent_values: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
        }
    }

    /// Align a timestamp to the configured interval
    fn align_timestamp(&self, timestamp: DateTime<Utc>) -> DateTime<Utc> {
        let interval_ms = self.config.interval.as_millis() as i64;
        let timestamp_ms = timestamp.timestamp_millis();

        let aligned_ms = match self.config.alignment {
            AlignmentStrategy::Start => {
                // Round down to interval boundary
                (timestamp_ms / interval_ms) * interval_ms
            }
            AlignmentStrategy::End => {
                // Round up to interval boundary
                ((timestamp_ms + interval_ms - 1) / interval_ms) * interval_ms
            }
            AlignmentStrategy::Center => {
                // Round to interval and add half interval
                let base = (timestamp_ms / interval_ms) * interval_ms;
                base + interval_ms / 2
            }
            AlignmentStrategy::Nearest => {
                // Round to nearest interval boundary
                ((timestamp_ms + interval_ms / 2) / interval_ms) * interval_ms
            }
        };

        DateTime::from_timestamp_millis(aligned_ms)
            .unwrap_or(timestamp)
    }

    /// Check if a value is an outlier using z-score
    fn is_outlier(&self, value: f64) -> bool {
        if !self.config.drop_outliers {
            return false;
        }

        let recent = self.recent_values.read().unwrap();
        if recent.len() < 3 {
            // Not enough data to determine outliers
            return false;
        }

        // Calculate mean and standard deviation
        let sum: f64 = recent.iter().sum();
        let mean = sum / recent.len() as f64;

        let variance: f64 = recent.iter()
            .map(|&v| (v - mean).powi(2))
            .sum::<f64>() / recent.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev < 1e-10 {
            // No variance, can't detect outliers
            return false;
        }

        // Calculate z-score
        let z_score = ((value - mean) / std_dev).abs();
        z_score > self.config.outlier_threshold
    }

    /// Update recent values buffer for outlier detection
    fn update_recent_values(&self, value: f64) {
        let mut recent = self.recent_values.write().unwrap();
        recent.push_back(value);
        if recent.len() > 100 {
            recent.pop_front();
        }
    }

    /// Fill missing intervals between two timestamps
    fn fill_missing_intervals(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        prev_value: Option<f64>,
        next_value: Option<f64>,
    ) -> Vec<TimestampedEvent> {
        let mut filled = Vec::new();
        let interval = self.config.interval;

        let mut current = start + interval;
        while current < end {
            let value = match self.config.fill_strategy {
                FillStrategy::Zero => 0.0,
                FillStrategy::ForwardFill => prev_value.unwrap_or(0.0),
                FillStrategy::BackwardFill => next_value.unwrap_or(0.0),
                FillStrategy::LinearInterpolation => {
                    if let (Some(prev), Some(next)) = (prev_value, next_value) {
                        // Linear interpolation
                        let total_gap = (end.timestamp_millis() - start.timestamp_millis()) as f64;
                        let current_offset = (current.timestamp_millis() - start.timestamp_millis()) as f64;
                        let ratio = current_offset / total_gap;
                        prev + (next - prev) * ratio
                    } else {
                        prev_value.or(next_value).unwrap_or(0.0)
                    }
                }
                FillStrategy::Drop => {
                    // Don't fill, skip to next
                    current = current + interval;
                    continue;
                }
                FillStrategy::Constant => {
                    // Could be configurable, defaulting to 0 for now
                    0.0
                }
                FillStrategy::Mean => {
                    if let (Some(prev), Some(next)) = (prev_value, next_value) {
                        (prev + next) / 2.0
                    } else {
                        prev_value.or(next_value).unwrap_or(0.0)
                    }
                }
            };

            filled.push(TimestampedEvent {
                timestamp: current,
                value,
            });

            current = current + interval;
        }

        filled
    }

    /// Get normalization statistics
    pub fn stats(&self) -> NormalizationStats {
        self.stats.read().unwrap().clone()
    }

    /// Reset the operator state
    pub fn reset(&self) {
        self.buffer.write().unwrap().clear();
        *self.last_emitted.write().unwrap() = None;
        *self.stats.write().unwrap() = NormalizationStats::default();
        self.recent_values.write().unwrap().clear();
    }
}

#[async_trait]
impl StreamOperator for NormalizationOperator {
    async fn process(
        &self,
        input: Vec<u8>,
        _ctx: &OperatorContext,
    ) -> Result<Vec<Vec<u8>>> {
        // If normalization is disabled, pass through
        if !self.config.enabled {
            return Ok(vec![input]);
        }

        // Deserialize the event
        let event: TimestampedEvent = bincode::deserialize(&input)?;

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.events_processed += 1;
        }

        // Check for outliers
        if self.is_outlier(event.value) {
            let mut stats = self.stats.write().unwrap();
            stats.outliers_dropped += 1;
            return Ok(vec![]); // Drop outlier
        }

        // Update recent values for outlier detection
        self.update_recent_values(event.value);

        // Align timestamp
        let aligned_timestamp = self.align_timestamp(event.timestamp);
        let aligned_ms = aligned_timestamp.timestamp_millis();

        // Add to buffer
        {
            let mut buffer = self.buffer.write().unwrap();
            buffer.entry(aligned_ms)
                .or_insert_with(Vec::new)
                .push(event.value);

            // Check for buffer overflow
            if buffer.len() > self.config.buffer_size {
                // Remove oldest entries
                if let Some(&oldest_key) = buffer.keys().next() {
                    buffer.remove(&oldest_key);
                    let mut stats = self.stats.write().unwrap();
                    stats.events_dropped += 1;
                }
            }
        }

        // Check if we can emit aligned events
        let mut output = Vec::new();
        let last_emitted = self.last_emitted.read().unwrap().clone();

        // Emit all buffered events up to current time - interval
        // (to allow for late arrivals within the buffer window)
        let emit_threshold = aligned_timestamp - self.config.interval;

        let events_to_emit: Vec<(DateTime<Utc>, f64)> = {
            let buffer = self.buffer.read().unwrap();
            buffer.iter()
                .filter_map(|(&ts_ms, values)| {
                    let ts = DateTime::from_timestamp_millis(ts_ms)?;
                    if ts <= emit_threshold {
                        // Average multiple values at same timestamp
                        let avg = values.iter().sum::<f64>() / values.len() as f64;
                        Some((ts, avg))
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Remove emitted events from buffer
        {
            let mut buffer = self.buffer.write().unwrap();
            for (ts, _) in &events_to_emit {
                buffer.remove(&ts.timestamp_millis());
            }
        }

        // Fill missing intervals if needed
        for (ts, value) in events_to_emit {
            // Check for gaps and fill if necessary
            if let Some(last) = last_emitted {
                let gap = ts.signed_duration_since(last);
                let interval_chrono = chrono::Duration::from_std(self.config.interval)
                    .unwrap_or(chrono::Duration::seconds(60));
                if gap > interval_chrono {
                    // Fill the gap
                    let filled = self.fill_missing_intervals(
                        last,
                        ts,
                        None, // We don't have the previous value stored
                        Some(value),
                    );

                    let mut stats = self.stats.write().unwrap();
                    stats.intervals_filled += filled.len() as u64;

                    for filled_event in filled {
                        let serialized = bincode::serialize(&filled_event)?;
                        output.push(serialized);
                    }
                }
            }

            // Emit the aligned event
            let aligned_event = TimestampedEvent {
                timestamp: ts,
                value,
            };

            let serialized = bincode::serialize(&aligned_event)?;
            output.push(serialized);

            // Update last emitted
            *self.last_emitted.write().unwrap() = Some(ts);

            let mut stats = self.stats.write().unwrap();
            stats.events_aligned += 1;
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn open(&mut self) -> Result<()> {
        self.reset();
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        // Emit any remaining buffered events
        self.buffer.write().unwrap().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watermark::Watermark;

    #[tokio::test]
    async fn test_normalization_operator_disabled() {
        let config = NormalizationConfig::default(); // disabled by default
        let operator = NormalizationOperator::new("test", config);
        let ctx = OperatorContext::new(Watermark::min(), 0);

        let event = TimestampedEvent {
            timestamp: Utc::now(),
            value: 42.0,
        };

        let input = bincode::serialize(&event).unwrap();
        let output = operator.process(input.clone(), &ctx).await.unwrap();

        // Should pass through when disabled
        assert_eq!(output.len(), 1);
        assert_eq!(output[0], input);
    }

    #[tokio::test]
    async fn test_normalization_operator_alignment() {
        let config = NormalizationConfig::new()
            .enabled()
            .with_interval(Duration::from_secs(5).to_std().unwrap())
            .with_alignment(AlignmentStrategy::Start);

        let operator = NormalizationOperator::new("test", config);

        // Test alignment
        let ts = DateTime::from_timestamp(1234567892, 0).unwrap(); // 2009-02-13 23:31:32
        let aligned = operator.align_timestamp(ts);

        // Should align to 2009-02-13 23:31:30 (nearest 5-second boundary)
        assert_eq!(aligned.timestamp() % 5, 0);
    }

    #[tokio::test]
    async fn test_normalization_operator_stats() {
        let config = NormalizationConfig::new()
            .enabled()
            .with_interval(Duration::from_secs(1).to_std().unwrap());

        let operator = NormalizationOperator::new("test", config);
        let ctx = OperatorContext::new(Watermark::min(), 0);

        let event = TimestampedEvent {
            timestamp: Utc::now(),
            value: 42.0,
        };

        let input = bincode::serialize(&event).unwrap();
        operator.process(input, &ctx).await.unwrap();

        let stats = operator.stats();
        assert_eq!(stats.events_processed, 1);
    }

    #[tokio::test]
    async fn test_normalization_operator_outlier_detection() {
        let config = NormalizationConfig::new()
            .enabled()
            .with_interval(Duration::from_secs(1).to_std().unwrap())
            .with_outlier_detection(2.0);

        let operator = NormalizationOperator::new("test", config);
        let ctx = OperatorContext::new(Watermark::min(), 0);

        // Add some normal values
        for i in 1..=10 {
            let event = TimestampedEvent {
                timestamp: Utc::now(),
                value: 10.0 + i as f64,
            };
            let input = bincode::serialize(&event).unwrap();
            operator.process(input, &ctx).await.unwrap();
        }

        // Add an outlier
        let outlier = TimestampedEvent {
            timestamp: Utc::now(),
            value: 1000.0,
        };
        let input = bincode::serialize(&outlier).unwrap();
        operator.process(input, &ctx).await.unwrap();

        let stats = operator.stats();
        assert!(stats.outliers_dropped > 0);
    }

    #[test]
    fn test_alignment_strategies() {
        let timestamp = DateTime::from_timestamp(1234567892, 0).unwrap();

        // Test Start alignment
        let config = NormalizationConfig::new()
            .with_interval(Duration::from_secs(5).to_std().unwrap())
            .with_alignment(AlignmentStrategy::Start);
        let operator = NormalizationOperator::new("test", config);
        let aligned = operator.align_timestamp(timestamp);
        assert_eq!(aligned.timestamp() % 5, 0);
        assert!(aligned <= timestamp);

        // Test End alignment
        let config = NormalizationConfig::new()
            .with_interval(Duration::from_secs(5).to_std().unwrap())
            .with_alignment(AlignmentStrategy::End);
        let operator = NormalizationOperator::new("test", config);
        let aligned = operator.align_timestamp(timestamp);
        assert_eq!(aligned.timestamp() % 5, 0);
        assert!(aligned >= timestamp);
    }

    #[test]
    fn test_normalization_operator_reset() {
        let config = NormalizationConfig::new().enabled();
        let operator = NormalizationOperator::new("test", config);

        // Add some state
        operator.update_recent_values(42.0);

        // Reset
        operator.reset();

        let stats = operator.stats();
        assert_eq!(stats.events_processed, 0);
    }
}
