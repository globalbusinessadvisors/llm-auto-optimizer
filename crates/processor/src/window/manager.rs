//! Window lifecycle management
//!
//! This module provides the `WindowManager` for managing the complete lifecycle of windows:
//! - Creating windows based on event timestamps
//! - Tracking active windows and their states
//! - Triggering window evaluation based on watermarks
//! - Cleaning up completed windows
//! - Merging session windows
//!
//! # Example
//!
//! ```rust
//! use processor::window::{
//!     WindowManager, TumblingWindowAssigner, OnWatermarkTrigger,
//! };
//! use processor::aggregation::{CompositeAggregator, AggregationResult};
//! use chrono::{Duration, Utc};
//!
//! # fn example() -> anyhow::Result<()> {
//! // Create a window manager with 5-minute tumbling windows
//! let assigner = TumblingWindowAssigner::new(Duration::minutes(5));
//! let trigger = OnWatermarkTrigger::new();
//! let mut manager = WindowManager::new(assigner, trigger);
//!
//! // Process events
//! let event_time = Utc::now();
//! let windows = manager.assign_event(event_time)?;
//!
//! // Check which windows should fire based on watermark
//! let watermark = Utc::now();
//! let ready_windows = manager.check_triggers(watermark)?;
//! # Ok(())
//! # }
//! ```

use super::{Window, WindowAssigner, WindowTrigger, TriggerContext, TriggerResult};
use crate::error::{ProcessorError, WindowError};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Statistics tracked for each window
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WindowStats {
    /// Number of events assigned to this window
    pub event_count: u64,
    /// Timestamp when the window was first created
    pub created_at: DateTime<Utc>,
    /// Timestamp when the window was last updated
    pub last_update: DateTime<Utc>,
    /// Timestamp when the window was closed (if closed)
    pub closed_at: Option<DateTime<Utc>>,
    /// Minimum event timestamp seen
    pub min_event_time: Option<DateTime<Utc>>,
    /// Maximum event timestamp seen
    pub max_event_time: Option<DateTime<Utc>>,
}

impl WindowStats {
    /// Create new window statistics
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            event_count: 0,
            created_at: now,
            last_update: now,
            closed_at: None,
            min_event_time: None,
            max_event_time: None,
        }
    }

    /// Update statistics with a new event
    pub fn record_event(&mut self, event_time: DateTime<Utc>) {
        self.event_count += 1;
        self.last_update = Utc::now();

        self.min_event_time = Some(
            self.min_event_time
                .map(|t| t.min(event_time))
                .unwrap_or(event_time)
        );

        self.max_event_time = Some(
            self.max_event_time
                .map(|t| t.max(event_time))
                .unwrap_or(event_time)
        );
    }

    /// Mark the window as closed
    pub fn close(&mut self) {
        self.closed_at = Some(Utc::now());
    }

    /// Check if the window is closed
    pub fn is_closed(&self) -> bool {
        self.closed_at.is_some()
    }
}

/// Metadata for an active window
#[derive(Debug, Clone)]
pub struct WindowMetadata {
    /// The window definition
    pub window: Window,
    /// Statistics for this window
    pub stats: WindowStats,
    /// Whether this window has been triggered
    pub triggered: bool,
}

impl WindowMetadata {
    /// Create new window metadata
    pub fn new(window: Window) -> Self {
        Self {
            window,
            stats: WindowStats::new(),
            triggered: false,
        }
    }

    /// Update with a new event
    pub fn record_event(&mut self, event_time: DateTime<Utc>) {
        self.stats.record_event(event_time);
    }

    /// Check if the window should be triggered
    pub fn check_trigger<T: WindowTrigger>(
        &mut self,
        trigger: &mut T,
        watermark: Option<DateTime<Utc>>,
    ) -> TriggerResult {
        if self.triggered {
            return TriggerResult::Continue;
        }

        let ctx = TriggerContext {
            window: self.window.clone(),
            event_time: self.stats.max_event_time,
            processing_time: Utc::now(),
            watermark,
            element_count: self.stats.event_count as usize,
        };

        let result = if let Some(wm) = watermark {
            trigger.on_watermark(&ctx)
        } else {
            trigger.on_element(&ctx)
        };

        if result == TriggerResult::Fire || result == TriggerResult::FireAndPurge {
            self.triggered = true;
        }

        result
    }

    /// Close this window
    pub fn close(&mut self) {
        self.stats.close();
    }
}

/// Manager for window lifecycles
///
/// The `WindowManager` coordinates all aspects of window management:
/// - Assigns events to appropriate windows
/// - Tracks active windows and their states
/// - Evaluates triggers to determine when windows should fire
/// - Handles window merging for session windows
/// - Cleans up completed windows
pub struct WindowManager<A, T>
where
    A: WindowAssigner,
    T: WindowTrigger,
{
    /// Window assigner for determining event-to-window mapping
    assigner: A,
    /// Trigger for determining when windows should fire
    trigger: T,
    /// Active windows indexed by window ID
    active_windows: Arc<DashMap<String, WindowMetadata>>,
    /// Windows ordered by end time for efficient watermark processing
    windows_by_end_time: BTreeMap<i64, Vec<String>>,
    /// Current watermark
    current_watermark: Option<DateTime<Utc>>,
    /// Configuration
    allow_late_events: bool,
    late_event_threshold: chrono::Duration,
}

impl<A, T> WindowManager<A, T>
where
    A: WindowAssigner,
    T: WindowTrigger,
{
    /// Create a new window manager
    pub fn new(assigner: A, trigger: T) -> Self {
        Self {
            assigner,
            trigger,
            active_windows: Arc::new(DashMap::new()),
            windows_by_end_time: BTreeMap::new(),
            current_watermark: None,
            allow_late_events: true,
            late_event_threshold: chrono::Duration::minutes(5),
        }
    }

    /// Create a window manager with custom late event handling
    pub fn with_late_events(
        assigner: A,
        trigger: T,
        allow_late_events: bool,
        late_event_threshold: chrono::Duration,
    ) -> Self {
        Self {
            assigner,
            trigger,
            active_windows: Arc::new(DashMap::new()),
            windows_by_end_time: BTreeMap::new(),
            current_watermark: None,
            allow_late_events,
            late_event_threshold,
        }
    }

    /// Assign an event to windows and update window metadata
    ///
    /// Returns the list of windows that this event belongs to.
    pub fn assign_event(
        &mut self,
        event_time: DateTime<Utc>,
    ) -> Result<Vec<Window>, ProcessorError> {
        // Check for late events
        if let Some(watermark) = self.current_watermark {
            if event_time < watermark {
                let late_by = watermark - event_time;

                if !self.allow_late_events {
                    return Err(WindowError::LateEvent {
                        event_time: event_time.timestamp_millis(),
                        watermark: watermark.timestamp_millis(),
                        late_by: late_by.num_milliseconds(),
                    }.into());
                }

                if late_by > self.late_event_threshold {
                    warn!(
                        event_time = %event_time,
                        watermark = %watermark,
                        late_by_ms = late_by.num_milliseconds(),
                        "Event is too late, dropping"
                    );
                    return Ok(Vec::new());
                }

                debug!(
                    event_time = %event_time,
                    watermark = %watermark,
                    late_by_ms = late_by.num_milliseconds(),
                    "Processing late event"
                );
            }
        }

        // Assign event to windows
        let windows = self.assigner.assign_windows(event_time);

        trace!(
            event_time = %event_time,
            window_count = windows.len(),
            "Assigned event to windows"
        );

        // Update or create window metadata
        for window in &windows {
            let mut metadata = self.active_windows
                .entry(window.id.clone())
                .or_insert_with(|| {
                    // Track window by end time
                    let end_millis = window.bounds.end.timestamp_millis();
                    self.windows_by_end_time
                        .entry(end_millis)
                        .or_insert_with(Vec::new)
                        .push(window.id.clone());

                    debug!(
                        window_id = %window.id,
                        window_start = %window.bounds.start,
                        window_end = %window.bounds.end,
                        "Created new window"
                    );

                    WindowMetadata::new(window.clone())
                });

            metadata.record_event(event_time);
        }

        Ok(windows)
    }

    /// Update the watermark and check which windows are ready to fire
    ///
    /// Returns a list of window IDs that should be evaluated.
    pub fn advance_watermark(
        &mut self,
        watermark: DateTime<Utc>,
    ) -> Result<Vec<String>, ProcessorError> {
        // Ensure watermark doesn't go backwards
        if let Some(current) = self.current_watermark {
            if watermark < current {
                warn!(
                    current = %current,
                    new_watermark = %watermark,
                    "Watermark went backwards, ignoring"
                );
                return Ok(Vec::new());
            }
        }

        debug!(
            old_watermark = ?self.current_watermark,
            new_watermark = %watermark,
            "Advancing watermark"
        );

        self.current_watermark = Some(watermark);

        // Find windows that should fire based on watermark
        let mut ready_windows = Vec::new();
        let watermark_millis = watermark.timestamp_millis();

        // Check all windows with end time <= watermark
        let ended_windows: Vec<_> = self.windows_by_end_time
            .range(..=watermark_millis)
            .flat_map(|(_, window_ids)| window_ids.clone())
            .collect();

        for window_id in ended_windows {
            if let Some(mut metadata) = self.active_windows.get_mut(&window_id) {
                let result = metadata.check_trigger(&mut self.trigger, Some(watermark));

                match result {
                    TriggerResult::Fire | TriggerResult::FireAndPurge => {
                        debug!(
                            window_id = %window_id,
                            trigger_result = ?result,
                            "Window ready to fire"
                        );
                        ready_windows.push(window_id.clone());
                    }
                    _ => {}
                }
            }
        }

        Ok(ready_windows)
    }

    /// Get metadata for a specific window
    pub fn get_window(&self, window_id: &str) -> Option<WindowMetadata> {
        self.active_windows.get(window_id).map(|m| m.clone())
    }

    /// Remove and return a window (for cleanup after processing)
    pub fn remove_window(&mut self, window_id: &str) -> Option<WindowMetadata> {
        // Remove from active windows
        let metadata = self.active_windows.remove(window_id).map(|(_, m)| m)?;

        // Remove from end time index
        let end_millis = metadata.window.bounds.end.timestamp_millis();
        if let Some(window_ids) = self.windows_by_end_time.get_mut(&end_millis) {
            window_ids.retain(|id| id != window_id);
            if window_ids.is_empty() {
                self.windows_by_end_time.remove(&end_millis);
            }
        }

        debug!(
            window_id = %window_id,
            "Removed window"
        );

        Some(metadata)
    }

    /// Get all active window IDs
    pub fn active_window_ids(&self) -> Vec<String> {
        self.active_windows.iter().map(|e| e.key().clone()).collect()
    }

    /// Get the number of active windows
    pub fn active_window_count(&self) -> usize {
        self.active_windows.len()
    }

    /// Get the current watermark
    pub fn current_watermark(&self) -> Option<DateTime<Utc>> {
        self.current_watermark
    }

    /// Clean up old windows that have been triggered and are beyond retention
    ///
    /// Returns the number of windows removed.
    pub fn cleanup_old_windows(
        &mut self,
        retention_duration: chrono::Duration,
    ) -> usize {
        let cutoff = Utc::now() - retention_duration;
        let mut removed_count = 0;

        // Find windows to remove
        let to_remove: Vec<String> = self.active_windows
            .iter()
            .filter(|entry| {
                let metadata = entry.value();
                metadata.triggered && metadata.window.bounds.end < cutoff
            })
            .map(|entry| entry.key().clone())
            .collect();

        // Remove them
        for window_id in to_remove {
            if self.remove_window(&window_id).is_some() {
                removed_count += 1;
            }
        }

        if removed_count > 0 {
            debug!(
                removed_count,
                cutoff = %cutoff,
                "Cleaned up old windows"
            );
        }

        removed_count
    }

    /// Get statistics for all active windows
    pub fn get_statistics(&self) -> HashMap<String, WindowStats> {
        self.active_windows
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().stats.clone()))
            .collect()
    }

    /// Reset the manager (clear all windows)
    pub fn reset(&mut self) {
        self.active_windows.clear();
        self.windows_by_end_time.clear();
        self.current_watermark = None;
        debug!("Window manager reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::{TumblingWindowAssigner, SlidingWindowAssigner, OnWatermarkTrigger};
    use chrono::{Duration, TimeZone};

    fn create_timestamp(millis: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(millis).unwrap()
    }

    #[test]
    fn test_window_manager_basic() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Assign event at 500ms
        let windows = manager.assign_event(create_timestamp(500)).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(manager.active_window_count(), 1);

        // Check the window
        let window_id = &windows[0].id;
        let metadata = manager.get_window(window_id).unwrap();
        assert_eq!(metadata.stats.event_count, 1);
        assert!(!metadata.triggered);
    }

    #[test]
    fn test_window_manager_watermark_trigger() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Assign events
        manager.assign_event(create_timestamp(100)).unwrap();
        manager.assign_event(create_timestamp(500)).unwrap();
        manager.assign_event(create_timestamp(900)).unwrap();

        // Advance watermark to trigger window
        let ready = manager.advance_watermark(create_timestamp(1000)).unwrap();
        assert_eq!(ready.len(), 1);

        // Check window is marked as triggered
        let window_id = &ready[0];
        let metadata = manager.get_window(window_id).unwrap();
        assert!(metadata.triggered);
        assert_eq!(metadata.stats.event_count, 3);
    }

    #[test]
    fn test_window_manager_multiple_windows() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Assign events to different windows
        manager.assign_event(create_timestamp(500)).unwrap();
        manager.assign_event(create_timestamp(1500)).unwrap();
        manager.assign_event(create_timestamp(2500)).unwrap();

        assert_eq!(manager.active_window_count(), 3);

        // Advance watermark to trigger first window
        let ready = manager.advance_watermark(create_timestamp(1000)).unwrap();
        assert_eq!(ready.len(), 1);

        // Advance to trigger second window
        let ready = manager.advance_watermark(create_timestamp(2000)).unwrap();
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn test_window_manager_sliding_windows() {
        let assigner = SlidingWindowAssigner::new(
            Duration::milliseconds(1000),
            Duration::milliseconds(500),
        );
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Event at 700ms should be in 2 windows
        let windows = manager.assign_event(create_timestamp(700)).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(manager.active_window_count(), 2);

        // Both windows should have the event
        for window in windows {
            let metadata = manager.get_window(&window.id).unwrap();
            assert_eq!(metadata.stats.event_count, 1);
        }
    }

    #[test]
    fn test_window_manager_late_events() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Advance watermark
        manager.advance_watermark(create_timestamp(1000)).unwrap();

        // Try to process late event (within threshold)
        let windows = manager.assign_event(create_timestamp(900)).unwrap();
        assert_eq!(windows.len(), 1);

        // Try to process very late event (beyond threshold)
        manager.advance_watermark(create_timestamp(10000)).unwrap();
        let windows = manager.assign_event(create_timestamp(500)).unwrap();
        assert_eq!(windows.len(), 0); // Should be dropped
    }

    #[test]
    fn test_window_manager_late_events_disallowed() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::with_late_events(
            assigner,
            trigger,
            false, // Don't allow late events
            Duration::minutes(5),
        );

        // Advance watermark
        manager.advance_watermark(create_timestamp(1000)).unwrap();

        // Try to process late event
        let result = manager.assign_event(create_timestamp(900));
        assert!(result.is_err());
    }

    #[test]
    fn test_window_manager_cleanup() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Create and trigger windows
        manager.assign_event(create_timestamp(500)).unwrap();
        manager.assign_event(create_timestamp(1500)).unwrap();

        manager.advance_watermark(create_timestamp(1000)).unwrap();
        manager.advance_watermark(create_timestamp(2000)).unwrap();

        assert_eq!(manager.active_window_count(), 2);

        // Cleanup old windows (both should be old enough)
        let removed = manager.cleanup_old_windows(Duration::milliseconds(0));
        assert_eq!(removed, 2);
        assert_eq!(manager.active_window_count(), 0);
    }

    #[test]
    fn test_window_manager_remove_window() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        let windows = manager.assign_event(create_timestamp(500)).unwrap();
        let window_id = &windows[0].id;

        assert_eq!(manager.active_window_count(), 1);

        let removed = manager.remove_window(window_id);
        assert!(removed.is_some());
        assert_eq!(manager.active_window_count(), 0);

        // Try to remove again
        let removed = manager.remove_window(window_id);
        assert!(removed.is_none());
    }

    #[test]
    fn test_window_stats() {
        let mut stats = WindowStats::new();

        assert_eq!(stats.event_count, 0);
        assert!(stats.min_event_time.is_none());
        assert!(stats.max_event_time.is_none());
        assert!(!stats.is_closed());

        stats.record_event(create_timestamp(500));
        stats.record_event(create_timestamp(1000));
        stats.record_event(create_timestamp(300));

        assert_eq!(stats.event_count, 3);
        assert_eq!(stats.min_event_time, Some(create_timestamp(300)));
        assert_eq!(stats.max_event_time, Some(create_timestamp(1000)));

        stats.close();
        assert!(stats.is_closed());
    }

    #[test]
    fn test_window_manager_statistics() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        // Add events to multiple windows
        manager.assign_event(create_timestamp(100)).unwrap();
        manager.assign_event(create_timestamp(200)).unwrap();
        manager.assign_event(create_timestamp(1500)).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.len(), 2);

        // Check first window stats
        let first_window_stats = stats.values()
            .find(|s| s.event_count == 2)
            .unwrap();
        assert_eq!(first_window_stats.min_event_time, Some(create_timestamp(100)));
        assert_eq!(first_window_stats.max_event_time, Some(create_timestamp(200)));
    }

    #[test]
    fn test_window_manager_reset() {
        let assigner = TumblingWindowAssigner::new(Duration::milliseconds(1000));
        let trigger = OnWatermarkTrigger::new();
        let mut manager = WindowManager::new(assigner, trigger);

        manager.assign_event(create_timestamp(500)).unwrap();
        manager.advance_watermark(create_timestamp(1000)).unwrap();

        assert_eq!(manager.active_window_count(), 1);
        assert!(manager.current_watermark().is_some());

        manager.reset();

        assert_eq!(manager.active_window_count(), 0);
        assert!(manager.current_watermark().is_none());
    }
}
