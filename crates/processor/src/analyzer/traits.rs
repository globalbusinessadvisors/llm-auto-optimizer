//! Core traits for the analyzer framework
//!
//! This module defines the `Analyzer` trait that all analyzers must implement,
//! along with lifecycle management and configuration traits.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

use super::types::{AnalyzerEvent, AnalysisReport, AnalyzerStats};

/// Result type for analyzer operations
pub type AnalyzerResult<T> = Result<T, AnalyzerError>;

/// Errors that can occur during analysis
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum AnalyzerError {
    /// Analysis failed due to invalid input
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Analysis failed due to insufficient data
    #[error("Insufficient data: {0}")]
    InsufficientData(String),

    /// Analysis failed due to configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Analysis failed due to resource exhaustion
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Analysis failed due to internal error
    #[error("Internal error: {0}")]
    InternalError(String),

    /// Analyzer is not in the correct state
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Timeout occurred during analysis
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Analyzer is not running
    #[error("Analyzer is not running")]
    NotRunning,

    /// Invalid state transition attempted
    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition {
        from: String,
        to: String,
    },
}

/// Lifecycle state of an analyzer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnalyzerState {
    /// Analyzer is initialized but not started
    Initialized,
    /// Analyzer is starting up
    Starting,
    /// Analyzer is running and processing events
    Running,
    /// Analyzer is draining remaining events before stopping
    Draining,
    /// Analyzer has stopped
    Stopped,
    /// Analyzer encountered an error
    Failed,
}

impl AnalyzerState {
    /// Check if the analyzer can accept new events
    pub fn can_accept_events(&self) -> bool {
        matches!(self, AnalyzerState::Running)
    }

    /// Check if the analyzer is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, AnalyzerState::Stopped | AnalyzerState::Failed)
    }

    /// Convert the state to a string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            AnalyzerState::Initialized => "Initialized",
            AnalyzerState::Starting => "Starting",
            AnalyzerState::Running => "Running",
            AnalyzerState::Draining => "Draining",
            AnalyzerState::Stopped => "Stopped",
            AnalyzerState::Failed => "Failed",
        }
    }
}

/// Configuration for an analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Unique identifier for this analyzer instance
    pub id: String,

    /// Whether the analyzer is enabled
    pub enabled: bool,

    /// Events to filter and process (empty = all events)
    pub event_filter: Vec<String>,

    /// Window duration for sliding window analysis
    pub window_duration: Duration,

    /// Slide interval for sliding windows
    pub slide_interval: Duration,

    /// Sample rate (0.0-1.0) for high-volume events
    pub sample_rate: f64,

    /// Maximum events to buffer before backpressure
    pub max_buffer_size: usize,

    /// Maximum memory usage in bytes
    pub max_memory_bytes: usize,

    /// Analysis interval (how often to generate reports)
    pub analysis_interval: Duration,

    /// Enable detailed tracing
    pub enable_tracing: bool,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            enabled: true,
            event_filter: Vec::new(),
            window_duration: Duration::from_secs(300), // 5 minutes
            slide_interval: Duration::from_secs(60),   // 1 minute
            sample_rate: 1.0,                           // No sampling
            max_buffer_size: 10_000,
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
            analysis_interval: Duration::from_secs(60), // 1 minute
            enable_tracing: false,
        }
    }
}

/// Core trait that all analyzers must implement
///
/// This trait defines the interface for real-time analysis of LLM optimization metrics.
/// Analyzers receive events, maintain state, perform analysis, and generate reports
/// with insights and recommendations.
#[async_trait]
pub trait Analyzer: Send + Sync {
    /// Get the analyzer's unique name
    fn name(&self) -> &str;

    /// Get the analyzer's configuration
    fn config(&self) -> &AnalyzerConfig;

    /// Get the current state of the analyzer
    fn state(&self) -> AnalyzerState;

    /// Start the analyzer
    ///
    /// This initializes internal state and prepares the analyzer to receive events.
    ///
    /// # Errors
    ///
    /// Returns an error if the analyzer is already running or if initialization fails.
    async fn start(&mut self) -> AnalyzerResult<()>;

    /// Stop the analyzer gracefully
    ///
    /// This drains any remaining events and performs cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the analyzer is already stopped or if cleanup fails.
    async fn stop(&mut self) -> AnalyzerResult<()>;

    /// Process a single event
    ///
    /// This is the core analysis method that processes incoming events.
    /// The analyzer updates its internal state and may generate insights
    /// immediately or batch them for the next report.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to process
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the event was processed successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The analyzer is not in the `Running` state
    /// - The event is invalid
    /// - Internal processing fails
    async fn process_event(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()>;

    /// Generate a report of current analysis results
    ///
    /// This method returns the current state of analysis, including any insights,
    /// recommendations, and alerts generated since the last report.
    ///
    /// # Returns
    ///
    /// Returns a report containing:
    /// - Summary statistics
    /// - Insights discovered
    /// - Recommendations generated
    /// - Alerts triggered
    ///
    /// # Errors
    ///
    /// Returns an error if report generation fails.
    async fn generate_report(&self) -> AnalyzerResult<AnalysisReport>;

    /// Get current statistics about the analyzer's operation
    ///
    /// This includes metrics like:
    /// - Events processed
    /// - Analysis duration
    /// - Memory usage
    /// - Error counts
    fn get_stats(&self) -> AnalyzerStats;

    /// Reset the analyzer's state
    ///
    /// This clears all accumulated state and resets counters.
    /// The analyzer remains in its current lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns an error if reset fails.
    async fn reset(&mut self) -> AnalyzerResult<()>;

    /// Health check for the analyzer
    ///
    /// Returns `Ok(())` if the analyzer is healthy and able to process events.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Memory usage exceeds limits
    /// - Error rate is too high
    /// - Internal state is corrupted
    async fn health_check(&self) -> AnalyzerResult<()>;
}

/// Builder for creating analyzer configurations
pub struct AnalyzerConfigBuilder {
    config: AnalyzerConfig,
}

impl AnalyzerConfigBuilder {
    /// Create a new builder with the given analyzer ID
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            config: AnalyzerConfig {
                id: id.into(),
                ..Default::default()
            },
        }
    }

    /// Set whether the analyzer is enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set the event filter
    pub fn event_filter(mut self, filter: Vec<String>) -> Self {
        self.config.event_filter = filter;
        self
    }

    /// Set the window duration
    pub fn window_duration(mut self, duration: Duration) -> Self {
        self.config.window_duration = duration;
        self
    }

    /// Set the slide interval
    pub fn slide_interval(mut self, interval: Duration) -> Self {
        self.config.slide_interval = interval;
        self
    }

    /// Set the sample rate
    pub fn sample_rate(mut self, rate: f64) -> Self {
        self.config.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set the maximum buffer size
    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.config.max_buffer_size = size;
        self
    }

    /// Set the maximum memory usage
    pub fn max_memory_bytes(mut self, bytes: usize) -> Self {
        self.config.max_memory_bytes = bytes;
        self
    }

    /// Set the analysis interval
    pub fn analysis_interval(mut self, interval: Duration) -> Self {
        self.config.analysis_interval = interval;
        self
    }

    /// Enable or disable detailed tracing
    pub fn enable_tracing(mut self, enabled: bool) -> Self {
        self.config.enable_tracing = enabled;
        self
    }

    /// Build the configuration
    pub fn build(self) -> AnalyzerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyzer_state_can_accept_events() {
        assert!(!AnalyzerState::Initialized.can_accept_events());
        assert!(!AnalyzerState::Starting.can_accept_events());
        assert!(AnalyzerState::Running.can_accept_events());
        assert!(!AnalyzerState::Draining.can_accept_events());
        assert!(!AnalyzerState::Stopped.can_accept_events());
        assert!(!AnalyzerState::Failed.can_accept_events());
    }

    #[test]
    fn test_analyzer_state_is_terminal() {
        assert!(!AnalyzerState::Initialized.is_terminal());
        assert!(!AnalyzerState::Starting.is_terminal());
        assert!(!AnalyzerState::Running.is_terminal());
        assert!(!AnalyzerState::Draining.is_terminal());
        assert!(AnalyzerState::Stopped.is_terminal());
        assert!(AnalyzerState::Failed.is_terminal());
    }

    #[test]
    fn test_analyzer_config_builder() {
        let config = AnalyzerConfigBuilder::new("test-analyzer")
            .enabled(true)
            .window_duration(Duration::from_secs(600))
            .sample_rate(0.5)
            .max_buffer_size(5000)
            .build();

        assert_eq!(config.id, "test-analyzer");
        assert!(config.enabled);
        assert_eq!(config.window_duration, Duration::from_secs(600));
        assert_eq!(config.sample_rate, 0.5);
        assert_eq!(config.max_buffer_size, 5000);
    }

    #[test]
    fn test_sample_rate_clamping() {
        let config = AnalyzerConfigBuilder::new("test")
            .sample_rate(1.5) // Should be clamped to 1.0
            .build();
        assert_eq!(config.sample_rate, 1.0);

        let config = AnalyzerConfigBuilder::new("test")
            .sample_rate(-0.5) // Should be clamped to 0.0
            .build();
        assert_eq!(config.sample_rate, 0.0);
    }
}
