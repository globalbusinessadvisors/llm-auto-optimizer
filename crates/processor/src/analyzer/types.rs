//! Type definitions for the analyzer framework
//!
//! This module contains all the core data structures used by analyzers,
//! including events, results, insights, recommendations, and alerts.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Events that analyzers can process
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AnalyzerEvent {
    /// Metric event (latency, throughput, cost, etc.)
    Metric {
        timestamp: DateTime<Utc>,
        name: String,
        value: f64,
        unit: String,
        labels: HashMap<String, String>,
    },

    /// Request event (LLM API request)
    Request {
        timestamp: DateTime<Utc>,
        request_id: String,
        model: String,
        prompt_tokens: u32,
        max_tokens: u32,
        temperature: f64,
        metadata: HashMap<String, String>,
    },

    /// Response event (LLM API response)
    Response {
        timestamp: DateTime<Utc>,
        request_id: String,
        model: String,
        completion_tokens: u32,
        total_tokens: u32,
        latency_ms: u64,
        success: bool,
        error: Option<String>,
        metadata: HashMap<String, String>,
    },

    /// Feedback event (user feedback on quality)
    Feedback {
        timestamp: DateTime<Utc>,
        request_id: String,
        rating: f64, // 0.0-1.0
        feedback_type: FeedbackType,
        comment: Option<String>,
        metadata: HashMap<String, String>,
    },

    /// Cost event (billing/cost information)
    Cost {
        timestamp: DateTime<Utc>,
        model: String,
        tokens: u32,
        cost_usd: f64,
        metadata: HashMap<String, String>,
    },

    /// Alert event (external alerts to process)
    Alert {
        timestamp: DateTime<Utc>,
        alert_type: String,
        severity: Severity,
        message: String,
        metadata: HashMap<String, String>,
    },

    /// Custom event (extensibility)
    Custom {
        timestamp: DateTime<Utc>,
        event_type: String,
        data: serde_json::Value,
    },
}

impl AnalyzerEvent {
    /// Get the timestamp of the event
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            AnalyzerEvent::Metric { timestamp, .. } => *timestamp,
            AnalyzerEvent::Request { timestamp, .. } => *timestamp,
            AnalyzerEvent::Response { timestamp, .. } => *timestamp,
            AnalyzerEvent::Feedback { timestamp, .. } => *timestamp,
            AnalyzerEvent::Cost { timestamp, .. } => *timestamp,
            AnalyzerEvent::Alert { timestamp, .. } => *timestamp,
            AnalyzerEvent::Custom { timestamp, .. } => *timestamp,
        }
    }

    /// Get metadata if available
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        match self {
            AnalyzerEvent::Metric { labels, .. } => Some(labels),
            AnalyzerEvent::Request { metadata, .. } => Some(metadata),
            AnalyzerEvent::Response { metadata, .. } => Some(metadata),
            AnalyzerEvent::Feedback { metadata, .. } => Some(metadata),
            AnalyzerEvent::Cost { metadata, .. } => Some(metadata),
            AnalyzerEvent::Alert { metadata, .. } => Some(metadata),
            AnalyzerEvent::Custom { .. } => None,
        }
    }
}

/// Type of user feedback
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackType {
    /// Positive feedback (thumbs up)
    Positive,
    /// Negative feedback (thumbs down)
    Negative,
    /// Rating (1-5 stars)
    Rating,
    /// Quality assessment
    Quality,
    /// Custom feedback
    Custom,
}

/// Severity levels for insights and alerts
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational message
    Info,
    /// Warning that should be reviewed
    Warning,
    /// Error that needs attention
    Error,
    /// Critical issue requiring immediate action
    Critical,
}

impl Severity {
    /// Check if this severity requires immediate action
    pub fn is_urgent(&self) -> bool {
        matches!(self, Severity::Critical | Severity::Error)
    }
}

/// Confidence level for analysis results
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Confidence(f64);

impl Confidence {
    /// Create a new confidence value (0.0-1.0)
    pub fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    /// Get the confidence value
    pub fn value(&self) -> f64 {
        self.0
    }

    /// Check if confidence is high (>= 0.8)
    pub fn is_high(&self) -> bool {
        self.0 >= 0.8
    }

    /// Check if confidence is medium (0.5 - 0.8)
    pub fn is_medium(&self) -> bool {
        self.0 >= 0.5 && self.0 < 0.8
    }

    /// Check if confidence is low (< 0.5)
    pub fn is_low(&self) -> bool {
        self.0 < 0.5
    }
}

/// An insight discovered by an analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insight {
    /// Unique identifier for this insight
    pub id: String,

    /// Analyzer that generated this insight
    pub analyzer: String,

    /// Timestamp when the insight was generated
    pub timestamp: DateTime<Utc>,

    /// Severity of the insight
    pub severity: Severity,

    /// Confidence in the insight (0.0-1.0)
    pub confidence: Confidence,

    /// Title of the insight
    pub title: String,

    /// Detailed description
    pub description: String,

    /// Category of the insight
    pub category: InsightCategory,

    /// Evidence supporting the insight
    pub evidence: Vec<Evidence>,

    /// Related metrics
    pub metrics: HashMap<String, f64>,

    /// Tags for categorization
    pub tags: Vec<String>,
}

/// Category of an insight
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsightCategory {
    /// Performance-related insight
    Performance,
    /// Cost-related insight
    Cost,
    /// Quality-related insight
    Quality,
    /// Pattern or trend insight
    Pattern,
    /// Anomaly or outlier
    Anomaly,
    /// Optimization opportunity
    Optimization,
    /// Risk or concern
    Risk,
    /// General observation
    General,
}

/// Evidence supporting an insight
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    /// Type of evidence
    pub evidence_type: EvidenceType,

    /// Description of the evidence
    pub description: String,

    /// Data supporting the evidence
    pub data: serde_json::Value,

    /// Timestamp of the evidence
    pub timestamp: DateTime<Utc>,
}

/// Type of evidence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceType {
    /// Statistical analysis
    Statistical,
    /// Time series data
    TimeSeries,
    /// Comparison data
    Comparison,
    /// Threshold violation
    Threshold,
    /// Pattern match
    Pattern,
    /// Anomaly detection
    Anomaly,
    /// User feedback
    Feedback,
    /// Historical data
    Historical,
}

/// A recommendation for optimization or action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    /// Unique identifier for this recommendation
    pub id: String,

    /// Analyzer that generated this recommendation
    pub analyzer: String,

    /// Timestamp when generated
    pub timestamp: DateTime<Utc>,

    /// Priority of the recommendation
    pub priority: Priority,

    /// Title of the recommendation
    pub title: String,

    /// Detailed description
    pub description: String,

    /// Action to take
    pub action: Action,

    /// Expected impact
    pub expected_impact: Impact,

    /// Confidence in the recommendation (0.0-1.0)
    pub confidence: Confidence,

    /// Related insights
    pub related_insights: Vec<String>,

    /// Tags for categorization
    pub tags: Vec<String>,
}

/// Priority level for recommendations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// Low priority
    Low,
    /// Medium priority
    Medium,
    /// High priority
    High,
    /// Urgent (immediate action)
    Urgent,
}

/// Action to take based on a recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    /// Type of action
    pub action_type: ActionType,

    /// Detailed instructions
    pub instructions: String,

    /// Parameters for the action
    pub parameters: HashMap<String, serde_json::Value>,

    /// Estimated effort (hours)
    pub estimated_effort: Option<f64>,

    /// Risk level of taking this action
    pub risk_level: RiskLevel,
}

/// Type of action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    /// Adjust configuration
    ConfigChange,
    /// Scale resources
    Scale,
    /// Optimize code or query
    Optimize,
    /// Investigate issue
    Investigate,
    /// Add monitoring
    Monitor,
    /// Alert on threshold
    Alert,
    /// Review and decide
    Review,
    /// Take no action
    NoAction,
}

impl ActionType {
    /// Convert the action type to a lowercase string representation
    pub fn to_lowercase(&self) -> String {
        match self {
            ActionType::ConfigChange => "configchange".to_string(),
            ActionType::Scale => "scale".to_string(),
            ActionType::Optimize => "optimize".to_string(),
            ActionType::Investigate => "investigate".to_string(),
            ActionType::Monitor => "monitor".to_string(),
            ActionType::Alert => "alert".to_string(),
            ActionType::Review => "review".to_string(),
            ActionType::NoAction => "noaction".to_string(),
        }
    }
}

/// Risk level of an action
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    /// No risk
    None,
    /// Low risk
    Low,
    /// Medium risk
    Medium,
    /// High risk
    High,
}

/// Expected impact of a recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Impact {
    /// Performance impact (latency reduction, throughput increase)
    pub performance: Option<ImpactMetric>,

    /// Cost impact (cost savings or increase)
    pub cost: Option<ImpactMetric>,

    /// Quality impact (quality improvement)
    pub quality: Option<ImpactMetric>,

    /// Description of the impact
    pub description: String,
}

/// Quantified impact metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactMetric {
    /// Metric name
    pub name: String,

    /// Current value
    pub current: f64,

    /// Expected value after change
    pub expected: f64,

    /// Unit of measurement
    pub unit: String,

    /// Percentage improvement
    pub improvement_pct: f64,
}

/// An alert triggered by an analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique identifier for this alert
    pub id: String,

    /// Analyzer that triggered the alert
    pub analyzer: String,

    /// Timestamp when triggered
    pub timestamp: DateTime<Utc>,

    /// Severity of the alert
    pub severity: Severity,

    /// Title of the alert
    pub title: String,

    /// Detailed message
    pub message: String,

    /// Alert status
    pub status: AlertStatus,

    /// Threshold that was violated (if applicable)
    pub threshold: Option<Threshold>,

    /// Related insights
    pub related_insights: Vec<String>,

    /// Tags for routing and filtering
    pub tags: Vec<String>,

    /// Alert metadata
    pub metadata: HashMap<String, String>,
}

/// Status of an alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertStatus {
    /// Alert just triggered
    Firing,
    /// Alert condition resolved
    Resolved,
    /// Alert acknowledged but not resolved
    Acknowledged,
    /// Alert silenced
    Silenced,
}

/// Threshold configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Threshold {
    /// Metric name
    pub metric: String,

    /// Threshold value
    pub value: f64,

    /// Comparison operator
    pub operator: ThresholdOperator,

    /// Duration over which condition must hold
    pub duration: Duration,

    /// Actual value that triggered
    pub actual_value: f64,
}

/// Threshold comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdOperator {
    /// Greater than
    GreaterThan,
    /// Greater than or equal
    GreaterThanOrEqual,
    /// Less than
    LessThan,
    /// Less than or equal
    LessThanOrEqual,
    /// Equal to
    Equal,
    /// Not equal to
    NotEqual,
}

impl ThresholdOperator {
    /// Check if a value meets the threshold condition
    pub fn compare(&self, actual: f64, threshold: f64) -> bool {
        match self {
            ThresholdOperator::GreaterThan => actual > threshold,
            ThresholdOperator::GreaterThanOrEqual => actual >= threshold,
            ThresholdOperator::LessThan => actual < threshold,
            ThresholdOperator::LessThanOrEqual => actual <= threshold,
            ThresholdOperator::Equal => (actual - threshold).abs() < f64::EPSILON,
            ThresholdOperator::NotEqual => (actual - threshold).abs() >= f64::EPSILON,
        }
    }
}

/// Complete analysis report from an analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisReport {
    /// Analyzer name
    pub analyzer: String,

    /// Report generation timestamp
    pub timestamp: DateTime<Utc>,

    /// Time period covered by this report
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,

    /// Summary statistics
    pub summary: ReportSummary,

    /// Insights discovered
    pub insights: Vec<Insight>,

    /// Recommendations generated
    pub recommendations: Vec<Recommendation>,

    /// Alerts triggered
    pub alerts: Vec<Alert>,

    /// Raw metrics data
    pub metrics: HashMap<String, f64>,
}

/// Summary statistics for a report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    /// Total events processed
    pub events_processed: u64,

    /// Events per second
    pub events_per_second: f64,

    /// Number of insights generated
    pub insights_count: u64,

    /// Number of recommendations generated
    pub recommendations_count: u64,

    /// Number of alerts triggered
    pub alerts_count: u64,

    /// Analysis duration
    pub analysis_duration_ms: u64,

    /// Key highlights
    pub highlights: Vec<String>,
}

/// Statistics about an analyzer's operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzerStats {
    /// Analyzer name
    pub analyzer: String,

    /// Total events processed
    pub events_processed: u64,

    /// Total events dropped (sampling or errors)
    pub events_dropped: u64,

    /// Total insights generated
    pub insights_generated: u64,

    /// Total recommendations generated
    pub recommendations_generated: u64,

    /// Total alerts triggered
    pub alerts_triggered: u64,

    /// Total analysis time (milliseconds)
    pub analysis_time_ms: u64,

    /// Average analysis time per event (microseconds)
    pub avg_analysis_time_us: f64,

    /// Current memory usage (bytes)
    pub memory_usage_bytes: usize,

    /// Error count
    pub error_count: u64,

    /// Last error message
    pub last_error: Option<String>,

    /// Uptime in seconds
    pub uptime_seconds: u64,
}

impl Default for AnalyzerStats {
    fn default() -> Self {
        Self {
            analyzer: String::new(),
            events_processed: 0,
            events_dropped: 0,
            insights_generated: 0,
            recommendations_generated: 0,
            alerts_triggered: 0,
            analysis_time_ms: 0,
            avg_analysis_time_us: 0.0,
            memory_usage_bytes: 0,
            error_count: 0,
            last_error: None,
            uptime_seconds: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_creation() {
        let conf = Confidence::new(0.75);
        assert_eq!(conf.value(), 0.75);
        assert!(conf.is_medium());

        let conf = Confidence::new(1.5); // Should clamp to 1.0
        assert_eq!(conf.value(), 1.0);

        let conf = Confidence::new(-0.5); // Should clamp to 0.0
        assert_eq!(conf.value(), 0.0);
    }

    #[test]
    fn test_confidence_levels() {
        assert!(Confidence::new(0.9).is_high());
        assert!(!Confidence::new(0.9).is_medium());
        assert!(!Confidence::new(0.9).is_low());

        assert!(Confidence::new(0.6).is_medium());
        assert!(!Confidence::new(0.6).is_high());
        assert!(!Confidence::new(0.6).is_low());

        assert!(Confidence::new(0.3).is_low());
        assert!(!Confidence::new(0.3).is_medium());
        assert!(!Confidence::new(0.3).is_high());
    }

    #[test]
    fn test_severity_is_urgent() {
        assert!(!Severity::Info.is_urgent());
        assert!(!Severity::Warning.is_urgent());
        assert!(Severity::Error.is_urgent());
        assert!(Severity::Critical.is_urgent());
    }

    #[test]
    fn test_threshold_operator_compare() {
        assert!(ThresholdOperator::GreaterThan.compare(10.0, 5.0));
        assert!(!ThresholdOperator::GreaterThan.compare(5.0, 10.0));

        assert!(ThresholdOperator::LessThan.compare(5.0, 10.0));
        assert!(!ThresholdOperator::LessThan.compare(10.0, 5.0));

        assert!(ThresholdOperator::Equal.compare(5.0, 5.0));
        assert!(!ThresholdOperator::Equal.compare(5.0, 6.0));
    }

    #[test]
    fn test_analyzer_event_timestamp() {
        let now = Utc::now();
        let event = AnalyzerEvent::Metric {
            timestamp: now,
            name: "test".to_string(),
            value: 1.0,
            unit: "ms".to_string(),
            labels: HashMap::new(),
        };

        assert_eq!(event.timestamp(), now);
    }
}
