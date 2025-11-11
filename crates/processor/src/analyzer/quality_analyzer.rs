//! Quality Analyzer
//!
//! Monitors response quality, error rates, SLA compliance, and user feedback.
//!
//! # Features
//!
//! - **Error Rate Tracking**: Success/failure rates with thresholds
//! - **SLA Compliance**: Latency-based SLA monitoring (P95, P99)
//! - **Feedback Analysis**: User rating tracking and sentiment
//! - **Response Quality**: Completeness, accuracy, relevance metrics
//! - **Quality Degradation**: Detect declining quality trends
//!
//! # Example
//!
//! ```rust,no_run
//! use processor::analyzer::{QualityAnalyzer, QualityAnalyzerConfig, Analyzer, AnalyzerEvent};
//! use chrono::Utc;
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create analyzer with SLA targets
//!     let config = QualityAnalyzerConfig {
//!         sla_p95_ms: 1000.0,
//!         sla_p99_ms: 2000.0,
//!         min_success_rate_pct: 99.5,
//!         min_avg_rating: 4.0,
//!         ..Default::default()
//!     };
//!
//!     let mut analyzer = QualityAnalyzer::new(config);
//!     analyzer.start().await?;
//!
//!     // Process response event
//!     let event = AnalyzerEvent::Response {
//!         timestamp: Utc::now(),
//!         request_id: "req-123".to_string(),
//!         model: "gpt-4".to_string(),
//!         completion_tokens: 100,
//!         total_tokens: 200,
//!         latency_ms: 500,
//!         success: true,
//!         error: None,
//!         metadata: HashMap::new(),
//!     };
//!
//!     analyzer.process_event(event).await?;
//!
//!     // Generate quality report
//!     let report = analyzer.generate_report().await?;
//!     println!("Quality Insights: {}", report.insights.len());
//!
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::stats::{
    calculate_percentiles, mean, percentage_change, std_dev, CircularBuffer,
    ExponentialMovingAverage,
};
use super::traits::{Analyzer, AnalyzerConfig, AnalyzerError, AnalyzerResult, AnalyzerState};
use super::types::{
    Action, ActionType, Alert, AlertStatus, AnalysisReport, AnalyzerEvent, AnalyzerStats,
    Confidence, Evidence, EvidenceType, FeedbackType, Impact, ImpactMetric, Insight, InsightCategory, Priority,
    Recommendation, ReportSummary, RiskLevel, Severity,
};

/// Configuration for the Quality Analyzer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityAnalyzerConfig {
    /// Base analyzer configuration
    #[serde(flatten)]
    pub base: AnalyzerConfig,

    /// SLA target for P95 latency (ms)
    pub sla_p95_ms: f64,

    /// SLA target for P99 latency (ms)
    pub sla_p99_ms: f64,

    /// Minimum success rate percentage (0-100)
    pub min_success_rate_pct: f64,

    /// Minimum average user rating (0-5)
    pub min_avg_rating: f64,

    /// Error rate warning threshold (percentage)
    pub error_rate_warning_pct: f64,

    /// Error rate critical threshold (percentage)
    pub error_rate_critical_pct: f64,

    /// Minimum samples for quality calculations
    pub min_samples_for_quality: usize,

    /// Quality degradation threshold (percentage drop)
    pub quality_degradation_threshold_pct: f64,

    /// Feedback window size (number of ratings to track)
    pub feedback_window_size: usize,
}

impl Default for QualityAnalyzerConfig {
    fn default() -> Self {
        Self {
            base: AnalyzerConfig {
                id: "quality-analyzer".to_string(),
                enabled: true,
                event_filter: vec![
                    "response".to_string(),
                    "feedback".to_string(),
                    "request".to_string(),
                ],
                window_duration: std::time::Duration::from_secs(300), // 5 minutes
                slide_interval: std::time::Duration::from_secs(60),   // 1 minute
                sample_rate: 1.0,
                max_buffer_size: 10000,
                max_memory_bytes: 100 * 1024 * 1024, // 100 MB
                analysis_interval: std::time::Duration::from_secs(60),
                enable_tracing: true,
            },
            sla_p95_ms: 1000.0,
            sla_p99_ms: 2000.0,
            min_success_rate_pct: 99.5,
            min_avg_rating: 4.0,
            error_rate_warning_pct: 1.0,
            error_rate_critical_pct: 5.0,
            min_samples_for_quality: 10,
            quality_degradation_threshold_pct: 10.0,
            feedback_window_size: 100,
        }
    }
}

/// Quality metrics tracked by the analyzer
#[derive(Debug, Clone)]
struct QualityMetrics {
    /// Latency samples for SLA calculation
    latencies: CircularBuffer<f64>,

    /// Success/failure tracking
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,

    /// Error tracking by type
    error_counts: HashMap<String, u64>,

    /// User feedback ratings (1-5 scale)
    ratings: CircularBuffer<f64>,

    /// Feedback sentiment tracking
    positive_feedback: u64,
    negative_feedback: u64,
    neutral_feedback: u64,

    /// Response completeness tracking (0-1 scale)
    completeness_scores: CircularBuffer<f64>,

    /// Moving averages for trend detection
    success_rate_ema: ExponentialMovingAverage,
    rating_ema: ExponentialMovingAverage,

    /// Baseline metrics for degradation detection
    baseline_success_rate: Option<f64>,
    baseline_avg_rating: Option<f64>,
    baseline_p95_latency: Option<f64>,

    /// SLA compliance tracking
    sla_violations_p95: u64,
    sla_violations_p99: u64,
    sla_checks: u64,

    /// Per-model quality tracking
    model_metrics: HashMap<String, ModelQualityMetrics>,

    /// Timestamp tracking
    last_quality_check: DateTime<Utc>,
    first_event_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct ModelQualityMetrics {
    total_requests: u64,
    successful_requests: u64,
    avg_rating: f64,
    rating_count: u64,
}

impl ModelQualityMetrics {
    fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            avg_rating: 0.0,
            rating_count: 0,
        }
    }

    fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 100.0;
        }
        (self.successful_requests as f64 / self.total_requests as f64) * 100.0
    }
}

impl QualityMetrics {
    fn new(config: &QualityAnalyzerConfig) -> Self {
        Self {
            latencies: CircularBuffer::new(1000),
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            error_counts: HashMap::new(),
            ratings: CircularBuffer::new(config.feedback_window_size),
            positive_feedback: 0,
            negative_feedback: 0,
            neutral_feedback: 0,
            completeness_scores: CircularBuffer::new(1000),
            success_rate_ema: ExponentialMovingAverage::new(0.1),
            rating_ema: ExponentialMovingAverage::new(0.1),
            baseline_success_rate: None,
            baseline_avg_rating: None,
            baseline_p95_latency: None,
            sla_violations_p95: 0,
            sla_violations_p99: 0,
            sla_checks: 0,
            model_metrics: HashMap::new(),
            last_quality_check: Utc::now(),
            first_event_time: None,
        }
    }

    fn current_success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 100.0;
        }
        (self.successful_requests as f64 / self.total_requests as f64) * 100.0
    }

    fn current_error_rate(&self) -> f64 {
        100.0 - self.current_success_rate()
    }

    fn average_rating(&self) -> Option<f64> {
        let values = self.ratings.to_vec();
        if values.is_empty() {
            return None;
        }
        Some(mean(&values))
    }

    fn average_completeness(&self) -> Option<f64> {
        let values = self.completeness_scores.to_vec();
        if values.is_empty() {
            return None;
        }
        Some(mean(&values))
    }

    fn sla_compliance_rate(&self) -> f64 {
        if self.sla_checks == 0 {
            return 100.0;
        }
        let violations = self.sla_violations_p95 + self.sla_violations_p99;
        ((self.sla_checks - violations) as f64 / self.sla_checks as f64) * 100.0
    }
}

/// Quality Analyzer implementation
pub struct QualityAnalyzer {
    config: QualityAnalyzerConfig,
    state: AnalyzerState,
    metrics: Arc<RwLock<QualityMetrics>>,
    stats: Arc<RwLock<AnalyzerStats>>,
    start_time: Instant,
    last_report_time: Arc<RwLock<Instant>>,
}

impl QualityAnalyzer {
    /// Create a new Quality Analyzer with the given configuration
    pub fn new(config: QualityAnalyzerConfig) -> Self {
        let metrics = QualityMetrics::new(&config);

        Self {
            config,
            state: AnalyzerState::Initialized,
            metrics: Arc::new(RwLock::new(metrics)),
            stats: Arc::new(RwLock::new(AnalyzerStats::default())),
            start_time: Instant::now(),
            last_report_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create a Quality Analyzer with default configuration
    pub fn with_defaults() -> Self {
        Self::new(QualityAnalyzerConfig::default())
    }

    /// Process a response event
    async fn process_response_event(
        &mut self,
        timestamp: DateTime<Utc>,
        model: String,
        latency_ms: u64,
        success: bool,
        error: Option<String>,
    ) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;

        // Initialize first event time
        if metrics.first_event_time.is_none() {
            metrics.first_event_time = Some(timestamp);
        }

        // Update request counts
        metrics.total_requests += 1;
        if success {
            metrics.successful_requests += 1;
        } else {
            metrics.failed_requests += 1;
        }

        // Track error types
        if let Some(err) = error {
            *metrics.error_counts.entry(err).or_insert(0) += 1;
        }

        // Update latency tracking
        let latency_f64 = latency_ms as f64;
        metrics.latencies.push(latency_f64);

        // Update success rate EMA
        let success_val = if success { 1.0 } else { 0.0 };
        metrics.success_rate_ema.update(success_val);

        // Update per-model metrics
        let model_metric = metrics
            .model_metrics
            .entry(model)
            .or_insert_with(ModelQualityMetrics::new);
        model_metric.total_requests += 1;
        if success {
            model_metric.successful_requests += 1;
        }

        // Check SLA compliance
        if metrics.latencies.len() >= 10 {
            let latencies_vec = metrics.latencies.to_vec();
            let percentiles = calculate_percentiles(latencies_vec.clone(), &[0.95, 0.99]);

            metrics.sla_checks += 1;

            if let Some((_, p95)) = percentiles.first() {
                if *p95 > self.config.sla_p95_ms {
                    metrics.sla_violations_p95 += 1;
                }
            }

            if let Some((_, p99)) = percentiles.get(1) {
                if *p99 > self.config.sla_p99_ms {
                    metrics.sla_violations_p99 += 1;
                }
            }
        }

        // Establish baseline after sufficient samples
        if metrics.total_requests == 100 && metrics.baseline_success_rate.is_none() {
            metrics.baseline_success_rate = Some(metrics.current_success_rate());

            if metrics.latencies.len() >= 10 {
                let latencies_vec = metrics.latencies.to_vec();
                let percentiles = calculate_percentiles(latencies_vec, &[0.95]);
                if let Some((_, p95)) = percentiles.first() {
                    metrics.baseline_p95_latency = Some(*p95);
                }
            }
        }

        Ok(())
    }

    /// Process a feedback event
    async fn process_feedback_event(
        &mut self,
        timestamp: DateTime<Utc>,
        model: String,
        rating: Option<f64>,
        feedback_type: Option<String>,
    ) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;

        // Process rating
        if let Some(r) = rating {
            // Normalize rating to 0-5 scale
            let normalized_rating = r.max(0.0).min(5.0);
            metrics.ratings.push(normalized_rating);
            metrics.rating_ema.update(normalized_rating);

            // Update per-model rating
            if let Some(model_metric) = metrics.model_metrics.get_mut(&model) {
                let count = model_metric.rating_count as f64;
                model_metric.avg_rating =
                    (model_metric.avg_rating * count + normalized_rating) / (count + 1.0);
                model_metric.rating_count += 1;
            }

            // Categorize feedback
            if normalized_rating >= 4.0 {
                metrics.positive_feedback += 1;
            } else if normalized_rating >= 3.0 {
                metrics.neutral_feedback += 1;
            } else {
                metrics.negative_feedback += 1;
            }

            // Establish baseline
            if metrics.ratings.len() == self.config.feedback_window_size
                && metrics.baseline_avg_rating.is_none()
            {
                metrics.baseline_avg_rating = metrics.average_rating();
            }
        }

        Ok(())
    }

    /// Generate quality insights
    async fn generate_insights(&self) -> Vec<Insight> {
        let metrics = self.metrics.read().await;
        let mut insights = Vec::new();

        // Skip if insufficient data
        if metrics.total_requests < self.config.min_samples_for_quality as u64 {
            return insights;
        }

        // Check error rate
        let error_rate = metrics.current_error_rate();
        if error_rate >= self.config.error_rate_critical_pct {
            insights.push(self.create_error_rate_insight(
                error_rate,
                Severity::Critical,
                &metrics,
            ));
        } else if error_rate >= self.config.error_rate_warning_pct {
            insights.push(self.create_error_rate_insight(
                error_rate,
                Severity::Warning,
                &metrics,
            ));
        }

        // Check success rate
        let success_rate = metrics.current_success_rate();
        if success_rate < self.config.min_success_rate_pct {
            insights.push(self.create_low_success_rate_insight(success_rate, &metrics));
        }

        // Check SLA compliance
        let sla_compliance = metrics.sla_compliance_rate();
        if sla_compliance < 95.0 {
            insights.push(self.create_sla_violation_insight(sla_compliance, &metrics));
        }

        // Check average rating
        if let Some(avg_rating) = metrics.average_rating() {
            if avg_rating < self.config.min_avg_rating {
                insights.push(self.create_low_rating_insight(avg_rating, &metrics));
            }
        }

        // Check for quality degradation
        if let Some(baseline_sr) = metrics.baseline_success_rate {
            let degradation = percentage_change(baseline_sr, success_rate);
            if degradation.abs() >= self.config.quality_degradation_threshold_pct {
                insights.push(self.create_degradation_insight(degradation, &metrics));
            }
        }

        insights
    }

    /// Generate quality recommendations
    async fn generate_recommendations(&self) -> Vec<Recommendation> {
        let metrics = self.metrics.read().await;
        let mut recommendations = Vec::new();

        // Skip if insufficient data
        if metrics.total_requests < self.config.min_samples_for_quality as u64 {
            return recommendations;
        }

        let error_rate = metrics.current_error_rate();
        let success_rate = metrics.current_success_rate();

        // High error rate recommendations
        if error_rate >= self.config.error_rate_warning_pct {
            recommendations.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                analyzer: "quality-analyzer".to_string(),
                timestamp: Utc::now(),
                priority: if error_rate >= self.config.error_rate_critical_pct {
                    Priority::Urgent
                } else {
                    Priority::High
                },
                title: "Investigate and fix error sources".to_string(),
                description: format!(
                    "Error rate is {:.2}%. Review error logs and implement fixes for top error types: {}",
                    error_rate,
                    self.format_top_errors(&metrics)
                ),
                action: Action {
                    action_type: ActionType::Investigate,
                    instructions: "Review error logs and implement error handling improvements"
                        .to_string(),
                    parameters: Default::default(),
                    estimated_effort: Some(4.0),
                    risk_level: RiskLevel::Low,
                },
                expected_impact: Impact {
                    quality: Some(ImpactMetric {
                        name: "error_rate".to_string(),
                        current: error_rate,
                        expected: self.config.error_rate_warning_pct / 2.0,
                        unit: "%".to_string(),
                        improvement_pct: ((error_rate - self.config.error_rate_warning_pct / 2.0) / error_rate) * 100.0,
                    }),
                    performance: None,
                    cost: None,
                    description: format!("Reduce error rate from {:.2}% to {:.2}%", error_rate, self.config.error_rate_warning_pct / 2.0),
                },
                confidence: Confidence::new(0.8),
                related_insights: vec![],
                tags: vec!["quality".to_string(), "errors".to_string()],
            });
        }

        // Low success rate recommendations
        if success_rate < self.config.min_success_rate_pct {
            recommendations.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                analyzer: "quality-analyzer".to_string(),
                timestamp: Utc::now(),
                priority: Priority::High,
                title: "Improve request success rate".to_string(),
                description: format!(
                    "Success rate is {:.2}%, below target of {:.2}%. Implement retry logic and better error handling.",
                    success_rate, self.config.min_success_rate_pct
                ),
                action: Action {
                    action_type: ActionType::ConfigChange,
                    instructions: "Enable exponential backoff retry logic".to_string(),
                    parameters: Default::default(),
                    estimated_effort: Some(2.0),
                    risk_level: RiskLevel::Low,
                },
                expected_impact: Impact {
                    quality: Some(ImpactMetric {
                        name: "success_rate".to_string(),
                        current: success_rate,
                        expected: self.config.min_success_rate_pct,
                        unit: "%".to_string(),
                        improvement_pct: ((self.config.min_success_rate_pct - success_rate) / success_rate) * 100.0,
                    }),
                    performance: None,
                    cost: None,
                    description: format!("Improve success rate from {:.2}% to {:.2}%", success_rate, self.config.min_success_rate_pct),
                },
                confidence: Confidence::new(0.8),
                related_insights: vec![],
                tags: vec!["quality".to_string(), "reliability".to_string()],
            });
        }

        // SLA compliance recommendations
        if metrics.sla_compliance_rate() < 95.0 {
            recommendations.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                analyzer: "quality-analyzer".to_string(),
                timestamp: Utc::now(),
                priority: Priority::High,
                title: "Improve SLA compliance".to_string(),
                description: format!(
                    "SLA compliance is {:.2}%. Optimize performance to meet P95 ({:.0}ms) and P99 ({:.0}ms) targets.",
                    metrics.sla_compliance_rate(),
                    self.config.sla_p95_ms,
                    self.config.sla_p99_ms
                ),
                action: Action {
                    action_type: ActionType::Optimize,
                    instructions: "Enable caching and optimize slow queries".to_string(),
                    parameters: Default::default(),
                    estimated_effort: Some(8.0),
                    risk_level: RiskLevel::Medium,
                },
                expected_impact: Impact {
                    performance: Some(ImpactMetric {
                        name: "p95_latency".to_string(),
                        current: metrics.sla_violations_p95 as f64,
                        expected: 0.0,
                        unit: "violations".to_string(),
                        improvement_pct: 100.0,
                    }),
                    cost: None,
                    quality: None,
                    description: "Reduce P95 latency SLA violations to zero".to_string(),
                },
                confidence: Confidence::new(0.6),
                related_insights: vec![],
                tags: vec!["quality".to_string(), "sla".to_string()],
            });
        }

        // Low rating recommendations
        if let Some(avg_rating) = metrics.average_rating() {
            if avg_rating < self.config.min_avg_rating {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "quality-analyzer".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Medium,
                    title: "Improve response quality based on user feedback".to_string(),
                    description: format!(
                        "Average rating is {:.2}/5.0, below target of {:.2}. Review negative feedback and improve response quality.",
                        avg_rating, self.config.min_avg_rating
                    ),
                    action: Action {
                        action_type: ActionType::Optimize,
                        instructions: "Analyze negative feedback patterns and adjust prompts"
                            .to_string(),
                        parameters: Default::default(),
                        estimated_effort: Some(6.0),
                        risk_level: RiskLevel::Medium,
                    },
                    expected_impact: Impact {
                        quality: Some(ImpactMetric {
                            name: "quality_score".to_string(),
                            current: avg_rating,
                            expected: self.config.min_avg_rating,
                            unit: "rating".to_string(),
                            improvement_pct: ((self.config.min_avg_rating - avg_rating) / avg_rating) * 100.0,
                        }),
                        performance: None,
                        cost: None,
                        description: format!("Improve quality score from {:.2} to {:.2}", avg_rating, self.config.min_avg_rating),
                    },
                    confidence: Confidence::new(0.6),
                    related_insights: vec![],
                    tags: vec!["quality".to_string(), "feedback".to_string()],
                });
            }
        }

        recommendations
    }

    fn create_error_rate_insight(
        &self,
        error_rate: f64,
        severity: Severity,
        metrics: &QualityMetrics,
    ) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "quality-analyzer".to_string(),
            timestamp: Utc::now(),
            severity,
            confidence: Confidence::new(0.8),
            title: format!("High error rate: {:.2}%", error_rate),
            description: format!(
                "Error rate is {:.2}%, which exceeds the {} threshold. Total failures: {}. Top errors: {}",
                error_rate,
                if severity == Severity::Critical { "critical" } else { "warning" },
                metrics.failed_requests,
                self.format_top_errors(metrics)
            ),
            category: InsightCategory::Quality,
            evidence: vec![
                Evidence {
                    evidence_type: EvidenceType::Statistical,
                    description: format!("Error rate: {:.2}%", error_rate),
                    data: serde_json::json!({"value": error_rate}),
                    timestamp: Utc::now(),
                },
                Evidence {
                    evidence_type: EvidenceType::Statistical,
                    description: format!("Failed requests: {}", metrics.failed_requests),
                    data: serde_json::json!({"value": metrics.failed_requests as f64}),
                    timestamp: Utc::now(),
                },
            ],
            metrics: {
                let mut m = HashMap::new();
                m.insert("error_rate_pct".to_string(), error_rate);
                m.insert("failed_requests".to_string(), metrics.failed_requests as f64);
                m
            },
            tags: vec![
                "quality".to_string(),
                "errors".to_string(),
                "reliability".to_string(),
            ],
        }
    }

    fn create_low_success_rate_insight(
        &self,
        success_rate: f64,
        metrics: &QualityMetrics,
    ) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "quality-analyzer".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Warning,
            confidence: Confidence::new(0.8),
            title: format!(
                "Success rate {:.2}% below target {:.2}%",
                success_rate, self.config.min_success_rate_pct
            ),
            description: format!(
                "Current success rate is {:.2}%, which is below the target of {:.2}%. {} successful out of {} total requests.",
                success_rate,
                self.config.min_success_rate_pct,
                metrics.successful_requests,
                metrics.total_requests
            ),
            category: InsightCategory::Quality,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Statistical,
                description: format!("Success rate: {:.2}%", success_rate),
                data: serde_json::json!({"value": success_rate}),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("success_rate_pct".to_string(), success_rate);
                m.insert(
                    "successful_requests".to_string(),
                    metrics.successful_requests as f64,
                );
                m.insert("total_requests".to_string(), metrics.total_requests as f64);
                m
            },
            tags: vec!["quality".to_string(), "success-rate".to_string()],
        }
    }

    fn create_sla_violation_insight(
        &self,
        sla_compliance: f64,
        metrics: &QualityMetrics,
    ) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "quality-analyzer".to_string(),
            timestamp: Utc::now(),
            severity: if sla_compliance < 90.0 {
                Severity::Critical
            } else {
                Severity::Warning
            },
            confidence: Confidence::new(0.8),
            title: format!("SLA compliance at {:.1}%", sla_compliance),
            description: format!(
                "SLA compliance is {:.1}%, with {} P95 violations and {} P99 violations. Targets: P95 < {:.0}ms, P99 < {:.0}ms",
                sla_compliance,
                metrics.sla_violations_p95,
                metrics.sla_violations_p99,
                self.config.sla_p95_ms,
                self.config.sla_p99_ms
            ),
            category: InsightCategory::Quality,
            evidence: vec![
                Evidence {
                    evidence_type: EvidenceType::Statistical,
                    description: format!("SLA compliance: {:.1}%", sla_compliance),
                    data: serde_json::json!({"value": sla_compliance}),
                    timestamp: Utc::now(),
                },
                Evidence {
                    evidence_type: EvidenceType::Statistical,
                    description: format!("P95 violations: {}", metrics.sla_violations_p95),
                    data: serde_json::json!({"value": metrics.sla_violations_p95 as f64}),
                    timestamp: Utc::now(),
                },
            ],
            metrics: {
                let mut m = HashMap::new();
                m.insert("sla_compliance_pct".to_string(), sla_compliance);
                m.insert(
                    "p95_violations".to_string(),
                    metrics.sla_violations_p95 as f64,
                );
                m.insert(
                    "p99_violations".to_string(),
                    metrics.sla_violations_p99 as f64,
                );
                m
            },
            tags: vec!["quality".to_string(), "sla".to_string()],
        }
    }

    fn create_low_rating_insight(&self, avg_rating: f64, metrics: &QualityMetrics) -> Insight {
        let total_feedback =
            metrics.positive_feedback + metrics.negative_feedback + metrics.neutral_feedback;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "quality-analyzer".to_string(),
            timestamp: Utc::now(),
            severity: if avg_rating < 3.0 {
                Severity::Error
            } else {
                Severity::Warning
            },
            confidence: Confidence::new(0.6),
            title: format!(
                "Average rating {:.2}/5.0 below target {:.2}/5.0",
                avg_rating, self.config.min_avg_rating
            ),
            description: format!(
                "Average user rating is {:.2}/5.0, below target of {:.2}/5.0. {} positive, {} neutral, {} negative (total: {})",
                avg_rating,
                self.config.min_avg_rating,
                metrics.positive_feedback,
                metrics.neutral_feedback,
                metrics.negative_feedback,
                total_feedback
            ),
            category: InsightCategory::Quality,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Statistical,
                description: format!("Average rating: {:.2}/5.0", avg_rating),
                data: serde_json::json!({"value": avg_rating}),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("avg_rating".to_string(), avg_rating);
                m.insert("positive_feedback".to_string(), metrics.positive_feedback as f64);
                m.insert("negative_feedback".to_string(), metrics.negative_feedback as f64);
                m.insert("neutral_feedback".to_string(), metrics.neutral_feedback as f64);
                m
            },
            tags: vec!["quality".to_string(), "feedback".to_string()],
        }
    }

    fn create_degradation_insight(&self, degradation: f64, metrics: &QualityMetrics) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "quality-analyzer".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Warning,
            confidence: Confidence::new(0.6),
            title: format!("Quality degradation detected: {:.1}%", degradation.abs()),
            description: format!(
                "Success rate has degraded by {:.1}% from baseline. Current: {:.2}%, Baseline: {:.2}%",
                degradation.abs(),
                metrics.current_success_rate(),
                metrics.baseline_success_rate.unwrap_or(0.0)
            ),
            category: InsightCategory::Quality,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::TimeSeries,
                description: format!("Quality degradation: {:.1}%", degradation.abs()),
                data: serde_json::json!({"value": degradation}),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("degradation_pct".to_string(), degradation);
                m.insert(
                    "current_success_rate".to_string(),
                    metrics.current_success_rate(),
                );
                m.insert(
                    "baseline_success_rate".to_string(),
                    metrics.baseline_success_rate.unwrap_or(0.0),
                );
                m
            },
            tags: vec!["quality".to_string(), "degradation".to_string()],
        }
    }

    fn format_top_errors(&self, metrics: &QualityMetrics) -> String {
        let mut errors: Vec<_> = metrics.error_counts.iter().collect();
        errors.sort_by(|a, b| b.1.cmp(a.1));

        errors
            .iter()
            .take(3)
            .map(|(err, count)| format!("{} ({})", err, count))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[async_trait]
impl Analyzer for QualityAnalyzer {
    fn name(&self) -> &str {
        "quality-analyzer"
    }

    fn config(&self) -> &AnalyzerConfig {
        &self.config.base
    }

    fn state(&self) -> AnalyzerState {
        self.state.clone()
    }

    async fn start(&mut self) -> AnalyzerResult<()> {
        if self.state == AnalyzerState::Running {
            return Err(AnalyzerError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: AnalyzerState::Starting.as_str().to_string(),
            });
        }

        info!("Starting Quality Analyzer");
        self.state = AnalyzerState::Starting;

        // Perform any initialization here
        self.state = AnalyzerState::Running;
        info!("Quality Analyzer started successfully");

        Ok(())
    }

    async fn stop(&mut self) -> AnalyzerResult<()> {
        if self.state != AnalyzerState::Running {
            return Err(AnalyzerError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: AnalyzerState::Draining.as_str().to_string(),
            });
        }

        info!("Stopping Quality Analyzer");
        self.state = AnalyzerState::Draining;

        // Perform cleanup
        self.state = AnalyzerState::Stopped;
        info!("Quality Analyzer stopped");

        Ok(())
    }

    async fn process_event(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if self.state != AnalyzerState::Running {
            return Err(AnalyzerError::NotRunning);
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.events_processed += 1;
        // Note: last_event_time field removed from AnalyzerStats
        drop(stats);

        // Process based on event type
        match event {
            AnalyzerEvent::Response {
                timestamp,
                model,
                latency_ms,
                success,
                error,
                ..
            } => {
                self.process_response_event(timestamp, model, latency_ms, success, error)
                    .await?;
            }
            AnalyzerEvent::Feedback {
                timestamp,
                request_id,
                rating,
                feedback_type,
                metadata,
                ..
            } => {
                // Extract model from metadata or use request_id as fallback
                let model = metadata
                    .get("model")
                    .unwrap_or(&request_id)
                    .to_string();

                let feedback_str = match feedback_type {
                    FeedbackType::Positive => "positive",
                    FeedbackType::Negative => "negative",
                    FeedbackType::Rating => "rating",
                    FeedbackType::Quality => "quality",
                    FeedbackType::Custom => "custom",
                };
                self.process_feedback_event(timestamp, model, Some(rating), Some(feedback_str.to_string()))
                    .await?;
            }
            _ => {
                debug!("Ignoring event type not relevant to quality analysis");
            }
        }

        Ok(())
    }

    async fn generate_report(&self) -> AnalyzerResult<AnalysisReport> {
        let metrics = self.metrics.read().await;
        let stats = self.stats.read().await;

        let now = Utc::now();
        let period_start = metrics
            .first_event_time
            .unwrap_or_else(|| now - ChronoDuration::minutes(5));

        let insights = self.generate_insights().await;
        let recommendations = self.generate_recommendations().await;

        // Calculate report metrics
        let mut report_metrics = HashMap::new();
        report_metrics.insert(
            "success_rate_pct".to_string(),
            metrics.current_success_rate(),
        );
        report_metrics.insert("error_rate_pct".to_string(), metrics.current_error_rate());
        report_metrics.insert("total_requests".to_string(), metrics.total_requests as f64);
        report_metrics.insert(
            "successful_requests".to_string(),
            metrics.successful_requests as f64,
        );
        report_metrics.insert(
            "failed_requests".to_string(),
            metrics.failed_requests as f64,
        );

        if let Some(avg_rating) = metrics.average_rating() {
            report_metrics.insert("avg_rating".to_string(), avg_rating);
        }

        report_metrics.insert(
            "sla_compliance_pct".to_string(),
            metrics.sla_compliance_rate(),
        );
        report_metrics.insert(
            "positive_feedback".to_string(),
            metrics.positive_feedback as f64,
        );
        report_metrics.insert(
            "negative_feedback".to_string(),
            metrics.negative_feedback as f64,
        );

        let critical_issues_count = insights
            .iter()
            .filter(|i| i.severity == Severity::Critical)
            .count();
        let warnings_count = insights
            .iter()
            .filter(|i| i.severity == Severity::Warning)
            .count();

        let summary = ReportSummary {
            events_processed: stats.events_processed,
            events_per_second: 0.0,  // TODO: Calculate properly
            insights_count: insights.len() as u64,
            recommendations_count: recommendations.len() as u64,
            alerts_count: 0,
            analysis_duration_ms: 0,  // TODO: Calculate properly
            highlights: vec![
                format!("Critical issues: {}", critical_issues_count),
                format!("Warnings: {}", warnings_count),
            ],
        };

        Ok(AnalysisReport {
            analyzer: self.name().to_string(),
            timestamp: now,
            period_start,
            period_end: now,
            summary,
            insights,
            recommendations,
            alerts: vec![],
            metrics: report_metrics,
        })
    }

    fn get_stats(&self) -> AnalyzerStats {
        // This is synchronous, so we can't await. Return a default for now.
        // In a real implementation, we'd use try_read() or similar
        AnalyzerStats {
            analyzer: "QualityAnalyzer".to_string(),
            events_processed: 0,
            events_dropped: 0,
            insights_generated: 0,
            recommendations_generated: 0,
            alerts_triggered: 0,  // was: alerts_generated
            analysis_time_ms: 0,
            avg_analysis_time_us: 0.0,  // was: avg_processing_time_ms
            memory_usage_bytes: 0,
            error_count: 0,  // was: errors_encountered
            last_error: None,
            uptime_seconds: 0,
        }
    }

    async fn reset(&mut self) -> AnalyzerResult<()> {
        info!("Resetting Quality Analyzer");

        let mut metrics = self.metrics.write().await;
        *metrics = QualityMetrics::new(&self.config);

        let mut stats = self.stats.write().await;
        *stats = AnalyzerStats::default();

        Ok(())
    }

    async fn health_check(&self) -> AnalyzerResult<()> {
        if self.state != AnalyzerState::Running {
            return Err(AnalyzerError::NotRunning);
        }

        let metrics = self.metrics.read().await;

        // Check if we're receiving events
        if let Some(last_event) = metrics.first_event_time {
            let elapsed = Utc::now() - last_event;
            if elapsed > ChronoDuration::minutes(10) {
                warn!("No events received in the last 10 minutes");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_quality_analyzer_lifecycle() {
        let mut analyzer = QualityAnalyzer::with_defaults();

        assert_eq!(analyzer.state(), AnalyzerState::Initialized);

        analyzer.start().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Running);

        analyzer.stop().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Stopped);
    }

    #[tokio::test]
    async fn test_response_event_processing() {
        let mut analyzer = QualityAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = AnalyzerEvent::Response {
            timestamp: Utc::now(),
            request_id: "req-1".to_string(),
            model: "gpt-4".to_string(),
            completion_tokens: 100,
            total_tokens: 200,
            latency_ms: 500,
            success: true,
            error: None,
            metadata: HashMap::new(),
        };

        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.successful_requests, 1);
        assert_eq!(metrics.failed_requests, 0);
    }

    #[tokio::test]
    async fn test_error_tracking() {
        let mut analyzer = QualityAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Process 5 successful and 2 failed requests
        for i in 0..7 {
            let success = i < 5;
            let error = if success {
                None
            } else {
                Some("timeout".to_string())
            };

            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: format!("req-{}", i),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: 500,
                success,
                error,
                metadata: HashMap::new(),
            };

            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.total_requests, 7);
        assert_eq!(metrics.successful_requests, 5);
        assert_eq!(metrics.failed_requests, 2);
        assert!((metrics.current_success_rate() - 71.43).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_feedback_processing() {
        let mut analyzer = QualityAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Process some ratings
        for rating in vec![5.0, 4.5, 4.0, 3.5, 3.0] {
            let event = AnalyzerEvent::Feedback {
                timestamp: Utc::now(),
                request_id: "req-1".to_string(),
                rating: Some(rating),
                feedback_type: Some("positive".to_string()),
                comment: None,
                metadata: {
                    let mut m = HashMap::new();
                    m.insert("model".to_string(), serde_json::json!("gpt-4"));
                    m
                },
            };

            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        let avg_rating = metrics.average_rating().unwrap();
        assert!((avg_rating - 4.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_high_error_rate_insight() {
        let config = QualityAnalyzerConfig {
            error_rate_warning_pct: 5.0,
            min_samples_for_quality: 10,
            ..Default::default()
        };

        let mut analyzer = QualityAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Process 10 requests with 15% error rate
        for i in 0..10 {
            let success = i < 8; // 8 success, 2 failures = 20% error rate

            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: format!("req-{}", i),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: 500,
                success,
                error: if success {
                    None
                } else {
                    Some("error".to_string())
                },
                metadata: HashMap::new(),
            };

            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert!(!report.insights.is_empty());
        assert!(report
            .insights
            .iter()
            .any(|i| i.title.contains("error rate")));
    }

    #[tokio::test]
    async fn test_low_rating_insight() {
        let config = QualityAnalyzerConfig {
            min_avg_rating: 4.0,
            min_samples_for_quality: 5,
            ..Default::default()
        };

        let mut analyzer = QualityAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Add some requests first to meet sample requirements
        for i in 0..10 {
            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: format!("req-{}", i),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: 500,
                success: true,
                error: None,
                metadata: HashMap::new(),
            };
            analyzer.process_event(event).await.unwrap();
        }

        // Process low ratings
        for rating in vec![2.0, 2.5, 3.0, 2.5, 3.0] {
            let event = AnalyzerEvent::Feedback {
                timestamp: Utc::now(),
                request_id: "req-1".to_string(),
                rating: Some(rating),
                feedback_type: Some("negative".to_string()),
                comment: None,
                metadata: {
                    let mut m = HashMap::new();
                    m.insert("model".to_string(), serde_json::json!("gpt-4"));
                    m
                },
            };

            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert!(report.insights.iter().any(|i| i.title.contains("rating")));
    }

    #[tokio::test]
    async fn test_sla_compliance_tracking() {
        let config = QualityAnalyzerConfig {
            sla_p95_ms: 500.0,
            sla_p99_ms: 1000.0,
            min_samples_for_quality: 10,
            ..Default::default()
        };

        let mut analyzer = QualityAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Process requests with varying latencies
        for latency in vec![100, 200, 300, 400, 600, 800, 1200, 300, 400, 500, 600, 700] {
            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: "req-1".to_string(),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: latency,
                success: true,
                error: None,
                metadata: HashMap::new(),
            };

            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        assert!(metrics.sla_checks > 0);
    }

    #[tokio::test]
    async fn test_recommendation_generation() {
        let config = QualityAnalyzerConfig {
            error_rate_warning_pct: 5.0,
            min_samples_for_quality: 10,
            ..Default::default()
        };

        let mut analyzer = QualityAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Generate high error rate
        for i in 0..10 {
            let success = i < 7; // 30% error rate

            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: format!("req-{}", i),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: 500,
                success,
                error: if success {
                    None
                } else {
                    Some("error".to_string())
                },
                metadata: HashMap::new(),
            };

            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert!(!report.recommendations.is_empty());
        assert!(report
            .recommendations
            .iter()
            .any(|r| r.title.contains("error") || r.title.contains("success")));
    }

    #[tokio::test]
    async fn test_reset() {
        let mut analyzer = QualityAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Process some events
        for _ in 0..5 {
            let event = AnalyzerEvent::Response {
                timestamp: Utc::now(),
                request_id: "req-1".to_string(),
                model: "gpt-4".to_string(),
                completion_tokens: 100,
                total_tokens: 200,
                latency_ms: 500,
                success: true,
                error: None,
                metadata: HashMap::new(),
            };

            analyzer.process_event(event).await.unwrap();
        }

        let metrics_before = analyzer.metrics.read().await;
        assert_eq!(metrics_before.total_requests, 5);
        drop(metrics_before);

        // Reset
        analyzer.reset().await.unwrap();

        let metrics_after = analyzer.metrics.read().await;
        assert_eq!(metrics_after.total_requests, 0);
    }

    #[tokio::test]
    async fn test_health_check() {
        let mut analyzer = QualityAnalyzer::with_defaults();

        // Health check should fail when not running
        assert!(analyzer.health_check().await.is_err());

        // Start and health check should succeed
        analyzer.start().await.unwrap();
        assert!(analyzer.health_check().await.is_ok());
    }
}
