//! Anomaly Analyzer
//!
//! Detects statistical anomalies, outliers, and drift in LLM metrics using
//! Z-score and IQR methods. Provides confidence scoring and categorized alerts.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use super::stats::{
    calculate_iqr, is_outlier_iqr, is_outlier_zscore, mean, std_dev, z_score, CircularBuffer,
};
use super::traits::{
    Analyzer, AnalyzerConfig, AnalyzerError, AnalyzerResult, AnalyzerState,
};
use super::types::{
    Action, ActionType, Alert, AlertStatus, AnalysisReport, AnalyzerEvent, AnalyzerStats,
    Confidence, Evidence, EvidenceType, Impact, ImpactMetric, Insight, InsightCategory, Priority,
    Recommendation, ReportSummary, RiskLevel, Severity, Threshold, ThresholdOperator,
};

/// Configuration specific to the Anomaly Analyzer
#[derive(Debug, Clone)]
pub struct AnomalyAnalyzerConfig {
    /// Base analyzer configuration
    pub base: AnalyzerConfig,

    /// Z-score threshold for anomaly detection (|z| > threshold)
    pub zscore_threshold: f64,

    /// IQR multiplier for outlier detection
    pub iqr_multiplier: f64,

    /// Minimum samples required for anomaly detection
    pub min_samples: usize,

    /// Number of consecutive anomalies required to trigger alert
    pub consecutive_threshold: usize,

    /// Window size for anomaly detection
    pub anomaly_window_size: usize,

    /// Enable drift detection
    pub enable_drift_detection: bool,

    /// Drift detection window size
    pub drift_window_size: usize,
}

impl Default for AnomalyAnalyzerConfig {
    fn default() -> Self {
        Self {
            base: AnalyzerConfig::default(),
            zscore_threshold: 3.0,      // 99.7% confidence interval
            iqr_multiplier: 1.5,         // Standard outlier detection
            min_samples: 30,             // Minimum for statistical significance
            consecutive_threshold: 2,    // Reduce false positives
            anomaly_window_size: 1000,
            enable_drift_detection: true,
            drift_window_size: 100,
        }
    }
}

/// Type of anomaly detected
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyType {
    /// Latency anomaly
    LatencyAnomaly,
    /// Cost anomaly
    CostAnomaly,
    /// Request volume anomaly
    VolumeAnomaly,
    /// Error rate anomaly
    ErrorRateAnomaly,
    /// Unknown anomaly type
    UnknownAnomaly,
}

impl AnomalyType {
    fn as_str(&self) -> &str {
        match self {
            AnomalyType::LatencyAnomaly => "latency_anomaly",
            AnomalyType::CostAnomaly => "cost_anomaly",
            AnomalyType::VolumeAnomaly => "volume_anomaly",
            AnomalyType::ErrorRateAnomaly => "error_rate_anomaly",
            AnomalyType::UnknownAnomaly => "unknown_anomaly",
        }
    }
}

/// Detected anomaly record
#[derive(Debug, Clone)]
struct AnomalyRecord {
    timestamp: DateTime<Utc>,
    anomaly_type: AnomalyType,
    value: f64,
    zscore: f64,
    severity: Severity,
    consecutive_count: usize,
}

/// Metrics tracked for anomaly detection
#[derive(Debug, Clone)]
struct AnomalyMetrics {
    /// Latency samples (ms)
    latencies: CircularBuffer<f64>,

    /// Cost samples (USD)
    costs: CircularBuffer<f64>,

    /// Request timestamps for volume calculation
    request_times: VecDeque<DateTime<Utc>>,

    /// Error count over time
    errors: CircularBuffer<u64>,
    total_requests: CircularBuffer<u64>,

    /// Detected anomalies
    anomalies: VecDeque<AnomalyRecord>,

    /// Consecutive anomaly counters
    consecutive_latency: usize,
    consecutive_cost: usize,
    consecutive_volume: usize,
    consecutive_error: usize,

    /// Drift detection baseline
    baseline_latency_mean: Option<f64>,
    baseline_cost_mean: Option<f64>,
    baseline_volume_mean: Option<f64>,
}

impl AnomalyMetrics {
    fn new(window_size: usize, drift_window_size: usize) -> Self {
        Self {
            latencies: CircularBuffer::new(window_size),
            costs: CircularBuffer::new(window_size),
            request_times: VecDeque::with_capacity(window_size),
            errors: CircularBuffer::new(window_size),
            total_requests: CircularBuffer::new(window_size),
            anomalies: VecDeque::with_capacity(1000),
            consecutive_latency: 0,
            consecutive_cost: 0,
            consecutive_volume: 0,
            consecutive_error: 0,
            baseline_latency_mean: None,
            baseline_cost_mean: None,
            baseline_volume_mean: None,
        }
    }

    fn record_latency(&mut self, latency_ms: f64) {
        self.latencies.push(latency_ms);
    }

    fn record_cost(&mut self, cost_usd: f64) {
        self.costs.push(cost_usd);
    }

    fn record_request(&mut self, timestamp: DateTime<Utc>) {
        self.request_times.push_back(timestamp);
        // Keep only recent requests for volume calculation
        while self.request_times.len() > 1000 {
            self.request_times.pop_front();
        }
    }

    fn record_error(&mut self, is_error: bool) {
        let error_val = if is_error { 1 } else { 0 };
        self.errors.push(error_val);
        self.total_requests.push(1);
    }

    fn add_anomaly(&mut self, record: AnomalyRecord) {
        self.anomalies.push_back(record);
        // Keep only recent anomalies
        while self.anomalies.len() > 1000 {
            self.anomalies.pop_front();
        }
    }

    fn get_recent_anomalies(&self, duration: Duration) -> Vec<&AnomalyRecord> {
        let cutoff = Utc::now() - chrono::Duration::from_std(duration).unwrap();
        self.anomalies
            .iter()
            .filter(|a| a.timestamp > cutoff)
            .collect()
    }

    fn calculate_request_volume(&self, duration: Duration) -> f64 {
        let cutoff = Utc::now() - chrono::Duration::from_std(duration).unwrap();
        let count = self
            .request_times
            .iter()
            .filter(|&&t| t > cutoff)
            .count();
        count as f64 / duration.as_secs_f64()
    }

    fn calculate_error_rate(&self) -> f64 {
        if self.total_requests.is_empty() {
            return 0.0;
        }

        let errors: u64 = self.errors.to_vec().iter().sum();
        let total: u64 = self.total_requests.to_vec().iter().sum();

        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }
}

/// Anomaly Analyzer implementation
pub struct AnomalyAnalyzer {
    config: AnomalyAnalyzerConfig,
    state: AnalyzerState,
    metrics: Arc<RwLock<AnomalyMetrics>>,
    stats: Arc<RwLock<AnalyzerStats>>,
    start_time: Instant,
    last_report_time: Arc<RwLock<Instant>>,
}

impl AnomalyAnalyzer {
    /// Create a new Anomaly Analyzer
    pub fn new(config: AnomalyAnalyzerConfig) -> Self {
        let window_size = config.anomaly_window_size;
        let drift_window_size = config.drift_window_size;

        Self {
            config,
            state: AnalyzerState::Initialized,
            metrics: Arc::new(RwLock::new(AnomalyMetrics::new(
                window_size,
                drift_window_size,
            ))),
            stats: Arc::new(RwLock::new(AnalyzerStats {
                analyzer: "anomaly".to_string(),
                ..Default::default()
            })),
            start_time: Instant::now(),
            last_report_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(AnomalyAnalyzerConfig::default())
    }

    /// Process a response event
    async fn process_response(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Response {
            timestamp,
            latency_ms,
            success,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_latency(latency_ms as f64);
            metrics.record_request(timestamp);
            metrics.record_error(!success);

            trace!(
                "Recorded response metrics: latency={}ms, success={}",
                latency_ms,
                success
            );
        }

        Ok(())
    }

    /// Process a cost event
    async fn process_cost(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Cost {
            timestamp,
            cost_usd,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_cost(cost_usd);
            metrics.record_request(timestamp);

            trace!("Recorded cost: ${:.4}", cost_usd);
        }

        Ok(())
    }

    /// Process a metric event
    async fn process_metric(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Metric {
            name,
            value,
            timestamp,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;

            if name.contains("latency") || name.contains("duration") {
                metrics.record_latency(value);
                metrics.record_request(timestamp);
            } else if name.contains("cost") {
                metrics.record_cost(value);
                metrics.record_request(timestamp);
            }
        }

        Ok(())
    }

    /// Detect latency anomalies
    async fn detect_latency_anomalies(&self) -> Vec<AnomalyRecord> {
        let mut metrics = self.metrics.write().await;
        let latencies = metrics.latencies.to_vec();

        if latencies.len() < self.config.min_samples {
            return Vec::new();
        }

        let latest = latencies[latencies.len() - 1];
        let m = mean(&latencies);
        let sd = std_dev(&latencies);
        let z = z_score(latest, m, sd);

        let mut anomalies = Vec::new();

        // Z-score method
        if is_outlier_zscore(latest, m, sd, self.config.zscore_threshold) {
            metrics.consecutive_latency += 1;

            if metrics.consecutive_latency >= self.config.consecutive_threshold {
                let severity = if z.abs() > 5.0 {
                    Severity::Critical
                } else if z.abs() > 4.0 {
                    Severity::Error
                } else {
                    Severity::Warning
                };

                anomalies.push(AnomalyRecord {
                    timestamp: Utc::now(),
                    anomaly_type: AnomalyType::LatencyAnomaly,
                    value: latest,
                    zscore: z,
                    severity,
                    consecutive_count: metrics.consecutive_latency,
                });
            }
        } else {
            metrics.consecutive_latency = 0;
        }

        // IQR method
        if is_outlier_iqr(latest, &latencies, self.config.iqr_multiplier) {
            debug!("IQR outlier detected: latency={}ms", latest);
        }

        anomalies
    }

    /// Detect cost anomalies
    async fn detect_cost_anomalies(&self) -> Vec<AnomalyRecord> {
        let mut metrics = self.metrics.write().await;
        let costs = metrics.costs.to_vec();

        if costs.len() < self.config.min_samples {
            return Vec::new();
        }

        let latest = costs[costs.len() - 1];
        let m = mean(&costs);
        let sd = std_dev(&costs);
        let z = z_score(latest, m, sd);

        let mut anomalies = Vec::new();

        if is_outlier_zscore(latest, m, sd, self.config.zscore_threshold) {
            metrics.consecutive_cost += 1;

            if metrics.consecutive_cost >= self.config.consecutive_threshold {
                let severity = if z.abs() > 5.0 {
                    Severity::Critical
                } else if z.abs() > 4.0 {
                    Severity::Error
                } else {
                    Severity::Warning
                };

                anomalies.push(AnomalyRecord {
                    timestamp: Utc::now(),
                    anomaly_type: AnomalyType::CostAnomaly,
                    value: latest,
                    zscore: z,
                    severity,
                    consecutive_count: metrics.consecutive_cost,
                });
            }
        } else {
            metrics.consecutive_cost = 0;
        }

        anomalies
    }

    /// Detect volume anomalies
    async fn detect_volume_anomalies(&self) -> Vec<AnomalyRecord> {
        let metrics = self.metrics.read().await;
        let mut anomalies = Vec::new();

        if metrics.request_times.len() < self.config.min_samples {
            return anomalies;
        }

        // Calculate volume over last minute
        let current_volume = metrics.calculate_request_volume(Duration::from_secs(60));

        // Compare with baseline if available
        if let Some(baseline) = metrics.baseline_volume_mean {
            let change_pct = ((current_volume - baseline) / baseline).abs() * 100.0;

            if change_pct > 50.0 {
                // 50% change threshold
                let severity = if change_pct > 100.0 {
                    Severity::Critical
                } else if change_pct > 75.0 {
                    Severity::Error
                } else {
                    Severity::Warning
                };

                anomalies.push(AnomalyRecord {
                    timestamp: Utc::now(),
                    anomaly_type: AnomalyType::VolumeAnomaly,
                    value: current_volume,
                    zscore: change_pct / 10.0, // Approximate z-score
                    severity,
                    consecutive_count: 1,
                });
            }
        }

        anomalies
    }

    /// Detect error rate anomalies
    async fn detect_error_rate_anomalies(&self) -> Vec<AnomalyRecord> {
        let metrics = self.metrics.read().await;
        let mut anomalies = Vec::new();

        if metrics.total_requests.len() < self.config.min_samples {
            return anomalies;
        }

        let error_rate = metrics.calculate_error_rate();

        // Alert if error rate exceeds thresholds
        if error_rate > 0.05 {
            // 5% error rate
            let severity = if error_rate > 0.20 {
                Severity::Critical
            } else if error_rate > 0.10 {
                Severity::Error
            } else {
                Severity::Warning
            };

            anomalies.push(AnomalyRecord {
                timestamp: Utc::now(),
                anomaly_type: AnomalyType::ErrorRateAnomaly,
                value: error_rate,
                zscore: error_rate * 10.0, // Approximate z-score
                severity,
                consecutive_count: 1,
            });
        }

        anomalies
    }

    /// Analyze current metrics and generate insights
    async fn analyze_metrics(&self) -> Vec<Insight> {
        let mut insights = Vec::new();

        // Detect all anomaly types
        let latency_anomalies = self.detect_latency_anomalies().await;
        let cost_anomalies = self.detect_cost_anomalies().await;
        let volume_anomalies = self.detect_volume_anomalies().await;
        let error_anomalies = self.detect_error_rate_anomalies().await;

        // Store anomalies
        let mut metrics = self.metrics.write().await;
        for anomaly in &latency_anomalies {
            metrics.add_anomaly(anomaly.clone());
            insights.push(self.create_anomaly_insight(anomaly));
        }
        for anomaly in &cost_anomalies {
            metrics.add_anomaly(anomaly.clone());
            insights.push(self.create_anomaly_insight(anomaly));
        }
        for anomaly in &volume_anomalies {
            metrics.add_anomaly(anomaly.clone());
            insights.push(self.create_anomaly_insight(anomaly));
        }
        for anomaly in &error_anomalies {
            metrics.add_anomaly(anomaly.clone());
            insights.push(self.create_anomaly_insight(anomaly));
        }

        // Detect collective anomalies (multiple types occurring together)
        let recent = metrics.get_recent_anomalies(Duration::from_secs(300)); // 5 minutes
        if recent.len() >= 3 {
            insights.push(self.create_collective_anomaly_insight(&recent));
        }

        insights
    }

    /// Create insight for an anomaly
    fn create_anomaly_insight(&self, anomaly: &AnomalyRecord) -> Insight {
        let (title, description, category) = match anomaly.anomaly_type {
            AnomalyType::LatencyAnomaly => (
                format!("Latency Anomaly: {:.2}ms (z={:.2})", anomaly.value, anomaly.zscore),
                format!(
                    "Detected latency anomaly with value {:.2}ms. Z-score of {:.2} indicates this is significantly outside normal range. {} consecutive anomalies detected.",
                    anomaly.value, anomaly.zscore, anomaly.consecutive_count
                ),
                InsightCategory::Anomaly,
            ),
            AnomalyType::CostAnomaly => (
                format!("Cost Anomaly: ${:.4} (z={:.2})", anomaly.value, anomaly.zscore),
                format!(
                    "Detected cost anomaly with value ${:.4}. Z-score of {:.2} indicates unusual spending pattern. {} consecutive anomalies detected.",
                    anomaly.value, anomaly.zscore, anomaly.consecutive_count
                ),
                InsightCategory::Cost,
            ),
            AnomalyType::VolumeAnomaly => (
                format!("Volume Anomaly: {:.2} req/s", anomaly.value),
                format!(
                    "Detected unusual request volume: {:.2} req/s. This represents a significant deviation from baseline traffic patterns.",
                    anomaly.value
                ),
                InsightCategory::Anomaly,
            ),
            AnomalyType::ErrorRateAnomaly => (
                format!("Error Rate Anomaly: {:.1}%", anomaly.value * 100.0),
                format!(
                    "Detected elevated error rate: {:.1}%. This is significantly higher than expected and requires investigation.",
                    anomaly.value * 100.0
                ),
                InsightCategory::Risk,
            ),
            AnomalyType::UnknownAnomaly => (
                "Unknown Anomaly Detected".to_string(),
                "Detected anomaly of unknown type.".to_string(),
                InsightCategory::Anomaly,
            ),
        };

        // Calculate confidence based on z-score and consecutive count
        let confidence_val = (anomaly.zscore.abs() / 5.0)
            .min(1.0)
            .max(0.6)
            * (anomaly.consecutive_count as f64 / (self.config.consecutive_threshold as f64 + 2.0))
                .min(1.0)
                .max(0.7);

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "anomaly".to_string(),
            timestamp: anomaly.timestamp,
            severity: anomaly.severity,
            confidence: Confidence::new(confidence_val),
            title,
            description,
            category,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Anomaly,
                description: format!(
                    "Z-score: {:.2}, Value: {:.2}, Type: {}",
                    anomaly.zscore,
                    anomaly.value,
                    anomaly.anomaly_type.as_str()
                ),
                data: serde_json::json!({
                    "zscore": anomaly.zscore,
                    "value": anomaly.value,
                    "anomaly_type": anomaly.anomaly_type.as_str(),
                    "consecutive_count": anomaly.consecutive_count,
                }),
                timestamp: anomaly.timestamp,
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("zscore".to_string(), anomaly.zscore);
                m.insert("value".to_string(), anomaly.value);
                m.insert("consecutive_count".to_string(), anomaly.consecutive_count as f64);
                m
            },
            tags: vec![
                "anomaly".to_string(),
                anomaly.anomaly_type.as_str().to_string(),
            ],
        }
    }

    /// Create insight for collective anomalies
    fn create_collective_anomaly_insight(&self, anomalies: &[&AnomalyRecord]) -> Insight {
        let anomaly_types: Vec<String> = anomalies
            .iter()
            .map(|a| a.anomaly_type.as_str().to_string())
            .collect();

        let highest_severity = anomalies
            .iter()
            .map(|a| a.severity)
            .max()
            .unwrap_or(Severity::Warning);

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "anomaly".to_string(),
            timestamp: Utc::now(),
            severity: highest_severity,
            confidence: Confidence::new(0.85),
            title: format!("Collective Anomaly: {} anomalies detected", anomalies.len()),
            description: format!(
                "Detected {} anomalies occurring together in a short time window. Types: {}. This pattern suggests a systemic issue requiring investigation.",
                anomalies.len(),
                anomaly_types.join(", ")
            ),
            category: InsightCategory::Anomaly,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Pattern,
                description: "Multiple concurrent anomalies detected".to_string(),
                data: serde_json::json!({
                    "count": anomalies.len(),
                    "types": anomaly_types,
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("anomaly_count".to_string(), anomalies.len() as f64);
                m
            },
            tags: vec!["collective_anomaly".to_string(), "pattern".to_string()],
        }
    }

    /// Generate recommendations based on insights
    fn generate_recommendations(&self, insights: &[Insight]) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        for insight in insights {
            if insight.title.contains("Latency Anomaly") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "anomaly".to_string(),
                    timestamp: Utc::now(),
                    priority: if insight.severity == Severity::Critical {
                        Priority::Urgent
                    } else {
                        Priority::High
                    },
                    title: "Investigate latency anomaly root cause".to_string(),
                    description: "Latency anomaly detected. Investigate infrastructure, database queries, external API calls, and resource utilization.".to_string(),
                    action: Action {
                        action_type: ActionType::Investigate,
                        instructions: "1. Check infrastructure metrics (CPU, memory, network)\n2. Review recent deployments or configuration changes\n3. Analyze slow query logs\n4. Check external API response times\n5. Review application logs for errors".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(2.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "latency".to_string(),
                            current: insight.metrics.get("value").copied().unwrap_or(0.0),
                            expected: insight.metrics.get("value").copied().unwrap_or(0.0) * 0.8,
                            unit: "ms".to_string(),
                            improvement_pct: 20.0,
                        }),
                        cost: None,
                        quality: None,
                        description: "Resolve latency anomaly and restore normal performance".to_string(),
                    },
                    confidence: Confidence::new(0.75),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["investigation".to_string(), "latency".to_string()],
                });
            }

            if insight.title.contains("Cost Anomaly") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "anomaly".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::High,
                    title: "Review and adjust cost controls".to_string(),
                    description: "Cost anomaly detected. Review usage patterns and implement cost controls.".to_string(),
                    action: Action {
                        action_type: ActionType::Review,
                        instructions: "1. Review token usage patterns\n2. Check for unexpected traffic or abuse\n3. Implement rate limiting if needed\n4. Consider model switching for lower-priority tasks\n5. Set up cost alerts and budgets".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(1.5),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: Some(ImpactMetric {
                            name: "cost".to_string(),
                            current: insight.metrics.get("value").copied().unwrap_or(0.0),
                            expected: insight.metrics.get("value").copied().unwrap_or(0.0) * 0.7,
                            unit: "usd".to_string(),
                            improvement_pct: 30.0,
                        }),
                        quality: None,
                        description: "Reduce unexpected costs and establish controls".to_string(),
                    },
                    confidence: Confidence::new(0.80),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["cost".to_string(), "review".to_string()],
                });
            }

            if insight.title.contains("Error Rate Anomaly") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "anomaly".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Urgent,
                    title: "Investigate and resolve elevated error rate".to_string(),
                    description: "Elevated error rate detected. Immediate investigation required.".to_string(),
                    action: Action {
                        action_type: ActionType::Investigate,
                        instructions: "1. Review error logs and identify error patterns\n2. Check API status and rate limits\n3. Verify authentication and credentials\n4. Test with known-good requests\n5. Implement circuit breaker if needed".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(1.0),
                        risk_level: RiskLevel::Medium,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: None,
                        quality: Some(ImpactMetric {
                            name: "error_rate".to_string(),
                            current: insight.metrics.get("value").copied().unwrap_or(0.0),
                            expected: 0.01, // Target 1% error rate
                            unit: "rate".to_string(),
                            improvement_pct: 80.0,
                        }),
                        description: "Reduce error rate to normal levels".to_string(),
                    },
                    confidence: Confidence::new(0.85),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["errors".to_string(), "investigation".to_string()],
                });
            }

            if insight.title.contains("Collective Anomaly") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "anomaly".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Urgent,
                    title: "Add monitoring for correlated metrics".to_string(),
                    description: "Multiple anomalies detected together. Add monitoring to track correlated metrics.".to_string(),
                    action: Action {
                        action_type: ActionType::Monitor,
                        instructions: "1. Set up dashboards for correlated metrics\n2. Create alerts for anomaly patterns\n3. Implement automated runbooks for common patterns\n4. Review system architecture for single points of failure".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(3.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: None,
                        quality: None,
                        description: "Improved visibility and faster incident response".to_string(),
                    },
                    confidence: Confidence::new(0.70),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["monitoring".to_string(), "observability".to_string()],
                });
            }
        }

        recommendations
    }

    /// Generate alerts based on anomalies
    fn generate_alerts(&self, insights: &[Insight]) -> Vec<Alert> {
        let mut alerts = Vec::new();

        for insight in insights {
            if insight.severity.is_urgent() {
                alerts.push(Alert {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "anomaly".to_string(),
                    timestamp: insight.timestamp,
                    severity: insight.severity,
                    title: insight.title.clone(),
                    message: insight.description.clone(),
                    status: AlertStatus::Firing,
                    threshold: None,
                    related_insights: vec![insight.id.clone()],
                    tags: insight.tags.clone(),
                    metadata: HashMap::new(),
                });
            }
        }

        alerts
    }
}

#[async_trait]
impl Analyzer for AnomalyAnalyzer {
    fn name(&self) -> &str {
        "anomaly"
    }

    fn config(&self) -> &AnalyzerConfig {
        &self.config.base
    }

    fn state(&self) -> AnalyzerState {
        self.state
    }

    async fn start(&mut self) -> AnalyzerResult<()> {
        if self.state != AnalyzerState::Initialized && self.state != AnalyzerState::Stopped {
            return Err(AnalyzerError::InvalidState(format!(
                "Cannot start from state: {:?}",
                self.state
            )));
        }

        self.state = AnalyzerState::Starting;
        info!("Starting Anomaly Analyzer");

        self.state = AnalyzerState::Running;
        info!("Anomaly Analyzer started successfully");

        Ok(())
    }

    async fn stop(&mut self) -> AnalyzerResult<()> {
        if !matches!(
            self.state,
            AnalyzerState::Running | AnalyzerState::Draining
        ) {
            return Err(AnalyzerError::InvalidState(format!(
                "Cannot stop from state: {:?}",
                self.state
            )));
        }

        self.state = AnalyzerState::Draining;
        info!("Stopping Anomaly Analyzer");

        self.state = AnalyzerState::Stopped;
        info!("Anomaly Analyzer stopped");

        Ok(())
    }

    async fn process_event(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if !self.state.can_accept_events() {
            return Err(AnalyzerError::InvalidState(format!(
                "Cannot process events in state: {:?}",
                self.state
            )));
        }

        let start = Instant::now();

        let result = match &event {
            AnalyzerEvent::Response { .. } => self.process_response(event).await,
            AnalyzerEvent::Cost { .. } => self.process_cost(event).await,
            AnalyzerEvent::Metric { .. } => self.process_metric(event).await,
            _ => Ok(()), // Ignore other event types
        };

        // Update stats
        let mut stats = self.stats.write().await;
        stats.events_processed += 1;
        let elapsed = start.elapsed().as_micros() as u64;
        stats.analysis_time_ms += elapsed / 1000;
        stats.avg_analysis_time_us =
            (stats.analysis_time_ms * 1000) as f64 / stats.events_processed as f64;

        if result.is_err() {
            stats.error_count += 1;
            stats.last_error = result.as_ref().err().map(|e| e.to_string());
        }

        result
    }

    async fn generate_report(&self) -> AnalyzerResult<AnalysisReport> {
        let metrics = self.metrics.read().await;
        let stats = self.stats.read().await;
        let last_report = *self.last_report_time.read().await;

        let insights = self.analyze_metrics().await;
        let recommendations = self.generate_recommendations(&insights);
        let alerts = self.generate_alerts(&insights);

        let recent_anomalies = metrics.get_recent_anomalies(Duration::from_secs(3600)); // Last hour

        let mut report_metrics = HashMap::new();
        report_metrics.insert("anomalies_detected".to_string(), recent_anomalies.len() as f64);
        report_metrics.insert("latency_samples".to_string(), metrics.latencies.len() as f64);
        report_metrics.insert("cost_samples".to_string(), metrics.costs.len() as f64);

        let report = AnalysisReport {
            analyzer: "anomaly".to_string(),
            timestamp: Utc::now(),
            period_start: Utc::now() - chrono::Duration::from_std(last_report.elapsed()).unwrap(),
            period_end: Utc::now(),
            summary: ReportSummary {
                events_processed: stats.events_processed,
                events_per_second: stats.events_processed as f64
                    / last_report.elapsed().as_secs_f64().max(1.0),
                insights_count: insights.len() as u64,
                recommendations_count: recommendations.len() as u64,
                alerts_count: alerts.len() as u64,
                analysis_duration_ms: stats.analysis_time_ms,
                highlights: vec![
                    format!("Anomalies detected: {}", recent_anomalies.len()),
                    format!("Alerts triggered: {}", alerts.len()),
                    format!("Samples analyzed: {}", metrics.latencies.len()),
                ],
            },
            insights,
            recommendations,
            alerts,
            metrics: report_metrics,
        };

        // Update last report time
        *self.last_report_time.write().await = Instant::now();

        Ok(report)
    }

    fn get_stats(&self) -> AnalyzerStats {
        AnalyzerStats {
            analyzer: "anomaly".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            ..Default::default()
        }
    }

    async fn reset(&mut self) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = AnomalyMetrics::new(
            self.config.anomaly_window_size,
            self.config.drift_window_size,
        );

        let mut stats = self.stats.write().await;
        *stats = AnalyzerStats {
            analyzer: "anomaly".to_string(),
            ..Default::default()
        };

        info!("Anomaly Analyzer reset");
        Ok(())
    }

    async fn health_check(&self) -> AnalyzerResult<()> {
        let stats = self.stats.read().await;

        // Check error rate
        if stats.events_processed > 0 {
            let error_rate = stats.error_count as f64 / stats.events_processed as f64;
            if error_rate > 0.1 {
                return Err(AnalyzerError::InternalError(format!(
                    "High error rate: {:.2}%",
                    error_rate * 100.0
                )));
            }
        }

        // Check memory usage
        if stats.memory_usage_bytes > self.config.base.max_memory_bytes {
            return Err(AnalyzerError::ResourceExhausted(format!(
                "Memory usage {} exceeds limit {}",
                stats.memory_usage_bytes, self.config.base.max_memory_bytes
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_response_event(latency_ms: u64, success: bool) -> AnalyzerEvent {
        AnalyzerEvent::Response {
            timestamp: Utc::now(),
            request_id: Uuid::new_v4().to_string(),
            model: "gpt-4".to_string(),
            completion_tokens: 100,
            total_tokens: 200,
            latency_ms,
            success,
            error: if success {
                None
            } else {
                Some("Test error".to_string())
            },
            metadata: HashMap::new(),
        }
    }

    fn create_test_cost_event(cost_usd: f64) -> AnalyzerEvent {
        AnalyzerEvent::Cost {
            timestamp: Utc::now(),
            model: "gpt-4".to_string(),
            tokens: 200,
            cost_usd,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_analyzer_lifecycle() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();

        assert_eq!(analyzer.state(), AnalyzerState::Initialized);

        analyzer.start().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Running);

        analyzer.stop().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Stopped);
    }

    #[tokio::test]
    async fn test_process_response_event() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event(500, true);
        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.latencies.len(), 1);
    }

    #[tokio::test]
    async fn test_latency_anomaly_detection() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add normal latencies
        for _ in 0..50 {
            let event = create_test_response_event(100, true);
            analyzer.process_event(event).await.unwrap();
        }

        // Add anomalous latencies (consecutive to trigger detection)
        for _ in 0..3 {
            let event = create_test_response_event(1000, true);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(!insights.is_empty());
        assert!(insights
            .iter()
            .any(|i| i.title.contains("Latency Anomaly")));
    }

    #[tokio::test]
    async fn test_cost_anomaly_detection() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add normal costs
        for _ in 0..50 {
            let event = create_test_cost_event(0.01);
            analyzer.process_event(event).await.unwrap();
        }

        // Add anomalous costs (consecutive)
        for _ in 0..3 {
            let event = create_test_cost_event(0.50);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(insights.iter().any(|i| i.title.contains("Cost Anomaly")));
    }

    #[tokio::test]
    async fn test_error_rate_anomaly_detection() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add events with high error rate
        for i in 0..50 {
            let event = create_test_response_event(500, i >= 40); // 80% failure
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(insights
            .iter()
            .any(|i| i.title.contains("Error Rate Anomaly")));
    }

    #[tokio::test]
    async fn test_consecutive_threshold() {
        let mut config = AnomalyAnalyzerConfig::default();
        config.consecutive_threshold = 3;

        let mut analyzer = AnomalyAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Add normal latencies
        for _ in 0..50 {
            let event = create_test_response_event(100, true);
            analyzer.process_event(event).await.unwrap();
        }

        // Add only 2 anomalous latencies (below threshold)
        for _ in 0..2 {
            let event = create_test_response_event(1000, true);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        // Should not detect anomaly due to consecutive threshold
        assert!(insights
            .iter()
            .filter(|i| i.title.contains("Latency Anomaly"))
            .count()
            == 0);
    }

    #[tokio::test]
    async fn test_generate_report() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add test events
        for _ in 0..20 {
            let event = create_test_response_event(100, true);
            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert_eq!(report.analyzer, "anomaly");
        assert_eq!(report.summary.events_processed, 20);
        assert!(!report.metrics.is_empty());
    }

    #[tokio::test]
    async fn test_reset() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event(500, true);
        analyzer.process_event(event).await.unwrap();

        analyzer.reset().await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.latencies.len(), 0);
        assert_eq!(metrics.anomalies.len(), 0);
    }

    #[tokio::test]
    async fn test_confidence_scoring() {
        let mut analyzer = AnomalyAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add normal latencies
        for _ in 0..50 {
            let event = create_test_response_event(100, true);
            analyzer.process_event(event).await.unwrap();
        }

        // Add high z-score anomaly
        for _ in 0..3 {
            let event = create_test_response_event(2000, true);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        let anomaly_insights: Vec<_> = insights
            .iter()
            .filter(|i| i.title.contains("Latency Anomaly"))
            .collect();

        assert!(!anomaly_insights.is_empty());
        // Confidence should be reasonable (> 0.6)
        assert!(anomaly_insights[0].confidence.value() > 0.6);
    }
}
