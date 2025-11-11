//! Performance Analyzer
//!
//! Analyzes performance metrics including latency percentiles, throughput,
//! token usage, and detects performance degradation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use super::stats::{
    calculate_percentiles, mean, percentage_change, std_dev, CircularBuffer,
    ExponentialMovingAverage,
};
use super::traits::{
    Analyzer, AnalyzerConfig, AnalyzerError, AnalyzerResult, AnalyzerState,
};
use super::types::{
    Action, ActionType, Alert, AlertStatus, AnalysisReport, AnalyzerEvent, AnalyzerStats,
    Confidence, Evidence, EvidenceType, Impact, ImpactMetric, Insight, InsightCategory,
    Priority, Recommendation, ReportSummary, RiskLevel, Severity, Threshold, ThresholdOperator,
};

/// Configuration specific to the Performance Analyzer
#[derive(Debug, Clone)]
pub struct PerformanceAnalyzerConfig {
    /// Base analyzer configuration
    pub base: AnalyzerConfig,

    /// Latency thresholds (ms)
    pub latency_warning_ms: u64,
    pub latency_critical_ms: u64,

    /// Throughput thresholds (requests/sec)
    pub throughput_low_rps: f64,

    /// Performance degradation threshold (percentage)
    pub degradation_threshold_pct: f64,

    /// Window size for percentile calculation
    pub percentile_window_size: usize,

    /// Enable regression detection
    pub enable_regression_detection: bool,
}

impl Default for PerformanceAnalyzerConfig {
    fn default() -> Self {
        Self {
            base: AnalyzerConfig::default(),
            latency_warning_ms: 1000,  // 1 second
            latency_critical_ms: 5000, // 5 seconds
            throughput_low_rps: 1.0,
            degradation_threshold_pct: 20.0,
            percentile_window_size: 1000,
            enable_regression_detection: true,
        }
    }
}

/// Performance metrics tracked over time
#[derive(Debug, Clone)]
struct PerformanceMetrics {
    /// Latency samples (ms)
    latencies: CircularBuffer<f64>,

    /// Token usage per request
    tokens_per_request: CircularBuffer<u32>,

    /// Request timestamps for throughput calculation
    request_times: CircularBuffer<DateTime<Utc>>,

    /// Success/failure counts
    success_count: u64,
    failure_count: u64,

    /// Exponential moving average of latency
    latency_ema: ExponentialMovingAverage,

    /// Historical baseline for comparison
    baseline_p95_ms: Option<f64>,
    baseline_throughput_rps: Option<f64>,
}

impl PerformanceMetrics {
    fn new(window_size: usize) -> Self {
        Self {
            latencies: CircularBuffer::new(window_size),
            tokens_per_request: CircularBuffer::new(window_size),
            request_times: CircularBuffer::new(window_size),
            success_count: 0,
            failure_count: 0,
            latency_ema: ExponentialMovingAverage::new(0.3),
            baseline_p95_ms: None,
            baseline_throughput_rps: None,
        }
    }

    fn record_latency(&mut self, latency_ms: f64) {
        self.latencies.push(latency_ms);
        self.latency_ema.update(latency_ms);
    }

    fn record_tokens(&mut self, tokens: u32) {
        self.tokens_per_request.push(tokens);
    }

    fn record_request(&mut self, timestamp: DateTime<Utc>, success: bool) {
        self.request_times.push(timestamp);
        if success {
            self.success_count += 1;
        } else {
            self.failure_count += 1;
        }
    }

    fn calculate_percentiles(&self) -> HashMap<String, f64> {
        let latencies = self.latencies.to_vec();
        if latencies.is_empty() {
            return HashMap::new();
        }

        let percentiles = calculate_percentiles(latencies, &[50.0, 90.0, 95.0, 99.0, 99.9]);

        percentiles
            .into_iter()
            .map(|(p, v)| (format!("p{}", p), v))
            .collect()
    }

    fn calculate_throughput(&self) -> f64 {
        if self.request_times.len() < 2 {
            return 0.0;
        }

        let times = self.request_times.as_slice();
        let first = times.front().unwrap();
        let last = times.back().unwrap();

        let duration_secs = (*last - *first).num_milliseconds() as f64 / 1000.0;
        if duration_secs == 0.0 {
            return 0.0;
        }

        self.request_times.len() as f64 / duration_secs
    }

    fn get_success_rate(&self) -> f64 {
        let total = self.success_count + self.failure_count;
        if total == 0 {
            return 1.0;
        }
        self.success_count as f64 / total as f64
    }
}

/// Performance Analyzer implementation
pub struct PerformanceAnalyzer {
    config: PerformanceAnalyzerConfig,
    state: AnalyzerState,
    metrics: Arc<RwLock<PerformanceMetrics>>,
    stats: Arc<RwLock<AnalyzerStats>>,
    start_time: Instant,
    last_report_time: Arc<RwLock<Instant>>,
}

impl PerformanceAnalyzer {
    /// Create a new Performance Analyzer
    pub fn new(config: PerformanceAnalyzerConfig) -> Self {
        let window_size = config.percentile_window_size;

        Self {
            config,
            state: AnalyzerState::Initialized,
            metrics: Arc::new(RwLock::new(PerformanceMetrics::new(window_size))),
            stats: Arc::new(RwLock::new(AnalyzerStats {
                analyzer: "performance".to_string(),
                ..Default::default()
            })),
            start_time: Instant::now(),
            last_report_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PerformanceAnalyzerConfig::default())
    }

    /// Process a response event
    async fn process_response(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Response {
            timestamp,
            latency_ms,
            total_tokens,
            success,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_latency(latency_ms as f64);
            metrics.record_tokens(total_tokens);
            metrics.record_request(timestamp, success);

            trace!(
                "Recorded performance metrics: latency={}ms, tokens={}, success={}",
                latency_ms,
                total_tokens,
                success
            );
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
            if name.contains("latency") || name.contains("duration") {
                let mut metrics = self.metrics.write().await;
                metrics.record_latency(value);
                metrics.record_request(timestamp, true);
            }
        }

        Ok(())
    }

    /// Analyze current metrics and generate insights
    async fn analyze_metrics(&self) -> Vec<Insight> {
        let metrics = self.metrics.read().await;
        let mut insights = Vec::new();

        // Check latency thresholds
        let percentiles = metrics.calculate_percentiles();
        if let Some(&p95) = percentiles.get("p95.0") {
            if p95 > self.config.latency_critical_ms as f64 {
                insights.push(self.create_high_latency_insight(p95, Severity::Critical));
            } else if p95 > self.config.latency_warning_ms as f64 {
                insights.push(self.create_high_latency_insight(p95, Severity::Warning));
            }
        }

        // Check throughput
        let throughput = metrics.calculate_throughput();
        if throughput > 0.0 && throughput < self.config.throughput_low_rps {
            insights.push(self.create_low_throughput_insight(throughput));
        }

        // Check success rate
        let success_rate = metrics.get_success_rate();
        if success_rate < 0.95 {
            insights.push(self.create_low_success_rate_insight(success_rate));
        }

        // Check for performance degradation
        if self.config.enable_regression_detection {
            if let Some(baseline_p95) = metrics.baseline_p95_ms {
                if let Some(&current_p95) = percentiles.get("p95.0") {
                    let change_pct = percentage_change(baseline_p95, current_p95);
                    if change_pct > self.config.degradation_threshold_pct {
                        insights.push(
                            self.create_degradation_insight(baseline_p95, current_p95, change_pct),
                        );
                    }
                }
            }
        }

        insights
    }

    /// Create insight for high latency
    fn create_high_latency_insight(&self, p95_latency: f64, severity: Severity) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "performance".to_string(),
            timestamp: Utc::now(),
            severity,
            confidence: Confidence::new(0.95),
            title: format!("High P95 Latency: {:.2}ms", p95_latency),
            description: format!(
                "The 95th percentile latency is {:.2}ms, which exceeds the {} threshold of {}ms. This indicates that 5% of requests are experiencing significant delays.",
                p95_latency,
                if severity == Severity::Critical { "critical" } else { "warning" },
                if severity == Severity::Critical {
                    self.config.latency_critical_ms
                } else {
                    self.config.latency_warning_ms
                }
            ),
            category: InsightCategory::Performance,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Threshold,
                description: format!("P95 latency: {:.2}ms", p95_latency),
                data: serde_json::json!({
                    "p95_latency_ms": p95_latency,
                    "threshold_ms": if severity == Severity::Critical {
                        self.config.latency_critical_ms
                    } else {
                        self.config.latency_warning_ms
                    }
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("p95_latency_ms".to_string(), p95_latency);
                m
            },
            tags: vec!["latency".to_string(), "performance".to_string()],
        }
    }

    /// Create insight for low throughput
    fn create_low_throughput_insight(&self, throughput: f64) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "performance".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Warning,
            confidence: Confidence::new(0.85),
            title: format!("Low Throughput: {:.2} req/s", throughput),
            description: format!(
                "Current throughput is {:.2} requests/second, which is below the expected threshold of {:.2} req/s. This may indicate capacity issues or upstream bottlenecks.",
                throughput, self.config.throughput_low_rps
            ),
            category: InsightCategory::Performance,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Statistical,
                description: format!("Throughput: {:.2} req/s", throughput),
                data: serde_json::json!({
                    "throughput_rps": throughput,
                    "threshold_rps": self.config.throughput_low_rps
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("throughput_rps".to_string(), throughput);
                m
            },
            tags: vec!["throughput".to_string(), "performance".to_string()],
        }
    }

    /// Create insight for low success rate
    fn create_low_success_rate_insight(&self, success_rate: f64) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "performance".to_string(),
            timestamp: Utc::now(),
            severity: if success_rate < 0.90 {
                Severity::Error
            } else {
                Severity::Warning
            },
            confidence: Confidence::new(0.90),
            title: format!("Low Success Rate: {:.1}%", success_rate * 100.0),
            description: format!(
                "Request success rate is {:.1}%, which is below the expected 95% threshold. Investigate error causes and consider implementing retry logic or circuit breakers.",
                success_rate * 100.0
            ),
            category: InsightCategory::Quality,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Statistical,
                description: format!("Success rate: {:.1}%", success_rate * 100.0),
                data: serde_json::json!({
                    "success_rate": success_rate,
                    "threshold": 0.95
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("success_rate".to_string(), success_rate);
                m
            },
            tags: vec!["reliability".to_string(), "errors".to_string()],
        }
    }

    /// Create insight for performance degradation
    fn create_degradation_insight(
        &self,
        baseline: f64,
        current: f64,
        change_pct: f64,
    ) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "performance".to_string(),
            timestamp: Utc::now(),
            severity: if change_pct > 50.0 {
                Severity::Critical
            } else {
                Severity::Warning
            },
            confidence: Confidence::new(0.88),
            title: format!("Performance Degradation: +{:.1}%", change_pct),
            description: format!(
                "P95 latency has increased by {:.1}% from baseline {:.2}ms to current {:.2}ms. This indicates a performance regression that should be investigated.",
                change_pct, baseline, current
            ),
            category: InsightCategory::Performance,
            evidence: vec![
                Evidence {
                    evidence_type: EvidenceType::Comparison,
                    description: "Baseline vs current comparison".to_string(),
                    data: serde_json::json!({
                        "baseline_p95_ms": baseline,
                        "current_p95_ms": current,
                        "change_pct": change_pct
                    }),
                    timestamp: Utc::now(),
                },
            ],
            metrics: {
                let mut m = HashMap::new();
                m.insert("baseline_p95_ms".to_string(), baseline);
                m.insert("current_p95_ms".to_string(), current);
                m.insert("degradation_pct".to_string(), change_pct);
                m
            },
            tags: vec![
                "degradation".to_string(),
                "regression".to_string(),
                "performance".to_string(),
            ],
        }
    }

    /// Generate recommendations based on insights
    fn generate_recommendations(&self, insights: &[Insight]) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        for insight in insights {
            if insight.title.contains("High") && insight.title.contains("Latency") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "performance".to_string(),
                    timestamp: Utc::now(),
                    priority: if insight.severity == Severity::Critical {
                        Priority::Urgent
                    } else {
                        Priority::High
                    },
                    title: "Optimize latency-critical paths".to_string(),
                    description: "Consider enabling caching, optimizing database queries, or scaling resources to reduce latency.".to_string(),
                    action: Action {
                        action_type: ActionType::Optimize,
                        instructions: "1. Enable response caching for frequently requested prompts\n2. Review and optimize slow database queries\n3. Consider horizontal scaling if CPU/memory is constrained".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(4.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "p95_latency".to_string(),
                            current: insight.metrics.get("p95_latency_ms").copied().unwrap_or(0.0),
                            expected: insight.metrics.get("p95_latency_ms").copied().unwrap_or(0.0) * 0.7, // 30% improvement
                            unit: "ms".to_string(),
                            improvement_pct: 30.0,
                        }),
                        cost: None,
                        quality: None,
                        description: "Expected 30% latency reduction".to_string(),
                    },
                    confidence: Confidence::new(0.75),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["optimization".to_string(), "latency".to_string()],
                });
            }

            if insight.title.contains("Low Throughput") {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "performance".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Medium,
                    title: "Scale resources to improve throughput".to_string(),
                    description: "Current throughput is below target. Consider scaling horizontally or vertically.".to_string(),
                    action: Action {
                        action_type: ActionType::Scale,
                        instructions: "1. Monitor CPU and memory utilization\n2. Scale horizontally by adding more instances\n3. Consider load balancing improvements".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(2.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "throughput".to_string(),
                            current: insight.metrics.get("throughput_rps").copied().unwrap_or(0.0),
                            expected: insight.metrics.get("throughput_rps").copied().unwrap_or(0.0) * 2.0, // 2x improvement
                            unit: "req/s".to_string(),
                            improvement_pct: 100.0,
                        }),
                        cost: None,
                        quality: None,
                        description: "Expected 2x throughput increase".to_string(),
                    },
                    confidence: Confidence::new(0.80),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["scaling".to_string(), "throughput".to_string()],
                });
            }
        }

        recommendations
    }
}

#[async_trait]
impl Analyzer for PerformanceAnalyzer {
    fn name(&self) -> &str {
        "performance"
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
        info!("Starting Performance Analyzer");

        // Initialization logic here

        self.state = AnalyzerState::Running;
        info!("Performance Analyzer started successfully");

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
        info!("Stopping Performance Analyzer");

        // Cleanup logic here

        self.state = AnalyzerState::Stopped;
        info!("Performance Analyzer stopped");

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
        let alerts = Vec::new(); // Alerts would be generated based on thresholds

        let mut percentiles = metrics.calculate_percentiles();
        let throughput = metrics.calculate_throughput();
        let success_rate = metrics.get_success_rate();

        let p95_latency = percentiles.get("p95.0").copied().unwrap_or(0.0);

        let mut report_metrics = HashMap::new();
        for (k, v) in percentiles.drain() {
            report_metrics.insert(k, v);
        }
        report_metrics.insert("throughput_rps".to_string(), throughput);
        report_metrics.insert("success_rate".to_string(), success_rate);

        let report = AnalysisReport {
            analyzer: "performance".to_string(),
            timestamp: Utc::now(),
            period_start: Utc::now() - chrono::Duration::from_std(last_report.elapsed()).unwrap(),
            period_end: Utc::now(),
            summary: ReportSummary {
                events_processed: stats.events_processed,
                events_per_second: stats.events_processed as f64
                    / last_report.elapsed().as_secs_f64(),
                insights_count: insights.len() as u64,
                recommendations_count: recommendations.len() as u64,
                alerts_count: alerts.len() as u64,
                analysis_duration_ms: stats.analysis_time_ms,
                highlights: vec![
                    format!("P95 latency: {:.2}ms", p95_latency),
                    format!("Throughput: {:.2} req/s", throughput),
                    format!("Success rate: {:.1}%", success_rate * 100.0),
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
        // This is synchronous, so we can't use .await
        // In production, you'd want to make this async or use blocking read
        AnalyzerStats {
            analyzer: "performance".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            ..Default::default()
        }
    }

    async fn reset(&mut self) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = PerformanceMetrics::new(self.config.percentile_window_size);

        let mut stats = self.stats.write().await;
        *stats = AnalyzerStats {
            analyzer: "performance".to_string(),
            ..Default::default()
        };

        info!("Performance Analyzer reset");
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
            error: if success { None } else { Some("Test error".to_string()) },
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_analyzer_lifecycle() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();

        assert_eq!(analyzer.state(), AnalyzerState::Initialized);

        analyzer.start().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Running);

        analyzer.stop().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Stopped);
    }

    #[tokio::test]
    async fn test_process_response_event() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event(500, true);
        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.latencies.len(), 1);
        assert_eq!(metrics.success_count, 1);
    }

    #[tokio::test]
    async fn test_high_latency_detection() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add several high-latency events
        for _ in 0..100 {
            let event = create_test_response_event(2000, true); // 2 seconds
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(!insights.is_empty());
        assert!(insights
            .iter()
            .any(|i| i.title.contains("High") && i.title.contains("Latency")));
    }

    #[tokio::test]
    async fn test_low_success_rate_detection() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add events with 80% failure rate
        for i in 0..100 {
            let event = create_test_response_event(500, i >= 80);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(insights
            .iter()
            .any(|i| i.title.contains("Low Success Rate")));
    }

    #[tokio::test]
    async fn test_generate_report() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add some test events
        for _ in 0..10 {
            let event = create_test_response_event(500, true);
            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert_eq!(report.analyzer, "performance");
        assert_eq!(report.summary.events_processed, 10);
        assert!(!report.metrics.is_empty());
    }

    #[tokio::test]
    async fn test_reset() {
        let mut analyzer = PerformanceAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event(500, true);
        analyzer.process_event(event).await.unwrap();

        analyzer.reset().await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.latencies.len(), 0);
        assert_eq!(metrics.success_count, 0);
    }
}
