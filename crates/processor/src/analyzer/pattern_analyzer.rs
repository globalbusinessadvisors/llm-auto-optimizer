//! Pattern Analyzer
//!
//! Analyzes temporal, traffic, and usage patterns to identify trends, bursts,
//! seasonality, and forecast future behavior.

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use super::stats::{
    calculate_linear_trend, mean, percentage_change, std_dev, CircularBuffer,
    SimpleMovingAverage,
};
use super::traits::{
    Analyzer, AnalyzerConfig, AnalyzerError, AnalyzerResult, AnalyzerState,
};
use super::types::{
    Action, ActionType, Alert, AlertStatus, AnalysisReport, AnalyzerEvent, AnalyzerStats,
    Confidence, Evidence, EvidenceType, Impact, ImpactMetric, Insight, InsightCategory,
    Priority, Recommendation, ReportSummary, RiskLevel, Severity, Threshold, ThresholdOperator,
};

/// Configuration specific to the Pattern Analyzer
#[derive(Debug, Clone)]
pub struct PatternAnalyzerConfig {
    /// Base analyzer configuration
    pub base: AnalyzerConfig,

    /// Burst detection threshold (multiplier of average)
    pub burst_threshold_multiplier: f64,

    /// Growth rate threshold for alerting (percentage)
    pub growth_rate_threshold_pct: f64,

    /// Minimum requests for pattern detection
    pub min_requests_for_patterns: usize,

    /// Window size for moving averages
    pub moving_average_window: usize,

    /// Enable forecasting
    pub enable_forecasting: bool,
}

impl Default for PatternAnalyzerConfig {
    fn default() -> Self {
        Self {
            base: AnalyzerConfig::default(),
            burst_threshold_multiplier: 2.0,
            growth_rate_threshold_pct: 20.0,
            min_requests_for_patterns: 50,
            moving_average_window: 10,
            enable_forecasting: true,
        }
    }
}

/// Types of patterns that can be detected
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    /// High traffic periods
    PeakHours,
    /// Low traffic periods
    LowTraffic,
    /// Sudden traffic spikes
    TrafficBurst,
    /// Consistent increase over time
    SteadyGrowth,
    /// Weekly/daily cycles
    SeasonalPattern,
}

impl PatternType {
    fn as_str(&self) -> &str {
        match self {
            PatternType::PeakHours => "peak_hours",
            PatternType::LowTraffic => "low_traffic",
            PatternType::TrafficBurst => "traffic_burst",
            PatternType::SteadyGrowth => "steady_growth",
            PatternType::SeasonalPattern => "seasonal_pattern",
        }
    }
}

/// Temporal pattern metrics
#[derive(Debug, Clone)]
struct TemporalMetrics {
    /// Request count by hour of day (0-23)
    hourly_counts: [u64; 24],

    /// Request count by day of week (0-6, Monday=0)
    daily_counts: [u64; 7],

    /// Recent request timestamps for burst detection
    recent_requests: CircularBuffer<DateTime<Utc>>,

    /// Hourly request counts for trend analysis
    hourly_series: Vec<(f64, f64)>, // (hour_index, count)

    /// Moving average tracker
    moving_average: SimpleMovingAverage,

    /// Total requests processed
    total_requests: u64,

    /// Request counts by user ID
    user_requests: HashMap<String, u64>,

    /// Request counts by endpoint
    endpoint_requests: HashMap<String, u64>,

    /// Request counts by model
    model_requests: HashMap<String, u64>,
}

impl TemporalMetrics {
    fn new(window_size: usize, history_size: usize) -> Self {
        Self {
            hourly_counts: [0; 24],
            daily_counts: [0; 7],
            recent_requests: CircularBuffer::new(history_size),
            hourly_series: Vec::new(),
            moving_average: SimpleMovingAverage::new(window_size),
            total_requests: 0,
            user_requests: HashMap::new(),
            endpoint_requests: HashMap::new(),
            model_requests: HashMap::new(),
        }
    }

    fn record_request(&mut self, timestamp: DateTime<Utc>, metadata: &HashMap<String, String>) {
        // Record temporal patterns
        let hour = timestamp.hour() as usize;
        let day = timestamp.weekday().num_days_from_monday() as usize;

        self.hourly_counts[hour] += 1;
        self.daily_counts[day] += 1;
        self.recent_requests.push(timestamp);
        self.total_requests += 1;

        // Record by user
        if let Some(user_id) = metadata.get("user_id") {
            *self.user_requests.entry(user_id.clone()).or_insert(0) += 1;
        }

        // Record by endpoint
        if let Some(endpoint) = metadata.get("endpoint") {
            *self.endpoint_requests.entry(endpoint.clone()).or_insert(0) += 1;
        }

        // Update moving average (requests per minute)
        self.update_moving_average();
    }

    fn record_model(&mut self, model: &str) {
        *self.model_requests.entry(model.to_string()).or_insert(0) += 1;
    }

    fn update_moving_average(&mut self) {
        // Calculate requests in the last minute from recent_requests
        let now = Utc::now();
        let one_minute_ago = now - chrono::Duration::minutes(1);

        let recent_count = self
            .recent_requests
            .as_slice()
            .iter()
            .filter(|&&ts| ts > one_minute_ago)
            .count();

        self.moving_average.update(recent_count as f64);
    }

    fn get_peak_hours(&self, threshold_multiplier: f64) -> Vec<usize> {
        if self.total_requests < 24 {
            return Vec::new();
        }

        let avg = self.hourly_counts.iter().sum::<u64>() as f64 / 24.0;
        let threshold = avg * threshold_multiplier;

        self.hourly_counts
            .iter()
            .enumerate()
            .filter(|(_, &count)| count as f64 > threshold)
            .map(|(hour, _)| hour)
            .collect()
    }

    fn get_low_traffic_hours(&self, threshold_multiplier: f64) -> Vec<usize> {
        if self.total_requests < 24 {
            return Vec::new();
        }

        let avg = self.hourly_counts.iter().sum::<u64>() as f64 / 24.0;
        let threshold = avg / threshold_multiplier;

        self.hourly_counts
            .iter()
            .enumerate()
            .filter(|(_, &count)| count > 0 && (count as f64) < threshold)
            .map(|(hour, _)| hour)
            .collect()
    }

    fn detect_burst(&self, threshold_multiplier: f64) -> Option<f64> {
        if let Some(avg) = self.moving_average.value() {
            if avg == 0.0 {
                return None;
            }

            // Count requests in the last minute
            let now = Utc::now();
            let one_minute_ago = now - chrono::Duration::minutes(1);

            let recent_count = self
                .recent_requests
                .as_slice()
                .iter()
                .filter(|&&ts| ts > one_minute_ago)
                .count() as f64;

            if recent_count > avg * threshold_multiplier {
                return Some(recent_count / avg);
            }
        }

        None
    }

    fn calculate_growth_rate(&self) -> Option<f64> {
        // Use hourly series to calculate trend
        if self.hourly_series.len() < 2 {
            return None;
        }

        if let Some(trend) = calculate_linear_trend(&self.hourly_series) {
            // Convert slope to percentage growth
            if self.hourly_series.first().map(|(_, v)| *v).unwrap_or(0.0) > 0.0 {
                let baseline = self.hourly_series.first().unwrap().1;
                let growth_pct = (trend.slope / baseline) * 100.0;
                return Some(growth_pct);
            }
        }

        None
    }

    fn detect_weekday_pattern(&self) -> Option<f64> {
        if self.total_requests < 100 {
            return None;
        }

        // Compare weekday (Mon-Fri) vs weekend (Sat-Sun) traffic
        let weekday_total: u64 = self.daily_counts[0..5].iter().sum();
        let weekend_total: u64 = self.daily_counts[5..7].iter().sum();

        if weekend_total == 0 {
            return None;
        }

        let weekday_avg = weekday_total as f64 / 5.0;
        let weekend_avg = weekend_total as f64 / 2.0;

        if weekend_avg > 0.0 {
            let difference_pct = ((weekday_avg - weekend_avg) / weekend_avg) * 100.0;
            Some(difference_pct)
        } else {
            None
        }
    }

    fn get_top_users(&self, limit: usize) -> Vec<(String, u64)> {
        let mut users: Vec<_> = self.user_requests.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        users.sort_by(|a, b| b.1.cmp(&a.1));
        users.truncate(limit);
        users
    }

    fn get_top_endpoints(&self, limit: usize) -> Vec<(String, u64)> {
        let mut endpoints: Vec<_> = self.endpoint_requests.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        endpoints.sort_by(|a, b| b.1.cmp(&a.1));
        endpoints.truncate(limit);
        endpoints
    }
}

/// Pattern Analyzer implementation
pub struct PatternAnalyzer {
    config: PatternAnalyzerConfig,
    state: AnalyzerState,
    metrics: Arc<RwLock<TemporalMetrics>>,
    stats: Arc<RwLock<AnalyzerStats>>,
    start_time: Instant,
    last_report_time: Arc<RwLock<Instant>>,
}

impl PatternAnalyzer {
    /// Create a new Pattern Analyzer
    pub fn new(config: PatternAnalyzerConfig) -> Self {
        let window_size = config.moving_average_window;
        let history_size = 1000;

        Self {
            config,
            state: AnalyzerState::Initialized,
            metrics: Arc::new(RwLock::new(TemporalMetrics::new(window_size, history_size))),
            stats: Arc::new(RwLock::new(AnalyzerStats {
                analyzer: "pattern".to_string(),
                ..Default::default()
            })),
            start_time: Instant::now(),
            last_report_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PatternAnalyzerConfig::default())
    }

    /// Process a request event
    async fn process_request(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Request {
            timestamp,
            model,
            metadata,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_request(timestamp, &metadata);
            metrics.record_model(&model);

            trace!(
                "Recorded pattern metrics: timestamp={}, model={}",
                timestamp,
                model
            );
        }

        Ok(())
    }

    /// Process a response event
    async fn process_response(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Response {
            timestamp,
            model,
            metadata,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_request(timestamp, &metadata);
            metrics.record_model(&model);

            trace!(
                "Recorded pattern metrics from response: timestamp={}, model={}",
                timestamp,
                model
            );
        }

        Ok(())
    }

    /// Process a metric event
    async fn process_metric(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Metric {
            timestamp,
            labels,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_request(timestamp, &labels);
        }

        Ok(())
    }

    /// Analyze patterns and generate insights
    async fn analyze_patterns(&self) -> Vec<Insight> {
        let metrics = self.metrics.read().await;
        let mut insights = Vec::new();

        // Check if we have enough data
        if metrics.total_requests < self.config.min_requests_for_patterns as u64 {
            debug!(
                "Insufficient data for pattern analysis: {} requests (minimum: {})",
                metrics.total_requests, self.config.min_requests_for_patterns
            );
            return insights;
        }

        // Detect peak hours
        let peak_hours = metrics.get_peak_hours(1.5);
        if !peak_hours.is_empty() {
            insights.push(self.create_peak_hours_insight(&peak_hours, &metrics));
        }

        // Detect low traffic hours
        let low_traffic_hours = metrics.get_low_traffic_hours(2.0);
        if !low_traffic_hours.is_empty() {
            insights.push(self.create_low_traffic_insight(&low_traffic_hours, &metrics));
        }

        // Detect traffic bursts
        if let Some(burst_ratio) = metrics.detect_burst(self.config.burst_threshold_multiplier) {
            insights.push(self.create_burst_insight(burst_ratio));
        }

        // Detect growth trends
        if let Some(growth_rate) = metrics.calculate_growth_rate() {
            if growth_rate.abs() > self.config.growth_rate_threshold_pct {
                insights.push(self.create_growth_insight(growth_rate));
            }
        }

        // Detect weekday patterns
        if let Some(weekday_diff) = metrics.detect_weekday_pattern() {
            if weekday_diff.abs() > 30.0 {
                insights.push(self.create_seasonal_insight(weekday_diff));
            }
        }

        insights
    }

    /// Create insight for peak hours
    fn create_peak_hours_insight(&self, peak_hours: &[usize], metrics: &TemporalMetrics) -> Insight {
        let hours_str = if peak_hours.len() > 3 {
            format!(
                "{}:00-{}:00",
                peak_hours.first().unwrap(),
                peak_hours.last().unwrap()
            )
        } else {
            peak_hours
                .iter()
                .map(|h| format!("{}:00", h))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let peak_count: u64 = peak_hours.iter().map(|&h| metrics.hourly_counts[h]).sum();
        let avg_count = metrics.hourly_counts.iter().sum::<u64>() as f64 / 24.0;
        let peak_avg = peak_count as f64 / peak_hours.len() as f64;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "pattern".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Info,
            confidence: Confidence::new(0.90),
            title: format!("Peak traffic hours: {}", hours_str),
            description: format!(
                "Traffic peaks during {} with {:.0}% higher volume than average. Peak hours average {:.0} requests/hour vs overall average of {:.0} requests/hour.",
                hours_str,
                ((peak_avg - avg_count) / avg_count) * 100.0,
                peak_avg,
                avg_count
            ),
            category: InsightCategory::Pattern,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Pattern,
                description: format!("Hourly distribution analysis"),
                data: serde_json::json!({
                    "peak_hours": peak_hours,
                    "peak_avg_requests": peak_avg,
                    "overall_avg_requests": avg_count,
                    "increase_pct": ((peak_avg - avg_count) / avg_count) * 100.0
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("peak_avg_requests".to_string(), peak_avg);
                m.insert("overall_avg_requests".to_string(), avg_count);
                m
            },
            tags: vec!["temporal".to_string(), "peak_hours".to_string()],
        }
    }

    /// Create insight for low traffic periods
    fn create_low_traffic_insight(&self, low_hours: &[usize], metrics: &TemporalMetrics) -> Insight {
        let hours_str = if low_hours.len() > 3 {
            format!(
                "{}:00-{}:00",
                low_hours.first().unwrap(),
                low_hours.last().unwrap()
            )
        } else {
            low_hours
                .iter()
                .map(|h| format!("{}:00", h))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let low_count: u64 = low_hours.iter().map(|&h| metrics.hourly_counts[h]).sum();
        let low_avg = low_count as f64 / low_hours.len() as f64;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "pattern".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Info,
            confidence: Confidence::new(0.85),
            title: format!("Low traffic period: {}", hours_str),
            description: format!(
                "Traffic is significantly lower during {} with an average of {:.0} requests/hour. Consider cost optimization during these periods.",
                hours_str, low_avg
            ),
            category: InsightCategory::Pattern,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Pattern,
                description: "Low traffic period detected".to_string(),
                data: serde_json::json!({
                    "low_traffic_hours": low_hours,
                    "avg_requests": low_avg
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("low_traffic_avg_requests".to_string(), low_avg);
                m
            },
            tags: vec!["temporal".to_string(), "low_traffic".to_string()],
        }
    }

    /// Create insight for traffic bursts
    fn create_burst_insight(&self, burst_ratio: f64) -> Insight {
        let severity = if burst_ratio > 5.0 {
            Severity::Warning
        } else {
            Severity::Info
        };

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "pattern".to_string(),
            timestamp: Utc::now(),
            severity,
            confidence: Confidence::new(0.92),
            title: format!("Traffic burst detected: {:.1}x normal", burst_ratio),
            description: format!(
                "Current traffic is {:.1}x higher than the moving average. This sudden spike may indicate increased user activity, viral content, or potential issues.",
                burst_ratio
            ),
            category: InsightCategory::Anomaly,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Anomaly,
                description: "Burst detection via moving average".to_string(),
                data: serde_json::json!({
                    "burst_ratio": burst_ratio,
                    "threshold": self.config.burst_threshold_multiplier
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("burst_ratio".to_string(), burst_ratio);
                m
            },
            tags: vec!["burst".to_string(), "traffic".to_string(), "anomaly".to_string()],
        }
    }

    /// Create insight for growth trends
    fn create_growth_insight(&self, growth_rate: f64) -> Insight {
        let is_positive = growth_rate > 0.0;
        let severity = if growth_rate.abs() > 50.0 {
            Severity::Warning
        } else {
            Severity::Info
        };

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "pattern".to_string(),
            timestamp: Utc::now(),
            severity,
            confidence: Confidence::new(0.82),
            title: format!(
                "{} trend: {:.1}% growth",
                if is_positive { "Growth" } else { "Decline" },
                growth_rate.abs()
            ),
            description: format!(
                "Traffic shows a {} trend with {:.1}% {} over the analyzed period. {} capacity planning to accommodate this trend.",
                if is_positive { "positive growth" } else { "declining" },
                growth_rate.abs(),
                if is_positive { "increase" } else { "decrease" },
                if is_positive { "Consider" } else { "Review" }
            ),
            category: InsightCategory::Pattern,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::TimeSeries,
                description: "Linear trend analysis".to_string(),
                data: serde_json::json!({
                    "growth_rate_pct": growth_rate,
                    "trend_type": if is_positive { "growth" } else { "decline" }
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("growth_rate_pct".to_string(), growth_rate);
                m
            },
            tags: vec![
                "growth".to_string(),
                "trend".to_string(),
                if is_positive { "increasing".to_string() } else { "decreasing".to_string() }
            ],
        }
    }

    /// Create insight for seasonal patterns
    fn create_seasonal_insight(&self, weekday_diff: f64) -> Insight {
        let is_higher_weekday = weekday_diff > 0.0;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "pattern".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Info,
            confidence: Confidence::new(0.88),
            title: format!(
                "Weekly pattern: {:.0}% {} traffic on {}",
                weekday_diff.abs(),
                if is_higher_weekday { "higher" } else { "lower" },
                if is_higher_weekday { "weekdays" } else { "weekends" }
            ),
            description: format!(
                "Traffic shows a clear weekly pattern with {:.0}% {} volume on {} compared to {}. This seasonal pattern can be used for capacity planning and cost optimization.",
                weekday_diff.abs(),
                if is_higher_weekday { "higher" } else { "lower" },
                if is_higher_weekday { "weekdays (Mon-Fri)" } else { "weekends (Sat-Sun)" },
                if is_higher_weekday { "weekends" } else { "weekdays" }
            ),
            category: InsightCategory::Pattern,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Pattern,
                description: "Weekday vs weekend comparison".to_string(),
                data: serde_json::json!({
                    "weekday_weekend_diff_pct": weekday_diff,
                    "pattern_type": "weekly"
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("weekday_weekend_diff_pct".to_string(), weekday_diff.abs());
                m
            },
            tags: vec!["seasonal".to_string(), "weekly".to_string(), "pattern".to_string()],
        }
    }

    /// Generate recommendations based on insights
    fn generate_recommendations(&self, insights: &[Insight], metrics: &TemporalMetrics) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        for insight in insights {
            // Peak hours recommendations
            if insight.tags.contains(&"peak_hours".to_string()) {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "pattern".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Medium,
                    title: "Scale resources for peak hours".to_string(),
                    description: "Configure auto-scaling to handle peak traffic periods efficiently.".to_string(),
                    action: Action {
                        action_type: ActionType::Scale,
                        instructions: "1. Set up auto-scaling policies for peak hours\n2. Pre-warm resources before peak periods\n3. Configure rate limiting to protect during spikes".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(3.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "latency_during_peaks".to_string(),
                            current: 0.0,
                            expected: 0.0,
                            unit: "ms".to_string(),
                            improvement_pct: 25.0,
                        }),
                        cost: Some(ImpactMetric {
                            name: "infrastructure_cost".to_string(),
                            current: 100.0,
                            expected: 110.0,
                            unit: "usd".to_string(),
                            improvement_pct: -10.0,
                        }),
                        quality: None,
                        description: "Better performance during peaks with slightly higher cost".to_string(),
                    },
                    confidence: Confidence::new(0.85),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["scaling".to_string(), "capacity".to_string()],
                });
            }

            // Low traffic recommendations
            if insight.tags.contains(&"low_traffic".to_string()) {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "pattern".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Low,
                    title: "Optimize costs during low-traffic periods".to_string(),
                    description: "Scale down resources during consistently low-traffic hours to reduce costs.".to_string(),
                    action: Action {
                        action_type: ActionType::ConfigChange,
                        instructions: "1. Configure auto-scaling to reduce capacity during low-traffic hours\n2. Consider spot instances for non-critical workloads\n3. Schedule batch jobs during these periods".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(2.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: Some(ImpactMetric {
                            name: "infrastructure_cost".to_string(),
                            current: 100.0,
                            expected: 85.0,
                            unit: "usd".to_string(),
                            improvement_pct: 15.0,
                        }),
                        quality: None,
                        description: "15% cost savings with no performance impact".to_string(),
                    },
                    confidence: Confidence::new(0.80),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["cost_optimization".to_string(), "scaling".to_string()],
                });
            }

            // Burst recommendations
            if insight.tags.contains(&"burst".to_string()) {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "pattern".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::High,
                    title: "Implement burst handling strategies".to_string(),
                    description: "Deploy caching and rate limiting to handle traffic bursts efficiently.".to_string(),
                    action: Action {
                        action_type: ActionType::ConfigChange,
                        instructions: "1. Enable CDN caching for static responses\n2. Implement request coalescing for identical prompts\n3. Set up rate limiting per user/API key\n4. Configure burst capacity in load balancers".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(4.0),
                        risk_level: RiskLevel::Medium,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "burst_handling".to_string(),
                            current: 0.0,
                            expected: 0.0,
                            unit: "requests".to_string(),
                            improvement_pct: 50.0,
                        }),
                        cost: Some(ImpactMetric {
                            name: "api_cost".to_string(),
                            current: 100.0,
                            expected: 80.0,
                            unit: "usd".to_string(),
                            improvement_pct: 20.0,
                        }),
                        quality: None,
                        description: "Better burst handling with 20% cost reduction via caching".to_string(),
                    },
                    confidence: Confidence::new(0.82),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["caching".to_string(), "rate_limiting".to_string()],
                });
            }

            // Growth recommendations
            if insight.tags.contains(&"growth".to_string()) && insight.tags.contains(&"increasing".to_string()) {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "pattern".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::High,
                    title: "Plan for continued growth".to_string(),
                    description: "Proactively scale infrastructure to accommodate growth trend.".to_string(),
                    action: Action {
                        action_type: ActionType::Review,
                        instructions: "1. Review capacity projections for next 30-90 days\n2. Negotiate volume discounts with LLM providers\n3. Consider reserved capacity for predictable baseline\n4. Implement aggressive caching strategies".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(6.0),
                        risk_level: RiskLevel::Medium,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: Some(ImpactMetric {
                            name: "future_cost".to_string(),
                            current: 100.0,
                            expected: 120.0,
                            unit: "usd".to_string(),
                            improvement_pct: -20.0,
                        }),
                        quality: None,
                        description: "Ensure capacity for growth with optimized pricing".to_string(),
                    },
                    confidence: Confidence::new(0.75),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["capacity_planning".to_string(), "growth".to_string()],
                });
            }
        }

        // Add caching recommendation for common patterns
        let top_endpoints = metrics.get_top_endpoints(5);
        if !top_endpoints.is_empty() {
            let top_endpoint_pct = (top_endpoints.first().unwrap().1 as f64 / metrics.total_requests as f64) * 100.0;
            if top_endpoint_pct > 20.0 {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "pattern".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Medium,
                    title: "Cache responses for common endpoints".to_string(),
                    description: format!(
                        "Top endpoint accounts for {:.1}% of traffic. Implement semantic caching.",
                        top_endpoint_pct
                    ),
                    action: Action {
                        action_type: ActionType::Optimize,
                        instructions: "1. Implement semantic caching for similar prompts\n2. Cache responses with TTL based on content type\n3. Monitor cache hit rates and adjust strategies".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(5.0),
                        risk_level: RiskLevel::Low,
                    },
                    expected_impact: Impact {
                        performance: Some(ImpactMetric {
                            name: "latency".to_string(),
                            current: 1000.0,
                            expected: 100.0,
                            unit: "ms".to_string(),
                            improvement_pct: 90.0,
                        }),
                        cost: Some(ImpactMetric {
                            name: "api_cost".to_string(),
                            current: 100.0,
                            expected: 70.0,
                            unit: "usd".to_string(),
                            improvement_pct: 30.0,
                        }),
                        quality: None,
                        description: "90% latency reduction and 30% cost savings via caching".to_string(),
                    },
                    confidence: Confidence::new(0.88),
                    related_insights: vec![],
                    tags: vec!["caching".to_string(), "optimization".to_string()],
                });
            }
        }

        recommendations
    }
}

#[async_trait]
impl Analyzer for PatternAnalyzer {
    fn name(&self) -> &str {
        "pattern"
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
        info!("Starting Pattern Analyzer");

        // Initialization logic here

        self.state = AnalyzerState::Running;
        info!("Pattern Analyzer started successfully");

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
        info!("Stopping Pattern Analyzer");

        // Cleanup logic here

        self.state = AnalyzerState::Stopped;
        info!("Pattern Analyzer stopped");

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
            AnalyzerEvent::Request { .. } => self.process_request(event).await,
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

        let insights = self.analyze_patterns().await;
        let recommendations = self.generate_recommendations(&insights, &metrics);
        let alerts = Vec::new(); // Alerts would be generated based on thresholds

        let mut report_metrics = HashMap::new();
        report_metrics.insert("total_requests".to_string(), metrics.total_requests as f64);

        if let Some(ma) = metrics.moving_average.value() {
            report_metrics.insert("requests_per_minute".to_string(), ma);
        }

        // Add hourly distribution
        for (hour, count) in metrics.hourly_counts.iter().enumerate() {
            report_metrics.insert(format!("hour_{}", hour), *count as f64);
        }

        // Add daily distribution
        let days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
        for (day, count) in metrics.daily_counts.iter().enumerate() {
            report_metrics.insert(format!("day_{}", days[day]), *count as f64);
        }

        let report = AnalysisReport {
            analyzer: "pattern".to_string(),
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
                    format!("Total requests: {}", metrics.total_requests),
                    format!("Unique users: {}", metrics.user_requests.len()),
                    format!("Unique endpoints: {}", metrics.endpoint_requests.len()),
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
        AnalyzerStats {
            analyzer: "pattern".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            ..Default::default()
        }
    }

    async fn reset(&mut self) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = TemporalMetrics::new(
            self.config.moving_average_window,
            1000,
        );

        let mut stats = self.stats.write().await;
        *stats = AnalyzerStats {
            analyzer: "pattern".to_string(),
            ..Default::default()
        };

        info!("Pattern Analyzer reset");
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

    fn create_test_request_event(hour: u32, metadata: HashMap<String, String>) -> AnalyzerEvent {
        let timestamp = Utc::now()
            .date_naive()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
            .and_utc();

        AnalyzerEvent::Request {
            timestamp,
            request_id: Uuid::new_v4().to_string(),
            model: "gpt-4".to_string(),
            prompt_tokens: 100,
            max_tokens: 1000,
            temperature: 0.7,
            metadata,
        }
    }

    fn create_test_response_event(hour: u32) -> AnalyzerEvent {
        let timestamp = Utc::now()
            .date_naive()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
            .and_utc();

        AnalyzerEvent::Response {
            timestamp,
            request_id: Uuid::new_v4().to_string(),
            model: "gpt-4".to_string(),
            completion_tokens: 100,
            total_tokens: 200,
            latency_ms: 500,
            success: true,
            error: None,
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_analyzer_lifecycle() {
        let mut analyzer = PatternAnalyzer::with_defaults();

        assert_eq!(analyzer.state(), AnalyzerState::Initialized);

        analyzer.start().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Running);

        analyzer.stop().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Stopped);
    }

    #[tokio::test]
    async fn test_process_request_event() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_request_event(14, HashMap::new());
        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.hourly_counts[14], 1);
    }

    #[tokio::test]
    async fn test_peak_hours_detection() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Create traffic pattern with peak at 2-4 PM
        for _ in 0..20 {
            for hour in 0..24 {
                let count = if (14..=16).contains(&hour) { 10 } else { 2 };
                for _ in 0..count {
                    let event = create_test_response_event(hour);
                    analyzer.process_event(event).await.unwrap();
                }
            }
        }

        let insights = analyzer.analyze_patterns().await;
        assert!(!insights.is_empty());
        assert!(insights.iter().any(|i| i.title.contains("Peak traffic")));
    }

    #[tokio::test]
    async fn test_low_traffic_detection() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Create traffic pattern with low traffic at night
        for _ in 0..20 {
            for hour in 0..24 {
                let count = if (0..=5).contains(&hour) { 1 } else { 10 };
                for _ in 0..count {
                    let event = create_test_response_event(hour);
                    analyzer.process_event(event).await.unwrap();
                }
            }
        }

        let insights = analyzer.analyze_patterns().await;
        assert!(insights.iter().any(|i| i.title.contains("Low traffic")));
    }

    #[tokio::test]
    async fn test_burst_detection() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Establish baseline
        for _ in 0..100 {
            let event = create_test_response_event(12);
            analyzer.process_event(event).await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Create burst
        for _ in 0..500 {
            let event = create_test_response_event(12);
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_patterns().await;
        assert!(insights.iter().any(|i| i.title.contains("burst")));
    }

    #[tokio::test]
    async fn test_seasonal_pattern_detection() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Create weekday/weekend pattern
        let mut metrics = analyzer.metrics.write().await;

        // Weekdays (Mon-Fri): high traffic
        for day in 0..5 {
            metrics.daily_counts[day] = 100;
        }

        // Weekends (Sat-Sun): low traffic
        metrics.daily_counts[5] = 30;
        metrics.daily_counts[6] = 30;

        metrics.total_requests = 560;
        drop(metrics);

        let insights = analyzer.analyze_patterns().await;
        assert!(insights.iter().any(|i| i.title.contains("Weekly pattern")));
    }

    #[tokio::test]
    async fn test_user_tracking() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("user_id".to_string(), "user123".to_string());

        for _ in 0..10 {
            let event = create_test_request_event(12, metadata.clone());
            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.user_requests.get("user123"), Some(&10));
    }

    #[tokio::test]
    async fn test_endpoint_tracking() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("endpoint".to_string(), "/api/chat".to_string());

        for _ in 0..15 {
            let event = create_test_request_event(12, metadata.clone());
            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.endpoint_requests.get("/api/chat"), Some(&15));
    }

    #[tokio::test]
    async fn test_generate_report() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Add test events
        for hour in 0..24 {
            for _ in 0..5 {
                let event = create_test_response_event(hour);
                analyzer.process_event(event).await.unwrap();
            }
        }

        let report = analyzer.generate_report().await.unwrap();
        assert_eq!(report.analyzer, "pattern");
        assert!(report.summary.events_processed > 0);
        assert!(!report.metrics.is_empty());
    }

    #[tokio::test]
    async fn test_reset() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event(12);
        analyzer.process_event(event).await.unwrap();

        analyzer.reset().await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.total_requests, 0);
        assert_eq!(metrics.hourly_counts.iter().sum::<u64>(), 0);
    }

    #[tokio::test]
    async fn test_recommendations_generation() {
        let mut analyzer = PatternAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Create pattern with peak hours
        for _ in 0..20 {
            for hour in 0..24 {
                let count = if (14..=16).contains(&hour) { 10 } else { 2 };
                for _ in 0..count {
                    let event = create_test_response_event(hour);
                    analyzer.process_event(event).await.unwrap();
                }
            }
        }

        let report = analyzer.generate_report().await.unwrap();
        assert!(!report.recommendations.is_empty());
        assert!(report.recommendations.iter().any(|r| r.title.contains("Scale")));
    }
}
