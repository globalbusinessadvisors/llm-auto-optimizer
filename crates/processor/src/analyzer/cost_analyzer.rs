//! Cost Analyzer
//!
//! Analyzes cost metrics including token costs by model, budget utilization,
//! cost per request, and detects cost anomalies and optimization opportunities.

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use super::stats::{
    calculate_percentiles, is_outlier_zscore, mean, percentage_change, std_dev, CircularBuffer,
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

/// Model pricing information (per 1K tokens)
#[derive(Debug, Clone)]
pub struct ModelPricing {
    pub prompt_cost_per_1k: f64,
    pub completion_cost_per_1k: f64,
}

impl ModelPricing {
    /// Get pricing for a model
    pub fn for_model(model: &str) -> Self {
        let model_lower = model.to_lowercase();

        if model_lower.contains("gpt-4") {
            Self {
                prompt_cost_per_1k: 0.03,
                completion_cost_per_1k: 0.06,
            }
        } else if model_lower.contains("gpt-3.5") {
            Self {
                prompt_cost_per_1k: 0.001,
                completion_cost_per_1k: 0.002,
            }
        } else if model_lower.contains("claude-3-opus") || model_lower.contains("claude-opus") {
            Self {
                prompt_cost_per_1k: 0.015,
                completion_cost_per_1k: 0.075,
            }
        } else if model_lower.contains("claude-3-sonnet") || model_lower.contains("claude-sonnet") {
            Self {
                prompt_cost_per_1k: 0.003,
                completion_cost_per_1k: 0.015,
            }
        } else {
            // Default to GPT-3.5 pricing for unknown models
            Self {
                prompt_cost_per_1k: 0.001,
                completion_cost_per_1k: 0.002,
            }
        }
    }

    /// Calculate cost for given token counts
    pub fn calculate_cost(&self, prompt_tokens: u32, completion_tokens: u32) -> f64 {
        let prompt_cost = (prompt_tokens as f64 / 1000.0) * self.prompt_cost_per_1k;
        let completion_cost = (completion_tokens as f64 / 1000.0) * self.completion_cost_per_1k;
        prompt_cost + completion_cost
    }
}

/// Configuration specific to the Cost Analyzer
#[derive(Debug, Clone)]
pub struct CostAnalyzerConfig {
    /// Base analyzer configuration
    pub base: AnalyzerConfig,

    /// Daily budget limit in USD
    pub daily_budget_usd: f64,

    /// Monthly budget limit in USD
    pub monthly_budget_usd: f64,

    /// Budget warning threshold (percentage)
    pub budget_warning_threshold_pct: f64,

    /// Budget critical threshold (percentage)
    pub budget_critical_threshold_pct: f64,

    /// Cost spike detection threshold (percentage increase)
    pub cost_spike_threshold_pct: f64,

    /// Window size for cost tracking
    pub cost_window_size: usize,

    /// Enable cost optimization recommendations
    pub enable_optimization_recommendations: bool,

    /// Minimum cost difference to recommend model switching (USD)
    pub min_cost_difference_for_recommendation: f64,
}

impl Default for CostAnalyzerConfig {
    fn default() -> Self {
        Self {
            base: AnalyzerConfig::default(),
            daily_budget_usd: 100.0,
            monthly_budget_usd: 3000.0,
            budget_warning_threshold_pct: 80.0,
            budget_critical_threshold_pct: 90.0,
            cost_spike_threshold_pct: 50.0,
            cost_window_size: 1000,
            enable_optimization_recommendations: true,
            min_cost_difference_for_recommendation: 0.01,
        }
    }
}

/// Per-model cost tracking
#[derive(Debug, Clone)]
struct ModelCostMetrics {
    total_cost: f64,
    request_count: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    cost_samples: CircularBuffer<f64>,
}

impl ModelCostMetrics {
    fn new(window_size: usize) -> Self {
        Self {
            total_cost: 0.0,
            request_count: 0,
            prompt_tokens: 0,
            completion_tokens: 0,
            cost_samples: CircularBuffer::new(window_size),
        }
    }

    fn record_request(&mut self, cost: f64, prompt_tokens: u32, completion_tokens: u32) {
        self.total_cost += cost;
        self.request_count += 1;
        self.prompt_tokens += prompt_tokens as u64;
        self.completion_tokens += completion_tokens as u64;
        self.cost_samples.push(cost);
    }

    fn average_cost_per_request(&self) -> f64 {
        if self.request_count == 0 {
            return 0.0;
        }
        self.total_cost / self.request_count as f64
    }
}

/// Cost metrics tracked over time
#[derive(Debug, Clone)]
struct CostMetrics {
    /// Per-model costs
    model_costs: HashMap<String, ModelCostMetrics>,

    /// Total cost across all models
    total_cost: f64,

    /// Daily costs (reset each day)
    daily_cost: f64,
    daily_reset_time: DateTime<Utc>,

    /// Monthly costs (reset each month)
    monthly_cost: f64,
    monthly_reset_time: DateTime<Utc>,

    /// Cost per request samples
    cost_per_request: CircularBuffer<f64>,

    /// Request timestamps for trend analysis
    request_times: CircularBuffer<DateTime<Utc>>,

    /// Cost trend tracker
    cost_ema: ExponentialMovingAverage,

    /// Historical baseline for anomaly detection
    baseline_daily_cost: Option<f64>,
    baseline_cost_per_request: Option<f64>,
}

impl CostMetrics {
    fn new(window_size: usize) -> Self {
        let now = Utc::now();
        Self {
            model_costs: HashMap::new(),
            total_cost: 0.0,
            daily_cost: 0.0,
            daily_reset_time: now,
            monthly_cost: 0.0,
            monthly_reset_time: now,
            cost_per_request: CircularBuffer::new(window_size),
            request_times: CircularBuffer::new(window_size),
            cost_ema: ExponentialMovingAverage::new(0.2),
            baseline_daily_cost: None,
            baseline_cost_per_request: None,
        }
    }

    fn check_and_reset_periods(&mut self) {
        let now = Utc::now();

        // Check daily reset
        if now.date_naive() != self.daily_reset_time.date_naive() {
            self.baseline_daily_cost = Some(self.daily_cost);
            self.daily_cost = 0.0;
            self.daily_reset_time = now;
        }

        // Check monthly reset
        if now.date_naive().month() != self.monthly_reset_time.date_naive().month()
            || now.date_naive().year() != self.monthly_reset_time.date_naive().year() {
            self.monthly_cost = 0.0;
            self.monthly_reset_time = now;
        }
    }

    fn record_cost(
        &mut self,
        model: &str,
        cost: f64,
        prompt_tokens: u32,
        completion_tokens: u32,
        timestamp: DateTime<Utc>,
    ) {
        self.check_and_reset_periods();

        // Update totals
        self.total_cost += cost;
        self.daily_cost += cost;
        self.monthly_cost += cost;
        self.cost_per_request.push(cost);
        self.request_times.push(timestamp);
        self.cost_ema.update(cost);

        // Update per-model metrics
        let model_metrics = self.model_costs
            .entry(model.to_string())
            .or_insert_with(|| ModelCostMetrics::new(100));
        model_metrics.record_request(cost, prompt_tokens, completion_tokens);
    }

    fn daily_budget_utilization(&self, daily_budget: f64) -> f64 {
        if daily_budget == 0.0 {
            return 0.0;
        }
        (self.daily_cost / daily_budget) * 100.0
    }

    fn monthly_budget_utilization(&self, monthly_budget: f64) -> f64 {
        if monthly_budget == 0.0 {
            return 0.0;
        }
        (self.monthly_cost / monthly_budget) * 100.0
    }

    fn average_cost_per_request(&self) -> f64 {
        let costs: Vec<f64> = self.cost_per_request.to_vec();
        if costs.is_empty() {
            return 0.0;
        }
        mean(&costs)
    }

    fn is_cost_trending_up(&self) -> bool {
        if let Some(ema) = self.cost_ema.value() {
            let avg = self.average_cost_per_request();
            if avg > 0.0 {
                return ema > avg * 1.1; // EMA is 10% higher than average
            }
        }
        false
    }
}

/// Cost Analyzer implementation
pub struct CostAnalyzer {
    config: CostAnalyzerConfig,
    state: AnalyzerState,
    metrics: Arc<RwLock<CostMetrics>>,
    stats: Arc<RwLock<AnalyzerStats>>,
    start_time: Instant,
    last_report_time: Arc<RwLock<Instant>>,
}

impl CostAnalyzer {
    /// Create a new Cost Analyzer
    pub fn new(config: CostAnalyzerConfig) -> Self {
        let window_size = config.cost_window_size;

        Self {
            config,
            state: AnalyzerState::Initialized,
            metrics: Arc::new(RwLock::new(CostMetrics::new(window_size))),
            stats: Arc::new(RwLock::new(AnalyzerStats {
                analyzer: "cost".to_string(),
                ..Default::default()
            })),
            start_time: Instant::now(),
            last_report_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CostAnalyzerConfig::default())
    }

    /// Process a request event to calculate cost
    async fn process_request(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Request {
            timestamp,
            model,
            prompt_tokens,
            ..
        } = event
        {
            // Store for later matching with response
            trace!(
                "Processed request: model={}, prompt_tokens={}",
                model,
                prompt_tokens
            );
        }

        Ok(())
    }

    /// Process a response event to calculate cost
    async fn process_response(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Response {
            timestamp,
            model,
            completion_tokens,
            total_tokens,
            success,
            ..
        } = event
        {
            if !success {
                return Ok(()); // Don't charge for failed requests
            }

            let prompt_tokens = total_tokens.saturating_sub(completion_tokens);
            let pricing = ModelPricing::for_model(&model);
            let cost = pricing.calculate_cost(prompt_tokens, completion_tokens);

            let mut metrics = self.metrics.write().await;
            metrics.record_cost(&model, cost, prompt_tokens, completion_tokens, timestamp);

            trace!(
                "Recorded cost: model={}, cost=${:.4}, prompt_tokens={}, completion_tokens={}",
                model,
                cost,
                prompt_tokens,
                completion_tokens
            );
        }

        Ok(())
    }

    /// Process a cost event
    async fn process_cost(&mut self, event: AnalyzerEvent) -> AnalyzerResult<()> {
        if let AnalyzerEvent::Cost {
            timestamp,
            model,
            tokens,
            cost_usd,
            ..
        } = event
        {
            let mut metrics = self.metrics.write().await;
            metrics.record_cost(&model, cost_usd, tokens, 0, timestamp);

            trace!(
                "Recorded direct cost: model={}, cost=${:.4}",
                model,
                cost_usd
            );
        }

        Ok(())
    }

    /// Analyze current metrics and generate insights
    async fn analyze_metrics(&self) -> Vec<Insight> {
        let metrics = self.metrics.read().await;
        let mut insights = Vec::new();

        // Check budget utilization
        let daily_utilization = metrics.daily_budget_utilization(self.config.daily_budget_usd);
        if daily_utilization >= self.config.budget_critical_threshold_pct {
            insights.push(self.create_budget_alert_insight(
                daily_utilization,
                self.config.daily_budget_usd,
                metrics.daily_cost,
                "daily",
                Severity::Critical,
            ));
        } else if daily_utilization >= self.config.budget_warning_threshold_pct {
            insights.push(self.create_budget_alert_insight(
                daily_utilization,
                self.config.daily_budget_usd,
                metrics.daily_cost,
                "daily",
                Severity::Warning,
            ));
        }

        let monthly_utilization = metrics.monthly_budget_utilization(self.config.monthly_budget_usd);
        if monthly_utilization >= self.config.budget_critical_threshold_pct {
            insights.push(self.create_budget_alert_insight(
                monthly_utilization,
                self.config.monthly_budget_usd,
                metrics.monthly_cost,
                "monthly",
                Severity::Critical,
            ));
        } else if monthly_utilization >= self.config.budget_warning_threshold_pct {
            insights.push(self.create_budget_alert_insight(
                monthly_utilization,
                self.config.monthly_budget_usd,
                metrics.monthly_cost,
                "monthly",
                Severity::Warning,
            ));
        }

        // Check for cost spikes
        if let Some(baseline) = metrics.baseline_cost_per_request {
            let current = metrics.average_cost_per_request();
            if current > 0.0 {
                let change_pct = percentage_change(baseline, current);
                if change_pct > self.config.cost_spike_threshold_pct {
                    insights.push(self.create_cost_spike_insight(baseline, current, change_pct));
                }
            }
        }

        // Check for cost trends
        if metrics.is_cost_trending_up() {
            insights.push(self.create_cost_trend_insight(&metrics));
        }

        // Detect anomalies in per-request costs
        let cost_samples: Vec<f64> = metrics.cost_per_request.to_vec();
        if cost_samples.len() >= 10 {
            let avg = mean(&cost_samples);
            let std = std_dev(&cost_samples);

            // Check recent costs for outliers
            let recent_costs: Vec<f64> = cost_samples.iter().rev().take(5).copied().collect();
            for cost in recent_costs {
                if is_outlier_zscore(cost, avg, std, 3.0) {
                    insights.push(self.create_cost_anomaly_insight(cost, avg, std));
                    break; // Only report one anomaly per analysis
                }
            }
        }

        insights
    }

    /// Create insight for budget threshold violations
    fn create_budget_alert_insight(
        &self,
        utilization_pct: f64,
        budget: f64,
        current_cost: f64,
        period: &str,
        severity: Severity,
    ) -> Insight {
        let remaining = budget - current_cost;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "cost".to_string(),
            timestamp: Utc::now(),
            severity,
            confidence: Confidence::new(0.99),
            title: format!("{} Budget at {:.1}%",
                period.chars().next().unwrap().to_uppercase().to_string() + &period[1..],
                utilization_pct
            ),
            description: format!(
                "Current {} cost is ${:.2}, which is {:.1}% of the ${:.2} budget. Remaining budget: ${:.2}. {}",
                period,
                current_cost,
                utilization_pct,
                budget,
                remaining,
                if severity == Severity::Critical {
                    "Immediate action required to prevent budget overrun."
                } else {
                    "Monitor closely to avoid exceeding budget."
                }
            ),
            category: InsightCategory::Cost,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Threshold,
                description: format!("Budget utilization: {:.1}%", utilization_pct),
                data: serde_json::json!({
                    "period": period,
                    "current_cost": current_cost,
                    "budget": budget,
                    "utilization_pct": utilization_pct,
                    "remaining": remaining
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert(format!("{}_cost_usd", period), current_cost);
                m.insert(format!("{}_budget_usd", period), budget);
                m.insert(format!("{}_utilization_pct", period), utilization_pct);
                m.insert(format!("{}_remaining_usd", period), remaining);
                m
            },
            tags: vec!["budget".to_string(), "cost".to_string(), period.to_string()],
        }
    }

    /// Create insight for cost spikes
    fn create_cost_spike_insight(
        &self,
        baseline: f64,
        current: f64,
        change_pct: f64,
    ) -> Insight {
        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "cost".to_string(),
            timestamp: Utc::now(),
            severity: if change_pct > 100.0 {
                Severity::Critical
            } else {
                Severity::Warning
            },
            confidence: Confidence::new(0.85),
            title: format!("Cost Spike Detected: +{:.1}%", change_pct),
            description: format!(
                "Average cost per request has increased by {:.1}% from baseline ${:.4} to current ${:.4}. This unexpected spike may indicate inefficient usage or increased model complexity.",
                change_pct, baseline, current
            ),
            category: InsightCategory::Anomaly,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Comparison,
                description: "Baseline vs current cost comparison".to_string(),
                data: serde_json::json!({
                    "baseline_cost": baseline,
                    "current_cost": current,
                    "change_pct": change_pct
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("baseline_cost_per_request".to_string(), baseline);
                m.insert("current_cost_per_request".to_string(), current);
                m.insert("cost_increase_pct".to_string(), change_pct);
                m
            },
            tags: vec!["spike".to_string(), "anomaly".to_string(), "cost".to_string()],
        }
    }

    /// Create insight for cost trends
    fn create_cost_trend_insight(&self, metrics: &CostMetrics) -> Insight {
        let avg = metrics.average_cost_per_request();
        let ema = metrics.cost_ema.value().unwrap_or(avg);
        let trend_increase = ((ema - avg) / avg) * 100.0;

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "cost".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Info,
            confidence: Confidence::new(0.75),
            title: "Increasing Cost Trend Detected".to_string(),
            description: format!(
                "Costs are trending upward. Recent requests are averaging {:.1}% higher than the baseline. Consider reviewing usage patterns and model selection.",
                trend_increase
            ),
            category: InsightCategory::Pattern,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::TimeSeries,
                description: "Cost trend analysis".to_string(),
                data: serde_json::json!({
                    "average_cost": avg,
                    "recent_trend": ema,
                    "trend_increase_pct": trend_increase
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("average_cost_per_request".to_string(), avg);
                m.insert("trend_cost_per_request".to_string(), ema);
                m.insert("trend_increase_pct".to_string(), trend_increase);
                m
            },
            tags: vec!["trend".to_string(), "cost".to_string()],
        }
    }

    /// Create insight for cost anomalies
    fn create_cost_anomaly_insight(&self, cost: f64, mean: f64, std_dev: f64) -> Insight {
        let z_score = super::stats::z_score(cost, mean, std_dev);

        Insight {
            id: Uuid::new_v4().to_string(),
            analyzer: "cost".to_string(),
            timestamp: Utc::now(),
            severity: Severity::Warning,
            confidence: Confidence::new(0.90),
            title: format!("Unusual Cost Detected: ${:.4}", cost),
            description: format!(
                "A request with cost ${:.4} is significantly higher than the average ${:.4} (z-score: {:.2}). This may indicate an unusually complex request or inefficient prompt design.",
                cost, mean, z_score
            ),
            category: InsightCategory::Anomaly,
            evidence: vec![Evidence {
                evidence_type: EvidenceType::Anomaly,
                description: format!("Cost outlier detected (z-score: {:.2})", z_score),
                data: serde_json::json!({
                    "cost": cost,
                    "mean": mean,
                    "std_dev": std_dev,
                    "z_score": z_score
                }),
                timestamp: Utc::now(),
            }],
            metrics: {
                let mut m = HashMap::new();
                m.insert("anomalous_cost".to_string(), cost);
                m.insert("mean_cost".to_string(), mean);
                m.insert("z_score".to_string(), z_score);
                m
            },
            tags: vec!["anomaly".to_string(), "outlier".to_string(), "cost".to_string()],
        }
    }

    /// Generate recommendations based on insights
    fn generate_recommendations(&self, insights: &[Insight], metrics: &CostMetrics) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        // Budget recommendations
        for insight in insights {
            if insight.title.contains("Budget") && insight.severity.is_urgent() {
                recommendations.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    analyzer: "cost".to_string(),
                    timestamp: Utc::now(),
                    priority: Priority::Urgent,
                    title: "Reduce usage to stay within budget".to_string(),
                    description: "Budget is critically low. Implement rate limiting or pause non-essential requests.".to_string(),
                    action: Action {
                        action_type: ActionType::ConfigChange,
                        instructions: "1. Enable rate limiting to control request volume\n2. Pause or defer non-critical batch jobs\n3. Review and increase budget if justified".to_string(),
                        parameters: HashMap::new(),
                        estimated_effort: Some(1.0),
                        risk_level: RiskLevel::Medium,
                    },
                    expected_impact: Impact {
                        performance: None,
                        cost: Some(ImpactMetric {
                            name: "daily_cost".to_string(),
                            current: insight.metrics.get("daily_cost_usd").copied().unwrap_or(0.0),
                            expected: insight.metrics.get("daily_budget_usd").copied().unwrap_or(0.0) * 0.9,
                            unit: "USD".to_string(),
                            improvement_pct: 10.0,
                        }),
                        quality: None,
                        description: "Prevent budget overrun".to_string(),
                    },
                    confidence: Confidence::new(0.95),
                    related_insights: vec![insight.id.clone()],
                    tags: vec!["budget".to_string(), "urgent".to_string()],
                });
            }
        }

        // Model optimization recommendations
        if self.config.enable_optimization_recommendations {
            recommendations.extend(self.generate_model_optimization_recommendations(metrics));
        }

        // Caching recommendations
        if metrics.model_costs.values().any(|m| m.request_count > 100) {
            recommendations.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                analyzer: "cost".to_string(),
                timestamp: Utc::now(),
                priority: Priority::Medium,
                title: "Implement caching to reduce costs".to_string(),
                description: "High request volume detected. Implementing response caching for common queries can significantly reduce costs.".to_string(),
                action: Action {
                    action_type: ActionType::Optimize,
                    instructions: "1. Implement semantic caching for similar prompts\n2. Cache responses for frequently requested queries\n3. Set appropriate TTL based on data freshness requirements".to_string(),
                    parameters: {
                        let mut p = HashMap::new();
                        p.insert("cache_ttl".to_string(), serde_json::json!(3600));
                        p.insert("similarity_threshold".to_string(), serde_json::json!(0.95));
                        p
                    },
                    estimated_effort: Some(8.0),
                    risk_level: RiskLevel::Low,
                },
                expected_impact: Impact {
                    performance: Some(ImpactMetric {
                        name: "latency".to_string(),
                        current: 1000.0,
                        expected: 50.0,
                        unit: "ms".to_string(),
                        improvement_pct: 95.0,
                    }),
                    cost: Some(ImpactMetric {
                        name: "daily_cost".to_string(),
                        current: metrics.daily_cost,
                        expected: metrics.daily_cost * 0.7,
                        unit: "USD".to_string(),
                        improvement_pct: 30.0,
                    }),
                    quality: None,
                    description: "30% cost reduction with 95% faster responses for cached queries".to_string(),
                },
                confidence: Confidence::new(0.80),
                related_insights: vec![],
                tags: vec!["optimization".to_string(), "caching".to_string(), "cost".to_string()],
            });
        }

        recommendations
    }

    /// Generate model optimization recommendations
    fn generate_model_optimization_recommendations(&self, metrics: &CostMetrics) -> Vec<Recommendation> {
        let mut recommendations = Vec::new();

        // Find the most expensive model
        if let Some((expensive_model, expensive_metrics)) = metrics.model_costs
            .iter()
            .max_by(|a, b| a.1.average_cost_per_request().partial_cmp(&b.1.average_cost_per_request()).unwrap())
        {
            let expensive_avg = expensive_metrics.average_cost_per_request();

            // Check if there's a significantly cheaper alternative
            for (cheaper_model, cheaper_metrics) in &metrics.model_costs {
                if cheaper_model == expensive_model {
                    continue;
                }

                let cheaper_avg = cheaper_metrics.average_cost_per_request();
                let cost_diff = expensive_avg - cheaper_avg;

                if cost_diff > self.config.min_cost_difference_for_recommendation {
                    let savings_pct = (cost_diff / expensive_avg) * 100.0;
                    let potential_savings = cost_diff * expensive_metrics.request_count as f64;

                    recommendations.push(Recommendation {
                        id: Uuid::new_v4().to_string(),
                        analyzer: "cost".to_string(),
                        timestamp: Utc::now(),
                        priority: if savings_pct > 50.0 { Priority::High } else { Priority::Medium },
                        title: format!("Consider switching from {} to {}", expensive_model, cheaper_model),
                        description: format!(
                            "Switching from {} to {} could save ${:.4} per request ({:.1}% reduction). Potential total savings: ${:.2}",
                            expensive_model, cheaper_model, cost_diff, savings_pct, potential_savings
                        ),
                        action: Action {
                            action_type: ActionType::Review,
                            instructions: format!(
                                "1. Evaluate if {} can handle your use case\n2. Run quality tests comparing {} vs {}\n3. Implement gradual rollout if quality is acceptable\n4. Monitor quality metrics closely",
                                cheaper_model, expensive_model, cheaper_model
                            ),
                            parameters: {
                                let mut p = HashMap::new();
                                p.insert("current_model".to_string(), serde_json::json!(expensive_model));
                                p.insert("recommended_model".to_string(), serde_json::json!(cheaper_model));
                                p.insert("cost_savings_per_request".to_string(), serde_json::json!(cost_diff));
                                p
                            },
                            estimated_effort: Some(16.0),
                            risk_level: RiskLevel::Medium,
                        },
                        expected_impact: Impact {
                            performance: None,
                            cost: Some(ImpactMetric {
                                name: "cost_per_request".to_string(),
                                current: expensive_avg,
                                expected: cheaper_avg,
                                unit: "USD".to_string(),
                                improvement_pct: savings_pct,
                            }),
                            quality: None,
                            description: format!("{:.1}% cost reduction (quality validation required)", savings_pct),
                        },
                        confidence: Confidence::new(0.70),
                        related_insights: vec![],
                        tags: vec!["optimization".to_string(), "model-switch".to_string(), "cost".to_string()],
                    });
                    break; // Only recommend one switch
                }
            }
        }

        recommendations
    }
}

#[async_trait]
impl Analyzer for CostAnalyzer {
    fn name(&self) -> &str {
        "cost"
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
        info!("Starting Cost Analyzer");

        // Initialization logic here

        self.state = AnalyzerState::Running;
        info!("Cost Analyzer started successfully");

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
        info!("Stopping Cost Analyzer");

        // Cleanup logic here

        self.state = AnalyzerState::Stopped;
        info!("Cost Analyzer stopped");

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
            AnalyzerEvent::Cost { .. } => self.process_cost(event).await,
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
        let recommendations = self.generate_recommendations(&insights, &metrics);
        let alerts = Vec::new();

        let mut report_metrics = HashMap::new();
        report_metrics.insert("total_cost_usd".to_string(), metrics.total_cost);
        report_metrics.insert("daily_cost_usd".to_string(), metrics.daily_cost);
        report_metrics.insert("monthly_cost_usd".to_string(), metrics.monthly_cost);
        report_metrics.insert("average_cost_per_request".to_string(), metrics.average_cost_per_request());
        report_metrics.insert("daily_budget_utilization_pct".to_string(),
            metrics.daily_budget_utilization(self.config.daily_budget_usd));
        report_metrics.insert("monthly_budget_utilization_pct".to_string(),
            metrics.monthly_budget_utilization(self.config.monthly_budget_usd));

        // Per-model costs
        for (model, model_metrics) in &metrics.model_costs {
            report_metrics.insert(
                format!("model_{}_total_cost", model.replace('-', "_")),
                model_metrics.total_cost,
            );
            report_metrics.insert(
                format!("model_{}_requests", model.replace('-', "_")),
                model_metrics.request_count as f64,
            );
        }

        let report = AnalysisReport {
            analyzer: "cost".to_string(),
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
                    format!("Total cost: ${:.2}", metrics.total_cost),
                    format!("Daily cost: ${:.2} ({:.1}% of budget)",
                        metrics.daily_cost,
                        metrics.daily_budget_utilization(self.config.daily_budget_usd)),
                    format!("Monthly cost: ${:.2} ({:.1}% of budget)",
                        metrics.monthly_cost,
                        metrics.monthly_budget_utilization(self.config.monthly_budget_usd)),
                    format!("Avg cost/request: ${:.4}", metrics.average_cost_per_request()),
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
            analyzer: "cost".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            ..Default::default()
        }
    }

    async fn reset(&mut self) -> AnalyzerResult<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = CostMetrics::new(self.config.cost_window_size);

        let mut stats = self.stats.write().await;
        *stats = AnalyzerStats {
            analyzer: "cost".to_string(),
            ..Default::default()
        };

        info!("Cost Analyzer reset");
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

    fn create_test_response_event(
        model: &str,
        prompt_tokens: u32,
        completion_tokens: u32,
        success: bool,
    ) -> AnalyzerEvent {
        AnalyzerEvent::Response {
            timestamp: Utc::now(),
            request_id: Uuid::new_v4().to_string(),
            model: model.to_string(),
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
            latency_ms: 500,
            success,
            error: if success { None } else { Some("Test error".to_string()) },
            metadata: HashMap::new(),
        }
    }

    fn create_test_cost_event(model: &str, tokens: u32, cost: f64) -> AnalyzerEvent {
        AnalyzerEvent::Cost {
            timestamp: Utc::now(),
            model: model.to_string(),
            tokens,
            cost_usd: cost,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_model_pricing_gpt4() {
        let pricing = ModelPricing::for_model("gpt-4");
        assert_eq!(pricing.prompt_cost_per_1k, 0.03);
        assert_eq!(pricing.completion_cost_per_1k, 0.06);

        let cost = pricing.calculate_cost(1000, 500);
        assert!((cost - 0.06).abs() < 0.001); // 1000 * 0.03/1000 + 500 * 0.06/1000 = 0.06
    }

    #[test]
    fn test_model_pricing_gpt35() {
        let pricing = ModelPricing::for_model("gpt-3.5-turbo");
        assert_eq!(pricing.prompt_cost_per_1k, 0.001);
        assert_eq!(pricing.completion_cost_per_1k, 0.002);

        let cost = pricing.calculate_cost(1000, 1000);
        assert!((cost - 0.003).abs() < 0.0001);
    }

    #[test]
    fn test_model_pricing_claude_opus() {
        let pricing = ModelPricing::for_model("claude-3-opus");
        assert_eq!(pricing.prompt_cost_per_1k, 0.015);
        assert_eq!(pricing.completion_cost_per_1k, 0.075);
    }

    #[test]
    fn test_model_pricing_claude_sonnet() {
        let pricing = ModelPricing::for_model("claude-3-sonnet");
        assert_eq!(pricing.prompt_cost_per_1k, 0.003);
        assert_eq!(pricing.completion_cost_per_1k, 0.015);
    }

    #[tokio::test]
    async fn test_analyzer_lifecycle() {
        let mut analyzer = CostAnalyzer::with_defaults();

        assert_eq!(analyzer.state(), AnalyzerState::Initialized);

        analyzer.start().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Running);

        analyzer.stop().await.unwrap();
        assert_eq!(analyzer.state(), AnalyzerState::Stopped);
    }

    #[tokio::test]
    async fn test_process_response_event() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event("gpt-4", 1000, 500, true);
        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert!(metrics.total_cost > 0.0);
        assert_eq!(metrics.model_costs.len(), 1);
        assert!(metrics.model_costs.contains_key("gpt-4"));
    }

    #[tokio::test]
    async fn test_cost_calculation_accuracy() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_response_event("gpt-4", 1000, 500, true);
        analyzer.process_event(event).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        let expected_cost = 0.06; // 1000 * 0.03/1000 + 500 * 0.06/1000
        assert!((metrics.total_cost - expected_cost).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_budget_utilization_tracking() {
        let config = CostAnalyzerConfig {
            daily_budget_usd: 1.0,
            monthly_budget_usd: 30.0,
            ..Default::default()
        };
        let mut analyzer = CostAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Add events totaling $0.5
        for _ in 0..5 {
            let event = create_test_cost_event("gpt-4", 1000, 0.1);
            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        assert!((metrics.daily_cost - 0.5).abs() < 0.001);
        let utilization = metrics.daily_budget_utilization(1.0);
        assert!((utilization - 50.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_budget_alert_generation() {
        let config = CostAnalyzerConfig {
            daily_budget_usd: 1.0,
            budget_warning_threshold_pct: 80.0,
            ..Default::default()
        };
        let mut analyzer = CostAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Spend 85% of budget
        let event = create_test_cost_event("gpt-4", 1000, 0.85);
        analyzer.process_event(event).await.unwrap();

        let insights = analyzer.analyze_metrics().await;
        assert!(insights.iter().any(|i| i.title.contains("Budget")));
    }

    #[tokio::test]
    async fn test_cost_spike_detection() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        // Establish baseline
        for _ in 0..50 {
            let event = create_test_cost_event("gpt-4", 1000, 0.05);
            analyzer.process_event(event).await.unwrap();
        }

        // Set baseline
        {
            let mut metrics = analyzer.metrics.write().await;
            metrics.baseline_cost_per_request = Some(0.05);
        }

        // Add spike
        for _ in 0..10 {
            let event = create_test_cost_event("gpt-4", 1000, 0.15); // 3x higher
            analyzer.process_event(event).await.unwrap();
        }

        let insights = analyzer.analyze_metrics().await;
        assert!(insights.iter().any(|i| i.title.contains("Spike")));
    }

    #[tokio::test]
    async fn test_per_model_cost_tracking() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event1 = create_test_response_event("gpt-4", 1000, 500, true);
        let event2 = create_test_response_event("gpt-3.5-turbo", 1000, 500, true);

        analyzer.process_event(event1).await.unwrap();
        analyzer.process_event(event2).await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.model_costs.len(), 2);
        assert!(metrics.model_costs.contains_key("gpt-4"));
        assert!(metrics.model_costs.contains_key("gpt-3.5-turbo"));

        let gpt4_cost = metrics.model_costs.get("gpt-4").unwrap().total_cost;
        let gpt35_cost = metrics.model_costs.get("gpt-3.5-turbo").unwrap().total_cost;
        assert!(gpt4_cost > gpt35_cost); // GPT-4 should be more expensive
    }

    #[tokio::test]
    async fn test_generate_report() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        for _ in 0..10 {
            let event = create_test_cost_event("gpt-4", 1000, 0.05);
            analyzer.process_event(event).await.unwrap();
        }

        let report = analyzer.generate_report().await.unwrap();
        assert_eq!(report.analyzer, "cost");
        assert_eq!(report.summary.events_processed, 10);
        assert!(!report.metrics.is_empty());
        assert!(report.metrics.contains_key("total_cost_usd"));
        assert!(report.metrics.contains_key("daily_cost_usd"));
    }

    #[tokio::test]
    async fn test_reset() {
        let mut analyzer = CostAnalyzer::with_defaults();
        analyzer.start().await.unwrap();

        let event = create_test_cost_event("gpt-4", 1000, 0.05);
        analyzer.process_event(event).await.unwrap();

        analyzer.reset().await.unwrap();

        let metrics = analyzer.metrics.read().await;
        assert_eq!(metrics.total_cost, 0.0);
        assert_eq!(metrics.model_costs.len(), 0);
    }

    #[tokio::test]
    async fn test_model_optimization_recommendation() {
        let config = CostAnalyzerConfig {
            enable_optimization_recommendations: true,
            ..Default::default()
        };
        let mut analyzer = CostAnalyzer::new(config);
        analyzer.start().await.unwrap();

        // Use expensive model
        for _ in 0..50 {
            let event = create_test_response_event("gpt-4", 1000, 500, true);
            analyzer.process_event(event).await.unwrap();
        }

        // Use cheaper model
        for _ in 0..50 {
            let event = create_test_response_event("gpt-3.5-turbo", 1000, 500, true);
            analyzer.process_event(event).await.unwrap();
        }

        let metrics = analyzer.metrics.read().await;
        let insights = analyzer.analyze_metrics().await;
        let recommendations = analyzer.generate_recommendations(&insights, &metrics);

        assert!(recommendations.iter().any(|r| r.title.contains("switching")));
    }
}
