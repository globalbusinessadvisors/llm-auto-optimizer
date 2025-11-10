//! Caching Strategy for the Decision Engine
//!
//! This strategy analyzes request patterns to determine when to enable or update
//! caching configurations to reduce costs and latency. It supports semantic similarity
//! caching and provides comprehensive cost-benefit analysis.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use uuid::Uuid;

use super::config::CachingConfig;
use super::error::{DecisionError, DecisionResult};
use super::traits::OptimizationStrategy;
use super::types::{
    ComparisonOperator, ConfigChange, ConfigType, Decision, DecisionCriteria, DecisionInput,
    DecisionOutcome, DecisionType, ExpectedImpact, RollbackCondition, RollbackPlan, SafetyCheck,
};

/// Caching optimization strategy
///
/// Analyzes request patterns to identify caching opportunities and generates
/// decisions to enable or configure caching with semantic similarity support.
pub struct CachingStrategy {
    /// Strategy configuration
    config: CachingConfig,

    /// Request pattern history for analysis
    request_patterns: VecDeque<RequestPattern>,

    /// Cache hit rate tracking
    hit_rate_history: VecDeque<HitRateRecord>,

    /// Similarity matrix for request clustering
    similarity_cache: HashMap<String, Vec<SimilarityScore>>,

    /// Strategy statistics
    stats: CachingStats,

    /// Last evaluation timestamp
    last_evaluation: Option<DateTime<Utc>>,

    /// Current cache configuration state
    current_config: Option<CacheConfiguration>,

    /// Learning data from outcomes
    learning_data: LearningData,
}

/// A request pattern captured for analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestPattern {
    /// Pattern identifier
    id: String,

    /// Timestamp
    timestamp: DateTime<Utc>,

    /// Request fingerprint (hash of normalized request)
    fingerprint: String,

    /// Request metadata
    metadata: HashMap<String, String>,

    /// Prompt tokens
    tokens: u32,

    /// Model used
    model: String,

    /// Request count with this pattern
    count: u64,
}

/// Cache hit rate record
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HitRateRecord {
    /// Timestamp
    timestamp: DateTime<Utc>,

    /// Hit rate (0-100)
    hit_rate_pct: f64,

    /// Total requests
    total_requests: u64,

    /// Cache hits
    cache_hits: u64,

    /// Cache size
    cache_size: usize,

    /// Average latency saved (ms)
    avg_latency_saved_ms: f64,

    /// Cost saved (USD)
    cost_saved_usd: f64,
}

/// Similarity score between two requests
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimilarityScore {
    /// Request pattern ID
    pattern_id: String,

    /// Similarity score (0-1)
    score: f64,

    /// Timestamp
    timestamp: DateTime<Utc>,
}

/// Current cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheConfiguration {
    /// Whether caching is enabled
    enabled: bool,

    /// Cache TTL
    ttl: Duration,

    /// Maximum cache size
    max_size: usize,

    /// Similarity threshold
    similarity_threshold: f64,

    /// Cache strategy type
    strategy_type: CacheStrategyType,

    /// Last updated
    last_updated: DateTime<Utc>,
}

/// Type of cache strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum CacheStrategyType {
    /// Exact match only
    Exact,

    /// Semantic similarity based
    Semantic,

    /// Hybrid (exact + semantic)
    Hybrid,
}

/// Statistics for the caching strategy
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CachingStats {
    /// Total decisions made
    total_decisions: u64,

    /// Caching enabled decisions
    caching_enabled: u64,

    /// Caching disabled decisions
    caching_disabled: u64,

    /// Configuration updates
    config_updates: u64,

    /// Average predicted hit rate
    avg_predicted_hit_rate: f64,

    /// Average actual hit rate
    avg_actual_hit_rate: f64,

    /// Total cost savings (USD)
    total_cost_savings_usd: f64,

    /// Total latency savings (ms)
    total_latency_savings_ms: f64,

    /// Prediction accuracy
    prediction_accuracy: f64,

    /// Patterns analyzed
    patterns_analyzed: u64,

    /// Learning iterations
    learning_iterations: u64,
}

/// Learning data for strategy improvement
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LearningData {
    /// Predicted vs actual hit rates
    hit_rate_predictions: VecDeque<HitRatePrediction>,

    /// Cost benefit accuracy
    cost_accuracy: VecDeque<CostAccuracy>,

    /// Similarity threshold effectiveness
    threshold_effectiveness: HashMap<String, f64>,

    /// Model for hit rate adjustment
    hit_rate_adjustment_factor: f64,

    /// Model for cost estimation adjustment
    cost_adjustment_factor: f64,
}

/// Hit rate prediction record
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HitRatePrediction {
    /// Decision ID
    decision_id: String,

    /// Predicted hit rate
    predicted: f64,

    /// Actual hit rate
    actual: f64,

    /// Variance
    variance: f64,

    /// Timestamp
    timestamp: DateTime<Utc>,
}

/// Cost accuracy record
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CostAccuracy {
    /// Decision ID
    decision_id: String,

    /// Predicted savings
    predicted_savings_usd: f64,

    /// Actual savings
    actual_savings_usd: f64,

    /// Variance
    variance: f64,

    /// Timestamp
    timestamp: DateTime<Utc>,
}

/// Request cluster for pattern analysis
#[derive(Debug, Clone)]
struct RequestCluster {
    /// Cluster ID
    id: String,

    /// Representative pattern
    representative: RequestPattern,

    /// Member patterns
    members: Vec<RequestPattern>,

    /// Total request count
    total_count: u64,

    /// Average similarity
    avg_similarity: f64,

    /// Frequency (requests per minute)
    frequency: f64,
}

/// Cache opportunity analysis result
#[derive(Debug, Clone)]
struct CacheOpportunity {
    /// Cluster being analyzed
    cluster: RequestCluster,

    /// Expected hit rate
    expected_hit_rate_pct: f64,

    /// Expected cost savings (USD/day)
    expected_cost_savings_usd: f64,

    /// Expected latency savings (ms)
    expected_latency_savings_ms: f64,

    /// Recommended cache size
    recommended_cache_size: usize,

    /// Recommended TTL
    recommended_ttl: Duration,

    /// Recommended similarity threshold
    recommended_similarity_threshold: f64,

    /// Confidence in recommendation
    confidence: f64,
}

impl CachingStrategy {
    /// Create a new caching strategy
    pub fn new(config: CachingConfig) -> Self {
        Self {
            config,
            request_patterns: VecDeque::new(),
            hit_rate_history: VecDeque::new(),
            similarity_cache: HashMap::new(),
            stats: CachingStats::default(),
            last_evaluation: None,
            current_config: None,
            learning_data: LearningData {
                hit_rate_adjustment_factor: 1.0,
                cost_adjustment_factor: 1.0,
                ..Default::default()
            },
        }
    }

    /// Analyze request patterns from the input
    fn analyze_request_patterns(&mut self, input: &DecisionInput) -> DecisionResult<()> {
        // Extract request patterns from analysis reports
        for report in &input.analysis_reports {
            for insight in &report.insights {
                // Look for request patterns in insights
                if let Some(pattern_data) = insight.metrics.get("request_pattern") {
                    let pattern = RequestPattern {
                        id: Uuid::new_v4().to_string(),
                        timestamp: insight.timestamp,
                        fingerprint: format!("fp_{}", pattern_data),
                        metadata: HashMap::new(),
                        tokens: *insight.metrics.get("tokens").unwrap_or(&0.0) as u32,
                        model: insight
                            .metrics
                            .get("model")
                            .and_then(|v| Some(format!("model_{}", v)))
                            .unwrap_or_else(|| "unknown".to_string()),
                        count: *insight.metrics.get("count").unwrap_or(&1.0) as u64,
                    };

                    self.request_patterns.push_back(pattern);
                }
            }
        }

        // Limit history size
        while self.request_patterns.len() > 10000 {
            self.request_patterns.pop_front();
        }

        self.stats.patterns_analyzed += 1;
        Ok(())
    }

    /// Cluster similar requests for pattern analysis
    fn cluster_requests(&self) -> Vec<RequestCluster> {
        let mut clusters = Vec::new();
        let mut processed = std::collections::HashSet::new();

        for pattern in self.request_patterns.iter() {
            if processed.contains(&pattern.id) {
                continue;
            }

            // Find similar patterns
            let similar = self.find_similar_patterns(pattern);
            let total_count: u64 = similar.iter().map(|p| p.count).sum();

            let cluster = RequestCluster {
                id: Uuid::new_v4().to_string(),
                representative: pattern.clone(),
                members: similar.clone(),
                total_count,
                avg_similarity: self.calculate_avg_similarity(&similar),
                frequency: self.calculate_frequency(&similar),
            };

            for member in &similar {
                processed.insert(member.id.clone());
            }

            clusters.push(cluster);
        }

        clusters
    }

    /// Find patterns similar to the given pattern
    fn find_similar_patterns(&self, pattern: &RequestPattern) -> Vec<RequestPattern> {
        let threshold = self.config.min_similarity_threshold;
        let mut similar = vec![pattern.clone()];

        for other in self.request_patterns.iter() {
            if other.id == pattern.id {
                continue;
            }

            let similarity = self.calculate_similarity(pattern, other);
            if similarity >= threshold {
                similar.push(other.clone());
            }
        }

        similar
    }

    /// Calculate similarity between two patterns
    fn calculate_similarity(&self, a: &RequestPattern, b: &RequestPattern) -> f64 {
        // Check cache first
        if let Some(scores) = self.similarity_cache.get(&a.id) {
            if let Some(score) = scores.iter().find(|s| s.pattern_id == b.id) {
                return score.score;
            }
        }

        // Simple similarity based on fingerprint, model, and token count
        let mut similarity = 0.0;

        // Fingerprint similarity (exact match for now, could use embeddings)
        if a.fingerprint == b.fingerprint {
            similarity += 0.6;
        }

        // Model similarity
        if a.model == b.model {
            similarity += 0.2;
        }

        // Token count similarity (within 10%)
        let token_diff = (a.tokens as f64 - b.tokens as f64).abs() / a.tokens.max(1) as f64;
        if token_diff < 0.1 {
            similarity += 0.2;
        } else if token_diff < 0.3 {
            similarity += 0.1;
        }

        similarity
    }

    /// Calculate average similarity within a cluster
    fn calculate_avg_similarity(&self, patterns: &[RequestPattern]) -> f64 {
        if patterns.len() <= 1 {
            return 1.0;
        }

        let mut total_similarity = 0.0;
        let mut count = 0;

        for i in 0..patterns.len() {
            for j in (i + 1)..patterns.len() {
                total_similarity += self.calculate_similarity(&patterns[i], &patterns[j]);
                count += 1;
            }
        }

        if count > 0 {
            total_similarity / count as f64
        } else {
            1.0
        }
    }

    /// Calculate request frequency for a cluster
    fn calculate_frequency(&self, patterns: &[RequestPattern]) -> f64 {
        if patterns.is_empty() {
            return 0.0;
        }

        // Calculate time span
        let timestamps: Vec<_> = patterns.iter().map(|p| p.timestamp).collect();
        let min_time = timestamps.iter().min().unwrap();
        let max_time = timestamps.iter().max().unwrap();

        let duration_mins = (*max_time - *min_time).num_minutes().max(1) as f64;
        let total_requests: u64 = patterns.iter().map(|p| p.count).sum();

        total_requests as f64 / duration_mins
    }

    /// Identify caching opportunities from clusters
    fn identify_opportunities(&self, clusters: &[RequestCluster]) -> Vec<CacheOpportunity> {
        let mut opportunities = Vec::new();

        for cluster in clusters {
            // Only consider clusters with sufficient frequency
            if cluster.frequency < self.config.min_request_frequency {
                continue;
            }

            // Calculate expected hit rate based on cluster characteristics
            let expected_hit_rate = self.calculate_expected_hit_rate(cluster);

            // Only recommend caching if expected hit rate meets threshold
            if expected_hit_rate < self.config.min_expected_hit_rate_pct {
                continue;
            }

            // Calculate cost and latency savings
            let cost_savings = self.calculate_cost_savings(cluster, expected_hit_rate);
            let latency_savings = self.calculate_latency_savings(cluster, expected_hit_rate);

            // Determine optimal cache configuration
            let cache_size = self.recommend_cache_size(cluster);
            let ttl = self.recommend_ttl(cluster);
            let similarity_threshold = self.recommend_similarity_threshold(cluster);

            // Calculate confidence based on data quality
            let confidence = self.calculate_opportunity_confidence(cluster, expected_hit_rate);

            opportunities.push(CacheOpportunity {
                cluster: cluster.clone(),
                expected_hit_rate_pct: expected_hit_rate,
                expected_cost_savings_usd: cost_savings,
                expected_latency_savings_ms: latency_savings,
                recommended_cache_size: cache_size,
                recommended_ttl: ttl,
                recommended_similarity_threshold: similarity_threshold,
                confidence,
            });
        }

        // Sort by expected cost savings
        opportunities.sort_by(|a, b| {
            b.expected_cost_savings_usd
                .partial_cmp(&a.expected_cost_savings_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        opportunities
    }

    /// Calculate expected cache hit rate for a cluster
    fn calculate_expected_hit_rate(&self, cluster: &RequestCluster) -> f64 {
        // Base hit rate on cluster characteristics
        let mut base_rate = 0.0;

        // Higher similarity -> higher hit rate
        base_rate += cluster.avg_similarity * 50.0;

        // Higher frequency -> higher hit rate potential
        let freq_factor = (cluster.frequency / 100.0).min(1.0);
        base_rate += freq_factor * 30.0;

        // Cluster size factor
        let size_factor = (cluster.members.len() as f64 / 10.0).min(1.0);
        base_rate += size_factor * 20.0;

        // Apply learning adjustment
        base_rate *= self.learning_data.hit_rate_adjustment_factor;

        // Cap at 95% (never assume perfect hit rate)
        base_rate.min(95.0)
    }

    /// Calculate expected cost savings
    fn calculate_cost_savings(&self, cluster: &RequestCluster, hit_rate_pct: f64) -> f64 {
        // Estimate daily requests
        let daily_requests = cluster.frequency * 60.0 * 24.0;

        // Estimate cost per request (assuming average of $0.001 per request)
        let cost_per_request = 0.001;

        // Calculate savings
        let savings = daily_requests * (hit_rate_pct / 100.0) * cost_per_request;

        // Apply learning adjustment
        savings * self.learning_data.cost_adjustment_factor
    }

    /// Calculate expected latency savings
    fn calculate_latency_savings(&self, cluster: &RequestCluster, hit_rate_pct: f64) -> f64 {
        // Assume cache hit saves ~200ms on average
        let latency_saved_per_hit = 200.0;

        // Average latency saved per request
        (hit_rate_pct / 100.0) * latency_saved_per_hit
    }

    /// Recommend optimal cache size
    fn recommend_cache_size(&self, cluster: &RequestCluster) -> usize {
        // Base size on cluster characteristics
        let base_size = cluster.members.len() * 10;

        // Cap at configured maximum
        base_size.min(self.config.max_cache_size)
    }

    /// Recommend cache TTL
    fn recommend_ttl(&self, cluster: &RequestCluster) -> Duration {
        // Base TTL on request frequency and pattern stability
        let base_ttl = if cluster.frequency > 50.0 {
            // High frequency: shorter TTL
            Duration::from_secs(1800) // 30 minutes
        } else if cluster.frequency > 20.0 {
            // Medium frequency: standard TTL
            Duration::from_secs(3600) // 1 hour
        } else {
            // Lower frequency: longer TTL
            Duration::from_secs(7200) // 2 hours
        };

        // Don't exceed configured TTL
        if base_ttl > self.config.cache_ttl {
            self.config.cache_ttl
        } else {
            base_ttl
        }
    }

    /// Recommend similarity threshold
    fn recommend_similarity_threshold(&self, cluster: &RequestCluster) -> f64 {
        // Base threshold on cluster similarity
        let base_threshold: f64 = if cluster.avg_similarity > 0.9 {
            // Very similar: can use lower threshold
            0.80
        } else if cluster.avg_similarity > 0.8 {
            // Similar: standard threshold
            0.85
        } else {
            // Less similar: higher threshold
            0.90
        };

        // Don't go below configured minimum
        base_threshold.max(self.config.min_similarity_threshold)
    }

    /// Calculate confidence in the opportunity
    fn calculate_opportunity_confidence(&self, cluster: &RequestCluster, hit_rate: f64) -> f64 {
        let mut confidence = 0.5; // Base confidence

        // More cluster members -> higher confidence
        let size_factor = (cluster.members.len() as f64 / 50.0).min(1.0);
        confidence += size_factor * 0.2;

        // Higher similarity -> higher confidence
        confidence += cluster.avg_similarity * 0.15;

        // Higher frequency -> higher confidence
        let freq_factor = (cluster.frequency / 100.0).min(1.0);
        confidence += freq_factor * 0.1;

        // Historical accuracy improves confidence
        confidence += self.stats.prediction_accuracy * 0.05;

        confidence.min(1.0)
    }

    /// Create a decision from a caching opportunity
    fn create_decision(
        &self,
        opportunity: &CacheOpportunity,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Decision> {
        let decision_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // Build configuration changes
        let mut config_changes = Vec::new();

        // Enable caching
        config_changes.push(ConfigChange {
            config_type: ConfigType::Cache,
            path: "cache.enabled".to_string(),
            old_value: Some(serde_json::json!(false)),
            new_value: serde_json::json!(true),
            description: "Enable semantic caching".to_string(),
        });

        // Set cache size
        config_changes.push(ConfigChange {
            config_type: ConfigType::Cache,
            path: "cache.max_size".to_string(),
            old_value: self
                .current_config
                .as_ref()
                .map(|c| serde_json::json!(c.max_size)),
            new_value: serde_json::json!(opportunity.recommended_cache_size),
            description: format!("Set cache size to {}", opportunity.recommended_cache_size),
        });

        // Set TTL
        config_changes.push(ConfigChange {
            config_type: ConfigType::Cache,
            path: "cache.ttl_seconds".to_string(),
            old_value: self
                .current_config
                .as_ref()
                .map(|c| serde_json::json!(c.ttl.as_secs())),
            new_value: serde_json::json!(opportunity.recommended_ttl.as_secs()),
            description: format!(
                "Set cache TTL to {} seconds",
                opportunity.recommended_ttl.as_secs()
            ),
        });

        // Set similarity threshold
        config_changes.push(ConfigChange {
            config_type: ConfigType::Cache,
            path: "cache.similarity_threshold".to_string(),
            old_value: self
                .current_config
                .as_ref()
                .map(|c| serde_json::json!(c.similarity_threshold)),
            new_value: serde_json::json!(opportunity.recommended_similarity_threshold),
            description: format!(
                "Set similarity threshold to {:.2}",
                opportunity.recommended_similarity_threshold
            ),
        });

        // Safety checks
        let mut safety_checks = Vec::new();

        // Check storage limits
        let storage_check = self.check_storage_limits(opportunity)?;
        safety_checks.push(storage_check);

        // Check latency impact
        let latency_check = self.check_latency_impact(opportunity, criteria)?;
        safety_checks.push(latency_check);

        // Check cost impact
        let cost_check = self.check_cost_impact(opportunity, criteria)?;
        safety_checks.push(cost_check);

        // Verify all safety checks passed
        if !safety_checks.iter().all(|c| c.passed) {
            return Err(DecisionError::ValidationFailed(
                "Safety checks failed for caching decision".to_string(),
            ));
        }

        // Create expected impact
        let expected_impact = ExpectedImpact {
            cost_change_usd: Some(-opportunity.expected_cost_savings_usd),
            latency_change_ms: Some(-opportunity.expected_latency_savings_ms),
            quality_change: None,
            throughput_change_rps: None,
            success_rate_change_pct: None,
            time_to_impact: Duration::from_secs(300), // 5 minutes to see impact
            confidence: opportunity.confidence,
        };

        // Create rollback plan
        let rollback_plan = RollbackPlan {
            description: "Disable caching if hit rate is too low or costs increase".to_string(),
            revert_changes: config_changes
                .iter()
                .map(|c| ConfigChange {
                    config_type: c.config_type,
                    path: c.path.clone(),
                    old_value: Some(c.new_value.clone()),
                    new_value: c.old_value.clone().unwrap_or(serde_json::json!(null)),
                    description: format!("Revert: {}", c.description),
                })
                .collect(),
            trigger_conditions: vec![
                RollbackCondition {
                    metric: "cache_hit_rate_pct".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: self.config.min_expected_hit_rate_pct * 0.5,
                    description: "Hit rate fell below 50% of expected".to_string(),
                },
                RollbackCondition {
                    metric: "cost_change_usd".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 0.0,
                    description: "Cost increased instead of decreased".to_string(),
                },
            ],
            max_duration: Duration::from_secs(3600), // 1 hour
        };

        // Build justification
        let justification = format!(
            "Detected {} requests with {:.1}% similarity occurring at {:.1} req/min. \
             Expected {:.1}% cache hit rate would save ${:.2}/day and {:.1}ms latency. \
             Confidence: {:.1}%",
            opportunity.cluster.total_count,
            opportunity.cluster.avg_similarity * 100.0,
            opportunity.cluster.frequency,
            opportunity.expected_hit_rate_pct,
            opportunity.expected_cost_savings_usd,
            opportunity.expected_latency_savings_ms,
            opportunity.confidence * 100.0
        );

        Ok(Decision {
            id: decision_id,
            timestamp: now,
            strategy: "caching".to_string(),
            decision_type: DecisionType::EnableCaching,
            confidence: opportunity.confidence,
            expected_impact,
            config_changes,
            justification,
            related_insights: Vec::new(),
            related_recommendations: Vec::new(),
            priority: if opportunity.expected_cost_savings_usd > 10.0 {
                100
            } else if opportunity.expected_cost_savings_usd > 5.0 {
                80
            } else {
                60
            },
            requires_approval: opportunity.confidence < 0.8,
            safety_checks,
            rollback_plan: Some(rollback_plan),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert(
                    "cluster_id".to_string(),
                    serde_json::json!(opportunity.cluster.id),
                );
                meta.insert(
                    "expected_hit_rate".to_string(),
                    serde_json::json!(opportunity.expected_hit_rate_pct),
                );
                meta.insert(
                    "request_frequency".to_string(),
                    serde_json::json!(opportunity.cluster.frequency),
                );
                meta
            },
        })
    }

    /// Check storage limits safety
    fn check_storage_limits(&self, opportunity: &CacheOpportunity) -> DecisionResult<SafetyCheck> {
        let passed = opportunity.recommended_cache_size <= self.config.max_cache_size;

        Ok(SafetyCheck {
            name: "storage_limits".to_string(),
            passed,
            details: format!(
                "Recommended cache size {} is {} configured limit {}",
                opportunity.recommended_cache_size,
                if passed { "within" } else { "exceeds" },
                self.config.max_cache_size
            ),
            timestamp: Utc::now(),
        })
    }

    /// Check latency impact safety
    fn check_latency_impact(
        &self,
        opportunity: &CacheOpportunity,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<SafetyCheck> {
        // Cache lookup adds ~5-10ms overhead
        let cache_overhead_ms = 7.5;

        // Net latency change (overhead - savings)
        let net_latency_change = cache_overhead_ms - opportunity.expected_latency_savings_ms;

        let passed = if let Some(max_increase) = criteria.max_latency_increase_ms {
            net_latency_change <= max_increase
        } else {
            net_latency_change <= 0.0 // Should improve latency
        };

        Ok(SafetyCheck {
            name: "latency_impact".to_string(),
            passed,
            details: format!(
                "Net latency change: {:.2}ms (overhead: {:.2}ms, savings: {:.2}ms)",
                net_latency_change, cache_overhead_ms, opportunity.expected_latency_savings_ms
            ),
            timestamp: Utc::now(),
        })
    }

    /// Check cost impact safety
    fn check_cost_impact(
        &self,
        opportunity: &CacheOpportunity,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<SafetyCheck> {
        // Cache has storage cost (~$0.10/GB/month, estimate 1MB per 1000 entries)
        let storage_cost_per_day =
            (opportunity.recommended_cache_size as f64 / 1000.0) * 0.001 * 0.1 / 30.0;

        // Net cost change (savings - storage cost)
        let net_cost_change = -opportunity.expected_cost_savings_usd + storage_cost_per_day;

        let passed = if let Some(max_increase) = criteria.max_cost_increase_usd {
            net_cost_change <= max_increase
        } else {
            net_cost_change <= 0.0 // Should save money
        };

        Ok(SafetyCheck {
            name: "cost_impact".to_string(),
            passed,
            details: format!(
                "Net cost change: ${:.4}/day (savings: ${:.4}, storage: ${:.4})",
                net_cost_change, opportunity.expected_cost_savings_usd, storage_cost_per_day
            ),
            timestamp: Utc::now(),
        })
    }

    /// Update learning model from decision outcome
    fn update_learning_model(&mut self, decision: &Decision, outcome: &DecisionOutcome) {
        if outcome.success && outcome.actual_impact.is_some() {
            let actual = outcome.actual_impact.as_ref().unwrap();

            // Extract predicted values from decision metadata
            if let Some(predicted_hit_rate) = decision.metadata.get("expected_hit_rate") {
                if let Some(predicted_rate) = predicted_hit_rate.as_f64() {
                    // Get actual hit rate from metrics after
                    if let Some(actual_rate) = outcome
                        .metrics_after
                        .as_ref()
                        .and_then(|m| m.cache_hit_rate_pct)
                    {
                        let variance = (predicted_rate - actual_rate).abs() / predicted_rate.max(1.0);

                        self.learning_data.hit_rate_predictions.push_back(HitRatePrediction {
                            decision_id: decision.id.clone(),
                            predicted: predicted_rate,
                            actual: actual_rate,
                            variance,
                            timestamp: outcome.execution_time,
                        });

                        // Update adjustment factor
                        if !self.learning_data.hit_rate_predictions.is_empty() {
                            let avg_variance: f64 = self
                                .learning_data
                                .hit_rate_predictions
                                .iter()
                                .map(|p| p.variance)
                                .sum::<f64>()
                                / self.learning_data.hit_rate_predictions.len() as f64;

                            // Adjust factor to compensate for systematic bias
                            if avg_variance > 0.2 {
                                let avg_ratio: f64 = self
                                    .learning_data
                                    .hit_rate_predictions
                                    .iter()
                                    .map(|p| p.actual / p.predicted.max(0.01))
                                    .sum::<f64>()
                                    / self.learning_data.hit_rate_predictions.len() as f64;

                                self.learning_data.hit_rate_adjustment_factor = avg_ratio;
                            }
                        }
                    }
                }
            }

            // Learn from cost predictions
            if let Some(expected_cost) = decision.expected_impact.cost_change_usd {
                let actual_cost = actual.cost_change_usd;
                let variance = (expected_cost - actual_cost).abs() / expected_cost.abs().max(0.01);

                self.learning_data.cost_accuracy.push_back(CostAccuracy {
                    decision_id: decision.id.clone(),
                    predicted_savings_usd: -expected_cost,
                    actual_savings_usd: -actual_cost,
                    variance,
                    timestamp: outcome.execution_time,
                });

                // Update cost adjustment factor
                if !self.learning_data.cost_accuracy.is_empty() {
                    let avg_ratio: f64 = self
                        .learning_data
                        .cost_accuracy
                        .iter()
                        .map(|c| c.actual_savings_usd / c.predicted_savings_usd.max(0.01))
                        .sum::<f64>()
                        / self.learning_data.cost_accuracy.len() as f64;

                    self.learning_data.cost_adjustment_factor = avg_ratio;
                }
            }

            // Update prediction accuracy
            let hit_rate_accuracy = if !self.learning_data.hit_rate_predictions.is_empty() {
                1.0 - self
                    .learning_data
                    .hit_rate_predictions
                    .iter()
                    .map(|p| p.variance)
                    .sum::<f64>()
                    / self.learning_data.hit_rate_predictions.len() as f64
            } else {
                0.5
            };

            let cost_accuracy = if !self.learning_data.cost_accuracy.is_empty() {
                1.0 - self
                    .learning_data
                    .cost_accuracy
                    .iter()
                    .map(|c| c.variance)
                    .sum::<f64>()
                    / self.learning_data.cost_accuracy.len() as f64
            } else {
                0.5
            };

            self.stats.prediction_accuracy = (hit_rate_accuracy + cost_accuracy) / 2.0;
        }

        // Limit history size
        while self.learning_data.hit_rate_predictions.len() > 100 {
            self.learning_data.hit_rate_predictions.pop_front();
        }
        while self.learning_data.cost_accuracy.len() > 100 {
            self.learning_data.cost_accuracy.pop_front();
        }

        self.stats.learning_iterations += 1;
    }
}

#[async_trait]
impl OptimizationStrategy for CachingStrategy {
    fn name(&self) -> &str {
        "caching"
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_applicable(&self, input: &DecisionInput) -> bool {
        // Strategy is applicable if:
        // 1. We have analysis reports with insights
        // 2. We have request pattern data
        // 3. Strategy is enabled

        if !self.config.enabled {
            return false;
        }

        if input.analysis_reports.is_empty() {
            return false;
        }

        // Check if any reports contain request pattern insights
        input.analysis_reports.iter().any(|report| {
            !report.insights.is_empty()
                || report.metrics.contains_key("request_count")
                || report.metrics.contains_key("cache_hit_rate")
        })
    }

    async fn evaluate(
        &self,
        input: &DecisionInput,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Vec<Decision>> {
        let mut decisions = Vec::new();

        // Create a mutable copy for pattern analysis
        let mut strategy = self.clone();
        strategy.analyze_request_patterns(input)?;

        // Cluster similar requests
        let clusters = strategy.cluster_requests();

        if clusters.is_empty() {
            return Ok(decisions);
        }

        // Identify caching opportunities
        let opportunities = strategy.identify_opportunities(&clusters);

        // Create decisions for top opportunities
        for opportunity in opportunities.iter().take(3) {
            // Limit to top 3
            // Check confidence threshold
            if opportunity.confidence < self.config.min_confidence {
                continue;
            }

            // Create decision
            match strategy.create_decision(opportunity, criteria) {
                Ok(decision) => decisions.push(decision),
                Err(e) => {
                    tracing::warn!("Failed to create caching decision: {}", e);
                }
            }
        }

        Ok(decisions)
    }

    async fn validate(&self, decision: &Decision, criteria: &DecisionCriteria) -> DecisionResult<()> {
        // Validate decision type
        if decision.decision_type != DecisionType::EnableCaching {
            return Err(DecisionError::ValidationFailed(
                "Invalid decision type for caching strategy".to_string(),
            ));
        }

        // Validate confidence
        if decision.confidence < criteria.min_confidence {
            return Err(DecisionError::ValidationFailed(format!(
                "Confidence {:.2} below minimum {:.2}",
                decision.confidence, criteria.min_confidence
            )));
        }

        // Validate expected impact
        if let Some(cost_change) = decision.expected_impact.cost_change_usd {
            if let Some(max_increase) = criteria.max_cost_increase_usd {
                if cost_change > max_increase {
                    return Err(DecisionError::ConstraintViolation(format!(
                        "Cost increase ${:.2} exceeds maximum ${:.2}",
                        cost_change, max_increase
                    )));
                }
            }
        }

        // Validate safety checks
        if !decision.safety_checks.iter().all(|c| c.passed) {
            return Err(DecisionError::ValidationFailed(
                "Not all safety checks passed".to_string(),
            ));
        }

        Ok(())
    }

    async fn learn(&mut self, decision: &Decision, outcome: &DecisionOutcome) -> DecisionResult<()> {
        // Only learn from caching decisions
        if decision.decision_type != DecisionType::EnableCaching {
            return Ok(());
        }

        // Update learning model
        self.update_learning_model(decision, outcome);

        // Update statistics
        if outcome.success {
            self.stats.caching_enabled += 1;

            if let Some(actual) = &outcome.actual_impact {
                self.stats.total_cost_savings_usd += -actual.cost_change_usd;
                self.stats.total_latency_savings_ms += -actual.latency_change_ms;
            }
        }

        self.stats.total_decisions += 1;

        Ok(())
    }

    fn get_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "total_decisions": self.stats.total_decisions,
            "caching_enabled": self.stats.caching_enabled,
            "caching_disabled": self.stats.caching_disabled,
            "config_updates": self.stats.config_updates,
            "avg_predicted_hit_rate": self.stats.avg_predicted_hit_rate,
            "avg_actual_hit_rate": self.stats.avg_actual_hit_rate,
            "total_cost_savings_usd": self.stats.total_cost_savings_usd,
            "total_latency_savings_ms": self.stats.total_latency_savings_ms,
            "prediction_accuracy": self.stats.prediction_accuracy,
            "patterns_analyzed": self.stats.patterns_analyzed,
            "learning_iterations": self.stats.learning_iterations,
            "hit_rate_adjustment_factor": self.learning_data.hit_rate_adjustment_factor,
            "cost_adjustment_factor": self.learning_data.cost_adjustment_factor,
            "request_patterns_count": self.request_patterns.len(),
            "hit_rate_history_count": self.hit_rate_history.len(),
        })
    }
}

// Implement Clone for CachingStrategy
impl Clone for CachingStrategy {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            request_patterns: self.request_patterns.clone(),
            hit_rate_history: self.hit_rate_history.clone(),
            similarity_cache: self.similarity_cache.clone(),
            stats: self.stats.clone(),
            last_evaluation: self.last_evaluation,
            current_config: self.current_config.clone(),
            learning_data: self.learning_data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{AnalysisReport, Insight, ReportSummary};
    use crate::decision::types::SystemMetrics;

    fn create_test_config() -> CachingConfig {
        CachingConfig {
            enabled: true,
            priority: 90,
            min_confidence: 0.6,
            min_request_frequency: 5.0,
            min_similarity_threshold: 0.85,
            cache_ttl: Duration::from_secs(3600),
            max_cache_size: 1000,
            min_expected_hit_rate_pct: 20.0,
        }
    }

    fn create_test_input() -> DecisionInput {
        let now = Utc::now();

        DecisionInput {
            timestamp: now,
            analysis_reports: vec![AnalysisReport {
                analyzer: "test".to_string(),
                timestamp: now,
                period_start: now - chrono::Duration::hours(1),
                period_end: now,
                summary: ReportSummary {
                    events_processed: 100,
                    events_per_second: 1.0,
                    insights_count: 1,
                    recommendations_count: 0,
                    alerts_count: 0,
                    analysis_duration_ms: 100,
                    highlights: vec![],
                },
                insights: vec![Insight {
                    id: "test".to_string(),
                    analyzer: "test".to_string(),
                    timestamp: now,
                    severity: crate::analyzer::Severity::Info,
                    confidence: crate::analyzer::Confidence::new(0.8),
                    title: "Test".to_string(),
                    description: "Test".to_string(),
                    category: crate::analyzer::InsightCategory::Pattern,
                    evidence: vec![],
                    metrics: {
                        let mut m = HashMap::new();
                        m.insert("request_pattern".to_string(), 1.0);
                        m.insert("count".to_string(), 100.0);
                        m.insert("tokens".to_string(), 500.0);
                        m
                    },
                    tags: vec![],
                }],
                recommendations: vec![],
                alerts: vec![],
                metrics: {
                    let mut m = HashMap::new();
                    m.insert("request_count".to_string(), 100.0);
                    m
                },
            }],
            insights: vec![],
            recommendations: vec![],
            current_metrics: SystemMetrics {
                avg_latency_ms: 100.0,
                p95_latency_ms: 200.0,
                p99_latency_ms: 300.0,
                success_rate_pct: 99.0,
                error_rate_pct: 1.0,
                throughput_rps: 10.0,
                total_cost_usd: 100.0,
                daily_cost_usd: 10.0,
                avg_rating: Some(4.0),
                cache_hit_rate_pct: None,
                request_count: 1000,
                timestamp: now,
            },
            context: HashMap::new(),
        }
    }

    #[test]
    fn test_strategy_creation() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config.clone());

        assert_eq!(strategy.name(), "caching");
        assert_eq!(strategy.priority(), config.priority);
        assert_eq!(strategy.request_patterns.len(), 0);
    }

    #[test]
    fn test_is_applicable() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);
        let input = create_test_input();

        assert!(strategy.is_applicable(&input));
    }

    #[test]
    fn test_is_not_applicable_when_disabled() {
        let mut config = create_test_config();
        config.enabled = false;
        let strategy = CachingStrategy::new(config);
        let input = create_test_input();

        assert!(!strategy.is_applicable(&input));
    }

    #[test]
    fn test_is_not_applicable_with_empty_reports() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);
        let mut input = create_test_input();
        input.analysis_reports.clear();

        assert!(!strategy.is_applicable(&input));
    }

    #[test]
    fn test_pattern_analysis() {
        let config = create_test_config();
        let mut strategy = CachingStrategy::new(config);
        let input = create_test_input();

        strategy.analyze_request_patterns(&input).unwrap();

        assert!(!strategy.request_patterns.is_empty());
    }

    #[test]
    fn test_similarity_calculation_exact_match() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let pattern1 = RequestPattern {
            id: "1".to_string(),
            timestamp: Utc::now(),
            fingerprint: "fp1".to_string(),
            metadata: HashMap::new(),
            tokens: 100,
            model: "model1".to_string(),
            count: 1,
        };

        let pattern2 = pattern1.clone();

        let similarity = strategy.calculate_similarity(&pattern1, &pattern2);
        assert_eq!(similarity, 1.0);
    }

    #[test]
    fn test_similarity_calculation_different() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let pattern1 = RequestPattern {
            id: "1".to_string(),
            timestamp: Utc::now(),
            fingerprint: "fp1".to_string(),
            metadata: HashMap::new(),
            tokens: 100,
            model: "model1".to_string(),
            count: 1,
        };

        let pattern2 = RequestPattern {
            id: "2".to_string(),
            timestamp: Utc::now(),
            fingerprint: "fp2".to_string(),
            metadata: HashMap::new(),
            tokens: 500,
            model: "model2".to_string(),
            count: 1,
        };

        let similarity = strategy.calculate_similarity(&pattern1, &pattern2);
        assert!(similarity < 0.5);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let cluster = RequestCluster {
            id: "c1".to_string(),
            representative: RequestPattern {
                id: "1".to_string(),
                timestamp: Utc::now(),
                fingerprint: "fp1".to_string(),
                metadata: HashMap::new(),
                tokens: 100,
                model: "model1".to_string(),
                count: 10,
            },
            members: vec![],
            total_count: 100,
            avg_similarity: 0.9,
            frequency: 50.0,
        };

        let hit_rate = strategy.calculate_expected_hit_rate(&cluster);
        assert!(hit_rate > 0.0);
        assert!(hit_rate <= 95.0);
    }

    #[test]
    fn test_cost_savings_calculation() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let cluster = RequestCluster {
            id: "c1".to_string(),
            representative: RequestPattern {
                id: "1".to_string(),
                timestamp: Utc::now(),
                fingerprint: "fp1".to_string(),
                metadata: HashMap::new(),
                tokens: 100,
                model: "model1".to_string(),
                count: 10,
            },
            members: vec![],
            total_count: 100,
            avg_similarity: 0.9,
            frequency: 50.0,
        };

        let savings = strategy.calculate_cost_savings(&cluster, 50.0);
        assert!(savings > 0.0);
    }

    #[test]
    fn test_cache_size_recommendation() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config.clone());

        let cluster = RequestCluster {
            id: "c1".to_string(),
            representative: RequestPattern {
                id: "1".to_string(),
                timestamp: Utc::now(),
                fingerprint: "fp1".to_string(),
                metadata: HashMap::new(),
                tokens: 100,
                model: "model1".to_string(),
                count: 10,
            },
            members: vec![],
            total_count: 100,
            avg_similarity: 0.9,
            frequency: 50.0,
        };

        let size = strategy.recommend_cache_size(&cluster);
        assert!(size > 0);
        assert!(size <= config.max_cache_size);
    }

    #[test]
    fn test_ttl_recommendation() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config.clone());

        let cluster = RequestCluster {
            id: "c1".to_string(),
            representative: RequestPattern {
                id: "1".to_string(),
                timestamp: Utc::now(),
                fingerprint: "fp1".to_string(),
                metadata: HashMap::new(),
                tokens: 100,
                model: "model1".to_string(),
                count: 10,
            },
            members: vec![],
            total_count: 100,
            avg_similarity: 0.9,
            frequency: 50.0,
        };

        let ttl = strategy.recommend_ttl(&cluster);
        assert!(ttl > Duration::from_secs(0));
        assert!(ttl <= config.cache_ttl);
    }

    #[tokio::test]
    async fn test_evaluate_generates_decisions() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();

        // May or may not generate decisions depending on patterns
        assert!(decisions.len() <= 3);
    }

    #[tokio::test]
    async fn test_validate_correct_decision() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let decision = Decision {
            id: "test".to_string(),
            timestamp: Utc::now(),
            strategy: "caching".to_string(),
            decision_type: DecisionType::EnableCaching,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-5.0),
                latency_change_ms: Some(-100.0),
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 90,
            requires_approval: false,
            safety_checks: vec![SafetyCheck {
                name: "test".to_string(),
                passed: true,
                details: "test".to_string(),
                timestamp: Utc::now(),
            }],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let criteria = DecisionCriteria::default();
        assert!(strategy.validate(&decision, &criteria).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_wrong_decision_type() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let decision = Decision {
            id: "test".to_string(),
            timestamp: Utc::now(),
            strategy: "caching".to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-5.0),
                latency_change_ms: Some(-100.0),
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 90,
            requires_approval: false,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let criteria = DecisionCriteria::default();
        assert!(strategy.validate(&decision, &criteria).await.is_err());
    }

    #[tokio::test]
    async fn test_validate_low_confidence() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let decision = Decision {
            id: "test".to_string(),
            timestamp: Utc::now(),
            strategy: "caching".to_string(),
            decision_type: DecisionType::EnableCaching,
            confidence: 0.5,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-5.0),
                latency_change_ms: Some(-100.0),
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.5,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 90,
            requires_approval: false,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let criteria = DecisionCriteria::default();
        assert!(strategy.validate(&decision, &criteria).await.is_err());
    }

    #[tokio::test]
    async fn test_learn_updates_statistics() {
        let config = create_test_config();
        let mut strategy = CachingStrategy::new(config);

        let decision = Decision {
            id: "test".to_string(),
            timestamp: Utc::now(),
            strategy: "caching".to_string(),
            decision_type: DecisionType::EnableCaching,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-5.0),
                latency_change_ms: Some(-100.0),
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 90,
            requires_approval: false,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: {
                let mut m = HashMap::new();
                m.insert("expected_hit_rate".to_string(), serde_json::json!(50.0));
                m
            },
        };

        let outcome = DecisionOutcome {
            decision_id: "test".to_string(),
            execution_time: Utc::now(),
            success: true,
            error: None,
            actual_impact: Some(crate::decision::types::ActualImpact {
                cost_change_usd: -4.5,
                latency_change_ms: -95.0,
                quality_change: 0.0,
                throughput_change_rps: 0.0,
                success_rate_change_pct: 0.0,
                time_to_impact: Duration::from_secs(300),
                variance_from_expected: 0.1,
            }),
            rolled_back: false,
            rollback_reason: None,
            metrics_before: create_test_input().current_metrics,
            metrics_after: Some(SystemMetrics {
                cache_hit_rate_pct: Some(48.0),
                ..create_test_input().current_metrics
            }),
            duration: Duration::from_secs(3600),
        };

        let initial_decisions = strategy.stats.total_decisions;
        strategy.learn(&decision, &outcome).await.unwrap();

        assert_eq!(strategy.stats.total_decisions, initial_decisions + 1);
        assert_eq!(strategy.stats.caching_enabled, 1);
    }

    #[test]
    fn test_get_stats() {
        let config = create_test_config();
        let strategy = CachingStrategy::new(config);

        let stats = strategy.get_stats();
        assert!(stats.is_object());
        assert!(stats.get("total_decisions").is_some());
        assert!(stats.get("prediction_accuracy").is_some());
    }
}
