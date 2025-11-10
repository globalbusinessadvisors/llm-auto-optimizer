//! Model Selection Strategy
//!
//! This strategy uses multi-objective optimization to select the optimal LLM model
//! based on cost, performance (latency), and quality. It implements Pareto frontier
//! analysis to find the best trade-offs between these competing objectives.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use super::config::ModelSelectionConfig;
use super::error::{DecisionError, DecisionResult};
use super::traits::OptimizationStrategy;
use super::types::{
    ComparisonOperator, ConfigChange, ConfigType, Decision, DecisionCriteria, DecisionInput,
    DecisionOutcome, DecisionType, ExpectedImpact, RollbackCondition, RollbackPlan, SafetyCheck,
};

/// Model Selection Strategy
///
/// This strategy evaluates different LLM models based on:
/// - Cost: Token cost and total daily cost
/// - Performance: Latency (P50, P95, P99)
/// - Quality: User ratings and success rates
///
/// It uses Pareto optimization to find models that are optimal for at least
/// one objective while not being dominated by other models in all objectives.
#[derive(Debug)]
pub struct ModelSelectionStrategy {
    /// Configuration
    config: ModelSelectionConfig,

    /// Performance history for each model
    model_performance: HashMap<String, ModelPerformance>,

    /// Current model being used
    current_model: Option<String>,

    /// Last evaluation time
    last_evaluation: Option<DateTime<Utc>>,

    /// Decision history for learning
    decision_history: Vec<DecisionRecord>,

    /// Statistics
    stats: StrategyStats,
}

/// Performance metrics for a specific model
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelPerformance {
    /// Model name
    model: String,

    /// Number of requests
    request_count: u64,

    /// Average latency (ms)
    avg_latency_ms: f64,

    /// P95 latency (ms)
    p95_latency_ms: f64,

    /// P99 latency (ms)
    p99_latency_ms: f64,

    /// Average cost per request (USD)
    avg_cost_per_request: f64,

    /// Total cost (USD)
    total_cost: f64,

    /// Success rate (0-1)
    success_rate: f64,

    /// Average quality score (0-5)
    avg_quality_score: f64,

    /// Last updated
    last_updated: DateTime<Utc>,

    /// Sample size for statistics
    sample_size: usize,
}

/// A point in the objective space (cost, latency, quality)
#[derive(Debug, Clone)]
struct ObjectivePoint {
    model: String,
    cost: f64,      // Lower is better
    latency: f64,   // Lower is better
    quality: f64,   // Higher is better (we'll negate for dominance)
}

/// Record of a decision for learning
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionRecord {
    decision_id: String,
    from_model: String,
    to_model: String,
    timestamp: DateTime<Utc>,
    expected_impact: ExpectedImpact,
    actual_impact: Option<super::types::ActualImpact>,
    success: bool,
}

/// Strategy statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StrategyStats {
    total_evaluations: u64,
    total_decisions: u64,
    successful_switches: u64,
    failed_switches: u64,
    total_cost_savings: f64,
    total_latency_improvement: f64,
    total_quality_improvement: f64,
}

impl ModelSelectionStrategy {
    /// Create a new model selection strategy
    pub fn new(config: ModelSelectionConfig) -> Self {
        Self {
            config,
            model_performance: HashMap::new(),
            current_model: None,
            last_evaluation: None,
            decision_history: Vec::new(),
            stats: StrategyStats::default(),
        }
    }

    /// Calculate Pareto frontier from a set of objective points
    ///
    /// A point is on the Pareto frontier if it is not dominated by any other point.
    /// Point A dominates point B if A is better or equal in all objectives and
    /// strictly better in at least one objective.
    fn calculate_pareto_frontier(&self, points: &[ObjectivePoint]) -> Vec<ObjectivePoint> {
        let mut frontier = Vec::new();

        for candidate in points {
            let mut is_dominated = false;

            // Check if this candidate is dominated by any other point
            for other in points {
                if self.dominates(other, candidate) {
                    is_dominated = true;
                    break;
                }
            }

            if !is_dominated {
                frontier.push(candidate.clone());
            }
        }

        frontier
    }

    /// Check if point A dominates point B
    ///
    /// A dominates B if:
    /// - A.cost <= B.cost AND A.latency <= B.latency AND A.quality >= B.quality
    /// - At least one of the above is strictly better
    fn dominates(&self, a: &ObjectivePoint, b: &ObjectivePoint) -> bool {
        // Skip self-comparison
        if a.model == b.model {
            return false;
        }

        let cost_better = a.cost <= b.cost;
        let latency_better = a.latency <= b.latency;
        let quality_better = a.quality >= b.quality;

        let cost_strictly_better = a.cost < b.cost;
        let latency_strictly_better = a.latency < b.latency;
        let quality_strictly_better = a.quality > b.quality;

        // A dominates B if it's better or equal in all dimensions
        // and strictly better in at least one
        cost_better
            && latency_better
            && quality_better
            && (cost_strictly_better || latency_strictly_better || quality_strictly_better)
    }

    /// Score a model based on weighted objectives
    fn score_model(&self, point: &ObjectivePoint) -> f64 {
        // Normalize metrics (assuming reasonable ranges)
        let cost_norm = (100.0 - point.cost.min(100.0)) / 100.0; // 0-100 USD range
        let latency_norm = (5000.0 - point.latency.min(5000.0)) / 5000.0; // 0-5000ms range
        let quality_norm = point.quality / 5.0; // 0-5 range

        // Weighted sum
        self.config.cost_weight * cost_norm
            + self.config.performance_weight * latency_norm
            + self.config.quality_weight * quality_norm
    }

    /// Select the best model from the Pareto frontier
    fn select_best_model(&self, frontier: &[ObjectivePoint]) -> Option<ObjectivePoint> {
        if frontier.is_empty() {
            return None;
        }

        // Score each point and select the highest
        frontier
            .iter()
            .max_by(|a, b| {
                self.score_model(a)
                    .partial_cmp(&self.score_model(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Extract model performance from decision input
    fn extract_model_performance(&self, input: &DecisionInput) -> Vec<ObjectivePoint> {
        let mut points = Vec::new();

        // Get performance metrics from insights and recommendations
        for report in &input.analysis_reports {
            // Try to extract model metrics from insights since metadata field doesn't exist
            // This is a placeholder - actual implementation would extract from insights
            if false {
                let model_metrics = serde_json::Value::Null;
                if let Ok(metrics) =
                    serde_json::from_value::<HashMap<String, ModelMetrics>>(model_metrics.clone())
                {
                    for (model, perf) in metrics {
                        points.push(ObjectivePoint {
                            model,
                            cost: perf.avg_cost_per_request,
                            latency: perf.avg_latency_ms,
                            quality: perf.avg_quality_score,
                        });
                    }
                }
            }
        }

        // If no metrics from reports, use stored performance data
        if points.is_empty() {
            for (model, perf) in &self.model_performance {
                points.push(ObjectivePoint {
                    model: model.clone(),
                    cost: perf.avg_cost_per_request,
                    latency: perf.avg_latency_ms,
                    quality: perf.avg_quality_score,
                });
            }
        }

        // Add current system metrics as the baseline
        if let Some(current_model) = &self.current_model {
            points.push(ObjectivePoint {
                model: current_model.clone(),
                cost: input.current_metrics.daily_cost_usd / input.current_metrics.request_count as f64,
                latency: input.current_metrics.avg_latency_ms,
                quality: input.current_metrics.avg_rating.unwrap_or(3.5),
            });
        }

        points
    }

    /// Perform safety checks for a model switch decision
    fn safety_checks(
        &self,
        current: &ObjectivePoint,
        target: &ObjectivePoint,
        criteria: &DecisionCriteria,
    ) -> Vec<SafetyCheck> {
        let mut checks = Vec::new();
        let now = Utc::now();

        // Cost check
        let cost_increase = target.cost - current.cost;
        let cost_increase_pct = (cost_increase / current.cost) * 100.0;
        let cost_passed = cost_increase_pct <= self.config.max_cost_increase_pct;

        checks.push(SafetyCheck {
            name: "cost_check".to_string(),
            passed: cost_passed,
            details: format!(
                "Cost change: {:.2}% ({:.4} -> {:.4} USD/req). Max allowed: {:.2}%",
                cost_increase_pct,
                current.cost,
                target.cost,
                self.config.max_cost_increase_pct
            ),
            timestamp: now,
        });

        // Latency check
        if let Some(max_latency_increase) = criteria.max_latency_increase_ms {
            let latency_increase = target.latency - current.latency;
            let latency_passed = latency_increase <= max_latency_increase;

            checks.push(SafetyCheck {
                name: "latency_check".to_string(),
                passed: latency_passed,
                details: format!(
                    "Latency change: {:.2}ms ({:.2} -> {:.2}ms). Max allowed: {:.2}ms",
                    latency_increase, current.latency, target.latency, max_latency_increase
                ),
                timestamp: now,
            });
        }

        // Quality check
        if let Some(min_quality) = criteria.min_quality_score {
            let quality_passed = target.quality >= min_quality;

            checks.push(SafetyCheck {
                name: "quality_check".to_string(),
                passed: quality_passed,
                details: format!(
                    "Quality: {:.2} (current: {:.2}). Minimum: {:.2}",
                    target.quality, current.quality, min_quality
                ),
                timestamp: now,
            });
        }

        // Minimum evaluation period check
        let eval_period_passed = if let Some(last_eval) = self.last_evaluation {
            let elapsed = Utc::now() - last_eval;
            elapsed.to_std().unwrap_or(Duration::from_secs(0)) >= self.config.min_evaluation_period
        } else {
            true
        };

        checks.push(SafetyCheck {
            name: "evaluation_period_check".to_string(),
            passed: eval_period_passed,
            details: format!(
                "Minimum evaluation period: {:?}. Last evaluation: {:?}",
                self.config.min_evaluation_period, self.last_evaluation
            ),
            timestamp: now,
        });

        checks
    }

    /// Generate a rollback plan for a model switch
    fn generate_rollback_plan(&self, from_model: &str, to_model: &str) -> RollbackPlan {
        RollbackPlan {
            description: format!("Rollback from {} to {} if metrics degrade", to_model, from_model),
            revert_changes: vec![ConfigChange {
                config_type: ConfigType::Model,
                path: "model.name".to_string(),
                old_value: Some(serde_json::json!(to_model)),
                new_value: serde_json::json!(from_model),
                description: format!("Revert model selection from {} to {}", to_model, from_model),
            }],
            trigger_conditions: vec![
                RollbackCondition {
                    metric: "error_rate_pct".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 10.0,
                    description: "Error rate exceeds 10%".to_string(),
                },
                RollbackCondition {
                    metric: "p95_latency_ms".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: 2000.0,
                    description: "P95 latency exceeds 2000ms".to_string(),
                },
                RollbackCondition {
                    metric: "avg_rating".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: 3.0,
                    description: "Average rating drops below 3.0".to_string(),
                },
            ],
            max_duration: Duration::from_secs(3600), // 1 hour
        }
    }

    /// Calculate confidence based on sample size and variance
    fn calculate_confidence(&self, perf: &ModelPerformance) -> f64 {
        // Base confidence on sample size
        let sample_confidence = (perf.sample_size as f64 / 1000.0).min(1.0);

        // Adjust for success rate
        let success_confidence = perf.success_rate;

        // Combined confidence
        (sample_confidence * 0.5 + success_confidence * 0.5).min(1.0)
    }

    /// Update model performance from decision input
    fn update_model_performance(&mut self, input: &DecisionInput) {
        // Extract current model performance
        if let Some(current_model) = &self.current_model {
            let perf = self
                .model_performance
                .entry(current_model.clone())
                .or_insert_with(|| ModelPerformance {
                    model: current_model.clone(),
                    request_count: 0,
                    avg_latency_ms: 0.0,
                    p95_latency_ms: 0.0,
                    p99_latency_ms: 0.0,
                    avg_cost_per_request: 0.0,
                    total_cost: 0.0,
                    success_rate: 0.0,
                    avg_quality_score: 0.0,
                    last_updated: Utc::now(),
                    sample_size: 0,
                });

            // Update with current metrics
            perf.request_count = input.current_metrics.request_count;
            perf.avg_latency_ms = input.current_metrics.avg_latency_ms;
            perf.p95_latency_ms = input.current_metrics.p95_latency_ms;
            perf.p99_latency_ms = input.current_metrics.p99_latency_ms;
            perf.avg_cost_per_request = if input.current_metrics.request_count > 0 {
                input.current_metrics.daily_cost_usd / input.current_metrics.request_count as f64
            } else {
                0.0
            };
            perf.total_cost = input.current_metrics.total_cost_usd;
            perf.success_rate = input.current_metrics.success_rate_pct / 100.0;
            perf.avg_quality_score = input.current_metrics.avg_rating.unwrap_or(3.5);
            perf.last_updated = Utc::now();
            perf.sample_size = input.current_metrics.request_count as usize;
        }
    }
}

/// Helper struct for deserializing model metrics
#[derive(Debug, Clone, Deserialize)]
struct ModelMetrics {
    avg_cost_per_request: f64,
    avg_latency_ms: f64,
    avg_quality_score: f64,
}

#[async_trait]
impl OptimizationStrategy for ModelSelectionStrategy {
    fn name(&self) -> &str {
        "model_selection"
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_applicable(&self, input: &DecisionInput) -> bool {
        // Strategy is applicable if:
        // 1. We have sufficient metrics
        // 2. Multiple models are available
        // 3. Enough time has passed since last evaluation

        if !self.config.enabled {
            return false;
        }

        // Check if we have metrics
        let has_metrics = input.current_metrics.request_count > 100;

        // Check evaluation period
        let eval_period_ok = if let Some(last_eval) = self.last_evaluation {
            let elapsed = Utc::now() - last_eval;
            elapsed.to_std().unwrap_or(Duration::from_secs(0)) >= self.config.min_evaluation_period
        } else {
            true
        };

        has_metrics && eval_period_ok
    }

    async fn evaluate(
        &self,
        input: &DecisionInput,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Vec<Decision>> {
        let mut decisions = Vec::new();

        // Extract model performance data
        let points = self.extract_model_performance(input);

        if points.is_empty() {
            return Ok(decisions);
        }

        // Calculate Pareto frontier
        let frontier = self.calculate_pareto_frontier(&points);

        if frontier.is_empty() {
            return Ok(decisions);
        }

        // Select the best model from the frontier
        let best_model = self
            .select_best_model(&frontier)
            .ok_or_else(|| DecisionError::InsufficientData("No suitable model found".to_string()))?;

        // Get current model
        let current_model = if let Some(current) = &self.current_model {
            points
                .iter()
                .find(|p| p.model == *current)
                .cloned()
                .unwrap_or_else(|| ObjectivePoint {
                    model: current.clone(),
                    cost: input.current_metrics.daily_cost_usd
                        / input.current_metrics.request_count as f64,
                    latency: input.current_metrics.avg_latency_ms,
                    quality: input.current_metrics.avg_rating.unwrap_or(3.5),
                })
        } else {
            // No current model set, use the first point as baseline
            points[0].clone()
        };

        // Check if switching is beneficial
        if best_model.model == current_model.model {
            // Already using the best model
            return Ok(decisions);
        }

        // Perform safety checks
        let safety_checks = self.safety_checks(&current_model, &best_model, criteria);

        // Check if all required safety checks passed
        let all_passed = safety_checks.iter().all(|check| check.passed);

        if !all_passed {
            // Safety checks failed, don't make a decision
            return Ok(decisions);
        }

        // Calculate expected impact
        let cost_change = best_model.cost - current_model.cost;
        let latency_change = best_model.latency - current_model.latency;
        let quality_change = best_model.quality - current_model.quality;

        let expected_impact = ExpectedImpact {
            cost_change_usd: Some(cost_change * input.current_metrics.request_count as f64),
            latency_change_ms: Some(latency_change),
            quality_change: Some(quality_change),
            throughput_change_rps: None,
            success_rate_change_pct: None,
            time_to_impact: Duration::from_secs(300), // 5 minutes
            confidence: self.config.min_confidence,
        };

        // Generate rollback plan
        let rollback_plan = self.generate_rollback_plan(&current_model.model, &best_model.model);

        // Build justification
        let justification = format!(
            "Pareto optimization recommends switching from {} to {}. \
             Expected impact: cost {}{:.2}%, latency {}{:.2}ms, quality {}{:.2}. \
             Model {} is on the Pareto frontier and scores {:.3} (weighted by cost={:.1}, perf={:.1}, quality={:.1}). \
             This model is not dominated by any other model in cost-latency-quality space.",
            current_model.model,
            best_model.model,
            if cost_change > 0.0 { "+" } else { "" },
            (cost_change / current_model.cost) * 100.0,
            if latency_change > 0.0 { "+" } else { "" },
            latency_change,
            if quality_change > 0.0 { "+" } else { "" },
            quality_change,
            best_model.model,
            self.score_model(&best_model),
            self.config.cost_weight,
            self.config.performance_weight,
            self.config.quality_weight
        );

        // Create the decision
        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: self.name().to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: self.config.min_confidence,
            expected_impact,
            config_changes: vec![ConfigChange {
                config_type: ConfigType::Model,
                path: "model.name".to_string(),
                old_value: Some(serde_json::json!(current_model.model)),
                new_value: serde_json::json!(best_model.model),
                description: format!(
                    "Switch model from {} to {} based on Pareto optimization",
                    current_model.model, best_model.model
                ),
            }],
            justification,
            related_insights: input
                .insights
                .iter()
                .filter(|i| i.analyzer == "cost" || i.analyzer == "performance" || i.analyzer == "quality")
                .map(|i| i.id.clone())
                .collect(),
            related_recommendations: input
                .recommendations
                .iter()
                .filter(|r| r.tags.contains(&"optimization".to_string()))
                .map(|r| r.id.clone())
                .collect(),
            priority: self.priority(),
            requires_approval: true,
            safety_checks,
            rollback_plan: Some(rollback_plan),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("from_model".to_string(), serde_json::json!(current_model.model));
                meta.insert("to_model".to_string(), serde_json::json!(best_model.model));
                meta.insert("pareto_frontier_size".to_string(), serde_json::json!(frontier.len()));
                meta.insert(
                    "score".to_string(),
                    serde_json::json!(self.score_model(&best_model)),
                );
                meta
            },
        };

        decisions.push(decision);

        Ok(decisions)
    }

    async fn validate(&self, decision: &Decision, criteria: &DecisionCriteria) -> DecisionResult<()> {
        // Validate decision type
        if decision.decision_type != DecisionType::ModelSwitch {
            return Err(DecisionError::ValidationFailed(
                "Invalid decision type for model selection strategy".to_string(),
            ));
        }

        // Validate confidence
        if decision.confidence < criteria.min_confidence {
            return Err(DecisionError::ValidationFailed(format!(
                "Confidence {:.2} is below minimum {:.2}",
                decision.confidence, criteria.min_confidence
            )));
        }

        // Validate all required safety checks passed
        for check_name in &criteria.required_safety_checks {
            let check = decision
                .safety_checks
                .iter()
                .find(|c| c.name == *check_name)
                .ok_or_else(|| {
                    DecisionError::ValidationFailed(format!("Missing required safety check: {}", check_name))
                })?;

            if !check.passed {
                return Err(DecisionError::ValidationFailed(format!(
                    "Safety check '{}' failed: {}",
                    check_name, check.details
                )));
            }
        }

        // Validate config changes
        if decision.config_changes.is_empty() {
            return Err(DecisionError::ValidationFailed(
                "No configuration changes specified".to_string(),
            ));
        }

        // Validate that we have model change
        let has_model_change = decision
            .config_changes
            .iter()
            .any(|c| c.config_type == ConfigType::Model && c.path == "model.name");

        if !has_model_change {
            return Err(DecisionError::ValidationFailed(
                "No model configuration change found".to_string(),
            ));
        }

        // Validate cost increase
        if let Some(max_cost_increase) = criteria.max_cost_increase_usd {
            if let Some(cost_change) = decision.expected_impact.cost_change_usd {
                if cost_change > max_cost_increase {
                    return Err(DecisionError::ConstraintViolation(format!(
                        "Cost increase {:.2} USD exceeds maximum {:.2} USD",
                        cost_change, max_cost_increase
                    )));
                }
            }
        }

        // Validate latency increase
        if let Some(max_latency_increase) = criteria.max_latency_increase_ms {
            if let Some(latency_change) = decision.expected_impact.latency_change_ms {
                if latency_change > max_latency_increase {
                    return Err(DecisionError::ConstraintViolation(format!(
                        "Latency increase {:.2}ms exceeds maximum {:.2}ms",
                        latency_change, max_latency_increase
                    )));
                }
            }
        }

        Ok(())
    }

    async fn learn(&mut self, decision: &Decision, outcome: &DecisionOutcome) -> DecisionResult<()> {
        // Update statistics
        self.stats.total_decisions += 1;

        if outcome.success {
            self.stats.successful_switches += 1;

            // Update model performance
            if let Some(actual_impact) = &outcome.actual_impact {
                if let Some(cost_change) = actual_impact.cost_change_usd.into() {
                    self.stats.total_cost_savings -= cost_change;
                }
                self.stats.total_latency_improvement -= actual_impact.latency_change_ms;
                self.stats.total_quality_improvement += actual_impact.quality_change;
            }

            // Update current model
            if let Some(to_model) = decision.metadata.get("to_model") {
                if let Some(model_name) = to_model.as_str() {
                    self.current_model = Some(model_name.to_string());
                }
            }

            // Record decision
            if let (Some(from_model), Some(to_model)) = (
                decision.metadata.get("from_model").and_then(|v| v.as_str()),
                decision.metadata.get("to_model").and_then(|v| v.as_str()),
            ) {
                self.decision_history.push(DecisionRecord {
                    decision_id: decision.id.clone(),
                    from_model: from_model.to_string(),
                    to_model: to_model.to_string(),
                    timestamp: decision.timestamp,
                    expected_impact: decision.expected_impact.clone(),
                    actual_impact: outcome.actual_impact.clone(),
                    success: true,
                });
            }
        } else {
            self.stats.failed_switches += 1;

            // Record failed decision
            if let (Some(from_model), Some(to_model)) = (
                decision.metadata.get("from_model").and_then(|v| v.as_str()),
                decision.metadata.get("to_model").and_then(|v| v.as_str()),
            ) {
                self.decision_history.push(DecisionRecord {
                    decision_id: decision.id.clone(),
                    from_model: from_model.to_string(),
                    to_model: to_model.to_string(),
                    timestamp: decision.timestamp,
                    expected_impact: decision.expected_impact.clone(),
                    actual_impact: outcome.actual_impact.clone(),
                    success: false,
                });
            }
        }

        // Update last evaluation time
        self.last_evaluation = Some(Utc::now());

        // Trim decision history if needed
        if self.decision_history.len() > 1000 {
            self.decision_history.drain(0..100);
        }

        Ok(())
    }

    fn get_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "total_evaluations": self.stats.total_evaluations,
            "total_decisions": self.stats.total_decisions,
            "successful_switches": self.stats.successful_switches,
            "failed_switches": self.stats.failed_switches,
            "success_rate": if self.stats.total_decisions > 0 {
                self.stats.successful_switches as f64 / self.stats.total_decisions as f64
            } else {
                0.0
            },
            "total_cost_savings": self.stats.total_cost_savings,
            "total_latency_improvement": self.stats.total_latency_improvement,
            "total_quality_improvement": self.stats.total_quality_improvement,
            "current_model": self.current_model,
            "tracked_models": self.model_performance.len(),
            "last_evaluation": self.last_evaluation,
            "decision_history_size": self.decision_history.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ModelSelectionConfig {
        ModelSelectionConfig {
            enabled: true,
            priority: 100,
            min_confidence: 0.7,
            cost_weight: 0.4,
            performance_weight: 0.3,
            quality_weight: 0.3,
            available_models: vec![
                "gpt-4".to_string(),
                "gpt-3.5-turbo".to_string(),
                "claude-3-opus".to_string(),
            ],
            min_evaluation_period: Duration::from_secs(60),
            max_cost_increase_pct: 50.0,
        }
    }

    fn create_test_input(current_model: &str, request_count: u64) -> DecisionInput {
        DecisionInput {
            timestamp: Utc::now(),
            analysis_reports: vec![],
            insights: vec![],
            recommendations: vec![],
            current_metrics: super::super::types::SystemMetrics {
                avg_latency_ms: 500.0,
                p95_latency_ms: 800.0,
                p99_latency_ms: 1200.0,
                success_rate_pct: 99.0,
                error_rate_pct: 1.0,
                throughput_rps: 100.0,
                total_cost_usd: 100.0,
                daily_cost_usd: 100.0,
                avg_rating: Some(4.0),
                cache_hit_rate_pct: Some(50.0),
                request_count,
                timestamp: Utc::now(),
            },
            context: {
                let mut ctx = HashMap::new();
                ctx.insert("current_model".to_string(), serde_json::json!(current_model));
                ctx
            },
        }
    }

    fn create_test_criteria() -> DecisionCriteria {
        DecisionCriteria {
            min_confidence: 0.7,
            max_cost_increase_usd: Some(100.0),
            max_latency_increase_ms: Some(500.0),
            min_quality_score: Some(3.5),
            required_safety_checks: vec![
                "cost_check".to_string(),
                "latency_check".to_string(),
                "quality_check".to_string(),
            ],
            allow_breaking_changes: false,
            max_concurrent_decisions: 3,
            enabled_strategies: vec!["model_selection".to_string()],
            strategy_priorities: {
                let mut priorities = HashMap::new();
                priorities.insert("model_selection".to_string(), 100);
                priorities
            },
        }
    }

    #[test]
    fn test_strategy_creation() {
        let config = create_test_config();
        let strategy = ModelSelectionStrategy::new(config);

        assert_eq!(strategy.name(), "model_selection");
        assert_eq!(strategy.priority(), 100);
        assert!(strategy.model_performance.is_empty());
        assert!(strategy.current_model.is_none());
    }

    #[test]
    fn test_dominance_check() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let point_a = ObjectivePoint {
            model: "model_a".to_string(),
            cost: 10.0,
            latency: 100.0,
            quality: 4.5,
        };

        let point_b = ObjectivePoint {
            model: "model_b".to_string(),
            cost: 15.0,
            latency: 120.0,
            quality: 4.0,
        };

        // A dominates B (lower cost, lower latency, higher quality)
        assert!(strategy.dominates(&point_a, &point_b));
        assert!(!strategy.dominates(&point_b, &point_a));
    }

    #[test]
    fn test_dominance_partial() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let point_a = ObjectivePoint {
            model: "model_a".to_string(),
            cost: 10.0,
            latency: 100.0,
            quality: 4.0,
        };

        let point_b = ObjectivePoint {
            model: "model_b".to_string(),
            cost: 15.0,
            latency: 80.0,
            quality: 4.5,
        };

        // Neither dominates (A is cheaper, B is faster and better quality)
        assert!(!strategy.dominates(&point_a, &point_b));
        assert!(!strategy.dominates(&point_b, &point_a));
    }

    #[test]
    fn test_pareto_frontier_single_point() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let points = vec![ObjectivePoint {
            model: "model_a".to_string(),
            cost: 10.0,
            latency: 100.0,
            quality: 4.5,
        }];

        let frontier = strategy.calculate_pareto_frontier(&points);
        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier[0].model, "model_a");
    }

    #[test]
    fn test_pareto_frontier_multiple_points() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let points = vec![
            ObjectivePoint {
                model: "cheap_slow".to_string(),
                cost: 5.0,
                latency: 500.0,
                quality: 3.5,
            },
            ObjectivePoint {
                model: "expensive_fast".to_string(),
                cost: 20.0,
                latency: 100.0,
                quality: 4.5,
            },
            ObjectivePoint {
                model: "balanced".to_string(),
                cost: 10.0,
                latency: 200.0,
                quality: 4.0,
            },
            ObjectivePoint {
                model: "dominated".to_string(),
                cost: 15.0,
                latency: 300.0,
                quality: 3.8,
            },
        ];

        let frontier = strategy.calculate_pareto_frontier(&points);

        // Should have 3 points on frontier (all except dominated)
        assert_eq!(frontier.len(), 3);

        let frontier_models: Vec<String> = frontier.iter().map(|p| p.model.clone()).collect();
        assert!(frontier_models.contains(&"cheap_slow".to_string()));
        assert!(frontier_models.contains(&"expensive_fast".to_string()));
        assert!(frontier_models.contains(&"balanced".to_string()));
        assert!(!frontier_models.contains(&"dominated".to_string()));
    }

    #[test]
    fn test_model_scoring() {
        let mut config = create_test_config();
        config.cost_weight = 0.5;
        config.performance_weight = 0.3;
        config.quality_weight = 0.2;
        let strategy = ModelSelectionStrategy::new(config);

        let point = ObjectivePoint {
            model: "test_model".to_string(),
            cost: 10.0,
            latency: 200.0,
            quality: 4.0,
        };

        let score = strategy.score_model(&point);
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_best_model_selection() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let frontier = vec![
            ObjectivePoint {
                model: "model_a".to_string(),
                cost: 5.0,
                latency: 500.0,
                quality: 3.5,
            },
            ObjectivePoint {
                model: "model_b".to_string(),
                cost: 20.0,
                latency: 100.0,
                quality: 4.5,
            },
            ObjectivePoint {
                model: "model_c".to_string(),
                cost: 10.0,
                latency: 200.0,
                quality: 4.0,
            },
        ];

        let best = strategy.select_best_model(&frontier);
        assert!(best.is_some());

        // With default weights (0.4, 0.3, 0.3), model_c should score highest
        let best = best.unwrap();
        assert_eq!(best.model, "model_c");
    }

    #[test]
    fn test_safety_checks_all_pass() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let current = ObjectivePoint {
            model: "current".to_string(),
            cost: 10.0,
            latency: 200.0,
            quality: 4.0,
        };

        let target = ObjectivePoint {
            model: "target".to_string(),
            cost: 12.0, // 20% increase (within 50% limit)
            latency: 180.0,
            quality: 4.2,
        };

        let criteria = create_test_criteria();
        let checks = strategy.safety_checks(&current, &target, &criteria);

        // All checks should pass
        assert!(checks.iter().all(|c| c.passed));
    }

    #[test]
    fn test_safety_checks_cost_fail() {
        let strategy = ModelSelectionStrategy::new(create_test_config());

        let current = ObjectivePoint {
            model: "current".to_string(),
            cost: 10.0,
            latency: 200.0,
            quality: 4.0,
        };

        let target = ObjectivePoint {
            model: "target".to_string(),
            cost: 20.0, // 100% increase (exceeds 50% limit)
            latency: 180.0,
            quality: 4.2,
        };

        let criteria = create_test_criteria();
        let checks = strategy.safety_checks(&current, &target, &criteria);

        // Cost check should fail
        let cost_check = checks.iter().find(|c| c.name == "cost_check").unwrap();
        assert!(!cost_check.passed);
    }

    #[test]
    fn test_is_applicable_insufficient_metrics() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let input = create_test_input("gpt-4", 50); // Only 50 requests

        assert!(!strategy.is_applicable(&input));
    }

    #[test]
    fn test_is_applicable_sufficient_metrics() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let input = create_test_input("gpt-4", 1000); // 1000 requests

        assert!(strategy.is_applicable(&input));
    }

    #[test]
    fn test_is_applicable_disabled_strategy() {
        let mut config = create_test_config();
        config.enabled = false;
        let strategy = ModelSelectionStrategy::new(config);
        let input = create_test_input("gpt-4", 1000);

        assert!(!strategy.is_applicable(&input));
    }

    #[tokio::test]
    async fn test_validate_valid_decision() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let criteria = create_test_criteria();

        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "model_selection".to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(10.0),
                latency_change_ms: Some(-50.0),
                quality_change: Some(0.2),
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![ConfigChange {
                config_type: ConfigType::Model,
                path: "model.name".to_string(),
                old_value: Some(serde_json::json!("gpt-4")),
                new_value: serde_json::json!("claude-3-opus"),
                description: "Switch model".to_string(),
            }],
            justification: "Test justification".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 100,
            requires_approval: true,
            safety_checks: vec![
                SafetyCheck {
                    name: "cost_check".to_string(),
                    passed: true,
                    details: "Cost OK".to_string(),
                    timestamp: Utc::now(),
                },
                SafetyCheck {
                    name: "latency_check".to_string(),
                    passed: true,
                    details: "Latency OK".to_string(),
                    timestamp: Utc::now(),
                },
                SafetyCheck {
                    name: "quality_check".to_string(),
                    passed: true,
                    details: "Quality OK".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let result = strategy.validate(&decision, &criteria).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_wrong_decision_type() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let criteria = create_test_criteria();

        let mut decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "model_selection".to_string(),
            decision_type: DecisionType::EnableCaching, // Wrong type
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(10.0),
                latency_change_ms: Some(-50.0),
                quality_change: Some(0.2),
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 100,
            requires_approval: true,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let result = strategy.validate(&decision, &criteria).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_low_confidence() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let criteria = create_test_criteria();

        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "model_selection".to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: 0.5, // Below minimum
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(10.0),
                latency_change_ms: Some(-50.0),
                quality_change: Some(0.2),
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.5,
            },
            config_changes: vec![ConfigChange {
                config_type: ConfigType::Model,
                path: "model.name".to_string(),
                old_value: Some(serde_json::json!("gpt-4")),
                new_value: serde_json::json!("claude-3-opus"),
                description: "Switch model".to_string(),
            }],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 100,
            requires_approval: true,
            safety_checks: vec![
                SafetyCheck {
                    name: "cost_check".to_string(),
                    passed: true,
                    details: "OK".to_string(),
                    timestamp: Utc::now(),
                },
                SafetyCheck {
                    name: "latency_check".to_string(),
                    passed: true,
                    details: "OK".to_string(),
                    timestamp: Utc::now(),
                },
                SafetyCheck {
                    name: "quality_check".to_string(),
                    passed: true,
                    details: "OK".to_string(),
                    timestamp: Utc::now(),
                },
            ],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let result = strategy.validate(&decision, &criteria).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_learn_successful_outcome() {
        let mut strategy = ModelSelectionStrategy::new(create_test_config());

        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "model_selection".to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-10.0),
                latency_change_ms: Some(-50.0),
                quality_change: Some(0.2),
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 100,
            requires_approval: true,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("from_model".to_string(), serde_json::json!("gpt-4"));
                meta.insert("to_model".to_string(), serde_json::json!("claude-3-opus"));
                meta
            },
        };

        let outcome = DecisionOutcome {
            decision_id: decision.id.clone(),
            execution_time: Utc::now(),
            success: true,
            error: None,
            actual_impact: Some(super::super::types::ActualImpact {
                cost_change_usd: -12.0,
                latency_change_ms: -45.0,
                quality_change: 0.25,
                throughput_change_rps: 0.0,
                success_rate_change_pct: 0.0,
                time_to_impact: Duration::from_secs(300),
                variance_from_expected: 0.1,
            }),
            rolled_back: false,
            rollback_reason: None,
            metrics_before: create_test_input("gpt-4", 1000).current_metrics,
            metrics_after: Some(create_test_input("claude-3-opus", 1000).current_metrics),
            duration: Duration::from_secs(3600),
        };

        let result = strategy.learn(&decision, &outcome).await;
        assert!(result.is_ok());
        assert_eq!(strategy.stats.successful_switches, 1);
        assert_eq!(strategy.stats.total_decisions, 1);
        assert_eq!(strategy.current_model, Some("claude-3-opus".to_string()));
    }

    #[tokio::test]
    async fn test_learn_failed_outcome() {
        let mut strategy = ModelSelectionStrategy::new(create_test_config());

        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "model_selection".to_string(),
            decision_type: DecisionType::ModelSwitch,
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(-10.0),
                latency_change_ms: Some(-50.0),
                quality_change: Some(0.2),
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: Duration::from_secs(300),
                confidence: 0.8,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 100,
            requires_approval: true,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("from_model".to_string(), serde_json::json!("gpt-4"));
                meta.insert("to_model".to_string(), serde_json::json!("claude-3-opus"));
                meta
            },
        };

        let outcome = DecisionOutcome {
            decision_id: decision.id.clone(),
            execution_time: Utc::now(),
            success: false,
            error: Some("Model switch failed".to_string()),
            actual_impact: None,
            rolled_back: true,
            rollback_reason: Some("Error rate exceeded threshold".to_string()),
            metrics_before: create_test_input("gpt-4", 1000).current_metrics,
            metrics_after: None,
            duration: Duration::from_secs(600),
        };

        let result = strategy.learn(&decision, &outcome).await;
        assert!(result.is_ok());
        assert_eq!(strategy.stats.failed_switches, 1);
        assert_eq!(strategy.stats.total_decisions, 1);
    }

    #[test]
    fn test_get_stats() {
        let mut strategy = ModelSelectionStrategy::new(create_test_config());
        strategy.stats.total_decisions = 10;
        strategy.stats.successful_switches = 8;
        strategy.stats.failed_switches = 2;
        strategy.stats.total_cost_savings = 150.0;

        let stats = strategy.get_stats();

        assert_eq!(stats["total_decisions"], 10);
        assert_eq!(stats["successful_switches"], 8);
        assert_eq!(stats["failed_switches"], 2);
        assert_eq!(stats["success_rate"], 0.8);
        assert_eq!(stats["total_cost_savings"], 150.0);
    }

    #[test]
    fn test_rollback_plan_generation() {
        let strategy = ModelSelectionStrategy::new(create_test_config());
        let plan = strategy.generate_rollback_plan("gpt-4", "claude-3-opus");

        assert!(plan.description.contains("gpt-4"));
        assert!(plan.description.contains("claude-3-opus"));
        assert_eq!(plan.revert_changes.len(), 1);
        assert_eq!(plan.trigger_conditions.len(), 3);

        // Check trigger conditions
        let error_condition = plan
            .trigger_conditions
            .iter()
            .find(|c| c.metric == "error_rate_pct")
            .unwrap();
        assert_eq!(error_condition.operator, ComparisonOperator::GreaterThan);
        assert_eq!(error_condition.threshold, 10.0);
    }
}
