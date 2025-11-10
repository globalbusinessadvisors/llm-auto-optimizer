//! Prompt Optimization Strategy
//!
//! This strategy analyzes prompts for optimization opportunities, estimates token savings,
//! generates A/B test decisions for prompt variants, tracks quality metrics during tests,
//! and makes final rollout decisions based on results.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::config::PromptOptimizationConfig;
use super::error::{DecisionError, DecisionResult};
use super::traits::OptimizationStrategy;
use super::types::{
    ComparisonOperator, ConfigChange, ConfigType, Decision, DecisionCriteria, DecisionInput,
    DecisionOutcome, DecisionType, ExpectedImpact, RollbackCondition, RollbackPlan, SafetyCheck,
};

/// Prompt optimization strategy that reduces token usage while maintaining quality
pub struct PromptOptimizationStrategy {
    config: PromptOptimizationConfig,
    state: Arc<RwLock<StrategyState>>,
}

/// Internal state for the strategy
#[derive(Debug, Clone)]
struct StrategyState {
    /// Active A/B tests
    active_tests: HashMap<String, ABTest>,
    /// Completed tests
    completed_tests: Vec<ABTest>,
    /// Statistics
    stats: StrategyStats,
    /// Learning history
    learning_history: Vec<LearningRecord>,
}

/// An A/B test for prompt optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ABTest {
    /// Test ID
    id: String,
    /// Original prompt identifier
    original_prompt_id: String,
    /// Optimized prompt identifier
    optimized_prompt_id: String,
    /// Start time
    start_time: DateTime<Utc>,
    /// End time (planned)
    end_time: DateTime<Utc>,
    /// Traffic split (0-1, proportion going to optimized variant)
    traffic_split: f64,
    /// Expected token reduction percentage
    expected_token_reduction_pct: f64,
    /// Expected quality impact
    expected_quality_impact: f64,
    /// Observations collected
    observations: TestObservations,
    /// Test status
    status: TestStatus,
}

/// Status of an A/B test
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TestStatus {
    /// Test is running
    Running,
    /// Test completed successfully
    Completed,
    /// Test was stopped early (e.g., quality degradation)
    Stopped,
    /// Test rolled out (optimized variant won)
    RolledOut,
    /// Test rolled back (original variant better)
    RolledBack,
}

/// Observations collected during an A/B test
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TestObservations {
    /// Control group (original prompt) observations
    control: VariantObservations,
    /// Treatment group (optimized prompt) observations
    treatment: VariantObservations,
}

/// Observations for a single variant
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VariantObservations {
    /// Sample count
    sample_count: u64,
    /// Total tokens used
    total_tokens: u64,
    /// Average tokens per request
    avg_tokens: f64,
    /// Quality ratings (if available)
    quality_ratings: Vec<f64>,
    /// Average quality
    avg_quality: f64,
    /// Success rate
    success_rate: f64,
    /// Successful requests
    successful_requests: u64,
    /// Failed requests
    failed_requests: u64,
}

/// Strategy statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StrategyStats {
    /// Total optimizations evaluated
    total_evaluations: u64,
    /// Total A/B tests created
    total_tests_created: u64,
    /// Successful rollouts
    successful_rollouts: u64,
    /// Rollbacks due to quality degradation
    rollbacks_quality: u64,
    /// Rollbacks due to insufficient savings
    rollbacks_savings: u64,
    /// Total token savings achieved (cumulative)
    total_token_savings: u64,
    /// Total cost savings (USD)
    total_cost_savings_usd: f64,
}

/// Record of learning from a decision outcome
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LearningRecord {
    /// Timestamp
    timestamp: DateTime<Utc>,
    /// Decision ID
    decision_id: String,
    /// What was learned
    lesson: String,
    /// Confidence adjustment
    confidence_adjustment: f64,
    /// Metadata
    metadata: HashMap<String, serde_json::Value>,
}

/// Analysis result for a prompt
#[derive(Debug, Clone)]
struct PromptAnalysis {
    /// Identified optimization opportunities
    opportunities: Vec<OptimizationOpportunity>,
    /// Estimated token reduction
    estimated_token_reduction: u64,
    /// Estimated token reduction percentage
    estimated_token_reduction_pct: f64,
    /// Current average token count
    current_avg_tokens: f64,
    /// Confidence in the analysis
    confidence: f64,
}

/// An optimization opportunity in a prompt
#[derive(Debug, Clone)]
enum OptimizationOpportunity {
    /// Remove redundant phrases
    RemoveRedundancy { phrase: String, occurrences: usize },
    /// Simplify verbose instructions
    SimplifyInstructions { section: String, savings: u64 },
    /// Remove unnecessary examples
    RemoveExamples { count: usize, savings: u64 },
    /// Consolidate repetitive sections
    ConsolidateSections { sections: Vec<String>, savings: u64 },
    /// Use more concise language
    ConciseLanguage { before: String, after: String, savings: u64 },
}

impl PromptOptimizationStrategy {
    /// Create a new prompt optimization strategy
    pub fn new(config: PromptOptimizationConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(StrategyState {
                active_tests: HashMap::new(),
                completed_tests: Vec::new(),
                stats: StrategyStats::default(),
                learning_history: Vec::new(),
            })),
        }
    }

    /// Analyze a prompt for optimization opportunities
    async fn analyze_prompt(&self, input: &DecisionInput) -> DecisionResult<PromptAnalysis> {
        // Extract prompt information from recommendations
        let prompt_recommendations = input
            .recommendations
            .iter()
            .filter(|r| r.tags.contains(&"prompt".to_string()));

        let mut opportunities = Vec::new();
        let mut estimated_savings = 0u64;

        // Analyze for various optimization opportunities
        for rec in prompt_recommendations {
            // Check for redundancy patterns
            if let Some(redundancy) = self.detect_redundancy(rec) {
                estimated_savings += redundancy.1;
                opportunities.push(redundancy.0);
            }

            // Check for verbose instructions
            if let Some(verbose) = self.detect_verbose_instructions(rec) {
                estimated_savings += verbose.1;
                opportunities.push(verbose.0);
            }

            // Check for unnecessary examples
            if let Some(examples) = self.detect_unnecessary_examples(rec) {
                estimated_savings += examples.1;
                opportunities.push(examples.0);
            }

            // Check for repetitive sections
            if let Some(repetitive) = self.detect_repetitive_sections(rec) {
                estimated_savings += repetitive.1;
                opportunities.push(repetitive.0);
            }
        }

        // Get current token metrics
        let current_avg_tokens = input
            .current_metrics
            .request_count
            .max(1) as f64;

        let estimated_token_reduction_pct = if current_avg_tokens > 0.0 {
            (estimated_savings as f64 / current_avg_tokens) * 100.0
        } else {
            0.0
        };

        // Calculate confidence based on amount of data and clarity of opportunities
        let confidence = self.calculate_analysis_confidence(&opportunities, input);

        Ok(PromptAnalysis {
            opportunities,
            estimated_token_reduction: estimated_savings,
            estimated_token_reduction_pct,
            current_avg_tokens,
            confidence,
        })
    }

    /// Detect redundant phrases in prompts
    fn detect_redundancy(
        &self,
        _rec: &crate::analyzer::types::Recommendation,
    ) -> Option<(OptimizationOpportunity, u64)> {
        // Simplified detection - in production, this would use NLP analysis
        // Looking for repetitive phrases in the recommendation description
        let phrases = vec![
            ("please make sure to", 5),
            ("it is important that", 5),
            ("you should always", 4),
        ];

        for (phrase, token_cost) in phrases {
            // In a real implementation, we'd parse the actual prompt text
            // For now, return a sample opportunity
            return Some((
                OptimizationOpportunity::RemoveRedundancy {
                    phrase: phrase.to_string(),
                    occurrences: 1,
                },
                token_cost,
            ));
        }

        None
    }

    /// Detect verbose instructions
    fn detect_verbose_instructions(
        &self,
        _rec: &crate::analyzer::types::Recommendation,
    ) -> Option<(OptimizationOpportunity, u64)> {
        // Simplified detection
        Some((
            OptimizationOpportunity::SimplifyInstructions {
                section: "main_instructions".to_string(),
                savings: 15,
            },
            15,
        ))
    }

    /// Detect unnecessary examples
    fn detect_unnecessary_examples(
        &self,
        _rec: &crate::analyzer::types::Recommendation,
    ) -> Option<(OptimizationOpportunity, u64)> {
        // In production, would analyze if examples are actually helping quality
        Some((
            OptimizationOpportunity::RemoveExamples {
                count: 2,
                savings: 50,
            },
            50,
        ))
    }

    /// Detect repetitive sections
    fn detect_repetitive_sections(
        &self,
        _rec: &crate::analyzer::types::Recommendation,
    ) -> Option<(OptimizationOpportunity, u64)> {
        // Would use structural analysis to find repeated content
        Some((
            OptimizationOpportunity::ConsolidateSections {
                sections: vec!["intro".to_string(), "outro".to_string()],
                savings: 30,
            },
            30,
        ))
    }

    /// Calculate confidence in the analysis
    fn calculate_analysis_confidence(
        &self,
        opportunities: &[OptimizationOpportunity],
        input: &DecisionInput,
    ) -> f64 {
        // Base confidence from data volume
        let data_confidence = if input.current_metrics.request_count > 1000 {
            0.9
        } else if input.current_metrics.request_count > 100 {
            0.75
        } else {
            0.6
        };

        // Opportunity clarity confidence
        let opportunity_confidence = if opportunities.is_empty() {
            0.5
        } else if opportunities.len() >= 3 {
            0.9
        } else {
            0.7
        };

        // Combined confidence
        (data_confidence + opportunity_confidence) / 2.0
    }

    /// Estimate quality impact of optimization
    fn estimate_quality_impact(&self, analysis: &PromptAnalysis) -> f64 {
        // Conservative estimate - assumes some quality degradation
        let base_impact = -0.05; // -5% quality by default

        // Adjust based on type of optimizations
        let mut adjustment = 0.0;
        for opp in &analysis.opportunities {
            match opp {
                OptimizationOpportunity::RemoveRedundancy { .. } => {
                    adjustment += 0.01; // Removing redundancy might improve clarity
                }
                OptimizationOpportunity::SimplifyInstructions { .. } => {
                    adjustment -= 0.02; // Simplifying might lose nuance
                }
                OptimizationOpportunity::RemoveExamples { .. } => {
                    adjustment -= 0.03; // Removing examples likely reduces quality
                }
                OptimizationOpportunity::ConsolidateSections { .. } => {
                    adjustment += 0.005; // Consolidation might improve coherence
                }
                OptimizationOpportunity::ConciseLanguage { .. } => {
                    adjustment += 0.01; // Conciseness might help
                }
            }
        }

        base_impact + adjustment
    }

    /// Create an A/B test decision
    fn create_ab_test_decision(
        &self,
        analysis: PromptAnalysis,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Decision> {
        let test_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // Calculate expected impact
        let expected_quality_impact = self.estimate_quality_impact(&analysis);

        // Estimate cost savings based on token reduction
        // Assuming $0.002 per 1K tokens (rough average)
        let tokens_saved_per_day = analysis.estimated_token_reduction as f64 * 1000.0;
        let cost_savings_per_day = (tokens_saved_per_day / 1000.0) * 0.002;

        let expected_impact = ExpectedImpact {
            cost_change_usd: Some(-cost_savings_per_day),
            latency_change_ms: Some(0.0), // Prompt optimization shouldn't affect latency much
            quality_change: Some(expected_quality_impact),
            throughput_change_rps: None,
            success_rate_change_pct: None,
            time_to_impact: std::time::Duration::from_secs(3600), // 1 hour
            confidence: analysis.confidence,
        };

        // Safety checks
        let mut safety_checks = Vec::new();

        // Quality check
        let min_quality = criteria.min_quality_score.unwrap_or(3.5);
        let projected_quality = 4.0 + expected_quality_impact; // Assuming baseline of 4.0
        let quality_check_passed = projected_quality >= min_quality
            && expected_quality_impact.abs() <= (self.config.max_quality_degradation_pct / 100.0);

        safety_checks.push(SafetyCheck {
            name: "quality_check".to_string(),
            passed: quality_check_passed,
            details: format!(
                "Projected quality: {:.2}, Min required: {:.2}, Max degradation: {:.1}%",
                projected_quality, min_quality, self.config.max_quality_degradation_pct
            ),
            timestamp: now,
        });

        // Token reduction check
        let token_reduction_check_passed =
            analysis.estimated_token_reduction_pct >= self.config.min_token_reduction_pct;

        safety_checks.push(SafetyCheck {
            name: "token_reduction_check".to_string(),
            passed: token_reduction_check_passed,
            details: format!(
                "Estimated reduction: {:.1}%, Min required: {:.1}%",
                analysis.estimated_token_reduction_pct, self.config.min_token_reduction_pct
            ),
            timestamp: now,
        });

        // Confidence check
        let confidence_check_passed = analysis.confidence >= criteria.min_confidence;

        safety_checks.push(SafetyCheck {
            name: "confidence_check".to_string(),
            passed: confidence_check_passed,
            details: format!(
                "Analysis confidence: {:.2}, Min required: {:.2}",
                analysis.confidence, criteria.min_confidence
            ),
            timestamp: now,
        });

        // Overall safety check
        let all_checks_passed = safety_checks.iter().all(|c| c.passed);

        if !all_checks_passed {
            return Err(DecisionError::ValidationFailed(
                "Safety checks failed for prompt optimization".to_string(),
            ));
        }

        // Configuration changes
        let config_changes = vec![
            ConfigChange {
                config_type: ConfigType::Prompt,
                path: "prompt.optimization.enabled".to_string(),
                old_value: Some(serde_json::json!(false)),
                new_value: serde_json::json!(true),
                description: "Enable optimized prompt variant for A/B testing".to_string(),
            },
            ConfigChange {
                config_type: ConfigType::Prompt,
                path: "prompt.ab_test.traffic_split".to_string(),
                old_value: Some(serde_json::json!(0.0)),
                new_value: serde_json::json!(self.config.ab_test_traffic_split),
                description: format!(
                    "Route {:.0}% of traffic to optimized variant",
                    self.config.ab_test_traffic_split * 100.0
                ),
            },
        ];

        // Rollback plan
        let rollback_plan = RollbackPlan {
            description: "Rollback to original prompt if quality degrades".to_string(),
            revert_changes: vec![
                ConfigChange {
                    config_type: ConfigType::Prompt,
                    path: "prompt.optimization.enabled".to_string(),
                    old_value: Some(serde_json::json!(true)),
                    new_value: serde_json::json!(false),
                    description: "Disable optimized prompt variant".to_string(),
                },
                ConfigChange {
                    config_type: ConfigType::Prompt,
                    path: "prompt.ab_test.traffic_split".to_string(),
                    old_value: Some(serde_json::json!(self.config.ab_test_traffic_split)),
                    new_value: serde_json::json!(0.0),
                    description: "Stop routing traffic to optimized variant".to_string(),
                },
            ],
            trigger_conditions: vec![
                RollbackCondition {
                    metric: "quality_score".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: min_quality,
                    description: format!(
                        "Quality score drops below {:.2}",
                        min_quality
                    ),
                },
                RollbackCondition {
                    metric: "quality_degradation_pct".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    threshold: self.config.max_quality_degradation_pct,
                    description: format!(
                        "Quality degradation exceeds {:.1}%",
                        self.config.max_quality_degradation_pct
                    ),
                },
                RollbackCondition {
                    metric: "success_rate_pct".to_string(),
                    operator: ComparisonOperator::LessThan,
                    threshold: 95.0,
                    description: "Success rate drops below 95%".to_string(),
                },
            ],
            max_duration: std::time::Duration::from_secs(
                self.config.ab_test_duration.as_secs()
            ),
        };

        // Build justification
        let justification = format!(
            "Prompt optimization opportunity identified with {:.1}% token reduction potential. \
             {} optimization opportunities found. Expected cost savings: ${:.2}/day. \
             Quality impact: {:.1}%. A/B test will run for {} hours with {:.0}% traffic split.",
            analysis.estimated_token_reduction_pct,
            analysis.opportunities.len(),
            cost_savings_per_day,
            expected_quality_impact * 100.0,
            self.config.ab_test_duration.as_secs() / 3600,
            self.config.ab_test_traffic_split * 100.0
        );

        let mut metadata = HashMap::new();
        metadata.insert(
            "test_id".to_string(),
            serde_json::json!(test_id),
        );
        metadata.insert(
            "opportunity_count".to_string(),
            serde_json::json!(analysis.opportunities.len()),
        );
        metadata.insert(
            "estimated_token_reduction".to_string(),
            serde_json::json!(analysis.estimated_token_reduction),
        );
        metadata.insert(
            "estimated_token_reduction_pct".to_string(),
            serde_json::json!(analysis.estimated_token_reduction_pct),
        );

        Ok(Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: now,
            strategy: self.name().to_string(),
            decision_type: DecisionType::OptimizePrompt,
            confidence: analysis.confidence,
            expected_impact,
            config_changes,
            justification,
            related_insights: Vec::new(),
            related_recommendations: Vec::new(),
            priority: self.priority(),
            requires_approval: expected_quality_impact.abs() > 0.03, // Require approval for >3% quality change
            safety_checks,
            rollback_plan: Some(rollback_plan),
            metadata,
        })
    }

    /// Analyze A/B test results for statistical significance
    fn analyze_test_results(&self, test: &ABTest) -> TestAnalysis {
        let control = &test.observations.control;
        let treatment = &test.observations.treatment;

        // Calculate relative differences
        let token_reduction_pct = if control.avg_tokens > 0.0 {
            ((control.avg_tokens - treatment.avg_tokens) / control.avg_tokens) * 100.0
        } else {
            0.0
        };

        let quality_change_pct = if control.avg_quality > 0.0 {
            ((treatment.avg_quality - control.avg_quality) / control.avg_quality) * 100.0
        } else {
            0.0
        };

        // Simplified statistical significance test (would use proper t-test in production)
        let token_significance = self.calculate_significance(
            control.sample_count,
            treatment.sample_count,
            control.avg_tokens,
            treatment.avg_tokens,
        );

        let quality_significance = self.calculate_significance(
            control.quality_ratings.len() as u64,
            treatment.quality_ratings.len() as u64,
            control.avg_quality,
            treatment.avg_quality,
        );

        // Overall significance
        let is_statistically_significant =
            token_significance > 0.95 || quality_significance > 0.95;

        // Recommendation
        let recommendation = if !is_statistically_significant {
            TestRecommendation::ContinueTesting
        } else if token_reduction_pct >= self.config.min_token_reduction_pct
            && quality_change_pct.abs() <= self.config.max_quality_degradation_pct
        {
            TestRecommendation::Rollout
        } else if quality_change_pct < -(self.config.max_quality_degradation_pct) {
            TestRecommendation::Rollback
        } else if token_reduction_pct < self.config.min_token_reduction_pct {
            TestRecommendation::Rollback
        } else {
            TestRecommendation::ContinueTesting
        };

        TestAnalysis {
            token_reduction_pct,
            quality_change_pct,
            token_significance,
            quality_significance,
            is_statistically_significant,
            recommendation,
        }
    }

    /// Calculate statistical significance (simplified)
    fn calculate_significance(&self, n1: u64, n2: u64, mean1: f64, mean2: f64) -> f64 {
        // Simplified significance calculation
        // In production, would use proper t-test with variance
        let min_samples = n1.min(n2);
        let diff_magnitude = (mean1 - mean2).abs();
        let avg_magnitude = (mean1.abs() + mean2.abs()) / 2.0;

        let relative_diff = if avg_magnitude > 0.0 {
            diff_magnitude / avg_magnitude
        } else {
            0.0
        };

        // Simple heuristic: more samples + larger difference = higher significance
        let sample_factor = (min_samples as f64 / 100.0).min(1.0);
        let diff_factor = (relative_diff * 10.0).min(1.0);

        (sample_factor * 0.7 + diff_factor * 0.3).clamp(0.0, 1.0)
    }
}

/// Analysis of A/B test results
#[derive(Debug, Clone)]
struct TestAnalysis {
    token_reduction_pct: f64,
    quality_change_pct: f64,
    token_significance: f64,
    quality_significance: f64,
    is_statistically_significant: bool,
    recommendation: TestRecommendation,
}

/// Recommendation from test analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestRecommendation {
    /// Continue testing (need more data)
    ContinueTesting,
    /// Roll out optimized variant
    Rollout,
    /// Roll back to original
    Rollback,
}

#[async_trait]
impl OptimizationStrategy for PromptOptimizationStrategy {
    fn name(&self) -> &str {
        "prompt_optimization"
    }

    fn priority(&self) -> u32 {
        self.config.priority
    }

    fn is_applicable(&self, input: &DecisionInput) -> bool {
        // Check if strategy is enabled
        if !self.config.enabled {
            return false;
        }

        // Check if there are prompt-related recommendations
        let has_prompt_recommendations = input
            .recommendations
            .iter()
            .any(|r| r.tags.contains(&"prompt".to_string()));

        // Check if we have sufficient data
        let has_sufficient_data = input.current_metrics.request_count >= 100;

        has_prompt_recommendations && has_sufficient_data
    }

    async fn evaluate(
        &self,
        input: &DecisionInput,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Vec<Decision>> {
        let mut decisions = Vec::new();

        // Update statistics
        {
            let mut state = self.state.write().await;
            state.stats.total_evaluations += 1;
        }

        // Analyze the prompt for optimization opportunities
        let analysis = self.analyze_prompt(input).await?;

        // Check if optimization meets minimum thresholds
        if analysis.estimated_token_reduction_pct < self.config.min_token_reduction_pct {
            return Ok(decisions); // No decision - savings too small
        }

        if analysis.confidence < criteria.min_confidence {
            return Ok(decisions); // No decision - confidence too low
        }

        // Create A/B test decision
        let decision = self.create_ab_test_decision(analysis, criteria)?;

        // Update statistics
        {
            let mut state = self.state.write().await;
            state.stats.total_tests_created += 1;
        }

        decisions.push(decision);
        Ok(decisions)
    }

    async fn validate(
        &self,
        decision: &Decision,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<()> {
        // Verify decision type
        if decision.decision_type != DecisionType::OptimizePrompt {
            return Err(DecisionError::ValidationFailed(
                "Decision type mismatch".to_string(),
            ));
        }

        // Verify confidence
        if decision.confidence < criteria.min_confidence {
            return Err(DecisionError::ValidationFailed(format!(
                "Confidence {:.2} below minimum {:.2}",
                decision.confidence, criteria.min_confidence
            )));
        }

        // Verify expected impact
        if let Some(quality_change) = decision.expected_impact.quality_change {
            let quality_degradation_pct = -quality_change * 100.0;
            if quality_degradation_pct > self.config.max_quality_degradation_pct {
                return Err(DecisionError::ValidationFailed(format!(
                    "Expected quality degradation {:.1}% exceeds maximum {:.1}%",
                    quality_degradation_pct, self.config.max_quality_degradation_pct
                )));
            }
        }

        // Verify safety checks passed
        if !decision.safety_checks.iter().all(|c| c.passed) {
            return Err(DecisionError::ValidationFailed(
                "Not all safety checks passed".to_string(),
            ));
        }

        Ok(())
    }

    async fn learn(
        &mut self,
        decision: &Decision,
        outcome: &DecisionOutcome,
    ) -> DecisionResult<()> {
        let mut state = self.state.write().await;

        // Extract test ID from decision metadata
        let test_id = decision
            .metadata
            .get("test_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DecisionError::Internal("Missing test_id in metadata".to_string()))?;

        // Check if we have an active test - use a scope to limit the borrow
        let (should_update_stats, update_info) = {
            if let Some(test) = state.active_tests.get_mut(test_id) {
                // Update test observations based on outcome
                if outcome.success {
                    // Analyze the actual impact
                    if let Some(actual_impact) = &outcome.actual_impact {
                        // Calculate actual token reduction
                        let actual_token_reduction_pct = if outcome.metrics_before.request_count > 0 {
                            -actual_impact.cost_change_usd / outcome.metrics_before.total_cost_usd * 100.0
                        } else {
                            0.0
                        };

                        // Update test observations (simplified - would track per-variant in production)
                        test.observations.treatment.sample_count += 1;
                        test.observations.treatment.avg_tokens =
                            outcome.metrics_before.request_count as f64 * (1.0 - actual_token_reduction_pct / 100.0);

                        if let Some(quality_after) = outcome.metrics_after.as_ref().and_then(|m| m.avg_rating) {
                            test.observations.treatment.quality_ratings.push(quality_after);
                            test.observations.treatment.avg_quality = test
                                .observations
                                .treatment
                                .quality_ratings
                                .iter()
                                .sum::<f64>()
                                / test.observations.treatment.quality_ratings.len() as f64;
                        }

                        // Analyze test results
                        let analysis = self.analyze_test_results(test);

                        // Prepare update info based on recommendation
                        let update_type = match analysis.recommendation {
                            TestRecommendation::Rollout => {
                                test.status = TestStatus::RolledOut;
                                let token_savings = (test.expected_token_reduction_pct / 100.0
                                    * outcome.metrics_before.request_count as f64) as u64;
                                Some((
                                    "rollout",
                                    token_savings,
                                    -actual_impact.cost_change_usd,
                                    format!(
                                        "Successful rollout: {:.1}% token reduction, {:.1}% quality change",
                                        analysis.token_reduction_pct, analysis.quality_change_pct
                                    ),
                                    0.05f64,
                                ))
                            }
                            TestRecommendation::Rollback => {
                                test.status = TestStatus::RolledBack;
                                let rollback_type = if analysis.quality_change_pct < -(self.config.max_quality_degradation_pct) {
                                    "quality"
                                } else {
                                    "savings"
                                };
                                Some((
                                    rollback_type,
                                    0u64,
                                    0.0f64,
                                    format!(
                                        "Rolled back: insufficient savings or quality degradation. \
                                         Token reduction: {:.1}%, Quality change: {:.1}%",
                                        analysis.token_reduction_pct, analysis.quality_change_pct
                                    ),
                                    -0.05f64,
                                ))
                            }
                            TestRecommendation::ContinueTesting => None,
                        };

                        // Check if test is complete
                        let is_complete = test.status != TestStatus::Running;
                        let completed_test = if is_complete { Some(test.clone()) } else { None };

                        (true, Some((update_type, is_complete, completed_test)))
                    } else {
                        (false, None)
                    }
                } else {
                    // Decision failed - rollback test
                    test.status = TestStatus::Stopped;
                    let lesson = format!(
                        "Test failed: {}",
                        outcome.error.as_ref().unwrap_or(&"Unknown error".to_string())
                    );
                    (true, Some((Some(("failed", 0u64, 0.0f64, lesson, -0.1f64)), false, None)))
                }
            } else {
                (false, None)
            }
        }; // End of borrow scope

        // Now update state.stats without holding a borrow to active_tests
        if should_update_stats {
            if let Some((update_type, is_complete, completed_test)) = update_info {
                // Update stats based on the recommendation
                if let Some((action, token_savings, cost_savings, lesson, confidence_adj)) = update_type {
                    match action {
                        "rollout" => {
                            state.stats.successful_rollouts += 1;
                            state.stats.total_token_savings += token_savings;
                            state.stats.total_cost_savings_usd += cost_savings;
                        }
                        "quality" => {
                            state.stats.rollbacks_quality += 1;
                        }
                        "savings" => {
                            state.stats.rollbacks_savings += 1;
                        }
                        "failed" => {
                            state.stats.rollbacks_quality += 1;
                        }
                        _ => {}
                    }

                    state.learning_history.push(LearningRecord {
                        timestamp: Utc::now(),
                        decision_id: decision.id.clone(),
                        lesson,
                        confidence_adjustment: confidence_adj,
                        metadata: HashMap::new(),
                    });
                }

                // Move to completed tests if done
                if is_complete {
                    if let Some(test) = completed_test {
                        state.completed_tests.push(test);
                    }
                    state.active_tests.remove(test_id);
                }
            }
        }

        Ok(())
    }

    fn get_stats(&self) -> serde_json::Value {
        let state = futures::executor::block_on(self.state.read());
        serde_json::json!({
            "total_evaluations": state.stats.total_evaluations,
            "total_tests_created": state.stats.total_tests_created,
            "active_tests": state.active_tests.len(),
            "completed_tests": state.completed_tests.len(),
            "successful_rollouts": state.stats.successful_rollouts,
            "rollbacks_quality": state.stats.rollbacks_quality,
            "rollbacks_savings": state.stats.rollbacks_savings,
            "total_token_savings": state.stats.total_token_savings,
            "total_cost_savings_usd": state.stats.total_cost_savings_usd,
            "learning_records": state.learning_history.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::types::{
        Action, ActionType, Confidence as AnalyzerConfidence, Impact, Priority, Recommendation,
        RiskLevel,
    };
    use crate::decision::types::SystemMetrics;

    fn create_test_config() -> PromptOptimizationConfig {
        PromptOptimizationConfig {
            enabled: true,
            priority: 85,
            min_confidence: 0.7,
            min_token_reduction_pct: 10.0,
            ab_test_duration: ChronoDuration::hours(1),
            ab_test_traffic_split: 0.1,
            min_quality_score: 4.0,
            max_quality_degradation_pct: 5.0,
        }
    }

    fn create_test_input() -> DecisionInput {
        let now = Utc::now();
        DecisionInput {
            timestamp: now,
            analysis_reports: Vec::new(),
            insights: Vec::new(),
            recommendations: vec![Recommendation {
                id: "rec1".to_string(),
                analyzer: "prompt_analyzer".to_string(),
                timestamp: now,
                priority: Priority::High,
                title: "Optimize prompt".to_string(),
                description: "Prompt contains redundant instructions".to_string(),
                action: Action {
                    action_type: ActionType::Optimize,
                    instructions: "Remove redundant phrases".to_string(),
                    parameters: HashMap::new(),
                    estimated_effort: Some(1.0),
                    risk_level: RiskLevel::Low,
                },
                expected_impact: Impact {
                    performance: None,
                    cost: None,
                    quality: None,
                    description: "Reduce token usage".to_string(),
                },
                confidence: AnalyzerConfidence::new(0.8),
                related_insights: Vec::new(),
                tags: vec!["prompt".to_string()],
            }],
            current_metrics: SystemMetrics {
                avg_latency_ms: 200.0,
                p95_latency_ms: 300.0,
                p99_latency_ms: 400.0,
                success_rate_pct: 98.0,
                error_rate_pct: 2.0,
                throughput_rps: 10.0,
                total_cost_usd: 100.0,
                daily_cost_usd: 50.0,
                avg_rating: Some(4.2),
                cache_hit_rate_pct: Some(30.0),
                request_count: 1000,
                timestamp: now,
            },
            context: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_strategy_creation() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);

        assert_eq!(strategy.name(), "prompt_optimization");
        assert_eq!(strategy.priority(), 85);
    }

    #[tokio::test]
    async fn test_is_applicable_with_prompt_recommendations() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();

        assert!(strategy.is_applicable(&input));
    }

    #[tokio::test]
    async fn test_is_applicable_without_prompt_recommendations() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let mut input = create_test_input();
        input.recommendations.clear();

        assert!(!strategy.is_applicable(&input));
    }

    #[tokio::test]
    async fn test_is_applicable_insufficient_data() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let mut input = create_test_input();
        input.current_metrics.request_count = 50; // Less than minimum 100

        assert!(!strategy.is_applicable(&input));
    }

    #[tokio::test]
    async fn test_analyze_prompt() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();

        let analysis = strategy.analyze_prompt(&input).await.unwrap();

        assert!(!analysis.opportunities.is_empty());
        assert!(analysis.estimated_token_reduction > 0);
        assert!(analysis.confidence > 0.0);
    }

    #[tokio::test]
    async fn test_evaluate_generates_decision() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();

        assert_eq!(decisions.len(), 1);
        let decision = &decisions[0];
        assert_eq!(decision.decision_type, DecisionType::OptimizePrompt);
        assert!(decision.confidence >= criteria.min_confidence);
        assert!(!decision.safety_checks.is_empty());
        assert!(decision.rollback_plan.is_some());
    }

    #[tokio::test]
    async fn test_evaluate_low_savings_no_decision() {
        let mut config = create_test_config();
        config.min_token_reduction_pct = 90.0; // Set very high threshold
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();

        assert_eq!(decisions.len(), 0);
    }

    #[tokio::test]
    async fn test_validate_valid_decision() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();
        let decision = &decisions[0];

        let result = strategy.validate(decision, &criteria).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_wrong_decision_type() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let criteria = DecisionCriteria::default();

        let mut decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "prompt_optimization".to_string(),
            decision_type: DecisionType::ModelSwitch, // Wrong type
            confidence: 0.8,
            expected_impact: ExpectedImpact {
                cost_change_usd: None,
                latency_change_ms: None,
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: std::time::Duration::from_secs(3600),
                confidence: 0.8,
            },
            config_changes: Vec::new(),
            justification: "Test".to_string(),
            related_insights: Vec::new(),
            related_recommendations: Vec::new(),
            priority: 85,
            requires_approval: false,
            safety_checks: vec![SafetyCheck {
                name: "test".to_string(),
                passed: true,
                details: "Test".to_string(),
                timestamp: Utc::now(),
            }],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let result = strategy.validate(&decision, &criteria).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_low_confidence() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let mut criteria = DecisionCriteria::default();
        criteria.min_confidence = 0.95; // Very high threshold

        let input = create_test_input();
        let default_criteria = DecisionCriteria::default();
        let decisions = strategy.evaluate(&input, &default_criteria).await.unwrap();
        let decision = &decisions[0];

        let result = strategy.validate(decision, &criteria).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_calculate_significance() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);

        // Test with large sample and large difference
        let sig1 = strategy.calculate_significance(1000, 1000, 100.0, 80.0);
        assert!(sig1 > 0.5);

        // Test with small sample
        let sig2 = strategy.calculate_significance(10, 10, 100.0, 80.0);
        assert!(sig2 < sig1);

        // Test with no difference
        let sig3 = strategy.calculate_significance(1000, 1000, 100.0, 100.0);
        assert!(sig3 < sig1);
    }

    #[tokio::test]
    async fn test_analyze_test_results() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);

        let test = ABTest {
            id: "test1".to_string(),
            original_prompt_id: "orig".to_string(),
            optimized_prompt_id: "opt".to_string(),
            start_time: Utc::now(),
            end_time: Utc::now() + ChronoDuration::hours(1),
            traffic_split: 0.1,
            expected_token_reduction_pct: 15.0,
            expected_quality_impact: -0.02,
            observations: TestObservations {
                control: VariantObservations {
                    sample_count: 1000,
                    total_tokens: 100000,
                    avg_tokens: 100.0,
                    quality_ratings: vec![4.2; 100],
                    avg_quality: 4.2,
                    success_rate: 98.0,
                    successful_requests: 980,
                    failed_requests: 20,
                },
                treatment: VariantObservations {
                    sample_count: 100,
                    total_tokens: 8500,
                    avg_tokens: 85.0, // 15% reduction
                    quality_ratings: vec![4.15; 100],
                    avg_quality: 4.15, // Small quality drop
                    success_rate: 97.0,
                    successful_requests: 97,
                    failed_requests: 3,
                },
            },
            status: TestStatus::Running,
        };

        let analysis = strategy.analyze_test_results(&test);

        assert!(analysis.token_reduction_pct > 10.0);
        assert!(analysis.quality_change_pct.abs() < 5.0);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);

        let stats = strategy.get_stats();

        assert!(stats.is_object());
        assert!(stats.get("total_evaluations").is_some());
        assert!(stats.get("total_tests_created").is_some());
        assert!(stats.get("successful_rollouts").is_some());
    }

    #[tokio::test]
    async fn test_estimate_quality_impact() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);

        let analysis = PromptAnalysis {
            opportunities: vec![
                OptimizationOpportunity::RemoveRedundancy {
                    phrase: "test".to_string(),
                    occurrences: 2,
                },
            ],
            estimated_token_reduction: 50,
            estimated_token_reduction_pct: 15.0,
            current_avg_tokens: 300.0,
            confidence: 0.8,
        };

        let impact = strategy.estimate_quality_impact(&analysis);

        // Should be a small negative or positive number
        assert!(impact.abs() < 0.2);
    }

    #[tokio::test]
    async fn test_decision_metadata() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();
        let decision = &decisions[0];

        assert!(decision.metadata.contains_key("test_id"));
        assert!(decision.metadata.contains_key("opportunity_count"));
        assert!(decision.metadata.contains_key("estimated_token_reduction"));
    }

    #[tokio::test]
    async fn test_rollback_plan() {
        let config = create_test_config();
        let strategy = PromptOptimizationStrategy::new(config);
        let input = create_test_input();
        let criteria = DecisionCriteria::default();

        let decisions = strategy.evaluate(&input, &criteria).await.unwrap();
        let decision = &decisions[0];

        let rollback_plan = decision.rollback_plan.as_ref().unwrap();
        assert!(!rollback_plan.trigger_conditions.is_empty());
        assert!(!rollback_plan.revert_changes.is_empty());
        assert!(rollback_plan.max_duration.as_secs() > 0);
    }
}
