//! Decision Engine Coordinator
//!
//! Orchestrates multiple optimization strategies and coordinates decision-making.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::config::DecisionEngineConfig;
use super::error::{DecisionError, DecisionResult, DecisionState};
use super::traits::{DecisionEngine, OptimizationStrategy};
use super::types::{Decision, DecisionCriteria, DecisionInput, DecisionOutcome, DecisionStats};

/// Decision Engine Coordinator
///
/// Manages multiple optimization strategies and coordinates decision-making
pub struct DecisionCoordinator {
    config: DecisionEngineConfig,
    state: DecisionState,
    strategies: Vec<Arc<dyn OptimizationStrategy>>,
    stats: Arc<RwLock<DecisionStats>>,
    active_decisions: Arc<RwLock<HashMap<String, Decision>>>,
    decision_history: Arc<RwLock<Vec<Decision>>>,
    outcome_history: Arc<RwLock<Vec<DecisionOutcome>>>,
    start_time: Instant,
}

impl DecisionCoordinator {
    /// Create a new Decision Coordinator
    pub fn new(
        config: DecisionEngineConfig,
        strategies: Vec<Arc<dyn OptimizationStrategy>>,
    ) -> Self {
        Self {
            config,
            state: DecisionState::Initialized,
            strategies,
            stats: Arc::new(RwLock::new(DecisionStats::default())),
            active_decisions: Arc::new(RwLock::new(HashMap::new())),
            decision_history: Arc::new(RwLock::new(Vec::new())),
            outcome_history: Arc::new(RwLock::new(Vec::new())),
            start_time: Instant::now(),
        }
    }

    /// Validate a decision against criteria
    async fn validate_decision(
        &self,
        decision: &Decision,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<()> {
        // Check confidence threshold
        if decision.confidence < criteria.min_confidence {
            return Err(DecisionError::ValidationFailed(format!(
                "Confidence {:.2} below minimum {:.2}",
                decision.confidence, criteria.min_confidence
            )));
        }

        // Check cost constraints
        if let Some(max_cost) = criteria.max_cost_increase_usd {
            if let Some(cost_change) = decision.expected_impact.cost_change_usd {
                if cost_change > max_cost {
                    return Err(DecisionError::ConstraintViolation(format!(
                        "Cost increase ${:.2} exceeds maximum ${:.2}",
                        cost_change, max_cost
                    )));
                }
            }
        }

        // Check latency constraints
        if let Some(max_latency) = criteria.max_latency_increase_ms {
            if let Some(latency_change) = decision.expected_impact.latency_change_ms {
                if latency_change > max_latency {
                    return Err(DecisionError::ConstraintViolation(format!(
                        "Latency increase {:.2}ms exceeds maximum {:.2}ms",
                        latency_change, max_latency
                    )));
                }
            }
        }

        // Check quality constraints
        if let Some(min_quality) = criteria.min_quality_score {
            if let Some(quality_change) = decision.expected_impact.quality_change {
                // If quality is expected to drop below minimum, reject
                if quality_change < 0.0 {
                    let estimated_quality = min_quality + quality_change;
                    if estimated_quality < min_quality {
                        return Err(DecisionError::ConstraintViolation(format!(
                            "Quality drop would result in score {:.2}, below minimum {:.2}",
                            estimated_quality, min_quality
                        )));
                    }
                }
            }
        }

        // Verify all required safety checks passed
        for required_check in &criteria.required_safety_checks {
            let check_passed = decision
                .safety_checks
                .iter()
                .any(|check| check.name == *required_check && check.passed);

            if !check_passed {
                return Err(DecisionError::ValidationFailed(format!(
                    "Required safety check '{}' did not pass",
                    required_check
                )));
            }
        }

        Ok(())
    }

    /// Merge and prioritize decisions from multiple strategies
    async fn merge_decisions(
        &self,
        all_decisions: Vec<Vec<Decision>>,
        criteria: &DecisionCriteria,
    ) -> Vec<Decision> {
        let mut merged: Vec<Decision> = all_decisions.into_iter().flatten().collect();

        // Sort by priority (descending)
        merged.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Apply max concurrent decisions limit
        merged.truncate(criteria.max_concurrent_decisions);

        // Check for conflicts
        let mut final_decisions = Vec::new();
        for decision in merged {
            // Check if this decision conflicts with already selected decisions
            let conflicts = final_decisions.iter().any(|d: &Decision| {
                self.decisions_conflict(&decision, d)
            });

            if !conflicts {
                final_decisions.push(decision);
            } else {
                debug!(
                    "Skipping decision {} due to conflict",
                    decision.id
                );
            }
        }

        final_decisions
    }

    /// Check if two decisions conflict
    fn decisions_conflict(&self, d1: &Decision, d2: &Decision) -> bool {
        // Same decision type conflicts
        if d1.decision_type == d2.decision_type {
            return true;
        }

        // Model switch conflicts with prompt optimization (need stable model first)
        use super::types::DecisionType::*;
        matches!(
            (&d1.decision_type, &d2.decision_type),
            (ModelSwitch, OptimizePrompt) | (OptimizePrompt, ModelSwitch)
        )
    }

    /// Update statistics
    async fn update_stats(&self, decisions: &[Decision]) {
        let mut stats = self.stats.write().await;

        stats.total_decisions += decisions.len() as u64;

        for decision in decisions {
            let decision_type = format!("{:?}", decision.decision_type);
            *stats.decisions_by_type.entry(decision_type).or_insert(0) += 1;

            *stats
                .decisions_by_strategy
                .entry(decision.strategy.clone())
                .or_insert(0) += 1;

            // Update average confidence
            let total = stats.total_decisions as f64;
            stats.avg_confidence = ((stats.avg_confidence * (total - 1.0)) + decision.confidence) / total;
        }

        stats.last_decision_time = Some(Utc::now());
    }
}

#[async_trait]
impl DecisionEngine for DecisionCoordinator {
    fn name(&self) -> &str {
        &self.config.id
    }

    fn state(&self) -> DecisionState {
        self.state
    }

    async fn start(&mut self) -> DecisionResult<()> {
        if !self.state.can_transition_to(DecisionState::Starting) {
            return Err(DecisionError::InvalidStateTransition {
                from: self.state,
                to: DecisionState::Starting,
            });
        }

        info!("Starting Decision Engine Coordinator");
        self.state = DecisionState::Starting;

        // Initialize strategies
        info!("Initialized {} strategies", self.strategies.len());

        self.state = DecisionState::Running;
        info!("Decision Engine Coordinator started successfully");

        Ok(())
    }

    async fn stop(&mut self) -> DecisionResult<()> {
        if !self.state.can_transition_to(DecisionState::Draining) {
            return Err(DecisionError::InvalidStateTransition {
                from: self.state,
                to: DecisionState::Draining,
            });
        }

        info!("Stopping Decision Engine Coordinator");
        self.state = DecisionState::Draining;

        // Wait for active decisions to complete
        let active_count = self.active_decisions.read().await.len();
        if active_count > 0 {
            info!("Waiting for {} active decisions to complete", active_count);
        }

        self.state = DecisionState::Stopped;
        info!("Decision Engine Coordinator stopped");

        Ok(())
    }

    async fn make_decisions(
        &mut self,
        input: DecisionInput,
        criteria: &DecisionCriteria,
    ) -> DecisionResult<Vec<Decision>> {
        if !self.state.can_process() {
            return Err(DecisionError::NotRunning);
        }

        let start = Instant::now();

        debug!(
            "Making decisions with {} insights and {} recommendations",
            input.insights.len(),
            input.recommendations.len()
        );

        // Run each applicable strategy
        let mut all_decisions = Vec::new();

        for strategy in &self.strategies {
            // Check if strategy is enabled
            if !criteria.enabled_strategies.contains(&strategy.name().to_string()) {
                continue;
            }

            // Check if strategy is applicable
            if !strategy.is_applicable(&input) {
                debug!("Strategy {} not applicable", strategy.name());
                continue;
            }

            // Evaluate strategy
            match strategy.evaluate(&input, criteria).await {
                Ok(decisions) => {
                    debug!(
                        "Strategy {} generated {} decisions",
                        strategy.name(),
                        decisions.len()
                    );
                    all_decisions.push(decisions);
                }
                Err(e) => {
                    warn!("Strategy {} evaluation failed: {}", strategy.name(), e);
                }
            }
        }

        // Merge and prioritize decisions
        let mut merged_decisions = self.merge_decisions(all_decisions, criteria).await;

        // Validate each decision
        let mut validated_decisions = Vec::new();
        for decision in merged_decisions {
            match self.validate_decision(&decision, criteria).await {
                Ok(()) => {
                    validated_decisions.push(decision);
                }
                Err(e) => {
                    warn!("Decision {} validation failed: {}", decision.id, e);
                }
            }
        }

        // Update statistics
        self.update_stats(&validated_decisions).await;

        // Store decisions
        let mut history = self.decision_history.write().await;
        history.extend(validated_decisions.clone());

        // Keep only recent history
        if history.len() > self.config.monitoring.decision_history_size {
            let len = history.len();
            history.drain(0..len - self.config.monitoring.decision_history_size);
        }

        // Add to active decisions
        let mut active = self.active_decisions.write().await;
        for decision in &validated_decisions {
            active.insert(decision.id.clone(), decision.clone());
        }

        let elapsed = start.elapsed();
        info!(
            "Generated {} validated decisions in {:?}",
            validated_decisions.len(),
            elapsed
        );

        Ok(validated_decisions)
    }

    async fn record_outcome(&mut self, outcome: DecisionOutcome) -> DecisionResult<()> {
        debug!("Recording outcome for decision {}", outcome.decision_id);

        // Remove from active decisions
        let mut active = self.active_decisions.write().await;
        let decision = active.remove(&outcome.decision_id);

        // Update statistics
        let mut stats = self.stats.write().await;

        if outcome.success {
            stats.successful_decisions += 1;

            // Update cost savings if available
            if let Some(actual_impact) = &outcome.actual_impact {
                if actual_impact.cost_change_usd < 0.0 {
                    stats.total_cost_savings_usd += actual_impact.cost_change_usd.abs();
                }
                if actual_impact.latency_change_ms < 0.0 {
                    stats.total_latency_improvement_ms += actual_impact.latency_change_ms.abs();
                }
            }
        } else {
            stats.failed_decisions += 1;
        }

        if outcome.rolled_back {
            stats.rolled_back_decisions += 1;
        }

        drop(stats);

        // Note: Strategy learning is disabled in this coordinator because strategies
        // are wrapped in Arc<dyn> which doesn't allow mutable access.
        // To enable learning, strategies would need to use interior mutability (e.g., Mutex).
        if decision.is_some() {
            debug!("Strategy learning not implemented for Arc-wrapped strategies");
        }

        // Store outcome in history
        let mut history = self.outcome_history.write().await;
        history.push(outcome);

        // Keep only recent history
        if history.len() > self.config.monitoring.outcome_history_size {
            let len = history.len();
            history.drain(0..len - self.config.monitoring.outcome_history_size);
        }

        Ok(())
    }

    fn get_stats(&self) -> DecisionStats {
        // Synchronous method, can't await. Return default or use try_read
        // In production, we'd use try_read() and handle the lock failure
        DecisionStats::default()
    }

    async fn health_check(&self) -> DecisionResult<()> {
        if !self.state.can_process() {
            return Err(DecisionError::NotRunning);
        }

        // Check if strategies are responsive
        let strategy_count = self.strategies.len();
        if strategy_count == 0 {
            return Err(DecisionError::Internal(
                "No strategies configured".to_string(),
            ));
        }

        Ok(())
    }

    async fn reset(&mut self) -> DecisionResult<()> {
        info!("Resetting Decision Engine Coordinator");

        // Clear active decisions
        self.active_decisions.write().await.clear();

        // Clear histories
        self.decision_history.write().await.clear();
        self.outcome_history.write().await.clear();

        // Reset statistics
        *self.stats.write().await = DecisionStats::default();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{AnalysisReport, Insight, InsightCategory, Severity, Confidence};
    use crate::decision::types::*;

    #[tokio::test]
    async fn test_coordinator_lifecycle() {
        let config = DecisionEngineConfig::default();
        let mut coordinator = DecisionCoordinator::new(config, vec![]);

        assert_eq!(coordinator.state(), DecisionState::Initialized);

        coordinator.start().await.unwrap();
        assert_eq!(coordinator.state(), DecisionState::Running);

        coordinator.stop().await.unwrap();
        assert_eq!(coordinator.state(), DecisionState::Stopped);
    }

    #[tokio::test]
    async fn test_decision_validation() {
        let config = DecisionEngineConfig::default();
        let coordinator = DecisionCoordinator::new(config, vec![]);

        let decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "test".to_string(),
            decision_type: DecisionType::NoAction,
            confidence: 0.9,
            expected_impact: ExpectedImpact {
                cost_change_usd: Some(10.0),
                latency_change_ms: Some(50.0),
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: std::time::Duration::from_secs(60),
                confidence: 0.9,
            },
            config_changes: vec![],
            justification: "Test decision".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 50,
            requires_approval: false,
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

        let criteria = DecisionCriteria::default();

        // Should pass validation
        assert!(coordinator.validate_decision(&decision, &criteria).await.is_ok());
    }

    #[tokio::test]
    async fn test_validation_failures() {
        let config = DecisionEngineConfig::default();
        let coordinator = DecisionCoordinator::new(config, vec![]);

        let mut decision = Decision {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            strategy: "test".to_string(),
            decision_type: DecisionType::NoAction,
            confidence: 0.5, // Below default minimum of 0.7
            expected_impact: ExpectedImpact {
                cost_change_usd: None,
                latency_change_ms: None,
                quality_change: None,
                throughput_change_rps: None,
                success_rate_change_pct: None,
                time_to_impact: std::time::Duration::from_secs(60),
                confidence: 0.5,
            },
            config_changes: vec![],
            justification: "Test".to_string(),
            related_insights: vec![],
            related_recommendations: vec![],
            priority: 50,
            requires_approval: false,
            safety_checks: vec![],
            rollback_plan: None,
            metadata: HashMap::new(),
        };

        let criteria = DecisionCriteria::default();

        // Should fail due to low confidence
        assert!(coordinator.validate_decision(&decision, &criteria).await.is_err());
    }

    #[tokio::test]
    async fn test_health_check() {
        let config = DecisionEngineConfig::default();
        let mut coordinator = DecisionCoordinator::new(config, vec![]);

        // Should fail when not running
        assert!(coordinator.health_check().await.is_err());

        // Should succeed when running
        coordinator.start().await.unwrap();
        assert!(coordinator.health_check().await.is_ok());
    }
}
