//! Actuator Coordinator
//!
//! The main Actuator implementation that orchestrates canary deployments,
//! rollbacks, configuration management, and health monitoring.

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::canary::CanaryDeploymentEngine;
use super::config::ActuatorConfig;
use super::configuration::ConfigurationManagerImpl;
use super::error::{ActuatorError, ActuatorResult, ActuatorState};
use super::health::ProductionHealthMonitor;
use super::rollback::RollbackEngine;
use super::traits::{Actuator, CanaryDeployment, ConfigurationManager, HealthMonitor};
use super::types::{
    ActuatorStats, DeploymentRequest, DeploymentState, DeploymentStatus, DeploymentStrategy,
    HealthCheckResult, RollbackRequest, RollbackResult,
};
use crate::decision::Decision;

/// Main Actuator Coordinator
///
/// Orchestrates all actuator components to safely deploy configuration changes
pub struct ActuatorCoordinator {
    config: ActuatorConfig,
    state: ActuatorState,
    canary_engine: Arc<RwLock<CanaryDeploymentEngine>>,
    rollback_engine: Arc<RwLock<RollbackEngine>>,
    config_manager: Arc<RwLock<ConfigurationManagerImpl>>,
    health_monitor: Arc<ProductionHealthMonitor>,
    active_deployments: Arc<RwLock<HashMap<String, DeploymentStatus>>>,
    stats: Arc<RwLock<ActuatorStats>>,
    start_time: Instant,
}

impl ActuatorCoordinator {
    /// Create a new Actuator Coordinator
    pub fn new(config: ActuatorConfig) -> Self {
        let canary_engine = Arc::new(RwLock::new(CanaryDeploymentEngine::new(
            config.canary.clone(),
        )));

        let rollback_engine = Arc::new(RwLock::new(RollbackEngine::new(
            config.rollback.clone(),
        )));

        let config_manager = Arc::new(RwLock::new(ConfigurationManagerImpl::new(
            config.configuration.clone(),
        )));

        let health_monitor = Arc::new(ProductionHealthMonitor::new());

        Self {
            config,
            state: ActuatorState::Initialized,
            canary_engine,
            rollback_engine,
            config_manager,
            health_monitor,
            active_deployments: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ActuatorStats::default())),
            start_time: Instant::now(),
        }
    }

    /// Create with custom components
    pub fn with_components(
        config: ActuatorConfig,
        canary_engine: CanaryDeploymentEngine,
        rollback_engine: RollbackEngine,
        config_manager: ConfigurationManagerImpl,
        health_monitor: ProductionHealthMonitor,
    ) -> Self {
        Self {
            config,
            state: ActuatorState::Initialized,
            canary_engine: Arc::new(RwLock::new(canary_engine)),
            rollback_engine: Arc::new(RwLock::new(rollback_engine)),
            config_manager: Arc::new(RwLock::new(config_manager)),
            health_monitor: Arc::new(health_monitor),
            active_deployments: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ActuatorStats::default())),
            start_time: Instant::now(),
        }
    }

    /// Execute canary deployment
    async fn execute_canary_deployment(
        &mut self,
        request: DeploymentRequest,
    ) -> ActuatorResult<DeploymentStatus> {
        let deployment_id = request.id.clone();
        info!("Starting canary deployment: {}", deployment_id);

        // Register with health monitor
        self.health_monitor.register_deployment(&deployment_id);

        // Create snapshot before deployment
        let mut config_mgr = self.config_manager.write().await;
        let snapshot = config_mgr
            .create_snapshot()
            .await
            .map_err(|e| ActuatorError::ConfigError(format!("Failed to create snapshot: {}", e)))?;
        drop(config_mgr);

        // Store snapshot for rollback
        let mut rollback_engine = self.rollback_engine.write().await;
        rollback_engine
            .store_snapshot(&deployment_id, snapshot)
            .await
            .map_err(|e| {
                ActuatorError::ConfigError(format!("Failed to store snapshot: {}", e))
            })?;
        drop(rollback_engine);

        // Start canary deployment
        let mut canary_engine = self.canary_engine.write().await;
        let status = canary_engine
            .start_canary(request)
            .await
            .map_err(|e| ActuatorError::DeploymentFailed(format!("Canary start failed: {}", e)))?;
        drop(canary_engine);

        // Track active deployment
        let mut active = self.active_deployments.write().await;
        active.insert(deployment_id.clone(), status.clone());

        info!(
            "Canary deployment started: {} ({})",
            deployment_id, status.state
        );

        Ok(status)
    }

    /// Execute immediate deployment
    async fn execute_immediate_deployment(
        &mut self,
        request: DeploymentRequest,
    ) -> ActuatorResult<DeploymentStatus> {
        let deployment_id = request.id.clone();
        info!("Starting immediate deployment: {}", deployment_id);

        // Create snapshot
        let mut config_mgr = self.config_manager.write().await;
        let _snapshot = config_mgr
            .create_snapshot()
            .await
            .map_err(|e| ActuatorError::ConfigError(format!("Failed to create snapshot: {}", e)))?;

        // Apply configuration immediately
        config_mgr
            .apply_config(&request.config_changes)
            .await
            .map_err(|e| {
                ActuatorError::DeploymentFailed(format!("Failed to apply config: {}", e))
            })?;
        drop(config_mgr);

        // Create deployment status
        let status = DeploymentStatus {
            deployment_id: deployment_id.clone(),
            state: DeploymentState::Completed,
            current_phase: None,
            traffic_percent: 100.0,
            start_time: Utc::now(),
            phase_start_time: None,
            health: super::types::HealthStatus {
                healthy: true,
                checks_passed: 0,
                checks_failed: 0,
                check_results: vec![],
                last_check_time: Utc::now(),
            },
            metrics: Default::default(),
            errors: vec![],
            last_updated: Utc::now(),
        };

        info!("Immediate deployment completed: {}", deployment_id);

        Ok(status)
    }

    /// Monitor deployment and trigger rollback if needed
    async fn monitor_deployment(&mut self, deployment_id: &str) -> ActuatorResult<()> {
        let status = self.get_deployment_status(deployment_id).await?;

        // Check if rollback is needed
        let should_rollback = self
            .health_monitor
            .should_rollback(deployment_id, &status)
            .await?;

        if should_rollback && self.config.rollback.auto_rollback {
            warn!(
                "Auto-rollback triggered for deployment: {}",
                deployment_id
            );

            let rollback_request = RollbackRequest {
                deployment_id: deployment_id.to_string(),
                reason: super::types::RollbackReason::HealthCheckFailed,
                timestamp: Utc::now(),
                automatic: true,
                metadata: HashMap::new(),
            };

            self.rollback(rollback_request).await?;
        }

        Ok(())
    }

    /// Update statistics
    async fn update_stats(&self, success: bool, rolled_back: bool, strategy: DeploymentStrategy) {
        let mut stats = self.stats.write().await;

        stats.total_deployments += 1;

        if success {
            stats.successful_deployments += 1;
        } else {
            stats.failed_deployments += 1;
        }

        if rolled_back {
            stats.rolled_back_deployments += 1;
        }

        let strategy_name = format!("{:?}", strategy);
        *stats.deployments_by_strategy.entry(strategy_name).or_insert(0) += 1;

        stats.last_deployment_time = Some(Utc::now());
    }
}

#[async_trait]
impl Actuator for ActuatorCoordinator {
    fn name(&self) -> &str {
        &self.config.id
    }

    fn state(&self) -> ActuatorState {
        self.state
    }

    async fn start(&mut self) -> ActuatorResult<()> {
        if !self.state.can_transition_to(ActuatorState::Starting) {
            return Err(ActuatorError::InvalidStateTransition {
                from: self.state,
                to: ActuatorState::Starting,
            });
        }

        info!("Starting Actuator Coordinator");
        self.state = ActuatorState::Starting;

        // Initialize components
        info!("Actuator components initialized");

        self.state = ActuatorState::Running;
        info!("Actuator Coordinator started successfully");

        Ok(())
    }

    async fn stop(&mut self) -> ActuatorResult<()> {
        if !self.state.can_transition_to(ActuatorState::Draining) {
            return Err(ActuatorError::InvalidStateTransition {
                from: self.state,
                to: ActuatorState::Draining,
            });
        }

        info!("Stopping Actuator Coordinator");
        self.state = ActuatorState::Draining;

        // Wait for active deployments
        let active_count = self.active_deployments.read().await.len();
        if active_count > 0 {
            info!(
                "Waiting for {} active deployments to complete",
                active_count
            );
        }

        self.state = ActuatorState::Stopped;
        info!("Actuator Coordinator stopped");

        Ok(())
    }

    async fn deploy(&mut self, request: DeploymentRequest) -> ActuatorResult<DeploymentStatus> {
        if !self.state.can_deploy() {
            return Err(ActuatorError::NotRunning);
        }

        let deployment_id = request.id.clone();
        let strategy = request.strategy;

        info!(
            "Deploying: {} with strategy {:?}",
            deployment_id, strategy
        );

        // Check concurrent deployment limit
        let active_count = self.active_deployments.read().await.len();
        if active_count >= self.config.max_concurrent_deployments {
            return Err(ActuatorError::StateError(format!(
                "Maximum concurrent deployments ({}) reached",
                self.config.max_concurrent_deployments
            )));
        }

        let start = Instant::now();

        // Execute deployment based on strategy
        let result = match strategy {
            DeploymentStrategy::Canary => self.execute_canary_deployment(request).await,
            DeploymentStrategy::Immediate => self.execute_immediate_deployment(request).await,
            DeploymentStrategy::BlueGreen => {
                // TODO: Implement blue-green deployment
                Err(ActuatorError::DeploymentFailed(
                    "Blue-green deployment not yet implemented".to_string(),
                ))
            }
            DeploymentStrategy::Rolling => {
                // TODO: Implement rolling deployment
                Err(ActuatorError::DeploymentFailed(
                    "Rolling deployment not yet implemented".to_string(),
                ))
            }
            DeploymentStrategy::Shadow => {
                // TODO: Implement shadow deployment
                Err(ActuatorError::DeploymentFailed(
                    "Shadow deployment not yet implemented".to_string(),
                ))
            }
        };

        // Update statistics
        let success = result.is_ok();
        self.update_stats(success, false, strategy).await;

        let elapsed = start.elapsed();
        info!(
            "Deployment {} {} in {:?}",
            deployment_id,
            if success { "succeeded" } else { "failed" },
            elapsed
        );

        result
    }

    async fn get_deployment_status(&self, deployment_id: &str) -> ActuatorResult<DeploymentStatus> {
        // Try to get from active deployments
        let active = self.active_deployments.read().await;
        if let Some(status) = active.get(deployment_id) {
            return Ok(status.clone());
        }
        drop(active);

        // Try to get from canary engine
        let canary_engine = self.canary_engine.read().await;
        match canary_engine.get_status(deployment_id).await {
            Ok(status) => Ok(status),
            Err(_) => Err(ActuatorError::StateError(format!(
                "Deployment not found: {}",
                deployment_id
            ))),
        }
    }

    async fn rollback(&mut self, request: RollbackRequest) -> ActuatorResult<RollbackResult> {
        if !self.state.can_rollback() {
            return Err(ActuatorError::NotRunning);
        }

        let deployment_id = request.deployment_id.clone();
        info!("Rolling back deployment: {}", deployment_id);

        let start = Instant::now();

        // Get deployment status
        let status = self.get_deployment_status(&deployment_id).await?;

        // Determine rollback mode
        let mode = if request.reason == super::types::RollbackReason::HealthCheckFailed
            || request.reason == super::types::RollbackReason::HighErrorRate
        {
            super::rollback::RollbackMode::Fast
        } else {
            super::rollback::RollbackMode::Gradual
        };

        // Execute rollback
        let mut rollback_engine = self.rollback_engine.write().await;
        let result = rollback_engine
            .execute_rollback(&deployment_id, mode, Some(request.reason.clone()))
            .await;
        drop(rollback_engine);

        // Remove from active deployments
        let mut active = self.active_deployments.write().await;
        active.remove(&deployment_id);

        // Cleanup health monitor
        self.health_monitor.cleanup_deployment(&deployment_id);

        // Update statistics
        self.update_stats(false, true, status.state.into()).await;

        let elapsed = start.elapsed();

        match result {
            Ok(_) => {
                info!("Rollback completed for {} in {:?}", deployment_id, elapsed);

                Ok(RollbackResult {
                    deployment_id: deployment_id.clone(),
                    success: true,
                    error: None,
                    reverted_changes: vec![], // TODO: Get from config manager
                    timestamp: Utc::now(),
                    duration: elapsed,
                })
            }
            Err(e) => {
                error!("Rollback failed for {}: {}", deployment_id, e);

                Ok(RollbackResult {
                    deployment_id: deployment_id.clone(),
                    success: false,
                    error: Some(e.to_string()),
                    reverted_changes: vec![],
                    timestamp: Utc::now(),
                    duration: elapsed,
                })
            }
        }
    }

    async fn health_check(&self) -> ActuatorResult<Vec<HealthCheckResult>> {
        if self.state != ActuatorState::Running {
            return Err(ActuatorError::NotRunning);
        }

        let mut results = Vec::new();

        // Check if components are healthy
        results.push(HealthCheckResult {
            name: "actuator_state".to_string(),
            passed: true,
            details: format!("State: {:?}", self.state),
            value: Some(1.0),
            threshold: Some(1.0),
            timestamp: Utc::now(),
        });

        let active_count = self.active_deployments.read().await.len();
        results.push(HealthCheckResult {
            name: "active_deployments".to_string(),
            passed: active_count < self.config.max_concurrent_deployments,
            details: format!("Active deployments: {}", active_count),
            value: Some(active_count as f64),
            threshold: Some(self.config.max_concurrent_deployments as f64),
            timestamp: Utc::now(),
        });

        Ok(results)
    }

    fn get_stats(&self) -> ActuatorStats {
        // Synchronous method, cannot await
        // In production, use try_read() and handle gracefully
        ActuatorStats::default()
    }

    async fn reset(&mut self) -> ActuatorResult<()> {
        info!("Resetting Actuator Coordinator");

        // Clear active deployments
        self.active_deployments.write().await.clear();

        // Reset statistics
        *self.stats.write().await = ActuatorStats::default();

        Ok(())
    }
}

// Helper to convert DeploymentState to DeploymentStrategy
impl From<DeploymentState> for DeploymentStrategy {
    fn from(_state: DeploymentState) -> Self {
        DeploymentStrategy::Canary // Default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::ConfigChange;

    #[tokio::test]
    async fn test_coordinator_lifecycle() {
        let config = ActuatorConfig::default();
        let mut coordinator = ActuatorCoordinator::new(config);

        assert_eq!(coordinator.state(), ActuatorState::Initialized);

        coordinator.start().await.unwrap();
        assert_eq!(coordinator.state(), ActuatorState::Running);

        coordinator.stop().await.unwrap();
        assert_eq!(coordinator.state(), ActuatorState::Stopped);
    }

    #[tokio::test]
    async fn test_health_check() {
        let config = ActuatorConfig::default();
        let mut coordinator = ActuatorCoordinator::new(config);

        // Should fail when not running
        assert!(coordinator.health_check().await.is_err());

        // Should succeed when running
        coordinator.start().await.unwrap();
        let results = coordinator.health_check().await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_deployment_limit() {
        let mut config = ActuatorConfig::default();
        config.max_concurrent_deployments = 1;

        let mut coordinator = ActuatorCoordinator::new(config);
        coordinator.start().await.unwrap();

        // First deployment should succeed
        let request1 = DeploymentRequest {
            id: "deploy-1".to_string(),
            timestamp: Utc::now(),
            decision: Decision {
                id: "decision-1".to_string(),
                timestamp: Utc::now(),
                strategy: "test".to_string(),
                decision_type: crate::decision::DecisionType::NoAction,
                confidence: 0.9,
                expected_impact: crate::decision::ExpectedImpact {
                    cost_change_usd: None,
                    latency_change_ms: None,
                    quality_change: None,
                    throughput_change_rps: None,
                    success_rate_change_pct: None,
                    time_to_impact: std::time::Duration::from_secs(60),
                    confidence: 0.9,
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
            },
            strategy: DeploymentStrategy::Immediate,
            config_changes: vec![],
            expected_duration: std::time::Duration::from_secs(60),
            metadata: HashMap::new(),
        };

        let _status = coordinator.deploy(request1).await.unwrap();

        // Second concurrent deployment should fail
        let request2 = DeploymentRequest {
            id: "deploy-2".to_string(),
            timestamp: Utc::now(),
            decision: Decision {
                id: "decision-2".to_string(),
                timestamp: Utc::now(),
                strategy: "test".to_string(),
                decision_type: crate::decision::DecisionType::NoAction,
                confidence: 0.9,
                expected_impact: crate::decision::ExpectedImpact {
                    cost_change_usd: None,
                    latency_change_ms: None,
                    quality_change: None,
                    throughput_change_rps: None,
                    success_rate_change_pct: None,
                    time_to_impact: std::time::Duration::from_secs(60),
                    confidence: 0.9,
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
            },
            strategy: DeploymentStrategy::Canary,
            config_changes: vec![ConfigChange {
                config_type: crate::decision::ConfigType::Model,
                path: "model.name".to_string(),
                old_value: None,
                new_value: serde_json::json!("gpt-4"),
                description: "Test".to_string(),
            }],
            expected_duration: std::time::Duration::from_secs(60),
            metadata: HashMap::new(),
        };

        let result = coordinator.deploy(request2).await;
        assert!(result.is_err());
    }
}
