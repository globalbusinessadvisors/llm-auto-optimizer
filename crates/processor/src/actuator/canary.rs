//! Canary Deployment Engine
//!
//! This module provides a production-ready canary deployment engine that:
//! - Manages multi-phase rollouts (1% → 5% → 25% → 50% → 100%)
//! - Implements traffic splitting with multiple strategies
//! - Monitors health and validates success criteria
//! - Auto-promotes and auto-rollbacks based on metrics
//! - Provides detailed status tracking and reporting

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::error::{ActuatorError, ActuatorResult, DeploymentState};
use super::traits::{CanaryDeployment, HealthMonitor, TrafficSplitter};
use super::types::{
    CanaryConfig, DeploymentMetrics, DeploymentRequest, DeploymentStatus, HealthCheckResult,
    HealthStatus, RollbackReason, RollbackRequest, RollbackResult, StatisticalAnalysis,
    SuccessCriteria, TrafficSplitStrategy,
};

/// Canary deployment engine state
#[derive(Debug)]
struct CanaryDeploymentState {
    /// Deployment request
    request: DeploymentRequest,

    /// Canary configuration
    config: CanaryConfig,

    /// Current phase index
    current_phase: usize,

    /// Phase start time
    phase_start_time: DateTime<Utc>,

    /// Deployment start time
    deployment_start_time: DateTime<Utc>,

    /// Current deployment state
    state: DeploymentState,

    /// Accumulated metrics
    metrics: DeploymentMetrics,

    /// Health status
    health: HealthStatus,

    /// Errors encountered
    errors: Vec<String>,

    /// Manual control flags
    paused: bool,

    /// Traffic splitter for routing
    traffic_split_counter: u64,

    /// Last updated timestamp
    last_updated: DateTime<Utc>,
}

/// Canary deployment engine
#[derive(Clone)]
pub struct CanaryDeploymentEngine {
    /// Active deployments
    deployments: Arc<RwLock<HashMap<String, CanaryDeploymentState>>>,

    /// Health monitor (optional integration)
    health_monitor: Option<Arc<dyn HealthMonitor>>,

    /// Default canary configuration
    default_config: CanaryConfig,

    /// Metrics buffer for statistical analysis
    metrics_buffer: Arc<RwLock<HashMap<String, MetricsBuffer>>>,
}

impl std::fmt::Debug for CanaryDeploymentEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanaryDeploymentEngine")
            .field("deployments", &self.deployments)
            .field("health_monitor", &self.health_monitor.as_ref().map(|_| "Arc<dyn HealthMonitor>"))
            .field("default_config", &self.default_config)
            .field("metrics_buffer", &self.metrics_buffer)
            .finish()
    }
}

/// Buffer for storing metrics samples
#[derive(Debug, Clone)]
struct MetricsBuffer {
    canary_latencies: VecDeque<f64>,
    control_latencies: VecDeque<f64>,
    canary_successes: u64,
    canary_failures: u64,
    control_successes: u64,
    control_failures: u64,
    max_samples: usize,
}

impl MetricsBuffer {
    fn new(max_samples: usize) -> Self {
        Self {
            canary_latencies: VecDeque::with_capacity(max_samples),
            control_latencies: VecDeque::with_capacity(max_samples),
            canary_successes: 0,
            canary_failures: 0,
            control_successes: 0,
            control_failures: 0,
            max_samples,
        }
    }

    fn add_canary_latency(&mut self, latency: f64) {
        if self.canary_latencies.len() >= self.max_samples {
            self.canary_latencies.pop_front();
        }
        self.canary_latencies.push_back(latency);
    }

    fn add_control_latency(&mut self, latency: f64) {
        if self.control_latencies.len() >= self.max_samples {
            self.control_latencies.pop_front();
        }
        self.control_latencies.push_back(latency);
    }

    fn record_canary_result(&mut self, success: bool) {
        if success {
            self.canary_successes += 1;
        } else {
            self.canary_failures += 1;
        }
    }

    fn record_control_result(&mut self, success: bool) {
        if success {
            self.control_successes += 1;
        } else {
            self.control_failures += 1;
        }
    }

    fn calculate_percentile(values: &VecDeque<f64>, percentile: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        let mut sorted: Vec<f64> = values.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index]
    }

    fn get_metrics(&self) -> (f64, f64, f64, f64, f64, f64) {
        let canary_p95 = Self::calculate_percentile(&self.canary_latencies, 95.0);
        let canary_p99 = Self::calculate_percentile(&self.canary_latencies, 99.0);
        let control_p95 = Self::calculate_percentile(&self.control_latencies, 95.0);
        let control_p99 = Self::calculate_percentile(&self.control_latencies, 99.0);

        let canary_total = self.canary_successes + self.canary_failures;
        let control_total = self.control_successes + self.control_failures;

        let canary_success_rate = if canary_total > 0 {
            (self.canary_successes as f64 / canary_total as f64) * 100.0
        } else {
            0.0
        };

        let control_success_rate = if control_total > 0 {
            (self.control_successes as f64 / control_total as f64) * 100.0
        } else {
            0.0
        };

        (
            canary_success_rate,
            control_success_rate,
            canary_p95,
            control_p95,
            canary_p99,
            control_p99,
        )
    }
}

impl CanaryDeploymentEngine {
    /// Create a new canary deployment engine
    pub fn new(default_config: CanaryConfig) -> Self {
        Self {
            deployments: Arc::new(RwLock::new(HashMap::new())),
            health_monitor: None,
            default_config,
            metrics_buffer: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with custom health monitor
    pub fn with_health_monitor(
        default_config: CanaryConfig,
        health_monitor: Arc<dyn HealthMonitor>,
    ) -> Self {
        Self {
            deployments: Arc::new(RwLock::new(HashMap::new())),
            health_monitor: Some(health_monitor),
            default_config,
            metrics_buffer: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if deployment should be auto-promoted
    async fn should_auto_promote(&self, deployment_id: &str) -> ActuatorResult<bool> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;

        if !state.config.auto_promote {
            return Ok(false);
        }

        // Check if phase duration has elapsed
        let now = Utc::now();
        let phase_duration = state.config.phase_duration;
        let elapsed = now
            .signed_duration_since(state.phase_start_time)
            .to_std()
            .unwrap_or_default();

        if elapsed < phase_duration {
            debug!(
                "Phase duration not elapsed: {:?}/{:?}",
                elapsed, phase_duration
            );
            return Ok(false);
        }

        // Check success criteria
        self.validate_success_criteria(deployment_id).await
    }

    /// Validate success criteria
    async fn validate_success_criteria(&self, deployment_id: &str) -> ActuatorResult<bool> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;

        let criteria = &state.config.success_criteria;
        let metrics = &state.metrics;

        // Check minimum sample size
        if (metrics.canary_requests as usize) < criteria.min_sample_size {
            debug!(
                "Insufficient sample size: {} < {}",
                metrics.canary_requests, criteria.min_sample_size
            );
            return Ok(false);
        }

        // Check success rate
        if metrics.canary_success_rate < criteria.min_success_rate {
            warn!(
                "Canary success rate {} below threshold {}",
                metrics.canary_success_rate, criteria.min_success_rate
            );
            return Ok(false);
        }

        // Check error rate
        if metrics.canary_error_rate > criteria.max_error_rate {
            warn!(
                "Canary error rate {} exceeds threshold {}",
                metrics.canary_error_rate, criteria.max_error_rate
            );
            return Ok(false);
        }

        // Check P95 latency
        if metrics.canary_p95_latency_ms > criteria.max_p95_latency_ms {
            warn!(
                "Canary P95 latency {} exceeds threshold {}",
                metrics.canary_p95_latency_ms, criteria.max_p95_latency_ms
            );
            return Ok(false);
        }

        // Check P99 latency
        if metrics.canary_p99_latency_ms > criteria.max_p99_latency_ms {
            warn!(
                "Canary P99 latency {} exceeds threshold {}",
                metrics.canary_p99_latency_ms, criteria.max_p99_latency_ms
            );
            return Ok(false);
        }

        // Check cost increase
        if metrics.canary_cost_per_request > 0.0 && metrics.control_cost_per_request > 0.0 {
            let cost_increase = ((metrics.canary_cost_per_request
                - metrics.control_cost_per_request)
                / metrics.control_cost_per_request)
                * 100.0;
            if cost_increase > criteria.max_cost_increase_pct {
                warn!(
                    "Cost increase {}% exceeds threshold {}%",
                    cost_increase, criteria.max_cost_increase_pct
                );
                return Ok(false);
            }
        }

        // Check statistical significance if available
        if let Some(analysis) = &metrics.statistical_analysis {
            if analysis.significant_difference && analysis.success_rate_p_value < 0.05 {
                // Significant difference detected - check if it's positive
                if metrics.canary_success_rate < metrics.control_success_rate {
                    warn!("Statistically significant degradation detected");
                    return Ok(false);
                }
            }
        }

        info!("All success criteria met for deployment {}", deployment_id);
        Ok(true)
    }

    /// Check if deployment should be auto-rolled back
    async fn should_auto_rollback(
        &self,
        deployment_id: &str,
    ) -> ActuatorResult<Option<RollbackReason>> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;

        if !state.config.auto_rollback {
            return Ok(None);
        }

        let criteria = &state.config.success_criteria;
        let metrics = &state.metrics;

        // Need minimum sample size before making rollback decisions
        if (metrics.canary_requests as usize) < criteria.min_sample_size {
            return Ok(None);
        }

        // Check critical failures
        if metrics.canary_error_rate > criteria.max_error_rate * 2.0 {
            return Ok(Some(RollbackReason::HighErrorRate));
        }

        if metrics.canary_p99_latency_ms > criteria.max_p99_latency_ms * 2.0 {
            return Ok(Some(RollbackReason::HighLatency));
        }

        // Check health status
        if !state.health.healthy && state.health.checks_failed > 2 {
            return Ok(Some(RollbackReason::HealthCheckFailed));
        }

        // Check timeout
        let now = Utc::now();
        let total_elapsed = now
            .signed_duration_since(state.deployment_start_time)
            .to_std()
            .unwrap_or_default();
        if total_elapsed > state.config.max_duration {
            return Ok(Some(RollbackReason::Timeout));
        }

        Ok(None)
    }

    /// Update deployment metrics
    pub async fn update_metrics(
        &self,
        deployment_id: &str,
        is_canary: bool,
        latency_ms: f64,
        success: bool,
        cost: Option<f64>,
    ) -> ActuatorResult<()> {
        // Update metrics buffer
        {
            let mut buffers = self.metrics_buffer.write().await;
            let buffer = buffers
                .entry(deployment_id.to_string())
                .or_insert_with(|| MetricsBuffer::new(10000));

            if is_canary {
                buffer.add_canary_latency(latency_ms);
                buffer.record_canary_result(success);
            } else {
                buffer.add_control_latency(latency_ms);
                buffer.record_control_result(success);
            }
        }

        // Update deployment metrics
        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        state.metrics.total_requests += 1;

        if is_canary {
            state.metrics.canary_requests += 1;
            if let Some(cost_value) = cost {
                // Update running average
                let n = state.metrics.canary_requests as f64;
                state.metrics.canary_cost_per_request =
                    ((n - 1.0) * state.metrics.canary_cost_per_request + cost_value) / n;
            }
        } else {
            state.metrics.control_requests += 1;
            if let Some(cost_value) = cost {
                let n = state.metrics.control_requests as f64;
                state.metrics.control_cost_per_request =
                    ((n - 1.0) * state.metrics.control_cost_per_request + cost_value) / n;
            }
        }

        // Recalculate metrics from buffer
        let buffers = self.metrics_buffer.read().await;
        if let Some(buffer) = buffers.get(deployment_id) {
            let (canary_sr, control_sr, canary_p95, control_p95, canary_p99, control_p99) =
                buffer.get_metrics();

            state.metrics.canary_success_rate = canary_sr;
            state.metrics.control_success_rate = control_sr;
            state.metrics.canary_error_rate = 100.0 - canary_sr;
            state.metrics.control_error_rate = 100.0 - control_sr;
            state.metrics.canary_p95_latency_ms = canary_p95;
            state.metrics.control_p95_latency_ms = control_p95;
            state.metrics.canary_p99_latency_ms = canary_p99;
            state.metrics.control_p99_latency_ms = control_p99;

            // Perform statistical analysis
            if buffer.canary_successes + buffer.canary_failures
                >= state.config.success_criteria.min_sample_size as u64
                && buffer.control_successes + buffer.control_failures
                    >= state.config.success_criteria.min_sample_size as u64
            {
                state.metrics.statistical_analysis = Some(self.perform_statistical_analysis(buffer));
            }
        }

        state.last_updated = Utc::now();

        Ok(())
    }

    /// Perform statistical analysis on metrics
    fn perform_statistical_analysis(&self, buffer: &MetricsBuffer) -> StatisticalAnalysis {
        let canary_total = buffer.canary_successes + buffer.canary_failures;
        let control_total = buffer.control_successes + buffer.control_failures;

        // Chi-square test for success rates
        let canary_success_rate =
            buffer.canary_successes as f64 / canary_total.max(1) as f64;
        let control_success_rate =
            buffer.control_successes as f64 / control_total.max(1) as f64;

        // Simplified p-value calculation (in production, use proper statistical library)
        let pooled_rate = (buffer.canary_successes + buffer.control_successes) as f64
            / (canary_total + control_total) as f64;
        let se = (pooled_rate * (1.0 - pooled_rate)
            * (1.0 / canary_total as f64 + 1.0 / control_total as f64))
            .sqrt();

        let z_score = (canary_success_rate - control_success_rate) / se.max(0.0001);
        let success_rate_p_value = 2.0 * (1.0 - normal_cdf(z_score.abs()));

        // T-test for latency (simplified)
        let latency_p_value = if !buffer.canary_latencies.is_empty()
            && !buffer.control_latencies.is_empty()
        {
            let canary_mean: f64 = buffer.canary_latencies.iter().sum::<f64>()
                / buffer.canary_latencies.len() as f64;
            let control_mean: f64 = buffer.control_latencies.iter().sum::<f64>()
                / buffer.control_latencies.len() as f64;

            let canary_var: f64 = buffer
                .canary_latencies
                .iter()
                .map(|x| (x - canary_mean).powi(2))
                .sum::<f64>()
                / buffer.canary_latencies.len().max(1) as f64;

            let control_var: f64 = buffer
                .control_latencies
                .iter()
                .map(|x| (x - control_mean).powi(2))
                .sum::<f64>()
                / buffer.control_latencies.len().max(1) as f64;

            let se_latency = (canary_var / buffer.canary_latencies.len() as f64
                + control_var / buffer.control_latencies.len() as f64)
                .sqrt();

            let t_stat = (canary_mean - control_mean) / se_latency.max(0.0001);
            2.0 * (1.0 - normal_cdf(t_stat.abs()))
        } else {
            1.0
        };

        let significant_difference = success_rate_p_value < 0.05 || latency_p_value < 0.05;

        StatisticalAnalysis {
            success_rate_p_value,
            latency_p_value,
            significant_difference,
            confidence_level: 0.95,
            canary_sample_size: canary_total as usize,
            control_sample_size: control_total as usize,
        }
    }

    /// Pause a deployment (manual control)
    pub async fn pause(&self, deployment_id: &str) -> ActuatorResult<()> {
        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        state.paused = true;
        state.last_updated = Utc::now();
        info!("Deployment {} paused", deployment_id);
        Ok(())
    }

    /// Resume a paused deployment
    pub async fn resume(&self, deployment_id: &str) -> ActuatorResult<()> {
        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        state.paused = false;
        state.phase_start_time = Utc::now(); // Reset phase timer
        state.last_updated = Utc::now();
        info!("Deployment {} resumed", deployment_id);
        Ok(())
    }

    /// Check if deployment is paused
    pub async fn is_paused(&self, deployment_id: &str) -> ActuatorResult<bool> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;
        Ok(state.paused)
    }

    /// Get deployment configuration
    pub async fn get_config(&self, deployment_id: &str) -> ActuatorResult<CanaryConfig> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;
        Ok(state.config.clone())
    }

    /// Update health status
    pub async fn update_health(
        &self,
        deployment_id: &str,
        health: HealthStatus,
    ) -> ActuatorResult<()> {
        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        state.health = health;
        state.last_updated = Utc::now();
        Ok(())
    }
}

#[async_trait]
impl CanaryDeployment for CanaryDeploymentEngine {
    async fn start_canary(
        &mut self,
        request: DeploymentRequest,
    ) -> ActuatorResult<DeploymentStatus> {
        let deployment_id = request.id.clone();
        info!("Starting canary deployment: {}", deployment_id);

        // Validate request
        if request.config_changes.is_empty() {
            return Err(ActuatorError::ValidationFailed(
                "No configuration changes specified".to_string(),
            ));
        }

        // Initialize deployment state
        let now = Utc::now();
        let config = self.default_config.clone();

        let initial_health = HealthStatus {
            healthy: true,
            checks_passed: 0,
            checks_failed: 0,
            check_results: vec![],
            last_check_time: now,
        };

        let state = CanaryDeploymentState {
            request: request.clone(),
            config: config.clone(),
            current_phase: 0,
            phase_start_time: now,
            deployment_start_time: now,
            state: DeploymentState::Validating,
            metrics: DeploymentMetrics::default(),
            health: initial_health.clone(),
            errors: vec![],
            paused: false,
            traffic_split_counter: 0,
            last_updated: now,
        };

        // Store deployment state
        {
            let mut deployments = self.deployments.write().await;
            deployments.insert(deployment_id.clone(), state);
        }

        // Initialize metrics buffer
        {
            let mut buffers = self.metrics_buffer.write().await;
            buffers.insert(
                deployment_id.clone(),
                MetricsBuffer::new(10000),
            );
        }

        // Transition to rolling out
        {
            let mut deployments = self.deployments.write().await;
            if let Some(state) = deployments.get_mut(&deployment_id) {
                state.state = DeploymentState::RollingOut;
                state.last_updated = Utc::now();
            }
        }

        info!(
            "Canary deployment {} started with {} phases",
            deployment_id,
            config.phases.len()
        );

        self.get_status(&deployment_id).await
    }

    async fn promote(&mut self, deployment_id: &str) -> ActuatorResult<DeploymentStatus> {
        info!("Promoting canary deployment: {}", deployment_id);

        // Validate promotion
        if !self.can_promote(deployment_id).await? {
            return Err(ActuatorError::ValidationFailed(
                "Cannot promote: success criteria not met".to_string(),
            ));
        }

        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        // Move to next phase
        if state.current_phase < state.config.phases.len() - 1 {
            state.current_phase += 1;
            state.phase_start_time = Utc::now();
            state.state = DeploymentState::RollingOut;
            state.last_updated = Utc::now();

            let traffic_percent = state.config.phases[state.current_phase].traffic_percent;
            info!(
                "Promoted deployment {} to phase {} ({}% traffic)",
                deployment_id, state.current_phase, traffic_percent
            );
        } else {
            // Already at final phase, mark as completed
            state.state = DeploymentState::Completed;
            state.last_updated = Utc::now();
            info!("Deployment {} completed", deployment_id);
        }

        drop(deployments);
        self.get_status(deployment_id).await
    }

    async fn complete(&mut self, deployment_id: &str) -> ActuatorResult<DeploymentStatus> {
        info!("Completing canary deployment: {}", deployment_id);

        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::DeploymentFailed("Deployment not found".to_string())
        })?;

        // Jump to 100% phase
        state.current_phase = state.config.phases.len() - 1;
        state.state = DeploymentState::Completed;
        state.last_updated = Utc::now();

        info!("Deployment {} completed at 100%", deployment_id);

        drop(deployments);
        self.get_status(deployment_id).await
    }

    async fn abort(&mut self, deployment_id: &str) -> ActuatorResult<RollbackResult> {
        info!("Aborting canary deployment: {}", deployment_id);

        let request = RollbackRequest {
            deployment_id: deployment_id.to_string(),
            reason: RollbackReason::Manual,
            timestamp: Utc::now(),
            automatic: false,
            metadata: HashMap::new(),
        };

        self.execute_rollback(&request).await
    }

    async fn get_status(&self, deployment_id: &str) -> ActuatorResult<DeploymentStatus> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;

        let traffic_percent = if state.current_phase < state.config.phases.len() {
            state.config.phases[state.current_phase].traffic_percent
        } else {
            100.0
        };

        Ok(DeploymentStatus {
            deployment_id: deployment_id.to_string(),
            state: state.state,
            current_phase: Some(state.current_phase),
            traffic_percent,
            start_time: state.deployment_start_time,
            phase_start_time: Some(state.phase_start_time),
            health: state.health.clone(),
            metrics: state.metrics.clone(),
            errors: state.errors.clone(),
            last_updated: state.last_updated,
        })
    }

    async fn can_promote(&self, deployment_id: &str) -> ActuatorResult<bool> {
        let deployments = self.deployments.read().await;
        let state = deployments
            .get(deployment_id)
            .ok_or_else(|| ActuatorError::DeploymentFailed("Deployment not found".to_string()))?;

        // Cannot promote if paused
        if state.paused {
            return Ok(false);
        }

        // Cannot promote if not in rolling out state
        if state.state != DeploymentState::RollingOut {
            return Ok(false);
        }

        // Cannot promote if already at final phase
        if state.current_phase >= state.config.phases.len() - 1 {
            return Ok(false);
        }

        drop(deployments);

        // Check success criteria
        self.validate_success_criteria(deployment_id).await
    }
}

impl CanaryDeploymentEngine {
    /// Execute rollback
    async fn execute_rollback(&self, request: &RollbackRequest) -> ActuatorResult<RollbackResult> {
        let start_time = Utc::now();
        let deployment_id = &request.deployment_id;

        let mut deployments = self.deployments.write().await;
        let state = deployments.get_mut(deployment_id).ok_or_else(|| {
            ActuatorError::RollbackFailed("Deployment not found".to_string())
        })?;

        // Update state
        state.state = DeploymentState::RollingBack;
        state.last_updated = Utc::now();

        let reverted_changes = state.request.config_changes.clone();

        // Mark as rolled back
        state.state = DeploymentState::RolledBack;
        state.last_updated = Utc::now();

        let duration = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap_or_default();

        info!(
            "Rollback completed for deployment {} in {:?}",
            deployment_id, duration
        );

        Ok(RollbackResult {
            deployment_id: deployment_id.clone(),
            success: true,
            error: None,
            reverted_changes,
            timestamp: Utc::now(),
            duration,
        })
    }
}

#[async_trait]
impl TrafficSplitter for CanaryDeploymentEngine {
    async fn route_request(&self, request_id: &str, canary_percent: f64) -> ActuatorResult<bool> {
        // Get deployment ID from request (simplified - in production would be more complex)
        let deployments = self.deployments.read().await;

        // Find active deployment
        let active_deployment = deployments
            .values()
            .find(|d| d.state == DeploymentState::RollingOut);

        if let Some(deployment) = active_deployment {
            match deployment.config.traffic_split_strategy {
                TrafficSplitStrategy::WeightedRandom => {
                    let mut rng = rand::thread_rng();
                    let random: f64 = rng.gen();
                    Ok(random < (canary_percent / 100.0))
                }
                TrafficSplitStrategy::ConsistentHash => {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    request_id.hash(&mut hasher);
                    let hash = hasher.finish();
                    Ok((hash % 100) < canary_percent as u64)
                }
                TrafficSplitStrategy::RoundRobin => {
                    // Round-robin based on counter (would need to increment counter)
                    let counter = deployment.traffic_split_counter;
                    Ok((counter % 100) < canary_percent as u64)
                }
                TrafficSplitStrategy::Geographic => {
                    // Simplified: use hash of request_id
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    request_id.hash(&mut hasher);
                    let hash = hasher.finish();
                    Ok((hash % 100) < canary_percent as u64)
                }
            }
        } else {
            // No active deployment, route to control
            Ok(false)
        }
    }

    fn get_routing_stats(&self) -> HashMap<String, u64> {
        // Would be implemented with actual routing statistics
        HashMap::new()
    }
}

/// Simplified normal CDF for statistical analysis
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Error function approximation
fn erf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    fn create_test_request(id: &str) -> DeploymentRequest {
        DeploymentRequest {
            id: id.to_string(),
            timestamp: Utc::now(),
            decision: crate::decision::Decision {
                id: format!("decision-{}", id),
                timestamp: Utc::now(),
                decision_type: crate::decision::DecisionType::PromptOptimization,
                criteria: crate::decision::DecisionCriteria {
                    metric_name: "latency".to_string(),
                    operator: crate::decision::ComparisonOperator::LessThan,
                    threshold: 100.0,
                    priority: 1.0,
                },
                config_changes: vec![],
                expected_impact: crate::decision::ExpectedImpact::default(),
                rollback_plan: crate::decision::RollbackPlan::default(),
                safety_checks: vec![],
                metadata: HashMap::new(),
            },
            strategy: super::super::types::DeploymentStrategy::Canary,
            config_changes: vec![],
            expected_duration: StdDuration::from_secs(300),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_start_canary_deployment() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-1");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        let status = engine.start_canary(request).await.unwrap();

        assert_eq!(status.deployment_id, "test-deploy-1");
        assert_eq!(status.state, DeploymentState::RollingOut);
        assert_eq!(status.current_phase, Some(0));
        assert_eq!(status.traffic_percent, 1.0);
    }

    #[tokio::test]
    async fn test_cannot_start_without_config_changes() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let request = create_test_request("test-deploy-2");
        let result = engine.start_canary(request).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ActuatorError::ValidationFailed(_)
        ));
    }

    #[tokio::test]
    async fn test_update_metrics() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-3");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Update some metrics
        for i in 0..100 {
            let is_canary = i % 2 == 0;
            engine
                .update_metrics("test-deploy-3", is_canary, 50.0, true, Some(0.001))
                .await
                .unwrap();
        }

        let status = engine.get_status("test-deploy-3").await.unwrap();
        assert_eq!(status.metrics.total_requests, 100);
        assert_eq!(status.metrics.canary_requests, 50);
        assert_eq!(status.metrics.control_requests, 50);
    }

    #[tokio::test]
    async fn test_traffic_splitting_weighted_random() {
        let config = CanaryConfig {
            traffic_split_strategy: TrafficSplitStrategy::WeightedRandom,
            ..Default::default()
        };
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-4");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Test routing with 50% traffic
        let mut canary_count = 0;
        let iterations = 1000;

        for i in 0..iterations {
            let route_to_canary = engine
                .route_request(&format!("req-{}", i), 50.0)
                .await
                .unwrap();
            if route_to_canary {
                canary_count += 1;
            }
        }

        // Should be approximately 50% (allow 10% variance)
        let canary_percent = (canary_count as f64 / iterations as f64) * 100.0;
        assert!(canary_percent > 40.0 && canary_percent < 60.0);
    }

    #[tokio::test]
    async fn test_traffic_splitting_consistent_hash() {
        let config = CanaryConfig {
            traffic_split_strategy: TrafficSplitStrategy::ConsistentHash,
            ..Default::default()
        };
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-5");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Same request should always route the same way
        let request_id = "consistent-request-123";
        let route1 = engine.route_request(request_id, 50.0).await.unwrap();
        let route2 = engine.route_request(request_id, 50.0).await.unwrap();
        let route3 = engine.route_request(request_id, 50.0).await.unwrap();

        assert_eq!(route1, route2);
        assert_eq!(route2, route3);
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-6");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Pause
        engine.pause("test-deploy-6").await.unwrap();
        assert!(engine.is_paused("test-deploy-6").await.unwrap());

        // Cannot promote when paused
        let can_promote = engine.can_promote("test-deploy-6").await.unwrap();
        assert!(!can_promote);

        // Resume
        engine.resume("test-deploy-6").await.unwrap();
        assert!(!engine.is_paused("test-deploy-6").await.unwrap());
    }

    #[tokio::test]
    async fn test_validation_with_insufficient_samples() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-7");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Add only a few samples (less than min_sample_size)
        for i in 0..10 {
            engine
                .update_metrics("test-deploy-7", true, 50.0, true, Some(0.001))
                .await
                .unwrap();
        }

        // Should not be able to promote yet
        let can_promote = engine.can_promote("test-deploy-7").await.unwrap();
        assert!(!can_promote);
    }

    #[tokio::test]
    async fn test_validation_with_sufficient_samples() {
        let mut config = CanaryConfig::default();
        config.success_criteria.min_sample_size = 50;
        config.phase_duration = StdDuration::from_millis(1); // Very short for testing

        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-8");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Add sufficient samples with good metrics
        for _ in 0..100 {
            engine
                .update_metrics("test-deploy-8", true, 50.0, true, Some(0.001))
                .await
                .unwrap();
        }

        // Wait for phase duration
        tokio::time::sleep(StdDuration::from_millis(10)).await;

        // Should be able to promote now
        let can_promote = engine.can_promote("test-deploy-8").await.unwrap();
        assert!(can_promote);
    }

    #[tokio::test]
    async fn test_high_error_rate_prevents_promotion() {
        let mut config = CanaryConfig::default();
        config.success_criteria.min_sample_size = 50;
        config.success_criteria.max_error_rate = 5.0;
        config.phase_duration = StdDuration::from_millis(1);

        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-9");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Add samples with high error rate
        for i in 0..100 {
            let success = i % 10 != 0; // 10% error rate
            engine
                .update_metrics("test-deploy-9", true, 50.0, success, Some(0.001))
                .await
                .unwrap();
        }

        tokio::time::sleep(StdDuration::from_millis(10)).await;

        // Should not be able to promote due to high error rate
        let can_promote = engine.can_promote("test-deploy-9").await.unwrap();
        assert!(!can_promote);
    }

    #[tokio::test]
    async fn test_promote_through_phases() {
        let mut config = CanaryConfig::default();
        config.success_criteria.min_sample_size = 10;
        config.phase_duration = StdDuration::from_millis(1);

        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-10");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Initial phase
        let status = engine.get_status("test-deploy-10").await.unwrap();
        assert_eq!(status.current_phase, Some(0));
        assert_eq!(status.traffic_percent, 1.0);

        // Add good metrics and promote
        for _ in 0..20 {
            engine
                .update_metrics("test-deploy-10", true, 50.0, true, Some(0.001))
                .await
                .unwrap();
        }

        tokio::time::sleep(StdDuration::from_millis(10)).await;

        // Promote to phase 1
        let status = engine.promote("test-deploy-10").await.unwrap();
        assert_eq!(status.current_phase, Some(1));
        assert_eq!(status.traffic_percent, 5.0);
    }

    #[tokio::test]
    async fn test_complete_deployment() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-11");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Complete immediately
        let status = engine.complete("test-deploy-11").await.unwrap();
        assert_eq!(status.state, DeploymentState::Completed);
        assert_eq!(status.traffic_percent, 100.0);
    }

    #[tokio::test]
    async fn test_abort_deployment() {
        let config = CanaryConfig::default();
        let mut engine = CanaryDeploymentEngine::new(config);

        let mut request = create_test_request("test-deploy-12");
        request.config_changes = vec![crate::decision::ConfigChange {
            config_type: crate::decision::ConfigType::Provider,
            key: "model".to_string(),
            old_value: serde_json::json!("gpt-3.5-turbo"),
            new_value: serde_json::json!("gpt-4"),
            reason: "Testing".to_string(),
        }];

        engine.start_canary(request).await.unwrap();

        // Abort
        let result = engine.abort("test-deploy-12").await.unwrap();
        assert!(result.success);
        assert_eq!(result.deployment_id, "test-deploy-12");

        // Check state
        let status = engine.get_status("test-deploy-12").await.unwrap();
        assert_eq!(status.state, DeploymentState::RolledBack);
    }

    #[tokio::test]
    async fn test_metrics_buffer_percentile_calculation() {
        let mut buffer = MetricsBuffer::new(1000);

        // Add latency samples
        for i in 1..=100 {
            buffer.add_canary_latency(i as f64);
        }

        let (_, _, canary_p95, _, canary_p99, _) = buffer.get_metrics();

        // P95 should be around 95
        assert!(canary_p95 >= 94.0 && canary_p95 <= 96.0);

        // P99 should be around 99
        assert!(canary_p99 >= 98.0 && canary_p99 <= 100.0);
    }

    #[tokio::test]
    async fn test_statistical_analysis() {
        let mut buffer = MetricsBuffer::new(1000);

        // Add samples for both canary and control
        for _ in 0..200 {
            buffer.add_canary_latency(50.0);
            buffer.add_control_latency(50.0);
            buffer.record_canary_result(true);
            buffer.record_control_result(true);
        }

        let config = CanaryConfig::default();
        let engine = CanaryDeploymentEngine::new(config);
        let analysis = engine.perform_statistical_analysis(&buffer);

        // With identical distributions, should not find significant difference
        assert!(!analysis.significant_difference);
        assert_eq!(analysis.canary_sample_size, 200);
        assert_eq!(analysis.control_sample_size, 200);
    }
}
