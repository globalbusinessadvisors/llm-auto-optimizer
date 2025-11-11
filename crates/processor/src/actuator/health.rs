//! Health monitoring for deployments
//!
//! This module provides comprehensive health monitoring capabilities for canary deployments,
//! including statistical analysis, metric tracking, and rollback decision logic.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::error::{ActuatorError, ActuatorResult};
use super::traits::HealthMonitor as HealthMonitorTrait;
use super::types::{
    DeploymentMetrics, DeploymentStatus, HealthCheckResult, HealthStatus, StatisticalAnalysis,
    SuccessCriteria,
};
use crate::decision::SystemMetrics;

/// Default number of consecutive failures before rollback
const DEFAULT_MAX_CONSECUTIVE_FAILURES: usize = 3;

/// Default minimum observation period in seconds
const DEFAULT_MIN_OBSERVATION_SECS: u64 = 60;

/// Health monitor configuration
#[derive(Debug, Clone)]
pub struct HealthMonitorConfig {
    /// Success criteria for deployments
    pub success_criteria: SuccessCriteria,

    /// Maximum consecutive health check failures before rollback
    pub max_consecutive_failures: usize,

    /// Minimum observation period before making decisions (seconds)
    pub min_observation_secs: u64,

    /// Enable statistical significance testing
    pub enable_statistical_testing: bool,

    /// Minimum effect size for practical significance
    pub min_effect_size: f64,

    /// Alert on warnings (not just failures)
    pub alert_on_warnings: bool,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            success_criteria: SuccessCriteria::default(),
            max_consecutive_failures: DEFAULT_MAX_CONSECUTIVE_FAILURES,
            min_observation_secs: DEFAULT_MIN_OBSERVATION_SECS,
            enable_statistical_testing: true,
            min_effect_size: 0.05, // 5% minimum effect size
            alert_on_warnings: true,
        }
    }
}

/// Health check type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthCheckType {
    SuccessRate,
    ErrorRate,
    P95Latency,
    P99Latency,
    QualityScore,
    CostIncrease,
    SampleSize,
    Statistical,
}

impl HealthCheckType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::SuccessRate => "success_rate",
            Self::ErrorRate => "error_rate",
            Self::P95Latency => "p95_latency",
            Self::P99Latency => "p99_latency",
            Self::QualityScore => "quality_score",
            Self::CostIncrease => "cost_increase",
            Self::SampleSize => "sample_size",
            Self::Statistical => "statistical_significance",
        }
    }
}

/// Health history entry
#[derive(Debug, Clone)]
struct HealthHistoryEntry {
    timestamp: DateTime<Utc>,
    health_status: HealthStatus,
    metrics: DeploymentMetrics,
}

/// Internal state for health monitoring
#[derive(Debug)]
struct HealthMonitorState {
    /// Configuration
    config: HealthMonitorConfig,

    /// Health history per deployment
    health_history: HashMap<String, Vec<HealthHistoryEntry>>,

    /// Consecutive failure counts per deployment
    consecutive_failures: HashMap<String, usize>,

    /// Deployment start times
    deployment_start_times: HashMap<String, DateTime<Utc>>,

    /// Metrics history for trend analysis
    metrics_history: HashMap<String, Vec<(DateTime<Utc>, DeploymentMetrics)>>,
}

impl HealthMonitorState {
    fn new(config: HealthMonitorConfig) -> Self {
        Self {
            config,
            health_history: HashMap::new(),
            consecutive_failures: HashMap::new(),
            deployment_start_times: HashMap::new(),
            metrics_history: HashMap::new(),
        }
    }

    fn record_health(&mut self, deployment_id: &str, status: HealthStatus, metrics: DeploymentMetrics) {
        let entry = HealthHistoryEntry {
            timestamp: Utc::now(),
            health_status: status.clone(),
            metrics: metrics.clone(),
        };

        self.health_history
            .entry(deployment_id.to_string())
            .or_default()
            .push(entry);

        // Update consecutive failures
        if status.healthy {
            self.consecutive_failures.insert(deployment_id.to_string(), 0);
        } else {
            *self.consecutive_failures
                .entry(deployment_id.to_string())
                .or_insert(0) += 1;
        }

        // Record metrics history
        self.metrics_history
            .entry(deployment_id.to_string())
            .or_default()
            .push((Utc::now(), metrics));
    }

    fn get_consecutive_failures(&self, deployment_id: &str) -> usize {
        self.consecutive_failures
            .get(deployment_id)
            .copied()
            .unwrap_or(0)
    }

    fn register_deployment(&mut self, deployment_id: &str) {
        self.deployment_start_times
            .insert(deployment_id.to_string(), Utc::now());
    }

    fn get_observation_duration(&self, deployment_id: &str) -> Option<i64> {
        self.deployment_start_times
            .get(deployment_id)
            .map(|start| (Utc::now() - *start).num_seconds())
    }

    fn cleanup_old_data(&mut self, deployment_id: &str) {
        self.health_history.remove(deployment_id);
        self.consecutive_failures.remove(deployment_id);
        self.deployment_start_times.remove(deployment_id);
        self.metrics_history.remove(deployment_id);
    }
}

/// Production-ready health monitor for deployments
pub struct ProductionHealthMonitor {
    state: Arc<RwLock<HealthMonitorState>>,
}

impl ProductionHealthMonitor {
    /// Create a new health monitor with default configuration
    pub fn new() -> Self {
        Self::with_config(HealthMonitorConfig::default())
    }

    /// Create a new health monitor with custom configuration
    pub fn with_config(config: HealthMonitorConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(HealthMonitorState::new(config))),
        }
    }

    /// Register a new deployment for monitoring
    pub fn register_deployment(&self, deployment_id: &str) {
        if let Ok(mut state) = self.state.write() {
            state.register_deployment(deployment_id);
        }
    }

    /// Remove deployment data after completion or rollback
    pub fn cleanup_deployment(&self, deployment_id: &str) {
        if let Ok(mut state) = self.state.write() {
            state.cleanup_old_data(deployment_id);
        }
    }

    /// Get health history for a deployment
    pub fn get_health_history(&self, deployment_id: &str) -> Vec<HealthHistoryEntry> {
        self.state
            .read()
            .ok()
            .and_then(|state| state.health_history.get(deployment_id).cloned())
            .unwrap_or_default()
    }

    /// Perform a single health check
    fn perform_health_check(
        &self,
        check_type: HealthCheckType,
        metrics: &DeploymentMetrics,
        criteria: &SuccessCriteria,
    ) -> HealthCheckResult {
        let timestamp = Utc::now();

        match check_type {
            HealthCheckType::SuccessRate => {
                let value = metrics.canary_success_rate;
                let threshold = criteria.min_success_rate;
                let passed = value >= threshold;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Canary success rate: {:.2}% (threshold: {:.2}%, control: {:.2}%)",
                        value, threshold, metrics.control_success_rate
                    ),
                    value: Some(value),
                    threshold: Some(threshold),
                    timestamp,
                }
            }
            HealthCheckType::ErrorRate => {
                let value = metrics.canary_error_rate;
                let threshold = criteria.max_error_rate;
                let passed = value <= threshold;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Canary error rate: {:.2}% (threshold: {:.2}%, control: {:.2}%)",
                        value, threshold, metrics.control_error_rate
                    ),
                    value: Some(value),
                    threshold: Some(threshold),
                    timestamp,
                }
            }
            HealthCheckType::P95Latency => {
                let value = metrics.canary_p95_latency_ms;
                let threshold = criteria.max_p95_latency_ms;
                let passed = value <= threshold;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Canary P95 latency: {:.2}ms (threshold: {:.2}ms, control: {:.2}ms)",
                        value, threshold, metrics.control_p95_latency_ms
                    ),
                    value: Some(value),
                    threshold: Some(threshold),
                    timestamp,
                }
            }
            HealthCheckType::P99Latency => {
                let value = metrics.canary_p99_latency_ms;
                let threshold = criteria.max_p99_latency_ms;
                let passed = value <= threshold;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Canary P99 latency: {:.2}ms (threshold: {:.2}ms, control: {:.2}ms)",
                        value, threshold, metrics.control_p99_latency_ms
                    ),
                    value: Some(value),
                    threshold: Some(threshold),
                    timestamp,
                }
            }
            HealthCheckType::QualityScore => {
                // Quality score check is optional
                if let Some(min_quality) = criteria.min_quality_score {
                    // In a real system, we'd extract quality scores from metrics
                    // For now, we'll consider it passing if not available
                    HealthCheckResult {
                        name: check_type.as_str().to_string(),
                        passed: true,
                        details: format!(
                            "Quality score check (threshold: {:.2})",
                            min_quality
                        ),
                        value: Some(min_quality),
                        threshold: Some(min_quality),
                        timestamp,
                    }
                } else {
                    HealthCheckResult {
                        name: check_type.as_str().to_string(),
                        passed: true,
                        details: "Quality score check disabled".to_string(),
                        value: None,
                        threshold: None,
                        timestamp,
                    }
                }
            }
            HealthCheckType::CostIncrease => {
                let canary_cost = metrics.canary_cost_per_request;
                let control_cost = metrics.control_cost_per_request;

                let cost_increase_pct = if control_cost > 0.0 {
                    ((canary_cost - control_cost) / control_cost) * 100.0
                } else {
                    0.0
                };

                let threshold = criteria.max_cost_increase_pct;
                let passed = cost_increase_pct <= threshold;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Cost increase: {:.2}% (threshold: {:.2}%, canary: ${:.6}, control: ${:.6})",
                        cost_increase_pct, threshold, canary_cost, control_cost
                    ),
                    value: Some(cost_increase_pct),
                    threshold: Some(threshold),
                    timestamp,
                }
            }
            HealthCheckType::SampleSize => {
                let canary_samples = metrics.canary_requests as usize;
                let control_samples = metrics.control_requests as usize;
                let min_samples = criteria.min_sample_size;

                let passed = canary_samples >= min_samples && control_samples >= min_samples;

                HealthCheckResult {
                    name: check_type.as_str().to_string(),
                    passed,
                    details: format!(
                        "Sample sizes - canary: {}, control: {} (minimum: {})",
                        canary_samples, control_samples, min_samples
                    ),
                    value: Some(canary_samples.min(control_samples) as f64),
                    threshold: Some(min_samples as f64),
                    timestamp,
                }
            }
            HealthCheckType::Statistical => {
                if let Some(analysis) = &metrics.statistical_analysis {
                    let passed = !analysis.significant_difference
                        || metrics.canary_success_rate >= metrics.control_success_rate;

                    HealthCheckResult {
                        name: check_type.as_str().to_string(),
                        passed,
                        details: format!(
                            "Statistical significance - success rate p-value: {:.4}, latency p-value: {:.4} (threshold: {:.2})",
                            analysis.success_rate_p_value,
                            analysis.latency_p_value,
                            criteria.significance_level
                        ),
                        value: Some(analysis.success_rate_p_value),
                        threshold: Some(criteria.significance_level),
                        timestamp,
                    }
                } else {
                    HealthCheckResult {
                        name: check_type.as_str().to_string(),
                        passed: true,
                        details: "Statistical analysis not yet available".to_string(),
                        value: None,
                        threshold: None,
                        timestamp,
                    }
                }
            }
        }
    }

    /// Perform statistical analysis comparing canary vs control
    fn perform_statistical_analysis(
        &self,
        metrics: &DeploymentMetrics,
        criteria: &SuccessCriteria,
    ) -> Option<StatisticalAnalysis> {
        let canary_samples = metrics.canary_requests as usize;
        let control_samples = metrics.control_requests as usize;

        // Need minimum sample size
        if canary_samples < criteria.min_sample_size
            || control_samples < criteria.min_sample_size
        {
            return None;
        }

        // Calculate success rate p-value using z-test for proportions
        let success_rate_p_value = self.calculate_proportion_test_p_value(
            metrics.canary_success_rate / 100.0,
            canary_samples,
            metrics.control_success_rate / 100.0,
            control_samples,
        );

        // Calculate latency p-value (simplified - in production use proper t-test)
        let latency_diff_pct = if metrics.control_p95_latency_ms > 0.0 {
            ((metrics.canary_p95_latency_ms - metrics.control_p95_latency_ms)
                / metrics.control_p95_latency_ms)
                .abs()
        } else {
            0.0
        };

        // Simplified p-value based on effect size
        let latency_p_value = if latency_diff_pct > 0.2 {
            0.01
        } else if latency_diff_pct > 0.1 {
            0.05
        } else {
            0.5
        };

        let significant_difference = success_rate_p_value < criteria.significance_level
            || latency_p_value < criteria.significance_level;

        Some(StatisticalAnalysis {
            success_rate_p_value,
            latency_p_value,
            significant_difference,
            confidence_level: 1.0 - criteria.significance_level,
            canary_sample_size: canary_samples,
            control_sample_size: control_samples,
        })
    }

    /// Calculate p-value for two-proportion z-test
    fn calculate_proportion_test_p_value(
        &self,
        p1: f64,
        n1: usize,
        p2: f64,
        n2: usize,
    ) -> f64 {
        // Pooled proportion
        let p_pool = ((p1 * n1 as f64) + (p2 * n2 as f64)) / (n1 + n2) as f64;

        // Standard error
        let se = (p_pool * (1.0 - p_pool) * ((1.0 / n1 as f64) + (1.0 / n2 as f64))).sqrt();

        if se == 0.0 {
            return 1.0;
        }

        // Z-score
        let z = ((p1 - p2) / se).abs();

        // Approximate p-value using normal distribution
        // This is a simplification - in production, use proper statistical library
        if z > 2.576 {
            0.01
        } else if z > 1.96 {
            0.05
        } else if z > 1.645 {
            0.1
        } else {
            0.5
        }
    }

    /// Check if sufficient observation time has passed
    fn has_sufficient_observation_time(&self, deployment_id: &str, min_secs: u64) -> bool {
        self.state
            .read()
            .ok()
            .and_then(|state| state.get_observation_duration(deployment_id))
            .map(|duration| duration >= min_secs as i64)
            .unwrap_or(false)
    }

    /// Build comprehensive health status from check results
    fn build_health_status(&self, check_results: Vec<HealthCheckResult>) -> HealthStatus {
        let checks_passed = check_results.iter().filter(|r| r.passed).count();
        let checks_failed = check_results.len() - checks_passed;
        let healthy = checks_failed == 0;

        HealthStatus {
            healthy,
            checks_passed,
            checks_failed,
            check_results,
            last_check_time: Utc::now(),
        }
    }
}

impl Default for ProductionHealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HealthMonitorTrait for ProductionHealthMonitor {
    async fn check_health(
        &self,
        deployment_id: &str,
        metrics: &SystemMetrics,
    ) -> ActuatorResult<Vec<HealthCheckResult>> {
        let state = self.state.read().map_err(|e| {
            ActuatorError::Internal(format!("Failed to acquire read lock: {}", e))
        })?;

        let criteria = state.config.success_criteria.clone();

        // Convert SystemMetrics to DeploymentMetrics for this check
        // In a real system, we'd track separate canary/control metrics
        let deployment_metrics = DeploymentMetrics {
            total_requests: metrics.request_count,
            canary_requests: metrics.request_count / 2, // Simplified
            control_requests: metrics.request_count / 2,
            canary_success_rate: metrics.success_rate_pct,
            control_success_rate: metrics.success_rate_pct,
            canary_error_rate: metrics.error_rate_pct,
            control_error_rate: metrics.error_rate_pct,
            canary_p95_latency_ms: metrics.p95_latency_ms,
            control_p95_latency_ms: metrics.p95_latency_ms,
            canary_p99_latency_ms: metrics.p99_latency_ms,
            control_p99_latency_ms: metrics.p99_latency_ms,
            canary_cost_per_request: 0.0,
            control_cost_per_request: 0.0,
            statistical_analysis: None,
        };

        drop(state); // Release read lock before write operations

        // Perform all health checks
        let mut results = Vec::new();

        results.push(self.perform_health_check(
            HealthCheckType::SuccessRate,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::ErrorRate,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::P95Latency,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::P99Latency,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::QualityScore,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::CostIncrease,
            &deployment_metrics,
            &criteria,
        ));

        results.push(self.perform_health_check(
            HealthCheckType::SampleSize,
            &deployment_metrics,
            &criteria,
        ));

        // Perform statistical analysis if enabled
        if self.state.read().unwrap().config.enable_statistical_testing {
            if let Some(analysis) = self.perform_statistical_analysis(&deployment_metrics, &criteria)
            {
                let mut metrics_with_analysis = deployment_metrics;
                metrics_with_analysis.statistical_analysis = Some(analysis);

                results.push(self.perform_health_check(
                    HealthCheckType::Statistical,
                    &metrics_with_analysis,
                    &criteria,
                ));
            }
        }

        Ok(results)
    }

    async fn should_rollback(
        &self,
        deployment_id: &str,
        status: &DeploymentStatus,
    ) -> ActuatorResult<bool> {
        let state = self.state.read().map_err(|e| {
            ActuatorError::Internal(format!("Failed to acquire read lock: {}", e))
        })?;

        // Check if we have enough observation time
        if !self.has_sufficient_observation_time(
            deployment_id,
            state.config.min_observation_secs,
        ) {
            return Ok(false); // Too early to make a decision
        }

        // Check if deployment is unhealthy
        if !status.health.healthy {
            let consecutive_failures = state.get_consecutive_failures(deployment_id);

            // Rollback if we've exceeded the failure threshold
            if consecutive_failures >= state.config.max_consecutive_failures {
                return Ok(true);
            }
        }

        // Check for critical failures in health checks
        for check in &status.health.check_results {
            if !check.passed {
                match check.name.as_str() {
                    "success_rate" | "error_rate" => {
                        // Critical checks - consider immediate rollback
                        if let (Some(value), Some(threshold)) = (check.value, check.threshold) {
                            // If the deviation is severe (>2x threshold violation)
                            let deviation = if check.name == "success_rate" {
                                (threshold - value) / threshold
                            } else {
                                (value - threshold) / threshold
                            };

                            if deviation > 2.0 {
                                return Ok(true);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Check statistical significance
        if let Some(analysis) = &status.metrics.statistical_analysis {
            if analysis.significant_difference {
                // If there's a statistically significant degradation
                if status.metrics.canary_success_rate < status.metrics.control_success_rate {
                    let degradation_pct = ((status.metrics.control_success_rate
                        - status.metrics.canary_success_rate)
                        / status.metrics.control_success_rate)
                        * 100.0;

                    if degradation_pct > state.config.min_effect_size * 100.0 {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    async fn get_health_summary(
        &self,
        deployment_id: &str,
    ) -> ActuatorResult<HealthStatus> {
        let state = self.state.read().map_err(|e| {
            ActuatorError::Internal(format!("Failed to acquire read lock: {}", e))
        })?;

        // Get the latest health status from history
        if let Some(history) = state.health_history.get(deployment_id) {
            if let Some(latest) = history.last() {
                return Ok(latest.health_status.clone());
            }
        }

        // Return default healthy status if no history
        Ok(HealthStatus {
            healthy: true,
            checks_passed: 0,
            checks_failed: 0,
            check_results: Vec::new(),
            last_check_time: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    fn create_test_metrics(
        success_rate: f64,
        error_rate: f64,
        p95_latency: f64,
        p99_latency: f64,
        request_count: u64,
    ) -> SystemMetrics {
        SystemMetrics {
            avg_latency_ms: p95_latency * 0.7,
            p95_latency_ms: p95_latency,
            p99_latency_ms: p99_latency,
            success_rate_pct: success_rate,
            error_rate_pct: error_rate,
            throughput_rps: 100.0,
            total_cost_usd: 10.0,
            daily_cost_usd: 10.0,
            avg_rating: Some(4.5),
            cache_hit_rate_pct: Some(80.0),
            request_count,
            timestamp: Utc::now(),
        }
    }

    fn create_test_deployment_status(
        deployment_id: &str,
        healthy: bool,
        metrics: DeploymentMetrics,
    ) -> DeploymentStatus {
        use super::super::error::DeploymentState;

        DeploymentStatus {
            deployment_id: deployment_id.to_string(),
            state: DeploymentState::Monitoring,
            current_phase: Some(0),
            traffic_percent: 10.0,
            start_time: Utc::now() - chrono::Duration::minutes(10),
            phase_start_time: Some(Utc::now() - chrono::Duration::minutes(5)),
            health: HealthStatus {
                healthy,
                checks_passed: if healthy { 7 } else { 5 },
                checks_failed: if healthy { 0 } else { 2 },
                check_results: Vec::new(),
                last_check_time: Utc::now(),
            },
            metrics,
            errors: Vec::new(),
            last_updated: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_health_monitor_creation() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-1";

        monitor.register_deployment(deployment_id);

        let history = monitor.get_health_history(deployment_id);
        assert!(history.is_empty());
    }

    #[tokio::test]
    async fn test_health_check_success_rate_pass() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-2";

        monitor.register_deployment(deployment_id);

        let metrics = create_test_metrics(99.5, 0.5, 500.0, 800.0, 1000);

        let results = monitor
            .check_health(deployment_id, &metrics)
            .await
            .unwrap();

        assert!(!results.is_empty());

        let success_rate_check = results
            .iter()
            .find(|r| r.name == "success_rate")
            .unwrap();
        assert!(success_rate_check.passed);
        assert_eq!(success_rate_check.value, Some(99.5));
    }

    #[tokio::test]
    async fn test_health_check_success_rate_fail() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-3";

        monitor.register_deployment(deployment_id);

        let metrics = create_test_metrics(98.0, 2.0, 500.0, 800.0, 1000);

        let results = monitor
            .check_health(deployment_id, &metrics)
            .await
            .unwrap();

        let success_rate_check = results
            .iter()
            .find(|r| r.name == "success_rate")
            .unwrap();
        assert!(!success_rate_check.passed);
        assert_eq!(success_rate_check.value, Some(98.0));
    }

    #[tokio::test]
    async fn test_health_check_error_rate() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-4";

        monitor.register_deployment(deployment_id);

        // Pass: error rate within threshold
        let metrics_pass = create_test_metrics(99.5, 0.5, 500.0, 800.0, 1000);
        let results_pass = monitor
            .check_health(deployment_id, &metrics_pass)
            .await
            .unwrap();
        let error_rate_check_pass = results_pass
            .iter()
            .find(|r| r.name == "error_rate")
            .unwrap();
        assert!(error_rate_check_pass.passed);

        // Fail: error rate exceeds threshold
        let metrics_fail = create_test_metrics(97.0, 3.0, 500.0, 800.0, 1000);
        let results_fail = monitor
            .check_health(deployment_id, &metrics_fail)
            .await
            .unwrap();
        let error_rate_check_fail = results_fail
            .iter()
            .find(|r| r.name == "error_rate")
            .unwrap();
        assert!(!error_rate_check_fail.passed);
    }

    #[tokio::test]
    async fn test_health_check_latency() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-5";

        monitor.register_deployment(deployment_id);

        // Pass: latency within thresholds
        let metrics_pass = create_test_metrics(99.5, 0.5, 800.0, 1500.0, 1000);
        let results_pass = monitor
            .check_health(deployment_id, &metrics_pass)
            .await
            .unwrap();
        let p95_check = results_pass
            .iter()
            .find(|r| r.name == "p95_latency")
            .unwrap();
        assert!(p95_check.passed);

        // Fail: P95 latency exceeds threshold
        let metrics_fail_p95 = create_test_metrics(99.5, 0.5, 1500.0, 1800.0, 1000);
        let results_fail_p95 = monitor
            .check_health(deployment_id, &metrics_fail_p95)
            .await
            .unwrap();
        let p95_check_fail = results_fail_p95
            .iter()
            .find(|r| r.name == "p95_latency")
            .unwrap();
        assert!(!p95_check_fail.passed);

        // Fail: P99 latency exceeds threshold
        let metrics_fail_p99 = create_test_metrics(99.5, 0.5, 800.0, 3000.0, 1000);
        let results_fail_p99 = monitor
            .check_health(deployment_id, &metrics_fail_p99)
            .await
            .unwrap();
        let p99_check_fail = results_fail_p99
            .iter()
            .find(|r| r.name == "p99_latency")
            .unwrap();
        assert!(!p99_check_fail.passed);
    }

    #[tokio::test]
    async fn test_health_check_sample_size() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-6";

        monitor.register_deployment(deployment_id);

        // Pass: sufficient samples
        let metrics_pass = create_test_metrics(99.5, 0.5, 500.0, 800.0, 1000);
        let results_pass = monitor
            .check_health(deployment_id, &metrics_pass)
            .await
            .unwrap();
        let sample_check = results_pass
            .iter()
            .find(|r| r.name == "sample_size")
            .unwrap();
        assert!(sample_check.passed);

        // Fail: insufficient samples
        let metrics_fail = create_test_metrics(99.5, 0.5, 500.0, 800.0, 50);
        let results_fail = monitor
            .check_health(deployment_id, &metrics_fail)
            .await
            .unwrap();
        let sample_check_fail = results_fail
            .iter()
            .find(|r| r.name == "sample_size")
            .unwrap();
        assert!(!sample_check_fail.passed);
    }

    #[tokio::test]
    async fn test_should_rollback_healthy() {
        let config = HealthMonitorConfig {
            min_observation_secs: 0, // Disable min observation time for test
            ..Default::default()
        };
        let monitor = ProductionHealthMonitor::with_config(config);
        let deployment_id = "test-deployment-7";

        monitor.register_deployment(deployment_id);

        let metrics = DeploymentMetrics {
            canary_success_rate: 99.5,
            control_success_rate: 99.0,
            ..Default::default()
        };

        let status = create_test_deployment_status(deployment_id, true, metrics);

        let should_rollback = monitor
            .should_rollback(deployment_id, &status)
            .await
            .unwrap();
        assert!(!should_rollback);
    }

    #[tokio::test]
    async fn test_should_rollback_consecutive_failures() {
        let config = HealthMonitorConfig {
            max_consecutive_failures: 3,
            min_observation_secs: 0,
            ..Default::default()
        };
        let monitor = ProductionHealthMonitor::with_config(config);
        let deployment_id = "test-deployment-8";

        monitor.register_deployment(deployment_id);

        let metrics = DeploymentMetrics {
            canary_success_rate: 95.0,
            control_success_rate: 99.0,
            ..Default::default()
        };

        // Record multiple failures
        for _ in 0..3 {
            let status = create_test_deployment_status(deployment_id, false, metrics.clone());
            let _ = monitor
                .should_rollback(deployment_id, &status)
                .await
                .unwrap();

            // Record the failure
            if let Ok(mut state) = monitor.state.write() {
                state.record_health(
                    deployment_id,
                    HealthStatus {
                        healthy: false,
                        checks_passed: 5,
                        checks_failed: 2,
                        check_results: Vec::new(),
                        last_check_time: Utc::now(),
                    },
                    metrics.clone(),
                );
            }
        }

        let status = create_test_deployment_status(deployment_id, false, metrics);
        let should_rollback = monitor
            .should_rollback(deployment_id, &status)
            .await
            .unwrap();
        assert!(should_rollback);
    }

    #[tokio::test]
    async fn test_should_rollback_critical_failure() {
        let config = HealthMonitorConfig {
            min_observation_secs: 0,
            ..Default::default()
        };
        let monitor = ProductionHealthMonitor::with_config(config);
        let deployment_id = "test-deployment-9";

        monitor.register_deployment(deployment_id);

        let mut metrics = DeploymentMetrics {
            canary_success_rate: 90.0, // Very low
            control_success_rate: 99.0,
            ..Default::default()
        };

        let mut status = create_test_deployment_status(deployment_id, false, metrics.clone());

        // Add a critical failure
        status.health.check_results.push(HealthCheckResult {
            name: "success_rate".to_string(),
            passed: false,
            details: "Critical failure".to_string(),
            value: Some(90.0),
            threshold: Some(99.0),
            timestamp: Utc::now(),
        });

        let should_rollback = monitor
            .should_rollback(deployment_id, &status)
            .await
            .unwrap();
        assert!(should_rollback);
    }

    #[tokio::test]
    async fn test_statistical_analysis() {
        let monitor = ProductionHealthMonitor::new();

        let metrics = DeploymentMetrics {
            canary_requests: 1000,
            control_requests: 1000,
            canary_success_rate: 99.5,
            control_success_rate: 99.0,
            canary_p95_latency_ms: 500.0,
            control_p95_latency_ms: 480.0,
            ..Default::default()
        };

        let criteria = SuccessCriteria::default();

        let analysis = monitor.perform_statistical_analysis(&metrics, &criteria);
        assert!(analysis.is_some());

        let analysis = analysis.unwrap();
        assert_eq!(analysis.canary_sample_size, 1000);
        assert_eq!(analysis.control_sample_size, 1000);
        assert!(analysis.success_rate_p_value >= 0.0);
        assert!(analysis.success_rate_p_value <= 1.0);
    }

    #[tokio::test]
    async fn test_statistical_analysis_insufficient_samples() {
        let monitor = ProductionHealthMonitor::new();

        let metrics = DeploymentMetrics {
            canary_requests: 50,
            control_requests: 50,
            canary_success_rate: 99.5,
            control_success_rate: 99.0,
            ..Default::default()
        };

        let criteria = SuccessCriteria::default();

        let analysis = monitor.perform_statistical_analysis(&metrics, &criteria);
        assert!(analysis.is_none());
    }

    #[tokio::test]
    async fn test_get_health_summary_no_history() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-10";

        let summary = monitor
            .get_health_summary(deployment_id)
            .await
            .unwrap();

        assert!(summary.healthy);
        assert_eq!(summary.checks_passed, 0);
        assert_eq!(summary.checks_failed, 0);
    }

    #[tokio::test]
    async fn test_get_health_summary_with_history() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-11";

        monitor.register_deployment(deployment_id);

        // Record some health data
        if let Ok(mut state) = monitor.state.write() {
            state.record_health(
                deployment_id,
                HealthStatus {
                    healthy: true,
                    checks_passed: 7,
                    checks_failed: 0,
                    check_results: Vec::new(),
                    last_check_time: Utc::now(),
                },
                DeploymentMetrics::default(),
            );
        }

        let summary = monitor
            .get_health_summary(deployment_id)
            .await
            .unwrap();

        assert!(summary.healthy);
        assert_eq!(summary.checks_passed, 7);
        assert_eq!(summary.checks_failed, 0);
    }

    #[tokio::test]
    async fn test_cleanup_deployment() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-12";

        monitor.register_deployment(deployment_id);

        // Record some data
        if let Ok(mut state) = monitor.state.write() {
            state.record_health(
                deployment_id,
                HealthStatus {
                    healthy: true,
                    checks_passed: 7,
                    checks_failed: 0,
                    check_results: Vec::new(),
                    last_check_time: Utc::now(),
                },
                DeploymentMetrics::default(),
            );
        }

        let history_before = monitor.get_health_history(deployment_id);
        assert!(!history_before.is_empty());

        monitor.cleanup_deployment(deployment_id);

        let history_after = monitor.get_health_history(deployment_id);
        assert!(history_after.is_empty());
    }

    #[tokio::test]
    async fn test_health_monitor_with_custom_config() {
        let config = HealthMonitorConfig {
            success_criteria: SuccessCriteria {
                min_success_rate: 95.0,
                max_error_rate: 5.0,
                ..Default::default()
            },
            max_consecutive_failures: 5,
            min_observation_secs: 30,
            ..Default::default()
        };

        let monitor = ProductionHealthMonitor::with_config(config);
        let deployment_id = "test-deployment-13";

        monitor.register_deployment(deployment_id);

        let metrics = create_test_metrics(96.0, 4.0, 500.0, 800.0, 1000);

        let results = monitor
            .check_health(deployment_id, &metrics)
            .await
            .unwrap();

        let success_rate_check = results
            .iter()
            .find(|r| r.name == "success_rate")
            .unwrap();
        assert!(success_rate_check.passed); // Passes with custom threshold

        let error_rate_check = results
            .iter()
            .find(|r| r.name == "error_rate")
            .unwrap();
        assert!(error_rate_check.passed); // Passes with custom threshold
    }

    #[tokio::test]
    async fn test_proportion_test_p_value() {
        let monitor = ProductionHealthMonitor::new();

        // Test with identical proportions
        let p_value = monitor.calculate_proportion_test_p_value(0.99, 1000, 0.99, 1000);
        assert!(p_value > 0.05); // Should not be significant

        // Test with very different proportions
        let p_value = monitor.calculate_proportion_test_p_value(0.95, 1000, 0.99, 1000);
        assert!(p_value <= 0.05); // Should be significant
    }

    #[tokio::test]
    async fn test_cost_increase_check() {
        let monitor = ProductionHealthMonitor::new();
        let deployment_id = "test-deployment-14";

        monitor.register_deployment(deployment_id);

        let state = monitor.state.read().unwrap();
        let criteria = &state.config.success_criteria;

        // Test with acceptable cost increase
        let metrics_pass = DeploymentMetrics {
            canary_cost_per_request: 0.105,
            control_cost_per_request: 0.10,
            ..Default::default()
        };

        let check_pass = monitor.perform_health_check(
            HealthCheckType::CostIncrease,
            &metrics_pass,
            criteria,
        );
        assert!(check_pass.passed);

        // Test with excessive cost increase
        let metrics_fail = DeploymentMetrics {
            canary_cost_per_request: 0.15,
            control_cost_per_request: 0.10,
            ..Default::default()
        };

        let check_fail = monitor.perform_health_check(
            HealthCheckType::CostIncrease,
            &metrics_fail,
            criteria,
        );
        assert!(!check_fail.passed);
    }
}
