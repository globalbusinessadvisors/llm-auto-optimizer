//! Actuator core types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::decision::{ConfigChange, Decision};
pub use crate::decision::SystemMetrics;

pub use super::error::DeploymentState;

/// Deployment request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRequest {
    /// Unique deployment ID
    pub id: String,

    /// Timestamp
    pub timestamp: DateTime<Utc>,

    /// Decision being deployed
    pub decision: Decision,

    /// Deployment strategy
    pub strategy: DeploymentStrategy,

    /// Configuration changes to apply
    pub config_changes: Vec<ConfigChange>,

    /// Expected duration
    pub expected_duration: Duration,

    /// Metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Deployment strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStrategy {
    /// Immediate deployment (no canary)
    Immediate,

    /// Canary deployment with gradual rollout
    Canary,

    /// Blue-green deployment
    BlueGreen,

    /// Rolling update
    Rolling,

    /// Shadow deployment (testing without affecting production)
    Shadow,
}

/// Canary deployment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryConfig {
    /// Rollout phases (traffic percentages)
    pub phases: Vec<CanaryPhase>,

    /// Duration for each phase
    pub phase_duration: Duration,

    /// Traffic splitting strategy
    pub traffic_split_strategy: TrafficSplitStrategy,

    /// Success criteria for promotion
    pub success_criteria: SuccessCriteria,

    /// Auto-promote if criteria met
    pub auto_promote: bool,

    /// Auto-rollback on failure
    pub auto_rollback: bool,

    /// Maximum deployment duration
    pub max_duration: Duration,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self {
            phases: vec![
                CanaryPhase::new(1.0),   // 1%
                CanaryPhase::new(5.0),   // 5%
                CanaryPhase::new(25.0),  // 25%
                CanaryPhase::new(50.0),  // 50%
                CanaryPhase::new(100.0), // 100%
            ],
            phase_duration: Duration::from_secs(300), // 5 minutes
            traffic_split_strategy: TrafficSplitStrategy::WeightedRandom,
            success_criteria: SuccessCriteria::default(),
            auto_promote: true,
            auto_rollback: true,
            max_duration: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Canary phase
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanaryPhase {
    /// Traffic percentage (0-100)
    pub traffic_percent: f64,

    /// Phase name
    pub name: String,

    /// Whether this phase is currently active
    #[serde(skip)]
    pub active: bool,

    /// Start time of this phase
    #[serde(skip)]
    pub start_time: Option<DateTime<Utc>>,

    /// End time of this phase
    #[serde(skip)]
    pub end_time: Option<DateTime<Utc>>,
}

impl CanaryPhase {
    pub fn new(traffic_percent: f64) -> Self {
        Self {
            traffic_percent,
            name: format!("{}%", traffic_percent as u32),
            active: false,
            start_time: None,
            end_time: None,
        }
    }
}

/// Traffic splitting strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrafficSplitStrategy {
    /// Weighted random selection
    WeightedRandom,

    /// Consistent hashing (sticky sessions)
    ConsistentHash,

    /// Round-robin
    RoundRobin,

    /// Geographic-based
    Geographic,
}

/// Success criteria for canary promotion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessCriteria {
    /// Minimum success rate (0-100)
    pub min_success_rate: f64,

    /// Maximum error rate (0-100)
    pub max_error_rate: f64,

    /// Maximum P95 latency (ms)
    pub max_p95_latency_ms: f64,

    /// Maximum P99 latency (ms)
    pub max_p99_latency_ms: f64,

    /// Minimum quality score (0-5)
    pub min_quality_score: Option<f64>,

    /// Maximum cost increase (%)
    pub max_cost_increase_pct: f64,

    /// Minimum sample size for statistical validity
    pub min_sample_size: usize,

    /// Statistical significance level (p-value)
    pub significance_level: f64,
}

impl Default for SuccessCriteria {
    fn default() -> Self {
        Self {
            min_success_rate: 99.0,
            max_error_rate: 1.0,
            max_p95_latency_ms: 1000.0,
            max_p99_latency_ms: 2000.0,
            min_quality_score: Some(4.0),
            max_cost_increase_pct: 10.0,
            min_sample_size: 100,
            significance_level: 0.05, // 95% confidence
        }
    }
}

/// Deployment status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentStatus {
    /// Deployment ID
    pub deployment_id: String,

    /// Current state
    pub state: DeploymentState,

    /// Current canary phase (if applicable)
    pub current_phase: Option<usize>,

    /// Traffic percentage
    pub traffic_percent: f64,

    /// Start time
    pub start_time: DateTime<Utc>,

    /// Current phase start time
    pub phase_start_time: Option<DateTime<Utc>>,

    /// Health status
    pub health: HealthStatus,

    /// Metrics during deployment
    pub metrics: DeploymentMetrics,

    /// Errors encountered
    pub errors: Vec<String>,

    /// Last updated
    pub last_updated: DateTime<Utc>,
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Overall health
    pub healthy: bool,

    /// Health checks passed
    pub checks_passed: usize,

    /// Health checks failed
    pub checks_failed: usize,

    /// Individual check results
    pub check_results: Vec<HealthCheckResult>,

    /// Last check time
    pub last_check_time: DateTime<Utc>,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Check name
    pub name: String,

    /// Passed or failed
    pub passed: bool,

    /// Details
    pub details: String,

    /// Metric value (if applicable)
    pub value: Option<f64>,

    /// Threshold (if applicable)
    pub threshold: Option<f64>,

    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Deployment metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeploymentMetrics {
    /// Total requests
    pub total_requests: u64,

    /// Canary requests
    pub canary_requests: u64,

    /// Control requests
    pub control_requests: u64,

    /// Canary success rate (%)
    pub canary_success_rate: f64,

    /// Control success rate (%)
    pub control_success_rate: f64,

    /// Canary error rate (%)
    pub canary_error_rate: f64,

    /// Control error rate (%)
    pub control_error_rate: f64,

    /// Canary P95 latency (ms)
    pub canary_p95_latency_ms: f64,

    /// Control P95 latency (ms)
    pub control_p95_latency_ms: f64,

    /// Canary P99 latency (ms)
    pub canary_p99_latency_ms: f64,

    /// Control P99 latency (ms)
    pub control_p99_latency_ms: f64,

    /// Canary cost per request
    pub canary_cost_per_request: f64,

    /// Control cost per request
    pub control_cost_per_request: f64,

    /// Statistical comparison results
    pub statistical_analysis: Option<StatisticalAnalysis>,
}

/// Statistical analysis of canary vs control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalAnalysis {
    /// P-value for success rate difference
    pub success_rate_p_value: f64,

    /// P-value for latency difference
    pub latency_p_value: f64,

    /// Statistically significant difference detected
    pub significant_difference: bool,

    /// Confidence level
    pub confidence_level: f64,

    /// Sample sizes
    pub canary_sample_size: usize,
    pub control_sample_size: usize,
}

/// Rollback request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackRequest {
    /// Deployment ID to rollback
    pub deployment_id: String,

    /// Reason for rollback
    pub reason: RollbackReason,

    /// Timestamp
    pub timestamp: DateTime<Utc>,

    /// Manual or automatic
    pub automatic: bool,

    /// Metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Reason for rollback
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollbackReason {
    /// Health check failed
    HealthCheckFailed,

    /// Error rate too high
    HighErrorRate,

    /// Latency too high
    HighLatency,

    /// Quality degradation
    QualityDegradation,

    /// Cost increase too high
    HighCost,

    /// Manual intervention
    Manual,

    /// Timeout
    Timeout,

    /// Other reason
    Other(String),
}

/// Rollback result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResult {
    /// Deployment ID
    pub deployment_id: String,

    /// Success or failure
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Configuration changes reverted
    pub reverted_changes: Vec<ConfigChange>,

    /// Timestamp
    pub timestamp: DateTime<Utc>,

    /// Duration
    pub duration: Duration,
}

/// Configuration snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationSnapshot {
    /// Snapshot ID
    pub id: String,

    /// Timestamp
    pub timestamp: DateTime<Utc>,

    /// Configuration values
    pub config: HashMap<String, serde_json::Value>,

    /// Deployment ID that created this snapshot
    pub deployment_id: Option<String>,

    /// Version number
    pub version: u64,

    /// Tags
    pub tags: Vec<String>,
}

/// Actuator statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActuatorStats {
    /// Total deployments
    pub total_deployments: u64,

    /// Successful deployments
    pub successful_deployments: u64,

    /// Failed deployments
    pub failed_deployments: u64,

    /// Rolled back deployments
    pub rolled_back_deployments: u64,

    /// Average deployment duration (seconds)
    pub avg_deployment_duration_secs: f64,

    /// Average rollback duration (seconds)
    pub avg_rollback_duration_secs: f64,

    /// Deployments by strategy
    pub deployments_by_strategy: HashMap<String, u64>,

    /// Current active deployments
    pub active_deployments: usize,

    /// Last deployment time
    pub last_deployment_time: Option<DateTime<Utc>>,
}
