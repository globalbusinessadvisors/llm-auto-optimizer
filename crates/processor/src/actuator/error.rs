//! Actuator error types

use thiserror::Error;

/// Result type for Actuator operations
pub type ActuatorResult<T> = Result<T, ActuatorError>;

/// Error types for the Actuator
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ActuatorError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Deployment failed
    #[error("Deployment failed: {0}")]
    DeploymentFailed(String),

    /// Rollback failed
    #[error("Rollback failed: {0}")]
    RollbackFailed(String),

    /// Health check failed
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),

    /// Validation failed
    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    /// Traffic splitting error
    #[error("Traffic splitting error: {0}")]
    TrafficSplitError(String),

    /// State error
    #[error("State error: {0}")]
    StateError(String),

    /// Not running
    #[error("Actuator is not running")]
    NotRunning,

    /// Invalid state transition
    #[error("Invalid state transition from {from:?} to {to:?}")]
    InvalidStateTransition {
        from: ActuatorState,
        to: ActuatorState,
    },

    /// Timeout
    #[error("Operation timeout after {0}ms")]
    Timeout(u64),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Actuator state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActuatorState {
    /// Initialized but not started
    Initialized,
    /// Starting up
    Starting,
    /// Running and ready to execute
    Running,
    /// Executing a deployment
    Deploying,
    /// Executing a rollback
    RollingBack,
    /// Draining before shutdown
    Draining,
    /// Stopped
    Stopped,
    /// Failed state
    Failed,
}

impl ActuatorState {
    /// Check if the state allows executing deployments
    pub fn can_deploy(&self) -> bool {
        matches!(self, ActuatorState::Running)
    }

    /// Check if the state allows rollback
    pub fn can_rollback(&self) -> bool {
        matches!(self, ActuatorState::Running | ActuatorState::Deploying)
    }

    /// Check if transition to another state is valid
    pub fn can_transition_to(&self, to: ActuatorState) -> bool {
        use ActuatorState::*;
        matches!(
            (self, to),
            (Initialized, Starting)
                | (Starting, Running)
                | (Starting, Failed)
                | (Running, Deploying)
                | (Running, RollingBack)
                | (Running, Draining)
                | (Running, Failed)
                | (Deploying, Running)
                | (Deploying, RollingBack)
                | (Deploying, Failed)
                | (RollingBack, Running)
                | (RollingBack, Failed)
                | (Draining, Stopped)
                | (Draining, Failed)
                | (Failed, Initialized)
        )
    }
}

/// Deployment state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DeploymentState {
    /// Deployment is pending
    Pending,
    /// Validating configuration
    Validating,
    /// Rolling out (canary)
    RollingOut,
    /// Monitoring health
    Monitoring,
    /// Completed successfully
    Completed,
    /// Rolling back
    RollingBack,
    /// Rolled back
    RolledBack,
    /// Failed
    Failed,
}

impl DeploymentState {
    /// Check if deployment is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            DeploymentState::Completed
                | DeploymentState::RolledBack
                | DeploymentState::Failed
        )
    }

    /// Check if deployment can be rolled back
    pub fn can_rollback(&self) -> bool {
        matches!(
            self,
            DeploymentState::RollingOut | DeploymentState::Monitoring
        )
    }
}

impl std::fmt::Display for DeploymentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentState::Pending => write!(f, "Pending"),
            DeploymentState::Validating => write!(f, "Validating"),
            DeploymentState::RollingOut => write!(f, "RollingOut"),
            DeploymentState::Monitoring => write!(f, "Monitoring"),
            DeploymentState::Completed => write!(f, "Completed"),
            DeploymentState::RollingBack => write!(f, "RollingBack"),
            DeploymentState::RolledBack => write!(f, "RolledBack"),
            DeploymentState::Failed => write!(f, "Failed"),
        }
    }
}
