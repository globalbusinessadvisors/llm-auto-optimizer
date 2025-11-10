//! Actuator module
//!
//! This module provides deployment and configuration management capabilities
//! with canary deployments and automatic rollback.
//!
//! # Example
//!
//! ```rust,no_run
//! use processor::actuator::{ActuatorCoordinator, ActuatorConfig, Actuator, DeploymentRequest};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create actuator
//!     let config = ActuatorConfig::default();
//!     let mut actuator = ActuatorCoordinator::new(config);
//!
//!     // Start actuator
//!     actuator.start().await?;
//!
//!     // Deploy configuration change
//!     let request = DeploymentRequest { /* ... */ };
//!     let status = actuator.deploy(request).await?;
//!
//!     println!("Deployment status: {:?}", status.state);
//!
//!     Ok(())
//! }
//! ```

pub mod canary;
pub mod config;
pub mod configuration;
pub mod coordinator;
pub mod error;
pub mod health;
pub mod rollback;
pub mod traits;
pub mod types;

// Re-export commonly used types

// Core traits
pub use traits::{Actuator, CanaryDeployment, ConfigurationManager, HealthMonitor, TrafficSplitter};

// Main coordinator
pub use coordinator::ActuatorCoordinator;

// Component implementations
pub use canary::CanaryDeploymentEngine;
pub use configuration::{
    AuditLogEntry, ConfigurationBackend, ConfigurationDiff, ConfigurationManagerImpl,
    DiffType, InMemoryBackend,
};
pub use health::{HealthCheckType, HealthMonitorConfig, ProductionHealthMonitor};
pub use rollback::{
    NotificationHandler, NotificationSeverity, RollbackEngine, RollbackHistoryEntry, RollbackMode,
    RollbackNotification, RollbackPhase, RollbackStats, RollbackStatus, RollbackTrigger,
    ThresholdViolation,
};

// Configuration
pub use config::{ActuatorConfig, ConfigurationManagementConfig, HealthMonitoringConfig, RollbackConfig};

// Types
pub use types::{
    ActuatorStats, CanaryConfig, CanaryPhase, ConfigurationSnapshot, DeploymentMetrics,
    DeploymentRequest, DeploymentState, DeploymentStatus, DeploymentStrategy, HealthCheckResult, HealthStatus,
    RollbackReason, RollbackRequest, RollbackResult, StatisticalAnalysis, SuccessCriteria,
    SystemMetrics, TrafficSplitStrategy,
};

// Error types
pub use error::{ActuatorError, ActuatorResult, ActuatorState};
