//! Storage module
//!
//! This module provides multi-backend storage capabilities with support for
//! PostgreSQL, Redis, and Sled. It includes a unified Storage trait and
//! a StorageManager for coordinating multiple backends.
//!
//! # Example
//!
//! ```rust,no_run
//! use processor::storage::{StorageManager, RoutingStrategy};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create storage manager
//!     let mut manager = StorageManager::new(RoutingStrategy::CacheAside);
//!
//!     // Initialize
//!     manager.initialize().await?;
//!
//!     // Use storage
//!     // ...
//!
//!     // Shutdown
//!     manager.shutdown().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod manager;
pub mod postgresql;
pub mod redis;
pub mod sled_backend;
pub mod traits;
pub mod types;

// Re-export commonly used types

// Core traits
pub use traits::{
    Backup, BackupMetadata, CacheStorage, CacheStats, KeyValueStorage, Migrator, Migration,
    PubSubStorage, RelationalStorage, SqlParam, SqlResult, Storage, Subscriber,
};

// Configuration
pub use config::{
    MonitoringConfig, PoolConfig, PostgreSQLConfig, RedisConfig, RetryConfig, SSLMode, SledConfig,
    SledMode, StorageConfig,
};

// Error types
pub use error::{ErrorCategory, StorageError, StorageResult};

// Types
pub use types::{
    Batch, Filter, Operation, Operator, Query, Sort, SortDirection, StorageBackend, StorageEntry,
    StorageHealth, StorageMetadata, StorageStats, Transaction, TransactionState, Value,
};

// Manager
pub use manager::{ManagerState, ManagerStats, RoutingStrategy, StorageManager};

// Backend implementations
pub use postgresql::PostgreSQLStorage;
pub use redis::RedisStorage;
pub use sled_backend::SledStorage;

// Storage wrapper enum for dynamic dispatch
mod wrapper;
pub use wrapper::AnyStorage;
