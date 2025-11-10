//! Storage traits

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

use super::error::{StorageError, StorageResult};
use super::types::{
    Batch, EntryMetadata, Operation, Query, StorageBackend, StorageEntry, StorageHealth,
    StorageMetadata, StorageStats, Transaction,
};

/// Core storage trait defining unified interface for all backends
#[async_trait]
pub trait Storage: Send + Sync + Debug {
    /// Get backend type
    fn backend(&self) -> StorageBackend;

    /// Get storage metadata
    async fn metadata(&self) -> StorageResult<StorageMetadata>;

    /// Health check
    async fn health_check(&self) -> StorageResult<StorageHealth>;

    /// Get statistics
    async fn stats(&self) -> StorageResult<StorageStats>;

    // Basic CRUD operations

    /// Insert a new entry
    async fn insert<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()>
    where
        Self: Sized;

    /// Insert entry with TTL (time-to-live in seconds)
    async fn insert_with_ttl<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        ttl_seconds: i64,
    ) -> StorageResult<()>
    where
        Self: Sized;

    /// Get an entry by key
    async fn get<T: DeserializeOwned>(
        &self,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<StorageEntry<T>>>
    where
        Self: Sized;

    /// Update an existing entry
    async fn update<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()>
    where
        Self: Sized;

    /// Update entry with optimistic locking (version check)
    async fn update_versioned<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        expected_version: u64,
    ) -> StorageResult<()>
    where
        Self: Sized;

    /// Delete an entry
    async fn delete(&self, table: &str, key: &str) -> StorageResult<()>;

    /// Check if entry exists
    async fn exists(&self, table: &str, key: &str) -> StorageResult<bool>;

    // Batch operations

    /// Execute batch operations
    async fn execute_batch(&self, batch: Batch) -> StorageResult<()>;

    // Query operations

    /// Query entries matching criteria
    async fn query<T: DeserializeOwned>(
        &self,
        query: Query,
    ) -> StorageResult<Vec<StorageEntry<T>>>
    where
        Self: Sized;

    /// Count entries matching criteria
    async fn count(&self, query: Query) -> StorageResult<usize>;

    // Transaction support (not all backends support this)

    /// Begin a transaction
    async fn begin_transaction(&self) -> StorageResult<Transaction>;

    /// Commit a transaction
    async fn commit_transaction(&self, transaction: Transaction) -> StorageResult<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&self, transaction: Transaction) -> StorageResult<()>;

    // Advanced operations

    /// Scan keys with prefix
    async fn scan_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<String>>;

    /// Get multiple entries by keys
    async fn get_multi<T: DeserializeOwned>(
        &self,
        table: &str,
        keys: Vec<String>,
    ) -> StorageResult<Vec<Option<StorageEntry<T>>>>
    where
        Self: Sized;

    /// Delete multiple entries
    async fn delete_multi(&self, table: &str, keys: Vec<String>) -> StorageResult<()>;

    /// Atomic compare-and-swap
    async fn compare_and_swap<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        expected_version: u64,
        new_value: &T,
    ) -> StorageResult<bool>
    where
        Self: Sized;

    // Time-based operations

    /// Get entries modified after timestamp
    async fn get_modified_since<T: DeserializeOwned>(
        &self,
        table: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>>
    where
        Self: Sized;

    /// Get entries that expire before timestamp
    async fn get_expiring_before<T: DeserializeOwned>(
        &self,
        table: &str,
        before: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>>
    where
        Self: Sized;

    // Maintenance operations

    /// Cleanup expired entries
    async fn cleanup_expired(&self, table: &str) -> StorageResult<usize>;

    /// Compact storage (if supported)
    async fn compact(&self) -> StorageResult<()>;

    /// Flush pending writes
    async fn flush(&self) -> StorageResult<()>;

    // Lifecycle

    /// Initialize storage
    async fn initialize(&mut self) -> StorageResult<()>;

    /// Shutdown storage
    async fn shutdown(&mut self) -> StorageResult<()>;
}

/// Cache-specific operations (for Redis)
#[async_trait]
pub trait CacheStorage: Storage {
    /// Get TTL for a key (seconds remaining)
    async fn ttl(&self, table: &str, key: &str) -> StorageResult<Option<i64>>;

    /// Set expiration time for a key
    async fn expire(&self, table: &str, key: &str, ttl_seconds: i64) -> StorageResult<()>;

    /// Remove expiration from a key
    async fn persist(&self, table: &str, key: &str) -> StorageResult<()>;

    /// Increment a numeric value atomically
    async fn increment(&self, table: &str, key: &str, delta: i64) -> StorageResult<i64>;

    /// Decrement a numeric value atomically
    async fn decrement(&self, table: &str, key: &str, delta: i64) -> StorageResult<i64>;

    /// Get cache statistics
    async fn cache_stats(&self) -> StorageResult<CacheStats>;
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub memory_used_bytes: u64,
    pub memory_available_bytes: u64,
    pub keys_count: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Relational database operations (for PostgreSQL)
#[async_trait]
pub trait RelationalStorage: Storage {
    /// Execute raw SQL query
    async fn execute_sql(&self, sql: &str, params: Vec<SqlParam>) -> StorageResult<SqlResult>;

    /// Execute SQL query and return results
    async fn query_sql<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: Vec<SqlParam>,
    ) -> StorageResult<Vec<T>>
    where
        Self: Sized;

    /// Create index
    async fn create_index(
        &self,
        table: &str,
        index_name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> StorageResult<()>;

    /// Drop index
    async fn drop_index(&self, index_name: &str) -> StorageResult<()>;

    /// Vacuum table (optimize storage)
    async fn vacuum(&self, table: &str) -> StorageResult<()>;

    /// Analyze table (update statistics)
    async fn analyze(&self, table: &str) -> StorageResult<()>;
}

/// SQL parameter
#[derive(Debug, Clone)]
pub enum SqlParam {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(DateTime<Utc>),
    Null,
}

/// SQL query result
#[derive(Debug, Clone)]
pub struct SqlResult {
    pub rows_affected: u64,
}

/// Key-value storage operations (for Sled)
#[async_trait]
pub trait KeyValueStorage: Storage {
    /// Get value by key as bytes
    async fn get_bytes(&self, table: &str, key: &str) -> StorageResult<Option<Vec<u8>>>;

    /// Set value as bytes
    async fn set_bytes(&self, table: &str, key: &str, value: Vec<u8>) -> StorageResult<()>;

    /// Iterate over all keys with prefix
    async fn iter_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<(String, Vec<u8>)>>;

    /// Get range of keys
    async fn get_range(
        &self,
        table: &str,
        start: &str,
        end: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>>;

    /// Merge value (append for log-structured merge trees)
    async fn merge(&self, table: &str, key: &str, value: Vec<u8>) -> StorageResult<()>;
}

/// Pub/Sub operations (for Redis)
#[async_trait]
pub trait PubSubStorage: Storage {
    /// Publish message to channel
    async fn publish(&self, channel: &str, message: &[u8]) -> StorageResult<usize>;

    /// Subscribe to channel
    async fn subscribe(&self, channel: &str) -> StorageResult<Box<dyn Subscriber>>;

    /// Unsubscribe from channel
    async fn unsubscribe(&self, channel: &str) -> StorageResult<()>;
}

/// Subscriber trait for receiving messages
#[async_trait]
pub trait Subscriber: Send + Sync {
    /// Receive next message
    async fn next_message(&mut self) -> StorageResult<Option<Vec<u8>>>;
}

/// Migration operations
#[async_trait]
pub trait Migrator: Send + Sync {
    /// Apply pending migrations
    async fn migrate(&self) -> StorageResult<usize>;

    /// Rollback last migration
    async fn rollback(&self) -> StorageResult<()>;

    /// Get applied migrations
    async fn applied_migrations(&self) -> StorageResult<Vec<Migration>>;

    /// Get pending migrations
    async fn pending_migrations(&self) -> StorageResult<Vec<Migration>>;
}

/// Migration metadata
#[derive(Debug, Clone)]
pub struct Migration {
    pub id: String,
    pub name: String,
    pub applied_at: Option<DateTime<Utc>>,
    pub checksum: String,
}

/// Backup operations
#[async_trait]
pub trait Backup: Send + Sync {
    /// Create backup
    async fn create_backup(&self, path: &str) -> StorageResult<BackupMetadata>;

    /// Restore from backup
    async fn restore_backup(&self, path: &str) -> StorageResult<()>;

    /// List available backups
    async fn list_backups(&self, directory: &str) -> StorageResult<Vec<BackupMetadata>>;

    /// Delete backup
    async fn delete_backup(&self, path: &str) -> StorageResult<()>;
}

/// Backup metadata
#[derive(Debug, Clone)]
pub struct BackupMetadata {
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub checksum: String,
}
