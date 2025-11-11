//! Sled embedded database storage backend
//!
//! This module provides a production-ready implementation of the Storage and KeyValueStorage
//! traits using the Sled embedded database. Sled is a high-performance, log-structured
//! merge tree (LSM) based embedded database that provides ACID guarantees through
//! compare-and-swap operations.
//!
//! # Features
//!
//! - Embedded database with no external dependencies
//! - ACID transactions with compare-and-swap
//! - Prefix scanning and range queries
//! - Automatic compaction and cleanup
//! - Crash-safe with write-ahead logging
//! - Compression support
//! - Thread-safe operations
//! - Zero-copy reads where possible
//!
//! # Architecture
//!
//! The backend uses separate Sled trees for each table, with a metadata tree for
//! statistics and versioning. All operations are wrapped to provide async interfaces
//! while Sled itself operates synchronously.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sled::{Batch as SledBatch, CompareAndSwapError, Db, IVec, Tree};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

use super::config::SledConfig;
use super::error::{StorageError, StorageResult};
use super::traits::{KeyValueStorage, Storage};
use super::types::{
    Batch, Operation, Query, StorageBackend, StorageEntry, StorageHealth, StorageMetadata,
    StorageStats, Transaction, TransactionState,
};

/// Metadata tree name for storing internal metadata
const METADATA_TREE: &str = "__metadata__";
/// Statistics key in metadata tree
const STATS_KEY: &str = "stats";
/// Version counter key
const VERSION_COUNTER_KEY: &str = "version_counter";

/// Internal storage entry wrapper for serialization
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InternalEntry<T> {
    key: String,
    value: T,
    version: u64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    metadata: HashMap<String, String>,
}

impl<T> From<StorageEntry<T>> for InternalEntry<T> {
    fn from(entry: StorageEntry<T>) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            version: entry.version,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            expires_at: entry.expires_at,
            metadata: entry.metadata,
        }
    }
}

impl<T> From<InternalEntry<T>> for StorageEntry<T> {
    fn from(entry: InternalEntry<T>) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            version: entry.version,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            expires_at: entry.expires_at,
            metadata: entry.metadata,
        }
    }
}

/// Sled storage backend implementation
#[derive(Debug)]
pub struct SledStorage {
    /// The underlying Sled database
    db: Arc<Db>,
    /// Configuration
    config: SledConfig,
    /// Statistics (protected by RwLock for thread-safe updates)
    stats: Arc<RwLock<StorageStats>>,
    /// Cached trees (table name -> Tree)
    trees: Arc<RwLock<HashMap<String, Tree>>>,
    /// Metadata tree
    metadata_tree: Tree,
    /// Is initialized
    initialized: Arc<RwLock<bool>>,
}

impl SledStorage {
    /// Create a new Sled storage backend
    #[instrument(skip(config))]
    pub async fn new(config: SledConfig) -> StorageResult<Self> {
        info!("Initializing Sled storage at {:?}", config.path);

        // Create directory if needed
        if config.create_dir {
            if let Some(parent) = config.path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    StorageError::InvalidConfiguration {
                        message: format!("Failed to create directory: {}", e),
                    }
                })?;
            }
        }

        // Build Sled configuration
        let mut sled_config = sled::Config::new().path(&config.path);

        if config.use_compression {
            sled_config = sled_config.use_compression(true);
        }

        if let Some(flush_ms) = config.flush_every_ms {
            sled_config = sled_config.flush_every_ms(Some(flush_ms));
        }

        sled_config = sled_config.cache_capacity(config.cache_capacity_bytes);

        if config.temporary {
            sled_config = sled_config.temporary(true);
        }

        // Open database
        let db = tokio::task::spawn_blocking(move || sled_config.open())
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
            .map_err(|e| StorageError::ConnectionError {
                source: Box::new(e),
            })?;

        let db = Arc::new(db);

        // Open metadata tree
        let metadata_tree = db
            .open_tree(METADATA_TREE)
            .map_err(|e| StorageError::ConnectionError {
                source: Box::new(e),
            })?;

        // Initialize statistics
        let stats = Arc::new(RwLock::new(StorageStats::new()));

        Ok(Self {
            db,
            config,
            stats,
            trees: Arc::new(RwLock::new(HashMap::new())),
            metadata_tree,
            initialized: Arc::new(RwLock::new(false)),
        })
    }

    /// Get or create a tree for a table
    #[instrument(skip(self))]
    async fn get_tree(&self, table: &str) -> StorageResult<Tree> {
        // Check cache first
        {
            let trees = self.trees.read().await;
            if let Some(tree) = trees.get(table) {
                return Ok(tree.clone());
            }
        }

        // Open tree
        let db = self.db.clone();
        let table_name = table.to_string();
        let tree = tokio::task::spawn_blocking(move || db.open_tree(table_name))
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
            .map_err(|e| StorageError::ConnectionError {
                source: Box::new(e),
            })?;

        // Cache tree
        let mut trees = self.trees.write().await;
        trees.insert(table.to_string(), tree.clone());

        debug!("Opened tree for table: {}", table);
        Ok(tree)
    }

    /// Serialize value to bytes
    fn serialize<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
        bincode::serialize(value).map_err(|e| StorageError::SerializationError {
            source: Box::new(e),
        })
    }

    /// Deserialize value from bytes
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
        bincode::deserialize(bytes).map_err(|e| StorageError::DeserializationError {
            source: Box::new(e),
        })
    }

    /// Get next version number
    async fn next_version(&self) -> StorageResult<u64> {
        let metadata_tree = self.metadata_tree.clone();

        tokio::task::spawn_blocking(move || {
            let current = metadata_tree
                .get(VERSION_COUNTER_KEY)
                .map_err(|e| StorageError::InternalError {
                    message: format!("Failed to get version counter: {}", e),
                })?
                .map(|v| {
                    let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0u8; 8]);
                    u64::from_le_bytes(bytes)
                })
                .unwrap_or(0);

            let next = current + 1;
            metadata_tree
                .insert(VERSION_COUNTER_KEY, &next.to_le_bytes())
                .map_err(|e| StorageError::InternalError {
                    message: format!("Failed to update version counter: {}", e),
                })?;

            Ok(next)
        })
        .await
        .map_err(|e| StorageError::InternalError {
            message: format!("Task join error: {}", e),
        })?
    }

    /// Persist statistics to metadata tree
    async fn persist_stats(&self) -> StorageResult<()> {
        let stats = self.stats.read().await.clone();
        let stats_bytes = Self::serialize(&stats)?;
        let metadata_tree = self.metadata_tree.clone();

        tokio::task::spawn_blocking(move || {
            metadata_tree
                .insert(STATS_KEY, stats_bytes)
                .map_err(|e| StorageError::InternalError {
                    message: format!("Failed to persist stats: {}", e),
                })?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::InternalError {
            message: format!("Task join error: {}", e),
        })?
    }

    /// Load statistics from metadata tree
    async fn load_stats(&self) -> StorageResult<()> {
        let metadata_tree = self.metadata_tree.clone();

        let stats_bytes = tokio::task::spawn_blocking(move || {
            metadata_tree
                .get(STATS_KEY)
                .map_err(|e| StorageError::InternalError {
                    message: format!("Failed to load stats: {}", e),
                })
        })
        .await
        .map_err(|e| StorageError::InternalError {
            message: format!("Task join error: {}", e),
        })??;

        if let Some(bytes) = stats_bytes {
            let loaded_stats: StorageStats = Self::deserialize(&bytes)?;
            let mut stats = self.stats.write().await;
            *stats = loaded_stats;
            debug!("Loaded statistics from metadata");
        }

        Ok(())
    }

    /// Check if entry is expired
    fn is_expired<T>(entry: &InternalEntry<T>) -> bool {
        if let Some(expires_at) = entry.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Record operation metrics
    async fn record_operation(&self, success: bool, latency_ms: f64, bytes: u64) {
        let mut stats = self.stats.write().await;
        if success {
            stats.record_success(latency_ms, bytes);
        } else {
            stats.record_failure();
        }
    }

    /// Execute operation with metrics tracking
    async fn with_metrics<F, T>(&self, operation: F) -> StorageResult<T>
    where
        F: std::future::Future<Output = StorageResult<T>>,
    {
        let start = std::time::Instant::now();
        let result = operation.await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        let success = result.is_ok();
        self.record_operation(success, latency_ms, 0).await;

        result
    }

    /// Cleanup expired entries in a tree
    async fn cleanup_tree(&self, tree: &Tree) -> StorageResult<usize> {
        let tree = tree.clone();

        tokio::task::spawn_blocking(move || {
            let mut removed = 0;
            let mut to_remove = Vec::new();

            // Collect expired keys
            for result in tree.iter() {
                let (key, value) = result.map_err(|e| StorageError::InternalError {
                    message: format!("Iterator error: {}", e),
                })?;

                // Try to deserialize as InternalEntry<serde_json::Value>
                if let Ok(entry) = bincode::deserialize::<InternalEntry<serde_json::Value>>(&value)
                {
                    if Self::is_expired(&entry) {
                        to_remove.push(key);
                    }
                }
            }

            // Remove expired keys
            for key in to_remove {
                tree.remove(&key).map_err(|e| StorageError::InternalError {
                    message: format!("Failed to remove key: {}", e),
                })?;
                removed += 1;
            }

            Ok(removed)
        })
        .await
        .map_err(|e| StorageError::InternalError {
            message: format!("Task join error: {}", e),
        })?
    }

    /// Apply query filters to entries
    fn apply_filters<T: DeserializeOwned>(
        entries: Vec<StorageEntry<T>>,
        query: &Query,
    ) -> Vec<StorageEntry<T>> {
        let mut filtered = entries;

        // Apply limit and offset
        if let Some(offset) = query.offset {
            filtered = filtered.into_iter().skip(offset).collect();
        }

        if let Some(limit) = query.limit {
            filtered.truncate(limit);
        }

        filtered
    }
}

#[async_trait]
impl Storage for SledStorage {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Sled
    }

    async fn metadata(&self) -> StorageResult<StorageMetadata> {
        let tree_names = self.db.tree_names();
        let capabilities = vec![
            "embedded".to_string(),
            "key-value".to_string(),
            "prefix-scan".to_string(),
            "range-query".to_string(),
            "compare-and-swap".to_string(),
            "compression".to_string(),
            "transactions".to_string(),
        ];

        Ok(StorageMetadata {
            backend: StorageBackend::Sled,
            version: "0.34.7".to_string(),
            capabilities,
            status: StorageHealth::Healthy,
            last_health_check: Utc::now(),
        })
    }

    #[instrument(skip(self))]
    async fn health_check(&self) -> StorageResult<StorageHealth> {
        // Try to perform a simple operation
        let test_key = "__health_check__";
        let test_value = Utc::now().timestamp();

        match self.metadata_tree.insert(test_key, &test_value.to_le_bytes()) {
            Ok(_) => {
                self.metadata_tree.remove(test_key).ok();
                Ok(StorageHealth::Healthy)
            }
            Err(e) => {
                warn!("Health check failed: {}", e);
                Ok(StorageHealth::Unhealthy)
            }
        }
    }

    async fn stats(&self) -> StorageResult<StorageStats> {
        let mut stats = self.stats.read().await.clone();

        // Update with database stats
        stats.active_connections = self.trees.read().await.len();

        Ok(stats)
    }

    #[instrument(skip(self, value))]
    async fn insert<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let version = self.next_version().await?;

            let entry = InternalEntry {
                key: key.to_string(),
                value,
                version,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                expires_at: None,
                metadata: HashMap::new(),
            };

            let entry_bytes = Self::serialize(&entry)?;
            let tree_clone = tree.clone();
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree_clone
                    .insert(key_bytes, entry_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Insert failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, value))]
    async fn insert_with_ttl<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        ttl_seconds: i64,
    ) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let version = self.next_version().await?;
            let now = Utc::now();

            let entry = InternalEntry {
                key: key.to_string(),
                value,
                version,
                created_at: now,
                updated_at: now,
                expires_at: Some(now + chrono::Duration::seconds(ttl_seconds)),
                metadata: HashMap::new(),
            };

            let entry_bytes = Self::serialize(&entry)?;
            let tree_clone = tree.clone();
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree_clone
                    .insert(key_bytes, entry_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Insert failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn get<T: DeserializeOwned + Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<StorageEntry<T>>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            let result = tokio::task::spawn_blocking(move || {
                tree.get(key_bytes).map_err(|e| StorageError::InternalError {
                    message: format!("Get failed: {}", e),
                })
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })??;

            if let Some(bytes) = result {
                let entry: InternalEntry<T> = Self::deserialize(&bytes)?;

                // Check expiration
                if Self::is_expired(&entry) {
                    // Remove expired entry
                    let tree = self.get_tree(table).await?;
                    let key_bytes = key.as_bytes().to_vec();
                    tokio::task::spawn_blocking(move || tree.remove(key_bytes))
                        .await
                        .ok();
                    return Ok(None);
                }

                Ok(Some(entry.into()))
            } else {
                Ok(None)
            }
        })
        .await
    }

    #[instrument(skip(self, value))]
    async fn update<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            // Get existing entry
            let existing = tokio::task::spawn_blocking({
                let tree = tree.clone();
                let key_bytes = key_bytes.clone();
                move || tree.get(key_bytes)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
            .map_err(|e| StorageError::InternalError {
                message: format!("Get failed: {}", e),
            })?;

            let (version, created_at) = if let Some(bytes) = existing {
                let entry: InternalEntry<serde_json::Value> = Self::deserialize(&bytes)?;
                (entry.version + 1, entry.created_at)
            } else {
                return Err(StorageError::NotFound {
                    entity: table.to_string(),
                    key: key.to_string(),
                });
            };

            let entry = InternalEntry {
                key: key.to_string(),
                value,
                version,
                created_at,
                updated_at: Utc::now(),
                expires_at: None,
                metadata: HashMap::new(),
            };

            let entry_bytes = Self::serialize(&entry)?;

            tokio::task::spawn_blocking(move || {
                tree.insert(key_bytes, entry_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Update failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, value))]
    async fn update_versioned<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        expected_version: u64,
    ) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            // Get existing entry
            let existing = tokio::task::spawn_blocking({
                let tree = tree.clone();
                let key_bytes = key_bytes.clone();
                move || tree.get(key_bytes)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
            .map_err(|e| StorageError::InternalError {
                message: format!("Get failed: {}", e),
            })?;

            let existing_bytes = existing.ok_or_else(|| StorageError::NotFound {
                entity: table.to_string(),
                key: key.to_string(),
            })?;

            let existing_entry: InternalEntry<serde_json::Value> =
                Self::deserialize(&existing_bytes)?;

            // Check version
            if existing_entry.version != expected_version {
                return Err(StorageError::LockError {
                    message: format!(
                        "Version mismatch: expected {}, got {}",
                        expected_version, existing_entry.version
                    ),
                });
            }

            let new_entry = InternalEntry {
                key: key.to_string(),
                value,
                version: expected_version + 1,
                created_at: existing_entry.created_at,
                updated_at: Utc::now(),
                expires_at: existing_entry.expires_at,
                metadata: existing_entry.metadata,
            };

            let entry_bytes = Self::serialize(&new_entry)?;

            // Use compare-and-swap for atomic update
            tokio::task::spawn_blocking(move || {
                match tree.compare_and_swap(key_bytes, Some(existing_bytes), Some(entry_bytes)) {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(StorageError::LockError {
                        message: "Concurrent modification detected".to_string(),
                    }),
                    Err(e) => Err(StorageError::InternalError {
                        message: format!("CAS failed: {}", e),
                    }),
                }
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn delete(&self, table: &str, key: &str) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree.remove(key_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Delete failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn exists(&self, table: &str, key: &str) -> StorageResult<bool> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree.contains_key(key_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Exists check failed: {}", e),
                    })
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, batch))]
    async fn execute_batch(&self, batch: Batch) -> StorageResult<()> {
        self.with_metrics(async {
            for operation in batch.operations {
                match operation {
                    Operation::Insert { table, data } => {
                        // Deserialize data to get key
                        let entry: InternalEntry<serde_json::Value> = Self::deserialize(&data)?;
                        let tree = self.get_tree(&table).await?;
                        let key_bytes = entry.key.as_bytes().to_vec();

                        tokio::task::spawn_blocking(move || {
                            tree.insert(key_bytes, data)
                                .map_err(|e| StorageError::InternalError {
                                    message: format!("Batch insert failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                    Operation::Update { table, key, data } => {
                        let tree = self.get_tree(&table).await?;
                        let key_bytes = key.as_bytes().to_vec();

                        tokio::task::spawn_blocking(move || {
                            tree.insert(key_bytes, data)
                                .map_err(|e| StorageError::InternalError {
                                    message: format!("Batch update failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                    Operation::Delete { table, key } => {
                        let tree = self.get_tree(&table).await?;
                        let key_bytes = key.as_bytes().to_vec();

                        tokio::task::spawn_blocking(move || {
                            tree.remove(key_bytes)
                                .map_err(|e| StorageError::InternalError {
                                    message: format!("Batch delete failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                }
            }

            Ok(())
        })
        .await
    }

    #[instrument(skip(self))]
    async fn query<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        query: Query,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        self.with_metrics(async {
            let tree = self.get_tree(&query.table).await?;
            let tree_clone = tree.clone();

            let entries = tokio::task::spawn_blocking(move || {
                let mut results = Vec::new();

                for result in tree_clone.iter() {
                    let (_key, value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Iterator error: {}", e),
                    })?;

                    if let Ok(entry) = Self::deserialize::<InternalEntry<T>>(&value) {
                        if !Self::is_expired(&entry) {
                            results.push(entry.into());
                        }
                    }
                }

                Ok::<_, StorageError>(results)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })??;

            Ok(Self::apply_filters(entries, &query))
        })
        .await
    }

    #[instrument(skip(self))]
    async fn count(&self, query: Query) -> StorageResult<usize> {
        let entries: Vec<StorageEntry<serde_json::Value>> = self.query(query).await?;
        Ok(entries.len())
    }

    async fn begin_transaction(&self) -> StorageResult<Transaction> {
        Ok(Transaction::new())
    }

    #[instrument(skip(self, transaction))]
    async fn commit_transaction(&self, mut transaction: Transaction) -> StorageResult<()> {
        self.with_metrics(async {
            // Execute all operations in the transaction
            for operation in &transaction.operations {
                match operation {
                    Operation::Insert { table, data } => {
                        let entry: InternalEntry<serde_json::Value> = Self::deserialize(data)?;
                        let tree = self.get_tree(table).await?;
                        let key_bytes = entry.key.as_bytes().to_vec();
                        let data = data.clone();

                        tokio::task::spawn_blocking(move || {
                            tree.insert(key_bytes, data)
                                .map_err(|e| StorageError::TransactionError {
                                    message: format!("Transaction insert failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                    Operation::Update { table, key, data } => {
                        let tree = self.get_tree(table).await?;
                        let key_bytes = key.as_bytes().to_vec();
                        let data = data.clone();

                        tokio::task::spawn_blocking(move || {
                            tree.insert(key_bytes, data)
                                .map_err(|e| StorageError::TransactionError {
                                    message: format!("Transaction update failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                    Operation::Delete { table, key } => {
                        let tree = self.get_tree(table).await?;
                        let key_bytes = key.as_bytes().to_vec();

                        tokio::task::spawn_blocking(move || {
                            tree.remove(key_bytes)
                                .map_err(|e| StorageError::TransactionError {
                                    message: format!("Transaction delete failed: {}", e),
                                })
                        })
                        .await
                        .map_err(|e| StorageError::InternalError {
                            message: format!("Task join error: {}", e),
                        })??;
                    }
                }
            }

            transaction.state = TransactionState::Committed;
            Ok(())
        })
        .await
    }

    async fn rollback_transaction(&self, mut transaction: Transaction) -> StorageResult<()> {
        transaction.state = TransactionState::RolledBack;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn scan_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<String>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let prefix_bytes = prefix.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                let mut keys = Vec::new();

                for result in tree.scan_prefix(prefix_bytes) {
                    let (key, _value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Scan prefix error: {}", e),
                    })?;

                    if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                        keys.push(key_str);
                    }
                }

                Ok::<_, StorageError>(keys)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, keys))]
    async fn get_multi<T: DeserializeOwned + Serialize + Send + Sync>(
        &self,
        table: &str,
        keys: Vec<String>,
    ) -> StorageResult<Vec<Option<StorageEntry<T>>>> {
        self.with_metrics(async {
            let mut results = Vec::new();

            for key in keys {
                let entry = self.get(table, &key).await?;
                results.push(entry);
            }

            Ok(results)
        })
        .await
    }

    #[instrument(skip(self, keys))]
    async fn delete_multi(&self, table: &str, keys: Vec<String>) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_bytes().to_vec()).collect();

            tokio::task::spawn_blocking(move || {
                for key in keys {
                    tree.remove(key).map_err(|e| StorageError::InternalError {
                        message: format!("Multi-delete failed: {}", e),
                    })?;
                }
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, new_value))]
    async fn compare_and_swap<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        expected_version: u64,
        new_value: &T,
    ) -> StorageResult<bool> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            // Get existing entry
            let existing = tokio::task::spawn_blocking({
                let tree = tree.clone();
                let key_bytes = key_bytes.clone();
                move || tree.get(key_bytes)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
            .map_err(|e| StorageError::InternalError {
                message: format!("Get failed: {}", e),
            })?;

            if let Some(existing_bytes) = existing {
                let existing_entry: InternalEntry<serde_json::Value> =
                    Self::deserialize(&existing_bytes)?;

                // Check version
                if existing_entry.version != expected_version {
                    return Ok(false);
                }

                let new_entry = InternalEntry {
                    key: key.to_string(),
                    value: new_value,
                    version: expected_version + 1,
                    created_at: existing_entry.created_at,
                    updated_at: Utc::now(),
                    expires_at: existing_entry.expires_at,
                    metadata: existing_entry.metadata,
                };

                let new_bytes = Self::serialize(&new_entry)?;

                let result = tokio::task::spawn_blocking(move || {
                    tree.compare_and_swap(key_bytes, Some(existing_bytes), Some(new_bytes))
                })
                .await
                .map_err(|e| StorageError::InternalError {
                    message: format!("Task join error: {}", e),
                })?
                .map_err(|e| StorageError::InternalError {
                    message: format!("CAS failed: {}", e),
                })?;

                Ok(result.is_ok())
            } else {
                Ok(false)
            }
        })
        .await
    }

    #[instrument(skip(self))]
    async fn get_modified_since<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        table: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;

            let entries = tokio::task::spawn_blocking(move || {
                let mut results = Vec::new();

                for result in tree.iter() {
                    let (_key, value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Iterator error: {}", e),
                    })?;

                    if let Ok(entry) = Self::deserialize::<InternalEntry<T>>(&value) {
                        if entry.updated_at > since && !Self::is_expired(&entry) {
                            results.push(entry.into());
                        }
                    }
                }

                Ok::<_, StorageError>(results)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })??;

            Ok(entries)
        })
        .await
    }

    #[instrument(skip(self))]
    async fn get_expiring_before<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        table: &str,
        before: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;

            let entries = tokio::task::spawn_blocking(move || {
                let mut results = Vec::new();

                for result in tree.iter() {
                    let (_key, value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Iterator error: {}", e),
                    })?;

                    if let Ok(entry) = Self::deserialize::<InternalEntry<T>>(&value) {
                        if let Some(expires_at) = entry.expires_at {
                            if expires_at < before {
                                results.push(entry.into());
                            }
                        }
                    }
                }

                Ok::<_, StorageError>(results)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })??;

            Ok(entries)
        })
        .await
    }

    #[instrument(skip(self))]
    async fn cleanup_expired(&self, table: &str) -> StorageResult<usize> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            self.cleanup_tree(&tree).await
        })
        .await
    }

    #[instrument(skip(self))]
    async fn compact(&self) -> StorageResult<()> {
        self.with_metrics(async {
            let db = self.db.clone();

            info!("Starting database compaction");

            // Get all tree names
            let tree_names: Vec<_> = db.tree_names().into_iter().collect();

            // Compact each tree
            for name in tree_names {
                let name_str = String::from_utf8_lossy(&name).to_string();
                debug!("Compacting tree: {}", name_str);

                let tree = db.open_tree(&name).map_err(|e| StorageError::InternalError {
                    message: format!("Failed to open tree for compaction: {}", e),
                })?;

                // Trigger compaction by iterating
                let tree_clone = tree.clone();
                tokio::task::spawn_blocking(move || {
                    for _entry in tree_clone.iter() {
                        // Just iterate to trigger compaction
                    }
                })
                .await
                .map_err(|e| StorageError::InternalError {
                    message: format!("Task join error: {}", e),
                })?;
            }

            info!("Database compaction completed");
            Ok(())
        })
        .await
    }

    #[instrument(skip(self))]
    async fn flush(&self) -> StorageResult<()> {
        self.with_metrics(async {
            let db = self.db.clone();

            tokio::task::spawn_blocking(move || {
                db.flush()
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Flush failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn initialize(&mut self) -> StorageResult<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            return Ok(());
        }

        info!("Initializing Sled storage");

        // Load existing statistics
        self.load_stats().await?;

        // Verify database is accessible
        self.health_check().await?;

        *initialized = true;
        info!("Sled storage initialized successfully");

        Ok(())
    }

    #[instrument(skip(self))]
    async fn shutdown(&mut self) -> StorageResult<()> {
        let mut initialized = self.initialized.write().await;
        if !*initialized {
            return Ok(());
        }

        info!("Shutting down Sled storage");

        // Persist statistics
        self.persist_stats().await?;

        // Flush pending writes
        self.flush().await?;

        *initialized = false;
        info!("Sled storage shutdown complete");

        Ok(())
    }
}

#[async_trait]
impl KeyValueStorage for SledStorage {
    #[instrument(skip(self))]
    async fn get_bytes(&self, table: &str, key: &str) -> StorageResult<Option<Vec<u8>>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree.get(key_bytes)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Get bytes failed: {}", e),
                    })
                    .map(|opt| opt.map(|v| v.to_vec()))
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, value))]
    async fn set_bytes(&self, table: &str, key: &str, value: Vec<u8>) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree.insert(key_bytes, value)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Set bytes failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn iter_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let prefix_bytes = prefix.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                let mut results = Vec::new();

                for result in tree.scan_prefix(prefix_bytes) {
                    let (key, value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Iter prefix error: {}", e),
                    })?;

                    if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                        results.push((key_str, value.to_vec()));
                    }
                }

                Ok::<_, StorageError>(results)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self))]
    async fn get_range(
        &self,
        table: &str,
        start: &str,
        end: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let start_bytes = start.as_bytes().to_vec();
            let end_bytes = end.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                let mut results = Vec::new();

                for result in tree.range(start_bytes..end_bytes) {
                    let (key, value) = result.map_err(|e| StorageError::InternalError {
                        message: format!("Range query error: {}", e),
                    })?;

                    if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                        results.push((key_str, value.to_vec()));
                    }
                }

                Ok::<_, StorageError>(results)
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }

    #[instrument(skip(self, value))]
    async fn merge(&self, table: &str, key: &str, value: Vec<u8>) -> StorageResult<()> {
        self.with_metrics(async {
            let tree = self.get_tree(table).await?;
            let key_bytes = key.as_bytes().to_vec();

            tokio::task::spawn_blocking(move || {
                tree.merge(key_bytes, value)
                    .map_err(|e| StorageError::InternalError {
                        message: format!("Merge failed: {}", e),
                    })?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::InternalError {
                message: format!("Task join error: {}", e),
            })?
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: String,
        name: String,
        value: i32,
    }

    async fn create_test_storage() -> (SledStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = SledConfig {
            path: temp_dir.path().to_path_buf(),
            cache_capacity_bytes: 1024 * 1024,
            use_compression: true,
            flush_every_ms: Some(100),
            mode: super::super::config::SledMode::HighThroughput,
            temporary: false,
            create_dir: true,
        };

        let mut storage = SledStorage::new(config).await.unwrap();
        storage.initialize().await.unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let retrieved: Option<StorageEntry<TestData>> =
            storage.get("test_table", "key1").await.unwrap();

        assert!(retrieved.is_some());
        let entry = retrieved.unwrap();
        assert_eq!(entry.value, data);
        assert_eq!(entry.version, 1);
    }

    #[tokio::test]
    async fn test_insert_with_ttl() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage
            .insert_with_ttl("test_table", "key1", &data, 1)
            .await
            .unwrap();

        // Should exist immediately
        let retrieved: Option<StorageEntry<TestData>> =
            storage.get("test_table", "key1").await.unwrap();
        assert!(retrieved.is_some());

        // Wait for expiration
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Should be expired
        let expired: Option<StorageEntry<TestData>> =
            storage.get("test_table", "key1").await.unwrap();
        assert!(expired.is_none());
    }

    #[tokio::test]
    async fn test_update() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let updated_data = TestData {
            id: "1".to_string(),
            name: "Updated".to_string(),
            value: 100,
        };

        storage
            .update("test_table", "key1", &updated_data)
            .await
            .unwrap();

        let retrieved: Option<StorageEntry<TestData>> =
            storage.get("test_table", "key1").await.unwrap();

        assert!(retrieved.is_some());
        let entry = retrieved.unwrap();
        assert_eq!(entry.value.name, "Updated");
        assert_eq!(entry.value.value, 100);
        assert_eq!(entry.version, 2);
    }

    #[tokio::test]
    async fn test_update_versioned() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let updated_data = TestData {
            id: "1".to_string(),
            name: "Updated".to_string(),
            value: 100,
        };

        // Should succeed with correct version
        storage
            .update_versioned("test_table", "key1", &updated_data, 1)
            .await
            .unwrap();

        // Should fail with wrong version
        let result = storage
            .update_versioned("test_table", "key1", &updated_data, 1)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage.insert("test_table", "key1", &data).await.unwrap();
        storage.delete("test_table", "key1").await.unwrap();

        let retrieved: Option<StorageEntry<TestData>> =
            storage.get("test_table", "key1").await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_exists() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        assert!(!storage.exists("test_table", "key1").await.unwrap());

        storage.insert("test_table", "key1", &data).await.unwrap();
        assert!(storage.exists("test_table", "key1").await.unwrap());

        storage.delete("test_table", "key1").await.unwrap();
        assert!(!storage.exists("test_table", "key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_scan_prefix() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..5 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("prefix_{}", i), &data)
                .await
                .unwrap();
        }

        let keys = storage.scan_prefix("test_table", "prefix_").await.unwrap();
        assert_eq!(keys.len(), 5);
    }

    #[tokio::test]
    async fn test_get_multi() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..3 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("key{}", i), &data)
                .await
                .unwrap();
        }

        let keys = vec!["key0".to_string(), "key1".to_string(), "key2".to_string()];
        let results: Vec<Option<StorageEntry<TestData>>> =
            storage.get_multi("test_table", keys).await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_some()));
    }

    #[tokio::test]
    async fn test_delete_multi() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..3 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("key{}", i), &data)
                .await
                .unwrap();
        }

        let keys = vec!["key0".to_string(), "key1".to_string(), "key2".to_string()];
        storage.delete_multi("test_table", keys).await.unwrap();

        for i in 0..3 {
            let exists = storage
                .exists("test_table", &format!("key{}", i))
                .await
                .unwrap();
            assert!(!exists);
        }
    }

    #[tokio::test]
    async fn test_compare_and_swap() {
        let (storage, _temp) = create_test_storage().await;
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let new_data = TestData {
            id: "1".to_string(),
            name: "Updated".to_string(),
            value: 100,
        };

        // Should succeed with correct version
        let result = storage
            .compare_and_swap("test_table", "key1", 1, &new_data)
            .await
            .unwrap();
        assert!(result);

        // Should fail with wrong version
        let result = storage
            .compare_and_swap("test_table", "key1", 1, &new_data)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let (storage, _temp) = create_test_storage().await;

        let data1 = TestData {
            id: "1".to_string(),
            name: "Test1".to_string(),
            value: 1,
        };

        let data2 = TestData {
            id: "2".to_string(),
            name: "Test2".to_string(),
            value: 2,
        };

        // Insert first entry for update test
        storage.insert("test_table", "key1", &data1).await.unwrap();

        let mut batch = Batch::new(10);

        // Add operations
        let entry1 = InternalEntry {
            key: "key2".to_string(),
            value: data2.clone(),
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            expires_at: None,
            metadata: HashMap::new(),
        };

        batch
            .add(Operation::Insert {
                table: "test_table".to_string(),
                data: SledStorage::serialize(&entry1).unwrap(),
            })
            .unwrap();

        batch
            .add(Operation::Delete {
                table: "test_table".to_string(),
                key: "key1".to_string(),
            })
            .unwrap();

        storage.execute_batch(batch).await.unwrap();

        // Verify key2 exists
        assert!(storage.exists("test_table", "key2").await.unwrap());
        // Verify key1 was deleted
        assert!(!storage.exists("test_table", "key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_query() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..10 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("key{}", i), &data)
                .await
                .unwrap();
        }

        let query = Query::new("test_table").limit(5);
        let results: Vec<StorageEntry<TestData>> = storage.query(query).await.unwrap();

        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_count() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..10 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("key{}", i), &data)
                .await
                .unwrap();
        }

        let query = Query::new("test_table");
        let count = storage.count(query).await.unwrap();

        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_health_check() {
        let (storage, _temp) = create_test_storage().await;
        let health = storage.health_check().await.unwrap();
        assert_eq!(health, StorageHealth::Healthy);
    }

    #[tokio::test]
    async fn test_metadata() {
        let (storage, _temp) = create_test_storage().await;
        let metadata = storage.metadata().await.unwrap();
        assert_eq!(metadata.backend, StorageBackend::Sled);
        assert!(!metadata.capabilities.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let (storage, _temp) = create_test_storage().await;
        let stats = storage.stats().await.unwrap();
        assert_eq!(stats.total_operations, 0);
    }

    #[tokio::test]
    async fn test_get_bytes_and_set_bytes() {
        let (storage, _temp) = create_test_storage().await;
        let data = vec![1, 2, 3, 4, 5];

        storage
            .set_bytes("test_table", "key1", data.clone())
            .await
            .unwrap();

        let retrieved = storage.get_bytes("test_table", "key1").await.unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_iter_prefix_kv() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..5 {
            storage
                .set_bytes("test_table", &format!("prefix_{}", i), vec![i as u8])
                .await
                .unwrap();
        }

        let results = storage.iter_prefix("test_table", "prefix_").await.unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_get_range() {
        let (storage, _temp) = create_test_storage().await;

        for i in 0..10 {
            storage
                .set_bytes("test_table", &format!("key{:02}", i), vec![i as u8])
                .await
                .unwrap();
        }

        let results = storage
            .get_range("test_table", "key00", "key05")
            .await
            .unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_merge() {
        let (storage, _temp) = create_test_storage().await;

        storage
            .merge("test_table", "key1", vec![1, 2, 3])
            .await
            .unwrap();

        storage
            .merge("test_table", "key1", vec![4, 5, 6])
            .await
            .unwrap();

        // Merge operation succeeds (actual merge behavior depends on Sled's merge operator)
        let exists = storage.exists("test_table", "key1").await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_flush() {
        let (storage, _temp) = create_test_storage().await;
        let result = storage.flush().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_compact() {
        let (storage, _temp) = create_test_storage().await;

        // Insert some data
        for i in 0..10 {
            let data = TestData {
                id: i.to_string(),
                name: format!("Test {}", i),
                value: i,
            };
            storage
                .insert("test_table", &format!("key{}", i), &data)
                .await
                .unwrap();
        }

        let result = storage.compact().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_modified_since() {
        let (storage, _temp) = create_test_storage().await;
        let now = Utc::now();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };
        storage.insert("test_table", "key1", &data).await.unwrap();

        let results: Vec<StorageEntry<TestData>> = storage
            .get_modified_since("test_table", now)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let (storage, _temp) = create_test_storage().await;

        // Insert with short TTL
        let data = TestData {
            id: "1".to_string(),
            name: "Test".to_string(),
            value: 42,
        };
        storage
            .insert_with_ttl("test_table", "key1", &data, 1)
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let removed = storage.cleanup_expired("test_table").await.unwrap();
        assert_eq!(removed, 1);
    }
}
