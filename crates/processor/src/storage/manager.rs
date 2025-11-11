//! Storage Manager
//!
//! Coordinates multiple storage backends and provides unified access

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

use super::error::{StorageError, StorageResult};
use super::traits::{Backup, BackupMetadata, Migrator, Migration, Storage};
use super::types::{
    Batch, Query, StorageBackend, StorageEntry, StorageHealth, StorageMetadata, StorageStats,
    Transaction,
};
use super::wrapper::AnyStorage;

/// Storage manager state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerState {
    Uninitialized,
    Initializing,
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Storage manager statistics
#[derive(Debug, Clone, Default)]
pub struct ManagerStats {
    /// Total operations across all backends
    pub total_operations: u64,

    /// Successful operations
    pub successful_operations: u64,

    /// Failed operations
    pub failed_operations: u64,

    /// Operations by backend
    pub operations_by_backend: HashMap<String, u64>,

    /// Fallback count (when primary backend fails)
    pub fallback_count: u64,

    /// Last updated
    pub last_updated: DateTime<Utc>,
}

/// Storage routing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Route to primary backend only
    PrimaryOnly,
    /// Route reads to cache, writes to primary
    CacheAside,
    /// Write to multiple backends
    MultiWrite,
    /// Read from fastest available backend
    FastestRead,
}

/// Storage manager coordinating multiple backends
pub struct StorageManager {
    /// Manager state
    state: Arc<RwLock<ManagerState>>,

    /// Primary storage backend
    primary: Option<Arc<AnyStorage>>,

    /// Cache backend (e.g., Redis)
    cache: Option<Arc<AnyStorage>>,

    /// Secondary/backup backend
    secondary: Option<Arc<AnyStorage>>,

    /// Available backends by type
    backends: Arc<RwLock<HashMap<StorageBackend, Arc<AnyStorage>>>>,

    /// Routing strategy
    routing_strategy: RoutingStrategy,

    /// Enable automatic fallback
    enable_fallback: bool,

    /// Manager statistics
    stats: Arc<RwLock<ManagerStats>>,
}

impl StorageManager {
    /// Create a new storage manager
    pub fn new(routing_strategy: RoutingStrategy) -> Self {
        Self {
            state: Arc::new(RwLock::new(ManagerState::Uninitialized)),
            primary: None,
            cache: None,
            secondary: None,
            backends: Arc::new(RwLock::new(HashMap::new())),
            routing_strategy,
            enable_fallback: true,
            stats: Arc::new(RwLock::new(ManagerStats::default())),
        }
    }

    /// Set primary storage backend
    pub fn with_primary(mut self, backend: Arc<AnyStorage>) -> Self {
        let backend_type = backend.backend();
        self.primary = Some(backend.clone());

        // Store in backends map (need to do this after creation)
        self.backends = Arc::new(RwLock::new({
            let mut map = HashMap::new();
            map.insert(backend_type, backend);
            map
        }));

        self
    }

    /// Set cache backend
    pub fn with_cache(mut self, backend: Arc<AnyStorage>) -> Self {
        self.cache = Some(backend.clone());
        self
    }

    /// Set secondary backend
    pub fn with_secondary(mut self, backend: Arc<AnyStorage>) -> Self {
        self.secondary = Some(backend);
        self
    }

    /// Enable or disable automatic fallback
    pub fn with_fallback(mut self, enable: bool) -> Self {
        self.enable_fallback = enable;
        self
    }

    /// Get current state
    pub async fn state(&self) -> ManagerState {
        *self.state.read().await
    }

    /// Get manager statistics
    pub async fn manager_stats(&self) -> ManagerStats {
        self.stats.read().await.clone()
    }

    /// Add a backend dynamically
    #[instrument(skip(self, backend))]
    pub async fn add_backend(&self, backend: Arc<AnyStorage>) -> StorageResult<()> {
        let backend_type = backend.backend();
        info!("Adding backend: {:?}", backend_type);

        let mut backends = self.backends.write().await;
        backends.insert(backend_type, backend);

        Ok(())
    }

    /// Remove a backend
    #[instrument(skip(self))]
    pub async fn remove_backend(&self, backend_type: StorageBackend) -> StorageResult<()> {
        info!("Removing backend: {:?}", backend_type);

        let mut backends = self.backends.write().await;
        backends.remove(&backend_type);

        Ok(())
    }

    /// Get backend by type
    pub async fn get_backend(
        &self,
        backend_type: StorageBackend,
    ) -> Option<Arc<AnyStorage>> {
        let backends = self.backends.read().await;
        backends.get(&backend_type).cloned()
    }

    /// List all available backends
    pub async fn list_backends(&self) -> Vec<StorageBackend> {
        let backends = self.backends.read().await;
        backends.keys().copied().collect()
    }

    /// Initialize all backends
    #[instrument(skip(self))]
    pub async fn initialize(&mut self) -> StorageResult<()> {
        info!("Initializing storage manager");
        *self.state.write().await = ManagerState::Initializing;

        // Initialize primary backend
        if let Some(ref mut primary) = self.primary {
            if let Some(storage) = Arc::get_mut(primary) {
                storage.initialize().await?;
            }
        }

        // Initialize cache backend
        if let Some(ref mut cache) = self.cache {
            if let Some(storage) = Arc::get_mut(cache) {
                storage.initialize().await?;
            }
        }

        // Initialize secondary backend
        if let Some(ref mut secondary) = self.secondary {
            if let Some(storage) = Arc::get_mut(secondary) {
                storage.initialize().await?;
            }
        }

        *self.state.write().await = ManagerState::Running;
        info!("Storage manager initialized successfully");

        Ok(())
    }

    /// Shutdown all backends
    #[instrument(skip(self))]
    pub async fn shutdown(&mut self) -> StorageResult<()> {
        info!("Shutting down storage manager");
        *self.state.write().await = ManagerState::Stopping;

        // Shutdown all backends
        if let Some(ref mut primary) = self.primary {
            if let Some(storage) = Arc::get_mut(primary) {
                storage.shutdown().await?;
            }
        }

        if let Some(ref mut cache) = self.cache {
            if let Some(storage) = Arc::get_mut(cache) {
                storage.shutdown().await?;
            }
        }

        if let Some(ref mut secondary) = self.secondary {
            if let Some(storage) = Arc::get_mut(secondary) {
                storage.shutdown().await?;
            }
        }

        *self.state.write().await = ManagerState::Stopped;
        info!("Storage manager shut down successfully");

        Ok(())
    }

    /// Execute operation with fallback
    async fn execute_with_fallback<F, T>(&self, operation: F) -> StorageResult<T>
    where
        F: Fn(Arc<AnyStorage>) -> futures::future::BoxFuture<'static, StorageResult<T>> + Send,
        T: Send,
    {
        // Try primary backend
        if let Some(ref primary) = self.primary {
            match operation(primary.clone()).await {
                Ok(result) => {
                    self.record_success(primary.backend()).await;
                    return Ok(result);
                }
                Err(e) if !self.enable_fallback => {
                    self.record_failure(primary.backend()).await;
                    return Err(e);
                }
                Err(e) => {
                    warn!("Primary backend failed: {}, trying fallback", e);
                    self.record_failure(primary.backend()).await;
                }
            }
        }

        // Try secondary backend as fallback
        if let Some(ref secondary) = self.secondary {
            match operation(secondary.clone()).await {
                Ok(result) => {
                    self.record_success(secondary.backend()).await;
                    self.record_fallback().await;
                    return Ok(result);
                }
                Err(e) => {
                    error!("Secondary backend also failed: {}", e);
                    self.record_failure(secondary.backend()).await;
                    return Err(e);
                }
            }
        }

        Err(StorageError::BackendNotAvailable {
            backend: "all".to_string(),
        })
    }

    /// Record successful operation
    async fn record_success(&self, backend: StorageBackend) {
        let mut stats = self.stats.write().await;
        stats.total_operations += 1;
        stats.successful_operations += 1;
        *stats
            .operations_by_backend
            .entry(backend.name().to_string())
            .or_insert(0) += 1;
        stats.last_updated = Utc::now();
    }

    /// Record failed operation
    async fn record_failure(&self, backend: StorageBackend) {
        let mut stats = self.stats.write().await;
        stats.total_operations += 1;
        stats.failed_operations += 1;
        stats.last_updated = Utc::now();
    }

    /// Record fallback usage
    async fn record_fallback(&self) {
        let mut stats = self.stats.write().await;
        stats.fallback_count += 1;
    }

    /// Health check all backends
    #[instrument(skip(self))]
    pub async fn health_check_all(&self) -> HashMap<StorageBackend, StorageHealth> {
        let mut results = HashMap::new();

        let backends = self.backends.read().await;
        for (backend_type, storage) in backends.iter() {
            match storage.health_check().await {
                Ok(health) => {
                    results.insert(*backend_type, health);
                }
                Err(e) => {
                    error!("Health check failed for {:?}: {}", backend_type, e);
                    results.insert(*backend_type, StorageHealth::Unhealthy);
                }
            }
        }

        results
    }

    /// Get statistics from all backends
    #[instrument(skip(self))]
    pub async fn stats_all(&self) -> HashMap<StorageBackend, StorageStats> {
        let mut results = HashMap::new();

        let backends = self.backends.read().await;
        for (backend_type, storage) in backends.iter() {
            match storage.stats().await {
                Ok(stats) => {
                    results.insert(*backend_type, stats);
                }
                Err(e) => {
                    error!("Failed to get stats for {:?}: {}", backend_type, e);
                }
            }
        }

        results
    }
}

// Implement Debug for StorageManager
impl std::fmt::Debug for StorageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageManager")
            .field("routing_strategy", &self.routing_strategy)
            .field("has_primary", &self.primary.is_some())
            .field("has_cache", &self.cache.is_some())
            .field("has_secondary", &self.secondary.is_some())
            .finish()
    }
}

// Implement Storage trait for StorageManager
#[async_trait]
impl Storage for StorageManager {
    fn backend(&self) -> StorageBackend {
        if let Some(ref primary) = self.primary {
            primary.backend()
        } else {
            StorageBackend::Sled // Default
        }
    }

    async fn metadata(&self) -> StorageResult<StorageMetadata> {
        if let Some(ref primary) = self.primary {
            primary.metadata().await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    async fn health_check(&self) -> StorageResult<StorageHealth> {
        // Check if any backend is healthy
        let health_results = self.health_check_all().await;

        if health_results.values().any(|h| h == &StorageHealth::Healthy) {
            Ok(StorageHealth::Healthy)
        } else if health_results
            .values()
            .any(|h| h == &StorageHealth::Degraded)
        {
            Ok(StorageHealth::Degraded)
        } else {
            Ok(StorageHealth::Unhealthy)
        }
    }

    async fn stats(&self) -> StorageResult<StorageStats> {
        if let Some(ref primary) = self.primary {
            primary.stats().await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, value))]
    async fn insert<T: Serialize + DeserializeOwned + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        match self.routing_strategy {
            RoutingStrategy::CacheAside | RoutingStrategy::PrimaryOnly => {
                // Write to primary only
                let table = table.to_string();
                let key = key.to_string();
                let value_clone = serde_json::to_value(value).map_err(|e| {
                    StorageError::SerializationError {
                        source: Box::new(e),
                    }
                })?;

                self.execute_with_fallback(|storage| {
                    let table = table.clone();
                    let key = key.clone();
                    let value = value_clone.clone();
                    Box::pin(async move {
                        storage
                            .insert(
                                &table,
                                &key,
                                &serde_json::from_value::<T>(value).unwrap(),
                            )
                            .await
                    })
                })
                .await
            }
            RoutingStrategy::MultiWrite => {
                // Write to both primary and secondary
                if let Some(ref primary) = self.primary {
                    primary.insert(table, key, value).await?;
                }
                if let Some(ref secondary) = self.secondary {
                    let _ = secondary.insert(table, key, value).await; // Best effort
                }
                Ok(())
            }
            RoutingStrategy::FastestRead => {
                // Same as primary only for writes
                if let Some(ref primary) = self.primary {
                    primary.insert(table, key, value).await
                } else {
                    Err(StorageError::BackendNotAvailable {
                        backend: "primary".to_string(),
                    })
                }
            }
        }
    }

    #[instrument(skip(self, value))]
    async fn insert_with_ttl<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        ttl_seconds: i64,
    ) -> StorageResult<()> {
        // Write to cache if available, otherwise primary
        if let Some(ref cache) = self.cache {
            cache.insert_with_ttl(table, key, value, ttl_seconds).await
        } else if let Some(ref primary) = self.primary {
            primary
                .insert_with_ttl(table, key, value, ttl_seconds)
                .await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "any".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn get<T: DeserializeOwned + Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<StorageEntry<T>>> {
        match self.routing_strategy {
            RoutingStrategy::CacheAside => {
                // Try cache first, then primary
                if let Some(ref cache) = self.cache {
                    if let Ok(Some(entry)) = cache.get(table, key).await {
                        debug!("Cache hit for key: {}", key);
                        return Ok(Some(entry));
                    }
                }

                // Cache miss, get from primary
                if let Some(ref primary) = self.primary {
                    let entry = primary.get(table, key).await?;

                    // Populate cache
                    if let Some(ref cache) = self.cache {
                        if let Some(ref e) = entry {
                            let _ = cache.insert(table, key, &e.value).await;
                        }
                    }

                    Ok(entry)
                } else {
                    Err(StorageError::BackendNotAvailable {
                        backend: "primary".to_string(),
                    })
                }
            }
            _ => {
                // Read from primary
                if let Some(ref primary) = self.primary {
                    primary.get(table, key).await
                } else {
                    Err(StorageError::BackendNotAvailable {
                        backend: "primary".to_string(),
                    })
                }
            }
        }
    }

    #[instrument(skip(self, value))]
    async fn update<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.update(table, key, value).await?;

            // Invalidate cache
            if let Some(ref cache) = self.cache {
                let _ = cache.delete(table, key).await;
            }

            Ok(())
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, value))]
    async fn update_versioned<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        expected_version: u64,
    ) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary
                .update_versioned(table, key, value, expected_version)
                .await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn delete(&self, table: &str, key: &str) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.delete(table, key).await?;

            // Delete from cache
            if let Some(ref cache) = self.cache {
                let _ = cache.delete(table, key).await;
            }

            Ok(())
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn exists(&self, table: &str, key: &str) -> StorageResult<bool> {
        if let Some(ref primary) = self.primary {
            primary.exists(table, key).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, batch))]
    async fn execute_batch(&self, batch: Batch) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.execute_batch(batch).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn query<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        query: Query,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        if let Some(ref primary) = self.primary {
            primary.query(query).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn count(&self, query: Query) -> StorageResult<usize> {
        if let Some(ref primary) = self.primary {
            primary.count(query).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn begin_transaction(&self) -> StorageResult<Transaction> {
        if let Some(ref primary) = self.primary {
            primary.begin_transaction().await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, transaction))]
    async fn commit_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.commit_transaction(transaction).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, transaction))]
    async fn rollback_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.rollback_transaction(transaction).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn scan_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<String>> {
        if let Some(ref primary) = self.primary {
            primary.scan_prefix(table, prefix).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn get_multi<T: DeserializeOwned + Serialize + Send + Sync>(
        &self,
        table: &str,
        keys: Vec<String>,
    ) -> StorageResult<Vec<Option<StorageEntry<T>>>> {
        if let Some(ref primary) = self.primary {
            primary.get_multi(table, keys).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn delete_multi(&self, table: &str, keys: Vec<String>) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.delete_multi(table, keys).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self, new_value))]
    async fn compare_and_swap<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        expected_version: u64,
        new_value: &T,
    ) -> StorageResult<bool> {
        if let Some(ref primary) = self.primary {
            primary
                .compare_and_swap(table, key, expected_version, new_value)
                .await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn get_modified_since<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        table: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        if let Some(ref primary) = self.primary {
            primary.get_modified_since(table, since).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn get_expiring_before<T: DeserializeOwned + Serialize + Send + Sync + 'static>(
        &self,
        table: &str,
        before: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        if let Some(ref primary) = self.primary {
            primary.get_expiring_before(table, before).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn cleanup_expired(&self, table: &str) -> StorageResult<usize> {
        if let Some(ref primary) = self.primary {
            primary.cleanup_expired(table).await
        } else {
            Err(StorageError::BackendNotAvailable {
                backend: "primary".to_string(),
            })
        }
    }

    #[instrument(skip(self))]
    async fn compact(&self) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.compact().await
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self))]
    async fn flush(&self) -> StorageResult<()> {
        if let Some(ref primary) = self.primary {
            primary.flush().await
        } else {
            Ok(())
        }
    }

    async fn initialize(&mut self) -> StorageResult<()> {
        self.initialize().await
    }

    async fn shutdown(&mut self) -> StorageResult<()> {
        self.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_manager_state() {
        let manager = StorageManager::new(RoutingStrategy::PrimaryOnly);
        assert_eq!(manager.state().await, ManagerState::Uninitialized);
    }

    #[tokio::test]
    async fn test_routing_strategies() {
        let strategies = vec![
            RoutingStrategy::PrimaryOnly,
            RoutingStrategy::CacheAside,
            RoutingStrategy::MultiWrite,
            RoutingStrategy::FastestRead,
        ];

        for strategy in strategies {
            let manager = StorageManager::new(strategy);
            assert_eq!(manager.routing_strategy, strategy);
        }
    }
}
