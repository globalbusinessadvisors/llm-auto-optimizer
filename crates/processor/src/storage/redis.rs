//! Redis storage backend implementation
//!
//! This module provides a production-ready Redis storage backend with comprehensive
//! features including connection pooling, TTL support, atomic operations, pub/sub,
//! distributed locking, and cache statistics.
//!
//! # Features
//!
//! - Connection pooling with deadpool-redis
//! - TTL (time-to-live) support for all entries
//! - Atomic operations (INCR, DECR, CAS)
//! - Key prefix namespacing
//! - Pub/Sub support for real-time messaging
//! - Pipeline operations for batching
//! - Lua scripting for complex atomic operations
//! - Cache statistics (hits, misses, evictions)
//! - Full CRUD operations with JSON serialization
//! - Distributed locking support
//!
//! # Architecture
//!
//! The Redis backend uses a key structure with namespacing:
//! - `{prefix}:{table}:{key}` -> JSON value
//! - `{prefix}:{table}:{key}:version` -> version number
//! - `{prefix}:{table}:{key}:metadata` -> metadata JSON
//! - `{prefix}:stats` -> cache statistics
//!
//! # Example
//!
//! ```rust,no_run
//! use processor::storage::redis::RedisStorage;
//! use processor::storage::config::RedisConfig;
//! use processor::storage::traits::Storage;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RedisConfig::default();
//!     let mut storage = RedisStorage::new(config).await?;
//!     storage.initialize().await?;
//!
//!     // Use storage operations
//!     storage.insert("users", "user:1", &user).await?;
//!
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_redis::{Config as PoolConfig, Connection, Pool, Runtime};
use redis::{AsyncCommands, RedisError, Script};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};

use super::config::RedisConfig;
use super::error::{StorageError, StorageResult};
use super::traits::{CacheStats, CacheStorage, PubSubStorage, Storage, Subscriber};
use super::types::{
    Batch, Operation, Query, StorageBackend, StorageEntry, StorageHealth, StorageMetadata,
    StorageStats, Transaction, TransactionState,
};

/// Redis storage backend with connection pooling and advanced features
#[derive(Debug)]
pub struct RedisStorage {
    /// Connection pool
    pool: Pool,

    /// Configuration
    config: RedisConfig,

    /// Storage statistics
    stats: Arc<RwLock<StorageStats>>,

    /// Cache statistics
    cache_stats: Arc<RwLock<InternalCacheStats>>,

    /// Active transactions
    transactions: Arc<RwLock<HashMap<String, Transaction>>>,

    /// Active subscriptions
    subscriptions: Arc<RwLock<HashMap<String, Arc<Mutex<RedisSubscriber>>>>>,

    /// Lua scripts cache
    scripts: Arc<LuaScripts>,

    /// Shutdown flag
    shutdown: Arc<RwLock<bool>>,
}

/// Internal cache statistics tracking
#[derive(Debug, Clone, Default)]
struct InternalCacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    sets: u64,
    deletes: u64,
}

/// Lua scripts for atomic operations
#[derive(Debug)]
struct LuaScripts {
    /// Compare-and-swap script
    cas: Script,

    /// Versioned update script
    versioned_update: Script,

    /// Conditional delete script
    conditional_delete: Script,

    /// Batch insert with TTL
    batch_insert_ttl: Script,
}

impl LuaScripts {
    fn new() -> Self {
        Self {
            cas: Script::new(
                r#"
                local key = KEYS[1]
                local expected_version = tonumber(ARGV[1])
                local new_value = ARGV[2]
                local new_version = tonumber(ARGV[3])

                local current_version = redis.call('GET', key .. ':version')
                if not current_version then
                    return 0
                end

                if tonumber(current_version) ~= expected_version then
                    return 0
                end

                redis.call('SET', key, new_value)
                redis.call('SET', key .. ':version', new_version)
                return 1
                "#,
            ),
            versioned_update: Script::new(
                r#"
                local key = KEYS[1]
                local expected_version = tonumber(ARGV[1])
                local new_value = ARGV[2]

                local current_version = redis.call('GET', key .. ':version')
                if not current_version then
                    return 0
                end

                if tonumber(current_version) ~= expected_version then
                    return 0
                end

                local new_version = expected_version + 1
                redis.call('SET', key, new_value)
                redis.call('SET', key .. ':version', new_version)
                return new_version
                "#,
            ),
            conditional_delete: Script::new(
                r#"
                local key = KEYS[1]
                local expected_version = tonumber(ARGV[1])

                local current_version = redis.call('GET', key .. ':version')
                if not current_version then
                    return 0
                end

                if tonumber(current_version) == expected_version then
                    redis.call('DEL', key)
                    redis.call('DEL', key .. ':version')
                    redis.call('DEL', key .. ':metadata')
                    return 1
                end

                return 0
                "#,
            ),
            batch_insert_ttl: Script::new(
                r#"
                local ttl = tonumber(ARGV[1])
                for i = 1, #KEYS do
                    local key = KEYS[i]
                    local value = ARGV[i + 1]
                    redis.call('SET', key, value)
                    if ttl > 0 then
                        redis.call('EXPIRE', key, ttl)
                    end
                end
                return #KEYS
                "#,
            ),
        }
    }
}

/// Redis pub/sub subscriber implementation
#[derive(Debug)]
struct RedisSubscriber {
    /// Channel name
    channel: String,

    /// Pub/Sub connection
    pubsub: redis::aio::PubSub,

    /// Message buffer
    buffer: Vec<Vec<u8>>,
}

impl RedisSubscriber {
    async fn new(conn: Connection, channel: String) -> StorageResult<Self> {
        let pubsub = Connection::take(conn)
            .into_pubsub();

        Ok(Self {
            channel,
            pubsub,
            buffer: Vec::new(),
        })
    }
}

#[async_trait]
impl Subscriber for RedisSubscriber {
    async fn next_message(&mut self) -> StorageResult<Option<Vec<u8>>> {
        // Check buffer first
        if !self.buffer.is_empty() {
            return Ok(Some(self.buffer.remove(0)));
        }

        // Wait for next message
        match self.pubsub.get_message().await {
            Ok(msg) => {
                let payload = msg.get_payload_bytes().to_vec();
                trace!("Received pub/sub message on channel {}: {} bytes", self.channel, payload.len());
                Ok(Some(payload))
            }
            Err(e) => {
                error!("Error receiving pub/sub message: {}", e);
                Err(StorageError::ConnectionError {
                    source: Box::new(e),
                })
            }
        }
    }
}

impl RedisStorage {
    /// Create a new Redis storage instance
    pub async fn new(config: RedisConfig) -> StorageResult<Self> {
        info!("Initializing Redis storage backend");
        debug!("Redis configuration: host={}:{}, database={}, key_prefix={}",
            config.host, config.port, config.database, config.key_prefix);

        // Create connection pool
        let pool_config = PoolConfig::from_url(&config.connection_url());
        let pool = pool_config
            .builder()
            .map_err(|e| StorageError::InvalidConfiguration {
                message: format!("Failed to create pool builder: {}", e),
            })?
            .max_size(16) // Default pool size
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| StorageError::ConnectionError {
                source: Box::new(e),
            })?;

        // Test connection
        let mut conn = pool.get().await.map_err(|e| StorageError::ConnectionError {
            source: Box::new(e),
        })?;

        // Ping to verify connection
        let _: String = redis::cmd("PING")
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::ConnectionError {
                source: Box::new(e),
            })?;

        info!("Successfully connected to Redis");

        Ok(Self {
            pool,
            config,
            stats: Arc::new(RwLock::new(StorageStats::new())),
            cache_stats: Arc::new(RwLock::new(InternalCacheStats::default())),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            scripts: Arc::new(LuaScripts::new()),
            shutdown: Arc::new(RwLock::new(false)),
        })
    }

    /// Build a namespaced key
    fn build_key(&self, table: &str, key: &str) -> String {
        format!("{}:{}:{}", self.config.key_prefix, table, key)
    }

    /// Build a version key
    fn build_version_key(&self, table: &str, key: &str) -> String {
        format!("{}:{}:{}:version", self.config.key_prefix, table, key)
    }

    /// Build a metadata key
    fn build_metadata_key(&self, table: &str, key: &str) -> String {
        format!("{}:{}:{}:metadata", self.config.key_prefix, table, key)
    }

    /// Build a table pattern for scanning
    fn build_table_pattern(&self, table: &str) -> String {
        format!("{}:{}:*", self.config.key_prefix, table)
    }

    /// Get a connection from the pool
    async fn get_connection(&self) -> StorageResult<Connection> {
        let start = Instant::now();

        let conn = self.pool.get().await.map_err(|e| {
            error!("Failed to get connection from pool: {}", e);
            StorageError::ConnectionError {
                source: Box::new(e),
            }
        })?;

        let elapsed = start.elapsed().as_millis() as f64;
        trace!("Acquired connection from pool in {:.2}ms", elapsed);

        Ok(conn)
    }

    /// Serialize value to JSON
    fn serialize_value<T: Serialize>(&self, value: &T) -> StorageResult<String> {
        serde_json::to_string(value).map_err(|e| StorageError::SerializationError {
            source: Box::new(e),
        })
    }

    /// Deserialize value from JSON
    fn deserialize_value<T: DeserializeOwned>(&self, data: &str) -> StorageResult<T> {
        serde_json::from_str(data).map_err(|e| StorageError::DeserializationError {
            source: Box::new(e),
        })
    }

    /// Get current version for a key
    async fn get_version(&self, conn: &mut Connection, table: &str, key: &str) -> StorageResult<u64> {
        let version_key = self.build_version_key(table, key);
        let version: Option<u64> = conn
            .get(&version_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get version: {}", e),
            })?;

        Ok(version.unwrap_or(0))
    }

    /// Set version for a key
    async fn set_version(&self, conn: &mut Connection, table: &str, key: &str, version: u64) -> StorageResult<()> {
        let version_key = self.build_version_key(table, key);
        conn.set(&version_key, version)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to set version: {}", e),
            })
    }

    /// Increment version for a key
    async fn increment_version(&self, conn: &mut Connection, table: &str, key: &str) -> StorageResult<u64> {
        let version_key = self.build_version_key(table, key);
        let new_version: u64 = conn
            .incr(&version_key, 1)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to increment version: {}", e),
            })?;

        Ok(new_version)
    }

    /// Get metadata for a key
    async fn get_metadata(&self, conn: &mut Connection, table: &str, key: &str) -> StorageResult<HashMap<String, String>> {
        let metadata_key = self.build_metadata_key(table, key);
        let metadata_json: Option<String> = conn
            .get(&metadata_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get metadata: {}", e),
            })?;

        if let Some(json) = metadata_json {
            self.deserialize_value(&json)
        } else {
            Ok(HashMap::new())
        }
    }

    /// Set metadata for a key
    async fn set_metadata(&self, conn: &mut Connection, table: &str, key: &str, metadata: &HashMap<String, String>) -> StorageResult<()> {
        let metadata_key = self.build_metadata_key(table, key);
        let metadata_json = self.serialize_value(metadata)?;

        conn.set(&metadata_key, metadata_json)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to set metadata: {}", e),
            })
    }

    /// Update cache statistics
    async fn update_cache_stats(&self, hit: bool) {
        let mut stats = self.cache_stats.write().await;
        if hit {
            stats.hits += 1;
        } else {
            stats.misses += 1;
        }
    }

    /// Update operation statistics
    async fn update_operation_stats(&self, success: bool, latency_ms: f64, bytes: u64) {
        let mut stats = self.stats.write().await;
        if success {
            stats.record_success(latency_ms, bytes);
        } else {
            stats.record_failure();
        }
    }

    /// Execute Redis command with retries
    async fn execute_with_retry<F, T>(&self, operation: F) -> StorageResult<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, RedisError>> + Send>> + Send,
    {
        let max_retries = 3;
        let mut last_error = None;

        for attempt in 0..max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!("Redis operation failed (attempt {}/{}): {}", attempt + 1, max_retries, e);
                    last_error = Some(e);

                    if attempt < max_retries - 1 {
                        let delay = std::time::Duration::from_millis(100 * (2_u64.pow(attempt as u32)));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(StorageError::QueryError {
            message: format!("Operation failed after {} retries: {}", max_retries, last_error.unwrap()),
        })
    }

    /// Scan keys with pattern
    async fn scan_keys(&self, pattern: &str) -> StorageResult<Vec<String>> {
        let mut conn = self.get_connection().await?;
        let mut keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::QueryError {
                    message: format!("SCAN failed: {}", e),
                })?;

            keys.extend(batch);
            cursor = new_cursor;

            if cursor == 0 {
                break;
            }
        }

        Ok(keys)
    }

    /// Build storage entry from Redis data
    async fn build_entry<T: DeserializeOwned>(
        &self,
        conn: &mut Connection,
        table: &str,
        key: &str,
        value_json: String,
    ) -> StorageResult<StorageEntry<T>> {
        let value: T = self.deserialize_value(&value_json)?;
        let version = self.get_version(conn, table, key).await?;
        let metadata = self.get_metadata(conn, table, key).await?;

        // Get TTL if available
        let redis_key = self.build_key(table, key);
        let ttl: i64 = conn
            .ttl(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get TTL: {}", e),
            })?;

        let expires_at = if ttl > 0 {
            Some(Utc::now() + chrono::Duration::seconds(ttl))
        } else {
            None
        };

        let now = Utc::now();
        Ok(StorageEntry {
            key: key.to_string(),
            value,
            version,
            created_at: now, // We don't track creation time separately in Redis
            updated_at: now,
            expires_at,
            metadata,
        })
    }

    /// Acquire distributed lock
    async fn acquire_lock(&self, lock_key: &str, ttl_seconds: i64) -> StorageResult<String> {
        let mut conn = self.get_connection().await?;
        let lock_value = uuid::Uuid::new_v4().to_string();
        let full_key = format!("{}:lock:{}", self.config.key_prefix, lock_key);

        let result: bool = redis::cmd("SET")
            .arg(&full_key)
            .arg(&lock_value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::LockError {
                message: format!("Failed to acquire lock: {}", e),
            })?;

        if result {
            debug!("Acquired lock: {}", lock_key);
            Ok(lock_value)
        } else {
            Err(StorageError::LockError {
                message: format!("Lock already held: {}", lock_key),
            })
        }
    }

    /// Release distributed lock
    async fn release_lock(&self, lock_key: &str, lock_value: &str) -> StorageResult<()> {
        let mut conn = self.get_connection().await?;
        let full_key = format!("{}:lock:{}", self.config.key_prefix, lock_key);

        let script = Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
            "#,
        );

        let result: i32 = script
            .key(&full_key)
            .arg(lock_value)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::LockError {
                message: format!("Failed to release lock: {}", e),
            })?;

        if result == 1 {
            debug!("Released lock: {}", lock_key);
            Ok(())
        } else {
            warn!("Lock not held or already released: {}", lock_key);
            Err(StorageError::LockError {
                message: format!("Lock not held: {}", lock_key),
            })
        }
    }
}

#[async_trait]
impl Storage for RedisStorage {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Redis
    }

    async fn metadata(&self) -> StorageResult<StorageMetadata> {
        let mut conn = self.get_connection().await?;

        // Get Redis info
        let info: String = redis::cmd("INFO")
            .arg("server")
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get server info: {}", e),
            })?;

        // Parse version from info
        let version = info
            .lines()
            .find(|line| line.starts_with("redis_version:"))
            .and_then(|line| line.split(':').nth(1))
            .unwrap_or("unknown")
            .to_string();

        Ok(StorageMetadata {
            backend: StorageBackend::Redis,
            version,
            capabilities: vec![
                "ttl".to_string(),
                "pubsub".to_string(),
                "atomic".to_string(),
                "pipeline".to_string(),
                "lua".to_string(),
                "cache".to_string(),
            ],
            status: StorageHealth::Healthy,
            last_health_check: Utc::now(),
        })
    }

    async fn health_check(&self) -> StorageResult<StorageHealth> {
        let start = Instant::now();

        match self.get_connection().await {
            Ok(mut conn) => {
                // Try to ping
                match redis::cmd("PING").query_async::<String>(&mut *conn).await {
                    Ok(_) => {
                        let elapsed = start.elapsed().as_millis();
                        if elapsed < 100 {
                            Ok(StorageHealth::Healthy)
                        } else if elapsed < 500 {
                            Ok(StorageHealth::Degraded)
                        } else {
                            Ok(StorageHealth::Unhealthy)
                        }
                    }
                    Err(e) => {
                        error!("Health check ping failed: {}", e);
                        Ok(StorageHealth::Unhealthy)
                    }
                }
            }
            Err(e) => {
                error!("Health check connection failed: {}", e);
                Ok(StorageHealth::Unhealthy)
            }
        }
    }

    async fn stats(&self) -> StorageResult<StorageStats> {
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }

    async fn insert<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let value_json = self.serialize_value(value)?;
        let bytes = value_json.len() as u64;

        let mut conn = self.get_connection().await?;

        // Set value
        let result: Result<(), RedisError> = conn.set(&redis_key, &value_json).await;

        if let Err(e) = result {
            let elapsed = start.elapsed().as_millis() as f64;
            self.update_operation_stats(false, elapsed, 0).await;
            return Err(StorageError::QueryError {
                message: format!("Failed to insert: {}", e),
            });
        }

        // Set initial version
        self.set_version(&mut conn, table, key, 1).await?;

        // Apply default TTL if configured
        if let Some(ttl) = self.config.default_ttl {
            conn.expire(&redis_key, ttl)
                .await
                .map_err(|e| StorageError::QueryError {
                    message: format!("Failed to set TTL: {}", e),
                })?;
        }

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, bytes).await;

        let mut cache_stats = self.cache_stats.write().await;
        cache_stats.sets += 1;

        debug!("Inserted key {} in table {} ({} bytes)", key, table, bytes);
        Ok(())
    }

    async fn insert_with_ttl<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        ttl_seconds: i64,
    ) -> StorageResult<()> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let value_json = self.serialize_value(value)?;
        let bytes = value_json.len() as u64;

        let mut conn = self.get_connection().await?;

        // Set value with TTL using SETEX
        let result: Result<(), RedisError> = redis::cmd("SETEX")
            .arg(&redis_key)
            .arg(ttl_seconds)
            .arg(&value_json)
            .query_async(&mut *conn)
            .await;

        if let Err(e) = result {
            let elapsed = start.elapsed().as_millis() as f64;
            self.update_operation_stats(false, elapsed, 0).await;
            return Err(StorageError::QueryError {
                message: format!("Failed to insert with TTL: {}", e),
            });
        }

        // Set initial version with same TTL
        let version_key = self.build_version_key(table, key);
        redis::cmd("SETEX")
            .arg(&version_key)
            .arg(ttl_seconds)
            .arg(1u64)
            .query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::QueryError {
                message: format!("Failed to set version: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, bytes).await;

        let mut cache_stats = self.cache_stats.write().await;
        cache_stats.sets += 1;

        debug!("Inserted key {} in table {} with TTL {}s ({} bytes)", key, table, ttl_seconds, bytes);
        Ok(())
    }

    async fn get<T: DeserializeOwned>(
        &self,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<StorageEntry<T>>> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);

        let mut conn = self.get_connection().await?;

        let value_json: Option<String> = conn
            .get(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get value: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;

        if let Some(json) = value_json {
            let entry = self.build_entry(&mut conn, table, key, json).await?;
            let bytes = serde_json::to_string(&entry.value)
                .map(|s| s.len())
                .unwrap_or(0) as u64;

            self.update_operation_stats(true, elapsed, bytes).await;
            self.update_cache_stats(true).await;

            trace!("Cache hit for key {} in table {}", key, table);
            Ok(Some(entry))
        } else {
            self.update_operation_stats(true, elapsed, 0).await;
            self.update_cache_stats(false).await;

            trace!("Cache miss for key {} in table {}", key, table);
            Ok(None)
        }
    }

    async fn update<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let value_json = self.serialize_value(value)?;
        let bytes = value_json.len() as u64;

        let mut conn = self.get_connection().await?;

        // Check if key exists
        let exists: bool = conn
            .exists(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to check existence: {}", e),
            })?;

        if !exists {
            return Err(StorageError::NotFound {
                entity: table.to_string(),
                key: key.to_string(),
            });
        }

        // Update value
        conn.set(&redis_key, &value_json)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to update: {}", e),
            })?;

        // Increment version
        self.increment_version(&mut conn, table, key).await?;

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, bytes).await;

        debug!("Updated key {} in table {} ({} bytes)", key, table, bytes);
        Ok(())
    }

    async fn update_versioned<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        expected_version: u64,
    ) -> StorageResult<()> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let value_json = self.serialize_value(value)?;
        let bytes = value_json.len() as u64;

        let mut conn = self.get_connection().await?;

        // Use Lua script for atomic versioned update
        let result: i64 = self
            .scripts
            .versioned_update
            .key(&redis_key)
            .arg(expected_version)
            .arg(&value_json)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Versioned update failed: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;

        if result == 0 {
            self.update_operation_stats(false, elapsed, 0).await;
            return Err(StorageError::ConstraintViolation {
                message: format!("Version mismatch for key {}: expected {}", key, expected_version),
            });
        }

        self.update_operation_stats(true, elapsed, bytes).await;

        debug!("Updated key {} in table {} with version check (new version: {})", key, table, result);
        Ok(())
    }

    async fn delete(&self, table: &str, key: &str) -> StorageResult<()> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let version_key = self.build_version_key(table, key);
        let metadata_key = self.build_metadata_key(table, key);

        let mut conn = self.get_connection().await?;

        // Delete all related keys
        let deleted: u32 = conn
            .del(&[&redis_key, &version_key, &metadata_key])
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to delete: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        let mut cache_stats = self.cache_stats.write().await;
        cache_stats.deletes += 1;

        debug!("Deleted {} keys for {} in table {}", deleted, key, table);
        Ok(())
    }

    async fn exists(&self, table: &str, key: &str) -> StorageResult<bool> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let exists: bool = conn
            .exists(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to check existence: {}", e),
            })?;

        Ok(exists)
    }

    async fn execute_batch(&self, batch: Batch) -> StorageResult<()> {
        if batch.operations.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();

        for operation in &batch.operations {
            match operation {
                Operation::Insert { table, data } => {
                    // Generate a key for the insert (using timestamp + random)
                    let key = format!("{}_{}", Utc::now().timestamp_millis(), uuid::Uuid::new_v4());
                    let redis_key = self.build_key(table, &key);
                    pipe.set(&redis_key, data);
                }
                Operation::Update { table, key, data } => {
                    let redis_key = self.build_key(table, key);
                    pipe.set(&redis_key, data);
                }
                Operation::Delete { table, key } => {
                    let redis_key = self.build_key(table, key);
                    pipe.del(&redis_key);
                }
            }
        }

        pipe.query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::QueryError {
                message: format!("Batch execution failed: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        debug!("Executed batch with {} operations", batch.operations.len());
        Ok(())
    }

    async fn query<T: DeserializeOwned>(&self, query: Query) -> StorageResult<Vec<StorageEntry<T>>> {
        let start = Instant::now();

        // For Redis, we scan keys and filter in memory
        let pattern = format!("{}:{}:*", self.config.key_prefix, query.table);
        let keys = self.scan_keys(&pattern).await?;

        let mut results = Vec::new();
        let mut conn = self.get_connection().await?;

        for full_key in keys {
            // Extract the actual key from the full Redis key
            if let Some(key_part) = full_key.strip_prefix(&format!("{}:{}:", self.config.key_prefix, query.table)) {
                // Skip version and metadata keys
                if key_part.ends_with(":version") || key_part.ends_with(":metadata") {
                    continue;
                }

                // Get the value
                if let Some(entry) = self.get::<T>(&query.table, key_part).await? {
                    results.push(entry);
                }
            }
        }

        // Apply limit and offset
        if let Some(offset) = query.offset {
            results = results.into_iter().skip(offset).collect();
        }

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        debug!("Query returned {} results from table {}", results.len(), query.table);
        Ok(results)
    }

    async fn count(&self, query: Query) -> StorageResult<usize> {
        let pattern = format!("{}:{}:*", self.config.key_prefix, query.table);
        let keys = self.scan_keys(&pattern).await?;

        // Filter out version and metadata keys
        let count = keys
            .iter()
            .filter(|k| !k.ends_with(":version") && !k.ends_with(":metadata"))
            .count();

        Ok(count)
    }

    async fn begin_transaction(&self) -> StorageResult<Transaction> {
        // Redis doesn't support full ACID transactions across multiple operations
        // We simulate it with MULTI/EXEC
        let transaction = Transaction::new();

        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction.id.clone(), transaction.clone());

        debug!("Started transaction {}", transaction.id);
        Ok(transaction)
    }

    async fn commit_transaction(&self, mut transaction: Transaction) -> StorageResult<()> {
        let start = Instant::now();

        // Execute all operations in the transaction using MULTI/EXEC
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        for operation in &transaction.operations {
            match operation {
                Operation::Insert { table, data } => {
                    let key = format!("{}_{}", Utc::now().timestamp_millis(), uuid::Uuid::new_v4());
                    let redis_key = self.build_key(table, &key);
                    pipe.set(&redis_key, data);
                }
                Operation::Update { table, key, data } => {
                    let redis_key = self.build_key(table, key);
                    pipe.set(&redis_key, data);
                }
                Operation::Delete { table, key } => {
                    let redis_key = self.build_key(table, key);
                    pipe.del(&redis_key);
                }
            }
        }

        pipe.query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::TransactionError {
                message: format!("Transaction commit failed: {}", e),
            })?;

        transaction.state = TransactionState::Committed;

        let mut transactions = self.transactions.write().await;
        transactions.remove(&transaction.id);

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        debug!("Committed transaction {} with {} operations", transaction.id, transaction.operations.len());
        Ok(())
    }

    async fn rollback_transaction(&self, mut transaction: Transaction) -> StorageResult<()> {
        transaction.state = TransactionState::RolledBack;

        let mut transactions = self.transactions.write().await;
        transactions.remove(&transaction.id);

        debug!("Rolled back transaction {}", transaction.id);
        Ok(())
    }

    async fn scan_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<String>> {
        let pattern = format!("{}:{}:{}*", self.config.key_prefix, table, prefix);
        let keys = self.scan_keys(&pattern).await?;

        // Extract just the key part
        let extracted_keys: Vec<String> = keys
            .into_iter()
            .filter_map(|full_key| {
                full_key
                    .strip_prefix(&format!("{}:{}:", self.config.key_prefix, table))
                    .map(|s| s.to_string())
            })
            .filter(|k| !k.ends_with(":version") && !k.ends_with(":metadata"))
            .collect();

        debug!("Scan prefix {} in table {} returned {} keys", prefix, table, extracted_keys.len());
        Ok(extracted_keys)
    }

    async fn get_multi<T: DeserializeOwned>(
        &self,
        table: &str,
        keys: Vec<String>,
    ) -> StorageResult<Vec<Option<StorageEntry<T>>>> {
        let start = Instant::now();

        let mut results = Vec::new();
        for key in keys {
            let entry = self.get(table, &key).await?;
            results.push(entry);
        }

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        Ok(results)
    }

    async fn delete_multi(&self, table: &str, keys: Vec<String>) -> StorageResult<()> {
        let start = Instant::now();

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();

        for key in &keys {
            let redis_key = self.build_key(table, key);
            let version_key = self.build_version_key(table, key);
            let metadata_key = self.build_metadata_key(table, key);

            pipe.del(&redis_key);
            pipe.del(&version_key);
            pipe.del(&metadata_key);
        }

        pipe.query_async(&mut *conn)
            .await
            .map_err(|e: redis::RedisError| StorageError::QueryError {
                message: format!("Multi-delete failed: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;
        self.update_operation_stats(true, elapsed, 0).await;

        debug!("Deleted {} keys from table {}", keys.len(), table);
        Ok(())
    }

    async fn compare_and_swap<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        expected_version: u64,
        new_value: &T,
    ) -> StorageResult<bool> {
        let start = Instant::now();
        let redis_key = self.build_key(table, key);
        let value_json = self.serialize_value(new_value)?;
        let new_version = expected_version + 1;

        let mut conn = self.get_connection().await?;

        let result: i32 = self
            .scripts
            .cas
            .key(&redis_key)
            .arg(expected_version)
            .arg(&value_json)
            .arg(new_version)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("CAS operation failed: {}", e),
            })?;

        let elapsed = start.elapsed().as_millis() as f64;
        let success = result == 1;
        self.update_operation_stats(success, elapsed, if success { value_json.len() as u64 } else { 0 }).await;

        if success {
            debug!("CAS succeeded for key {} in table {}", key, table);
        } else {
            debug!("CAS failed for key {} in table {} (version mismatch)", key, table);
        }

        Ok(success)
    }

    async fn get_modified_since<T: DeserializeOwned>(
        &self,
        table: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        // Redis doesn't track modification times natively
        // We would need to store this in metadata
        warn!("get_modified_since is not efficiently supported in Redis");

        // Return all entries and let caller filter
        let query = Query::new(table);
        self.query(query).await
    }

    async fn get_expiring_before<T: DeserializeOwned>(
        &self,
        table: &str,
        before: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        let pattern = format!("{}:{}:*", self.config.key_prefix, table);
        let keys = self.scan_keys(&pattern).await?;

        let mut results = Vec::new();
        let mut conn = self.get_connection().await?;
        let before_timestamp = before.timestamp();

        for full_key in keys {
            if full_key.ends_with(":version") || full_key.ends_with(":metadata") {
                continue;
            }

            let ttl: i64 = conn
                .ttl(&full_key)
                .await
                .map_err(|e| StorageError::QueryError {
                    message: format!("Failed to get TTL: {}", e),
                })?;

            if ttl > 0 {
                let expiration_timestamp = Utc::now().timestamp() + ttl;
                if expiration_timestamp < before_timestamp {
                    if let Some(key_part) = full_key.strip_prefix(&format!("{}:{}:", self.config.key_prefix, table)) {
                        if let Some(entry) = self.get::<T>(table, key_part).await? {
                            results.push(entry);
                        }
                    }
                }
            }
        }

        debug!("Found {} entries expiring before {} in table {}", results.len(), before, table);
        Ok(results)
    }

    async fn cleanup_expired(&self, table: &str) -> StorageResult<usize> {
        // Redis automatically handles expiration, so this is a no-op
        // We just return the count of how many keys would expire soon
        let before = Utc::now() + chrono::Duration::minutes(5);
        let expiring: Vec<StorageEntry<serde_json::Value>> = self.get_expiring_before(table, before).await?;

        debug!("Found {} entries that will expire soon in table {}", expiring.len(), table);
        Ok(expiring.len())
    }

    async fn compact(&self) -> StorageResult<()> {
        // Redis doesn't need compaction, but we can run memory optimization
        let mut conn = self.get_connection().await?;

        // Run MEMORY PURGE if available (Redis 4.0+)
        let _: Result<String, RedisError> = redis::cmd("MEMORY")
            .arg("PURGE")
            .query_async(&mut *conn)
            .await;

        info!("Ran memory purge on Redis");
        Ok(())
    }

    async fn flush(&self) -> StorageResult<()> {
        // Redis writes are already persisted based on configuration
        // We can optionally call BGSAVE for a background save
        let mut conn = self.get_connection().await?;

        let _: Result<String, RedisError> = redis::cmd("BGSAVE")
            .query_async(&mut *conn)
            .await;

        info!("Initiated background save");
        Ok(())
    }

    async fn initialize(&mut self) -> StorageResult<()> {
        info!("Initializing Redis storage");

        // Verify connection
        let health = self.health_check().await?;
        if health != StorageHealth::Healthy {
            return Err(StorageError::InvalidState {
                message: format!("Redis is not healthy: {:?}", health),
            });
        }

        // Initialize statistics
        let mut conn = self.get_connection().await?;
        let stats_key = format!("{}:stats", self.config.key_prefix);

        // Check if stats exist, if not create them
        let exists: bool = conn.exists(&stats_key).await.unwrap_or(false);
        if !exists {
            let initial_stats = InternalCacheStats::default();
            let stats_json = serde_json::to_string(&initial_stats).unwrap();
            let _: Result<(), RedisError> = conn.set(&stats_key, stats_json).await;
        }

        info!("Redis storage initialized successfully");
        Ok(())
    }

    async fn shutdown(&mut self) -> StorageResult<()> {
        info!("Shutting down Redis storage");

        // Mark as shutdown
        let mut shutdown = self.shutdown.write().await;
        *shutdown = true;

        // Close all subscriptions
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.clear();

        // Save final statistics
        let stats = self.cache_stats.read().await;
        let stats_json = serde_json::to_string(&*stats).unwrap();

        if let Ok(mut conn) = self.get_connection().await {
            let stats_key = format!("{}:stats", self.config.key_prefix);
            let _: Result<(), RedisError> = conn.set(&stats_key, stats_json).await;
        }

        info!("Redis storage shutdown complete");
        Ok(())
    }
}

#[async_trait]
impl CacheStorage for RedisStorage {
    async fn ttl(&self, table: &str, key: &str) -> StorageResult<Option<i64>> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let ttl: i64 = conn
            .ttl(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get TTL: {}", e),
            })?;

        // TTL returns -2 if key doesn't exist, -1 if no expiration
        if ttl == -2 {
            Ok(None)
        } else if ttl == -1 {
            Ok(Some(-1))
        } else {
            Ok(Some(ttl))
        }
    }

    async fn expire(&self, table: &str, key: &str, ttl_seconds: i64) -> StorageResult<()> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let result: bool = conn
            .expire(&redis_key, ttl_seconds)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to set expiration: {}", e),
            })?;

        if !result {
            return Err(StorageError::NotFound {
                entity: table.to_string(),
                key: key.to_string(),
            });
        }

        debug!("Set TTL of {}s for key {} in table {}", ttl_seconds, key, table);
        Ok(())
    }

    async fn persist(&self, table: &str, key: &str) -> StorageResult<()> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let result: bool = conn
            .persist(&redis_key)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to persist: {}", e),
            })?;

        if !result {
            return Err(StorageError::NotFound {
                entity: table.to_string(),
                key: key.to_string(),
            });
        }

        debug!("Removed expiration from key {} in table {}", key, table);
        Ok(())
    }

    async fn increment(&self, table: &str, key: &str, delta: i64) -> StorageResult<i64> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let new_value: i64 = conn
            .incr(&redis_key, delta)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to increment: {}", e),
            })?;

        debug!("Incremented key {} in table {} by {} (new value: {})", key, table, delta, new_value);
        Ok(new_value)
    }

    async fn decrement(&self, table: &str, key: &str, delta: i64) -> StorageResult<i64> {
        let redis_key = self.build_key(table, key);
        let mut conn = self.get_connection().await?;

        let new_value: i64 = conn
            .decr(&redis_key, delta)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to decrement: {}", e),
            })?;

        debug!("Decremented key {} in table {} by {} (new value: {})", key, table, delta, new_value);
        Ok(new_value)
    }

    async fn cache_stats(&self) -> StorageResult<CacheStats> {
        let mut conn = self.get_connection().await?;

        // Get Redis memory info
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to get memory info: {}", e),
            })?;

        // Parse memory stats
        let mut memory_used = 0u64;
        let mut memory_available = 0u64;

        for line in info.lines() {
            if let Some(value) = line.strip_prefix("used_memory:") {
                memory_used = value.trim().parse().unwrap_or(0);
            } else if let Some(value) = line.strip_prefix("maxmemory:") {
                memory_available = value.trim().parse().unwrap_or(0);
            }
        }

        // Get key count
        let keys_count: usize = redis::cmd("DBSIZE")
            .query_async(&mut *conn)
            .await
            .unwrap_or(0);

        // Get our tracked stats
        let cache_stats = self.cache_stats.read().await;

        Ok(CacheStats {
            hits: cache_stats.hits,
            misses: cache_stats.misses,
            evictions: cache_stats.evictions,
            memory_used_bytes: memory_used,
            memory_available_bytes: memory_available,
            keys_count,
        })
    }
}

#[async_trait]
impl PubSubStorage for RedisStorage {
    async fn publish(&self, channel: &str, message: &[u8]) -> StorageResult<usize> {
        let mut conn = self.get_connection().await?;
        let full_channel = format!("{}:pubsub:{}", self.config.key_prefix, channel);

        let subscribers: usize = conn
            .publish(&full_channel, message)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to publish message: {}", e),
            })?;

        debug!("Published message to channel {} ({} subscribers, {} bytes)",
            channel, subscribers, message.len());

        Ok(subscribers)
    }

    async fn subscribe(&self, channel: &str) -> StorageResult<Box<dyn Subscriber>> {
        let conn = self.get_connection().await?;
        let full_channel = format!("{}:pubsub:{}", self.config.key_prefix, channel);

        let mut subscriber = RedisSubscriber::new(conn, full_channel.clone()).await?;

        // Subscribe to channel
        subscriber.pubsub
            .subscribe(&full_channel)
            .await
            .map_err(|e| StorageError::QueryError {
                message: format!("Failed to subscribe to channel: {}", e),
            })?;

        info!("Subscribed to channel {}", channel);

        let subscriber_arc = Arc::new(Mutex::new(subscriber));

        // Store subscription
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(channel.to_string(), subscriber_arc.clone());

        Ok(Box::new(SubscriberWrapper {
            inner: subscriber_arc,
        }))
    }

    async fn unsubscribe(&self, channel: &str) -> StorageResult<()> {
        let mut subscriptions = self.subscriptions.write().await;

        if let Some(subscriber_arc) = subscriptions.remove(channel) {
            let mut subscriber = subscriber_arc.lock().await;
            let full_channel = format!("{}:pubsub:{}", self.config.key_prefix, channel);

            subscriber.pubsub
                .unsubscribe(&full_channel)
                .await
                .map_err(|e| StorageError::QueryError {
                    message: format!("Failed to unsubscribe from channel: {}", e),
                })?;

            info!("Unsubscribed from channel {}", channel);
        }

        Ok(())
    }
}

/// Wrapper for subscriber to implement Send
struct SubscriberWrapper {
    inner: Arc<Mutex<RedisSubscriber>>,
}

#[async_trait]
impl Subscriber for SubscriberWrapper {
    async fn next_message(&mut self) -> StorageResult<Option<Vec<u8>>> {
        let mut subscriber = self.inner.lock().await;
        subscriber.next_message().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: String,
        value: i32,
        name: String,
    }

    async fn create_test_storage() -> RedisStorage {
        let config = RedisConfig {
            host: "localhost".to_string(),
            port: 6379,
            database: 15, // Use different DB for tests
            key_prefix: "test:".to_string(),
            ..Default::default()
        };

        let mut storage = RedisStorage::new(config).await.expect("Failed to create storage");
        storage.initialize().await.expect("Failed to initialize");
        storage
    }

    async fn cleanup_test_data(storage: &RedisStorage) {
        let mut conn = storage.get_connection().await.unwrap();
        let _: Result<(), RedisError> = redis::cmd("FLUSHDB").query_async(&mut *conn).await;
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test1".to_string(),
            value: 42,
            name: "Test Data".to_string(),
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let result: Option<StorageEntry<TestData>> = storage.get("test_table", "key1").await.unwrap();
        assert!(result.is_some());

        let entry = result.unwrap();
        assert_eq!(entry.value, data);
        assert_eq!(entry.version, 1);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_insert_with_ttl() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test2".to_string(),
            value: 100,
            name: "TTL Test".to_string(),
        };

        storage.insert_with_ttl("test_table", "key2", &data, 10).await.unwrap();

        let result: Option<StorageEntry<TestData>> = storage.get("test_table", "key2").await.unwrap();
        assert!(result.is_some());

        let ttl = storage.ttl("test_table", "key2").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap() > 0 && ttl.unwrap() <= 10);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_update() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test3".to_string(),
            value: 50,
            name: "Original".to_string(),
        };

        storage.insert("test_table", "key3", &data).await.unwrap();

        let updated_data = TestData {
            id: "test3".to_string(),
            value: 75,
            name: "Updated".to_string(),
        };

        storage.update("test_table", "key3", &updated_data).await.unwrap();

        let result: Option<StorageEntry<TestData>> = storage.get("test_table", "key3").await.unwrap();
        assert!(result.is_some());

        let entry = result.unwrap();
        assert_eq!(entry.value.name, "Updated");
        assert_eq!(entry.version, 2);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_versioned_update() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test4".to_string(),
            value: 10,
            name: "Version Test".to_string(),
        };

        storage.insert("test_table", "key4", &data).await.unwrap();

        let updated_data = TestData {
            id: "test4".to_string(),
            value: 20,
            name: "Version Updated".to_string(),
        };

        // Should succeed with correct version
        storage.update_versioned("test_table", "key4", &updated_data, 1).await.unwrap();

        // Should fail with wrong version
        let result = storage.update_versioned("test_table", "key4", &updated_data, 1).await;
        assert!(result.is_err());

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test5".to_string(),
            value: 99,
            name: "To Delete".to_string(),
        };

        storage.insert("test_table", "key5", &data).await.unwrap();

        let exists = storage.exists("test_table", "key5").await.unwrap();
        assert!(exists);

        storage.delete("test_table", "key5").await.unwrap();

        let exists = storage.exists("test_table", "key5").await.unwrap();
        assert!(!exists);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_compare_and_swap() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test6".to_string(),
            value: 30,
            name: "CAS Test".to_string(),
        };

        storage.insert("test_table", "key6", &data).await.unwrap();

        let new_data = TestData {
            id: "test6".to_string(),
            value: 40,
            name: "CAS Updated".to_string(),
        };

        // Should succeed with correct version
        let success = storage.compare_and_swap("test_table", "key6", 1, &new_data).await.unwrap();
        assert!(success);

        // Should fail with wrong version
        let success = storage.compare_and_swap("test_table", "key6", 1, &new_data).await.unwrap();
        assert!(!success);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_increment_decrement() {
        let storage = create_test_storage().await;

        let value = storage.increment("counters", "counter1", 5).await.unwrap();
        assert_eq!(value, 5);

        let value = storage.increment("counters", "counter1", 3).await.unwrap();
        assert_eq!(value, 8);

        let value = storage.decrement("counters", "counter1", 2).await.unwrap();
        assert_eq!(value, 6);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_scan_prefix() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test".to_string(),
            value: 1,
            name: "Test".to_string(),
        };

        storage.insert("test_table", "user:1", &data).await.unwrap();
        storage.insert("test_table", "user:2", &data).await.unwrap();
        storage.insert("test_table", "user:3", &data).await.unwrap();
        storage.insert("test_table", "admin:1", &data).await.unwrap();

        let keys = storage.scan_prefix("test_table", "user:").await.unwrap();
        assert_eq!(keys.len(), 3);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_get_multi() {
        let storage = create_test_storage().await;

        let data1 = TestData {
            id: "1".to_string(),
            value: 1,
            name: "One".to_string(),
        };
        let data2 = TestData {
            id: "2".to_string(),
            value: 2,
            name: "Two".to_string(),
        };

        storage.insert("test_table", "k1", &data1).await.unwrap();
        storage.insert("test_table", "k2", &data2).await.unwrap();

        let results: Vec<Option<StorageEntry<TestData>>> = storage
            .get_multi("test_table", vec!["k1".to_string(), "k2".to_string(), "k3".to_string()])
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none());

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_delete_multi() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test".to_string(),
            value: 1,
            name: "Test".to_string(),
        };

        storage.insert("test_table", "k1", &data).await.unwrap();
        storage.insert("test_table", "k2", &data).await.unwrap();
        storage.insert("test_table", "k3", &data).await.unwrap();

        storage.delete_multi("test_table", vec!["k1".to_string(), "k2".to_string()]).await.unwrap();

        let exists1 = storage.exists("test_table", "k1").await.unwrap();
        let exists2 = storage.exists("test_table", "k2").await.unwrap();
        let exists3 = storage.exists("test_table", "k3").await.unwrap();

        assert!(!exists1);
        assert!(!exists2);
        assert!(exists3);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_transaction() {
        let storage = create_test_storage().await;

        let mut transaction = storage.begin_transaction().await.unwrap();

        let data = TestData {
            id: "test".to_string(),
            value: 1,
            name: "Test".to_string(),
        };
        let data_bytes = serde_json::to_vec(&data).unwrap();

        transaction.add_operation(Operation::Insert {
            table: "test_table".to_string(),
            data: data_bytes.clone(),
        });
        transaction.add_operation(Operation::Insert {
            table: "test_table".to_string(),
            data: data_bytes,
        });

        storage.commit_transaction(transaction).await.unwrap();

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let storage = create_test_storage().await;

        let data = TestData {
            id: "test".to_string(),
            value: 1,
            name: "Test".to_string(),
        };

        storage.insert("test_table", "key1", &data).await.unwrap();

        let _: Option<StorageEntry<TestData>> = storage.get("test_table", "key1").await.unwrap();
        let _: Option<StorageEntry<TestData>> = storage.get("test_table", "missing").await.unwrap();

        let stats = storage.cache_stats().await.unwrap();
        assert!(stats.hits > 0);
        assert!(stats.misses > 0);
        assert!(stats.hit_rate() > 0.0 && stats.hit_rate() <= 1.0);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_health_check() {
        let storage = create_test_storage().await;

        let health = storage.health_check().await.unwrap();
        assert_eq!(health, StorageHealth::Healthy);

        cleanup_test_data(&storage).await;
    }

    #[tokio::test]
    async fn test_metadata() {
        let storage = create_test_storage().await;

        let metadata = storage.metadata().await.unwrap();
        assert_eq!(metadata.backend, StorageBackend::Redis);
        assert!(metadata.capabilities.contains(&"ttl".to_string()));
        assert!(metadata.capabilities.contains(&"pubsub".to_string()));

        cleanup_test_data(&storage).await;
    }
}
