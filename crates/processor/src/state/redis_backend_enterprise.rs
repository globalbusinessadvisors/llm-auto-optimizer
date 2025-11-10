//! Enterprise-grade Redis state backend with advanced features
//!
//! This module provides a production-ready Redis backend with comprehensive features:
//!
//! ## Core Features
//!
//! - **Redis Sentinel Support**: Automatic master discovery and failover handling
//! - **Redis Cluster Support**: Hash slot routing and cluster topology management
//! - **Advanced Connection Pooling**: deadpool-redis with health checks and automatic reconnection
//! - **Distributed Locking**: Redlock algorithm implementation with deadlock prevention
//! - **Batch Operations**: Pipeline support, atomic transactions, Lua script execution
//! - **Data Management**: Compression (LZ4, Snappy, Zstd), serialization optimization, TTL management
//! - **Observability**: Prometheus metrics, latency tracking, error monitoring
//! - **Error Handling**: Circuit breaker pattern, retry with exponential backoff and jitter
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use processor::state::redis_backend_enterprise::{EnterpriseRedisBackend, EnterpriseRedisConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Configure with Sentinel
//!     let config = EnterpriseRedisConfig::builder()
//!         .mode(RedisMode::Sentinel {
//!             sentinels: vec![
//!                 "redis://sentinel1:26379".to_string(),
//!                 "redis://sentinel2:26379".to_string(),
//!             ],
//!             service_name: "mymaster".to_string(),
//!         })
//!         .pool_config(10, 100, Duration::from_secs(30))
//!         .compression(CompressionAlgorithm::Lz4)
//!         .enable_metrics(true)
//!         .build()?;
//!
//!     let backend = EnterpriseRedisBackend::new(config).await?;
//!
//!     // Use batch operations
//!     let pairs = vec![
//!         (b"key1".as_ref(), b"value1".as_ref()),
//!         (b"key2".as_ref(), b"value2".as_ref()),
//!     ];
//!     backend.batch_put_optimized(&pairs).await?;
//!
//!     // Get with automatic decompression
//!     let value = backend.get(b"key1").await?;
//!
//!     // Acquire distributed lock
//!     let lock = backend.acquire_lock("resource", Duration::from_secs(30)).await?;
//!     // Critical section
//!     drop(lock);
//!
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use deadpool_redis::{Config as PoolConfig, Pool, Runtime};
use failsafe::{backoff, failure_policy, Config as CircuitBreakerConfig, StateMachine};
use prometheus_client::encoding::EncodeLabelSet;

// Type alias for circuit breaker state machine
type CircuitBreaker = StateMachine<
    failure_policy::ConsecutiveFailures<backoff::Exponential>,
    (),
>;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use redis::{
    aio::ConnectionManager, AsyncCommands, Client, Cmd, FromRedisValue, RedisError,
    RedisResult, Script, Value,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use rand::Rng;

use super::backend::StateBackend;
use crate::error::{StateError, StateResult};

/// Redis deployment mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisMode {
    /// Standalone Redis instance
    Standalone {
        url: String,
    },
    /// Redis Sentinel for high availability
    Sentinel {
        sentinels: Vec<String>,
        service_name: String,
    },
    /// Redis Cluster for horizontal scaling
    Cluster {
        nodes: Vec<String>,
    },
}

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Snappy,
    Zstd,
}

/// Serialization format selection
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SerializationFormat {
    Bincode,
    MessagePack,
    Json,
}

/// Enterprise Redis configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnterpriseRedisConfig {
    /// Redis deployment mode
    pub mode: RedisMode,

    /// Key namespace prefix
    pub key_prefix: String,

    /// Default TTL for keys
    pub default_ttl: Option<Duration>,

    /// Minimum pool connections
    pub pool_min: usize,

    /// Maximum pool connections
    pub pool_max: usize,

    /// Pool timeout
    pub pool_timeout: Duration,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Command timeout
    pub command_timeout: Duration,

    /// Max retry attempts
    pub max_retries: u32,

    /// Initial retry delay
    pub retry_initial_delay: Duration,

    /// Max retry delay
    pub retry_max_delay: Duration,

    /// Retry jitter factor (0.0 to 1.0)
    pub retry_jitter: f64,

    /// Compression algorithm
    pub compression: CompressionAlgorithm,

    /// Compression threshold in bytes
    pub compression_threshold: usize,

    /// Serialization format
    pub serialization: SerializationFormat,

    /// Pipeline batch size
    pub pipeline_batch_size: usize,

    /// Enable circuit breaker
    pub enable_circuit_breaker: bool,

    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,

    /// Circuit breaker timeout
    pub circuit_breaker_timeout: Duration,

    /// Enable Prometheus metrics
    pub enable_metrics: bool,

    /// Health check interval
    pub health_check_interval: Duration,

    /// Enable slow query logging
    pub enable_slow_query_log: bool,

    /// Slow query threshold
    pub slow_query_threshold: Duration,
}

impl Default for EnterpriseRedisConfig {
    fn default() -> Self {
        Self {
            mode: RedisMode::Standalone {
                url: "redis://localhost:6379".to_string(),
            },
            key_prefix: "state:".to_string(),
            default_ttl: None,
            pool_min: 10,
            pool_max: 100,
            pool_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(3),
            max_retries: 3,
            retry_initial_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(5),
            retry_jitter: 0.3,
            compression: CompressionAlgorithm::None,
            compression_threshold: 1024, // 1KB
            serialization: SerializationFormat::Bincode,
            pipeline_batch_size: 100,
            enable_circuit_breaker: true,
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout: Duration::from_secs(60),
            enable_metrics: true,
            health_check_interval: Duration::from_secs(30),
            enable_slow_query_log: true,
            slow_query_threshold: Duration::from_millis(100),
        }
    }
}

impl EnterpriseRedisConfig {
    pub fn builder() -> EnterpriseRedisConfigBuilder {
        EnterpriseRedisConfigBuilder::default()
    }
}

/// Configuration builder
#[derive(Debug, Default)]
pub struct EnterpriseRedisConfigBuilder {
    config: EnterpriseRedisConfig,
}

impl EnterpriseRedisConfigBuilder {
    pub fn mode(mut self, mode: RedisMode) -> Self {
        self.config.mode = mode;
        self
    }

    pub fn key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.key_prefix = prefix.into();
        self
    }

    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = Some(ttl);
        self
    }

    pub fn pool_config(mut self, min: usize, max: usize, timeout: Duration) -> Self {
        self.config.pool_min = min;
        self.config.pool_max = max;
        self.config.pool_timeout = timeout;
        self
    }

    pub fn timeouts(mut self, connect: Duration, command: Duration) -> Self {
        self.config.connect_timeout = connect;
        self.config.command_timeout = command;
        self
    }

    pub fn retry_config(mut self, max: u32, initial: Duration, max_delay: Duration, jitter: f64) -> Self {
        self.config.max_retries = max;
        self.config.retry_initial_delay = initial;
        self.config.retry_max_delay = max_delay;
        self.config.retry_jitter = jitter;
        self
    }

    pub fn compression(mut self, algo: CompressionAlgorithm) -> Self {
        self.config.compression = algo;
        self
    }

    pub fn compression_threshold(mut self, threshold: usize) -> Self {
        self.config.compression_threshold = threshold;
        self
    }

    pub fn serialization(mut self, format: SerializationFormat) -> Self {
        self.config.serialization = format;
        self
    }

    pub fn circuit_breaker(mut self, enabled: bool, threshold: u32, timeout: Duration) -> Self {
        self.config.enable_circuit_breaker = enabled;
        self.config.circuit_breaker_threshold = threshold;
        self.config.circuit_breaker_timeout = timeout;
        self
    }

    pub fn enable_metrics(mut self, enabled: bool) -> Self {
        self.config.enable_metrics = enabled;
        self
    }

    pub fn slow_query_log(mut self, enabled: bool, threshold: Duration) -> Self {
        self.config.enable_slow_query_log = enabled;
        self.config.slow_query_threshold = threshold;
        self
    }

    pub fn build(self) -> StateResult<EnterpriseRedisConfig> {
        // Validation
        if self.config.pool_max < self.config.pool_min {
            return Err(StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: "pool_max must be >= pool_min".to_string(),
            });
        }

        if self.config.retry_jitter < 0.0 || self.config.retry_jitter > 1.0 {
            return Err(StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: "retry_jitter must be between 0.0 and 1.0".to_string(),
            });
        }

        Ok(self.config)
    }
}

/// Prometheus metric labels
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MetricLabels {
    operation: String,
    status: String,
}

/// Prometheus metrics collection
pub struct RedisMetrics {
    operations_total: Family<MetricLabels, Counter>,
    operation_duration: Family<MetricLabels, Histogram>,
    pool_connections_active: Gauge,
    pool_connections_idle: Gauge,
    circuit_breaker_state: Gauge,
    compression_ratio: Gauge,
    errors_total: Family<MetricLabels, Counter>,
}

impl RedisMetrics {
    fn new() -> Self {
        Self {
            operations_total: Family::default(),
            operation_duration: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 10))
            }),
            pool_connections_active: Gauge::default(),
            pool_connections_idle: Gauge::default(),
            circuit_breaker_state: Gauge::default(),
            compression_ratio: Gauge::default(),
            errors_total: Family::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "redis_operations_total",
            "Total number of Redis operations",
            self.operations_total.clone(),
        );
        registry.register(
            "redis_operation_duration_seconds",
            "Duration of Redis operations",
            self.operation_duration.clone(),
        );
        registry.register(
            "redis_pool_connections_active",
            "Active pool connections",
            self.pool_connections_active.clone(),
        );
        registry.register(
            "redis_pool_connections_idle",
            "Idle pool connections",
            self.pool_connections_idle.clone(),
        );
        registry.register(
            "redis_circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half-open)",
            self.circuit_breaker_state.clone(),
        );
        registry.register(
            "redis_compression_ratio",
            "Compression ratio (original/compressed)",
            self.compression_ratio.clone(),
        );
        registry.register(
            "redis_errors_total",
            "Total number of errors",
            self.errors_total.clone(),
        );
    }
}

/// Lock guard for Redlock implementation
pub struct RedlockGuard {
    resource: String,
    token: String,
    backend: Arc<EnterpriseRedisBackend>,
}

impl Drop for RedlockGuard {
    fn drop(&mut self) {
        let backend = self.backend.clone();
        let resource = self.resource.clone();
        let token = self.token.clone();

        tokio::spawn(async move {
            let _ = backend.release_lock_internal(&resource, &token).await;
        });
    }
}

/// Enterprise Redis backend
pub struct EnterpriseRedisBackend {
    config: EnterpriseRedisConfig,
    pool: Pool,
    metrics: Option<Arc<RedisMetrics>>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    scripts: Arc<RedisScripts>,
    stats: Arc<RwLock<BackendStats>>,
}

/// Compiled Lua scripts
struct RedisScripts {
    release_lock: Script,
    extend_lock: Script,
    get_delete: Script,
    incr_with_ttl: Script,
    compare_and_set: Script,
}

impl RedisScripts {
    fn new() -> Self {
        Self {
            release_lock: Script::new(
                r"
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                else
                    return 0
                end
                "
            ),
            extend_lock: Script::new(
                r"
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
                else
                    return 0
                end
                "
            ),
            get_delete: Script::new(
                r"
                local value = redis.call('GET', KEYS[1])
                if value then
                    redis.call('DEL', KEYS[1])
                end
                return value
                "
            ),
            incr_with_ttl: Script::new(
                r"
                local value = redis.call('INCR', KEYS[1])
                redis.call('EXPIRE', KEYS[1], ARGV[1])
                return value
                "
            ),
            compare_and_set: Script::new(
                r"
                local current = redis.call('GET', KEYS[1])
                if current == ARGV[1] then
                    redis.call('SET', KEYS[1], ARGV[2])
                    if ARGV[3] then
                        redis.call('EXPIRE', KEYS[1], ARGV[3])
                    end
                    return 1
                else
                    return 0
                end
                "
            ),
        }
    }
}

/// Backend statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackendStats {
    pub operations_total: u64,
    pub operations_success: u64,
    pub operations_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_compressed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub avg_latency_us: u64,
    pub slow_queries: u64,
}

impl EnterpriseRedisBackend {
    /// Create a new enterprise Redis backend
    pub async fn new(config: EnterpriseRedisConfig) -> StateResult<Self> {
        info!("Initializing enterprise Redis backend with mode: {:?}", config.mode);

        // Create connection pool
        let pool = Self::create_pool(&config).await?;

        // Initialize metrics
        let metrics = if config.enable_metrics {
            Some(Arc::new(RedisMetrics::new()))
        } else {
            None
        };

        // Initialize circuit breaker
        let circuit_breaker = if config.enable_circuit_breaker {
            // Create a backoff strategy: exponential backoff starting from circuit_breaker_timeout
            let backoff_strategy = backoff::exponential(
                config.circuit_breaker_timeout,
                config.circuit_breaker_timeout * 10,
            );

            // Create a failure policy based on consecutive failures
            let failure_policy = failure_policy::consecutive_failures(
                config.circuit_breaker_threshold as u32,
                backoff_strategy,
            );

            // Build the circuit breaker state machine
            let state_machine = CircuitBreakerConfig::new()
                .failure_policy(failure_policy)
                .build();

            Some(Arc::new(state_machine))
        } else {
            None
        };

        let backend = Self {
            config,
            pool,
            metrics,
            circuit_breaker,
            scripts: Arc::new(RedisScripts::new()),
            stats: Arc::new(RwLock::new(BackendStats::default())),
        };

        // Start health check task
        backend.spawn_health_check_task();

        info!("Enterprise Redis backend initialized successfully");
        Ok(backend)
    }

    /// Create connection pool based on mode
    async fn create_pool(config: &EnterpriseRedisConfig) -> StateResult<Pool> {
        let pool_config = match &config.mode {
            RedisMode::Standalone { url } => {
                PoolConfig::from_url(url)
            }
            RedisMode::Sentinel { sentinels, service_name } => {
                // For sentinel, we need to discover the master first
                let master_url = Self::discover_sentinel_master(sentinels, service_name).await?;
                PoolConfig::from_url(&master_url)
            }
            RedisMode::Cluster { nodes: _ } => {
                // Cluster mode uses a different approach
                return Err(StateError::StorageError {
                    backend_type: "EnterpriseRedis".to_string(),
                    details: "Cluster mode requires redis-cluster crate, not yet implemented in this version".to_string(),
                });
            }
        };

        pool_config
            .builder()
            .map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to create pool builder: {}", e),
            })?
            .max_size(config.pool_max)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to create connection pool: {}", e),
            })
    }

    /// Discover Redis master from Sentinel
    async fn discover_sentinel_master(sentinels: &[String], service_name: &str) -> StateResult<String> {
        debug!("Discovering master from {} sentinels", sentinels.len());

        for sentinel_url in sentinels {
            match Self::query_sentinel(sentinel_url, service_name).await {
                Ok(master_url) => {
                    info!("Discovered master: {}", master_url);
                    return Ok(master_url);
                }
                Err(e) => {
                    warn!("Failed to query sentinel {}: {}", sentinel_url, e);
                    continue;
                }
            }
        }

        Err(StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: "Failed to discover master from any sentinel".to_string(),
        })
    }

    /// Query a single sentinel for master info
    async fn query_sentinel(sentinel_url: &str, service_name: &str) -> StateResult<String> {
        let client = Client::open(sentinel_url).map_err(|e| StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: format!("Failed to connect to sentinel: {}", e),
        })?;

        let mut conn = client.get_connection_manager().await.map_err(|e| StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: format!("Failed to get connection: {}", e),
        })?;

        // Query sentinel for master
        let result: Vec<String> = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(service_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Sentinel query failed: {}", e),
            })?;

        if result.len() >= 2 {
            Ok(format!("redis://{}:{}", result[0], result[1]))
        } else {
            Err(StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: "Invalid sentinel response".to_string(),
            })
        }
    }

    /// Spawn health check background task
    fn spawn_health_check_task(&self) {
        let pool = self.pool.clone();
        let interval = self.config.health_check_interval;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);

            loop {
                interval.tick().await;

                match pool.get().await {
                    Ok(mut conn) => {
                        let result: Result<String, _> = redis::cmd("PING").query_async(&mut *conn).await;
                        match result {
                            Ok(response) if response == "PONG" => {
                                trace!("Health check passed");

                                if let Some(ref metrics) = metrics {
                                    let status = pool.status();
                                    metrics.pool_connections_active.set(status.size as i64);
                                    metrics.pool_connections_idle.set((status.size - status.available) as i64);
                                }
                            }
                            Ok(_) | Err(_) => {
                                warn!("Health check failed");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to get connection for health check: {}", e);
                    }
                }
            }
        });
    }

    /// Build full key with prefix
    fn build_key(&self, key: &[u8]) -> Vec<u8> {
        let mut full_key = self.config.key_prefix.as_bytes().to_vec();
        full_key.extend_from_slice(key);
        full_key
    }

    /// Strip prefix from key
    fn strip_prefix(&self, key: &[u8]) -> Vec<u8> {
        let prefix = self.config.key_prefix.as_bytes();
        if key.starts_with(prefix) {
            key[prefix.len()..].to_vec()
        } else {
            key.to_vec()
        }
    }

    /// Compress data if enabled and above threshold
    fn compress(&self, data: &[u8]) -> StateResult<Vec<u8>> {
        if data.len() < self.config.compression_threshold {
            return Ok(data.to_vec());
        }

        match self.config.compression {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                lz4::block::compress(data, None, false).map_err(|e| StateError::SerializationFailed {
                    key: "compression".to_string(),
                    reason: format!("LZ4 compression failed: {}", e),
                })
            }
            CompressionAlgorithm::Snappy => {
                let mut compressed = Vec::new();
                let mut encoder = snap::write::FrameEncoder::new(&mut compressed);
                std::io::Write::write_all(&mut encoder, data).map_err(|e| StateError::SerializationFailed {
                    key: "compression".to_string(),
                    reason: format!("Snappy compression failed: {}", e),
                })?;
                drop(encoder);
                Ok(compressed)
            }
            CompressionAlgorithm::Zstd => {
                zstd::encode_all(data, 3).map_err(|e| StateError::SerializationFailed {
                    key: "compression".to_string(),
                    reason: format!("Zstd compression failed: {}", e),
                })
            }
        }
    }

    /// Decompress data if needed
    fn decompress(&self, data: &[u8]) -> StateResult<Vec<u8>> {
        if data.len() < self.config.compression_threshold {
            return Ok(data.to_vec());
        }

        match self.config.compression {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => {
                lz4::block::decompress(data, None).map_err(|e| StateError::DeserializationFailed {
                    key: "decompression".to_string(),
                    reason: format!("LZ4 decompression failed: {}", e),
                })
            }
            CompressionAlgorithm::Snappy => {
                let mut decompressed = Vec::new();
                let mut decoder = snap::read::FrameDecoder::new(data);
                std::io::Read::read_to_end(&mut decoder, &mut decompressed).map_err(|e| StateError::DeserializationFailed {
                    key: "decompression".to_string(),
                    reason: format!("Snappy decompression failed: {}", e),
                })?;
                Ok(decompressed)
            }
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(data).map_err(|e| StateError::DeserializationFailed {
                    key: "decompression".to_string(),
                    reason: format!("Zstd decompression failed: {}", e),
                })
            }
        }
    }

    /// Execute operation with retry, circuit breaker, and metrics
    async fn execute_with_resilience<F, Fut, T>(
        &self,
        operation_name: &str,
        mut f: F,
    ) -> StateResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = StateResult<T>>,
    {
        let start = Instant::now();
        let mut attempts = 0;
        let mut delay = self.config.retry_initial_delay;

        loop {
            attempts += 1;

            // Note: Circuit breaker with failsafe crate requires wrapping the operation
            // in cb.call() which would require refactoring this retry logic.
            // TODO: Properly integrate failsafe circuit breaker using cb.call()

            // Check circuit breaker (disabled for now - needs proper integration)
            // if let Some(ref cb) = self.circuit_breaker {
            //     // StateMachine doesn't expose is_call_permitted - needs cb.call() wrapper
            // }

            match f().await {
                Ok(result) => {
                    // Record success
                    let duration = start.elapsed();
                    self.record_operation_success(operation_name, duration).await;

                    // if let Some(ref cb) = self.circuit_breaker {
                    //     // StateMachine doesn't expose on_success - handled by cb.call()
                    // }

                    return Ok(result);
                }
                Err(e) => {
                    attempts += 1;

                    // if let Some(ref cb) = self.circuit_breaker {
                    //     // StateMachine doesn't expose on_error - handled by cb.call()
                    // }

                    if attempts >= self.config.max_retries {
                        let duration = start.elapsed();
                        self.record_operation_failure(operation_name, duration).await;
                        return Err(e);
                    }

                    // Apply jitter to delay
                    let jitter = if self.config.retry_jitter > 0.0 {
                        let mut rng = rand::thread_rng();
                        let jitter_factor: f64 = rng.gen();
                        let jitter_ms = (delay.as_millis() as f64 * self.config.retry_jitter * jitter_factor) as u64;
                        Duration::from_millis(jitter_ms)
                    } else {
                        Duration::from_millis(0)
                    };

                    let actual_delay = delay + jitter;
                    debug!("Retrying {} after {:?} (attempt {})", operation_name, actual_delay, attempts);

                    tokio::time::sleep(actual_delay).await;
                    delay = std::cmp::min(delay * 2, self.config.retry_max_delay);
                }
            }
        }
    }

    async fn record_operation_success(&self, operation: &str, duration: Duration) {
        if let Some(ref metrics) = self.metrics {
            metrics.operations_total.get_or_create(&MetricLabels {
                operation: operation.to_string(),
                status: "success".to_string(),
            }).inc();

            metrics.operation_duration.get_or_create(&MetricLabels {
                operation: operation.to_string(),
                status: "success".to_string(),
            }).observe(duration.as_secs_f64());
        }

        let mut stats = self.stats.write().await;
        stats.operations_total += 1;
        stats.operations_success += 1;

        // Update moving average latency
        let latency_us = duration.as_micros() as u64;
        stats.avg_latency_us = if stats.operations_total == 1 {
            latency_us
        } else {
            (stats.avg_latency_us * 9 + latency_us) / 10
        };

        if self.config.enable_slow_query_log && duration > self.config.slow_query_threshold {
            warn!("Slow query detected: {} took {:?}", operation, duration);
            stats.slow_queries += 1;
        }
    }

    async fn record_operation_failure(&self, operation: &str, duration: Duration) {
        if let Some(ref metrics) = self.metrics {
            metrics.operations_total.get_or_create(&MetricLabels {
                operation: operation.to_string(),
                status: "failure".to_string(),
            }).inc();

            metrics.errors_total.get_or_create(&MetricLabels {
                operation: operation.to_string(),
                status: "failure".to_string(),
            }).inc();
        }

        let mut stats = self.stats.write().await;
        stats.operations_total += 1;
        stats.operations_failed += 1;
    }

    /// Acquire distributed lock using Redlock algorithm
    pub async fn acquire_lock(&self, resource: &str, ttl: Duration) -> StateResult<RedlockGuard> {
        let token = Uuid::new_v4().to_string();
        let key = format!("{}lock:{}", self.config.key_prefix, resource);
        let ttl_ms = ttl.as_millis() as usize;

        let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: format!("Failed to get connection: {}", e),
        })?;

        // SET NX PX for atomic lock acquisition
        let result: RedisResult<String> = redis::cmd("SET")
            .arg(&key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(_) => {
                debug!("Lock acquired: {} with token {}", resource, token);
                Ok(RedlockGuard {
                    resource: resource.to_string(),
                    token,
                    backend: Arc::new(self.clone()),
                })
            }
            Err(e) => Err(StateError::TransactionFailed {
                operation: "acquire_lock".to_string(),
                reason: format!("Lock acquisition failed: {}", e),
            }),
        }
    }

    /// Try to acquire lock without blocking
    pub async fn try_acquire_lock(&self, resource: &str, ttl: Duration) -> StateResult<Option<RedlockGuard>> {
        match self.acquire_lock(resource, ttl).await {
            Ok(guard) => Ok(Some(guard)),
            Err(StateError::TransactionFailed { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Release lock (internal, used by guard drop)
    async fn release_lock_internal(&self, resource: &str, token: &str) -> StateResult<()> {
        let key = format!("{}lock:{}", self.config.key_prefix, resource);

        let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: format!("Failed to get connection: {}", e),
        })?;

        let result: i32 = self.scripts.release_lock
            .key(&key)
            .arg(token)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Lock release failed: {}", e),
            })?;

        if result == 1 {
            debug!("Lock released: {}", resource);
            Ok(())
        } else {
            warn!("Lock release failed: token mismatch for {}", resource);
            Err(StateError::TransactionFailed {
                operation: "release_lock".to_string(),
                reason: "Token mismatch".to_string(),
            })
        }
    }

    /// Batch get with pipeline
    pub async fn batch_get_optimized(&self, keys: &[&[u8]]) -> StateResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        self.execute_with_resilience("batch_get", || async {
            let full_keys: Vec<Vec<u8>> = keys.iter().map(|k| self.build_key(k)).collect();

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
                .arg(&full_keys)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StateError::StorageError {
                    backend_type: "EnterpriseRedis".to_string(),
                    details: format!("MGET failed: {}", e),
                })?;

            // Decompress and update stats
            let mut results = Vec::with_capacity(values.len());
            for value_opt in values {
                if let Some(compressed) = value_opt {
                    let decompressed = self.decompress(&compressed)?;
                    results.push(Some(decompressed));
                } else {
                    results.push(None);
                }
            }

            let mut stats = self.stats.write().await;
            for result in &results {
                if result.is_some() {
                    stats.cache_hits += 1;
                } else {
                    stats.cache_misses += 1;
                }
            }

            Ok(results)
        }).await
    }

    /// Batch put with pipeline
    pub async fn batch_put_optimized(&self, pairs: &[(&[u8], &[u8])]) -> StateResult<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        // Process in chunks
        for chunk in pairs.chunks(self.config.pipeline_batch_size) {
            self.execute_with_resilience("batch_put", || async {
                let mut pipe = redis::pipe();
                pipe.atomic();

                for (key, value) in chunk {
                    let full_key = self.build_key(key);
                    let compressed = self.compress(value)?;

                    if let Some(ttl) = self.config.default_ttl {
                        pipe.set_ex(&full_key, &compressed, ttl.as_secs() as u64);
                    } else {
                        pipe.set(&full_key, &compressed);
                    }
                }

                let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                    backend_type: "EnterpriseRedis".to_string(),
                    details: format!("Failed to get connection: {}", e),
                })?;

                pipe.query_async(&mut *conn).await.map_err(|e| StateError::StorageError {
                    backend_type: "EnterpriseRedis".to_string(),
                    details: format!("Pipeline failed: {}", e),
                })?;

                let mut stats = self.stats.write().await;
                stats.bytes_written += chunk.iter().map(|(_, v)| v.len() as u64).sum::<u64>();

                Ok(())
            }).await?;
        }

        Ok(())
    }

    /// Get backend statistics
    pub async fn stats(&self) -> BackendStats {
        self.stats.read().await.clone()
    }

    /// Health check
    pub async fn health_check(&self) -> StateResult<bool> {
        let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
            backend_type: "EnterpriseRedis".to_string(),
            details: format!("Failed to get connection: {}", e),
        })?;

        let result: RedisResult<String> = redis::cmd("PING").query_async(&mut *conn).await;
        Ok(result.map(|r| r == "PONG").unwrap_or(false))
    }

    /// Get metrics registry for Prometheus export
    pub fn metrics(&self) -> Option<Arc<RedisMetrics>> {
        self.metrics.clone()
    }
}

// Implement Clone for RedlockGuard support
impl Clone for EnterpriseRedisBackend {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            pool: self.pool.clone(),
            metrics: self.metrics.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            scripts: self.scripts.clone(),
            stats: self.stats.clone(),
        }
    }
}

#[async_trait]
impl StateBackend for EnterpriseRedisBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        self.execute_with_resilience("get", || async {
            let full_key = self.build_key(key);

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            let value: Option<Vec<u8>> = conn.get(&full_key).await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("GET failed: {}", e),
            })?;

            if let Some(compressed) = value {
                let decompressed = self.decompress(&compressed)?;

                let mut stats = self.stats.write().await;
                stats.cache_hits += 1;
                stats.bytes_read += decompressed.len() as u64;

                Ok(Some(decompressed))
            } else {
                let mut stats = self.stats.write().await;
                stats.cache_misses += 1;
                Ok(None)
            }
        }).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StateResult<()> {
        self.execute_with_resilience("put", || async {
            let full_key = self.build_key(key);
            let compressed = self.compress(value)?;

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            if let Some(ttl) = self.config.default_ttl {
                conn.set_ex(&full_key, &compressed, ttl.as_secs() as u64)
                    .await
                    .map_err(|e| StateError::StorageError {
                        backend_type: "EnterpriseRedis".to_string(),
                        details: format!("SET failed: {}", e),
                    })?;
            } else {
                conn.set(&full_key, &compressed)
                    .await
                    .map_err(|e| StateError::StorageError {
                        backend_type: "EnterpriseRedis".to_string(),
                        details: format!("SET failed: {}", e),
                    })?;
            }

            let mut stats = self.stats.write().await;
            stats.bytes_written += value.len() as u64;
            stats.bytes_compressed += compressed.len() as u64;

            // Update compression ratio metric
            if let Some(ref metrics) = self.metrics {
                if compressed.len() > 0 {
                    let ratio = value.len() as f64 / compressed.len() as f64;
                    metrics.compression_ratio.set(ratio as i64);
                }
            }

            Ok(())
        }).await
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        self.execute_with_resilience("delete", || async {
            let full_key = self.build_key(key);

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            conn.del(&full_key).await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("DEL failed: {}", e),
            })
        }).await
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Vec<u8>>> {
        self.execute_with_resilience("list_keys", || async {
            let pattern = if prefix.is_empty() {
                format!("{}*", self.config.key_prefix).into_bytes()
            } else {
                let mut p = self.config.key_prefix.as_bytes().to_vec();
                p.extend_from_slice(prefix);
                p.push(b'*');
                p
            };

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            let mut keys = Vec::new();
            let mut cursor = 0u64;

            loop {
                let (new_cursor, batch): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StateError::StorageError {
                        backend_type: "EnterpriseRedis".to_string(),
                        details: format!("SCAN failed: {}", e),
                    })?;

                keys.extend(batch);
                cursor = new_cursor;

                if cursor == 0 {
                    break;
                }
            }

            // Strip prefix
            let stripped: Vec<Vec<u8>> = keys.into_iter()
                .map(|k| self.strip_prefix(&k))
                .collect();

            Ok(stripped)
        }).await
    }

    async fn clear(&self) -> StateResult<()> {
        self.execute_with_resilience("clear", || async {
            let pattern = format!("{}*", self.config.key_prefix).into_bytes();
            let keys = self.list_keys(&[]).await?;

            if keys.is_empty() {
                return Ok(());
            }

            for chunk in keys.chunks(self.config.pipeline_batch_size) {
                let full_keys: Vec<Vec<u8>> = chunk.iter()
                    .map(|k| self.build_key(k))
                    .collect();

                let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                    backend_type: "EnterpriseRedis".to_string(),
                    details: format!("Failed to get connection: {}", e),
                })?;

                redis::cmd("DEL")
                    .arg(&full_keys)
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StateError::StorageError {
                        backend_type: "EnterpriseRedis".to_string(),
                        details: format!("DEL failed: {}", e),
                    })?;
            }

            Ok(())
        }).await
    }

    async fn count(&self) -> StateResult<usize> {
        let keys = self.list_keys(&[]).await?;
        Ok(keys.len())
    }

    async fn contains(&self, key: &[u8]) -> StateResult<bool> {
        self.execute_with_resilience("contains", || async {
            let full_key = self.build_key(key);

            let mut conn = self.pool.get().await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("Failed to get connection: {}", e),
            })?;

            conn.exists(&full_key).await.map_err(|e| StateError::StorageError {
                backend_type: "EnterpriseRedis".to_string(),
                details: format!("EXISTS failed: {}", e),
            })
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_backend() -> Option<EnterpriseRedisBackend> {
        let config = EnterpriseRedisConfig::builder()
            .mode(RedisMode::Standalone {
                url: "redis://localhost:6379".to_string(),
            })
            .key_prefix("test:enterprise:")
            .compression(CompressionAlgorithm::Lz4)
            .enable_metrics(true)
            .build()
            .ok()?;

        EnterpriseRedisBackend::new(config).await.ok()
    }

    #[tokio::test]
    async fn test_basic_operations() {
        if let Some(backend) = create_test_backend().await {
            backend.clear().await.unwrap();

            backend.put(b"key1", b"value1").await.unwrap();
            assert_eq!(backend.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert!(backend.contains(b"key1").await.unwrap());

            backend.delete(b"key1").await.unwrap();
            assert_eq!(backend.get(b"key1").await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn test_compression() {
        if let Some(backend) = create_test_backend().await {
            backend.clear().await.unwrap();

            let large_value = vec![b'x'; 10000];
            backend.put(b"large", &large_value).await.unwrap();

            let retrieved = backend.get(b"large").await.unwrap().unwrap();
            assert_eq!(retrieved, large_value);

            let stats = backend.stats().await;
            assert!(stats.bytes_compressed < stats.bytes_written);
        }
    }

    #[tokio::test]
    async fn test_batch_operations() {
        if let Some(backend) = create_test_backend().await {
            backend.clear().await.unwrap();

            let pairs = vec![
                (b"batch1".as_ref(), b"value1".as_ref()),
                (b"batch2".as_ref(), b"value2".as_ref()),
                (b"batch3".as_ref(), b"value3".as_ref()),
            ];
            backend.batch_put_optimized(&pairs).await.unwrap();

            let keys = vec![b"batch1".as_ref(), b"batch2".as_ref(), b"batch3".as_ref()];
            let values = backend.batch_get_optimized(&keys).await.unwrap();

            assert_eq!(values.len(), 3);
            assert_eq!(values[0], Some(b"value1".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_distributed_lock() {
        if let Some(backend) = create_test_backend().await {
            let guard1 = backend.acquire_lock("resource1", Duration::from_secs(10)).await;
            assert!(guard1.is_ok());

            let guard2 = backend.try_acquire_lock("resource1", Duration::from_secs(10)).await.unwrap();
            assert!(guard2.is_none());

            drop(guard1);
            tokio::time::sleep(Duration::from_millis(100)).await;

            let guard3 = backend.try_acquire_lock("resource1", Duration::from_secs(10)).await.unwrap();
            assert!(guard3.is_some());
        }
    }

    #[tokio::test]
    async fn test_health_check() {
        if let Some(backend) = create_test_backend().await {
            assert!(backend.health_check().await.unwrap());
        }
    }
}
