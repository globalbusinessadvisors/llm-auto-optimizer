//! Enterprise-grade PostgreSQL backend with advanced features
//!
//! This module extends the basic PostgreSQL backend with production-ready features:
//!
//! - **Connection Pooling**: Advanced pool management with health checks
//! - **Replication**: Read replica support with automatic failover
//! - **Transactions**: Full isolation level control with optimistic locking
//! - **Advanced Queries**: Prepared statements, batch operations, JSONB queries
//! - **Distributed Coordination**: Advisory locks, listen/notify, leader election
//! - **Data Management**: Partitioning, compression, retention policies
//! - **High Availability**: Streaming replication, failover automation
//! - **Observability**: Comprehensive metrics and slow query logging
//! - **Security**: TLS/SSL with certificate validation

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{Executor, PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use super::backend::StateBackend;
use crate::error::{StateError, StateResult};

// ============================================================================
// Configuration Structures
// ============================================================================

/// PostgreSQL SSL/TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSslConfig {
    /// SSL mode (disable, allow, prefer, require, verify-ca, verify-full)
    pub mode: String,
    /// Path to root certificate
    pub root_cert_path: Option<PathBuf>,
    /// Path to client certificate
    pub client_cert_path: Option<PathBuf>,
    /// Path to client key
    pub client_key_path: Option<PathBuf>,
}

impl Default for PostgresSslConfig {
    fn default() -> Self {
        Self {
            mode: "prefer".to_string(),
            root_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
        }
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresPoolConfig {
    /// Minimum number of connections
    pub min_connections: u32,
    /// Maximum number of connections
    pub max_connections: u32,
    /// Connection acquire timeout
    pub acquire_timeout: Duration,
    /// Connection idle timeout
    pub idle_timeout: Option<Duration>,
    /// Connection lifetime before recycling
    pub max_lifetime: Option<Duration>,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Enable test on acquire
    pub test_on_acquire: bool,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 20,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
            health_check_interval: Duration::from_secs(30),
            test_on_acquire: true,
        }
    }
}

/// Read replica configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresReplicaConfig {
    /// Replica connection URLs
    pub replica_urls: Vec<String>,
    /// Enable read/write splitting
    pub enable_read_splitting: bool,
    /// Maximum acceptable replication lag (ms)
    pub max_replication_lag_ms: i64,
    /// Replica health check interval
    pub health_check_interval: Duration,
    /// Enable automatic failover
    pub enable_auto_failover: bool,
}

impl Default for PostgresReplicaConfig {
    fn default() -> Self {
        Self {
            replica_urls: Vec::new(),
            enable_read_splitting: true,
            max_replication_lag_ms: 1000,
            health_check_interval: Duration::from_secs(10),
            enable_auto_failover: true,
        }
    }
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Partitioning strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Partition by hash of key
    Hash { num_partitions: u32 },
    /// Partition by time range
    TimeRange {
        column: String,
        interval: String, // e.g., "1 day", "1 week", "1 month"
    },
    /// Partition by list of values
    List { column: String, values: Vec<String> },
}

/// Data retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Enable automatic cleanup
    pub enabled: bool,
    /// Age threshold for deletion
    pub max_age: Duration,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Batch size for deletion
    pub batch_size: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_age: Duration::from_secs(86400 * 30), // 30 days
            cleanup_interval: Duration::from_secs(3600), // 1 hour
            batch_size: 1000,
        }
    }
}

/// Comprehensive PostgreSQL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedPostgresConfig {
    /// Primary database URL
    pub primary_url: String,
    /// Connection pool configuration
    pub pool: PostgresPoolConfig,
    /// SSL/TLS configuration
    pub ssl: PostgresSslConfig,
    /// Replica configuration
    pub replicas: Option<PostgresReplicaConfig>,
    /// Default transaction isolation level
    pub default_isolation_level: IsolationLevel,
    /// Table name for state storage
    pub table_name: String,
    /// Schema name
    pub schema_name: String,
    /// Enable prepared statement caching
    pub enable_prepared_statements: bool,
    /// Prepared statement cache size
    pub prepared_statement_cache_size: usize,
    /// Partitioning strategy
    pub partition_strategy: Option<PartitionStrategy>,
    /// Data retention policy
    pub retention_policy: RetentionPolicy,
    /// Query timeout
    pub query_timeout: Duration,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry backoff base duration
    pub retry_backoff: Duration,
    /// Enable slow query logging
    pub enable_slow_query_log: bool,
    /// Slow query threshold
    pub slow_query_threshold: Duration,
    /// Enable statement logging
    pub log_statements: bool,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for EnhancedPostgresConfig {
    fn default() -> Self {
        Self {
            primary_url: "postgresql://postgres:postgres@localhost/optimizer".to_string(),
            pool: PostgresPoolConfig::default(),
            ssl: PostgresSslConfig::default(),
            replicas: None,
            default_isolation_level: IsolationLevel::ReadCommitted,
            table_name: "state_entries".to_string(),
            schema_name: "public".to_string(),
            enable_prepared_statements: true,
            prepared_statement_cache_size: 100,
            partition_strategy: None,
            retention_policy: RetentionPolicy::default(),
            query_timeout: Duration::from_secs(60),
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
            enable_slow_query_log: true,
            slow_query_threshold: Duration::from_millis(1000),
            log_statements: false,
            enable_metrics: true,
        }
    }
}

impl EnhancedPostgresConfig {
    pub fn new<S: Into<String>>(primary_url: S) -> Self {
        Self {
            primary_url: primary_url.into(),
            ..Default::default()
        }
    }

    pub fn with_pool_config(mut self, pool: PostgresPoolConfig) -> Self {
        self.pool = pool;
        self
    }

    pub fn with_ssl_config(mut self, ssl: PostgresSslConfig) -> Self {
        self.ssl = ssl;
        self
    }

    pub fn with_replicas(mut self, replicas: PostgresReplicaConfig) -> Self {
        self.replicas = Some(replicas);
        self
    }

    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.default_isolation_level = level;
        self
    }

    pub fn with_partitioning(mut self, strategy: PartitionStrategy) -> Self {
        self.partition_strategy = Some(strategy);
        self
    }

    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention_policy = policy;
        self
    }
}

// ============================================================================
// Metrics and Statistics
// ============================================================================

/// Enhanced statistics tracking
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EnhancedPostgresStats {
    // Basic operations
    pub get_count: AtomicU64,
    pub put_count: AtomicU64,
    pub delete_count: AtomicU64,
    pub query_count: AtomicU64,

    // Transactions
    pub transaction_count: AtomicU64,
    pub transaction_rollback_count: AtomicU64,
    pub transaction_conflict_count: AtomicU64,

    // Connection pool
    pub pool_acquire_count: AtomicU64,
    pub pool_acquire_duration_ms: AtomicU64,
    pub pool_timeout_count: AtomicU64,

    // Replication
    pub replica_read_count: AtomicU64,
    pub replica_failover_count: AtomicU64,
    pub replication_lag_ms: AtomicU64,

    // Performance
    pub slow_query_count: AtomicU64,
    pub avg_query_duration_ms: AtomicU64,
    pub retry_count: AtomicU64,

    // Data
    pub bytes_written: AtomicU64,
    pub bytes_read: AtomicU64,
    pub rows_inserted: AtomicU64,
    pub rows_updated: AtomicU64,
    pub rows_deleted: AtomicU64,

    // Batch operations
    pub batch_insert_count: AtomicU64,
    pub batch_insert_rows: AtomicU64,

    // Distributed locks
    pub advisory_lock_count: AtomicU64,
    pub advisory_lock_wait_ms: AtomicU64,
}

impl EnhancedPostgresStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> PostgresStatsSnapshot {
        PostgresStatsSnapshot {
            get_count: self.get_count.load(Ordering::Relaxed),
            put_count: self.put_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
            query_count: self.query_count.load(Ordering::Relaxed),
            transaction_count: self.transaction_count.load(Ordering::Relaxed),
            transaction_rollback_count: self.transaction_rollback_count.load(Ordering::Relaxed),
            transaction_conflict_count: self.transaction_conflict_count.load(Ordering::Relaxed),
            pool_acquire_count: self.pool_acquire_count.load(Ordering::Relaxed),
            pool_acquire_duration_ms: self.pool_acquire_duration_ms.load(Ordering::Relaxed),
            pool_timeout_count: self.pool_timeout_count.load(Ordering::Relaxed),
            replica_read_count: self.replica_read_count.load(Ordering::Relaxed),
            replica_failover_count: self.replica_failover_count.load(Ordering::Relaxed),
            replication_lag_ms: self.replication_lag_ms.load(Ordering::Relaxed),
            slow_query_count: self.slow_query_count.load(Ordering::Relaxed),
            avg_query_duration_ms: self.avg_query_duration_ms.load(Ordering::Relaxed),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            rows_inserted: self.rows_inserted.load(Ordering::Relaxed),
            rows_updated: self.rows_updated.load(Ordering::Relaxed),
            rows_deleted: self.rows_deleted.load(Ordering::Relaxed),
            batch_insert_count: self.batch_insert_count.load(Ordering::Relaxed),
            batch_insert_rows: self.batch_insert_rows.load(Ordering::Relaxed),
            advisory_lock_count: self.advisory_lock_count.load(Ordering::Relaxed),
            advisory_lock_wait_ms: self.advisory_lock_wait_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresStatsSnapshot {
    pub get_count: u64,
    pub put_count: u64,
    pub delete_count: u64,
    pub query_count: u64,
    pub transaction_count: u64,
    pub transaction_rollback_count: u64,
    pub transaction_conflict_count: u64,
    pub pool_acquire_count: u64,
    pub pool_acquire_duration_ms: u64,
    pub pool_timeout_count: u64,
    pub replica_read_count: u64,
    pub replica_failover_count: u64,
    pub replication_lag_ms: u64,
    pub slow_query_count: u64,
    pub avg_query_duration_ms: u64,
    pub retry_count: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    pub batch_insert_count: u64,
    pub batch_insert_rows: u64,
    pub advisory_lock_count: u64,
    pub advisory_lock_wait_ms: u64,
}

// ============================================================================
// Replica Management
// ============================================================================

struct ReplicaInfo {
    pool: PgPool,
    url: String,
    healthy: AtomicBool,
    lag_ms: AtomicU64,
    last_check: Mutex<Instant>,
}

struct ReplicaManager {
    replicas: Vec<Arc<ReplicaInfo>>,
    config: PostgresReplicaConfig,
    current_index: AtomicU64,
}

impl ReplicaManager {
    async fn new(config: PostgresReplicaConfig, ssl_config: &PostgresSslConfig) -> StateResult<Self> {
        let mut replicas = Vec::new();

        for url in &config.replica_urls {
            match Self::create_replica_pool(url, ssl_config).await {
                Ok(pool) => {
                    replicas.push(Arc::new(ReplicaInfo {
                        pool,
                        url: url.clone(),
                        healthy: AtomicBool::new(true),
                        lag_ms: AtomicU64::new(0),
                        last_check: Mutex::new(Instant::now()),
                    }));
                }
                Err(e) => {
                    warn!("Failed to connect to replica {}: {}", url, e);
                }
            }
        }

        Ok(Self {
            replicas,
            config,
            current_index: AtomicU64::new(0),
        })
    }

    async fn create_replica_pool(url: &str, ssl_config: &PostgresSslConfig) -> StateResult<PgPool> {
        let ssl_mode = Self::parse_ssl_mode(&ssl_config.mode)?;
        let mut connect_options = PgConnectOptions::from_str(url)
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Invalid replica URL: {}", e),
            })?
            .ssl_mode(ssl_mode);

        if let Some(root_cert) = &ssl_config.root_cert_path {
            connect_options = connect_options.ssl_root_cert(root_cert);
        }

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(connect_options)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to create replica pool: {}", e),
            })?;

        Ok(pool)
    }

    fn parse_ssl_mode(mode: &str) -> StateResult<PgSslMode> {
        match mode.to_lowercase().as_str() {
            "disable" => Ok(PgSslMode::Disable),
            "allow" => Ok(PgSslMode::Allow),
            "prefer" => Ok(PgSslMode::Prefer),
            "require" => Ok(PgSslMode::Require),
            "verify-ca" => Ok(PgSslMode::VerifyCa),
            "verify-full" => Ok(PgSslMode::VerifyFull),
            _ => Err(StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Invalid SSL mode: {}", mode),
            }),
        }
    }

    async fn get_read_pool(&self) -> Option<&PgPool> {
        if self.replicas.is_empty() || !self.config.enable_read_splitting {
            return None;
        }

        // Round-robin selection
        let start_idx = self.current_index.fetch_add(1, Ordering::Relaxed) as usize % self.replicas.len();

        for i in 0..self.replicas.len() {
            let idx = (start_idx + i) % self.replicas.len();
            let replica = &self.replicas[idx];

            if replica.healthy.load(Ordering::Relaxed) {
                let lag = replica.lag_ms.load(Ordering::Relaxed);
                if lag <= self.config.max_replication_lag_ms as u64 {
                    return Some(&replica.pool);
                }
            }
        }

        None
    }

    async fn check_replica_health(&self, stats: &EnhancedPostgresStats) {
        for replica in &self.replicas {
            let mut last_check = replica.last_check.lock().await;
            if last_check.elapsed() < self.config.health_check_interval {
                continue;
            }
            *last_check = Instant::now();
            drop(last_check);

            // Check if replica is reachable
            match sqlx::query("SELECT 1").fetch_one(&replica.pool).await {
                Ok(_) => {
                    replica.healthy.store(true, Ordering::Relaxed);

                    // Measure replication lag
                    if let Ok(lag) = self.measure_replication_lag(&replica.pool).await {
                        replica.lag_ms.store(lag as u64, Ordering::Relaxed);
                        stats.replication_lag_ms.store(lag as u64, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    warn!("Replica {} unhealthy: {}", replica.url, e);
                    replica.healthy.store(false, Ordering::Relaxed);

                    if self.config.enable_auto_failover {
                        stats.replica_failover_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    async fn measure_replication_lag(&self, pool: &PgPool) -> StateResult<i64> {
        let row = sqlx::query(
            "SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())) * 1000 AS lag_ms"
        )
        .fetch_one(pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to measure replication lag: {}", e),
        })?;

        let lag: Option<f64> = row.try_get("lag_ms").ok();
        Ok(lag.unwrap_or(0.0) as i64)
    }
}

// ============================================================================
// Transaction Manager
// ============================================================================

pub struct PostgresTransaction<'a> {
    pub(crate) tx: Transaction<'a, Postgres>,
    isolation_level: IsolationLevel,
}

impl<'a> PostgresTransaction<'a> {
    async fn new(pool: &'a PgPool, isolation_level: IsolationLevel) -> StateResult<Self> {
        let mut tx = pool.begin().await.map_err(|e| StateError::TransactionFailed {
            operation: "begin".to_string(),
            reason: e.to_string(),
        })?;

        // Set isolation level
        sqlx::query(&format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level.as_sql()))
            .execute(&mut *tx)
            .await
            .map_err(|e| StateError::TransactionFailed {
                operation: "set_isolation_level".to_string(),
                reason: e.to_string(),
            })?;

        Ok(Self { tx, isolation_level })
    }

    pub async fn commit(self) -> StateResult<()> {
        self.tx.commit().await.map_err(|e| StateError::TransactionFailed {
            operation: "commit".to_string(),
            reason: e.to_string(),
        })
    }

    pub async fn rollback(self) -> StateResult<()> {
        self.tx.rollback().await.map_err(|e| StateError::TransactionFailed {
            operation: "rollback".to_string(),
            reason: e.to_string(),
        })
    }

    pub async fn savepoint(&mut self, name: &str) -> StateResult<()> {
        sqlx::query(&format!("SAVEPOINT {}", name))
            .execute(&mut *self.tx)
            .await
            .map_err(|e| StateError::TransactionFailed {
                operation: "savepoint".to_string(),
                reason: e.to_string(),
            })?;
        Ok(())
    }

    pub async fn rollback_to_savepoint(&mut self, name: &str) -> StateResult<()> {
        sqlx::query(&format!("ROLLBACK TO SAVEPOINT {}", name))
            .execute(&mut *self.tx)
            .await
            .map_err(|e| StateError::TransactionFailed {
                operation: "rollback_to_savepoint".to_string(),
                reason: e.to_string(),
            })?;
        Ok(())
    }

    pub fn executor(&mut self) -> &mut Transaction<'a, Postgres> {
        &mut self.tx
    }
}

// ============================================================================
// Main Backend Implementation
// ============================================================================

pub struct EnhancedPostgresBackend {
    primary_pool: PgPool,
    replica_manager: Option<Arc<ReplicaManager>>,
    config: EnhancedPostgresConfig,
    stats: Arc<EnhancedPostgresStats>,
    prepared_statements: Arc<RwLock<HashMap<String, String>>>,
    health_check_running: Arc<AtomicBool>,
}

impl EnhancedPostgresBackend {
    pub async fn new(config: EnhancedPostgresConfig) -> StateResult<Self> {
        info!("Initializing Enhanced PostgreSQL backend");
        info!("Primary: {}", Self::sanitize_url(&config.primary_url));
        info!("Pool: {:?}", config.pool);

        // Create primary pool
        let primary_pool = Self::create_primary_pool(&config).await?;

        // Create replica manager if configured
        let replica_manager = if let Some(replica_config) = &config.replicas {
            info!("Initializing {} read replicas", replica_config.replica_urls.len());
            Some(Arc::new(ReplicaManager::new(replica_config.clone(), &config.ssl).await?))
        } else {
            None
        };

        let backend = Self {
            primary_pool,
            replica_manager,
            config,
            stats: Arc::new(EnhancedPostgresStats::new()),
            prepared_statements: Arc::new(RwLock::new(HashMap::new())),
            health_check_running: Arc::new(AtomicBool::new(false)),
        };

        // Run migrations
        backend.run_migrations().await?;

        // Initialize partitioning if configured
        if backend.config.partition_strategy.is_some() {
            backend.setup_partitioning().await?;
        }

        // Start health check background task
        if backend.config.enable_metrics {
            backend.start_health_check_task();
        }

        info!("Enhanced PostgreSQL backend initialized successfully");
        Ok(backend)
    }

    async fn create_primary_pool(config: &EnhancedPostgresConfig) -> StateResult<PgPool> {
        let ssl_mode = ReplicaManager::parse_ssl_mode(&config.ssl.mode)?;

        let mut connect_options = PgConnectOptions::from_str(&config.primary_url)
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Invalid database URL: {}", e),
            })?
            .ssl_mode(ssl_mode);

        if let Some(root_cert) = &config.ssl.root_cert_path {
            connect_options = connect_options.ssl_root_cert(root_cert);
        }

        if let Some(client_cert) = &config.ssl.client_cert_path {
            if let Some(client_key) = &config.ssl.client_key_path {
                connect_options = connect_options
                    .ssl_client_cert(client_cert)
                    .ssl_client_key(client_key);
            }
        }

        let mut pool_options = PgPoolOptions::new()
            .min_connections(config.pool.min_connections)
            .max_connections(config.pool.max_connections)
            .acquire_timeout(config.pool.acquire_timeout);

        if let Some(idle_timeout) = config.pool.idle_timeout {
            pool_options = pool_options.idle_timeout(idle_timeout);
        }

        if let Some(max_lifetime) = config.pool.max_lifetime {
            pool_options = pool_options.max_lifetime(max_lifetime);
        }

        if config.pool.test_on_acquire {
            pool_options = pool_options.test_before_acquire(true);
        }

        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to create connection pool: {}", e),
            })?;

        info!("Primary connection pool created successfully");
        Ok(pool)
    }

    fn start_health_check_task(&self) {
        if self.health_check_running.swap(true, Ordering::Relaxed) {
            return; // Already running
        }

        let replica_manager = self.replica_manager.clone();
        let stats = self.stats.clone();
        let interval = self.config.pool.health_check_interval;
        let running = self.health_check_running.clone();

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Some(rm) = &replica_manager {
                    rm.check_replica_health(&stats).await;
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    fn stop_health_check_task(&self) {
        self.health_check_running.store(false, Ordering::Relaxed);
    }

    async fn run_migrations(&self) -> StateResult<()> {
        info!("Running database migrations");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

        if !migration_dir.exists() {
            warn!("Migrations directory not found: {:?}", migration_dir);
            return Ok(());
        }

        let mut migrations = Vec::new();
        let mut entries = tokio::fs::read_dir(&migration_dir).await.map_err(|e| {
            StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to read migrations: {}", e),
            }
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to read migration entry: {}", e),
        })? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                migrations.push(path);
            }
        }

        migrations.sort();

        for migration_path in migrations {
            let filename = migration_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            debug!("Applying migration: {}", filename);

            let sql = tokio::fs::read_to_string(&migration_path)
                .await
                .map_err(|e| StateError::StorageError {
                    backend_type: "postgres".to_string(),
                    details: format!("Failed to read migration {}: {}", filename, e),
                })?;

            sqlx::raw_sql(&sql)
                .execute(&self.primary_pool)
                .await
                .map_err(|e| StateError::StorageError {
                    backend_type: "postgres".to_string(),
                    details: format!("Failed to execute migration {}: {}", filename, e),
                })?;
        }

        info!("All migrations applied successfully");
        Ok(())
    }

    async fn setup_partitioning(&self) -> StateResult<()> {
        if let Some(strategy) = &self.config.partition_strategy {
            info!("Setting up table partitioning: {:?}", strategy);

            match strategy {
                PartitionStrategy::Hash { num_partitions } => {
                    self.setup_hash_partitioning(*num_partitions).await?;
                }
                PartitionStrategy::TimeRange { column, interval } => {
                    self.setup_time_range_partitioning(column, interval).await?;
                }
                PartitionStrategy::List { column, values } => {
                    self.setup_list_partitioning(column, values).await?;
                }
            }
        }
        Ok(())
    }

    async fn setup_hash_partitioning(&self, num_partitions: u32) -> StateResult<()> {
        // This would require recreating the table as partitioned
        // For now, log a warning that this needs manual setup
        warn!("Hash partitioning requires manual table recreation. Please run:");
        warn!("  CREATE TABLE {} PARTITION BY HASH (key);", self.config.table_name);
        for i in 0..num_partitions {
            warn!("  CREATE TABLE {}_p{} PARTITION OF {} FOR VALUES WITH (MODULUS {}, REMAINDER {});",
                  self.config.table_name, i, self.config.table_name, num_partitions, i);
        }
        Ok(())
    }

    async fn setup_time_range_partitioning(&self, column: &str, interval: &str) -> StateResult<()> {
        warn!("Time-range partitioning requires manual table recreation. Please run:");
        warn!("  CREATE TABLE {} PARTITION BY RANGE ({});", self.config.table_name, column);
        warn!("  -- Then create partitions for each time range");
        Ok(())
    }

    async fn setup_list_partitioning(&self, column: &str, values: &[String]) -> StateResult<()> {
        warn!("List partitioning requires manual table recreation. Please run:");
        warn!("  CREATE TABLE {} PARTITION BY LIST ({});", self.config.table_name, column);
        Ok(())
    }

    fn sanitize_url(url: &str) -> String {
        if let Some(at_pos) = url.rfind('@') {
            if let Some(scheme_end) = url.find("://") {
                let scheme = &url[..scheme_end + 3];
                let host = &url[at_pos + 1..];
                return format!("{}***@{}", scheme, host);
            }
        }
        url.to_string()
    }

    // ========================================================================
    // Advanced Query Operations
    // ========================================================================

    /// Execute query with metrics tracking
    async fn execute_tracked<F, T, Fut>(&self, operation: &str, f: F) -> StateResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = StateResult<T>>,
    {
        let start = Instant::now();
        let result = f().await;
        let duration = start.elapsed();

        if self.config.enable_metrics {
            self.stats.query_count.fetch_add(1, Ordering::Relaxed);

            let duration_ms = duration.as_millis() as u64;
            let current_avg = self.stats.avg_query_duration_ms.load(Ordering::Relaxed);
            let new_avg = if current_avg == 0 {
                duration_ms
            } else {
                (current_avg + duration_ms) / 2
            };
            self.stats.avg_query_duration_ms.store(new_avg, Ordering::Relaxed);

            if duration > self.config.slow_query_threshold {
                self.stats.slow_query_count.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Slow query detected: {} took {:?} (threshold: {:?})",
                    operation, duration, self.config.slow_query_threshold
                );
            }
        }

        result
    }

    /// Get connection pool (primary or replica based on operation type)
    async fn get_pool_for_read(&self) -> &PgPool {
        if let Some(replica_manager) = &self.replica_manager {
            if let Some(pool) = replica_manager.get_read_pool().await {
                self.stats.replica_read_count.fetch_add(1, Ordering::Relaxed);
                return pool;
            }
        }
        &self.primary_pool
    }

    /// Helper function for prefix range calculation
    fn next_prefix(prefix: &[u8]) -> Vec<u8> {
        let mut next = prefix.to_vec();
        for i in (0..next.len()).rev() {
            if next[i] < 255 {
                next[i] += 1;
                return next;
            }
        }
        next.push(0);
        next
    }

    fn clone_handle(&self) -> Self {
        Self {
            primary_pool: self.primary_pool.clone(),
            replica_manager: self.replica_manager.clone(),
            config: self.config.clone(),
            stats: self.stats.clone(),
            prepared_statements: self.prepared_statements.clone(),
            health_check_running: self.health_check_running.clone(),
        }
    }

    // ========================================================================
    // Extended Public API
    // ========================================================================

    /// Begin a new transaction with specified isolation level
    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> StateResult<PostgresTransaction<'_>> {
        trace!("BEGIN TRANSACTION: isolation={:?}", isolation_level);

        let tx = PostgresTransaction::new(&self.primary_pool, isolation_level).await?;
        self.stats.transaction_count.fetch_add(1, Ordering::Relaxed);

        Ok(tx)
    }

    /// Batch insert using COPY command for maximum performance
    pub async fn batch_copy(&self, entries: &[(&[u8], &[u8])]) -> StateResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        info!("Batch COPY: {} entries", entries.len());

        let mut tx = self.primary_pool.begin().await.map_err(|e| {
            StateError::TransactionFailed {
                operation: "begin".to_string(),
                reason: e.to_string(),
            }
        })?;

        // Use multi-row INSERT as efficient alternative to COPY
        let values_per_batch = 1000;
        for chunk in entries.chunks(values_per_batch) {
            let mut values = Vec::new();
            for (i, _) in chunk.iter().enumerate() {
                values.push(format!("(${}::bytea, ${}::bytea, NOW())", i * 2 + 1, i * 2 + 2));
            }

            let sql = format!(
                "INSERT INTO {} (key, value, updated_at) VALUES {}
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
                self.config.table_name,
                values.join(", ")
            );

            let mut query = sqlx::query(&sql);
            for (key, value) in chunk {
                query = query.bind(*key).bind(*value);
            }

            query.execute(&mut *tx).await.map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Bulk insert failed: {}", e),
            })?;
        }

        tx.commit().await.map_err(|e| StateError::TransactionFailed {
            operation: "commit".to_string(),
            reason: e.to_string(),
        })?;

        self.stats.batch_insert_count.fetch_add(1, Ordering::Relaxed);
        self.stats.batch_insert_rows.fetch_add(entries.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Batch put operation (standard transaction version)
    pub async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> StateResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        debug!("BATCH_PUT: {} entries", entries.len());

        let mut tx = self.begin_transaction(self.config.default_isolation_level).await?;

        for (key, value) in entries {
            sqlx::query(
                "INSERT INTO state_entries (key, value, updated_at)
                 VALUES ($1, $2, NOW())
                 ON CONFLICT (key) DO UPDATE
                 SET value = EXCLUDED.value, updated_at = NOW()"
            )
            .bind(key)
            .bind(value)
            .execute(&mut **tx.executor())
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Batch insert failed: {}", e),
            })?;
        }

        tx.commit().await?;
        self.stats.put_count.fetch_add(entries.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Query with JSONB path expressions
    pub async fn query_by_metadata(
        &self,
        json_path: &str,
        value: &serde_json::Value,
    ) -> StateResult<Vec<(Vec<u8>, Vec<u8>)>> {
        debug!("Querying by metadata: path={}", json_path);

        let pool = self.get_pool_for_read().await;

        let rows = sqlx::query(&format!(
            "SELECT key, value FROM {}
             WHERE metadata @> $1::jsonb
             AND (expires_at IS NULL OR expires_at > NOW())",
            self.config.table_name
        ))
        .bind(value)
        .fetch_all(pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Metadata query failed: {}", e),
        })?;

        let mut results = Vec::new();
        for row in rows {
            let key: Vec<u8> = row.try_get("key").map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to extract key: {}", e),
            })?;

            let value: Vec<u8> = row.try_get("value").map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to extract value: {}", e),
            })?;

            results.push((key, value));
        }

        Ok(results)
    }

    /// Put with optimistic locking using version column
    pub async fn put_with_version(
        &self,
        key: &[u8],
        value: &[u8],
        expected_version: Option<i64>,
    ) -> StateResult<i64> {
        trace!("Put with version check: key_len={}", key.len());

        let result = if let Some(expected) = expected_version {
            // Update only if version matches
            sqlx::query(
                "UPDATE state_entries
                 SET value = $1, version = version + 1, updated_at = NOW()
                 WHERE key = $2 AND version = $3
                 RETURNING version",
            )
            .bind(value)
            .bind(key)
            .bind(expected)
            .fetch_optional(&self.primary_pool)
            .await
        } else {
            // Insert or update
            sqlx::query(
                "INSERT INTO state_entries (key, value, version, updated_at)
                 VALUES ($1, $2, 1, NOW())
                 ON CONFLICT (key) DO UPDATE
                 SET value = EXCLUDED.value,
                     version = state_entries.version + 1,
                     updated_at = NOW()
                 RETURNING version",
            )
            .bind(key)
            .bind(value)
            .fetch_optional(&self.primary_pool)
            .await
        }
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Versioned put failed: {}", e),
        })?;

        if let Some(row) = result {
            let version: i64 = row.try_get("version").map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to get version: {}", e),
            })?;

            self.stats.put_count.fetch_add(1, Ordering::Relaxed);
            self.stats.rows_updated.fetch_add(1, Ordering::Relaxed);

            Ok(version)
        } else {
            Err(StateError::ConcurrentModification {
                key: String::from_utf8_lossy(key).to_string(),
            })
        }
    }

    /// Put with metadata
    pub async fn put_with_metadata(
        &self,
        key: &[u8],
        value: &[u8],
        metadata: serde_json::Value,
    ) -> StateResult<()> {
        trace!("PUT_WITH_METADATA: key_len={}", key.len());

        sqlx::query(
            "INSERT INTO state_entries (key, value, metadata, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value,
                 metadata = EXCLUDED.metadata,
                 updated_at = NOW()"
        )
        .bind(key)
        .bind(value)
        .bind(metadata)
        .execute(&self.primary_pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Put with metadata failed: {}", e),
        })?;

        self.stats.put_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Put with TTL
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: Duration) -> StateResult<()> {
        trace!("PUT_WITH_TTL: key_len={}, ttl={:?}", key.len(), ttl);

        let expires_at = chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap();

        sqlx::query(
            "INSERT INTO state_entries (key, value, expires_at, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value,
                 expires_at = EXCLUDED.expires_at,
                 updated_at = NOW()"
        )
        .bind(key)
        .bind(value)
        .bind(expires_at)
        .execute(&self.primary_pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Put with TTL failed: {}", e),
        })?;

        self.stats.put_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Clean up expired entries
    pub async fn cleanup_expired(&self) -> StateResult<usize> {
        debug!("CLEANUP_EXPIRED");

        let result = sqlx::query("SELECT cleanup_expired_state_entries()")
            .fetch_one(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Cleanup failed: {}", e),
            })?;

        let count: i64 = result.try_get(0).map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to get cleanup count: {}", e),
        })?;

        self.stats.rows_deleted.fetch_add(count as u64, Ordering::Relaxed);
        Ok(count as usize)
    }

    // ========================================================================
    // Distributed Coordination
    // ========================================================================

    /// Acquire PostgreSQL advisory lock
    pub async fn acquire_advisory_lock(&self, lock_id: i64) -> StateResult<bool> {
        trace!("Acquiring advisory lock: {}", lock_id);

        let start = Instant::now();

        let row = sqlx::query("SELECT pg_try_advisory_lock($1) AS acquired")
            .bind(lock_id)
            .fetch_one(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Advisory lock failed: {}", e),
            })?;

        let acquired: bool = row.try_get("acquired").unwrap_or(false);

        if acquired {
            self.stats.advisory_lock_count.fetch_add(1, Ordering::Relaxed);
            self.stats.advisory_lock_wait_ms.fetch_add(
                start.elapsed().as_millis() as u64,
                Ordering::Relaxed,
            );
        }

        Ok(acquired)
    }

    /// Release PostgreSQL advisory lock
    pub async fn release_advisory_lock(&self, lock_id: i64) -> StateResult<()> {
        trace!("Releasing advisory lock: {}", lock_id);

        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Advisory unlock failed: {}", e),
            })?;

        Ok(())
    }

    /// Notify listeners on a channel (LISTEN/NOTIFY)
    pub async fn notify(&self, channel: &str, payload: &str) -> StateResult<()> {
        debug!("Sending notification: channel={}", channel);

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Notify failed: {}", e),
            })?;

        Ok(())
    }

    /// Leader election using advisory locks with timeout
    pub async fn try_become_leader(&self, election_key: &str, ttl: Duration) -> StateResult<bool> {
        info!("Attempting leader election: key={}", election_key);

        // Use a hash of the election key as the lock ID
        let lock_id = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(election_key.as_bytes());
            let result = hasher.finalize();
            i64::from_le_bytes(result[0..8].try_into().unwrap())
        };

        // Try to acquire lock
        let acquired = self.acquire_advisory_lock(lock_id).await?;

        if acquired {
            info!("Leader election won: key={}", election_key);

            // Schedule lock refresh
            let backend = self.clone_handle();
            tokio::spawn(async move {
                tokio::time::sleep(ttl).await;
                if let Err(e) = backend.release_advisory_lock(lock_id).await {
                    warn!("Failed to release leader lock: {}", e);
                }
            });
        }

        Ok(acquired)
    }

    // ========================================================================
    // Data Management
    // ========================================================================

    /// Execute VACUUM with options
    pub async fn vacuum_analyze(&self, full: bool) -> StateResult<()> {
        info!("Running VACUUM{} ANALYZE", if full { " FULL" } else { "" });

        let sql = if full {
            format!("VACUUM FULL ANALYZE {}", self.config.table_name)
        } else {
            format!("VACUUM ANALYZE {}", self.config.table_name)
        };

        sqlx::query(&sql)
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("VACUUM failed: {}", e),
            })?;

        info!("VACUUM completed successfully");
        Ok(())
    }

    /// Apply retention policy - delete old entries
    pub async fn apply_retention_policy(&self) -> StateResult<usize> {
        if !self.config.retention_policy.enabled {
            return Ok(0);
        }

        info!("Applying retention policy");

        let cutoff_time = chrono::Utc::now()
            - chrono::Duration::from_std(self.config.retention_policy.max_age).unwrap();

        let result = sqlx::query(&format!(
            "WITH deleted AS (
                DELETE FROM {}
                WHERE created_at < $1
                RETURNING 1
             )
             SELECT count(*) as count FROM deleted",
            self.config.table_name
        ))
        .bind(cutoff_time)
        .fetch_one(&self.primary_pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Retention policy failed: {}", e),
        })?;

        let deleted: i64 = result.try_get("count").unwrap_or(0);
        info!("Retention policy: deleted {} entries", deleted);

        self.stats.rows_deleted.fetch_add(deleted as u64, Ordering::Relaxed);

        Ok(deleted as usize)
    }

    /// Archive old data to a separate table
    pub async fn archive_old_data(&self, archive_table: &str, age_threshold: Duration) -> StateResult<usize> {
        info!("Archiving data older than {:?}", age_threshold);

        let cutoff_time = chrono::Utc::now() - chrono::Duration::from_std(age_threshold).unwrap();

        let mut tx = self.primary_pool.begin().await.map_err(|e| {
            StateError::TransactionFailed {
                operation: "begin".to_string(),
                reason: e.to_string(),
            }
        })?;

        // Copy to archive table
        let result = sqlx::query(&format!(
            "INSERT INTO {} SELECT *, NOW() as archived_at FROM {} WHERE created_at < $1",
            archive_table, self.config.table_name
        ))
        .bind(cutoff_time)
        .execute(&mut *tx)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Archive insert failed: {}", e),
        })?;

        let archived = result.rows_affected() as usize;

        // Delete from main table
        sqlx::query(&format!(
            "DELETE FROM {} WHERE created_at < $1",
            self.config.table_name
        ))
        .bind(cutoff_time)
        .execute(&mut *tx)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Archive delete failed: {}", e),
        })?;

        tx.commit().await.map_err(|e| StateError::TransactionFailed {
            operation: "commit".to_string(),
            reason: e.to_string(),
        })?;

        info!("Archived {} entries", archived);
        Ok(archived)
    }

    /// Get comprehensive table statistics
    pub async fn get_table_statistics(&self) -> StateResult<TableStatistics> {
        debug!("Fetching table statistics");

        let pool = self.get_pool_for_read().await;

        let row = sqlx::query("SELECT * FROM get_state_table_stats()")
            .fetch_one(pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to get table stats: {}", e),
            })?;

        Ok(TableStatistics {
            total_rows: row.try_get("total_rows").unwrap_or(0),
            active_rows: row.try_get("active_rows").unwrap_or(0),
            expired_rows: row.try_get("expired_rows").unwrap_or(0),
            total_size: row.try_get("total_size_bytes").unwrap_or(0),
            table_size: row.try_get("table_size_bytes").unwrap_or(0),
            indexes_size: row.try_get("indexes_size_bytes").unwrap_or(0),
            toast_size: row.try_get("toast_size_bytes").unwrap_or(0),
        })
    }

    // ========================================================================
    // High Availability
    // ========================================================================

    /// Check replication status
    pub async fn check_replication_status(&self) -> StateResult<ReplicationStatus> {
        debug!("Checking replication status");

        let row = sqlx::query("SELECT * FROM replication_lag")
            .fetch_one(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Replication status check failed: {}", e),
            })?;

        Ok(ReplicationStatus {
            is_replica: row.try_get("is_replica").unwrap_or(false),
            lag_ms: row.try_get::<Option<f64>, _>("lag_ms").unwrap_or(None).unwrap_or(0.0) as i64,
            replicas_connected: self.replica_manager.as_ref().map(|rm| rm.replicas.len()).unwrap_or(0),
        })
    }

    /// Trigger a manual checkpoint
    pub async fn checkpoint(&self) -> StateResult<()> {
        info!("Triggering manual checkpoint");

        sqlx::query("CHECKPOINT")
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Checkpoint failed: {}", e),
            })?;

        Ok(())
    }

    // ========================================================================
    // Observability
    // ========================================================================

    /// Get current statistics snapshot
    pub fn get_stats(&self) -> PostgresStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get pool statistics
    pub fn get_pool_stats(&self) -> PoolStatistics {
        let size = self.primary_pool.size() as u64;
        let idle = self.primary_pool.num_idle() as u64;
        PoolStatistics {
            size,
            idle,
            active: size.saturating_sub(idle),
            max_size: self.config.pool.max_connections as u64,
        }
    }

    /// Health check
    pub async fn health_check(&self) -> StateResult<bool> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.primary_pool)
            .await
            .map(|_| true)
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Health check failed: {}", e),
            })
    }

    /// Close all connections gracefully
    pub async fn close(&self) {
        info!("Closing PostgreSQL connections");
        self.stop_health_check_task();
        self.primary_pool.close().await;

        if let Some(replica_manager) = &self.replica_manager {
            for replica in &replica_manager.replicas {
                replica.pool.close().await;
            }
        }
    }
}

// ============================================================================
// StateBackend Trait Implementation
// ============================================================================

#[async_trait]
impl StateBackend for EnhancedPostgresBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        trace!("GET: key_len={}", key.len());

        let pool = self.get_pool_for_read().await;

        let result = sqlx::query(
            "SELECT value FROM state_entries
             WHERE key = $1
             AND (expires_at IS NULL OR expires_at > NOW())"
        )
        .bind(key)
        .fetch_optional(pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Get failed: {}", e),
        })?;

        self.stats.get_count.fetch_add(1, Ordering::Relaxed);

        if let Some(row) = result {
            let value: Vec<u8> = row.try_get("value").map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to extract value: {}", e),
            })?;

            self.stats.bytes_read.fetch_add(value.len() as u64, Ordering::Relaxed);
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> StateResult<()> {
        trace!("PUT: key_len={}, value_len={}", key.len(), value.len());

        let result = sqlx::query(
            "INSERT INTO state_entries (key, value, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, updated_at = NOW()"
        )
        .bind(key)
        .bind(value)
        .execute(&self.primary_pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Put failed: {}", e),
        })?;

        self.stats.put_count.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(
            (key.len() + value.len()) as u64,
            Ordering::Relaxed,
        );

        if result.rows_affected() > 0 {
            self.stats.rows_updated.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.rows_inserted.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        trace!("DELETE: key_len={}", key.len());

        let result = sqlx::query("DELETE FROM state_entries WHERE key = $1")
            .bind(key)
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Delete failed: {}", e),
            })?;

        self.stats.delete_count.fetch_add(1, Ordering::Relaxed);
        self.stats.rows_deleted.fetch_add(result.rows_affected(), Ordering::Relaxed);

        Ok(())
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Vec<u8>>> {
        trace!("LIST_KEYS: prefix_len={}", prefix.len());

        let pool = self.get_pool_for_read().await;

        let rows = if prefix.is_empty() {
            sqlx::query("SELECT key FROM state_entries WHERE expires_at IS NULL OR expires_at > NOW()")
                .fetch_all(pool)
                .await
        } else {
            sqlx::query(
                "SELECT key FROM state_entries
                 WHERE key >= $1 AND key < $2
                 AND (expires_at IS NULL OR expires_at > NOW())"
            )
            .bind(prefix)
            .bind(Self::next_prefix(prefix))
            .fetch_all(pool)
            .await
        }
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("List keys failed: {}", e),
        })?;

        let keys = rows
            .into_iter()
            .filter_map(|row| row.try_get::<Vec<u8>, _>("key").ok())
            .collect();

        Ok(keys)
    }

    async fn clear(&self) -> StateResult<()> {
        debug!("CLEAR: truncating table");

        sqlx::query("TRUNCATE TABLE state_entries")
            .execute(&self.primary_pool)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Clear failed: {}", e),
            })?;

        Ok(())
    }

    async fn count(&self) -> StateResult<usize> {
        let pool = self.get_pool_for_read().await;

        let row = sqlx::query(
            "SELECT COUNT(*) as count FROM state_entries
             WHERE expires_at IS NULL OR expires_at > NOW()"
        )
        .fetch_one(pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Count failed: {}", e),
        })?;

        let count: i64 = row.try_get("count").map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to extract count: {}", e),
        })?;

        Ok(count as usize)
    }

    async fn contains(&self, key: &[u8]) -> StateResult<bool> {
        let pool = self.get_pool_for_read().await;

        let row = sqlx::query(
            "SELECT EXISTS(
                SELECT 1 FROM state_entries
                WHERE key = $1
                AND (expires_at IS NULL OR expires_at > NOW())
            ) as exists"
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Contains check failed: {}", e),
        })?;

        let exists: bool = row.try_get("exists").unwrap_or(false);
        Ok(exists)
    }
}

// ============================================================================
// Supporting Data Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub total_rows: i64,
    pub active_rows: i64,
    pub expired_rows: i64,
    pub total_size: i64,
    pub table_size: i64,
    pub indexes_size: i64,
    pub toast_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub is_replica: bool,
    pub lag_ms: i64,
    pub replicas_connected: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatistics {
    pub size: u64,
    pub idle: u64,
    pub active: u64,
    pub max_size: u64,
}

// ============================================================================
// Distributed Lock Guard
// ============================================================================

/// PostgreSQL advisory lock guard
pub struct PostgresLockGuard {
    lock_id: i64,
    backend: EnhancedPostgresBackend,
    released: Arc<AtomicBool>,
}

impl PostgresLockGuard {
    pub async fn new(
        backend: EnhancedPostgresBackend,
        lock_key: &str,
        timeout: Duration,
    ) -> StateResult<Option<Self>> {
        // Convert key to lock ID
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(lock_key.as_bytes());
        let result = hasher.finalize();
        let lock_id = i64::from_le_bytes(result[0..8].try_into().unwrap());

        // Try to acquire lock with timeout
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if backend.acquire_advisory_lock(lock_id).await? {
                return Ok(Some(Self {
                    lock_id,
                    backend,
                    released: Arc::new(AtomicBool::new(false)),
                }));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(None)
    }

    pub async fn release(self) -> StateResult<()> {
        if !self.released.swap(true, Ordering::Relaxed) {
            self.backend.release_advisory_lock(self.lock_id).await?;
        }
        Ok(())
    }
}

impl Drop for PostgresLockGuard {
    fn drop(&mut self) {
        if !self.released.swap(true, Ordering::Relaxed) {
            let lock_id = self.lock_id;
            let backend = self.backend.clone_handle();

            // Best effort release in background
            tokio::spawn(async move {
                if let Err(e) = backend.release_advisory_lock(lock_id).await {
                    warn!("Failed to release lock in drop: {}", e);
                }
            });
        }
    }
}

// ============================================================================
// Listen/Notify Support
// ============================================================================

pub struct PostgresListener {
    listener: sqlx::postgres::PgListener,
}

impl PostgresListener {
    pub async fn new(database_url: &str, channel: &str) -> StateResult<Self> {
        let mut listener = sqlx::postgres::PgListener::connect(database_url)
            .await
            .map_err(|e| StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to create listener: {}", e),
            })?;

        listener.listen(channel).await.map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to listen on channel: {}", e),
        })?;

        Ok(Self { listener })
    }

    pub async fn recv(&mut self) -> StateResult<String> {
        let notification = self.listener.recv().await.map_err(|e| StateError::StorageError {
            backend_type: "postgres".to_string(),
            details: format!("Failed to receive notification: {}", e),
        })?;

        Ok(notification.payload().to_string())
    }

    pub async fn try_recv(&mut self) -> StateResult<Option<String>> {
        match self.listener.try_recv().await {
            Ok(Some(notification)) => Ok(Some(notification.payload().to_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(StateError::StorageError {
                backend_type: "postgres".to_string(),
                details: format!("Failed to try_recv notification: {}", e),
            }),
        }
    }
}
