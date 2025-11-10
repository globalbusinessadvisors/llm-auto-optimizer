//! Storage types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageBackend {
    /// PostgreSQL relational database
    PostgreSQL,
    /// Redis in-memory cache
    Redis,
    /// Sled embedded key-value store
    Sled,
}

impl StorageBackend {
    /// Get backend name
    pub fn name(&self) -> &'static str {
        match self {
            StorageBackend::PostgreSQL => "postgresql",
            StorageBackend::Redis => "redis",
            StorageBackend::Sled => "sled",
        }
    }

    /// Check if backend supports transactions
    pub fn supports_transactions(&self) -> bool {
        matches!(self, StorageBackend::PostgreSQL | StorageBackend::Sled)
    }

    /// Check if backend supports TTL
    pub fn supports_ttl(&self) -> bool {
        matches!(self, StorageBackend::Redis)
    }

    /// Check if backend supports pub/sub
    pub fn supports_pubsub(&self) -> bool {
        matches!(self, StorageBackend::Redis)
    }
}

/// Entry-level metadata (separate from StorageMetadata which is backend metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMetadata {
    /// Entry version
    pub version: u64,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Optional expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,
}

/// Storage entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEntry<T> {
    /// Entry key
    pub key: String,

    /// Entry value
    pub value: T,

    /// Entry version
    pub version: u64,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Optional expiration timestamp
    pub expires_at: Option<DateTime<Utc>>,

    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

impl<T> StorageEntry<T> {
    /// Create a new storage entry
    pub fn new(key: String, value: T) -> Self {
        let now = Utc::now();
        Self {
            key,
            value,
            version: 1,
            created_at: now,
            updated_at: now,
            expires_at: None,
            metadata: HashMap::new(),
        }
    }

    /// Create entry with TTL
    pub fn with_ttl(key: String, value: T, ttl_seconds: i64) -> Self {
        let now = Utc::now();
        Self {
            key,
            value,
            version: 1,
            created_at: now,
            updated_at: now,
            expires_at: Some(now + chrono::Duration::seconds(ttl_seconds)),
            metadata: HashMap::new(),
        }
    }

    /// Check if entry is expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    /// Update entry value and increment version
    pub fn update(&mut self, value: T) {
        self.value = value;
        self.version += 1;
        self.updated_at = Utc::now();
    }
}

/// Query builder for storage operations
#[derive(Debug, Clone, Default)]
pub struct Query {
    /// Table or collection name
    pub table: String,

    /// Filter conditions
    pub filters: Vec<Filter>,

    /// Sort order
    pub sort: Vec<Sort>,

    /// Limit
    pub limit: Option<usize>,

    /// Offset
    pub offset: Option<usize>,

    /// Fields to select (empty = all)
    pub select: Vec<String>,
}

impl Query {
    /// Create a new query for table
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            offset: None,
            select: Vec::new(),
        }
    }

    /// Add filter condition
    pub fn filter(mut self, field: impl Into<String>, op: Operator, value: Value) -> Self {
        self.filters.push(Filter {
            field: field.into(),
            operator: op,
            value,
        });
        self
    }

    /// Add sort order
    pub fn sort(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.sort.push(Sort {
            field: field.into(),
            direction,
        });
        self
    }

    /// Set limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Select specific fields
    pub fn select(mut self, fields: Vec<String>) -> Self {
        self.select = fields;
        self
    }
}

/// Filter condition
#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub operator: Operator,
    pub value: Value,
}

/// Comparison operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    NotIn,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
}

/// Query value
#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(DateTime<Utc>),
    List(Vec<Value>),
    Null,
}

/// Sort specification
#[derive(Debug, Clone)]
pub struct Sort {
    pub field: String,
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Transaction context
#[derive(Debug)]
pub struct Transaction {
    /// Transaction ID
    pub id: String,

    /// Operations in transaction
    pub operations: Vec<Operation>,

    /// Transaction state
    pub state: TransactionState,

    /// Created at
    pub created_at: DateTime<Utc>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            operations: Vec::new(),
            state: TransactionState::Active,
            created_at: Utc::now(),
        }
    }

    /// Add operation to transaction
    pub fn add_operation(&mut self, operation: Operation) {
        self.operations.push(operation);
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

/// Storage operation
#[derive(Debug, Clone)]
pub enum Operation {
    Insert { table: String, data: Vec<u8> },
    Update { table: String, key: String, data: Vec<u8> },
    Delete { table: String, key: String },
}

/// Batch operation
#[derive(Debug, Clone)]
pub struct Batch {
    /// Batch operations
    pub operations: Vec<Operation>,

    /// Batch size limit
    pub max_size: usize,
}

impl Batch {
    /// Create a new batch
    pub fn new(max_size: usize) -> Self {
        Self {
            operations: Vec::new(),
            max_size,
        }
    }

    /// Add operation to batch
    pub fn add(&mut self, operation: Operation) -> Result<(), &'static str> {
        if self.operations.len() >= self.max_size {
            return Err("Batch size limit exceeded");
        }
        self.operations.push(operation);
        Ok(())
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.operations.len() >= self.max_size
    }

    /// Get batch size
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

/// Storage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageStats {
    /// Total operations
    pub total_operations: u64,

    /// Successful operations
    pub successful_operations: u64,

    /// Failed operations
    pub failed_operations: u64,

    /// Total bytes read
    pub bytes_read: u64,

    /// Total bytes written
    pub bytes_written: u64,

    /// Average operation latency (ms)
    pub avg_latency_ms: f64,

    /// Peak operation latency (ms)
    pub peak_latency_ms: f64,

    /// Active connections
    pub active_connections: usize,

    /// Cache hit rate (0.0-1.0)
    pub cache_hit_rate: f64,

    /// Last update
    pub last_updated: DateTime<Utc>,
}

impl StorageStats {
    /// Create new stats
    pub fn new() -> Self {
        Self {
            last_updated: Utc::now(),
            ..Default::default()
        }
    }

    /// Record successful operation
    pub fn record_success(&mut self, latency_ms: f64, bytes: u64) {
        self.total_operations += 1;
        self.successful_operations += 1;
        self.bytes_read += bytes;

        // Update average latency
        let total = self.total_operations as f64;
        self.avg_latency_ms = ((self.avg_latency_ms * (total - 1.0)) + latency_ms) / total;

        // Update peak latency
        if latency_ms > self.peak_latency_ms {
            self.peak_latency_ms = latency_ms;
        }

        self.last_updated = Utc::now();
    }

    /// Record failed operation
    pub fn record_failure(&mut self) {
        self.total_operations += 1;
        self.failed_operations += 1;
        self.last_updated = Utc::now();
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.successful_operations as f64 / self.total_operations as f64
        }
    }
}

/// Storage health status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Storage metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    /// Backend type
    pub backend: StorageBackend,

    /// Version
    pub version: String,

    /// Capabilities
    pub capabilities: Vec<String>,

    /// Status
    pub status: StorageHealth,

    /// Last health check
    pub last_health_check: DateTime<Utc>,
}
