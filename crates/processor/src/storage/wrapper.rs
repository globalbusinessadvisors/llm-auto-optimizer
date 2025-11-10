//! Storage wrapper for dynamic dispatch
//!
//! This module provides an enum-based wrapper that allows storing different
//! storage backend types while maintaining the ability to use generic methods.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};

use super::error::StorageResult;
use super::postgresql::PostgreSQLStorage;
use super::redis::RedisStorage;
use super::sled_backend::SledStorage;
use super::traits::Storage;
use super::types::{
    Batch, Query, StorageBackend, StorageEntry, StorageHealth, StorageMetadata, StorageStats,
    Transaction,
};

/// Enum wrapper for different storage backend implementations
///
/// This allows dynamic dispatch while preserving the ability to call
/// generic methods that require `Self: Sized`.
#[derive(Debug)]
pub enum AnyStorage {
    PostgreSQL(PostgreSQLStorage),
    Redis(RedisStorage),
    Sled(SledStorage),
}

impl AnyStorage {
    /// Create from PostgreSQL storage
    pub fn from_postgresql(storage: PostgreSQLStorage) -> Self {
        Self::PostgreSQL(storage)
    }

    /// Create from Redis storage
    pub fn from_redis(storage: RedisStorage) -> Self {
        Self::Redis(storage)
    }

    /// Create from Sled storage
    pub fn from_sled(storage: SledStorage) -> Self {
        Self::Sled(storage)
    }
}

#[async_trait]
impl Storage for AnyStorage {
    fn backend(&self) -> StorageBackend {
        match self {
            Self::PostgreSQL(s) => s.backend(),
            Self::Redis(s) => s.backend(),
            Self::Sled(s) => s.backend(),
        }
    }

    async fn metadata(&self) -> StorageResult<StorageMetadata> {
        match self {
            Self::PostgreSQL(s) => s.metadata().await,
            Self::Redis(s) => s.metadata().await,
            Self::Sled(s) => s.metadata().await,
        }
    }

    async fn health_check(&self) -> StorageResult<StorageHealth> {
        match self {
            Self::PostgreSQL(s) => s.health_check().await,
            Self::Redis(s) => s.health_check().await,
            Self::Sled(s) => s.health_check().await,
        }
    }

    async fn stats(&self) -> StorageResult<StorageStats> {
        match self {
            Self::PostgreSQL(s) => s.stats().await,
            Self::Redis(s) => s.stats().await,
            Self::Sled(s) => s.stats().await,
        }
    }

    async fn insert<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.insert(table, key, value).await,
            Self::Redis(s) => s.insert(table, key, value).await,
            Self::Sled(s) => s.insert(table, key, value).await,
        }
    }

    async fn insert_with_ttl<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        ttl_seconds: i64,
    ) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.insert_with_ttl(table, key, value, ttl_seconds).await,
            Self::Redis(s) => s.insert_with_ttl(table, key, value, ttl_seconds).await,
            Self::Sled(s) => s.insert_with_ttl(table, key, value, ttl_seconds).await,
        }
    }

    async fn get<T: DeserializeOwned>(
        &self,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<StorageEntry<T>>> {
        match self {
            Self::PostgreSQL(s) => s.get(table, key).await,
            Self::Redis(s) => s.get(table, key).await,
            Self::Sled(s) => s.get(table, key).await,
        }
    }

    async fn update<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.update(table, key, value).await,
            Self::Redis(s) => s.update(table, key, value).await,
            Self::Sled(s) => s.update(table, key, value).await,
        }
    }

    async fn update_versioned<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        value: &T,
        expected_version: u64,
    ) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.update_versioned(table, key, value, expected_version).await,
            Self::Redis(s) => s.update_versioned(table, key, value, expected_version).await,
            Self::Sled(s) => s.update_versioned(table, key, value, expected_version).await,
        }
    }

    async fn delete(&self, table: &str, key: &str) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.delete(table, key).await,
            Self::Redis(s) => s.delete(table, key).await,
            Self::Sled(s) => s.delete(table, key).await,
        }
    }

    async fn exists(&self, table: &str, key: &str) -> StorageResult<bool> {
        match self {
            Self::PostgreSQL(s) => s.exists(table, key).await,
            Self::Redis(s) => s.exists(table, key).await,
            Self::Sled(s) => s.exists(table, key).await,
        }
    }

    async fn execute_batch(&self, batch: Batch) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.execute_batch(batch).await,
            Self::Redis(s) => s.execute_batch(batch).await,
            Self::Sled(s) => s.execute_batch(batch).await,
        }
    }

    async fn query<T: DeserializeOwned>(
        &self,
        query: Query,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        match self {
            Self::PostgreSQL(s) => s.query(query).await,
            Self::Redis(s) => s.query(query).await,
            Self::Sled(s) => s.query(query).await,
        }
    }

    async fn count(&self, query: Query) -> StorageResult<usize> {
        match self {
            Self::PostgreSQL(s) => s.count(query).await,
            Self::Redis(s) => s.count(query).await,
            Self::Sled(s) => s.count(query).await,
        }
    }

    async fn begin_transaction(&self) -> StorageResult<Transaction> {
        match self {
            Self::PostgreSQL(s) => s.begin_transaction().await,
            Self::Redis(s) => s.begin_transaction().await,
            Self::Sled(s) => s.begin_transaction().await,
        }
    }

    async fn commit_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.commit_transaction(transaction).await,
            Self::Redis(s) => s.commit_transaction(transaction).await,
            Self::Sled(s) => s.commit_transaction(transaction).await,
        }
    }

    async fn rollback_transaction(&self, transaction: Transaction) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.rollback_transaction(transaction).await,
            Self::Redis(s) => s.rollback_transaction(transaction).await,
            Self::Sled(s) => s.rollback_transaction(transaction).await,
        }
    }

    async fn scan_prefix(&self, table: &str, prefix: &str) -> StorageResult<Vec<String>> {
        match self {
            Self::PostgreSQL(s) => s.scan_prefix(table, prefix).await,
            Self::Redis(s) => s.scan_prefix(table, prefix).await,
            Self::Sled(s) => s.scan_prefix(table, prefix).await,
        }
    }

    async fn get_multi<T: DeserializeOwned>(
        &self,
        table: &str,
        keys: Vec<String>,
    ) -> StorageResult<Vec<Option<StorageEntry<T>>>> {
        match self {
            Self::PostgreSQL(s) => s.get_multi(table, keys).await,
            Self::Redis(s) => s.get_multi(table, keys).await,
            Self::Sled(s) => s.get_multi(table, keys).await,
        }
    }

    async fn delete_multi(&self, table: &str, keys: Vec<String>) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.delete_multi(table, keys).await,
            Self::Redis(s) => s.delete_multi(table, keys).await,
            Self::Sled(s) => s.delete_multi(table, keys).await,
        }
    }

    async fn compare_and_swap<T: Serialize + Send + Sync>(
        &self,
        table: &str,
        key: &str,
        expected_version: u64,
        new_value: &T,
    ) -> StorageResult<bool> {
        match self {
            Self::PostgreSQL(s) => s.compare_and_swap(table, key, expected_version, new_value).await,
            Self::Redis(s) => s.compare_and_swap(table, key, expected_version, new_value).await,
            Self::Sled(s) => s.compare_and_swap(table, key, expected_version, new_value).await,
        }
    }

    async fn get_modified_since<T: DeserializeOwned>(
        &self,
        table: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        match self {
            Self::PostgreSQL(s) => s.get_modified_since(table, since).await,
            Self::Redis(s) => s.get_modified_since(table, since).await,
            Self::Sled(s) => s.get_modified_since(table, since).await,
        }
    }

    async fn get_expiring_before<T: DeserializeOwned>(
        &self,
        table: &str,
        before: DateTime<Utc>,
    ) -> StorageResult<Vec<StorageEntry<T>>> {
        match self {
            Self::PostgreSQL(s) => s.get_expiring_before(table, before).await,
            Self::Redis(s) => s.get_expiring_before(table, before).await,
            Self::Sled(s) => s.get_expiring_before(table, before).await,
        }
    }

    async fn cleanup_expired(&self, table: &str) -> StorageResult<usize> {
        match self {
            Self::PostgreSQL(s) => s.cleanup_expired(table).await,
            Self::Redis(s) => s.cleanup_expired(table).await,
            Self::Sled(s) => s.cleanup_expired(table).await,
        }
    }

    async fn compact(&self) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.compact().await,
            Self::Redis(s) => s.compact().await,
            Self::Sled(s) => s.compact().await,
        }
    }

    async fn flush(&self) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.flush().await,
            Self::Redis(s) => s.flush().await,
            Self::Sled(s) => s.flush().await,
        }
    }

    async fn initialize(&mut self) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.initialize().await,
            Self::Redis(s) => s.initialize().await,
            Self::Sled(s) => s.initialize().await,
        }
    }

    async fn shutdown(&mut self) -> StorageResult<()> {
        match self {
            Self::PostgreSQL(s) => s.shutdown().await,
            Self::Redis(s) => s.shutdown().await,
            Self::Sled(s) => s.shutdown().await,
        }
    }
}
