//! Backend composition layer.
//!
//! `DataBackends` chooses the configured SQL driver, builds low-level pools,
//! instantiates concrete repositories, and exposes app-facing read/write,
//! lease, lock, worker, and maintenance handles. Request-path repository SQL
//! belongs in `repository/*`; backend-owned maintenance SQL lives in focused
//! modules such as `stats`, `wallet`, and `system`. Pool/client primitives
//! belong in `driver/*`.

mod leases;
mod locks;
mod maintenance;
mod mysql;
mod postgres;
mod read;
mod redis;
mod sqlite;
mod stats;
mod stats_common;
mod system;
mod transactions;
mod wallet;
mod workers;
mod write;

use crate::maintenance::DatabasePoolSummary;
pub use leases::DataLeaseBackends;
pub use locks::DataLockBackends;
pub use mysql::MysqlBackend;
pub use postgres::PostgresBackend;
pub use read::DataReadRepositories;
pub use redis::RedisBackend;
pub use sqlite::SqliteBackend;
pub use transactions::DataTransactionBackends;
pub use workers::DataWorkerBackends;
pub use write::DataWriteRepositories;

use crate::database::DatabaseDriver;
use crate::{DataLayerConfig, DataLayerError};

#[derive(Clone, Copy)]
enum SqlBackendRef<'a> {
    Postgres(&'a PostgresBackend),
    Mysql(&'a MysqlBackend),
    Sqlite(&'a SqliteBackend),
}

#[derive(Debug, Clone, Default)]
pub struct DataBackends {
    config: DataLayerConfig,
    postgres: Option<PostgresBackend>,
    mysql: Option<MysqlBackend>,
    sqlite: Option<SqliteBackend>,
    redis: Option<RedisBackend>,
    leases: DataLeaseBackends,
    locks: DataLockBackends,
    read: DataReadRepositories,
    transactions: DataTransactionBackends,
    workers: DataWorkerBackends,
    write: DataWriteRepositories,
}

fn summarize_pool(
    driver: DatabaseDriver,
    pool_size: usize,
    idle: usize,
    max_connections: u32,
) -> DatabasePoolSummary {
    let max_connections = max_connections.max(1);
    let checked_out = pool_size.saturating_sub(idle);
    let usage_rate = checked_out as f64 / f64::from(max_connections) * 100.0;

    DatabasePoolSummary {
        driver,
        checked_out,
        pool_size,
        idle,
        max_connections,
        usage_rate,
    }
}

impl DataBackends {
    fn sql_backend(&self) -> Option<SqlBackendRef<'_>> {
        self.postgres
            .as_ref()
            .map(SqlBackendRef::Postgres)
            .or_else(|| self.mysql.as_ref().map(SqlBackendRef::Mysql))
            .or_else(|| self.sqlite.as_ref().map(SqlBackendRef::Sqlite))
    }

    pub fn from_config(config: DataLayerConfig) -> Result<Self, DataLayerError> {
        config.validate()?;

        let database = config.effective_database();
        let postgres = match database.clone() {
            Some(database) if database.driver == DatabaseDriver::Postgres => Some(
                PostgresBackend::from_config(database.to_postgres_config()?)?,
            ),
            _ => None,
        };
        let mysql = match database.clone() {
            Some(database) if database.driver == DatabaseDriver::Mysql => {
                Some(MysqlBackend::from_config(database)?)
            }
            _ => None,
        };
        let sqlite = match database {
            Some(database) if database.driver == DatabaseDriver::Sqlite => {
                Some(SqliteBackend::from_config(database)?)
            }
            _ => None,
        };
        let redis = config
            .redis
            .clone()
            .map(RedisBackend::from_config)
            .transpose()?;
        let leases = DataLeaseBackends::from_postgres(postgres.as_ref())?;
        let locks = DataLockBackends::from_redis(redis.as_ref())?;
        let read =
            DataReadRepositories::from_backends(postgres.as_ref(), mysql.as_ref(), sqlite.as_ref());
        let transactions = DataTransactionBackends::from_postgres(postgres.as_ref());
        let workers = DataWorkerBackends::from_redis(redis.as_ref())?;
        let write = DataWriteRepositories::from_backends(
            postgres.as_ref(),
            mysql.as_ref(),
            sqlite.as_ref(),
        );

        Ok(Self {
            config,
            postgres,
            mysql,
            sqlite,
            redis,
            leases,
            locks,
            read,
            transactions,
            workers,
            write,
        })
    }

    pub fn config(&self) -> &DataLayerConfig {
        &self.config
    }

    pub fn postgres(&self) -> Option<&PostgresBackend> {
        self.postgres.as_ref()
    }

    pub fn database_driver(&self) -> Option<DatabaseDriver> {
        self.config
            .effective_database()
            .map(|database| database.driver)
    }

    pub fn mysql(&self) -> Option<&MysqlBackend> {
        self.mysql.as_ref()
    }

    pub fn sqlite(&self) -> Option<&SqliteBackend> {
        self.sqlite.as_ref()
    }

    pub fn redis(&self) -> Option<&RedisBackend> {
        self.redis.as_ref()
    }

    pub fn read(&self) -> &DataReadRepositories {
        &self.read
    }

    pub fn leases(&self) -> &DataLeaseBackends {
        &self.leases
    }

    pub fn locks(&self) -> &DataLockBackends {
        &self.locks
    }

    pub fn transactions(&self) -> &DataTransactionBackends {
        &self.transactions
    }

    pub fn workers(&self) -> &DataWorkerBackends {
        &self.workers
    }

    pub fn write(&self) -> &DataWriteRepositories {
        &self.write
    }

    pub fn has_runtime_backends(&self) -> bool {
        self.postgres.is_some()
            || self.mysql.is_some()
            || self.sqlite.is_some()
            || self.redis.is_some()
            || self.leases.has_any()
            || self.locks.has_any()
            || self.read.has_any()
            || self.transactions.has_any()
            || self.workers.has_any()
            || self.write.has_any()
    }
}

#[cfg(test)]
mod tests {
    use super::DataBackends;
    use crate::{
        driver::postgres::PostgresPoolConfig, DataLayerConfig, DatabaseDriver, SqlDatabaseConfig,
        SqlPoolConfig,
    };

    #[test]
    fn builds_empty_backends_from_default_config() {
        let backends = DataBackends::from_config(DataLayerConfig::default())
            .expect("empty config should be accepted");

        assert!(!backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.mysql().is_none());
        assert!(backends.sqlite().is_none());
        assert!(backends.redis().is_none());
        assert!(backends.leases().postgres().is_none());
        assert!(backends.locks().redis().is_none());
        assert!(backends.read().auth_api_keys().is_none());
        assert!(backends.read().auth_modules().is_none());
        assert!(backends.read().billing().is_none());
        assert!(backends.read().gemini_file_mappings().is_none());
        assert!(backends.read().global_models().is_none());
        assert!(backends.read().management_tokens().is_none());
        assert!(backends.read().oauth_providers().is_none());
        assert!(backends.read().proxy_nodes().is_none());
        assert!(backends.read().minimal_candidate_selection().is_none());
        assert!(backends.read().request_candidates().is_none());
        assert!(backends.read().provider_catalog().is_none());
        assert!(backends.read().usage().is_none());
        assert!(backends.read().video_tasks().is_none());
        assert!(backends.transactions().postgres().is_none());
        assert!(backends.workers().redis().is_none());
        assert!(backends.write().settlement().is_none());
        assert!(backends.write().usage().is_none());
    }

    #[tokio::test]
    async fn builds_postgres_backend_from_config() {
        let backends = DataBackends::from_config(DataLayerConfig {
            database: None,
            postgres: Some(PostgresPoolConfig {
                database_url: "postgres://localhost/aether".to_string(),
                min_connections: 1,
                max_connections: 4,
                acquire_timeout_ms: 1_000,
                idle_timeout_ms: 5_000,
                max_lifetime_ms: 30_000,
                statement_cache_capacity: 64,
                require_ssl: false,
            }),
            redis: None,
        })
        .expect("postgres backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_some());
        assert!(backends.mysql().is_none());
        assert!(backends.sqlite().is_none());
        assert!(backends.leases().postgres().is_some());
        assert!(backends.read().auth_api_keys().is_some());
        assert!(backends.read().auth_modules().is_some());
        assert!(backends.read().billing().is_some());
        assert!(backends.read().gemini_file_mappings().is_some());
        assert!(backends.read().global_models().is_some());
        assert!(backends.read().management_tokens().is_some());
        assert!(backends.read().minimal_candidate_selection().is_some());
        assert!(backends.read().oauth_providers().is_some());
        assert!(backends.read().proxy_nodes().is_some());
        assert!(backends.read().minimal_candidate_selection().is_some());
        assert!(backends.read().request_candidates().is_some());
        assert!(backends.read().provider_catalog().is_some());
        assert!(backends.read().provider_quotas().is_some());
        assert!(backends.read().usage().is_some());
        assert!(backends.read().video_tasks().is_some());
        assert!(backends.read().wallets().is_some());
        assert!(backends.transactions().postgres().is_some());
        assert!(backends.write().auth_modules().is_some());
        assert!(backends.write().gemini_file_mappings().is_some());
        assert!(backends.write().management_tokens().is_some());
        assert!(backends.write().oauth_providers().is_some());
        assert!(backends.write().proxy_nodes().is_some());
        assert!(backends.write().provider_catalog().is_some());
        assert!(backends.write().provider_quotas().is_some());
        assert!(backends.write().settlement().is_some());
        assert!(backends.write().usage().is_some());
        assert!(backends.write().wallets().is_some());
        assert!(backends.config().effective_database().is_some());
    }

    #[tokio::test]
    async fn builds_mysql_backend_from_database_config_with_first_core_repository() {
        let backends = DataBackends::from_config(DataLayerConfig {
            database: Some(SqlDatabaseConfig {
                driver: DatabaseDriver::Mysql,
                url: "mysql://user:pass@localhost:3306/aether".to_string(),
                pool: SqlPoolConfig::default(),
            }),
            postgres: None,
            redis: None,
        })
        .expect("mysql backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.mysql().is_some());
        assert!(backends.sqlite().is_none());
        assert!(backends.read().has_any());
        assert!(backends.read().announcements().is_some());
        assert!(backends.read().auth_api_keys().is_some());
        assert!(backends.read().auth_modules().is_some());
        assert!(backends.read().billing().is_some());
        assert!(backends.read().gemini_file_mappings().is_some());
        assert!(backends.read().global_models().is_some());
        assert!(backends.read().management_tokens().is_some());
        assert!(backends.read().minimal_candidate_selection().is_some());
        assert!(backends.read().oauth_providers().is_some());
        assert!(backends.read().provider_catalog().is_some());
        assert!(backends.read().provider_quotas().is_some());
        assert!(backends.read().proxy_nodes().is_some());
        assert!(backends.read().request_candidates().is_some());
        assert!(backends.read().users().is_some());
        assert!(backends.read().video_tasks().is_some());
        assert!(backends.has_stats_hourly_aggregation_backend());
        assert!(backends.has_stats_daily_aggregation_backend());
        assert!(backends.write().has_any());
        assert!(backends.write().announcements().is_some());
        assert!(backends.write().auth_api_keys().is_some());
        assert!(backends.write().auth_modules().is_some());
        assert!(backends.write().gemini_file_mappings().is_some());
        assert!(backends.write().global_models().is_some());
        assert!(backends.write().management_tokens().is_some());
        assert!(backends.write().oauth_providers().is_some());
        assert!(backends.write().proxy_nodes().is_some());
        assert!(backends.write().provider_catalog().is_some());
        assert!(backends.write().provider_quotas().is_some());
        assert!(backends.write().request_candidates().is_some());
        assert!(backends.write().video_tasks().is_some());
        assert!(backends.write().wallets().is_some());
        assert!(backends.config().effective_database().is_some());
    }

    #[tokio::test]
    async fn builds_sqlite_backend_from_database_config_with_first_core_repository() {
        let backends = DataBackends::from_config(DataLayerConfig {
            database: Some(SqlDatabaseConfig {
                driver: DatabaseDriver::Sqlite,
                url: "sqlite://./data/aether.db".to_string(),
                pool: SqlPoolConfig::default(),
            }),
            postgres: None,
            redis: None,
        })
        .expect("sqlite backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.mysql().is_none());
        assert!(backends.sqlite().is_some());
        assert!(backends.read().has_any());
        assert!(backends.read().announcements().is_some());
        assert!(backends.read().auth_api_keys().is_some());
        assert!(backends.read().auth_modules().is_some());
        assert!(backends.read().billing().is_some());
        assert!(backends.read().gemini_file_mappings().is_some());
        assert!(backends.read().global_models().is_some());
        assert!(backends.read().management_tokens().is_some());
        assert!(backends.read().oauth_providers().is_some());
        assert!(backends.read().provider_catalog().is_some());
        assert!(backends.read().provider_quotas().is_some());
        assert!(backends.read().proxy_nodes().is_some());
        assert!(backends.read().request_candidates().is_some());
        assert!(backends.read().users().is_some());
        assert!(backends.read().video_tasks().is_some());
        assert!(backends.has_stats_hourly_aggregation_backend());
        assert!(backends.has_stats_daily_aggregation_backend());
        assert!(backends.write().has_any());
        assert!(backends.write().announcements().is_some());
        assert!(backends.write().auth_api_keys().is_some());
        assert!(backends.write().auth_modules().is_some());
        assert!(backends.write().gemini_file_mappings().is_some());
        assert!(backends.write().global_models().is_some());
        assert!(backends.write().management_tokens().is_some());
        assert!(backends.write().oauth_providers().is_some());
        assert!(backends.write().proxy_nodes().is_some());
        assert!(backends.write().provider_catalog().is_some());
        assert!(backends.write().provider_quotas().is_some());
        assert!(backends.write().request_candidates().is_some());
        assert!(backends.write().video_tasks().is_some());
        assert!(backends.write().wallets().is_some());
        assert!(backends.config().effective_database().is_some());
    }

    #[test]
    fn builds_redis_backend_from_config() {
        let backends = DataBackends::from_config(DataLayerConfig {
            database: None,
            postgres: None,
            redis: Some(crate::driver::redis::RedisClientConfig {
                url: "redis://127.0.0.1/0".to_string(),
                key_prefix: Some("aether".to_string()),
            }),
        })
        .expect("redis backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.mysql().is_none());
        assert!(backends.sqlite().is_none());
        assert!(backends.redis().is_some());
        assert!(backends.leases().postgres().is_none());
        assert!(backends.locks().redis().is_some());
        assert!(backends.workers().redis().is_some());
        assert!(backends.read().auth_api_keys().is_none());
        assert!(backends.read().auth_modules().is_none());
        assert!(backends.read().global_models().is_none());
        assert!(backends.read().oauth_providers().is_none());
        assert!(backends.transactions().postgres().is_none());
        assert!(backends.write().settlement().is_none());
        assert!(backends.write().usage().is_none());
        assert!(backends.config().redis.is_some());
    }
}
