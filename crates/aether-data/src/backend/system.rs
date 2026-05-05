use futures_util::TryStreamExt;
use sqlx::Row;

use super::{MysqlBackend, PostgresBackend, SqliteBackend};
use crate::error::{SqlResultExt, SqlxResultExt};
use crate::repository::system::{AdminSystemStats, StoredSystemConfigEntry};
use crate::DataLayerError;

const POSTGRES_FIND_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
SELECT value
FROM system_configs
WHERE key = $1
LIMIT 1
"#;

const POSTGRES_UPSERT_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, NOW(), NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    description = COALESCE(EXCLUDED.description, system_configs.description),
    updated_at = NOW()
RETURNING value
"#;

const POSTGRES_LIST_SYSTEM_CONFIG_ENTRIES_SQL: &str = r#"
SELECT
    key,
    value,
    description,
    EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM system_configs
ORDER BY key ASC
"#;

const POSTGRES_UPSERT_SYSTEM_CONFIG_ENTRY_SQL: &str = r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, NOW(), NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    description = COALESCE(EXCLUDED.description, system_configs.description),
    updated_at = NOW()
RETURNING
    key,
    value,
    description,
    EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
"#;

const POSTGRES_DELETE_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
DELETE FROM system_configs
WHERE key = $1
"#;

const POSTGRES_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active IS TRUE) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM usage) AS total_requests
"#;

const MYSQL_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active = 1) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM `usage`) AS total_requests
"#;

const SQLITE_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active = 1) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM "usage") AS total_requests
"#;

impl PostgresBackend {
    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(POSTGRES_FIND_SYSTEM_CONFIG_VALUE_SQL)
            .bind(key)
            .fetch_optional(self.pool())
            .await
            .map_postgres_err()?;
        row.map(|row| row.try_get("value"))
            .transpose()
            .map_postgres_err()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        let row = sqlx::query(POSTGRES_UPSERT_SYSTEM_CONFIG_VALUE_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(key)
            .bind(value)
            .bind(description)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        row.try_get("value").map_postgres_err()
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let mut rows = sqlx::query(POSTGRES_LIST_SYSTEM_CONFIG_ENTRIES_SQL).fetch(self.pool());
        let mut entries = Vec::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            entries.push(StoredSystemConfigEntry {
                key: row.try_get("key").map_postgres_err()?,
                value: row.try_get("value").map_postgres_err()?,
                description: row.try_get("description").map_postgres_err()?,
                updated_at_unix_secs: row
                    .try_get::<Option<i64>, _>("updated_at_unix_secs")
                    .map_postgres_err()?
                    .map(|value| value.max(0) as u64),
            });
        }
        Ok(entries)
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let row = sqlx::query(POSTGRES_UPSERT_SYSTEM_CONFIG_ENTRY_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(key)
            .bind(value)
            .bind(description)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        Ok(StoredSystemConfigEntry {
            key: row.try_get("key").map_postgres_err()?,
            value: row.try_get("value").map_postgres_err()?,
            description: row.try_get("description").map_postgres_err()?,
            updated_at_unix_secs: row
                .try_get::<Option<i64>, _>("updated_at_unix_secs")
                .map_postgres_err()?
                .map(|value| value.max(0) as u64),
        })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(POSTGRES_DELETE_SYSTEM_CONFIG_VALUE_SQL)
            .bind(key)
            .execute(self.pool())
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(POSTGRES_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        postgres_admin_system_stats(row)
    }
}

impl MysqlBackend {
    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT value
FROM system_configs
WHERE `key` = ?
LIMIT 1
"#,
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_sql_err()?;

        row.map(|row| {
            row.try_get("value")
                .map_sql_err()
                .and_then(parse_json_value)
        })
        .transpose()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        Ok(self
            .upsert_system_config_entry(key, value, description)
            .await?
            .value)
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT `key`, value, description, updated_at
FROM system_configs
ORDER BY `key` ASC
"#,
        )
        .fetch_all(self.pool())
        .await
        .map_sql_err()?;

        rows.into_iter()
            .map(|row| {
                Ok(StoredSystemConfigEntry {
                    key: row.try_get("key").map_sql_err()?,
                    value: parse_json_value(row.try_get("value").map_sql_err()?)?,
                    description: row.try_get("description").map_sql_err()?,
                    updated_at_unix_secs: row
                        .try_get::<Option<i64>, _>("updated_at")
                        .map_sql_err()?
                        .map(|value| value.max(0) as u64),
                })
            })
            .collect()
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let now = current_unix_secs();
        let serialized = serialize_json_value(value)?;
        sqlx::query(
            r#"
INSERT INTO system_configs (id, `key`, value, description, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    value = VALUES(value),
    description = COALESCE(VALUES(description), description),
    updated_at = VALUES(updated_at)
"#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(key)
        .bind(serialized)
        .bind(description)
        .bind(now as i64)
        .bind(now as i64)
        .execute(self.pool())
        .await
        .map_sql_err()?;

        self.list_system_config_entries()
            .await?
            .into_iter()
            .find(|entry| entry.key == key)
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue(format!(
                    "system config key '{key}' missing after mysql upsert"
                ))
            })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(
            r#"
DELETE FROM system_configs
WHERE `key` = ?
"#,
        )
        .bind(key)
        .execute(self.pool())
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(MYSQL_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_sql_err()?;
        mysql_admin_system_stats(row)
    }
}

impl SqliteBackend {
    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT value
FROM system_configs
WHERE key = ?
LIMIT 1
"#,
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_sql_err()?;

        row.map(|row| {
            row.try_get("value")
                .map_sql_err()
                .and_then(parse_json_value)
        })
        .transpose()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        Ok(self
            .upsert_system_config_entry(key, value, description)
            .await?
            .value)
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT key, value, description, updated_at
FROM system_configs
ORDER BY key ASC
"#,
        )
        .fetch_all(self.pool())
        .await
        .map_sql_err()?;

        rows.into_iter()
            .map(|row| {
                Ok(StoredSystemConfigEntry {
                    key: row.try_get("key").map_sql_err()?,
                    value: parse_json_value(row.try_get("value").map_sql_err()?)?,
                    description: row.try_get("description").map_sql_err()?,
                    updated_at_unix_secs: row
                        .try_get::<Option<i64>, _>("updated_at")
                        .map_sql_err()?
                        .map(|value| value.max(0) as u64),
                })
            })
            .collect()
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let now = current_unix_secs();
        let serialized = serialize_json_value(value)?;
        sqlx::query(
            r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (key) DO UPDATE
SET value = excluded.value,
    description = COALESCE(excluded.description, system_configs.description),
    updated_at = excluded.updated_at
"#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(key)
        .bind(serialized)
        .bind(description)
        .bind(now as i64)
        .bind(now as i64)
        .execute(self.pool())
        .await
        .map_sql_err()?;

        self.list_system_config_entries()
            .await?
            .into_iter()
            .find(|entry| entry.key == key)
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue(format!(
                    "system config key '{key}' missing after sqlite upsert"
                ))
            })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(
            r#"
DELETE FROM system_configs
WHERE key = ?
"#,
        )
        .bind(key)
        .execute(self.pool())
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(SQLITE_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_sql_err()?;
        sqlite_admin_system_stats(row)
    }
}

fn current_unix_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn serialize_json_value(value: &serde_json::Value) -> Result<String, DataLayerError> {
    serde_json::to_string(value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid system config JSON value: {err}"))
    })
}

fn parse_json_value(value: String) -> Result<serde_json::Value, DataLayerError> {
    serde_json::from_str(&value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid system config JSON value: {err}"))
    })
}

fn postgres_admin_system_stats(
    row: sqlx::postgres::PgRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row
            .try_get::<i64, _>("total_users")
            .map_postgres_err()?
            .max(0) as u64,
        active_users: row
            .try_get::<i64, _>("active_users")
            .map_postgres_err()?
            .max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_postgres_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_postgres_err()?
            .max(0) as u64,
    })
}

fn mysql_admin_system_stats(
    row: sqlx::mysql::MySqlRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row.try_get::<i64, _>("total_users").map_sql_err()?.max(0) as u64,
        active_users: row.try_get::<i64, _>("active_users").map_sql_err()?.max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_sql_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_sql_err()?
            .max(0) as u64,
    })
}

fn sqlite_admin_system_stats(
    row: sqlx::sqlite::SqliteRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row.try_get::<i64, _>("total_users").map_sql_err()?.max(0) as u64,
        active_users: row.try_get::<i64, _>("active_users").map_sql_err()?.max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_sql_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_sql_err()?
            .max(0) as u64,
    })
}
