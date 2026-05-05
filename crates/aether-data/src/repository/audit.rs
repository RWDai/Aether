use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Row};

use crate::driver::mysql::MysqlPool;
use crate::driver::postgres::PostgresPool;
use crate::driver::sqlite::SqlitePool;
use crate::error::{SqlResultExt, SqlxResultExt};
use crate::repository::auth::ResolvedAuthApiKeySnapshot;
use crate::repository::candidates::DecisionTrace;
use crate::repository::usage::StoredRequestUsageAudit;
use crate::DataLayerError;

const SUSPICIOUS_EVENT_TYPES: &[&str] = &[
    "suspicious_activity",
    "unauthorized_access",
    "login_failed",
    "request_rate_limited",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditLogListQuery {
    pub cutoff_unix_secs: u64,
    pub username_pattern: Option<String>,
    pub event_type: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredAdminAuditLog {
    pub id: String,
    pub event_type: String,
    pub user_id: Option<String>,
    pub user_email: Option<String>,
    pub user_username: Option<String>,
    pub description: Option<String>,
    pub ip_address: Option<String>,
    pub status_code: Option<i32>,
    pub error_message: Option<String>,
    pub metadata: Option<Value>,
    pub created_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredSuspiciousActivity {
    pub id: String,
    pub event_type: String,
    pub user_id: Option<String>,
    pub description: Option<String>,
    pub ip_address: Option<String>,
    pub metadata: Option<Value>,
    pub created_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredUserAuditLog {
    pub id: String,
    pub event_type: String,
    pub description: Option<String>,
    pub ip_address: Option<String>,
    pub status_code: Option<i32>,
    pub created_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredAdminAuditLogPage {
    pub items: Vec<StoredAdminAuditLog>,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredUserAuditLogPage {
    pub items: Vec<StoredUserAuditLog>,
    pub total: u64,
}

#[async_trait]
pub trait AuditLogReadRepository: Send + Sync {
    async fn list_admin_audit_logs(
        &self,
        query: &AuditLogListQuery,
    ) -> Result<StoredAdminAuditLogPage, DataLayerError>;

    async fn list_admin_suspicious_activities(
        &self,
        cutoff_unix_secs: u64,
    ) -> Result<Vec<StoredSuspiciousActivity>, DataLayerError>;

    async fn read_admin_user_behavior_event_counts(
        &self,
        user_id: &str,
        cutoff_unix_secs: u64,
    ) -> Result<std::collections::BTreeMap<String, u64>, DataLayerError>;

    async fn list_user_audit_logs(
        &self,
        user_id: &str,
        query: &AuditLogListQuery,
    ) -> Result<StoredUserAuditLogPage, DataLayerError>;

    async fn delete_audit_logs_before(
        &self,
        cutoff_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError>;
}

#[derive(Debug, Clone)]
pub struct PostgresAuditLogReadRepository {
    pool: PostgresPool,
}

impl PostgresAuditLogReadRepository {
    pub fn new(pool: PostgresPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AuditLogReadRepository for PostgresAuditLogReadRepository {
    async fn list_admin_audit_logs(
        &self,
        query: &AuditLogListQuery,
    ) -> Result<StoredAdminAuditLogPage, DataLayerError> {
        let cutoff_time = postgres_cutoff_time(query.cutoff_unix_secs);
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= $1
  AND ($2::text IS NULL OR u.username ILIKE $2 ESCAPE '\')
  AND ($3::text IS NULL OR a.event_type = $3)
"#,
        )
        .bind(cutoff_time)
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_postgres_err()?;

        let mut rows = sqlx::query(
            r#"
SELECT
  a.id,
  a.event_type,
  a.user_id,
  u.email AS user_email,
  u.username AS user_username,
  a.description,
  a.ip_address,
  a.status_code,
  a.error_message,
  a.event_metadata AS metadata,
  a.created_at
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= $1
  AND ($2::text IS NULL OR u.username ILIKE $2 ESCAPE '\')
  AND ($3::text IS NULL OR a.event_type = $3)
ORDER BY a.created_at DESC
LIMIT $4 OFFSET $5
"#,
        )
        .bind(cutoff_time)
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .bind(i64::try_from(query.limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(query.offset).unwrap_or(i64::MAX))
        .fetch(&self.pool);

        let mut items = Vec::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            items.push(map_postgres_admin_audit_log_row(&row)?);
        }

        Ok(StoredAdminAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn list_admin_suspicious_activities(
        &self,
        cutoff_unix_secs: u64,
    ) -> Result<Vec<StoredSuspiciousActivity>, DataLayerError> {
        let cutoff_time = postgres_cutoff_time(cutoff_unix_secs);
        let mut rows = sqlx::query(
            r#"
SELECT
  id,
  event_type,
  user_id,
  description,
  ip_address,
  event_metadata AS metadata,
  created_at
FROM audit_logs
WHERE created_at >= $1
  AND event_type = ANY($2)
ORDER BY created_at DESC
LIMIT 100
"#,
        )
        .bind(cutoff_time)
        .bind(SUSPICIOUS_EVENT_TYPES.to_vec())
        .fetch(&self.pool);

        let mut items = Vec::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            items.push(map_postgres_suspicious_activity_row(&row)?);
        }
        Ok(items)
    }

    async fn read_admin_user_behavior_event_counts(
        &self,
        user_id: &str,
        cutoff_unix_secs: u64,
    ) -> Result<std::collections::BTreeMap<String, u64>, DataLayerError> {
        let cutoff_time = postgres_cutoff_time(cutoff_unix_secs);
        let mut rows = sqlx::query(
            r#"
SELECT event_type, COUNT(*)::bigint AS count
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
GROUP BY event_type
"#,
        )
        .bind(user_id)
        .bind(cutoff_time)
        .fetch(&self.pool);

        let mut counts = std::collections::BTreeMap::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            if let Ok((event_type, count)) = event_count_from_postgres_row(&row) {
                counts.insert(event_type, count);
            }
        }
        Ok(counts)
    }

    async fn list_user_audit_logs(
        &self,
        user_id: &str,
        query: &AuditLogListQuery,
    ) -> Result<StoredUserAuditLogPage, DataLayerError> {
        let cutoff_time = postgres_cutoff_time(query.cutoff_unix_secs);
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
  AND ($3::text IS NULL OR event_type = $3)
"#,
        )
        .bind(user_id)
        .bind(cutoff_time)
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_postgres_err()?;

        let mut rows = sqlx::query(
            r#"
SELECT id, event_type, description, ip_address, status_code, created_at
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
  AND ($3::text IS NULL OR event_type = $3)
ORDER BY created_at DESC
LIMIT $4 OFFSET $5
"#,
        )
        .bind(user_id)
        .bind(cutoff_time)
        .bind(query.event_type.as_deref())
        .bind(i64::try_from(query.limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(query.offset).unwrap_or(i64::MAX))
        .fetch(&self.pool);

        let mut items = Vec::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            items.push(map_postgres_user_audit_log_row(&row)?);
        }

        Ok(StoredUserAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn delete_audit_logs_before(
        &self,
        cutoff_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        let deleted = sqlx::query(
            r#"
WITH doomed AS (
    SELECT id
    FROM audit_logs
    WHERE created_at < $1
    ORDER BY created_at ASC, id ASC
    LIMIT $2
)
DELETE FROM audit_logs AS audit
USING doomed
WHERE audit.id = doomed.id
"#,
        )
        .bind(postgres_cutoff_time(cutoff_unix_secs))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .map_postgres_err()?
        .rows_affected();
        Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
    }
}

#[derive(Debug, Clone)]
pub struct MysqlAuditLogReadRepository {
    pool: MysqlPool,
}

impl MysqlAuditLogReadRepository {
    pub fn new(pool: MysqlPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AuditLogReadRepository for MysqlAuditLogReadRepository {
    async fn list_admin_audit_logs(
        &self,
        query: &AuditLogListQuery,
    ) -> Result<StoredAdminAuditLogPage, DataLayerError> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= ?
  AND (? IS NULL OR LOWER(u.username) LIKE LOWER(?) ESCAPE '\\')
  AND (? IS NULL OR a.event_type = ?)
"#,
        )
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.username_pattern.as_deref())
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT
  a.id,
  a.event_type,
  a.user_id,
  u.email AS user_email,
  u.username AS user_username,
  a.description,
  a.ip_address,
  a.status_code,
  a.error_message,
  a.event_metadata AS metadata,
  a.created_at
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= ?
  AND (? IS NULL OR LOWER(u.username) LIKE LOWER(?) ESCAPE '\\')
  AND (? IS NULL OR a.event_type = ?)
ORDER BY a.created_at DESC
LIMIT ? OFFSET ?
"#,
        )
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.username_pattern.as_deref())
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let items = rows
            .iter()
            .map(map_mysql_admin_audit_log_row)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StoredAdminAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn list_admin_suspicious_activities(
        &self,
        cutoff_unix_secs: u64,
    ) -> Result<Vec<StoredSuspiciousActivity>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT id, event_type, user_id, description, ip_address, event_metadata AS metadata, created_at
FROM audit_logs
WHERE created_at >= ?
  AND event_type IN (?, ?, ?, ?)
ORDER BY created_at DESC
LIMIT 100
"#,
        )
        .bind(cutoff_unix_secs as i64)
        .bind(SUSPICIOUS_EVENT_TYPES[0])
        .bind(SUSPICIOUS_EVENT_TYPES[1])
        .bind(SUSPICIOUS_EVENT_TYPES[2])
        .bind(SUSPICIOUS_EVENT_TYPES[3])
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        rows.iter().map(map_mysql_suspicious_activity_row).collect()
    }

    async fn read_admin_user_behavior_event_counts(
        &self,
        user_id: &str,
        cutoff_unix_secs: u64,
    ) -> Result<std::collections::BTreeMap<String, u64>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT event_type, COUNT(*) AS count
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
GROUP BY event_type
"#,
        )
        .bind(user_id)
        .bind(cutoff_unix_secs as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        Ok(rows
            .iter()
            .filter_map(|row| event_count_from_mysql_row(row).ok())
            .collect())
    }

    async fn list_user_audit_logs(
        &self,
        user_id: &str,
        query: &AuditLogListQuery,
    ) -> Result<StoredUserAuditLogPage, DataLayerError> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
  AND (? IS NULL OR event_type = ?)
"#,
        )
        .bind(user_id)
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT id, event_type, description, ip_address, status_code, created_at
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
  AND (? IS NULL OR event_type = ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?
"#,
        )
        .bind(user_id)
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let items = rows
            .iter()
            .map(map_mysql_user_audit_log_row)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StoredUserAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn delete_audit_logs_before(
        &self,
        cutoff_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        let deleted = sqlx::query(
            r#"
DELETE FROM audit_logs
WHERE id IN (
    SELECT id
    FROM (
        SELECT id
        FROM audit_logs
        WHERE created_at < ?
        ORDER BY created_at ASC, id ASC
        LIMIT ?
    ) AS doomed
)
"#,
        )
        .bind(cutoff_unix_secs.min(i64::MAX as u64) as i64)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .map_sql_err()?
        .rows_affected();
        Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
    }
}

#[derive(Debug, Clone)]
pub struct SqliteAuditLogReadRepository {
    pool: SqlitePool,
}

impl SqliteAuditLogReadRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AuditLogReadRepository for SqliteAuditLogReadRepository {
    async fn list_admin_audit_logs(
        &self,
        query: &AuditLogListQuery,
    ) -> Result<StoredAdminAuditLogPage, DataLayerError> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= ?
  AND (? IS NULL OR LOWER(u.username) LIKE LOWER(?) ESCAPE '\')
  AND (? IS NULL OR a.event_type = ?)
"#,
        )
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.username_pattern.as_deref())
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT
  a.id,
  a.event_type,
  a.user_id,
  u.email AS user_email,
  u.username AS user_username,
  a.description,
  a.ip_address,
  a.status_code,
  a.error_message,
  a.event_metadata AS metadata,
  a.created_at
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= ?
  AND (? IS NULL OR LOWER(u.username) LIKE LOWER(?) ESCAPE '\')
  AND (? IS NULL OR a.event_type = ?)
ORDER BY a.created_at DESC
LIMIT ? OFFSET ?
"#,
        )
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.username_pattern.as_deref())
        .bind(query.username_pattern.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let items = rows
            .iter()
            .map(map_sqlite_admin_audit_log_row)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StoredAdminAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn list_admin_suspicious_activities(
        &self,
        cutoff_unix_secs: u64,
    ) -> Result<Vec<StoredSuspiciousActivity>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT id, event_type, user_id, description, ip_address, event_metadata AS metadata, created_at
FROM audit_logs
WHERE created_at >= ?
  AND event_type IN (?, ?, ?, ?)
ORDER BY created_at DESC
LIMIT 100
"#,
        )
        .bind(cutoff_unix_secs as i64)
        .bind(SUSPICIOUS_EVENT_TYPES[0])
        .bind(SUSPICIOUS_EVENT_TYPES[1])
        .bind(SUSPICIOUS_EVENT_TYPES[2])
        .bind(SUSPICIOUS_EVENT_TYPES[3])
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        rows.iter()
            .map(map_sqlite_suspicious_activity_row)
            .collect()
    }

    async fn read_admin_user_behavior_event_counts(
        &self,
        user_id: &str,
        cutoff_unix_secs: u64,
    ) -> Result<std::collections::BTreeMap<String, u64>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT event_type, COUNT(*) AS count
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
GROUP BY event_type
"#,
        )
        .bind(user_id)
        .bind(cutoff_unix_secs as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        Ok(rows
            .iter()
            .filter_map(|row| event_count_from_sqlite_row(row).ok())
            .collect())
    }

    async fn list_user_audit_logs(
        &self,
        user_id: &str,
        query: &AuditLogListQuery,
    ) -> Result<StoredUserAuditLogPage, DataLayerError> {
        let total = sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
  AND (? IS NULL OR event_type = ?)
"#,
        )
        .bind(user_id)
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT id, event_type, description, ip_address, status_code, created_at
FROM audit_logs
WHERE user_id = ?
  AND created_at >= ?
  AND (? IS NULL OR event_type = ?)
ORDER BY created_at DESC
LIMIT ? OFFSET ?
"#,
        )
        .bind(user_id)
        .bind(query.cutoff_unix_secs as i64)
        .bind(query.event_type.as_deref())
        .bind(query.event_type.as_deref())
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let items = rows
            .iter()
            .map(map_sqlite_user_audit_log_row)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StoredUserAuditLogPage {
            items,
            total: total.max(0) as u64,
        })
    }

    async fn delete_audit_logs_before(
        &self,
        cutoff_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        let deleted = sqlx::query(
            r#"
DELETE FROM audit_logs
WHERE id IN (
    SELECT id
    FROM audit_logs
    WHERE created_at < ?
    ORDER BY created_at ASC, id ASC
    LIMIT ?
)
"#,
        )
        .bind(cutoff_unix_secs.min(i64::MAX as u64) as i64)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .map_sql_err()?
        .rows_affected();
        Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
    }
}

fn postgres_cutoff_time(cutoff_unix_secs: u64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(cutoff_unix_secs.min(i64::MAX as u64) as i64, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is valid"))
}

fn unix_secs_to_rfc3339(secs: u64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(secs.min(i64::MAX as u64) as i64, 0)
        .map(|value| value.to_rfc3339())
}

impl StoredAdminAuditLog {
    pub fn created_at_rfc3339(&self) -> Option<String> {
        unix_secs_to_rfc3339(self.created_at_unix_secs)
    }
}

impl StoredSuspiciousActivity {
    pub fn created_at_rfc3339(&self) -> Option<String> {
        unix_secs_to_rfc3339(self.created_at_unix_secs)
    }
}

impl StoredUserAuditLog {
    pub fn created_at_rfc3339(&self) -> Option<String> {
        unix_secs_to_rfc3339(self.created_at_unix_secs)
    }
}

fn postgres_created_at_unix_secs(row: &PgRow) -> Result<u64, DataLayerError> {
    let value = row
        .try_get::<DateTime<Utc>, _>("created_at")
        .map_postgres_err()?;
    Ok(value.timestamp().max(0) as u64)
}

fn mysql_created_at_unix_secs(row: &MySqlRow) -> Result<u64, DataLayerError> {
    let value = row.try_get::<i64, _>("created_at").map_sql_err()?;
    Ok(value.max(0) as u64)
}

fn sqlite_created_at_unix_secs(row: &SqliteRow) -> Result<u64, DataLayerError> {
    let value = row.try_get::<i64, _>("created_at").map_sql_err()?;
    Ok(value.max(0) as u64)
}

fn optional_json_from_text(value: Option<String>) -> Result<Option<Value>, DataLayerError> {
    value
        .filter(|raw| !raw.trim().is_empty())
        .map(|raw| {
            serde_json::from_str(&raw).map_err(|err| {
                DataLayerError::UnexpectedValue(format!("invalid audit log metadata json: {err}"))
            })
        })
        .transpose()
}

fn map_postgres_admin_audit_log_row(row: &PgRow) -> Result<StoredAdminAuditLog, DataLayerError> {
    Ok(StoredAdminAuditLog {
        id: row.try_get("id").map_postgres_err()?,
        event_type: row.try_get("event_type").map_postgres_err()?,
        user_id: row.try_get("user_id").map_postgres_err()?,
        user_email: row.try_get("user_email").map_postgres_err()?,
        user_username: row.try_get("user_username").map_postgres_err()?,
        description: row.try_get("description").map_postgres_err()?,
        ip_address: row.try_get("ip_address").map_postgres_err()?,
        status_code: row.try_get("status_code").map_postgres_err()?,
        error_message: row.try_get("error_message").map_postgres_err()?,
        metadata: row.try_get("metadata").map_postgres_err()?,
        created_at_unix_secs: postgres_created_at_unix_secs(row)?,
    })
}

fn map_mysql_admin_audit_log_row(row: &MySqlRow) -> Result<StoredAdminAuditLog, DataLayerError> {
    Ok(StoredAdminAuditLog {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        user_id: row.try_get("user_id").map_sql_err()?,
        user_email: row.try_get("user_email").map_sql_err()?,
        user_username: row.try_get("user_username").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        status_code: row.try_get("status_code").map_sql_err()?,
        error_message: row.try_get("error_message").map_sql_err()?,
        metadata: optional_json_from_text(row.try_get("metadata").map_sql_err()?)?,
        created_at_unix_secs: mysql_created_at_unix_secs(row)?,
    })
}

fn map_sqlite_admin_audit_log_row(row: &SqliteRow) -> Result<StoredAdminAuditLog, DataLayerError> {
    Ok(StoredAdminAuditLog {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        user_id: row.try_get("user_id").map_sql_err()?,
        user_email: row.try_get("user_email").map_sql_err()?,
        user_username: row.try_get("user_username").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        status_code: row.try_get("status_code").map_sql_err()?,
        error_message: row.try_get("error_message").map_sql_err()?,
        metadata: optional_json_from_text(row.try_get("metadata").map_sql_err()?)?,
        created_at_unix_secs: sqlite_created_at_unix_secs(row)?,
    })
}

fn map_postgres_suspicious_activity_row(
    row: &PgRow,
) -> Result<StoredSuspiciousActivity, DataLayerError> {
    Ok(StoredSuspiciousActivity {
        id: row.try_get("id").map_postgres_err()?,
        event_type: row.try_get("event_type").map_postgres_err()?,
        user_id: row.try_get("user_id").map_postgres_err()?,
        description: row.try_get("description").map_postgres_err()?,
        ip_address: row.try_get("ip_address").map_postgres_err()?,
        metadata: row.try_get("metadata").map_postgres_err()?,
        created_at_unix_secs: postgres_created_at_unix_secs(row)?,
    })
}

fn map_mysql_suspicious_activity_row(
    row: &MySqlRow,
) -> Result<StoredSuspiciousActivity, DataLayerError> {
    Ok(StoredSuspiciousActivity {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        user_id: row.try_get("user_id").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        metadata: optional_json_from_text(row.try_get("metadata").map_sql_err()?)?,
        created_at_unix_secs: mysql_created_at_unix_secs(row)?,
    })
}

fn map_sqlite_suspicious_activity_row(
    row: &SqliteRow,
) -> Result<StoredSuspiciousActivity, DataLayerError> {
    Ok(StoredSuspiciousActivity {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        user_id: row.try_get("user_id").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        metadata: optional_json_from_text(row.try_get("metadata").map_sql_err()?)?,
        created_at_unix_secs: sqlite_created_at_unix_secs(row)?,
    })
}

fn map_postgres_user_audit_log_row(row: &PgRow) -> Result<StoredUserAuditLog, DataLayerError> {
    Ok(StoredUserAuditLog {
        id: row.try_get("id").map_postgres_err()?,
        event_type: row.try_get("event_type").map_postgres_err()?,
        description: row.try_get("description").map_postgres_err()?,
        ip_address: row.try_get("ip_address").map_postgres_err()?,
        status_code: row.try_get("status_code").map_postgres_err()?,
        created_at_unix_secs: postgres_created_at_unix_secs(row)?,
    })
}

fn map_mysql_user_audit_log_row(row: &MySqlRow) -> Result<StoredUserAuditLog, DataLayerError> {
    Ok(StoredUserAuditLog {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        status_code: row.try_get("status_code").map_sql_err()?,
        created_at_unix_secs: mysql_created_at_unix_secs(row)?,
    })
}

fn map_sqlite_user_audit_log_row(row: &SqliteRow) -> Result<StoredUserAuditLog, DataLayerError> {
    Ok(StoredUserAuditLog {
        id: row.try_get("id").map_sql_err()?,
        event_type: row.try_get("event_type").map_sql_err()?,
        description: row.try_get("description").map_sql_err()?,
        ip_address: row.try_get("ip_address").map_sql_err()?,
        status_code: row.try_get("status_code").map_sql_err()?,
        created_at_unix_secs: sqlite_created_at_unix_secs(row)?,
    })
}

fn event_count_from_postgres_row(row: &PgRow) -> Result<(String, u64), DataLayerError> {
    let event_type = row.try_get("event_type").map_postgres_err()?;
    let count = row.try_get::<i64, _>("count").map_postgres_err()?.max(0) as u64;
    Ok((event_type, count))
}

fn event_count_from_mysql_row(row: &MySqlRow) -> Result<(String, u64), DataLayerError> {
    let event_type = row.try_get("event_type").map_sql_err()?;
    let count = row.try_get::<i64, _>("count").map_sql_err()?.max(0) as u64;
    Ok((event_type, count))
}

fn event_count_from_sqlite_row(row: &SqliteRow) -> Result<(String, u64), DataLayerError> {
    let event_type = row.try_get("event_type").map_sql_err()?;
    let count = row.try_get::<i64, _>("count").map_sql_err()?.max(0) as u64;
    Ok((event_type, count))
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestAuditBundle {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<StoredRequestUsageAudit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_trace: Option<DecisionTrace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_snapshot: Option<ResolvedAuthApiKeySnapshot>,
}

#[async_trait]
pub trait RequestAuditReader {
    async fn find_request_usage_audit_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError>;

    async fn read_request_decision_trace(
        &self,
        request_id: &str,
        attempted_only: bool,
    ) -> Result<Option<DecisionTrace>, DataLayerError>;

    async fn read_resolved_auth_api_key_snapshot(
        &self,
        user_id: &str,
        api_key_id: &str,
        now_unix_secs: u64,
    ) -> Result<Option<ResolvedAuthApiKeySnapshot>, DataLayerError>;
}

pub async fn read_request_audit_bundle(
    state: &impl RequestAuditReader,
    request_id: &str,
    attempted_only: bool,
    now_unix_secs: u64,
) -> Result<Option<RequestAuditBundle>, DataLayerError> {
    let usage = state
        .find_request_usage_audit_by_request_id(request_id)
        .await?;
    let decision_trace = state
        .read_request_decision_trace(request_id, attempted_only)
        .await?;

    let auth_snapshot = if let Some(usage) = usage.as_ref() {
        match (usage.user_id.as_deref(), usage.api_key_id.as_deref()) {
            (Some(user_id), Some(api_key_id)) => {
                state
                    .read_resolved_auth_api_key_snapshot(user_id, api_key_id, now_unix_secs)
                    .await?
            }
            _ => None,
        }
    } else {
        None
    };

    if usage.is_none() && decision_trace.is_none() && auth_snapshot.is_none() {
        return Ok(None);
    }

    Ok(Some(RequestAuditBundle {
        request_id: request_id.to_string(),
        usage,
        decision_trace,
        auth_snapshot,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::{
        read_request_audit_bundle, AuditLogListQuery, AuditLogReadRepository, RequestAuditReader,
        SqliteAuditLogReadRepository,
    };
    use crate::lifecycle::migrate::run_sqlite_migrations;
    use crate::repository::auth::{ResolvedAuthApiKeySnapshot, StoredAuthApiKeySnapshot};
    use crate::repository::candidates::{
        DecisionTrace, DecisionTraceCandidate, RequestCandidateFinalStatus, RequestCandidateStatus,
        StoredRequestCandidate,
    };
    use crate::repository::usage::StoredRequestUsageAudit;
    use crate::DataLayerError;

    #[derive(Default)]
    struct FakeRequestAuditReader {
        usage: Option<StoredRequestUsageAudit>,
        decision_trace: Option<DecisionTrace>,
        auth_snapshot: Option<ResolvedAuthApiKeySnapshot>,
        auth_snapshot_reads: AtomicUsize,
    }

    #[async_trait]
    impl RequestAuditReader for FakeRequestAuditReader {
        async fn find_request_usage_audit_by_request_id(
            &self,
            _request_id: &str,
        ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
            Ok(self.usage.clone())
        }

        async fn read_request_decision_trace(
            &self,
            _request_id: &str,
            _attempted_only: bool,
        ) -> Result<Option<DecisionTrace>, DataLayerError> {
            Ok(self.decision_trace.clone())
        }

        async fn read_resolved_auth_api_key_snapshot(
            &self,
            _user_id: &str,
            _api_key_id: &str,
            _now_unix_secs: u64,
        ) -> Result<Option<ResolvedAuthApiKeySnapshot>, DataLayerError> {
            self.auth_snapshot_reads.fetch_add(1, Ordering::Relaxed);
            Ok(self.auth_snapshot.clone())
        }
    }

    #[tokio::test]
    async fn read_request_audit_bundle_resolves_usage_trace_and_auth_snapshot() {
        let state = FakeRequestAuditReader {
            usage: Some(sample_usage("req-audit-1")),
            decision_trace: Some(sample_decision_trace("req-audit-1")),
            auth_snapshot: Some(sample_resolved_auth_snapshot("user-1", "api-key-1")),
            auth_snapshot_reads: AtomicUsize::new(0),
        };

        let bundle = read_request_audit_bundle(&state, "req-audit-1", true, 123)
            .await
            .expect("bundle should read")
            .expect("bundle should exist");

        assert_eq!(bundle.request_id, "req-audit-1");
        assert_eq!(
            bundle
                .usage
                .as_ref()
                .map(|usage| usage.provider_name.as_str()),
            Some("OpenAI")
        );
        assert_eq!(
            bundle
                .decision_trace
                .as_ref()
                .map(|trace| trace.total_candidates),
            Some(1)
        );
        assert_eq!(
            bundle
                .auth_snapshot
                .as_ref()
                .map(|snapshot| snapshot.api_key_id.as_str()),
            Some("api-key-1")
        );
        assert_eq!(state.auth_snapshot_reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn read_request_audit_bundle_returns_none_when_all_sources_are_empty() {
        let state = FakeRequestAuditReader::default();

        let bundle = read_request_audit_bundle(&state, "req-audit-empty", false, 123)
            .await
            .expect("bundle should read");

        assert!(bundle.is_none());
        assert_eq!(state.auth_snapshot_reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sqlite_audit_log_repository_reads_monitoring_views() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_sqlite_audit_logs(&pool).await;

        let repository = SqliteAuditLogReadRepository::new(pool);
        let admin_page = repository
            .list_admin_audit_logs(&AuditLogListQuery {
                cutoff_unix_secs: 150,
                username_pattern: Some("%ali%".to_string()),
                event_type: Some("login_failed".to_string()),
                limit: 10,
                offset: 0,
            })
            .await
            .expect("admin audit logs should read");
        assert_eq!(admin_page.total, 1);
        assert_eq!(admin_page.items[0].id, "audit-2");
        assert_eq!(admin_page.items[0].user_username.as_deref(), Some("alice"));
        assert_eq!(
            admin_page.items[0]
                .metadata
                .as_ref()
                .and_then(|value| value.get("risk"))
                .and_then(|value| value.as_str()),
            Some("high")
        );

        let suspicious = repository
            .list_admin_suspicious_activities(150)
            .await
            .expect("suspicious activities should read");
        assert_eq!(suspicious.len(), 1);
        assert_eq!(suspicious[0].event_type, "login_failed");

        let counts = repository
            .read_admin_user_behavior_event_counts("user-1", 0)
            .await
            .expect("user behavior counts should read");
        assert_eq!(counts.get("login_failed"), Some(&1));
        assert_eq!(counts.get("request_success"), Some(&1));

        let user_page = repository
            .list_user_audit_logs(
                "user-1",
                &AuditLogListQuery {
                    cutoff_unix_secs: 0,
                    username_pattern: None,
                    event_type: Some("request_success".to_string()),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .expect("user audit logs should read");
        assert_eq!(user_page.total, 1);
        assert_eq!(user_page.items[0].id, "audit-1");
        assert_eq!(user_page.items[0].status_code, Some(200));

        let deleted = repository
            .delete_audit_logs_before(250, 1)
            .await
            .expect("audit cleanup should delete one old row");
        assert_eq!(deleted, 1);
        let user_page = repository
            .list_user_audit_logs(
                "user-1",
                &AuditLogListQuery {
                    cutoff_unix_secs: 0,
                    username_pattern: None,
                    event_type: Some("request_success".to_string()),
                    limit: 10,
                    offset: 0,
                },
            )
            .await
            .expect("user audit logs should read after cleanup");
        assert_eq!(user_page.total, 0);
    }

    fn sample_usage(request_id: &str) -> StoredRequestUsageAudit {
        StoredRequestUsageAudit::new(
            "usage-1".to_string(),
            request_id.to_string(),
            Some("user-1".to_string()),
            Some("api-key-1".to_string()),
            Some("alice".to_string()),
            Some("default".to_string()),
            "OpenAI".to_string(),
            "gpt-4.1".to_string(),
            None,
            Some("provider-1".to_string()),
            Some("endpoint-1".to_string()),
            Some("provider-key-1".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            false,
            false,
            120,
            40,
            160,
            0.24,
            0.36,
            Some(200),
            None,
            None,
            Some(450),
            Some(120),
            "completed".to_string(),
            "settled".to_string(),
            100,
            101,
            Some(102),
        )
        .expect("usage should build")
    }

    async fn seed_sqlite_audit_logs(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO users (id, email, username, role, auth_source, created_at, updated_at)
VALUES
  ('user-1', 'alice@example.com', 'alice', 'user', 'local', 1, 1),
  ('user-2', 'bob@example.com', 'bob', 'user', 'local', 1, 1)
"#,
        )
        .execute(pool)
        .await
        .expect("users should insert");

        sqlx::query(
            r#"
INSERT INTO audit_logs (
    id,
    event_type,
    user_id,
    description,
    ip_address,
    event_metadata,
    status_code,
    created_at
)
VALUES
  ('audit-1', 'request_success', 'user-1', 'completed request', '127.0.0.1', NULL, 200, 100),
  ('audit-2', 'login_failed', 'user-1', 'failed login', '127.0.0.2', '{"risk":"high"}', 401, 200),
  ('audit-3', 'password_changed', 'user-2', 'other user changed password', '127.0.0.3', NULL, 200, 300)
"#,
        )
        .execute(pool)
        .await
        .expect("audit logs should insert");
    }

    fn sample_decision_trace(request_id: &str) -> DecisionTrace {
        let candidate = StoredRequestCandidate::new(
            "cand-1".to_string(),
            request_id.to_string(),
            Some("user-1".to_string()),
            Some("api-key-1".to_string()),
            Some("alice".to_string()),
            Some("default".to_string()),
            0,
            0,
            Some("provider-1".to_string()),
            Some("endpoint-1".to_string()),
            Some("provider-key-1".to_string()),
            RequestCandidateStatus::Success,
            None,
            false,
            Some(200),
            None,
            None,
            Some(37),
            None,
            None,
            None,
            100,
            Some(101),
            Some(102),
        )
        .expect("candidate should build");
        DecisionTrace {
            request_id: request_id.to_string(),
            total_candidates: 1,
            final_status: RequestCandidateFinalStatus::Success,
            total_latency_ms: 37,
            candidates: vec![DecisionTraceCandidate {
                candidate,
                provider_name: Some("OpenAI".to_string()),
                provider_website: None,
                provider_type: Some("custom".to_string()),
                provider_priority: Some(0),
                provider_keep_priority_on_conversion: Some(false),
                provider_enable_format_conversion: Some(false),
                endpoint_api_format: Some("openai:chat".to_string()),
                endpoint_api_family: Some("openai".to_string()),
                endpoint_kind: Some("chat".to_string()),
                endpoint_format_acceptance_config: None,
                provider_key_name: Some("prod".to_string()),
                provider_key_auth_type: Some("api_key".to_string()),
                provider_key_api_formats: None,
                provider_key_internal_priority: Some(10),
                provider_key_global_priority_by_format: None,
                provider_key_capabilities: None,
                provider_key_is_active: Some(true),
            }],
        }
    }

    fn sample_resolved_auth_snapshot(
        user_id: &str,
        api_key_id: &str,
    ) -> ResolvedAuthApiKeySnapshot {
        let stored = StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
        )
        .expect("auth snapshot should build");
        ResolvedAuthApiKeySnapshot::from_stored(stored, 123)
    }
}
