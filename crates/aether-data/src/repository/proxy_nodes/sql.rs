use async_trait::async_trait;
use sqlx::{postgres::PgRow, PgPool, Row};

use super::types::{
    normalize_proxy_metadata, ProxyNodeHeartbeatMutation, ProxyNodeReadRepository,
    ProxyNodeTunnelStatusMutation, ProxyNodeWriteRepository, StoredProxyNode, StoredProxyNodeEvent,
};
use crate::DataLayerError;

const FIND_PROXY_NODE_SQL: &str = r#"
SELECT
  id,
  name,
  ip,
  port,
  region,
  is_manual,
  proxy_url,
  proxy_username,
  proxy_password,
  CAST(status AS TEXT) AS status,
  registered_by,
  EXTRACT(EPOCH FROM last_heartbeat_at)::bigint AS last_heartbeat_at_unix_secs,
  heartbeat_interval,
  active_connections,
  total_requests,
  CAST(avg_latency_ms AS DOUBLE PRECISION) AS avg_latency_ms,
  failed_requests,
  dns_failures,
  stream_errors,
  proxy_metadata,
  hardware_info,
  estimated_max_concurrency,
  tunnel_mode,
  tunnel_connected,
  EXTRACT(EPOCH FROM tunnel_connected_at)::bigint AS tunnel_connected_at_unix_secs,
  remote_config,
  config_version,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM proxy_nodes
WHERE id = $1
LIMIT 1
"#;

const LIST_PROXY_NODES_SQL: &str = r#"
SELECT
  id,
  name,
  ip,
  port,
  region,
  is_manual,
  proxy_url,
  proxy_username,
  proxy_password,
  CAST(status AS TEXT) AS status,
  registered_by,
  EXTRACT(EPOCH FROM last_heartbeat_at)::bigint AS last_heartbeat_at_unix_secs,
  heartbeat_interval,
  active_connections,
  total_requests,
  CAST(avg_latency_ms AS DOUBLE PRECISION) AS avg_latency_ms,
  failed_requests,
  dns_failures,
  stream_errors,
  proxy_metadata,
  hardware_info,
  estimated_max_concurrency,
  tunnel_mode,
  tunnel_connected,
  EXTRACT(EPOCH FROM tunnel_connected_at)::bigint AS tunnel_connected_at_unix_secs,
  remote_config,
  config_version,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM proxy_nodes
ORDER BY name ASC, id ASC
"#;

const LIST_PROXY_NODE_EVENTS_SQL: &str = r#"
SELECT
  id,
  node_id,
  CAST(event_type AS TEXT) AS event_type,
  detail,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs
FROM proxy_node_events
WHERE node_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2
"#;

const APPLY_HEARTBEAT_SQL: &str = r#"
UPDATE proxy_nodes
SET
  last_heartbeat_at = NOW(),
  status = CASE
    WHEN status <> 'online'::proxynodestatus OR tunnel_connected = FALSE
      THEN 'online'::proxynodestatus
    ELSE status
  END,
  tunnel_connected = CASE
    WHEN status <> 'online'::proxynodestatus OR tunnel_connected = FALSE
      THEN TRUE
    ELSE tunnel_connected
  END,
  tunnel_connected_at = CASE
    WHEN status <> 'online'::proxynodestatus OR tunnel_connected = FALSE
      THEN NOW()
    ELSE tunnel_connected_at
  END,
  updated_at = CASE
    WHEN status <> 'online'::proxynodestatus OR tunnel_connected = FALSE
      THEN NOW()
    ELSE updated_at
  END,
  heartbeat_interval = COALESCE($2, heartbeat_interval),
  active_connections = COALESCE($3, active_connections),
  avg_latency_ms = COALESCE($4, avg_latency_ms),
  proxy_metadata = COALESCE($5, proxy_metadata),
  total_requests = total_requests + GREATEST(COALESCE($6, 0), 0),
  failed_requests = failed_requests + GREATEST(COALESCE($7, 0), 0),
  dns_failures = dns_failures + GREATEST(COALESCE($8, 0), 0),
  stream_errors = stream_errors + GREATEST(COALESCE($9, 0), 0)
WHERE id = $1
"#;

#[derive(Debug, Clone)]
pub struct SqlxProxyNodeRepository {
    pool: PgPool,
}

impl SqlxProxyNodeRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn optional_unix_secs(value: Option<i64>) -> Option<u64> {
        value.and_then(|value| u64::try_from(value).ok())
    }

    fn row_to_stored(row: &PgRow) -> Result<StoredProxyNode, DataLayerError> {
        Ok(StoredProxyNode::new(
            row.try_get("id")?,
            row.try_get("name")?,
            row.try_get("ip")?,
            row.try_get("port")?,
            row.try_get("is_manual")?,
            row.try_get("status")?,
            row.try_get("heartbeat_interval")?,
            row.try_get("active_connections")?,
            row.try_get("total_requests")?,
            row.try_get("failed_requests")?,
            row.try_get("dns_failures")?,
            row.try_get("stream_errors")?,
            row.try_get("tunnel_mode")?,
            row.try_get("tunnel_connected")?,
            row.try_get("config_version")?,
        )?
        .with_manual_proxy_fields(
            row.try_get("proxy_url")?,
            row.try_get("proxy_username")?,
            row.try_get("proxy_password")?,
        )
        .with_runtime_fields(
            row.try_get("region")?,
            row.try_get("registered_by")?,
            Self::optional_unix_secs(row.try_get("last_heartbeat_at_unix_secs")?),
            row.try_get("avg_latency_ms")?,
            row.try_get("proxy_metadata")?,
            row.try_get("hardware_info")?,
            row.try_get("estimated_max_concurrency")?,
            Self::optional_unix_secs(row.try_get("tunnel_connected_at_unix_secs")?),
            row.try_get("remote_config")?,
            Self::optional_unix_secs(row.try_get("created_at_unix_secs")?),
            Self::optional_unix_secs(row.try_get("updated_at_unix_secs")?),
        ))
    }

    fn row_to_event(row: &PgRow) -> Result<StoredProxyNodeEvent, DataLayerError> {
        Ok(StoredProxyNodeEvent {
            id: row.try_get("id")?,
            node_id: row.try_get("node_id")?,
            event_type: row.try_get("event_type")?,
            detail: row.try_get("detail")?,
            created_at_unix_secs: Self::optional_unix_secs(row.try_get("created_at_unix_secs")?),
        })
    }
}

#[async_trait]
impl ProxyNodeReadRepository for SqlxProxyNodeRepository {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, DataLayerError> {
        let rows = sqlx::query(LIST_PROXY_NODES_SQL)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::row_to_stored).collect()
    }

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let row = sqlx::query(FIND_PROXY_NODE_SQL)
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| Self::row_to_stored(&row)).transpose()
    }

    async fn list_proxy_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeEvent>, DataLayerError> {
        let rows = sqlx::query(LIST_PROXY_NODE_EVENTS_SQL)
            .bind(node_id)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(Self::row_to_event).collect()
    }
}

#[async_trait]
impl ProxyNodeWriteRepository for SqlxProxyNodeRepository {
    async fn apply_heartbeat(
        &self,
        mutation: &ProxyNodeHeartbeatMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let existing = self.find_proxy_node(&mutation.node_id).await?;
        let Some(existing) = existing else {
            return Ok(None);
        };
        if !existing.tunnel_mode {
            return Err(DataLayerError::InvalidInput(
                "non-tunnel mode is no longer supported, please upgrade aether-proxy to use tunnel mode"
                    .to_string(),
            ));
        }

        let normalized_proxy_metadata = normalize_proxy_metadata(
            mutation.proxy_metadata.as_ref(),
            mutation.proxy_version.as_deref(),
        );

        sqlx::query(APPLY_HEARTBEAT_SQL)
            .bind(&mutation.node_id)
            .bind(mutation.heartbeat_interval)
            .bind(mutation.active_connections)
            .bind(mutation.avg_latency_ms)
            .bind(normalized_proxy_metadata)
            .bind(mutation.total_requests_delta)
            .bind(mutation.failed_requests_delta)
            .bind(mutation.dns_failures_delta)
            .bind(mutation.stream_errors_delta)
            .execute(&self.pool)
            .await?;

        self.find_proxy_node(&mutation.node_id).await
    }

    async fn update_tunnel_status(
        &self,
        mutation: &ProxyNodeTunnelStatusMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let existing = self.find_proxy_node(&mutation.node_id).await?;
        let Some(existing) = existing else {
            return Ok(None);
        };

        let observed_at_unix_secs = mutation.observed_at_unix_secs;
        let event_type = if mutation.connected {
            "connected"
        } else {
            "disconnected"
        };
        let event_detail = mutation.detail.clone().unwrap_or_else(|| {
            format!(
                "[hub_node_status] conn_count={}",
                i32::max(mutation.conn_count, 0)
            )
        });

        let mut tx = self.pool.begin().await?;

        if existing
            .tunnel_connected_at_unix_secs
            .zip(observed_at_unix_secs)
            .is_some_and(|(last_transition, observed_at)| observed_at < last_transition)
        {
            sqlx::query(
                r#"
INSERT INTO proxy_node_events (node_id, event_type, detail, created_at)
VALUES (
  $1,
  $2,
  $3,
  NOW()
)
"#,
            )
            .bind(&mutation.node_id)
            .bind(event_type)
            .bind(format!("[stale_ignored] {event_detail}"))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return self.find_proxy_node(&mutation.node_id).await;
        }

        sqlx::query(
            r#"
UPDATE proxy_nodes
SET
  tunnel_connected = $2,
  tunnel_connected_at = CASE
    WHEN $3::double precision IS NULL THEN NOW()
    ELSE TO_TIMESTAMP($3::double precision)
  END,
  status = CASE
    WHEN $2 THEN 'online'::proxynodestatus
    ELSE 'offline'::proxynodestatus
  END,
  updated_at = CASE
    WHEN $3::double precision IS NULL THEN NOW()
    ELSE TO_TIMESTAMP($3::double precision)
  END
WHERE id = $1
"#,
        )
        .bind(&mutation.node_id)
        .bind(mutation.connected)
        .bind(observed_at_unix_secs.map(|value| value as f64))
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
INSERT INTO proxy_node_events (node_id, event_type, detail, created_at)
VALUES (
  $1,
  $2,
  $3,
  CASE
    WHEN $4::double precision IS NULL THEN NOW()
    ELSE TO_TIMESTAMP($4::double precision)
  END
)
"#,
        )
        .bind(&mutation.node_id)
        .bind(event_type)
        .bind(event_detail)
        .bind(observed_at_unix_secs.map(|value| value as f64))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        self.find_proxy_node(&mutation.node_id).await
    }
}
