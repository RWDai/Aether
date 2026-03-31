use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aether_data::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogProvider,
};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use flate2::{write::GzEncoder, Compression};
use futures_util::stream::{self, StreamExt};
use serde_json::Value;
use sqlx::Row;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::gateway::data::GatewayDataState;
use crate::gateway::handlers::admin_provider_ops_local_action_response;
use crate::gateway::{AppState, GatewayError};

const AUDIT_LOG_CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const GEMINI_FILE_MAPPING_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const PENDING_CLEANUP_INTERVAL: Duration = Duration::from_secs(5 * 60);
const POOL_MONITOR_INTERVAL: Duration = Duration::from_secs(5 * 60);
const PROVIDER_CHECKIN_CONCURRENCY: usize = 3;
const PROVIDER_CHECKIN_DEFAULT_TIME: &str = "01:05";
const REQUEST_CANDIDATE_CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const STATS_DAILY_AGGREGATION_HOUR: u32 = 0;
const STATS_DAILY_AGGREGATION_MINUTE: u32 = 5;
const STATS_HOURLY_AGGREGATION_MINUTE: u32 = 5;
const EXPIRED_API_KEY_PRE_CLEAN_BATCH_SIZE: usize = 2_000;
const USAGE_CLEANUP_HOUR: u32 = 3;
const USAGE_CLEANUP_MINUTE: u32 = 0;
const WALLET_DAILY_USAGE_AGGREGATION_HOUR: u32 = 0;
const WALLET_DAILY_USAGE_AGGREGATION_MINUTE: u32 = 10;
const DB_MAINTENANCE_WEEKLY_INTERVAL: chrono::Duration = chrono::Duration::days(7);
const DB_MAINTENANCE_WEEKDAY: Weekday = Weekday::Sun;
const DB_MAINTENANCE_HOUR: u32 = 5;
const DB_MAINTENANCE_MINUTE: u32 = 0;
const MAINTENANCE_DEFAULT_TIMEZONE: &str = "Asia/Shanghai";
const DB_MAINTENANCE_TABLES: &[&str] = &["usage", "request_candidates", "audit_logs"];
const SELECT_WALLET_DAILY_USAGE_AGGREGATION_ROWS_SQL: &str = r#"
SELECT
    wallet_id,
    COUNT(id) AS total_requests,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost_usd,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    COALESCE(SUM(cache_creation_input_tokens), 0) AS cache_creation_tokens,
    COALESCE(SUM(cache_read_input_tokens), 0) AS cache_read_tokens,
    MIN(finalized_at) AS first_finalized_at,
    MAX(finalized_at) AS last_finalized_at
FROM usage
WHERE wallet_id IS NOT NULL
  AND billing_status = 'settled'
  AND total_cost_usd > 0
  AND finalized_at >= $1
  AND finalized_at < $2
GROUP BY wallet_id
"#;
const UPSERT_WALLET_DAILY_USAGE_LEDGER_SQL: &str = r#"
INSERT INTO wallet_daily_usage_ledgers (
    id,
    wallet_id,
    billing_date,
    billing_timezone,
    total_cost_usd,
    total_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    first_finalized_at,
    last_finalized_at,
    aggregated_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15
)
ON CONFLICT (wallet_id, billing_date, billing_timezone)
DO UPDATE SET
    total_cost_usd = EXCLUDED.total_cost_usd,
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    first_finalized_at = EXCLUDED.first_finalized_at,
    last_finalized_at = EXCLUDED.last_finalized_at,
    aggregated_at = EXCLUDED.aggregated_at,
    updated_at = EXCLUDED.updated_at
"#;
const DELETE_STALE_WALLET_DAILY_USAGE_LEDGERS_SQL: &str = r#"
DELETE FROM wallet_daily_usage_ledgers AS ledgers
WHERE ledgers.billing_date = $1
  AND ledgers.billing_timezone = $2
  AND NOT EXISTS (
      SELECT 1
      FROM usage
      WHERE usage.wallet_id = ledgers.wallet_id
        AND usage.billing_status = 'settled'
        AND usage.total_cost_usd > 0
        AND usage.finalized_at >= $3
        AND usage.finalized_at < $4
  )
"#;
const SELECT_STALE_PENDING_USAGE_BATCH_SQL: &str = r#"
SELECT
  id,
  request_id,
  status,
  billing_status
FROM usage
WHERE status = ANY($1)
  AND created_at < $2
ORDER BY created_at ASC, id ASC
LIMIT $3
FOR UPDATE SKIP LOCKED
"#;
const SELECT_COMPLETED_PENDING_REQUEST_IDS_SQL: &str = r#"
SELECT DISTINCT request_id
FROM request_candidates
WHERE request_id = ANY($1)
  AND (
    status = 'streaming'
    OR (
      status = 'success'
      AND COALESCE(extra_data->>'stream_completed', 'false') = 'true'
    )
  )
"#;
const UPDATE_RECOVERED_STALE_USAGE_SQL: &str = r#"
UPDATE usage
SET status = 'completed',
    status_code = 200,
    error_message = NULL
WHERE id = $1
"#;
const UPDATE_FAILED_STALE_USAGE_SQL: &str = r#"
UPDATE usage
SET status = 'failed',
    status_code = 504,
    error_message = $2
WHERE id = $1
"#;
const UPDATE_FAILED_VOID_STALE_USAGE_SQL: &str = r#"
UPDATE usage
SET status = 'failed',
    status_code = 504,
    error_message = $2,
    billing_status = 'void',
    finalized_at = $3,
    total_cost_usd = 0,
    request_cost_usd = 0,
    actual_total_cost_usd = 0,
    actual_request_cost_usd = 0
WHERE id = $1
"#;
const UPDATE_RECOVERED_STREAMING_CANDIDATES_SQL: &str = r#"
UPDATE request_candidates
SET status = 'success',
    finished_at = $2
WHERE request_id = ANY($1)
  AND status = 'streaming'
"#;
const UPDATE_FAILED_PENDING_CANDIDATES_SQL: &str = r#"
UPDATE request_candidates
SET status = 'failed',
    finished_at = $2,
    error_message = '请求超时（服务器可能已重启）'
WHERE request_id = ANY($1)
  AND status = ANY($3)
"#;
const DELETE_OLD_USAGE_RECORDS_SQL: &str = r#"
WITH doomed AS (
    SELECT id
    FROM usage
    WHERE created_at < $1
    ORDER BY created_at ASC, id ASC
    LIMIT $2
)
DELETE FROM usage AS usage_rows
USING doomed
WHERE usage_rows.id = doomed.id
"#;
const SELECT_USAGE_HEADER_BATCH_SQL: &str = r#"
SELECT id
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_headers IS NOT NULL
    OR response_headers IS NOT NULL
    OR provider_request_headers IS NOT NULL
    OR client_response_headers IS NOT NULL
  )
ORDER BY created_at ASC, id ASC
LIMIT $3
"#;
const CLEAR_USAGE_HEADER_FIELDS_SQL: &str = r#"
UPDATE usage
SET request_headers = NULL,
    response_headers = NULL,
    provider_request_headers = NULL,
    client_response_headers = NULL
WHERE id = ANY($1)
"#;
const SELECT_USAGE_STALE_BODY_BATCH_SQL: &str = r#"
SELECT id
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_body IS NOT NULL
    OR response_body IS NOT NULL
    OR provider_request_body IS NOT NULL
    OR client_response_body IS NOT NULL
    OR request_body_compressed IS NOT NULL
    OR response_body_compressed IS NOT NULL
    OR provider_request_body_compressed IS NOT NULL
    OR client_response_body_compressed IS NOT NULL
  )
ORDER BY created_at ASC, id ASC
LIMIT $3
"#;
const CLEAR_USAGE_BODY_FIELDS_SQL: &str = r#"
UPDATE usage
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ANY($1)
"#;
const SELECT_USAGE_BODY_COMPRESSION_BATCH_SQL: &str = r#"
SELECT
    id,
    request_body,
    response_body,
    provider_request_body,
    client_response_body
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_body IS NOT NULL
    OR response_body IS NOT NULL
    OR provider_request_body IS NOT NULL
    OR client_response_body IS NOT NULL
  )
ORDER BY created_at ASC, id ASC
LIMIT $3
"#;
const UPDATE_USAGE_BODY_COMPRESSION_SQL: &str = r#"
UPDATE usage
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = $2,
    response_body_compressed = $3,
    provider_request_body_compressed = $4,
    client_response_body_compressed = $5
WHERE id = $1
"#;
const SELECT_EXPIRED_ACTIVE_API_KEYS_SQL: &str = r#"
SELECT id, auto_delete_on_expiry
FROM api_keys
WHERE expires_at <= NOW()
  AND is_active IS TRUE
ORDER BY expires_at ASC NULLS FIRST, id ASC
"#;
const NULLIFY_USAGE_API_KEY_BATCH_SQL: &str = r#"
WITH doomed AS (
    SELECT id
    FROM usage
    WHERE api_key_id = $1
    ORDER BY created_at ASC, id ASC
    LIMIT $2
)
UPDATE usage AS usage_rows
SET api_key_id = NULL
FROM doomed
WHERE usage_rows.id = doomed.id
"#;
const NULLIFY_REQUEST_CANDIDATE_API_KEY_BATCH_SQL: &str = r#"
WITH doomed AS (
    SELECT id
    FROM request_candidates
    WHERE api_key_id = $1
    ORDER BY created_at ASC, id ASC
    LIMIT $2
)
UPDATE request_candidates AS candidate_rows
SET api_key_id = NULL
FROM doomed
WHERE candidate_rows.id = doomed.id
"#;
const DELETE_EXPIRED_API_KEY_SQL: &str = r#"
DELETE FROM api_keys
WHERE id = $1
"#;
const DISABLE_EXPIRED_API_KEY_SQL: &str = r#"
UPDATE api_keys
SET is_active = FALSE,
    updated_at = $2
WHERE id = $1
  AND is_active IS TRUE
"#;
const DELETE_AUDIT_LOGS_BEFORE_SQL: &str = r#"
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
"#;
const SELECT_STATS_DAILY_AGGREGATE_SQL: &str = r#"
SELECT
    CAST(COUNT(id) AS BIGINT) AS total_requests,
    CAST(COALESCE(SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT) AS error_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS output_tokens,
    CAST(COALESCE(SUM(cache_creation_input_tokens), 0) AS BIGINT) AS cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_input_tokens), 0) AS BIGINT) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost,
    CAST(COALESCE(SUM(actual_total_cost_usd), 0) AS DOUBLE PRECISION) AS actual_total_cost,
    CAST(COALESCE(SUM(input_cost_usd), 0) AS DOUBLE PRECISION) AS input_cost,
    CAST(COALESCE(SUM(output_cost_usd), 0) AS DOUBLE PRECISION) AS output_cost,
    CAST(COALESCE(SUM(cache_creation_cost_usd), 0) AS DOUBLE PRECISION) AS cache_creation_cost,
    CAST(COALESCE(SUM(cache_read_cost_usd), 0) AS DOUBLE PRECISION) AS cache_read_cost,
    CAST(COALESCE(AVG(response_time_ms), 0) AS DOUBLE PRECISION) AS avg_response_time_ms,
    CAST(COUNT(DISTINCT model) AS BIGINT) AS unique_models,
    CAST(COUNT(DISTINCT provider_name) AS BIGINT) AS unique_providers
FROM usage
WHERE created_at >= $1
  AND created_at < $2
"#;
const SELECT_STATS_DAILY_FALLBACK_COUNT_SQL: &str = r#"
SELECT CAST(COUNT(*) AS BIGINT) AS fallback_count
FROM (
    SELECT request_id
    FROM request_candidates
    WHERE created_at >= $1
      AND created_at < $2
      AND status = ANY($3)
    GROUP BY request_id
    HAVING COUNT(id) > 1
) AS fallback_requests
"#;
const SELECT_STATS_DAILY_RESPONSE_TIME_PERCENTILES_SQL: &str = r#"
SELECT
    CAST(COUNT(*) AS BIGINT) AS sample_count,
    CAST(percentile_cont(0.5) WITHIN GROUP (ORDER BY response_time_ms) AS DOUBLE PRECISION) AS p50,
    CAST(percentile_cont(0.9) WITHIN GROUP (ORDER BY response_time_ms) AS DOUBLE PRECISION) AS p90,
    CAST(percentile_cont(0.99) WITHIN GROUP (ORDER BY response_time_ms) AS DOUBLE PRECISION) AS p99
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND status = 'completed'
  AND response_time_ms IS NOT NULL
"#;
const SELECT_STATS_DAILY_FIRST_BYTE_PERCENTILES_SQL: &str = r#"
SELECT
    CAST(COUNT(*) AS BIGINT) AS sample_count,
    CAST(percentile_cont(0.5) WITHIN GROUP (ORDER BY first_byte_time_ms) AS DOUBLE PRECISION) AS p50,
    CAST(percentile_cont(0.9) WITHIN GROUP (ORDER BY first_byte_time_ms) AS DOUBLE PRECISION) AS p90,
    CAST(percentile_cont(0.99) WITHIN GROUP (ORDER BY first_byte_time_ms) AS DOUBLE PRECISION) AS p99
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND status = 'completed'
  AND first_byte_time_ms IS NOT NULL
"#;
const UPSERT_STATS_DAILY_SQL: &str = r#"
INSERT INTO stats_daily (
    id,
    date,
    total_requests,
    success_requests,
    error_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    actual_total_cost,
    input_cost,
    output_cost,
    cache_creation_cost,
    cache_read_cost,
    avg_response_time_ms,
    p50_response_time_ms,
    p90_response_time_ms,
    p99_response_time_ms,
    p50_first_byte_time_ms,
    p90_first_byte_time_ms,
    p99_first_byte_time_ms,
    fallback_count,
    unique_models,
    unique_providers,
    is_complete,
    aggregated_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21, $22, $23, $24,
    $25, $26, $27, $28, $29
)
ON CONFLICT (date)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    success_requests = EXCLUDED.success_requests,
    error_requests = EXCLUDED.error_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    actual_total_cost = EXCLUDED.actual_total_cost,
    input_cost = EXCLUDED.input_cost,
    output_cost = EXCLUDED.output_cost,
    cache_creation_cost = EXCLUDED.cache_creation_cost,
    cache_read_cost = EXCLUDED.cache_read_cost,
    avg_response_time_ms = EXCLUDED.avg_response_time_ms,
    p50_response_time_ms = EXCLUDED.p50_response_time_ms,
    p90_response_time_ms = EXCLUDED.p90_response_time_ms,
    p99_response_time_ms = EXCLUDED.p99_response_time_ms,
    p50_first_byte_time_ms = EXCLUDED.p50_first_byte_time_ms,
    p90_first_byte_time_ms = EXCLUDED.p90_first_byte_time_ms,
    p99_first_byte_time_ms = EXCLUDED.p99_first_byte_time_ms,
    fallback_count = EXCLUDED.fallback_count,
    unique_models = EXCLUDED.unique_models,
    unique_providers = EXCLUDED.unique_providers,
    is_complete = EXCLUDED.is_complete,
    aggregated_at = EXCLUDED.aggregated_at,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_DAILY_MODEL_AGGREGATES_SQL: &str = r#"
SELECT
    model,
    CAST(COUNT(id) AS BIGINT) AS total_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS output_tokens,
    CAST(COALESCE(SUM(cache_creation_input_tokens), 0) AS BIGINT) AS cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_input_tokens), 0) AS BIGINT) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost,
    CAST(COALESCE(AVG(response_time_ms), 0) AS DOUBLE PRECISION) AS avg_response_time_ms
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND model IS NOT NULL
  AND model <> ''
GROUP BY model
"#;
const UPSERT_STATS_DAILY_MODEL_SQL: &str = r#"
INSERT INTO stats_daily_model (
    id,
    date,
    model,
    total_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    avg_response_time_ms,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (date, model)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    avg_response_time_ms = EXCLUDED.avg_response_time_ms,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_DAILY_PROVIDER_AGGREGATES_SQL: &str = r#"
SELECT
    COALESCE(provider_name, 'Unknown') AS provider_name,
    CAST(COUNT(id) AS BIGINT) AS total_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS output_tokens,
    CAST(COALESCE(SUM(cache_creation_input_tokens), 0) AS BIGINT) AS cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_input_tokens), 0) AS BIGINT) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost
FROM usage
WHERE created_at >= $1
  AND created_at < $2
GROUP BY COALESCE(provider_name, 'Unknown')
"#;
const UPSERT_STATS_DAILY_PROVIDER_SQL: &str = r#"
INSERT INTO stats_daily_provider (
    id,
    date,
    provider_name,
    total_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (date, provider_name)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_DAILY_API_KEY_AGGREGATES_SQL: &str = r#"
SELECT
    api_key_id,
    MAX(api_key_name) AS api_key_name,
    CAST(COUNT(id) AS BIGINT) AS total_requests,
    CAST(COALESCE(SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT) AS error_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS output_tokens,
    CAST(COALESCE(SUM(cache_creation_input_tokens), 0) AS BIGINT) AS cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_input_tokens), 0) AS BIGINT) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND api_key_id IS NOT NULL
GROUP BY api_key_id
"#;
const UPSERT_STATS_DAILY_API_KEY_SQL: &str = r#"
INSERT INTO stats_daily_api_key (
    id,
    api_key_id,
    api_key_name,
    date,
    total_requests,
    success_requests,
    error_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (api_key_id, date)
DO UPDATE SET
    api_key_name = COALESCE(EXCLUDED.api_key_name, stats_daily_api_key.api_key_name),
    total_requests = EXCLUDED.total_requests,
    success_requests = EXCLUDED.success_requests,
    error_requests = EXCLUDED.error_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    updated_at = EXCLUDED.updated_at
"#;
const DELETE_STATS_DAILY_ERRORS_FOR_DATE_SQL: &str = r#"
DELETE FROM stats_daily_error
WHERE date = $1
"#;
const SELECT_STATS_DAILY_ERROR_AGGREGATES_SQL: &str = r#"
SELECT
    error_category,
    provider_name,
    model,
    CAST(COUNT(id) AS BIGINT) AS total_count
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND error_category IS NOT NULL
GROUP BY error_category, provider_name, model
"#;
const INSERT_STATS_DAILY_ERROR_SQL: &str = r#"
INSERT INTO stats_daily_error (
    id,
    date,
    error_category,
    provider_name,
    model,
    count,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
"#;
const SELECT_ACTIVE_USER_IDS_SQL: &str = r#"
SELECT id
FROM users
WHERE is_active IS TRUE
ORDER BY id ASC
"#;
const SELECT_STATS_USER_DAILY_AGGREGATES_SQL: &str = r#"
SELECT
    user_id,
    MAX(username) AS username,
    CAST(COUNT(id) AS BIGINT) AS total_requests,
    CAST(COALESCE(SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT) AS error_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS output_tokens,
    CAST(COALESCE(SUM(cache_creation_input_tokens), 0) AS BIGINT) AS cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_input_tokens), 0) AS BIGINT) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND user_id IS NOT NULL
GROUP BY user_id
"#;
const UPSERT_STATS_USER_DAILY_SQL: &str = r#"
INSERT INTO stats_user_daily (
    id,
    user_id,
    username,
    date,
    total_requests,
    success_requests,
    error_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (user_id, date)
DO UPDATE SET
    username = COALESCE(EXCLUDED.username, stats_user_daily.username),
    total_requests = EXCLUDED.total_requests,
    success_requests = EXCLUDED.success_requests,
    error_requests = EXCLUDED.error_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_EXISTING_STATS_SUMMARY_ID_SQL: &str = r#"
SELECT id
FROM stats_summary
ORDER BY created_at ASC, id ASC
LIMIT 1
"#;
const SELECT_STATS_SUMMARY_TOTALS_SQL: &str = r#"
SELECT
    CAST(COALESCE(SUM(total_requests), 0) AS BIGINT) AS all_time_requests,
    CAST(COALESCE(SUM(success_requests), 0) AS BIGINT) AS all_time_success_requests,
    CAST(COALESCE(SUM(error_requests), 0) AS BIGINT) AS all_time_error_requests,
    CAST(COALESCE(SUM(input_tokens), 0) AS BIGINT) AS all_time_input_tokens,
    CAST(COALESCE(SUM(output_tokens), 0) AS BIGINT) AS all_time_output_tokens,
    CAST(COALESCE(SUM(cache_creation_tokens), 0) AS BIGINT) AS all_time_cache_creation_tokens,
    CAST(COALESCE(SUM(cache_read_tokens), 0) AS BIGINT) AS all_time_cache_read_tokens,
    CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS all_time_cost,
    CAST(COALESCE(SUM(actual_total_cost), 0) AS DOUBLE PRECISION) AS all_time_actual_cost
FROM stats_daily
WHERE date < $1
"#;
const SELECT_STATS_SUMMARY_ENTITY_COUNTS_SQL: &str = r#"
SELECT
    CAST((SELECT COUNT(id) FROM users) AS BIGINT) AS total_users,
    CAST((SELECT COUNT(id) FROM users WHERE is_active IS TRUE) AS BIGINT) AS active_users,
    CAST((SELECT COUNT(id) FROM api_keys) AS BIGINT) AS total_api_keys,
    CAST((SELECT COUNT(id) FROM api_keys WHERE is_active IS TRUE) AS BIGINT) AS active_api_keys
"#;
const INSERT_STATS_SUMMARY_SQL: &str = r#"
INSERT INTO stats_summary (
    id,
    cutoff_date,
    all_time_requests,
    all_time_success_requests,
    all_time_error_requests,
    all_time_input_tokens,
    all_time_output_tokens,
    all_time_cache_creation_tokens,
    all_time_cache_read_tokens,
    all_time_cost,
    all_time_actual_cost,
    total_users,
    active_users,
    total_api_keys,
    active_api_keys,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16, $17
)
"#;
const UPDATE_STATS_SUMMARY_SQL: &str = r#"
UPDATE stats_summary
SET cutoff_date = $2,
    all_time_requests = $3,
    all_time_success_requests = $4,
    all_time_error_requests = $5,
    all_time_input_tokens = $6,
    all_time_output_tokens = $7,
    all_time_cache_creation_tokens = $8,
    all_time_cache_read_tokens = $9,
    all_time_cost = $10,
    all_time_actual_cost = $11,
    total_users = $12,
    active_users = $13,
    total_api_keys = $14,
    active_api_keys = $15,
    updated_at = $16
WHERE id = $1
"#;
const SELECT_STATS_HOURLY_AGGREGATE_SQL: &str = r#"
SELECT
    COUNT(id) AS total_requests,
    COALESCE(SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS error_requests,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    COALESCE(SUM(cache_creation_input_tokens), 0) AS cache_creation_tokens,
    COALESCE(SUM(cache_read_input_tokens), 0) AS cache_read_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost,
    CAST(COALESCE(SUM(actual_total_cost_usd), 0) AS DOUBLE PRECISION) AS actual_total_cost,
    CAST(COALESCE(AVG(response_time_ms), 0) AS DOUBLE PRECISION) AS avg_response_time_ms
FROM usage
WHERE created_at >= $1
  AND created_at < $2
"#;
const UPSERT_STATS_HOURLY_SQL: &str = r#"
INSERT INTO stats_hourly (
    id,
    hour_utc,
    total_requests,
    success_requests,
    error_requests,
    input_tokens,
    output_tokens,
    cache_creation_tokens,
    cache_read_tokens,
    total_cost,
    actual_total_cost,
    avg_response_time_ms,
    is_complete,
    aggregated_at,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (hour_utc)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    success_requests = EXCLUDED.success_requests,
    error_requests = EXCLUDED.error_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    actual_total_cost = EXCLUDED.actual_total_cost,
    avg_response_time_ms = EXCLUDED.avg_response_time_ms,
    is_complete = EXCLUDED.is_complete,
    aggregated_at = EXCLUDED.aggregated_at,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_HOURLY_USER_AGGREGATES_SQL: &str = r#"
SELECT
    user_id,
    COUNT(id) AS total_requests,
    COALESCE(SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS error_requests,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost
FROM usage
WHERE created_at >= $1
  AND created_at < $2
  AND user_id IS NOT NULL
GROUP BY user_id
"#;
const UPSERT_STATS_HOURLY_USER_SQL: &str = r#"
INSERT INTO stats_hourly_user (
    id,
    hour_utc,
    user_id,
    total_requests,
    success_requests,
    error_requests,
    input_tokens,
    output_tokens,
    total_cost,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (hour_utc, user_id)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    success_requests = EXCLUDED.success_requests,
    error_requests = EXCLUDED.error_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    total_cost = EXCLUDED.total_cost,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_HOURLY_MODEL_AGGREGATES_SQL: &str = r#"
SELECT
    model,
    COUNT(id) AS total_requests,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost,
    CAST(COALESCE(AVG(response_time_ms), 0) AS DOUBLE PRECISION) AS avg_response_time_ms
FROM usage
WHERE created_at >= $1
  AND created_at < $2
GROUP BY model
"#;
const UPSERT_STATS_HOURLY_MODEL_SQL: &str = r#"
INSERT INTO stats_hourly_model (
    id,
    hour_utc,
    model,
    total_requests,
    input_tokens,
    output_tokens,
    total_cost,
    avg_response_time_ms,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (hour_utc, model)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    total_cost = EXCLUDED.total_cost,
    avg_response_time_ms = EXCLUDED.avg_response_time_ms,
    updated_at = EXCLUDED.updated_at
"#;
const SELECT_STATS_HOURLY_PROVIDER_AGGREGATES_SQL: &str = r#"
SELECT
    provider_name,
    COUNT(id) AS total_requests,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    CAST(COALESCE(SUM(total_cost_usd), 0) AS DOUBLE PRECISION) AS total_cost
FROM usage
WHERE created_at >= $1
  AND created_at < $2
GROUP BY provider_name
"#;
const UPSERT_STATS_HOURLY_PROVIDER_SQL: &str = r#"
INSERT INTO stats_hourly_provider (
    id,
    hour_utc,
    provider_name,
    total_requests,
    input_tokens,
    output_tokens,
    total_cost,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (hour_utc, provider_name)
DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    total_cost = EXCLUDED.total_cost,
    updated_at = EXCLUDED.updated_at
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DbMaintenanceRunSummary {
    attempted: usize,
    succeeded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalletDailyUsageAggregationSummary {
    pub(crate) billing_date: chrono::NaiveDate,
    pub(crate) billing_timezone: String,
    pub(crate) aggregated_wallets: usize,
    pub(crate) deleted_stale_ledgers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatsAggregationSummary {
    day_start_utc: DateTime<Utc>,
    total_requests: i64,
    model_rows: usize,
    provider_rows: usize,
    api_key_rows: usize,
    error_rows: usize,
    user_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct PercentileSummary {
    p50: Option<i64>,
    p90: Option<i64>,
    p99: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct UsageCleanupSummary {
    body_compressed: usize,
    body_cleaned: usize,
    header_cleaned: usize,
    keys_cleaned: usize,
    records_deleted: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PendingCleanupSummary {
    pub(crate) failed: usize,
    pub(crate) recovered: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProviderCheckinRunSummary {
    pub(crate) attempted: usize,
    pub(crate) succeeded: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderCheckinStatus {
    Succeeded,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderCheckinOutcome {
    provider_id: String,
    status: ProviderCheckinStatus,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalletDailyUsageAggregationTarget {
    billing_date: chrono::NaiveDate,
    billing_timezone: String,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StalePendingUsageRow {
    id: String,
    request_id: String,
    status: String,
    billing_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailedPendingUsageRow {
    id: String,
    error_message: String,
    should_void_billing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct PendingCleanupBatchPlan {
    recovered_usage_ids: Vec<String>,
    recovered_request_ids: Vec<String>,
    failed_usage_rows: Vec<FailedPendingUsageRow>,
    failed_request_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UsageCleanupSettings {
    detail_retention_days: u64,
    compressed_retention_days: u64,
    header_retention_days: u64,
    log_retention_days: u64,
    batch_size: usize,
    auto_delete_expired_keys: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UsageCleanupWindow {
    detail_cutoff: DateTime<Utc>,
    compressed_cutoff: DateTime<Utc>,
    header_cutoff: DateTime<Utc>,
    log_cutoff: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
struct UsageBodyCompressionRow {
    id: String,
    request_body: Option<Value>,
    response_body: Option<Value>,
    provider_request_body: Option<Value>,
    client_response_body: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PoolMonitorSummary {
    checked_out: usize,
    pool_size: usize,
    idle: usize,
    max_connections: u32,
    usage_rate: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct StatsHourlyAggregationSummary {
    hour_utc: DateTime<Utc>,
    total_requests: i64,
    user_rows: usize,
    model_rows: usize,
    provider_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiredApiKeyRow<'a> {
    id: &'a str,
    auto_delete_on_expiry: Option<bool>,
}

pub(crate) async fn cleanup_audit_logs_once(
    data: &GatewayDataState,
) -> Result<usize, aether_data::DataLayerError> {
    cleanup_audit_logs_with(data, |cutoff_time, delete_limit| async move {
        let Some(pool) = data.postgres_pool() else {
            return Ok(0);
        };
        let deleted = sqlx::query(DELETE_AUDIT_LOGS_BEFORE_SQL)
            .bind(cutoff_time)
            .bind(i64::try_from(delete_limit).unwrap_or(i64::MAX))
            .execute(&pool)
            .await?
            .rows_affected();
        Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
    })
    .await
}

pub(crate) async fn cleanup_expired_gemini_file_mappings_once(
    data: &GatewayDataState,
) -> Result<usize, aether_data::DataLayerError> {
    data.delete_expired_gemini_file_mappings(now_unix_secs())
        .await
}

async fn perform_db_maintenance_once(
    data: &GatewayDataState,
) -> Result<DbMaintenanceRunSummary, aether_data::DataLayerError> {
    let Some(pool) = data.postgres_pool() else {
        return Ok(DbMaintenanceRunSummary {
            attempted: 0,
            succeeded: 0,
        });
    };

    run_db_maintenance_with(data, |table_name| {
        let pool = pool.clone();
        async move {
            let statement = format!("VACUUM ANALYZE {table_name}");
            sqlx::raw_sql(&statement).execute(&pool).await?;
            Ok(())
        }
    })
    .await
}

async fn perform_wallet_daily_usage_aggregation_once(
    data: &GatewayDataState,
) -> Result<WalletDailyUsageAggregationSummary, aether_data::DataLayerError> {
    let timezone = maintenance_timezone();
    let now_utc = Utc::now();
    let target = wallet_daily_usage_aggregation_target(now_utc, timezone);
    let Some(pool) = data.postgres_pool() else {
        return Ok(WalletDailyUsageAggregationSummary {
            billing_date: target.billing_date,
            billing_timezone: target.billing_timezone,
            aggregated_wallets: 0,
            deleted_stale_ledgers: 0,
        });
    };

    let mut tx = pool.begin().await?;
    let rows = sqlx::query(SELECT_WALLET_DAILY_USAGE_AGGREGATION_ROWS_SQL)
        .bind(target.window_start_utc)
        .bind(target.window_end_utc)
        .fetch_all(&mut *tx)
        .await?;
    for row in &rows {
        sqlx::query(UPSERT_WALLET_DAILY_USAGE_LEDGER_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(row.try_get::<String, _>("wallet_id")?)
            .bind(target.billing_date)
            .bind(target.billing_timezone.as_str())
            .bind(row.try_get::<f64, _>("total_cost_usd")?)
            .bind(row.try_get::<i64, _>("total_requests")?)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<i64, _>("cache_creation_tokens")?)
            .bind(row.try_get::<i64, _>("cache_read_tokens")?)
            .bind(row.try_get::<Option<DateTime<Utc>>, _>("first_finalized_at")?)
            .bind(row.try_get::<Option<DateTime<Utc>>, _>("last_finalized_at")?)
            .bind(now_utc)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut *tx)
            .await?;
    }

    let deleted_stale_ledgers = sqlx::query(DELETE_STALE_WALLET_DAILY_USAGE_LEDGERS_SQL)
        .bind(target.billing_date)
        .bind(target.billing_timezone.as_str())
        .bind(target.window_start_utc)
        .bind(target.window_end_utc)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;

    Ok(WalletDailyUsageAggregationSummary {
        billing_date: target.billing_date,
        billing_timezone: target.billing_timezone,
        aggregated_wallets: rows.len(),
        deleted_stale_ledgers: usize::try_from(deleted_stale_ledgers).unwrap_or(usize::MAX),
    })
}

async fn perform_stats_aggregation_once(
    data: &GatewayDataState,
) -> Result<Option<StatsAggregationSummary>, aether_data::DataLayerError> {
    let Some(pool) = data.postgres_pool() else {
        return Ok(None);
    };
    if !system_config_bool(data, "enable_stats_aggregation", true).await? {
        return Ok(None);
    }

    let now_utc = Utc::now();
    let day_start_utc = stats_aggregation_target_day(now_utc);
    let day_end_utc = day_start_utc + chrono::Duration::days(1);
    let mut tx = pool.begin().await?;
    let aggregate_row = sqlx::query(SELECT_STATS_DAILY_AGGREGATE_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_one(&mut *tx)
        .await?;
    let total_requests = aggregate_row.try_get::<i64, _>("total_requests")?;
    let error_requests = aggregate_row.try_get::<i64, _>("error_requests")?;
    let success_requests = total_requests.saturating_sub(error_requests);
    let fallback_count = sqlx::query(SELECT_STATS_DAILY_FALLBACK_COUNT_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .bind(vec!["success", "failed"])
        .fetch_one(&mut *tx)
        .await?
        .try_get::<i64, _>("fallback_count")?;
    let response_percentiles = fetch_stats_daily_percentiles(
        &mut tx,
        SELECT_STATS_DAILY_RESPONSE_TIME_PERCENTILES_SQL,
        day_start_utc,
        day_end_utc,
    )
    .await?;
    let first_byte_percentiles = fetch_stats_daily_percentiles(
        &mut tx,
        SELECT_STATS_DAILY_FIRST_BYTE_PERCENTILES_SQL,
        day_start_utc,
        day_end_utc,
    )
    .await?;

    sqlx::query(UPSERT_STATS_DAILY_SQL)
        .bind(Uuid::new_v4().to_string())
        .bind(day_start_utc)
        .bind(total_requests)
        .bind(success_requests)
        .bind(error_requests)
        .bind(aggregate_row.try_get::<i64, _>("input_tokens")?)
        .bind(aggregate_row.try_get::<i64, _>("output_tokens")?)
        .bind(aggregate_row.try_get::<i64, _>("cache_creation_tokens")?)
        .bind(aggregate_row.try_get::<i64, _>("cache_read_tokens")?)
        .bind(aggregate_row.try_get::<f64, _>("total_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("actual_total_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("input_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("output_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("cache_creation_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("cache_read_cost")?)
        .bind(aggregate_row.try_get::<f64, _>("avg_response_time_ms")?)
        .bind(response_percentiles.p50)
        .bind(response_percentiles.p90)
        .bind(response_percentiles.p99)
        .bind(first_byte_percentiles.p50)
        .bind(first_byte_percentiles.p90)
        .bind(first_byte_percentiles.p99)
        .bind(fallback_count)
        .bind(aggregate_row.try_get::<i64, _>("unique_models")?)
        .bind(aggregate_row.try_get::<i64, _>("unique_providers")?)
        .bind(true)
        .bind(now_utc)
        .bind(now_utc)
        .bind(now_utc)
        .execute(&mut *tx)
        .await?;

    let model_rows =
        upsert_stats_daily_model_rows(&mut tx, day_start_utc, day_end_utc, now_utc).await?;
    let provider_rows =
        upsert_stats_daily_provider_rows(&mut tx, day_start_utc, day_end_utc, now_utc).await?;
    let api_key_rows =
        upsert_stats_daily_api_key_rows(&mut tx, day_start_utc, day_end_utc, now_utc).await?;
    let error_rows =
        refresh_stats_daily_error_rows(&mut tx, day_start_utc, day_end_utc, now_utc).await?;
    let user_rows =
        upsert_stats_user_daily_rows(&mut tx, day_start_utc, day_end_utc, now_utc).await?;
    refresh_stats_summary_row(&mut tx, day_end_utc, now_utc).await?;
    tx.commit().await?;

    Ok(Some(StatsAggregationSummary {
        day_start_utc,
        total_requests,
        model_rows,
        provider_rows,
        api_key_rows,
        error_rows,
        user_rows,
    }))
}

async fn fetch_stats_daily_percentiles(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
) -> Result<PercentileSummary, sqlx::Error> {
    let row = sqlx::query(sql)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_one(&mut **tx)
        .await?;
    let sample_count = row.try_get::<i64, _>("sample_count")?;
    if sample_count < 10 {
        return Ok(PercentileSummary::default());
    }

    Ok(PercentileSummary {
        p50: percentile_ms_to_i64(row.try_get::<Option<f64>, _>("p50")?),
        p90: percentile_ms_to_i64(row.try_get::<Option<f64>, _>("p90")?),
        p99: percentile_ms_to_i64(row.try_get::<Option<f64>, _>("p99")?),
    })
}

fn percentile_ms_to_i64(value: Option<f64>) -> Option<i64> {
    value.and_then(|raw| raw.is_finite().then_some(raw.floor() as i64))
}

async fn upsert_stats_daily_model_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_DAILY_MODEL_AGGREGATES_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_all(&mut **tx)
        .await?;

    for row in &rows {
        sqlx::query(UPSERT_STATS_DAILY_MODEL_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(day_start_utc)
            .bind(row.try_get::<String, _>("model")?)
            .bind(row.try_get::<i64, _>("total_requests")?)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<i64, _>("cache_creation_tokens")?)
            .bind(row.try_get::<i64, _>("cache_read_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(row.try_get::<f64, _>("avg_response_time_ms")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(rows.len())
}

async fn upsert_stats_daily_provider_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_DAILY_PROVIDER_AGGREGATES_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_all(&mut **tx)
        .await?;

    for row in &rows {
        sqlx::query(UPSERT_STATS_DAILY_PROVIDER_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(day_start_utc)
            .bind(row.try_get::<String, _>("provider_name")?)
            .bind(row.try_get::<i64, _>("total_requests")?)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<i64, _>("cache_creation_tokens")?)
            .bind(row.try_get::<i64, _>("cache_read_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(rows.len())
}

async fn upsert_stats_daily_api_key_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_DAILY_API_KEY_AGGREGATES_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_all(&mut **tx)
        .await?;

    for row in &rows {
        let total_requests = row.try_get::<i64, _>("total_requests")?;
        let error_requests = row.try_get::<i64, _>("error_requests")?;
        sqlx::query(UPSERT_STATS_DAILY_API_KEY_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(row.try_get::<String, _>("api_key_id")?)
            .bind(row.try_get::<Option<String>, _>("api_key_name")?)
            .bind(day_start_utc)
            .bind(total_requests)
            .bind(total_requests.saturating_sub(error_requests))
            .bind(error_requests)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<i64, _>("cache_creation_tokens")?)
            .bind(row.try_get::<i64, _>("cache_read_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(rows.len())
}

async fn refresh_stats_daily_error_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    sqlx::query(DELETE_STATS_DAILY_ERRORS_FOR_DATE_SQL)
        .bind(day_start_utc)
        .execute(&mut **tx)
        .await?;
    let rows = sqlx::query(SELECT_STATS_DAILY_ERROR_AGGREGATES_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_all(&mut **tx)
        .await?;

    for row in &rows {
        sqlx::query(INSERT_STATS_DAILY_ERROR_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(day_start_utc)
            .bind(row.try_get::<String, _>("error_category")?)
            .bind(row.try_get::<Option<String>, _>("provider_name")?)
            .bind(row.try_get::<Option<String>, _>("model")?)
            .bind(row.try_get::<i64, _>("total_count")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(rows.len())
}

async fn upsert_stats_user_daily_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    day_start_utc: DateTime<Utc>,
    day_end_utc: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let active_user_ids = sqlx::query(SELECT_ACTIVE_USER_IDS_SQL)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("id"))
        .collect::<Result<Vec<_>, _>>()?;
    if active_user_ids.is_empty() {
        return Ok(0);
    }

    let aggregated_rows = sqlx::query(SELECT_STATS_USER_DAILY_AGGREGATES_SQL)
        .bind(day_start_utc)
        .bind(day_end_utc)
        .fetch_all(&mut **tx)
        .await?;
    let mut aggregated_by_user = HashMap::with_capacity(aggregated_rows.len());
    for row in aggregated_rows {
        let user_id = row.try_get::<String, _>("user_id")?;
        aggregated_by_user.insert(user_id, row);
    }

    for user_id in &active_user_ids {
        let aggregated = aggregated_by_user.get(user_id);
        let total_requests = aggregated
            .map(|row| row.try_get::<i64, _>("total_requests"))
            .transpose()?
            .unwrap_or_default();
        let error_requests = aggregated
            .map(|row| row.try_get::<i64, _>("error_requests"))
            .transpose()?
            .unwrap_or_default();
        sqlx::query(UPSERT_STATS_USER_DAILY_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(user_id)
            .bind(
                aggregated
                    .map(|row| row.try_get::<Option<String>, _>("username"))
                    .transpose()?
                    .flatten(),
            )
            .bind(day_start_utc)
            .bind(total_requests)
            .bind(total_requests.saturating_sub(error_requests))
            .bind(error_requests)
            .bind(
                aggregated
                    .map(|row| row.try_get::<i64, _>("input_tokens"))
                    .transpose()?
                    .unwrap_or_default(),
            )
            .bind(
                aggregated
                    .map(|row| row.try_get::<i64, _>("output_tokens"))
                    .transpose()?
                    .unwrap_or_default(),
            )
            .bind(
                aggregated
                    .map(|row| row.try_get::<i64, _>("cache_creation_tokens"))
                    .transpose()?
                    .unwrap_or_default(),
            )
            .bind(
                aggregated
                    .map(|row| row.try_get::<i64, _>("cache_read_tokens"))
                    .transpose()?
                    .unwrap_or_default(),
            )
            .bind(
                aggregated
                    .map(|row| row.try_get::<f64, _>("total_cost"))
                    .transpose()?
                    .unwrap_or_default(),
            )
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(active_user_ids.len())
}

async fn refresh_stats_summary_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cutoff_date: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let totals_row = sqlx::query(SELECT_STATS_SUMMARY_TOTALS_SQL)
        .bind(cutoff_date)
        .fetch_one(&mut **tx)
        .await?;
    let entity_counts_row = sqlx::query(SELECT_STATS_SUMMARY_ENTITY_COUNTS_SQL)
        .fetch_one(&mut **tx)
        .await?;
    let existing_summary_id = sqlx::query_scalar::<_, String>(SELECT_EXISTING_STATS_SUMMARY_ID_SQL)
        .fetch_optional(&mut **tx)
        .await?;

    let all_time_requests = totals_row.try_get::<i64, _>("all_time_requests")?;
    let all_time_success_requests = totals_row.try_get::<i64, _>("all_time_success_requests")?;
    let all_time_error_requests = totals_row.try_get::<i64, _>("all_time_error_requests")?;
    let all_time_input_tokens = totals_row.try_get::<i64, _>("all_time_input_tokens")?;
    let all_time_output_tokens = totals_row.try_get::<i64, _>("all_time_output_tokens")?;
    let all_time_cache_creation_tokens =
        totals_row.try_get::<i64, _>("all_time_cache_creation_tokens")?;
    let all_time_cache_read_tokens = totals_row.try_get::<i64, _>("all_time_cache_read_tokens")?;
    let all_time_cost = totals_row.try_get::<f64, _>("all_time_cost")?;
    let all_time_actual_cost = totals_row.try_get::<f64, _>("all_time_actual_cost")?;
    let total_users = entity_counts_row.try_get::<i64, _>("total_users")?;
    let active_users = entity_counts_row.try_get::<i64, _>("active_users")?;
    let total_api_keys = entity_counts_row.try_get::<i64, _>("total_api_keys")?;
    let active_api_keys = entity_counts_row.try_get::<i64, _>("active_api_keys")?;

    if let Some(summary_id) = existing_summary_id {
        sqlx::query(UPDATE_STATS_SUMMARY_SQL)
            .bind(summary_id)
            .bind(cutoff_date)
            .bind(all_time_requests)
            .bind(all_time_success_requests)
            .bind(all_time_error_requests)
            .bind(all_time_input_tokens)
            .bind(all_time_output_tokens)
            .bind(all_time_cache_creation_tokens)
            .bind(all_time_cache_read_tokens)
            .bind(all_time_cost)
            .bind(all_time_actual_cost)
            .bind(total_users)
            .bind(active_users)
            .bind(total_api_keys)
            .bind(active_api_keys)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    } else {
        sqlx::query(INSERT_STATS_SUMMARY_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(cutoff_date)
            .bind(all_time_requests)
            .bind(all_time_success_requests)
            .bind(all_time_error_requests)
            .bind(all_time_input_tokens)
            .bind(all_time_output_tokens)
            .bind(all_time_cache_creation_tokens)
            .bind(all_time_cache_read_tokens)
            .bind(all_time_cost)
            .bind(all_time_actual_cost)
            .bind(total_users)
            .bind(active_users)
            .bind(total_api_keys)
            .bind(active_api_keys)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(())
}

async fn perform_usage_cleanup_once(
    data: &GatewayDataState,
) -> Result<UsageCleanupSummary, aether_data::DataLayerError> {
    let Some(pool) = data.postgres_pool() else {
        return Ok(UsageCleanupSummary::default());
    };
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(UsageCleanupSummary::default());
    }

    let settings = usage_cleanup_settings(data).await?;
    let window = usage_cleanup_window(Utc::now(), settings);
    let records_deleted =
        delete_old_usage_records(&pool, window.log_cutoff, settings.batch_size).await?;
    let header_cleaned = cleanup_usage_header_fields(
        &pool,
        window.header_cutoff,
        settings.batch_size,
        Some(window.log_cutoff),
    )
    .await?;
    let body_cleaned = cleanup_usage_stale_body_fields(
        &pool,
        window.compressed_cutoff,
        settings.batch_size,
        Some(window.log_cutoff),
    )
    .await?;
    let body_compressed = compress_usage_body_fields(
        &pool,
        window.detail_cutoff,
        settings.batch_size,
        Some(window.compressed_cutoff),
    )
    .await?;
    let keys_cleaned =
        match cleanup_expired_api_keys(&pool, settings.auto_delete_expired_keys).await {
            Ok(count) => count,
            Err(err) => {
                warn!(error = %err, "gateway expired api key cleanup failed");
                0
            }
        };

    Ok(UsageCleanupSummary {
        body_compressed,
        body_cleaned,
        header_cleaned,
        keys_cleaned,
        records_deleted,
    })
}

pub(crate) async fn cleanup_stale_pending_requests_once(
    data: &GatewayDataState,
) -> Result<PendingCleanupSummary, aether_data::DataLayerError> {
    let Some(pool) = data.postgres_pool() else {
        return Ok(PendingCleanupSummary::default());
    };

    let timeout_minutes = pending_cleanup_timeout_minutes(data).await?;
    let batch_size = pending_cleanup_batch_size(data).await?;
    let cutoff_time =
        Utc::now() - chrono::Duration::minutes(i64::try_from(timeout_minutes).unwrap_or(i64::MAX));
    let active_statuses = vec!["pending", "streaming"];
    let mut summary = PendingCleanupSummary::default();

    loop {
        let mut tx = pool.begin().await?;
        let stale_rows = sqlx::query(SELECT_STALE_PENDING_USAGE_BATCH_SQL)
            .bind(active_statuses.clone())
            .bind(cutoff_time)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .fetch_all(&mut *tx)
            .await?;
        if stale_rows.is_empty() {
            tx.rollback().await?;
            break;
        }

        let stale_rows = stale_rows
            .into_iter()
            .map(|row| {
                Ok(StalePendingUsageRow {
                    id: row.try_get::<String, _>("id")?,
                    request_id: row.try_get::<String, _>("request_id")?,
                    status: row.try_get::<String, _>("status")?,
                    billing_status: row.try_get::<String, _>("billing_status")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        let request_ids = stale_rows
            .iter()
            .map(|row| row.request_id.clone())
            .collect::<Vec<_>>();
        let completed_request_ids = if request_ids.is_empty() {
            HashSet::new()
        } else {
            sqlx::query(SELECT_COMPLETED_PENDING_REQUEST_IDS_SQL)
                .bind(request_ids)
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .filter_map(|row| row.try_get::<String, _>("request_id").ok())
                .collect::<HashSet<_>>()
        };
        let plan = plan_pending_cleanup_batch(stale_rows, &completed_request_ids, timeout_minutes);
        let now = Utc::now();

        for usage_id in &plan.recovered_usage_ids {
            sqlx::query(UPDATE_RECOVERED_STALE_USAGE_SQL)
                .bind(usage_id)
                .execute(&mut *tx)
                .await?;
        }
        for failed_row in &plan.failed_usage_rows {
            if failed_row.should_void_billing {
                sqlx::query(UPDATE_FAILED_VOID_STALE_USAGE_SQL)
                    .bind(&failed_row.id)
                    .bind(&failed_row.error_message)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
            } else {
                sqlx::query(UPDATE_FAILED_STALE_USAGE_SQL)
                    .bind(&failed_row.id)
                    .bind(&failed_row.error_message)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        if !plan.recovered_request_ids.is_empty() {
            sqlx::query(UPDATE_RECOVERED_STREAMING_CANDIDATES_SQL)
                .bind(plan.recovered_request_ids.clone())
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        if !plan.failed_request_ids.is_empty() {
            sqlx::query(UPDATE_FAILED_PENDING_CANDIDATES_SQL)
                .bind(plan.failed_request_ids.clone())
                .bind(now)
                .bind(active_statuses.clone())
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        summary.failed += plan.failed_usage_rows.len();
        summary.recovered += plan.recovered_usage_ids.len();
    }

    Ok(summary)
}

fn summarize_postgres_pool(data: &GatewayDataState) -> Option<PoolMonitorSummary> {
    let pool = data.postgres_pool()?;
    let max_connections = data.postgres_max_connections()?.max(1);
    let pool_size = usize::try_from(pool.size()).unwrap_or(usize::MAX);
    let idle = pool.num_idle();
    let checked_out = pool_size.saturating_sub(idle);
    let usage_rate = checked_out as f64 / f64::from(max_connections) * 100.0;

    Some(PoolMonitorSummary {
        checked_out,
        pool_size,
        idle,
        max_connections,
        usage_rate,
    })
}

async fn perform_stats_hourly_aggregation_once(
    data: &GatewayDataState,
) -> Result<Option<StatsHourlyAggregationSummary>, aether_data::DataLayerError> {
    let Some(pool) = data.postgres_pool() else {
        return Ok(None);
    };
    if !system_config_bool(data, "enable_stats_aggregation", true).await? {
        return Ok(None);
    }

    let now_utc = Utc::now();
    let hour_utc = stats_hourly_aggregation_target_hour(now_utc);
    let hour_end = hour_utc + chrono::Duration::hours(1);
    let aggregated_at = now_utc;
    let mut tx = pool.begin().await?;

    let row = sqlx::query(SELECT_STATS_HOURLY_AGGREGATE_SQL)
        .bind(hour_utc)
        .bind(hour_end)
        .fetch_one(&mut *tx)
        .await?;
    let total_requests = row.try_get::<i64, _>("total_requests")?;
    let error_requests = row.try_get::<i64, _>("error_requests")?;
    let success_requests = total_requests.saturating_sub(error_requests);
    sqlx::query(UPSERT_STATS_HOURLY_SQL)
        .bind(Uuid::new_v4().to_string())
        .bind(hour_utc)
        .bind(total_requests)
        .bind(success_requests)
        .bind(error_requests)
        .bind(row.try_get::<i64, _>("input_tokens")?)
        .bind(row.try_get::<i64, _>("output_tokens")?)
        .bind(row.try_get::<i64, _>("cache_creation_tokens")?)
        .bind(row.try_get::<i64, _>("cache_read_tokens")?)
        .bind(row.try_get::<f64, _>("total_cost")?)
        .bind(row.try_get::<f64, _>("actual_total_cost")?)
        .bind(row.try_get::<f64, _>("avg_response_time_ms")?)
        .bind(true)
        .bind(aggregated_at)
        .bind(aggregated_at)
        .bind(aggregated_at)
        .execute(&mut *tx)
        .await?;

    let user_rows =
        upsert_stats_hourly_user_rows(&mut tx, hour_utc, hour_end, aggregated_at).await?;
    let model_rows =
        upsert_stats_hourly_model_rows(&mut tx, hour_utc, hour_end, aggregated_at).await?;
    let provider_rows =
        upsert_stats_hourly_provider_rows(&mut tx, hour_utc, hour_end, aggregated_at).await?;
    tx.commit().await?;

    Ok(Some(StatsHourlyAggregationSummary {
        hour_utc,
        total_requests,
        user_rows,
        model_rows,
        provider_rows,
    }))
}

async fn upsert_stats_hourly_user_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    hour_utc: DateTime<Utc>,
    hour_end: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_HOURLY_USER_AGGREGATES_SQL)
        .bind(hour_utc)
        .bind(hour_end)
        .fetch_all(&mut **tx)
        .await?;

    for row in &rows {
        let user_id = row.try_get::<String, _>("user_id")?;
        let total_requests = row.try_get::<i64, _>("total_requests")?;
        let error_requests = row.try_get::<i64, _>("error_requests")?;
        let success_requests = total_requests.saturating_sub(error_requests);
        sqlx::query(UPSERT_STATS_HOURLY_USER_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(hour_utc)
            .bind(user_id)
            .bind(total_requests)
            .bind(success_requests)
            .bind(error_requests)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
    }

    Ok(rows.len())
}

async fn upsert_stats_hourly_model_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    hour_utc: DateTime<Utc>,
    hour_end: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_HOURLY_MODEL_AGGREGATES_SQL)
        .bind(hour_utc)
        .bind(hour_end)
        .fetch_all(&mut **tx)
        .await?;
    let mut inserted = 0usize;

    for row in &rows {
        let model = row.try_get::<Option<String>, _>("model")?;
        let Some(model) = model.filter(|value| !value.is_empty()) else {
            continue;
        };
        sqlx::query(UPSERT_STATS_HOURLY_MODEL_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(hour_utc)
            .bind(model)
            .bind(row.try_get::<i64, _>("total_requests")?)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(row.try_get::<f64, _>("avg_response_time_ms")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
        inserted += 1;
    }

    Ok(inserted)
}

async fn upsert_stats_hourly_provider_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    hour_utc: DateTime<Utc>,
    hour_end: DateTime<Utc>,
    now_utc: DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(SELECT_STATS_HOURLY_PROVIDER_AGGREGATES_SQL)
        .bind(hour_utc)
        .bind(hour_end)
        .fetch_all(&mut **tx)
        .await?;
    let mut inserted = 0usize;

    for row in &rows {
        let provider_name = row.try_get::<Option<String>, _>("provider_name")?;
        let Some(provider_name) = provider_name.filter(|value| !value.is_empty()) else {
            continue;
        };
        sqlx::query(UPSERT_STATS_HOURLY_PROVIDER_SQL)
            .bind(Uuid::new_v4().to_string())
            .bind(hour_utc)
            .bind(provider_name)
            .bind(row.try_get::<i64, _>("total_requests")?)
            .bind(row.try_get::<i64, _>("input_tokens")?)
            .bind(row.try_get::<i64, _>("output_tokens")?)
            .bind(row.try_get::<f64, _>("total_cost")?)
            .bind(now_utc)
            .bind(now_utc)
            .execute(&mut **tx)
            .await?;
        inserted += 1;
    }

    Ok(inserted)
}

pub(crate) async fn perform_provider_checkin_once(
    state: &AppState,
) -> Result<ProviderCheckinRunSummary, GatewayError> {
    if !system_config_bool(&state.data, "enable_provider_checkin", true)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
    {
        return Ok(ProviderCheckinRunSummary {
            attempted: 0,
            succeeded: 0,
            failed: 0,
            skipped: 0,
        });
    }

    let providers = state
        .list_provider_catalog_providers(true)
        .await?
        .into_iter()
        .filter(provider_has_ops_config)
        .collect::<Vec<_>>();
    if providers.is_empty() {
        return Ok(ProviderCheckinRunSummary {
            attempted: 0,
            succeeded: 0,
            failed: 0,
            skipped: 0,
        });
    }

    let provider_ids = providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<Vec<_>>();
    let mut endpoints_by_provider = HashMap::<String, Vec<StoredProviderCatalogEndpoint>>::new();
    for endpoint in state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?
    {
        endpoints_by_provider
            .entry(endpoint.provider_id.clone())
            .or_default()
            .push(endpoint);
    }

    let mut results = stream::iter(providers.into_iter().map(|provider| {
        let state = state.clone();
        let provider_id = provider.id.clone();
        let endpoints = endpoints_by_provider
            .remove(&provider_id)
            .unwrap_or_default();
        async move { run_provider_checkin_for_provider(&state, provider, endpoints).await }
    }))
    .buffer_unordered(PROVIDER_CHECKIN_CONCURRENCY);

    let mut summary = ProviderCheckinRunSummary {
        attempted: provider_ids.len(),
        succeeded: 0,
        failed: 0,
        skipped: 0,
    };
    while let Some(outcome) = results.next().await {
        match outcome.status {
            ProviderCheckinStatus::Succeeded => summary.succeeded += 1,
            ProviderCheckinStatus::Failed => {
                summary.failed += 1;
                warn!(
                    provider_id = %outcome.provider_id,
                    message = %outcome.message,
                    "gateway provider checkin failed"
                );
            }
            ProviderCheckinStatus::Skipped => {
                summary.skipped += 1;
                debug!(
                    provider_id = %outcome.provider_id,
                    message = %outcome.message,
                    "gateway provider checkin skipped"
                );
            }
        }
    }

    Ok(summary)
}

pub(crate) fn spawn_audit_cleanup_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = run_audit_cleanup_once(&data).await {
            warn!(error = %err, "gateway audit cleanup startup failed");
        }
        let mut interval = tokio::time::interval(AUDIT_LOG_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = run_audit_cleanup_once(&data).await {
                warn!(error = %err, "gateway audit cleanup tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_db_maintenance_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    let timezone = maintenance_timezone();
    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(duration_until_next_db_maintenance_run(Utc::now(), timezone)).await;
            if let Err(err) = run_db_maintenance_once(&data).await {
                warn!(error = %err, "gateway db maintenance tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_wallet_daily_usage_aggregation_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    let timezone = maintenance_timezone();
    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(duration_until_next_daily_run(
                Utc::now(),
                timezone,
                WALLET_DAILY_USAGE_AGGREGATION_HOUR,
                WALLET_DAILY_USAGE_AGGREGATION_MINUTE,
            ))
            .await;
            if let Err(err) = run_wallet_daily_usage_aggregation_once(&data).await {
                warn!(error = %err, "gateway wallet daily usage aggregation tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_stats_aggregation_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(duration_until_next_stats_aggregation_run(Utc::now())).await;
            if let Err(err) = run_stats_aggregation_once(&data).await {
                warn!(error = %err, "gateway stats aggregation tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_usage_cleanup_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    let timezone = maintenance_timezone();
    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(duration_until_next_daily_run(
                Utc::now(),
                timezone,
                USAGE_CLEANUP_HOUR,
                USAGE_CLEANUP_MINUTE,
            ))
            .await;
            if let Err(err) = run_usage_cleanup_once(&data).await {
                warn!(error = %err, "gateway usage cleanup tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_provider_checkin_worker(
    state: AppState,
) -> Option<tokio::task::JoinHandle<()>> {
    if !state.has_provider_catalog_data_reader() {
        return None;
    }

    let timezone = maintenance_timezone();
    Some(tokio::spawn(async move {
        loop {
            let (hour, minute) = match provider_checkin_schedule(&state.data).await {
                Ok(schedule) => schedule,
                Err(err) => {
                    warn!(
                        error = %err,
                        fallback = PROVIDER_CHECKIN_DEFAULT_TIME,
                        "gateway provider checkin schedule lookup failed; falling back"
                    );
                    parse_hhmm_time(PROVIDER_CHECKIN_DEFAULT_TIME)
                        .expect("default provider checkin time should parse")
                }
            };
            tokio::time::sleep(duration_until_next_daily_run(
                Utc::now(),
                timezone,
                hour,
                minute,
            ))
            .await;
            if let Err(err) = run_provider_checkin_once(&state).await {
                warn!(error = ?err, "gateway provider checkin tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_gemini_file_mapping_cleanup_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !data.has_gemini_file_mapping_writer() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = run_gemini_file_mapping_cleanup_once(&data).await {
            warn!(error = %err, "gateway gemini file mapping cleanup startup failed");
        }
        let mut interval = tokio::time::interval(GEMINI_FILE_MAPPING_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = run_gemini_file_mapping_cleanup_once(&data).await {
                warn!(error = %err, "gateway gemini file mapping cleanup tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_pending_cleanup_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = run_pending_cleanup_once(&data).await {
            warn!(error = %err, "gateway pending cleanup startup failed");
        }
        let mut interval = tokio::time::interval(PENDING_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = run_pending_cleanup_once(&data).await {
                warn!(error = %err, "gateway pending cleanup tick failed");
            }
        }
    }))
}

pub(crate) fn spawn_pool_monitor_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(POOL_MONITOR_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            run_pool_monitor_once(&data);
        }
    }))
}

pub(crate) fn spawn_stats_hourly_aggregation_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if data.postgres_pool().is_none() {
        return None;
    }

    Some(tokio::spawn(async move {
        loop {
            tokio::time::sleep(duration_until_next_stats_hourly_aggregation_run(Utc::now())).await;
            if let Err(err) = run_stats_hourly_aggregation_once(&data).await {
                warn!(error = %err, "gateway stats hourly aggregation tick failed");
            }
        }
    }))
}

pub(crate) async fn cleanup_request_candidates_once(
    data: &GatewayDataState,
) -> Result<usize, aether_data::DataLayerError> {
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(0);
    }

    let detail_log_retention_days = system_config_u64(data, "detail_log_retention_days", 7).await?;
    let retention_days = system_config_u64(
        data,
        "request_candidates_retention_days",
        detail_log_retention_days,
    )
    .await?
    .max(3);
    let cleanup_batch_size = system_config_usize(data, "cleanup_batch_size", 1_000).await?;
    let delete_limit = system_config_usize(
        data,
        "request_candidates_cleanup_batch_size",
        cleanup_batch_size.max(1),
    )
    .await?
    .max(1);
    let cutoff_unix_secs = now_unix_secs().saturating_sub(retention_days.saturating_mul(86_400));

    let mut total_deleted = 0usize;
    loop {
        let deleted = data
            .delete_request_candidates_created_before(cutoff_unix_secs, delete_limit)
            .await?;
        total_deleted += deleted;
        if deleted < delete_limit {
            break;
        }
    }

    Ok(total_deleted)
}

pub(crate) fn spawn_request_candidate_cleanup_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !data.has_request_candidate_writer() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = run_request_candidate_cleanup_once(&data).await {
            warn!(error = %err, "gateway request candidate cleanup startup failed");
        }
        let mut interval = tokio::time::interval(REQUEST_CANDIDATE_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = run_request_candidate_cleanup_once(&data).await {
                warn!(error = %err, "gateway request candidate cleanup tick failed");
            }
        }
    }))
}

async fn run_audit_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let deleted = cleanup_audit_logs_once(data).await?;
    if deleted > 0 {
        info!(deleted, "gateway deleted expired audit logs");
    }
    Ok(())
}

async fn run_gemini_file_mapping_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let deleted = cleanup_expired_gemini_file_mappings_once(data).await?;
    if deleted > 0 {
        info!(deleted, "gateway deleted expired gemini file mappings");
    }
    Ok(())
}

async fn run_db_maintenance_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let summary = perform_db_maintenance_once(data).await?;
    if summary.attempted > 0 {
        info!(
            attempted = summary.attempted,
            succeeded = summary.succeeded,
            failed = summary.attempted.saturating_sub(summary.succeeded),
            "gateway finished db maintenance"
        );
    }
    Ok(())
}

async fn run_wallet_daily_usage_aggregation_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let summary = perform_wallet_daily_usage_aggregation_once(data).await?;
    info!(
        billing_date = %summary.billing_date,
        billing_timezone = %summary.billing_timezone,
        wallets = summary.aggregated_wallets,
        stale_deleted = summary.deleted_stale_ledgers,
        "gateway aggregated wallet daily usage ledgers"
    );
    Ok(())
}

async fn run_stats_aggregation_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let Some(summary) = perform_stats_aggregation_once(data).await? else {
        return Ok(());
    };

    info!(
        day_start_utc = %summary.day_start_utc,
        total_requests = summary.total_requests,
        model_rows = summary.model_rows,
        provider_rows = summary.provider_rows,
        api_key_rows = summary.api_key_rows,
        error_rows = summary.error_rows,
        user_rows = summary.user_rows,
        "gateway aggregated daily stats tables"
    );
    Ok(())
}

async fn run_usage_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let summary = perform_usage_cleanup_once(data).await?;
    if summary.body_compressed > 0
        || summary.body_cleaned > 0
        || summary.header_cleaned > 0
        || summary.keys_cleaned > 0
        || summary.records_deleted > 0
    {
        info!(
            body_compressed = summary.body_compressed,
            body_cleaned = summary.body_cleaned,
            header_cleaned = summary.header_cleaned,
            keys_cleaned = summary.keys_cleaned,
            records_deleted = summary.records_deleted,
            "gateway finished usage cleanup"
        );
    }
    Ok(())
}

fn run_pool_monitor_once(data: &GatewayDataState) {
    let Some(summary) = summarize_postgres_pool(data) else {
        return;
    };

    info!(
        checked_out = summary.checked_out,
        pool_size = summary.pool_size,
        idle = summary.idle,
        max_connections = summary.max_connections,
        usage_rate = summary.usage_rate,
        "gateway postgres pool status"
    );
}

async fn run_pending_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let summary = cleanup_stale_pending_requests_once(data).await?;
    if summary.failed > 0 || summary.recovered > 0 {
        info!(
            failed = summary.failed,
            recovered = summary.recovered,
            "gateway cleaned stale pending and streaming requests"
        );
    }
    Ok(())
}

async fn run_stats_hourly_aggregation_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let Some(summary) = perform_stats_hourly_aggregation_once(data).await? else {
        return Ok(());
    };

    info!(
        hour_utc = %summary.hour_utc,
        total_requests = summary.total_requests,
        user_rows = summary.user_rows,
        model_rows = summary.model_rows,
        provider_rows = summary.provider_rows,
        "gateway aggregated stats hourly tables"
    );
    Ok(())
}

async fn run_provider_checkin_once(state: &AppState) -> Result<(), GatewayError> {
    let summary = perform_provider_checkin_once(state).await?;
    if summary.attempted > 0 {
        info!(
            attempted = summary.attempted,
            succeeded = summary.succeeded,
            failed = summary.failed,
            skipped = summary.skipped,
            "gateway finished provider checkin"
        );
    }
    Ok(())
}

async fn run_request_candidate_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), aether_data::DataLayerError> {
    let deleted = cleanup_request_candidates_once(data).await?;
    if deleted > 0 {
        info!(deleted, "gateway deleted expired request candidates");
    }
    Ok(())
}

async fn system_config_bool(
    data: &GatewayDataState,
    key: &str,
    default: bool,
) -> Result<bool, aether_data::DataLayerError> {
    Ok(match data.find_system_config_value(key).await? {
        Some(Value::Bool(value)) => value,
        Some(Value::Number(value)) => value.as_i64().map(|raw| raw != 0).unwrap_or(default),
        Some(Value::String(value)) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        },
        _ => default,
    })
}

async fn system_config_u64(
    data: &GatewayDataState,
    key: &str,
    default: u64,
) -> Result<u64, aether_data::DataLayerError> {
    Ok(match data.find_system_config_value(key).await? {
        Some(Value::Number(value)) => value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|raw| u64::try_from(raw).ok()))
            .unwrap_or(default),
        Some(Value::String(value)) => value.trim().parse::<u64>().unwrap_or(default),
        _ => default,
    })
}

async fn system_config_usize(
    data: &GatewayDataState,
    key: &str,
    default: usize,
) -> Result<usize, aether_data::DataLayerError> {
    Ok(match data.find_system_config_value(key).await? {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|raw| usize::try_from(raw).ok())
            .or_else(|| {
                value
                    .as_i64()
                    .and_then(|raw| u64::try_from(raw).ok())
                    .and_then(|raw| usize::try_from(raw).ok())
            })
            .unwrap_or(default),
        Some(Value::String(value)) => value.trim().parse::<usize>().unwrap_or(default),
        _ => default,
    })
}

async fn system_config_string(
    data: &GatewayDataState,
    key: &str,
    default: &str,
) -> Result<String, aether_data::DataLayerError> {
    Ok(match data.find_system_config_value(key).await? {
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                default.to_string()
            } else {
                trimmed.to_string()
            }
        }
        _ => default.to_string(),
    })
}

async fn pending_cleanup_timeout_minutes(
    data: &GatewayDataState,
) -> Result<u64, aether_data::DataLayerError> {
    system_config_u64(data, "pending_request_timeout_minutes", 10).await
}

async fn pending_cleanup_batch_size(
    data: &GatewayDataState,
) -> Result<usize, aether_data::DataLayerError> {
    Ok(system_config_usize(data, "cleanup_batch_size", 1_000)
        .await?
        .max(1)
        .min(200))
}

async fn usage_cleanup_settings(
    data: &GatewayDataState,
) -> Result<UsageCleanupSettings, aether_data::DataLayerError> {
    Ok(UsageCleanupSettings {
        detail_retention_days: system_config_u64(data, "detail_log_retention_days", 7).await?,
        compressed_retention_days: system_config_u64(data, "compressed_log_retention_days", 30)
            .await?,
        header_retention_days: system_config_u64(data, "header_retention_days", 90).await?,
        log_retention_days: system_config_u64(data, "log_retention_days", 365).await?,
        batch_size: system_config_usize(data, "cleanup_batch_size", 1_000)
            .await?
            .max(1),
        auto_delete_expired_keys: system_config_bool(data, "auto_delete_expired_keys", false)
            .await?,
    })
}

fn usage_cleanup_window(
    now_utc: DateTime<Utc>,
    settings: UsageCleanupSettings,
) -> UsageCleanupWindow {
    let minutes = |days: u64| chrono::Duration::days(i64::try_from(days).unwrap_or(i64::MAX));
    UsageCleanupWindow {
        detail_cutoff: now_utc - minutes(settings.detail_retention_days),
        compressed_cutoff: now_utc - minutes(settings.compressed_retention_days),
        header_cutoff: now_utc - minutes(settings.header_retention_days),
        log_cutoff: now_utc - minutes(settings.log_retention_days),
    }
}

fn plan_pending_cleanup_batch(
    stale_rows: Vec<StalePendingUsageRow>,
    completed_request_ids: &HashSet<String>,
    timeout_minutes: u64,
) -> PendingCleanupBatchPlan {
    let mut plan = PendingCleanupBatchPlan::default();
    for row in stale_rows {
        if completed_request_ids.contains(&row.request_id) {
            plan.recovered_usage_ids.push(row.id);
            plan.recovered_request_ids.push(row.request_id);
            continue;
        }

        plan.failed_request_ids.push(row.request_id);
        plan.failed_usage_rows.push(FailedPendingUsageRow {
            id: row.id,
            error_message: format!(
                "请求超时: 状态 '{}' 超过 {} 分钟未完成",
                row.status, timeout_minutes
            ),
            should_void_billing: row.billing_status == "pending",
        });
    }
    plan
}

async fn delete_old_usage_records(
    pool: &aether_data::postgres::PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
) -> Result<usize, aether_data::DataLayerError> {
    let mut total_deleted = 0usize;
    loop {
        let deleted = sqlx::query(DELETE_OLD_USAGE_RECORDS_SQL)
            .bind(cutoff_time)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .execute(pool)
            .await?
            .rows_affected();
        let deleted = usize::try_from(deleted).unwrap_or(usize::MAX);
        total_deleted += deleted;
        if deleted < batch_size {
            break;
        }
    }
    Ok(total_deleted)
}

async fn cleanup_usage_header_fields(
    pool: &aether_data::postgres::PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
) -> Result<usize, aether_data::DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            "gateway usage header cleanup skipped due to invalid window"
        );
        return Ok(0);
    }

    let mut total_cleaned = 0usize;
    loop {
        let ids = sqlx::query(SELECT_USAGE_HEADER_BATCH_SQL)
            .bind(cutoff_time)
            .bind(newer_than)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<String, _>("id"))
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        if ids.is_empty() {
            break;
        }

        let cleaned = sqlx::query(CLEAR_USAGE_HEADER_FIELDS_SQL)
            .bind(ids)
            .execute(pool)
            .await?
            .rows_affected();
        let cleaned = usize::try_from(cleaned).unwrap_or(usize::MAX);
        total_cleaned += cleaned;
        if cleaned == 0 || cleaned < batch_size {
            break;
        }
    }
    Ok(total_cleaned)
}

async fn cleanup_usage_stale_body_fields(
    pool: &aether_data::postgres::PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
) -> Result<usize, aether_data::DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            "gateway usage body cleanup skipped due to invalid window"
        );
        return Ok(0);
    }

    let mut total_cleaned = 0usize;
    loop {
        let ids = sqlx::query(SELECT_USAGE_STALE_BODY_BATCH_SQL)
            .bind(cutoff_time)
            .bind(newer_than)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| row.try_get::<String, _>("id"))
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        if ids.is_empty() {
            break;
        }

        let cleaned = sqlx::query(CLEAR_USAGE_BODY_FIELDS_SQL)
            .bind(ids)
            .execute(pool)
            .await?
            .rows_affected();
        let cleaned = usize::try_from(cleaned).unwrap_or(usize::MAX);
        total_cleaned += cleaned;
        if cleaned == 0 || cleaned < batch_size {
            break;
        }
    }
    Ok(total_cleaned)
}

async fn compress_usage_body_fields(
    pool: &aether_data::postgres::PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
) -> Result<usize, aether_data::DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            "gateway usage body compression skipped due to invalid window"
        );
        return Ok(0);
    }

    let mut total_compressed = 0usize;
    let mut no_progress_count = 0usize;
    let batch_size = batch_size.max(1).min(25);
    loop {
        let rows = sqlx::query(SELECT_USAGE_BODY_COMPRESSION_BATCH_SQL)
            .bind(cutoff_time)
            .bind(newer_than)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(UsageBodyCompressionRow {
                    id: row.try_get::<String, _>("id")?,
                    request_body: row.try_get::<Option<Value>, _>("request_body")?,
                    response_body: row.try_get::<Option<Value>, _>("response_body")?,
                    provider_request_body: row
                        .try_get::<Option<Value>, _>("provider_request_body")?,
                    client_response_body: row
                        .try_get::<Option<Value>, _>("client_response_body")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        if rows.is_empty() {
            break;
        }

        let mut batch_success = 0usize;
        for row in rows {
            let compressed = (
                compress_usage_json_value(row.request_body.as_ref()),
                compress_usage_json_value(row.response_body.as_ref()),
                compress_usage_json_value(row.provider_request_body.as_ref()),
                compress_usage_json_value(row.client_response_body.as_ref()),
            );
            let updated = sqlx::query(UPDATE_USAGE_BODY_COMPRESSION_SQL)
                .bind(row.id)
                .bind(compressed.0)
                .bind(compressed.1)
                .bind(compressed.2)
                .bind(compressed.3)
                .execute(pool)
                .await?
                .rows_affected();
            if updated > 0 {
                batch_success += 1;
            }
        }

        if batch_success == 0 {
            no_progress_count += 1;
            if no_progress_count >= 3 {
                warn!(
                    "gateway usage body compression stopped after repeated zero-progress batches"
                );
                break;
            }
        } else {
            no_progress_count = 0;
        }
        total_compressed += batch_success;
    }
    Ok(total_compressed)
}

fn compress_usage_json_value(value: Option<&Value>) -> Option<Vec<u8>> {
    let value = value?;
    let bytes = serde_json::to_vec(value).ok()?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(&bytes).ok()?;
    encoder.finish().ok()
}

async fn cleanup_expired_api_keys(
    pool: &aether_data::postgres::PostgresPool,
    auto_delete_expired_keys: bool,
) -> Result<usize, aether_data::DataLayerError> {
    let expired_keys = sqlx::query(SELECT_EXPIRED_ACTIVE_API_KEYS_SQL)
        .fetch_all(pool)
        .await?;
    let mut cleaned = 0usize;
    for row in &expired_keys {
        let api_key_id = row.try_get::<String, _>("id")?;
        let key = ExpiredApiKeyRow {
            id: api_key_id.as_str(),
            auto_delete_on_expiry: row.try_get::<Option<bool>, _>("auto_delete_on_expiry")?,
        };
        let should_delete = key
            .auto_delete_on_expiry
            .unwrap_or(auto_delete_expired_keys);
        if should_delete {
            nullify_expired_api_key_usage_refs(pool, key.id).await?;
            nullify_expired_api_key_candidate_refs(pool, key.id).await?;
            let deleted = sqlx::query(DELETE_EXPIRED_API_KEY_SQL)
                .bind(key.id)
                .execute(pool)
                .await?
                .rows_affected();
            if deleted > 0 {
                cleaned += 1;
            }
        } else {
            let updated = sqlx::query(DISABLE_EXPIRED_API_KEY_SQL)
                .bind(key.id)
                .bind(Utc::now())
                .execute(pool)
                .await?
                .rows_affected();
            if updated > 0 {
                cleaned += 1;
            }
        }
    }
    Ok(cleaned)
}

async fn nullify_expired_api_key_usage_refs(
    pool: &aether_data::postgres::PostgresPool,
    api_key_id: &str,
) -> Result<(), aether_data::DataLayerError> {
    loop {
        let updated = sqlx::query(NULLIFY_USAGE_API_KEY_BATCH_SQL)
            .bind(api_key_id)
            .bind(i64::try_from(EXPIRED_API_KEY_PRE_CLEAN_BATCH_SIZE).unwrap_or(i64::MAX))
            .execute(pool)
            .await?
            .rows_affected();
        let updated = usize::try_from(updated).unwrap_or(usize::MAX);
        if updated < EXPIRED_API_KEY_PRE_CLEAN_BATCH_SIZE {
            break;
        }
    }
    Ok(())
}

async fn nullify_expired_api_key_candidate_refs(
    pool: &aether_data::postgres::PostgresPool,
    api_key_id: &str,
) -> Result<(), aether_data::DataLayerError> {
    loop {
        let updated = sqlx::query(NULLIFY_REQUEST_CANDIDATE_API_KEY_BATCH_SQL)
            .bind(api_key_id)
            .bind(i64::try_from(EXPIRED_API_KEY_PRE_CLEAN_BATCH_SIZE).unwrap_or(i64::MAX))
            .execute(pool)
            .await?
            .rows_affected();
        let updated = usize::try_from(updated).unwrap_or(usize::MAX);
        if updated < EXPIRED_API_KEY_PRE_CLEAN_BATCH_SIZE {
            break;
        }
    }
    Ok(())
}

async fn cleanup_audit_logs_with<F, Fut>(
    data: &GatewayDataState,
    mut delete_batch: F,
) -> Result<usize, aether_data::DataLayerError>
where
    F: FnMut(DateTime<Utc>, usize) -> Fut,
    Fut: std::future::Future<Output = Result<usize, aether_data::DataLayerError>>,
{
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(0);
    }

    let retention_days = system_config_u64(data, "audit_log_retention_days", 30)
        .await?
        .max(7);
    let delete_limit = system_config_usize(data, "cleanup_batch_size", 1_000)
        .await?
        .max(1);
    let retention_days_i64 = i64::try_from(retention_days).unwrap_or(i64::MAX);
    let cutoff_time = Utc::now() - chrono::Duration::days(retention_days_i64);

    let mut total_deleted = 0usize;
    loop {
        let deleted = delete_batch(cutoff_time, delete_limit).await?;
        total_deleted += deleted;
        if deleted < delete_limit {
            break;
        }
    }

    Ok(total_deleted)
}

async fn run_db_maintenance_with<F, Fut>(
    data: &GatewayDataState,
    mut vacuum_table: F,
) -> Result<DbMaintenanceRunSummary, aether_data::DataLayerError>
where
    F: FnMut(&'static str) -> Fut,
    Fut: std::future::Future<Output = Result<(), aether_data::DataLayerError>>,
{
    if !system_config_bool(data, "enable_db_maintenance", true).await? {
        return Ok(DbMaintenanceRunSummary {
            attempted: 0,
            succeeded: 0,
        });
    }

    let mut summary = DbMaintenanceRunSummary {
        attempted: 0,
        succeeded: 0,
    };
    for table_name in DB_MAINTENANCE_TABLES {
        summary.attempted += 1;
        match vacuum_table(table_name).await {
            Ok(()) => summary.succeeded += 1,
            Err(err) => {
                warn!(table = table_name, error = %err, "gateway db maintenance table failed");
            }
        }
    }
    Ok(summary)
}

async fn provider_checkin_schedule(
    data: &GatewayDataState,
) -> Result<(u32, u32), aether_data::DataLayerError> {
    let configured =
        system_config_string(data, "provider_checkin_time", PROVIDER_CHECKIN_DEFAULT_TIME).await?;
    Ok(parse_hhmm_time(&configured).unwrap_or_else(|| {
        warn!(
            value = %configured,
            fallback = PROVIDER_CHECKIN_DEFAULT_TIME,
            "gateway provider checkin time invalid; falling back"
        );
        parse_hhmm_time(PROVIDER_CHECKIN_DEFAULT_TIME)
            .expect("default provider checkin time should parse")
    }))
}

fn maintenance_timezone() -> Tz {
    let configured = std::env::var("APP_TIMEZONE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| MAINTENANCE_DEFAULT_TIMEZONE.to_string());
    configured.parse().unwrap_or_else(|_| {
        warn!(
            timezone = %configured,
            fallback = MAINTENANCE_DEFAULT_TIMEZONE,
            "gateway maintenance timezone invalid; falling back"
        );
        MAINTENANCE_DEFAULT_TIMEZONE
            .parse()
            .expect("default maintenance timezone should parse")
    })
}

fn duration_until_next_db_maintenance_run(now_utc: DateTime<Utc>, timezone: Tz) -> Duration {
    next_db_maintenance_run_after(now_utc, timezone)
        .signed_duration_since(now_utc)
        .to_std()
        .unwrap_or_default()
}

fn next_db_maintenance_run_after(now_utc: DateTime<Utc>, timezone: Tz) -> DateTime<Utc> {
    let local_now = now_utc.with_timezone(&timezone);
    for day_offset in 0..=7 {
        let candidate_date = local_now.date_naive() + chrono::Duration::days(day_offset);
        if candidate_date.weekday() != DB_MAINTENANCE_WEEKDAY {
            continue;
        }
        let Some(candidate_naive) =
            candidate_date.and_hms_opt(DB_MAINTENANCE_HOUR, DB_MAINTENANCE_MINUTE, 0)
        else {
            continue;
        };
        let candidate_local = resolve_local_scheduled_time(timezone, candidate_naive);
        let Some(candidate_local) = candidate_local else {
            continue;
        };
        if candidate_local > local_now {
            return candidate_local.with_timezone(&Utc);
        }
    }

    let fallback_date = local_now.date_naive() + DB_MAINTENANCE_WEEKLY_INTERVAL;
    let fallback_naive = fallback_date
        .and_hms_opt(DB_MAINTENANCE_HOUR, DB_MAINTENANCE_MINUTE, 0)
        .expect("db maintenance fallback time should be valid");
    let fallback_local = resolve_local_scheduled_time(timezone, fallback_naive)
        .expect("db maintenance fallback local datetime should resolve");
    fallback_local.with_timezone(&Utc)
}

fn duration_until_next_daily_run(
    now_utc: DateTime<Utc>,
    timezone: Tz,
    hour: u32,
    minute: u32,
) -> Duration {
    next_daily_run_after(now_utc, timezone, hour, minute)
        .signed_duration_since(now_utc)
        .to_std()
        .unwrap_or_default()
}

fn duration_until_next_stats_aggregation_run(now_utc: DateTime<Utc>) -> Duration {
    next_stats_aggregation_run_after(now_utc)
        .signed_duration_since(now_utc)
        .to_std()
        .unwrap_or_default()
}

fn duration_until_next_stats_hourly_aggregation_run(now_utc: DateTime<Utc>) -> Duration {
    next_stats_hourly_aggregation_run_after(now_utc)
        .signed_duration_since(now_utc)
        .to_std()
        .unwrap_or_default()
}

fn next_daily_run_after(
    now_utc: DateTime<Utc>,
    timezone: Tz,
    hour: u32,
    minute: u32,
) -> DateTime<Utc> {
    let local_now = now_utc.with_timezone(&timezone);
    for day_offset in 0..=1 {
        let candidate_date = local_now.date_naive() + chrono::Duration::days(day_offset);
        let Some(candidate_naive) = candidate_date.and_hms_opt(hour, minute, 0) else {
            continue;
        };
        let candidate_local = resolve_local_scheduled_time(timezone, candidate_naive);
        let Some(candidate_local) = candidate_local else {
            continue;
        };
        if candidate_local > local_now {
            return candidate_local.with_timezone(&Utc);
        }
    }

    let fallback_date = local_now.date_naive() + chrono::Duration::days(1);
    let fallback_naive = fallback_date
        .and_hms_opt(hour, minute, 0)
        .expect("daily fallback time should be valid");
    let fallback_local = resolve_local_scheduled_time(timezone, fallback_naive)
        .expect("daily fallback local datetime should resolve");
    fallback_local.with_timezone(&Utc)
}

fn next_stats_aggregation_run_after(now_utc: DateTime<Utc>) -> DateTime<Utc> {
    next_daily_run_after(
        now_utc,
        chrono_tz::UTC,
        STATS_DAILY_AGGREGATION_HOUR,
        STATS_DAILY_AGGREGATION_MINUTE,
    )
}

fn next_stats_hourly_aggregation_run_after(now_utc: DateTime<Utc>) -> DateTime<Utc> {
    let current_hour_slot = Utc.from_utc_datetime(
        &now_utc
            .date_naive()
            .and_hms_opt(now_utc.hour(), STATS_HOURLY_AGGREGATION_MINUTE, 0)
            .expect("stats hourly aggregation slot should be valid"),
    );
    if current_hour_slot > now_utc {
        return current_hour_slot;
    }

    let next_hour = now_utc + chrono::Duration::hours(1);
    Utc.from_utc_datetime(
        &next_hour
            .date_naive()
            .and_hms_opt(next_hour.hour(), STATS_HOURLY_AGGREGATION_MINUTE, 0)
            .expect("stats hourly aggregation next slot should be valid"),
    )
}

fn stats_aggregation_target_day(now_utc: DateTime<Utc>) -> DateTime<Utc> {
    let previous_day = now_utc - chrono::Duration::days(1);
    Utc.from_utc_datetime(
        &previous_day
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("stats aggregation target day should be valid"),
    )
}

fn stats_hourly_aggregation_target_hour(now_utc: DateTime<Utc>) -> DateTime<Utc> {
    let previous_hour = now_utc - chrono::Duration::hours(1);
    Utc.from_utc_datetime(
        &previous_hour
            .date_naive()
            .and_hms_opt(previous_hour.hour(), 0, 0)
            .expect("stats hourly aggregation target hour should be valid"),
    )
}

fn wallet_daily_usage_aggregation_target(
    now_utc: DateTime<Utc>,
    timezone: Tz,
) -> WalletDailyUsageAggregationTarget {
    let local_today = now_utc.with_timezone(&timezone).date_naive();
    let billing_date = local_today - chrono::Duration::days(1);
    let next_billing_date = billing_date + chrono::Duration::days(1);

    WalletDailyUsageAggregationTarget {
        billing_date,
        billing_timezone: timezone.to_string(),
        window_start_utc: local_day_start_utc(billing_date, timezone),
        window_end_utc: local_day_start_utc(next_billing_date, timezone),
    }
}

fn local_day_start_utc(date: chrono::NaiveDate, timezone: Tz) -> DateTime<Utc> {
    let local_start = date
        .and_hms_opt(0, 0, 0)
        .and_then(|naive| resolve_local_scheduled_time(timezone, naive))
        .expect("local day start should resolve");
    local_start.with_timezone(&Utc)
}

fn resolve_local_scheduled_time(
    timezone: Tz,
    naive: chrono::NaiveDateTime,
) -> Option<chrono::DateTime<Tz>> {
    match timezone.from_local_datetime(&naive) {
        chrono::LocalResult::Single(value) => Some(value),
        chrono::LocalResult::Ambiguous(first, second) => Some(first.min(second)),
        chrono::LocalResult::None => None,
    }
}

fn parse_hhmm_time(value: &str) -> Option<(u32, u32)> {
    let (hour, minute) = value.trim().split_once(':')?;
    let hour = hour.parse::<u32>().ok()?;
    let minute = minute.parse::<u32>().ok()?;
    (hour <= 23 && minute <= 59).then_some((hour, minute))
}

fn provider_has_ops_config(provider: &StoredProviderCatalogProvider) -> bool {
    provider
        .config
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|config| config.get("provider_ops"))
        .and_then(serde_json::Value::as_object)
        .is_some_and(|config| !config.is_empty())
}

async fn run_provider_checkin_for_provider(
    state: &AppState,
    provider: StoredProviderCatalogProvider,
    endpoints: Vec<StoredProviderCatalogEndpoint>,
) -> ProviderCheckinOutcome {
    let provider_id = provider.id.clone();
    let payload = admin_provider_ops_local_action_response(
        state,
        &provider_id,
        Some(&provider),
        &endpoints,
        "query_balance",
        None,
    )
    .await;
    provider_checkin_outcome_from_payload(&provider_id, &payload)
}

fn provider_checkin_outcome_from_payload(
    provider_id: &str,
    payload: &serde_json::Value,
) -> ProviderCheckinOutcome {
    let default_message = || "未执行签到".to_string();
    let payload_status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let payload_message = payload
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();

    let (status, message) = if payload_status == "success" {
        let extra = payload
            .get("data")
            .and_then(|value| value.get("extra"))
            .and_then(serde_json::Value::as_object);
        let checkin_message = extra
            .and_then(|extra| extra.get("checkin_message"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_message);
        match extra
            .and_then(|extra| extra.get("checkin_success"))
            .and_then(serde_json::Value::as_bool)
        {
            Some(true) => (ProviderCheckinStatus::Succeeded, checkin_message),
            Some(false) => (ProviderCheckinStatus::Failed, checkin_message),
            None => (ProviderCheckinStatus::Skipped, checkin_message),
        }
    } else if payload_status == "not_supported" {
        (
            ProviderCheckinStatus::Skipped,
            if payload_message.is_empty() {
                default_message()
            } else {
                payload_message
            },
        )
    } else {
        (
            ProviderCheckinStatus::Failed,
            if payload_message.is_empty() {
                "签到失败".to_string()
            } else {
                payload_message
            },
        )
    };

    ProviderCheckinOutcome {
        provider_id: provider_id.to_string(),
        status,
        message,
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn spawn_audit_cleanup_worker_skips_when_postgres_unavailable() {
        assert!(spawn_audit_cleanup_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_db_maintenance_worker_skips_when_postgres_unavailable() {
        assert!(spawn_db_maintenance_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_pending_cleanup_worker_skips_when_postgres_unavailable() {
        assert!(spawn_pending_cleanup_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_pool_monitor_worker_skips_when_postgres_unavailable() {
        assert!(spawn_pool_monitor_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_stats_aggregation_worker_skips_when_postgres_unavailable() {
        assert!(spawn_stats_aggregation_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_stats_hourly_aggregation_worker_skips_when_postgres_unavailable() {
        assert!(
            spawn_stats_hourly_aggregation_worker(Arc::new(GatewayDataState::disabled())).is_none()
        );
    }

    #[tokio::test]
    async fn spawn_usage_cleanup_worker_skips_when_postgres_unavailable() {
        assert!(spawn_usage_cleanup_worker(Arc::new(GatewayDataState::disabled())).is_none());
    }

    #[tokio::test]
    async fn spawn_wallet_daily_usage_aggregation_worker_skips_when_postgres_unavailable() {
        assert!(spawn_wallet_daily_usage_aggregation_worker(
            Arc::new(GatewayDataState::disabled())
        )
        .is_none());
    }

    #[tokio::test]
    async fn spawn_provider_checkin_worker_skips_when_provider_catalog_unavailable() {
        let state = AppState::new(
            "http://127.0.0.1:18084",
            Some("http://127.0.0.1:18084".to_string()),
        )
        .expect("gateway state should build")
        .with_data_state_for_tests(GatewayDataState::disabled());

        assert!(spawn_provider_checkin_worker(state).is_none());
    }

    #[tokio::test]
    async fn cleanup_audit_logs_respects_auto_cleanup_toggle() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([(
            "enable_auto_cleanup".to_string(),
            json!(false),
        )]);

        let deleted = cleanup_audit_logs_with(&data, |_cutoff_time, _delete_limit| async move {
            panic!("audit cleanup should not run when auto cleanup is disabled");
            #[allow(unreachable_code)]
            Ok(0)
        })
        .await
        .expect("audit cleanup should short-circuit");

        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn cleanup_audit_logs_uses_retention_and_batch_settings() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([
            ("enable_auto_cleanup".to_string(), json!(true)),
            ("audit_log_retention_days".to_string(), json!(21)),
            ("cleanup_batch_size".to_string(), json!(2)),
        ]);
        let observed_limits = Arc::new(Mutex::new(Vec::new()));
        let observed_cutoffs = Arc::new(Mutex::new(Vec::new()));
        let batch_results = Arc::new(Mutex::new(VecDeque::from([2usize, 1usize])));
        let started_at = Utc::now();

        let deleted = cleanup_audit_logs_with(&data, {
            let observed_limits = Arc::clone(&observed_limits);
            let observed_cutoffs = Arc::clone(&observed_cutoffs);
            let batch_results = Arc::clone(&batch_results);
            move |cutoff_time, delete_limit| {
                observed_limits
                    .lock()
                    .expect("observed limits lock")
                    .push(delete_limit);
                observed_cutoffs
                    .lock()
                    .expect("observed cutoffs lock")
                    .push(cutoff_time);
                let next = batch_results
                    .lock()
                    .expect("batch results lock")
                    .pop_front()
                    .unwrap_or_default();
                async move { Ok(next) }
            }
        })
        .await
        .expect("audit cleanup should succeed");
        let finished_at = Utc::now();

        assert_eq!(deleted, 3);
        assert_eq!(
            *observed_limits.lock().expect("observed limits lock"),
            vec![2, 2]
        );
        let observed_cutoffs = observed_cutoffs.lock().expect("observed cutoffs lock");
        assert_eq!(observed_cutoffs.len(), 2);
        let earliest_expected = started_at - chrono::Duration::days(21);
        let latest_expected = finished_at - chrono::Duration::days(21);
        for cutoff_time in observed_cutoffs.iter() {
            assert!(*cutoff_time >= earliest_expected);
            assert!(*cutoff_time <= latest_expected);
        }
    }

    #[tokio::test]
    async fn pending_cleanup_settings_use_timeout_and_cap_batch_size() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([
            ("pending_request_timeout_minutes".to_string(), json!(25)),
            ("cleanup_batch_size".to_string(), json!(500)),
        ]);

        let timeout_minutes = pending_cleanup_timeout_minutes(&data)
            .await
            .expect("timeout should resolve");
        let batch_size = pending_cleanup_batch_size(&data)
            .await
            .expect("batch size should resolve");

        assert_eq!(timeout_minutes, 25);
        assert_eq!(batch_size, 200);
    }

    #[test]
    fn pending_cleanup_plan_recovers_completed_requests_and_voids_failed_pending_billing() {
        let plan = plan_pending_cleanup_batch(
            vec![
                StalePendingUsageRow {
                    id: "usage-1".to_string(),
                    request_id: "req-1".to_string(),
                    status: "streaming".to_string(),
                    billing_status: "pending".to_string(),
                },
                StalePendingUsageRow {
                    id: "usage-2".to_string(),
                    request_id: "req-2".to_string(),
                    status: "pending".to_string(),
                    billing_status: "pending".to_string(),
                },
                StalePendingUsageRow {
                    id: "usage-3".to_string(),
                    request_id: "req-3".to_string(),
                    status: "streaming".to_string(),
                    billing_status: "settled".to_string(),
                },
            ],
            &HashSet::from(["req-1".to_string()]),
            10,
        );

        assert_eq!(plan.recovered_usage_ids, vec!["usage-1".to_string()]);
        assert_eq!(plan.recovered_request_ids, vec!["req-1".to_string()]);
        assert_eq!(
            plan.failed_request_ids,
            vec!["req-2".to_string(), "req-3".to_string()]
        );
        assert_eq!(
            plan.failed_usage_rows,
            vec![
                FailedPendingUsageRow {
                    id: "usage-2".to_string(),
                    error_message: "请求超时: 状态 'pending' 超过 10 分钟未完成".to_string(),
                    should_void_billing: true,
                },
                FailedPendingUsageRow {
                    id: "usage-3".to_string(),
                    error_message: "请求超时: 状态 'streaming' 超过 10 分钟未完成".to_string(),
                    should_void_billing: false,
                },
            ]
        );
    }

    #[tokio::test]
    async fn usage_cleanup_settings_resolve_batch_and_delete_toggle() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([
            ("detail_log_retention_days".to_string(), json!(7)),
            ("compressed_log_retention_days".to_string(), json!(30)),
            ("header_retention_days".to_string(), json!(90)),
            ("log_retention_days".to_string(), json!(365)),
            ("cleanup_batch_size".to_string(), json!(0)),
            ("auto_delete_expired_keys".to_string(), json!(true)),
        ]);

        let settings = usage_cleanup_settings(&data)
            .await
            .expect("usage cleanup settings should resolve");

        assert_eq!(
            settings,
            UsageCleanupSettings {
                detail_retention_days: 7,
                compressed_retention_days: 30,
                header_retention_days: 90,
                log_retention_days: 365,
                batch_size: 1,
                auto_delete_expired_keys: true,
            }
        );
    }

    #[test]
    fn usage_cleanup_window_uses_non_overlapping_ranges() {
        let now_utc = "2026-03-18T03:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");
        let window = usage_cleanup_window(
            now_utc,
            UsageCleanupSettings {
                detail_retention_days: 7,
                compressed_retention_days: 30,
                header_retention_days: 90,
                log_retention_days: 365,
                batch_size: 123,
                auto_delete_expired_keys: false,
            },
        );

        assert_eq!(
            window.detail_cutoff.to_rfc3339(),
            "2026-03-11T03:00:00+00:00"
        );
        assert_eq!(
            window.compressed_cutoff.to_rfc3339(),
            "2026-02-16T03:00:00+00:00"
        );
        assert_eq!(
            window.header_cutoff.to_rfc3339(),
            "2025-12-18T03:00:00+00:00"
        );
        assert_eq!(window.log_cutoff.to_rfc3339(), "2025-03-18T03:00:00+00:00");
        assert!(window.detail_cutoff > window.compressed_cutoff);
        assert!(window.compressed_cutoff > window.log_cutoff);
    }

    #[test]
    fn summarize_postgres_pool_uses_busy_connections_for_usage_rate() {
        let data = GatewayDataState::from_config(
            crate::gateway::data::GatewayDataConfig::from_postgres_config(
                aether_data::postgres::PostgresPoolConfig {
                    database_url: "postgres://localhost/aether".to_string(),
                    min_connections: 1,
                    max_connections: 8,
                    acquire_timeout_ms: 1_000,
                    idle_timeout_ms: 5_000,
                    max_lifetime_ms: 30_000,
                    statement_cache_capacity: 64,
                    require_ssl: false,
                },
            ),
        )
        .expect("gateway data state should build");

        let summary = summarize_postgres_pool(&data).expect("pool summary should exist");

        assert_eq!(summary.checked_out, 0);
        assert_eq!(summary.pool_size, 0);
        assert_eq!(summary.idle, 0);
        assert_eq!(summary.max_connections, 8);
        assert_eq!(summary.usage_rate, 0.0);
    }

    #[test]
    fn stats_aggregation_target_uses_previous_utc_day() {
        let now_utc = "2026-04-05T10:20:30Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let target = stats_aggregation_target_day(now_utc);

        assert_eq!(target.to_rfc3339(), "2026-04-04T00:00:00+00:00");
    }

    #[test]
    fn next_stats_aggregation_run_aligns_to_same_day_when_before_slot() {
        let now_utc = "2026-04-05T00:04:59Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_stats_aggregation_run_after(now_utc);

        assert_eq!(next.to_rfc3339(), "2026-04-05T00:05:00+00:00");
    }

    #[test]
    fn next_stats_aggregation_run_rolls_to_next_day_after_slot() {
        let now_utc = "2026-04-05T00:05:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_stats_aggregation_run_after(now_utc);

        assert_eq!(next.to_rfc3339(), "2026-04-06T00:05:00+00:00");
    }

    #[test]
    fn stats_hourly_aggregation_target_uses_previous_utc_hour() {
        let now_utc = "2026-04-05T10:20:30Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let target = stats_hourly_aggregation_target_hour(now_utc);

        assert_eq!(target.to_rfc3339(), "2026-04-05T09:00:00+00:00");
    }

    #[test]
    fn next_stats_hourly_aggregation_run_aligns_to_same_hour_when_before_slot() {
        let now_utc = "2026-04-05T10:04:59Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_stats_hourly_aggregation_run_after(now_utc);

        assert_eq!(next.to_rfc3339(), "2026-04-05T10:05:00+00:00");
    }

    #[test]
    fn next_stats_hourly_aggregation_run_rolls_to_next_hour_after_slot() {
        let now_utc = "2026-04-05T10:05:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_stats_hourly_aggregation_run_after(now_utc);

        assert_eq!(next.to_rfc3339(), "2026-04-05T11:05:00+00:00");
    }

    #[tokio::test]
    async fn db_maintenance_respects_enable_toggle() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([(
            "enable_db_maintenance".to_string(),
            json!(false),
        )]);

        let summary = run_db_maintenance_with(&data, |_table_name| async move {
            panic!("db maintenance should not run when disabled");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .expect("db maintenance should short-circuit");

        assert_eq!(
            summary,
            DbMaintenanceRunSummary {
                attempted: 0,
                succeeded: 0,
            }
        );
    }

    #[tokio::test]
    async fn db_maintenance_continues_across_table_failures() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([(
            "enable_db_maintenance".to_string(),
            json!(true),
        )]);
        let seen_tables = Arc::new(Mutex::new(Vec::new()));

        let summary = run_db_maintenance_with(&data, {
            let seen_tables = Arc::clone(&seen_tables);
            move |table_name| {
                seen_tables
                    .lock()
                    .expect("seen tables lock")
                    .push(table_name.to_string());
                async move {
                    if table_name == "request_candidates" {
                        Err(aether_data::DataLayerError::InvalidInput(
                            "boom".to_string(),
                        ))
                    } else {
                        Ok(())
                    }
                }
            }
        })
        .await
        .expect("db maintenance should continue after failures");

        assert_eq!(
            summary,
            DbMaintenanceRunSummary {
                attempted: 3,
                succeeded: 2,
            }
        );
        assert_eq!(
            *seen_tables.lock().expect("seen tables lock"),
            vec![
                "usage".to_string(),
                "request_candidates".to_string(),
                "audit_logs".to_string(),
            ]
        );
    }

    #[test]
    fn next_db_maintenance_run_aligns_to_same_week_when_before_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-04-03T20:59:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_db_maintenance_run_after(now_utc, timezone);

        assert_eq!(next.to_rfc3339(), "2026-04-04T21:00:00+00:00");
    }

    #[test]
    fn next_db_maintenance_run_rolls_to_next_week_after_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-04-04T21:00:01Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_db_maintenance_run_after(now_utc, timezone);

        assert_eq!(next.to_rfc3339(), "2026-04-11T21:00:00+00:00");
    }

    #[test]
    fn wallet_daily_usage_aggregation_target_uses_previous_local_day_window() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-31T16:15:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let target = wallet_daily_usage_aggregation_target(now_utc, timezone);

        assert_eq!(target.billing_date.to_string(), "2026-03-31");
        assert_eq!(target.billing_timezone, "Asia/Shanghai");
        assert_eq!(
            target.window_start_utc.to_rfc3339(),
            "2026-03-30T16:00:00+00:00"
        );
        assert_eq!(
            target.window_end_utc.to_rfc3339(),
            "2026-03-31T16:00:00+00:00"
        );
    }

    #[tokio::test]
    async fn provider_checkin_schedule_uses_default_for_invalid_value() {
        let data = GatewayDataState::disabled().with_system_config_values_for_tests([(
            "provider_checkin_time".to_string(),
            json!("25:99"),
        )]);

        let schedule = provider_checkin_schedule(&data)
            .await
            .expect("provider checkin schedule should resolve");

        assert_eq!(schedule, (1, 5));
    }

    #[test]
    fn next_provider_checkin_run_aligns_to_same_day_when_before_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-31T16:59:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_daily_run_after(now_utc, timezone, 1, 5);

        assert_eq!(next.to_rfc3339(), "2026-03-31T17:05:00+00:00");
    }

    #[test]
    fn next_provider_checkin_run_rolls_to_next_day_after_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-31T17:05:01Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_daily_run_after(now_utc, timezone, 1, 5);

        assert_eq!(next.to_rfc3339(), "2026-04-01T17:05:00+00:00");
    }

    #[test]
    fn next_usage_cleanup_run_aligns_to_same_day_when_before_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-17T18:59:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next =
            next_daily_run_after(now_utc, timezone, USAGE_CLEANUP_HOUR, USAGE_CLEANUP_MINUTE);

        assert_eq!(next.to_rfc3339(), "2026-03-17T19:00:00+00:00");
    }

    #[test]
    fn next_usage_cleanup_run_rolls_to_next_day_after_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-17T19:00:01Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next =
            next_daily_run_after(now_utc, timezone, USAGE_CLEANUP_HOUR, USAGE_CLEANUP_MINUTE);

        assert_eq!(next.to_rfc3339(), "2026-03-18T19:00:00+00:00");
    }

    #[test]
    fn next_wallet_daily_usage_aggregation_run_aligns_to_same_day_when_before_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-31T16:09:00Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_daily_run_after(
            now_utc,
            timezone,
            WALLET_DAILY_USAGE_AGGREGATION_HOUR,
            WALLET_DAILY_USAGE_AGGREGATION_MINUTE,
        );

        assert_eq!(next.to_rfc3339(), "2026-03-31T16:10:00+00:00");
    }

    #[test]
    fn next_wallet_daily_usage_aggregation_run_rolls_to_next_day_after_slot() {
        let timezone: Tz = "Asia/Shanghai".parse().expect("timezone should parse");
        let now_utc = "2026-03-31T16:10:01Z"
            .parse::<DateTime<Utc>>()
            .expect("timestamp should parse");

        let next = next_daily_run_after(
            now_utc,
            timezone,
            WALLET_DAILY_USAGE_AGGREGATION_HOUR,
            WALLET_DAILY_USAGE_AGGREGATION_MINUTE,
        );

        assert_eq!(next.to_rfc3339(), "2026-04-01T16:10:00+00:00");
    }
}
