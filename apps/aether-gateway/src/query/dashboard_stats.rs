use aether_data::postgres::PostgresPool;
use aether_data_contracts::repository::usage::StoredUsageDashboardSummary;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use sqlx::Row;

use crate::GatewayError;

fn internal(err: impl ToString) -> GatewayError {
    GatewayError::Internal(err.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DashboardDailyTotalsAggregateRow {
    pub(crate) date: String,
    pub(crate) requests: u64,
    pub(crate) total_tokens: u64,
    pub(crate) total_cost_usd: f64,
    pub(crate) response_time_sum_ms: f64,
    pub(crate) response_time_samples: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DashboardDailyModelAggregateRow {
    pub(crate) date: String,
    pub(crate) model: String,
    pub(crate) requests: u64,
    pub(crate) total_tokens: u64,
    pub(crate) total_cost_usd: f64,
    pub(crate) response_time_sum_ms: f64,
    pub(crate) response_time_samples: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DashboardDailyProviderAggregateRow {
    pub(crate) date: String,
    pub(crate) provider: String,
    pub(crate) requests: u64,
    pub(crate) total_tokens: u64,
    pub(crate) total_cost_usd: f64,
}

pub(crate) async fn summarize_dashboard_usage_from_daily_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
    user_id: Option<&str>,
) -> Result<StoredUsageDashboardSummary, GatewayError> {
    let row = if let Some(user_id) = user_id {
        sqlx::query(
            r#"
SELECT
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
  COALESCE(SUM(effective_input_tokens), 0)::BIGINT AS effective_input_tokens,
  COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
  COALESCE(SUM(effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), 0)::BIGINT AS total_tokens,
  COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens,
  COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens,
  COALESCE(SUM(total_input_context), 0)::BIGINT AS total_input_context,
  CAST(COALESCE(SUM(cache_creation_cost), 0) AS DOUBLE PRECISION) AS cache_creation_cost_usd,
  CAST(COALESCE(SUM(cache_read_cost), 0) AS DOUBLE PRECISION) AS cache_read_cost_usd,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost_usd,
  CAST(COALESCE(SUM(actual_total_cost), 0) AS DOUBLE PRECISION) AS actual_total_cost_usd,
  COALESCE(SUM(error_requests), 0)::BIGINT AS error_requests,
  COALESCE(SUM(response_time_sum_ms), 0) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_user_daily
WHERE user_id = $1
  AND date >= $2
  AND date < $3
"#,
        )
        .bind(user_id)
        .bind(start_day_utc)
        .bind(end_day_utc)
        .fetch_one(pool)
        .await
        .map_err(|err| internal(format!("user daily aggregate summary lookup failed: {err}")))?
    } else {
        sqlx::query(
            r#"
SELECT
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
  COALESCE(SUM(effective_input_tokens), 0)::BIGINT AS effective_input_tokens,
  COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
  COALESCE(SUM(effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), 0)::BIGINT AS total_tokens,
  COALESCE(SUM(cache_creation_tokens), 0)::BIGINT AS cache_creation_tokens,
  COALESCE(SUM(cache_read_tokens), 0)::BIGINT AS cache_read_tokens,
  COALESCE(SUM(total_input_context), 0)::BIGINT AS total_input_context,
  CAST(COALESCE(SUM(cache_creation_cost), 0) AS DOUBLE PRECISION) AS cache_creation_cost_usd,
  CAST(COALESCE(SUM(cache_read_cost), 0) AS DOUBLE PRECISION) AS cache_read_cost_usd,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost_usd,
  CAST(COALESCE(SUM(actual_total_cost), 0) AS DOUBLE PRECISION) AS actual_total_cost_usd,
  COALESCE(SUM(error_requests), 0)::BIGINT AS error_requests,
  COALESCE(SUM(response_time_sum_ms), 0) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_daily
WHERE date >= $1
  AND date < $2
"#,
        )
        .bind(start_day_utc)
        .bind(end_day_utc)
        .fetch_one(pool)
        .await
        .map_err(|err| internal(format!("daily aggregate summary lookup failed: {err}")))?
    };

    Ok(StoredUsageDashboardSummary {
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        input_tokens: row
            .try_get::<i64, _>("input_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        effective_input_tokens: row
            .try_get::<i64, _>("effective_input_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        output_tokens: row
            .try_get::<i64, _>("output_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        total_tokens: row
            .try_get::<i64, _>("total_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        cache_creation_tokens: row
            .try_get::<i64, _>("cache_creation_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        cache_read_tokens: row
            .try_get::<i64, _>("cache_read_tokens")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        total_input_context: row
            .try_get::<i64, _>("total_input_context")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        cache_creation_cost_usd: row
            .try_get::<f64, _>("cache_creation_cost_usd")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?,
        cache_read_cost_usd: row
            .try_get::<f64, _>("cache_read_cost_usd")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?,
        total_cost_usd: row
            .try_get::<f64, _>("total_cost_usd")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?,
        actual_total_cost_usd: row
            .try_get::<f64, _>("actual_total_cost_usd")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?,
        error_requests: row
            .try_get::<i64, _>("error_requests")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
        response_time_sum_ms: row
            .try_get::<f64, _>("response_time_sum_ms")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?,
        response_time_samples: row
            .try_get::<i64, _>("response_time_samples")
            .map_err(|err| internal(format!("aggregate summary decode failed: {err}")))?
            .max(0) as u64,
    })
}

pub(crate) async fn read_stats_hourly_cutoff(
    pool: &PostgresPool,
) -> Result<Option<DateTime<Utc>>, GatewayError> {
    let row = sqlx::query(
        r#"
SELECT MAX(hour_utc) AS latest_hour
FROM stats_hourly
WHERE is_complete IS TRUE
"#,
    )
    .fetch_one(pool)
    .await
    .map_err(|err| internal(format!("hourly aggregate cutoff lookup failed: {err}")))?;
    let latest_hour = row
        .try_get::<Option<DateTime<Utc>>, _>("latest_hour")
        .map_err(|err| internal(format!("hourly aggregate cutoff decode failed: {err}")))?;
    Ok(latest_hour.map(|value| value + chrono::Duration::hours(1)))
}

pub(crate) async fn list_admin_dashboard_daily_totals_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
) -> Result<Vec<DashboardDailyTotalsAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  date,
  total_requests,
  effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens AS total_tokens,
  COALESCE(total_cost, 0)::DOUBLE PRECISION AS total_cost,
  response_time_sum_ms,
  response_time_samples
FROM stats_daily
WHERE date >= $1
  AND date < $2
ORDER BY date ASC
"#,
    )
    .bind(start_day_utc)
    .bind(end_day_utc)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("daily aggregate totals read failed: {err}")))?
    {
        let date = row
            .try_get::<DateTime<Utc>, _>("date")
            .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?;
        items.push(DashboardDailyTotalsAggregateRow {
            date: date.date_naive().to_string(),
            requests: row
                .try_get::<i32, _>("total_requests")
                .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row
                .try_get::<f64, _>("total_cost")
                .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| internal(format!("daily aggregate totals decode failed: {err}")))?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_admin_dashboard_hourly_totals_aggregates(
    pool: &PostgresPool,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    tz_offset_minutes: i32,
) -> Result<Vec<DashboardDailyTotalsAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  CAST(DATE(hour_utc + ($3::integer * INTERVAL '1 minute')) AS TEXT) AS date,
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), 0)::BIGINT AS total_tokens,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost,
  CAST(COALESCE(SUM(response_time_sum_ms), 0) AS DOUBLE PRECISION) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_hourly
WHERE hour_utc >= $1
  AND hour_utc < $2
GROUP BY date
ORDER BY date ASC
"#,
    )
    .bind(start_utc)
    .bind(end_utc)
    .bind(tz_offset_minutes)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("hourly aggregate totals read failed: {err}")))?
    {
        items.push(DashboardDailyTotalsAggregateRow {
            date: row
                .try_get::<String, _>("date")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?,
            requests: row
                .try_get::<i64, _>("total_requests")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row
                .try_get::<f64, _>("total_cost")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| internal(format!("hourly aggregate totals decode failed: {err}")))?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_admin_dashboard_daily_model_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
) -> Result<Vec<DashboardDailyModelAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  date,
  model,
  total_requests,
  input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens AS total_tokens,
  COALESCE(total_cost, 0)::DOUBLE PRECISION AS total_cost,
  response_time_sum_ms,
  response_time_samples
FROM stats_daily_model
WHERE date >= $1
  AND date < $2
ORDER BY date ASC, total_cost DESC, model ASC
"#,
    )
    .bind(start_day_utc)
    .bind(end_day_utc)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("daily aggregate model read failed: {err}")))?
    {
        let date = row
            .try_get::<DateTime<Utc>, _>("date")
            .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?;
        items.push(DashboardDailyModelAggregateRow {
            date: date.date_naive().to_string(),
            model: row
                .try_get::<String, _>("model")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?,
            requests: row
                .try_get::<i32, _>("total_requests")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row
                .try_get::<f64, _>("total_cost")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| internal(format!("daily aggregate model decode failed: {err}")))?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_admin_dashboard_hourly_model_aggregates(
    pool: &PostgresPool,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    tz_offset_minutes: i32,
) -> Result<Vec<DashboardDailyModelAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  CAST(DATE(hour_utc + ($3::integer * INTERVAL '1 minute')) AS TEXT) AS date,
  model,
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens + output_tokens), 0)::BIGINT AS total_tokens,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost,
  CAST(COALESCE(SUM(response_time_sum_ms), 0) AS DOUBLE PRECISION) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_hourly_model
WHERE hour_utc >= $1
  AND hour_utc < $2
GROUP BY date, model
ORDER BY date ASC, total_cost DESC, model ASC
"#,
    )
    .bind(start_utc)
    .bind(end_utc)
    .bind(tz_offset_minutes)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("hourly aggregate model read failed: {err}")))?
    {
        items.push(DashboardDailyModelAggregateRow {
            date: row
                .try_get::<String, _>("date")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?,
            model: row
                .try_get::<String, _>("model")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?,
            requests: row
                .try_get::<i64, _>("total_requests")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row
                .try_get::<f64, _>("total_cost")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| internal(format!("hourly aggregate model decode failed: {err}")))?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_admin_dashboard_daily_provider_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
) -> Result<Vec<DashboardDailyProviderAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  date,
  provider_name,
  total_requests,
  input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens AS total_tokens,
  COALESCE(total_cost, 0)::DOUBLE PRECISION AS total_cost
FROM stats_daily_provider
WHERE date >= $1
  AND date < $2
ORDER BY date ASC, total_cost DESC, provider_name ASC
"#,
    )
    .bind(start_day_utc)
    .bind(end_day_utc)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("daily aggregate provider read failed: {err}")))?
    {
        let date = row
            .try_get::<DateTime<Utc>, _>("date")
            .map_err(|err| internal(format!("daily aggregate provider decode failed: {err}")))?;
        items.push(DashboardDailyProviderAggregateRow {
            date: date.date_naive().to_string(),
            provider: row.try_get::<String, _>("provider_name").map_err(|err| {
                internal(format!("daily aggregate provider decode failed: {err}"))
            })?,
            requests: row
                .try_get::<i32, _>("total_requests")
                .map_err(|err| internal(format!("daily aggregate provider decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("daily aggregate provider decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("daily aggregate provider decode failed: {err}"))
            })?,
        });
    }

    Ok(items)
}

pub(crate) async fn list_admin_dashboard_hourly_provider_aggregates(
    pool: &PostgresPool,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    tz_offset_minutes: i32,
) -> Result<Vec<DashboardDailyProviderAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  CAST(DATE(hour_utc + ($3::integer * INTERVAL '1 minute')) AS TEXT) AS date,
  provider_name,
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens + output_tokens), 0)::BIGINT AS total_tokens,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost
FROM stats_hourly_provider
WHERE hour_utc >= $1
  AND hour_utc < $2
GROUP BY date, provider_name
ORDER BY date ASC, total_cost DESC, provider_name ASC
"#,
    )
    .bind(start_utc)
    .bind(end_utc)
    .bind(tz_offset_minutes)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("hourly aggregate provider read failed: {err}")))?
    {
        items.push(DashboardDailyProviderAggregateRow {
            date: row.try_get::<String, _>("date").map_err(|err| {
                internal(format!("hourly aggregate provider decode failed: {err}"))
            })?,
            provider: row.try_get::<String, _>("provider_name").map_err(|err| {
                internal(format!("hourly aggregate provider decode failed: {err}"))
            })?,
            requests: row
                .try_get::<i64, _>("total_requests")
                .map_err(|err| internal(format!("hourly aggregate provider decode failed: {err}")))?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| internal(format!("hourly aggregate provider decode failed: {err}")))?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("hourly aggregate provider decode failed: {err}"))
            })?,
        });
    }

    Ok(items)
}

pub(crate) async fn list_user_dashboard_daily_totals_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
    user_id: &str,
) -> Result<Vec<DashboardDailyTotalsAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  date,
  total_requests,
  effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens AS total_tokens,
  COALESCE(total_cost, 0)::DOUBLE PRECISION AS total_cost,
  response_time_sum_ms,
  response_time_samples
FROM stats_user_daily
WHERE user_id = $1
  AND date >= $2
  AND date < $3
ORDER BY date ASC
"#,
    )
    .bind(user_id)
    .bind(start_day_utc)
    .bind(end_day_utc)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("user daily aggregate totals read failed: {err}")))?
    {
        let date = row
            .try_get::<DateTime<Utc>, _>("date")
            .map_err(|err| internal(format!("user daily aggregate totals decode failed: {err}")))?;
        items.push(DashboardDailyTotalsAggregateRow {
            date: date.date_naive().to_string(),
            requests: row
                .try_get::<i32, _>("total_requests")
                .map_err(|err| {
                    internal(format!("user daily aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| {
                    internal(format!("user daily aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("user daily aggregate totals decode failed: {err}"))
            })?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| {
                    internal(format!("user daily aggregate totals decode failed: {err}"))
                })?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| {
                    internal(format!("user daily aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_user_dashboard_hourly_totals_aggregates(
    pool: &PostgresPool,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    tz_offset_minutes: i32,
    user_id: &str,
) -> Result<Vec<DashboardDailyTotalsAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  CAST(DATE(hour_utc + ($4::integer * INTERVAL '1 minute')) AS TEXT) AS date,
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), 0)::BIGINT AS total_tokens,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost,
  CAST(COALESCE(SUM(response_time_sum_ms), 0) AS DOUBLE PRECISION) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_hourly_user
WHERE user_id = $1
  AND hour_utc >= $2
  AND hour_utc < $3
GROUP BY date
ORDER BY date ASC
"#,
    )
    .bind(user_id)
    .bind(start_utc)
    .bind(end_utc)
    .bind(tz_offset_minutes)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("user hourly aggregate totals read failed: {err}")))?
    {
        items.push(DashboardDailyTotalsAggregateRow {
            date: row.try_get::<String, _>("date").map_err(|err| {
                internal(format!("user hourly aggregate totals decode failed: {err}"))
            })?,
            requests: row
                .try_get::<i64, _>("total_requests")
                .map_err(|err| {
                    internal(format!("user hourly aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| {
                    internal(format!("user hourly aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("user hourly aggregate totals decode failed: {err}"))
            })?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| {
                    internal(format!("user hourly aggregate totals decode failed: {err}"))
                })?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| {
                    internal(format!("user hourly aggregate totals decode failed: {err}"))
                })?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_user_dashboard_daily_model_aggregates(
    pool: &PostgresPool,
    start_day_utc: DateTime<Utc>,
    end_day_utc: DateTime<Utc>,
    user_id: &str,
) -> Result<Vec<DashboardDailyModelAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  date,
  model,
  total_requests,
  effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens AS total_tokens,
  COALESCE(total_cost, 0)::DOUBLE PRECISION AS total_cost,
  response_time_sum_ms,
  response_time_samples
FROM stats_user_daily_model
WHERE user_id = $1
  AND date >= $2
  AND date < $3
ORDER BY date ASC, total_cost DESC, model ASC
"#,
    )
    .bind(user_id)
    .bind(start_day_utc)
    .bind(end_day_utc)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("user daily aggregate model read failed: {err}")))?
    {
        let date = row
            .try_get::<DateTime<Utc>, _>("date")
            .map_err(|err| internal(format!("user daily aggregate model decode failed: {err}")))?;
        items.push(DashboardDailyModelAggregateRow {
            date: date.date_naive().to_string(),
            model: row.try_get::<String, _>("model").map_err(|err| {
                internal(format!("user daily aggregate model decode failed: {err}"))
            })?,
            requests: row
                .try_get::<i32, _>("total_requests")
                .map_err(|err| {
                    internal(format!("user daily aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| {
                    internal(format!("user daily aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("user daily aggregate model decode failed: {err}"))
            })?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| {
                    internal(format!("user daily aggregate model decode failed: {err}"))
                })?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| {
                    internal(format!("user daily aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
        });
    }

    Ok(items)
}

pub(crate) async fn list_user_dashboard_hourly_model_aggregates(
    pool: &PostgresPool,
    start_utc: DateTime<Utc>,
    end_utc: DateTime<Utc>,
    tz_offset_minutes: i32,
    user_id: &str,
) -> Result<Vec<DashboardDailyModelAggregateRow>, GatewayError> {
    let mut rows = sqlx::query(
        r#"
SELECT
  CAST(DATE(hour_utc + ($4::integer * INTERVAL '1 minute')) AS TEXT) AS date,
  model,
  COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
  COALESCE(SUM(input_tokens + output_tokens), 0)::BIGINT AS total_tokens,
  CAST(COALESCE(SUM(total_cost), 0) AS DOUBLE PRECISION) AS total_cost,
  CAST(COALESCE(SUM(response_time_sum_ms), 0) AS DOUBLE PRECISION) AS response_time_sum_ms,
  COALESCE(SUM(response_time_samples), 0)::BIGINT AS response_time_samples
FROM stats_hourly_user_model
WHERE user_id = $1
  AND hour_utc >= $2
  AND hour_utc < $3
GROUP BY date, model
ORDER BY date ASC, total_cost DESC, model ASC
"#,
    )
    .bind(user_id)
    .bind(start_utc)
    .bind(end_utc)
    .bind(tz_offset_minutes)
    .fetch(pool);

    let mut items = Vec::new();
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|err| internal(format!("user hourly aggregate model read failed: {err}")))?
    {
        items.push(DashboardDailyModelAggregateRow {
            date: row.try_get::<String, _>("date").map_err(|err| {
                internal(format!("user hourly aggregate model decode failed: {err}"))
            })?,
            model: row.try_get::<String, _>("model").map_err(|err| {
                internal(format!("user hourly aggregate model decode failed: {err}"))
            })?,
            requests: row
                .try_get::<i64, _>("total_requests")
                .map_err(|err| {
                    internal(format!("user hourly aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
            total_tokens: row
                .try_get::<i64, _>("total_tokens")
                .map_err(|err| {
                    internal(format!("user hourly aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
            total_cost_usd: row.try_get::<f64, _>("total_cost").map_err(|err| {
                internal(format!("user hourly aggregate model decode failed: {err}"))
            })?,
            response_time_sum_ms: row
                .try_get::<f64, _>("response_time_sum_ms")
                .map_err(|err| {
                    internal(format!("user hourly aggregate model decode failed: {err}"))
                })?,
            response_time_samples: row
                .try_get::<i64, _>("response_time_samples")
                .map_err(|err| {
                    internal(format!("user hourly aggregate model decode failed: {err}"))
                })?
                .max(0) as u64,
        });
    }

    Ok(items)
}
