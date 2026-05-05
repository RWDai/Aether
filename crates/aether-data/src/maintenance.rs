//! Maintenance and aggregation DTOs used by the runtime data layer.
//!
//! These are not cross-crate repository contracts, but they are shared across
//! the backend composition layer and maintenance entrypoints.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DatabaseMaintenanceSummary {
    pub attempted: usize,
    pub succeeded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabasePoolSummary {
    pub driver: crate::database::DatabaseDriver,
    pub checked_out: usize,
    pub pool_size: usize,
    pub idle: usize,
    pub max_connections: u32,
    pub usage_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletDailyUsageAggregationInput {
    pub billing_date: String,
    pub billing_timezone: String,
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    pub aggregated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalletDailyUsageAggregationResult {
    pub aggregated_wallets: usize,
    pub deleted_stale_ledgers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsHourlyAggregationInput {
    pub target_hour_utc: DateTime<Utc>,
    pub aggregated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsDailyAggregationInput {
    pub target_day_utc: DateTime<Utc>,
    pub aggregated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsDailyAggregationSummary {
    pub day_start_utc: DateTime<Utc>,
    pub total_requests: i64,
    pub model_rows: usize,
    pub provider_rows: usize,
    pub api_key_rows: usize,
    pub error_rows: usize,
    pub user_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsHourlyAggregationSummary {
    pub hour_utc: DateTime<Utc>,
    pub total_requests: i64,
    pub user_rows: usize,
    pub user_model_rows: usize,
    pub model_rows: usize,
    pub provider_rows: usize,
}
