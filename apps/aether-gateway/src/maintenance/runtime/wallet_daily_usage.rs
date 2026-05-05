use chrono::{DateTime, Utc};

use crate::data::GatewayDataState;
use aether_data_contracts::DataLayerError;

use super::{maintenance_timezone, wallet_daily_usage_aggregation_target};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalletDailyUsageAggregationSummary {
    pub(crate) billing_date: chrono::NaiveDate,
    pub(crate) billing_timezone: String,
    pub(crate) aggregated_wallets: usize,
    pub(crate) deleted_stale_ledgers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WalletDailyUsageAggregationTarget {
    pub(super) billing_date: chrono::NaiveDate,
    pub(super) billing_timezone: String,
    pub(super) window_start_utc: DateTime<Utc>,
    pub(super) window_end_utc: DateTime<Utc>,
}

pub(super) async fn perform_wallet_daily_usage_aggregation_once(
    data: &GatewayDataState,
) -> Result<WalletDailyUsageAggregationSummary, DataLayerError> {
    let timezone = maintenance_timezone();
    let now_utc = Utc::now();
    let target = wallet_daily_usage_aggregation_target(now_utc, timezone);
    if !data.has_wallet_daily_usage_aggregation_backend() {
        return Ok(WalletDailyUsageAggregationSummary {
            billing_date: target.billing_date,
            billing_timezone: target.billing_timezone,
            aggregated_wallets: 0,
            deleted_stale_ledgers: 0,
        });
    }

    let result = data
        .aggregate_wallet_daily_usage(&aether_data::WalletDailyUsageAggregationInput {
            billing_date: target.billing_date.to_string(),
            billing_timezone: target.billing_timezone.clone(),
            window_start_unix_secs: target.window_start_utc.timestamp().max(0) as u64,
            window_end_unix_secs: target.window_end_utc.timestamp().max(0) as u64,
            aggregated_at_unix_secs: now_utc.timestamp().max(0) as u64,
        })
        .await?;

    Ok(WalletDailyUsageAggregationSummary {
        billing_date: target.billing_date,
        billing_timezone: target.billing_timezone,
        aggregated_wallets: result.aggregated_wallets,
        deleted_stale_ledgers: result.deleted_stale_ledgers,
    })
}
