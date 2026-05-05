use aether_data::{DataLayerError, StatsDailyAggregationInput, StatsDailyAggregationSummary};
use chrono::Utc;

use crate::data::GatewayDataState;

use super::{stats_aggregation_target_day, system_config_bool};

pub(super) async fn perform_stats_aggregation_once(
    data: &GatewayDataState,
) -> Result<Option<StatsDailyAggregationSummary>, DataLayerError> {
    if !data.has_stats_daily_aggregation_backend() {
        return Ok(None);
    }
    if !system_config_bool(data, "enable_stats_aggregation", true).await? {
        return Ok(None);
    }

    let now_utc = Utc::now();
    data.aggregate_stats_daily(&StatsDailyAggregationInput {
        target_day_utc: stats_aggregation_target_day(now_utc),
        aggregated_at: now_utc,
    })
    .await
}
