use aether_data_contracts::repository::usage::UsageCleanupSummary;
use aether_data_contracts::DataLayerError;
use chrono::Utc;

use crate::data::GatewayDataState;

use super::{system_config_bool, usage_cleanup_settings, usage_cleanup_window};

pub(super) async fn perform_usage_cleanup_once(
    data: &GatewayDataState,
) -> Result<UsageCleanupSummary, DataLayerError> {
    if !data.has_usage_writer() {
        return Ok(UsageCleanupSummary::default());
    }
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(UsageCleanupSummary::default());
    }

    let settings = usage_cleanup_settings(data).await?;
    let window = usage_cleanup_window(Utc::now(), settings);
    data.cleanup_usage(
        &window,
        settings.batch_size,
        settings.auto_delete_expired_keys,
    )
    .await
}
