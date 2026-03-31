use aether_data::repository::usage::StoredRequestUsageAudit;
use aether_data::repository::wallet::UsageSettlementInput;
use aether_data::{DataLayerError, DataLayerError::InvalidInput};

use crate::gateway::data::GatewayDataState;

pub(crate) async fn settle_usage_if_needed(
    data: &GatewayDataState,
    usage: &StoredRequestUsageAudit,
) -> Result<(), DataLayerError> {
    if !data.has_wallet_writer() || usage.billing_status != "pending" {
        return Ok(());
    }
    if !matches!(usage.status.as_str(), "completed" | "failed" | "cancelled") {
        return Ok(());
    }

    let finalized_at_unix_secs = usage
        .finalized_at_unix_secs
        .or(Some(usage.updated_at_unix_secs));
    let input = UsageSettlementInput {
        request_id: usage.request_id.clone(),
        user_id: usage.user_id.clone(),
        api_key_id: usage.api_key_id.clone(),
        provider_id: usage.provider_id.clone(),
        status: usage.status.clone(),
        billing_status: usage.billing_status.clone(),
        total_cost_usd: finite_cost(usage.total_cost_usd)?,
        actual_total_cost_usd: finite_cost(usage.actual_total_cost_usd)?,
        finalized_at_unix_secs,
    };
    let _ = data.settle_usage(input).await?;
    Ok(())
}

fn finite_cost(value: f64) -> Result<f64, DataLayerError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(InvalidInput(
            "wallet settlement cost must be finite".to_string(),
        ))
    }
}
