use aether_data::repository::usage::StoredRequestUsageAudit;
use aether_data::DataLayerError;

use crate::gateway::data::GatewayDataState;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct RequestUsageAudit {
    #[serde(flatten)]
    pub(crate) usage: StoredRequestUsageAudit,
}

pub(crate) async fn read_request_usage_audit(
    state: &GatewayDataState,
    request_id: &str,
) -> Result<Option<RequestUsageAudit>, DataLayerError> {
    Ok(state
        .find_request_usage_by_request_id(request_id)
        .await?
        .map(|usage| RequestUsageAudit { usage }))
}
