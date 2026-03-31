use aether_data::DataLayerError;

use crate::gateway::data::DecisionTrace;
use crate::gateway::data::GatewayDataState;
use crate::gateway::data::StoredGatewayAuthApiKeySnapshot;

use super::{read_request_usage_audit, RequestUsageAudit};

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct RequestAuditBundle {
    pub(crate) request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) usage: Option<RequestUsageAudit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) decision_trace: Option<DecisionTrace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth_snapshot: Option<StoredGatewayAuthApiKeySnapshot>,
}

pub(crate) async fn read_request_audit_bundle(
    state: &GatewayDataState,
    request_id: &str,
    attempted_only: bool,
    now_unix_secs: u64,
) -> Result<Option<RequestAuditBundle>, DataLayerError> {
    let usage = read_request_usage_audit(state, request_id).await?;
    let decision_trace = state
        .read_decision_trace(request_id, attempted_only)
        .await?;

    let auth_snapshot = if let Some(usage) = usage.as_ref() {
        match (
            usage.usage.user_id.as_deref(),
            usage.usage.api_key_id.as_deref(),
        ) {
            (Some(user_id), Some(api_key_id)) => {
                state
                    .read_auth_api_key_snapshot(user_id, api_key_id, now_unix_secs)
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
