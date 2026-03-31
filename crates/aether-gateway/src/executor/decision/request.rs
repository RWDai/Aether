use axum::body::Bytes;
use base64::Engine as _;
use serde_json::json;

use super::super::*;

pub(super) fn parse_direct_request_body(
    parts: &http::request::Parts,
    body_bytes: &Bytes,
) -> Option<(serde_json::Value, Option<String>)> {
    if is_json_request(&parts.headers) {
        if body_bytes.is_empty() {
            Some((json!({}), None))
        } else {
            serde_json::from_slice::<serde_json::Value>(body_bytes)
                .ok()
                .map(|value| (value, None))
        }
    } else {
        Some((
            json!({}),
            (!body_bytes.is_empty())
                .then(|| base64::engine::general_purpose::STANDARD.encode(body_bytes)),
        ))
    }
}

pub(super) async fn build_gateway_plan_request(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: serde_json::Value,
    body_base64: Option<String>,
) -> Result<GatewayControlPlanRequest, GatewayError> {
    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;

    Ok(GatewayControlPlanRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json,
        body_base64,
        auth_context,
    })
}
