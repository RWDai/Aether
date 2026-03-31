use axum::body::{Body, Bytes};
use axum::http::Response;
use base64::Engine as _;
use serde::Serialize;

use crate::gateway::headers::{collect_control_headers, is_json_request};
use crate::gateway::{
    build_client_response, AppState, GatewayControlAuthContext, GatewayControlDecision,
    GatewayError,
};

use super::resolve_executor_auth_context;

#[derive(Debug, Serialize)]
struct GatewayControlExecuteRequest {
    trace_id: String,
    method: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_string: Option<String>,
    headers: std::collections::BTreeMap<String, String>,
    body_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_context: Option<GatewayControlAuthContext>,
}

pub(crate) fn allows_control_execute_emergency(decision: &GatewayControlDecision) -> bool {
    decision.executor_candidate
}

pub(crate) async fn maybe_execute_via_control(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
    require_stream: bool,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(control_base_url) = state.control_base_url.as_deref() else {
        return Ok(None);
    };
    let Some(decision) = decision else {
        return Ok(None);
    };

    let mut headers = collect_control_headers(&parts.headers);
    headers.insert(
        crate::gateway::constants::CONTROL_EXECUTE_FALLBACK_HEADER.to_string(),
        "true".to_string(),
    );
    headers.insert(
        crate::gateway::constants::LEGACY_INTERNAL_GATEWAY_HEADER.to_string(),
        "true".to_string(),
    );

    let (body_json, body_base64) = if is_json_request(&parts.headers) {
        if body_bytes.is_empty() {
            (serde_json::json!({}), None)
        } else {
            match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
                Ok(value) => (value, None),
                Err(_) => return Ok(None),
            }
        }
    } else {
        (
            serde_json::json!({}),
            (!body_bytes.is_empty())
                .then(|| base64::engine::general_purpose::STANDARD.encode(body_bytes.as_ref())),
        )
    };

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;
    let payload = GatewayControlExecuteRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers,
        body_json,
        body_base64,
        auth_context,
    };

    let endpoint = if require_stream {
        "/api/internal/gateway/execute-stream"
    } else {
        "/api/internal/gateway/execute-sync"
    };
    let response = state
        .client
        .post(format!("{control_base_url}{endpoint}"))
        .header(
            crate::gateway::constants::LEGACY_INTERNAL_GATEWAY_HEADER,
            "true",
        )
        .header(
            crate::gateway::constants::CONTROL_EXECUTE_FALLBACK_HEADER,
            "true",
        )
        .json(&payload)
        .send()
        .await
        .map_err(|err| GatewayError::UpstreamUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let executed = response
        .headers()
        .get(crate::gateway::constants::CONTROL_EXECUTED_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !executed {
        return Ok(None);
    }

    Ok(Some(build_client_response(
        response,
        trace_id,
        Some(decision),
    )?))
}
