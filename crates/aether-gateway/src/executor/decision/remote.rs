use super::super::*;

pub(in crate::gateway::executor) async fn maybe_execute_sync_via_remote_decision(
    _state: &AppState,
    _control_base_url: &str,
    _executor_base_url: &str,
    _parts: &http::request::Parts,
    _trace_id: &str,
    _decision: &GatewayControlDecision,
    _body_json: &serde_json::Value,
    _plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_execute_stream_via_remote_decision(
    _state: &AppState,
    _control_base_url: &str,
    _executor_base_url: &str,
    _parts: &http::request::Parts,
    _trace_id: &str,
    _decision: &GatewayControlDecision,
    _body_json: &serde_json::Value,
    _plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    Ok(None)
}
