use super::super::plan_builders::{
    build_gemini_stream_plan_from_decision, build_gemini_sync_plan_from_decision,
    build_openai_chat_stream_plan_from_decision, build_openai_chat_sync_plan_from_decision,
    build_openai_cli_stream_plan_from_decision, build_openai_cli_sync_plan_from_decision,
    build_passthrough_stream_plan_from_decision, build_passthrough_sync_plan_from_decision,
    build_standard_stream_plan_from_decision, build_standard_sync_plan_from_decision,
    LocalStreamPlanAndReport, LocalSyncPlanAndReport,
};
use super::super::*;
use super::request::build_gateway_plan_request;
use super::stream_path::maybe_build_stream_decision_payload_via_local_path;
use super::sync_path::maybe_build_sync_decision_payload_via_local_path;
use crate::gateway::scheduler::{
    resolve_direct_executor_stream_plan_kind, resolve_direct_executor_sync_plan_kind,
};
use crate::gateway::GatewayFallbackReason;
use tracing::warn;

pub(crate) async fn maybe_build_sync_plan_payload_via_local_path(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<&str>,
    body_is_empty: bool,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    let Some(plan_kind) = resolve_direct_executor_sync_plan_kind(parts, decision) else {
        return Ok(None);
    };
    let Some(payload) = maybe_build_sync_decision_payload_via_local_path(
        state,
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
        body_is_empty,
    )
    .await?
    else {
        return Ok(None);
    };

    build_sync_plan_payload_from_decision(parts, body_json, plan_kind, payload)
}

pub(crate) async fn maybe_build_stream_plan_payload_via_local_path(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    let Some(plan_kind) = resolve_direct_executor_stream_plan_kind(parts, decision) else {
        return Ok(None);
    };
    let Some(payload) = maybe_build_stream_decision_payload_via_local_path(
        state, parts, trace_id, decision, body_json,
    )
    .await?
    else {
        return Ok(None);
    };

    build_stream_plan_payload_from_decision(parts, body_json, plan_kind, payload)
}

pub(in crate::gateway::executor) async fn maybe_execute_sync_via_plan_fallback(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<String>,
    _plan_kind: &str,
    _bypass_cache_key: String,
    _fallback_reason: GatewayFallbackReason,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(payload) = request_remote_plan_payload(
        state,
        control_base_url,
        "/api/internal/gateway/plan-sync",
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
    )
    .await?
    else {
        return Ok(None);
    };

    let GatewayControlPlanResponse {
        action: _,
        plan_kind,
        plan,
        report_kind,
        report_context,
        auth_context: _,
    } = payload;

    let (Some(plan_kind), Some(plan)) = (plan_kind, plan) else {
        return Ok(None);
    };

    execute_executor_sync(
        state,
        control_base_url,
        executor_base_url,
        parts.uri.path(),
        plan,
        trace_id,
        decision,
        plan_kind.as_str(),
        report_kind,
        report_context,
    )
    .await
}

pub(in crate::gateway::executor) async fn maybe_execute_stream_via_plan_fallback(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<String>,
    _plan_kind: &str,
    _bypass_cache_key: String,
    _fallback_reason: GatewayFallbackReason,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(payload) = request_remote_plan_payload(
        state,
        control_base_url,
        "/api/internal/gateway/plan-stream",
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
    )
    .await?
    else {
        return Ok(None);
    };

    let GatewayControlPlanResponse {
        action: _,
        plan_kind,
        plan,
        report_kind,
        report_context,
        auth_context: _,
    } = payload;

    let (Some(plan_kind), Some(plan)) = (plan_kind, plan) else {
        return Ok(None);
    };

    execute_executor_stream(
        state,
        control_base_url,
        executor_base_url,
        plan,
        trace_id,
        decision,
        plan_kind.as_str(),
        report_kind,
        report_context,
    )
    .await
}

async fn request_remote_plan_payload(
    state: &AppState,
    control_base_url: &str,
    endpoint: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<String>,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    if control_base_url.trim().is_empty()
        || request_enables_control_execute_fallback(&parts.headers)
    {
        return Ok(None);
    }

    let request = build_gateway_plan_request(
        state,
        parts,
        trace_id,
        decision,
        body_json.clone(),
        body_base64,
    )
    .await?;

    let response = match state
        .client
        .post(format!("{control_base_url}{endpoint}"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                endpoint = %endpoint,
                error = %err,
                "gateway remote plan fallback request failed"
            );
            return Ok(None);
        }
    };

    if response.status() != http::StatusCode::OK {
        return Ok(None);
    }

    let payload = match response.json::<GatewayControlPlanResponse>().await {
        Ok(payload) => payload,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                endpoint = %endpoint,
                error = %err,
                "gateway remote plan fallback returned non-json payload"
            );
            return Ok(None);
        }
    };

    let expected_action = if endpoint.ends_with("plan-sync") {
        EXECUTOR_SYNC_ACTION
    } else {
        EXECUTOR_STREAM_ACTION
    };
    if payload.action != expected_action {
        return Ok(None);
    }

    Ok(Some(payload))
}

fn request_enables_control_execute_fallback(headers: &http::HeaderMap) -> bool {
    [
        CONTROL_EXECUTE_FALLBACK_HEADER,
        LEGACY_INTERNAL_GATEWAY_HEADER,
    ]
    .into_iter()
    .any(|header| {
        crate::gateway::headers::header_value_str(headers, header).is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
    })
}

fn build_sync_plan_payload_from_decision(
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
    plan_kind: &str,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    let auth_context = payload.auth_context.clone();
    let plan_and_report = match plan_kind {
        OPENAI_CHAT_SYNC_PLAN_KIND => {
            build_openai_chat_sync_plan_from_decision(parts, body_json, payload)?
        }
        OPENAI_CLI_SYNC_PLAN_KIND => {
            build_openai_cli_sync_plan_from_decision(parts, body_json, payload, false)?
        }
        OPENAI_COMPACT_SYNC_PLAN_KIND => {
            build_openai_cli_sync_plan_from_decision(parts, body_json, payload, true)?
        }
        CLAUDE_CHAT_SYNC_PLAN_KIND | CLAUDE_CLI_SYNC_PLAN_KIND => {
            build_standard_sync_plan_from_decision(parts, body_json, payload)?
        }
        GEMINI_CHAT_SYNC_PLAN_KIND | GEMINI_CLI_SYNC_PLAN_KIND => {
            build_gemini_sync_plan_from_decision(parts, body_json, payload)?
        }
        OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
        | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
        | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
        | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
        | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
        | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
        | GEMINI_FILES_LIST_PLAN_KIND
        | GEMINI_FILES_GET_PLAN_KIND
        | GEMINI_FILES_DELETE_PLAN_KIND => {
            build_passthrough_sync_plan_from_decision(parts, payload)?
        }
        _ => None,
    };

    Ok(plan_and_report.map(|value| build_sync_plan_response(plan_kind, value, auth_context)))
}

fn build_stream_plan_payload_from_decision(
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
    plan_kind: &str,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    let auth_context = payload.auth_context.clone();
    let plan_and_report = match plan_kind {
        OPENAI_CHAT_STREAM_PLAN_KIND => {
            build_openai_chat_stream_plan_from_decision(parts, body_json, payload)?
        }
        OPENAI_CLI_STREAM_PLAN_KIND => {
            build_openai_cli_stream_plan_from_decision(parts, body_json, payload, false)?
        }
        OPENAI_COMPACT_STREAM_PLAN_KIND => {
            build_openai_cli_stream_plan_from_decision(parts, body_json, payload, true)?
        }
        CLAUDE_CHAT_STREAM_PLAN_KIND | CLAUDE_CLI_STREAM_PLAN_KIND => {
            build_standard_stream_plan_from_decision(parts, body_json, payload, true)?
        }
        GEMINI_CHAT_STREAM_PLAN_KIND | GEMINI_CLI_STREAM_PLAN_KIND => {
            build_gemini_stream_plan_from_decision(parts, body_json, payload)?
        }
        OPENAI_VIDEO_CONTENT_PLAN_KIND | GEMINI_FILES_DOWNLOAD_PLAN_KIND => {
            build_passthrough_stream_plan_from_decision(parts, payload)?
        }
        _ => None,
    };

    Ok(plan_and_report.map(|value| build_stream_plan_response(plan_kind, value, auth_context)))
}

fn build_sync_plan_response(
    plan_kind: &str,
    value: LocalSyncPlanAndReport,
    auth_context: Option<GatewayControlAuthContext>,
) -> GatewayControlPlanResponse {
    GatewayControlPlanResponse {
        action: EXECUTOR_SYNC_ACTION.to_string(),
        plan_kind: Some(plan_kind.to_string()),
        plan: Some(value.plan),
        report_kind: value.report_kind,
        report_context: value.report_context,
        auth_context,
    }
}

fn build_stream_plan_response(
    plan_kind: &str,
    value: LocalStreamPlanAndReport,
    auth_context: Option<GatewayControlAuthContext>,
) -> GatewayControlPlanResponse {
    GatewayControlPlanResponse {
        action: EXECUTOR_STREAM_ACTION.to_string(),
        plan_kind: Some(plan_kind.to_string()),
        plan: Some(value.plan),
        report_kind: value.report_kind,
        report_context: value.report_context,
        auth_context,
    }
}
