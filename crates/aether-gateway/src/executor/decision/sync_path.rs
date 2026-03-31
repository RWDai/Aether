use axum::body::Body;
use axum::http::Response;
use std::collections::BTreeMap;

use super::super::sync::execute_executor_sync;
use super::super::*;
use super::plan::maybe_execute_sync_via_plan_fallback;
use super::policy::{build_direct_plan_bypass_cache_key, should_skip_direct_plan};
use super::request::parse_direct_request_body;
use super::{
    maybe_execute_sync_via_local_decision, maybe_execute_sync_via_local_gemini_files_decision,
    maybe_execute_sync_via_local_openai_cli_decision,
    maybe_execute_sync_via_local_same_format_provider_decision,
    maybe_execute_sync_via_local_video_decision, maybe_execute_sync_via_remote_decision,
};
use crate::gateway::scheduler::{
    is_matching_stream_request, resolve_direct_executor_stream_plan_kind,
    resolve_direct_executor_sync_plan_kind, supports_sync_scheduler_decision_kind,
};
use crate::gateway::GatewayFallbackReason;
use url::Url;

pub(crate) async fn maybe_build_sync_decision_payload_via_local_path(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<&str>,
    body_is_empty: bool,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    let Some(plan_kind) = resolve_direct_executor_sync_plan_kind(parts, decision) else {
        return Ok(None);
    };

    if let Some(payload) = maybe_build_local_video_task_follow_up_sync_decision_payload(
        state, parts, body_json, trace_id, decision, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    if let Some(payload) = super::video::maybe_build_sync_local_video_decision_payload(
        state, parts, body_json, trace_id, decision, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    if let Some(payload) = super::openai_chat::maybe_build_sync_local_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    if let Some(payload) = super::openai_cli::maybe_build_sync_local_openai_cli_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    if let Some(payload) =
        super::same_format_provider::maybe_build_sync_local_same_format_provider_decision_payload(
            state, parts, trace_id, decision, body_json, plan_kind,
        )
        .await?
    {
        return Ok(Some(payload));
    }

    if matches!(
        plan_kind,
        GEMINI_FILES_LIST_PLAN_KIND | GEMINI_FILES_GET_PLAN_KIND | GEMINI_FILES_DELETE_PLAN_KIND
    ) {
        if let Some(payload) = super::files::maybe_build_sync_local_gemini_files_decision_payload(
            state,
            parts,
            body_json,
            body_base64,
            body_is_empty,
            trace_id,
            decision,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(payload));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_execute_via_sync_decision_path(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Option<Response<Body>>, GatewayError> {
    if let Some(response) = maybe_build_local_video_task_read_response(
        state,
        executor_base_url,
        parts,
        trace_id,
        decision,
    )
    .await?
    {
        return Ok(Some(response));
    }

    let Some(plan_kind) = resolve_direct_executor_sync_plan_kind(parts, decision) else {
        return Ok(None);
    };

    let Some((body_json, body_base64)) = parse_direct_request_body(parts, body_bytes) else {
        return Ok(None);
    };

    if let Some(stream_plan_kind) = resolve_direct_executor_stream_plan_kind(parts, decision) {
        if is_matching_stream_request(stream_plan_kind, parts, &body_json) {
            return Ok(None);
        }
    }

    let bypass_cache_key =
        build_direct_plan_bypass_cache_key(plan_kind, parts, body_bytes, decision);
    if should_skip_direct_plan(state, &bypass_cache_key) {
        return Ok(None);
    }

    if let Some(response) = maybe_execute_local_video_task_follow_up_sync(
        state,
        control_base_url,
        executor_base_url,
        parts,
        &body_json,
        trace_id,
        decision,
        plan_kind,
    )
    .await?
    {
        return Ok(Some(response));
    }

    if supports_sync_scheduler_decision_kind(plan_kind) {
        if let Some(response) = maybe_execute_sync_via_local_video_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            &body_json,
            trace_id,
            decision,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = maybe_execute_sync_via_local_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            trace_id,
            decision,
            &body_json,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = maybe_execute_sync_via_local_openai_cli_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            trace_id,
            decision,
            &body_json,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = maybe_execute_sync_via_local_same_format_provider_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            trace_id,
            decision,
            &body_json,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = maybe_execute_sync_via_local_gemini_files_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            &body_json,
            body_base64.as_deref(),
            body_bytes.is_empty(),
            trace_id,
            decision,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }

        if let Some(response) = maybe_execute_sync_via_remote_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            trace_id,
            decision,
            &body_json,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }
    }

    maybe_execute_sync_via_plan_fallback(
        state,
        control_base_url,
        executor_base_url,
        parts,
        trace_id,
        decision,
        &body_json,
        body_base64,
        plan_kind,
        bypass_cache_key,
        if supports_sync_scheduler_decision_kind(plan_kind) {
            GatewayFallbackReason::RemoteDecisionMiss
        } else {
            GatewayFallbackReason::SchedulerDecisionUnsupported
        },
    )
    .await
}

async fn maybe_build_local_video_task_read_response(
    state: &AppState,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Option<Response<Body>>, GatewayError> {
    if parts.method != http::Method::GET || decision.route_kind.as_deref() != Some("video") {
        return Ok(None);
    }

    let _ = state
        .hydrate_video_task_for_route(decision.route_family.as_deref(), parts.uri.path())
        .await?;

    let refresh_plan = state.video_tasks.prepare_read_refresh_sync_plan(
        decision.route_family.as_deref(),
        parts.uri.path(),
        trace_id,
    );

    if let Some(refresh_plan) = refresh_plan {
        state
            .execute_video_task_refresh_plan(executor_base_url, &refresh_plan)
            .await?;
    }

    let read_response = state
        .video_tasks
        .read_response(decision.route_family.as_deref(), parts.uri.path());
    let read_response = match read_response {
        Some(read_response) => Some(read_response),
        None => {
            state
                .read_data_backed_video_task_response(
                    decision.route_family.as_deref(),
                    parts.uri.path(),
                )
                .await?
        }
    };
    let Some(read_response) = read_response else {
        return Ok(None);
    };

    let body_bytes = serde_json::to_vec(&read_response.body_json)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let mut headers = BTreeMap::new();
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("content-length".to_string(), body_bytes.len().to_string());

    Ok(Some(build_client_response_from_parts(
        read_response.status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

async fn maybe_execute_local_video_task_follow_up_sync(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !matches!(
        plan_kind,
        OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    ) {
        return Ok(None);
    }

    let _ = state
        .hydrate_video_task_for_route(decision.route_family.as_deref(), parts.uri.path())
        .await?;

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;
    let Some(follow_up) = state.video_tasks.prepare_follow_up_sync_plan(
        plan_kind,
        parts.uri.path(),
        Some(body_json),
        auth_context.as_ref(),
        trace_id,
    ) else {
        return Ok(None);
    };

    execute_executor_sync(
        state,
        control_base_url,
        executor_base_url,
        parts.uri.path(),
        follow_up.plan,
        trace_id,
        decision,
        plan_kind,
        follow_up.report_kind,
        follow_up.report_context,
    )
    .await
}

async fn maybe_build_local_video_task_follow_up_sync_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    if !matches!(
        plan_kind,
        OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    ) {
        return Ok(None);
    }

    let _ = state
        .hydrate_video_task_for_route(decision.route_family.as_deref(), parts.uri.path())
        .await?;

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;
    let Some(auth_context) = auth_context else {
        return Ok(None);
    };
    let Some(follow_up) = state.video_tasks.prepare_follow_up_sync_plan(
        plan_kind,
        parts.uri.path(),
        Some(body_json),
        Some(&auth_context),
        trace_id,
    ) else {
        return Ok(None);
    };

    let auth_pair = extract_auth_header_pair(&follow_up.plan.headers);

    Ok(Some(GatewayControlSyncDecisionResponse {
        action: EXECUTOR_SYNC_DECISION_ACTION.to_string(),
        decision_kind: Some(plan_kind.to_string()),
        request_id: Some(trace_id.to_string()),
        candidate_id: follow_up.plan.candidate_id.clone(),
        provider_name: follow_up.plan.provider_name.clone(),
        provider_id: Some(follow_up.plan.provider_id.clone()),
        endpoint_id: Some(follow_up.plan.endpoint_id.clone()),
        key_id: Some(follow_up.plan.key_id.clone()),
        upstream_base_url: infer_upstream_base_url(&follow_up.plan.url),
        upstream_url: Some(follow_up.plan.url.clone()),
        provider_request_method: Some(follow_up.plan.method.clone()),
        auth_header: auth_pair.as_ref().map(|(name, _)| name.clone()),
        auth_value: auth_pair.as_ref().map(|(_, value)| value.clone()),
        provider_api_format: Some(follow_up.plan.provider_api_format.clone()),
        client_api_format: Some(follow_up.plan.client_api_format.clone()),
        model_name: follow_up.plan.model_name.clone(),
        mapped_model: None,
        prompt_cache_key: None,
        extra_headers: BTreeMap::new(),
        provider_request_headers: follow_up.plan.headers.clone(),
        provider_request_body: follow_up.plan.body.json_body.clone(),
        provider_request_body_base64: follow_up.plan.body.body_bytes_b64.clone(),
        content_type: follow_up.plan.content_type.clone(),
        proxy: follow_up.plan.proxy.clone(),
        tls_profile: follow_up.plan.tls_profile.clone(),
        timeouts: follow_up.plan.timeouts.clone(),
        upstream_is_stream: false,
        report_kind: follow_up.report_kind,
        report_context: follow_up.report_context,
        auth_context: Some(auth_context),
    }))
}

fn extract_auth_header_pair(headers: &BTreeMap<String, String>) -> Option<(String, String)> {
    [
        "authorization",
        "x-api-key",
        "api-key",
        "x-goog-api-key",
        "proxy-authorization",
    ]
    .into_iter()
    .find_map(|name| {
        headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(header_name, value)| (header_name.clone(), value.clone()))
    })
}

fn infer_upstream_base_url(upstream_url: &str) -> Option<String> {
    let parsed = Url::parse(upstream_url).ok()?;
    let host = parsed.host_str()?;
    let mut base = format!("{}://{}", parsed.scheme(), host);
    if let Some(port) = parsed.port() {
        base.push(':');
        base.push_str(port.to_string().as_str());
    }
    Some(base)
}
