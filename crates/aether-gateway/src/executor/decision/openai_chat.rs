use axum::body::Body;
use axum::http::Response;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use aether_data::repository::candidates::{RequestCandidateStatus, UpsertRequestCandidateRecord};
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

use crate::gateway::executor::request_candidates::record_local_request_candidate_status;
use crate::gateway::headers::collect_control_headers;
use crate::gateway::provider_transport::{
    apply_local_body_rules, apply_local_header_rules, build_openai_chat_url,
    build_openai_passthrough_headers, build_passthrough_path_url, resolve_local_openai_chat_auth,
    resolve_transport_execution_timeouts, resolve_transport_proxy_snapshot,
    resolve_transport_tls_profile, supports_local_openai_chat_transport,
    LocalResolvedOAuthRequestAuth,
};
use crate::gateway::scheduler::{
    list_selectable_candidates, GatewayMinimalCandidateSelectionCandidate,
};
use crate::gateway::{AppState, GatewayControlDecision, GatewayError};

use super::super::plan_builders::{
    build_openai_chat_stream_plan_from_decision, build_openai_chat_sync_plan_from_decision,
    LocalStreamPlanAndReport, LocalSyncPlanAndReport,
};
use super::super::stream::execute_executor_stream;
use super::super::sync::execute_executor_sync;
use super::super::{
    GatewayControlSyncDecisionResponse, OPENAI_CHAT_STREAM_PLAN_KIND, OPENAI_CHAT_SYNC_PLAN_KIND,
};

pub(in crate::gateway::executor) async fn maybe_execute_sync_via_local_decision(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    let plan_and_reports = build_local_openai_chat_sync_plan_and_reports(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?;
    if plan_and_reports.is_empty() {
        return Ok(None);
    }

    let mut remaining = plan_and_reports.into_iter();
    while let Some(plan_and_report) = remaining.next() {
        if let Some(response) = execute_executor_sync(
            state,
            control_base_url,
            executor_base_url,
            parts.uri.path(),
            plan_and_report.plan,
            trace_id,
            decision,
            plan_kind,
            plan_and_report.report_kind,
            plan_and_report.report_context,
        )
        .await?
        {
            mark_unused_local_openai_chat_candidates(state, remaining.collect()).await;
            return Ok(Some(response));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_execute_stream_via_local_decision(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    let plan_and_reports = build_local_openai_chat_stream_plan_and_reports(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?;
    if plan_and_reports.is_empty() {
        return Ok(None);
    }

    let mut remaining = plan_and_reports.into_iter();
    while let Some(plan_and_report) = remaining.next() {
        if let Some(response) = execute_executor_stream(
            state,
            control_base_url,
            executor_base_url,
            plan_and_report.plan,
            trace_id,
            decision,
            plan_kind,
            plan_and_report.report_kind,
            plan_and_report.report_context,
        )
        .await?
        {
            mark_unused_local_openai_chat_candidates(state, remaining.collect()).await;
            return Ok(Some(response));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_build_sync_local_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    if plan_kind != OPENAI_CHAT_SYNC_PLAN_KIND {
        return Ok(None);
    }

    let Some(input) =
        resolve_local_openai_chat_decision_input(state, trace_id, decision, body_json).await
    else {
        return Ok(None);
    };

    let candidates = match list_selectable_candidates(
        state,
        "openai:chat",
        &input.requested_model,
        false,
        Some(&input.auth_snapshot),
        current_unix_secs(),
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat sync decision scheduler selection failed"
            );
            return Ok(None);
        }
    };

    let attempts =
        materialize_local_openai_chat_candidate_attempts(state, trace_id, &input, candidates).await;

    for attempt in attempts {
        if let Some(payload) = maybe_build_local_openai_chat_decision_payload_for_candidate(
            state,
            parts,
            trace_id,
            body_json,
            &input,
            attempt,
            OPENAI_CHAT_SYNC_PLAN_KIND,
            "openai_chat_sync_success",
            false,
        )
        .await
        {
            return Ok(Some(payload));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_build_stream_local_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    if plan_kind != OPENAI_CHAT_STREAM_PLAN_KIND {
        return Ok(None);
    }

    let Some(input) =
        resolve_local_openai_chat_decision_input(state, trace_id, decision, body_json).await
    else {
        return Ok(None);
    };

    let candidates = match list_selectable_candidates(
        state,
        "openai:chat",
        &input.requested_model,
        true,
        Some(&input.auth_snapshot),
        current_unix_secs(),
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat stream decision scheduler selection failed"
            );
            return Ok(None);
        }
    };

    let attempts =
        materialize_local_openai_chat_candidate_attempts(state, trace_id, &input, candidates).await;

    for attempt in attempts {
        if let Some(payload) = maybe_build_local_openai_chat_decision_payload_for_candidate(
            state,
            parts,
            trace_id,
            body_json,
            &input,
            attempt,
            OPENAI_CHAT_STREAM_PLAN_KIND,
            "openai_chat_stream_success",
            true,
        )
        .await
        {
            return Ok(Some(payload));
        }
    }

    Ok(None)
}

#[derive(Debug, Clone)]
struct LocalOpenAiChatDecisionInput {
    auth_context: crate::gateway::GatewayControlAuthContext,
    requested_model: String,
    auth_snapshot: crate::gateway::data::StoredGatewayAuthApiKeySnapshot,
}

#[derive(Debug, Clone)]
struct LocalOpenAiChatCandidateAttempt {
    candidate: GatewayMinimalCandidateSelectionCandidate,
    candidate_index: u32,
    candidate_id: String,
}

async fn build_local_openai_chat_sync_plan_and_reports(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Vec<LocalSyncPlanAndReport>, GatewayError> {
    if plan_kind != OPENAI_CHAT_SYNC_PLAN_KIND {
        return Ok(Vec::new());
    }

    let Some(input) =
        resolve_local_openai_chat_decision_input(state, trace_id, decision, body_json).await
    else {
        return Ok(Vec::new());
    };

    let candidates = match list_selectable_candidates(
        state,
        "openai:chat",
        &input.requested_model,
        false,
        Some(&input.auth_snapshot),
        current_unix_secs(),
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat sync decision scheduler selection failed"
            );
            return Ok(Vec::new());
        }
    };

    let attempts =
        materialize_local_openai_chat_candidate_attempts(state, trace_id, &input, candidates).await;

    let mut plans = Vec::new();
    for attempt in attempts {
        let Some(payload) = maybe_build_local_openai_chat_decision_payload_for_candidate(
            state,
            parts,
            trace_id,
            body_json,
            &input,
            attempt,
            OPENAI_CHAT_SYNC_PLAN_KIND,
            "openai_chat_sync_success",
            false,
        )
        .await
        else {
            continue;
        };

        match build_openai_chat_sync_plan_from_decision(parts, body_json, payload) {
            Ok(Some(value)) => plans.push(value),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    error = ?err,
                    "gateway local openai chat sync decision plan build failed"
                );
            }
        }
    }

    Ok(plans)
}

async fn build_local_openai_chat_stream_plan_and_reports(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Vec<LocalStreamPlanAndReport>, GatewayError> {
    if plan_kind != OPENAI_CHAT_STREAM_PLAN_KIND {
        return Ok(Vec::new());
    }

    let Some(input) =
        resolve_local_openai_chat_decision_input(state, trace_id, decision, body_json).await
    else {
        return Ok(Vec::new());
    };

    let candidates = match list_selectable_candidates(
        state,
        "openai:chat",
        &input.requested_model,
        true,
        Some(&input.auth_snapshot),
        current_unix_secs(),
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat stream decision scheduler selection failed"
            );
            return Ok(Vec::new());
        }
    };

    let attempts =
        materialize_local_openai_chat_candidate_attempts(state, trace_id, &input, candidates).await;

    let mut plans = Vec::new();
    for attempt in attempts {
        let Some(payload) = maybe_build_local_openai_chat_decision_payload_for_candidate(
            state,
            parts,
            trace_id,
            body_json,
            &input,
            attempt,
            OPENAI_CHAT_STREAM_PLAN_KIND,
            "openai_chat_stream_success",
            true,
        )
        .await
        else {
            continue;
        };

        match build_openai_chat_stream_plan_from_decision(parts, body_json, payload) {
            Ok(Some(value)) => plans.push(value),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    error = ?err,
                    "gateway local openai chat stream decision plan build failed"
                );
            }
        }
    }

    Ok(plans)
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn resolve_local_openai_chat_decision_input(
    state: &AppState,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
) -> Option<LocalOpenAiChatDecisionInput> {
    let Some(auth_context) = decision.auth_context.clone().filter(|auth_context| {
        !auth_context.user_id.trim().is_empty() && !auth_context.api_key_id.trim().is_empty()
    }) else {
        warn!(
            trace_id = %trace_id,
            route_class = ?decision.route_class,
            route_family = ?decision.route_family,
            route_kind = ?decision.route_kind,
            "gateway local openai chat decision skipped: missing_auth_context"
        );
        return None;
    };

    let Some(requested_model) = body_json
        .get("model")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
    else {
        warn!(
            trace_id = %trace_id,
            "gateway local openai chat decision skipped: missing_requested_model"
        );
        return None;
    };

    let now_unix_secs = current_unix_secs();
    let auth_snapshot = match state
        .read_auth_api_key_snapshot(
            &auth_context.user_id,
            &auth_context.api_key_id,
            now_unix_secs,
        )
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            warn!(
                trace_id = %trace_id,
                user_id = %auth_context.user_id,
                api_key_id = %auth_context.api_key_id,
                "gateway local openai chat decision skipped: auth_snapshot_missing"
            );
            return None;
        }
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat decision auth snapshot read failed"
            );
            return None;
        }
    };

    Some(LocalOpenAiChatDecisionInput {
        auth_context,
        requested_model,
        auth_snapshot,
    })
}

async fn maybe_build_local_openai_chat_decision_payload_for_candidate(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    body_json: &serde_json::Value,
    input: &LocalOpenAiChatDecisionInput,
    attempt: LocalOpenAiChatCandidateAttempt,
    decision_kind: &str,
    report_kind: &str,
    upstream_is_stream: bool,
) -> Option<GatewayControlSyncDecisionResponse> {
    let LocalOpenAiChatCandidateAttempt {
        candidate,
        candidate_index,
        candidate_id,
    } = attempt;
    let transport = match state
        .read_provider_transport_snapshot(
            &candidate.provider_id,
            &candidate.endpoint_id,
            &candidate.key_id,
        )
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            mark_skipped_local_openai_chat_candidate(
                state,
                input,
                trace_id,
                &candidate,
                candidate_index,
                &candidate_id,
                "transport_snapshot_missing",
            )
            .await;
            return None;
        }
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                error = ?err,
                "gateway local openai chat decision provider transport read failed"
            );
            mark_skipped_local_openai_chat_candidate(
                state,
                input,
                trace_id,
                &candidate,
                candidate_index,
                &candidate_id,
                "transport_snapshot_read_failed",
            )
            .await;
            return None;
        }
    };

    if !supports_local_openai_chat_transport(&transport) {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "transport_unsupported",
        )
        .await;
        return None;
    }

    let oauth_auth = if resolve_local_openai_chat_auth(&transport).is_none() {
        match state.resolve_local_oauth_request_auth(&transport).await {
            Ok(Some(LocalResolvedOAuthRequestAuth::Header { name, value })) => Some((name, value)),
            Ok(Some(LocalResolvedOAuthRequestAuth::Kiro(_))) => None,
            Ok(None) => None,
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    provider_type = %transport.provider.provider_type,
                    error = ?err,
                    "gateway local openai chat oauth auth resolution failed"
                );
                None
            }
        }
    } else {
        None
    };

    let Some((auth_header, auth_value)) = resolve_local_openai_chat_auth(&transport).or(oauth_auth)
    else {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "transport_auth_unavailable",
        )
        .await;
        return None;
    };
    let mapped_model = candidate.selected_provider_model_name.trim().to_string();
    if mapped_model.is_empty() {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "mapped_model_missing",
        )
        .await;
        return None;
    }

    let Some(provider_request_body) = build_local_openai_chat_request_body(
        body_json,
        &mapped_model,
        upstream_is_stream,
        transport.endpoint.body_rules.as_ref(),
    ) else {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "provider_request_body_missing",
        )
        .await;
        return None;
    };

    let Some(upstream_url) = build_local_openai_chat_upstream_url(parts, &transport) else {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "upstream_url_missing",
        )
        .await;
        return None;
    };

    let mut provider_request_headers = build_openai_passthrough_headers(
        &parts.headers,
        &auth_header,
        &auth_value,
        &BTreeMap::new(),
        Some("application/json"),
    );
    if !apply_local_header_rules(
        &mut provider_request_headers,
        transport.endpoint.header_rules.as_ref(),
        &[&auth_header, "content-type"],
        &provider_request_body,
        Some(body_json),
    ) {
        mark_skipped_local_openai_chat_candidate(
            state,
            input,
            trace_id,
            &candidate,
            candidate_index,
            &candidate_id,
            "transport_header_rules_apply_failed",
        )
        .await;
        return None;
    }
    let proxy = resolve_transport_proxy_snapshot(&transport);
    let tls_profile = resolve_transport_tls_profile(&transport);
    let prompt_cache_key = provider_request_body
        .get("prompt_cache_key")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Some(GatewayControlSyncDecisionResponse {
        action: if upstream_is_stream {
            super::super::EXECUTOR_STREAM_DECISION_ACTION.to_string()
        } else {
            super::super::EXECUTOR_SYNC_DECISION_ACTION.to_string()
        },
        decision_kind: Some(decision_kind.to_string()),
        request_id: Some(trace_id.to_string()),
        candidate_id: Some(candidate_id.clone()),
        provider_name: Some(transport.provider.name.clone()),
        provider_id: Some(candidate.provider_id.clone()),
        endpoint_id: Some(candidate.endpoint_id.clone()),
        key_id: Some(candidate.key_id.clone()),
        upstream_base_url: Some(transport.endpoint.base_url.clone()),
        upstream_url: Some(upstream_url.clone()),
        provider_request_method: None,
        auth_header: Some(auth_header),
        auth_value: Some(auth_value),
        provider_api_format: Some("openai:chat".to_string()),
        client_api_format: Some("openai:chat".to_string()),
        model_name: Some(input.requested_model.clone()),
        mapped_model: Some(mapped_model.clone()),
        prompt_cache_key,
        extra_headers: BTreeMap::new(),
        provider_request_headers: provider_request_headers.clone(),
        provider_request_body: Some(provider_request_body.clone()),
        provider_request_body_base64: None,
        content_type: Some("application/json".to_string()),
        proxy,
        tls_profile,
        timeouts: resolve_transport_execution_timeouts(&transport),
        upstream_is_stream,
        report_kind: Some(report_kind.to_string()),
        report_context: Some(json!({
            "user_id": input.auth_context.user_id,
            "api_key_id": input.auth_context.api_key_id,
            "request_id": trace_id,
            "candidate_id": candidate_id,
            "candidate_index": candidate_index,
            "retry_index": 0,
            "model": input.requested_model,
            "provider_name": transport.provider.name,
            "provider_id": candidate.provider_id,
            "endpoint_id": candidate.endpoint_id,
            "key_id": candidate.key_id,
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "mapped_model": mapped_model,
            "upstream_url": upstream_url,
            "provider_request_method": serde_json::Value::Null,
            "provider_request_headers": provider_request_headers,
            "provider_request_body": provider_request_body,
            "original_headers": collect_control_headers(&parts.headers),
            "original_request_body": body_json,
            "has_envelope": false,
            "needs_conversion": false,
        })),
        auth_context: Some(input.auth_context.clone()),
    })
}

fn build_local_openai_chat_request_body(
    body_json: &serde_json::Value,
    mapped_model: &str,
    upstream_is_stream: bool,
    body_rules: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    let request_body_object = body_json.as_object()?;
    let mut provider_request_body = serde_json::Map::from_iter(
        request_body_object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    provider_request_body.insert(
        "model".to_string(),
        serde_json::Value::String(mapped_model.to_string()),
    );
    if upstream_is_stream {
        provider_request_body.insert("stream".to_string(), serde_json::Value::Bool(true));
    }
    let mut provider_request_body = serde_json::Value::Object(provider_request_body);
    if !apply_local_body_rules(&mut provider_request_body, body_rules, Some(body_json)) {
        return None;
    }
    Some(provider_request_body)
}

fn build_local_openai_chat_upstream_url(
    parts: &http::request::Parts,
    transport: &crate::gateway::provider_transport::GatewayProviderTransportSnapshot,
) -> Option<String> {
    let custom_path = transport
        .endpoint
        .custom_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match custom_path {
        Some(path) => {
            build_passthrough_path_url(&transport.endpoint.base_url, path, parts.uri.query(), &[])
        }
        None => Some(build_openai_chat_url(
            &transport.endpoint.base_url,
            parts.uri.query(),
        )),
    }
}

async fn mark_skipped_local_openai_chat_candidate(
    state: &AppState,
    input: &LocalOpenAiChatDecisionInput,
    trace_id: &str,
    candidate: &GatewayMinimalCandidateSelectionCandidate,
    candidate_index: u32,
    candidate_id: &str,
    skip_reason: &'static str,
) {
    let terminal_unix_secs = current_unix_secs();
    if let Err(err) = state
        .upsert_request_candidate(UpsertRequestCandidateRecord {
            id: candidate_id.to_string(),
            request_id: trace_id.to_string(),
            user_id: Some(input.auth_context.user_id.clone()),
            api_key_id: Some(input.auth_context.api_key_id.clone()),
            username: None,
            api_key_name: None,
            candidate_index,
            retry_index: 0,
            provider_id: Some(candidate.provider_id.clone()),
            endpoint_id: Some(candidate.endpoint_id.clone()),
            key_id: Some(candidate.key_id.clone()),
            status: RequestCandidateStatus::Skipped,
            skip_reason: Some(skip_reason.to_string()),
            is_cached: Some(false),
            status_code: None,
            error_type: None,
            error_message: None,
            latency_ms: None,
            concurrent_requests: None,
            extra_data: None,
            required_capabilities: candidate.key_capabilities.clone(),
            created_at_unix_secs: None,
            started_at_unix_secs: None,
            finished_at_unix_secs: Some(terminal_unix_secs),
        })
        .await
    {
        warn!(
            trace_id = %trace_id,
            candidate_id = %candidate_id,
            skip_reason,
            error = ?err,
            "gateway local openai chat decision failed to persist skipped candidate"
        );
    }
}

async fn materialize_local_openai_chat_candidate_attempts(
    state: &AppState,
    trace_id: &str,
    input: &LocalOpenAiChatDecisionInput,
    candidates: Vec<GatewayMinimalCandidateSelectionCandidate>,
) -> Vec<LocalOpenAiChatCandidateAttempt> {
    let created_at_unix_secs = current_unix_secs();
    let mut attempts = Vec::with_capacity(candidates.len());

    for (candidate_index, candidate) in candidates.into_iter().enumerate() {
        let generated_candidate_id = Uuid::new_v4().to_string();
        let extra_data = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "global_model_id": candidate.global_model_id.clone(),
            "global_model_name": candidate.global_model_name.clone(),
            "model_id": candidate.model_id.clone(),
            "selected_provider_model_name": candidate.selected_provider_model_name.clone(),
            "mapping_matched_model": candidate.mapping_matched_model.clone(),
            "provider_name": candidate.provider_name.clone(),
            "key_name": candidate.key_name.clone(),
        });

        let candidate_id = match state
            .upsert_request_candidate(UpsertRequestCandidateRecord {
                id: generated_candidate_id.clone(),
                request_id: trace_id.to_string(),
                user_id: Some(input.auth_context.user_id.clone()),
                api_key_id: Some(input.auth_context.api_key_id.clone()),
                username: None,
                api_key_name: None,
                candidate_index: candidate_index as u32,
                retry_index: 0,
                provider_id: Some(candidate.provider_id.clone()),
                endpoint_id: Some(candidate.endpoint_id.clone()),
                key_id: Some(candidate.key_id.clone()),
                status: RequestCandidateStatus::Available,
                skip_reason: None,
                is_cached: Some(false),
                status_code: None,
                error_type: None,
                error_message: None,
                latency_ms: None,
                concurrent_requests: None,
                extra_data: Some(extra_data),
                required_capabilities: candidate.key_capabilities.clone(),
                created_at_unix_secs: Some(created_at_unix_secs),
                started_at_unix_secs: None,
                finished_at_unix_secs: None,
            })
            .await
        {
            Ok(Some(stored)) => stored.id,
            Ok(None) => generated_candidate_id.clone(),
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    error = ?err,
                    "gateway local openai chat decision request candidate upsert failed"
                );
                generated_candidate_id.clone()
            }
        };

        attempts.push(LocalOpenAiChatCandidateAttempt {
            candidate,
            candidate_index: candidate_index as u32,
            candidate_id,
        });
    }

    attempts
}

async fn mark_unused_local_openai_chat_candidates<T>(state: &AppState, remaining: Vec<T>)
where
    T: LocalOpenAiChatPlanAndReport,
{
    for plan_and_report in remaining {
        record_local_request_candidate_status(
            state,
            plan_and_report.plan(),
            plan_and_report.report_context(),
            RequestCandidateStatus::Unused,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
    }
}

trait LocalOpenAiChatPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan;

    fn report_context(&self) -> Option<&serde_json::Value>;
}

impl LocalOpenAiChatPlanAndReport for LocalSyncPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan {
        &self.plan
    }

    fn report_context(&self) -> Option<&serde_json::Value> {
        self.report_context.as_ref()
    }
}

impl LocalOpenAiChatPlanAndReport for LocalStreamPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan {
        &self.plan
    }

    fn report_context(&self) -> Option<&serde_json::Value> {
        self.report_context.as_ref()
    }
}
