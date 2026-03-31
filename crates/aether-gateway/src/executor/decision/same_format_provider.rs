use axum::body::Body;
use axum::http::Response;
use std::collections::BTreeMap;
use url::form_urlencoded;

use aether_data::repository::candidates::{RequestCandidateStatus, UpsertRequestCandidateRecord};
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::gateway::executor::request_candidates::{
    current_unix_secs, record_local_request_candidate_status,
};
use crate::gateway::headers::collect_control_headers;
use crate::gateway::provider_transport::{
    apply_local_body_rules, apply_local_header_rules, build_antigravity_safe_v1internal_request,
    build_antigravity_static_identity_headers, build_antigravity_v1internal_url,
    build_claude_code_messages_url, build_claude_code_passthrough_headers,
    build_claude_messages_url, build_gemini_content_url,
    build_kiro_generate_assistant_response_url, build_kiro_provider_headers,
    build_kiro_provider_request_body, build_openai_passthrough_headers, build_passthrough_headers,
    build_passthrough_path_url, build_vertex_api_key_gemini_content_url,
    classify_local_antigravity_request_support, ensure_upstream_auth_header,
    resolve_local_gemini_auth, resolve_local_standard_auth,
    resolve_local_vertex_api_key_query_auth, resolve_transport_execution_timeouts,
    resolve_transport_proxy_snapshot, resolve_transport_tls_profile,
    sanitize_claude_code_request_body, supports_local_claude_code_transport_with_network,
    supports_local_gemini_transport_with_network,
    supports_local_kiro_request_transport_with_network,
    supports_local_standard_transport_with_network,
    supports_local_vertex_api_key_gemini_transport_with_network, AntigravityEnvelopeRequestType,
    AntigravityRequestEnvelopeSupport, AntigravityRequestSideSupport, AntigravityRequestUrlAction,
    LocalResolvedOAuthRequestAuth, KIRO_ENVELOPE_NAME,
};
use crate::gateway::scheduler::{
    list_selectable_candidates, GatewayMinimalCandidateSelectionCandidate,
};
use crate::gateway::{AppState, GatewayControlDecision, GatewayError};

use super::super::plan_builders::{
    build_gemini_stream_plan_from_decision, build_gemini_sync_plan_from_decision,
    build_standard_stream_plan_from_decision, build_standard_sync_plan_from_decision,
    LocalStreamPlanAndReport, LocalSyncPlanAndReport,
};
use super::super::stream::execute_executor_stream;
use super::super::sync::execute_executor_sync;
use super::super::{
    GatewayControlSyncDecisionResponse, CLAUDE_CHAT_STREAM_PLAN_KIND, CLAUDE_CHAT_SYNC_PLAN_KIND,
    CLAUDE_CLI_STREAM_PLAN_KIND, CLAUDE_CLI_SYNC_PLAN_KIND, EXECUTOR_STREAM_DECISION_ACTION,
    EXECUTOR_SYNC_DECISION_ACTION, GEMINI_CHAT_STREAM_PLAN_KIND, GEMINI_CHAT_SYNC_PLAN_KIND,
    GEMINI_CLI_STREAM_PLAN_KIND, GEMINI_CLI_SYNC_PLAN_KIND,
};

const ANTIGRAVITY_ENVELOPE_NAME: &str = "antigravity:v1internal";

pub(in crate::gateway::executor) async fn maybe_execute_sync_via_local_same_format_provider_decision(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(spec) = resolve_sync_spec(plan_kind) else {
        return Ok(None);
    };

    let plan_and_reports =
        build_local_sync_plan_and_reports(state, parts, trace_id, decision, body_json, spec)
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
            mark_unused_local_provider_candidates(state, remaining.collect()).await;
            return Ok(Some(response));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_execute_stream_via_local_same_format_provider_decision(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(spec) = resolve_stream_spec(plan_kind) else {
        return Ok(None);
    };

    let plan_and_reports =
        build_local_stream_plan_and_reports(state, parts, trace_id, decision, body_json, spec)
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
            mark_unused_local_provider_candidates(state, remaining.collect()).await;
            return Ok(Some(response));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_build_sync_local_same_format_provider_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    let Some(spec) = resolve_sync_spec(plan_kind) else {
        return Ok(None);
    };

    let Some(input) = resolve_local_same_format_provider_decision_input(
        state, parts, trace_id, decision, body_json, spec,
    )
    .await
    else {
        return Ok(None);
    };

    let attempts =
        materialize_local_same_format_provider_candidate_attempts(state, trace_id, &input, spec)
            .await?;

    for attempt in attempts {
        if let Some(payload) =
            maybe_build_local_same_format_provider_decision_payload_for_candidate(
                state, parts, trace_id, body_json, &input, attempt, spec,
            )
            .await
        {
            return Ok(Some(payload));
        }
    }

    Ok(None)
}

pub(in crate::gateway::executor) async fn maybe_build_stream_local_same_format_provider_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    let Some(spec) = resolve_stream_spec(plan_kind) else {
        return Ok(None);
    };

    let Some(input) = resolve_local_same_format_provider_decision_input(
        state, parts, trace_id, decision, body_json, spec,
    )
    .await
    else {
        return Ok(None);
    };

    let attempts =
        materialize_local_same_format_provider_candidate_attempts(state, trace_id, &input, spec)
            .await?;

    for attempt in attempts {
        if let Some(payload) =
            maybe_build_local_same_format_provider_decision_payload_for_candidate(
                state, parts, trace_id, body_json, &input, attempt, spec,
            )
            .await
        {
            return Ok(Some(payload));
        }
    }

    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSameFormatProviderFamily {
    Standard,
    Gemini,
}

#[derive(Debug, Clone, Copy)]
struct LocalSameFormatProviderSpec {
    api_format: &'static str,
    decision_kind: &'static str,
    report_kind: &'static str,
    family: LocalSameFormatProviderFamily,
    require_streaming: bool,
}

#[derive(Debug, Clone)]
struct LocalSameFormatProviderDecisionInput {
    auth_context: crate::gateway::GatewayControlAuthContext,
    requested_model: String,
    auth_snapshot: crate::gateway::data::StoredGatewayAuthApiKeySnapshot,
}

#[derive(Debug, Clone)]
struct LocalSameFormatProviderCandidateAttempt {
    candidate: GatewayMinimalCandidateSelectionCandidate,
    candidate_index: u32,
    candidate_id: String,
}

fn resolve_sync_spec(plan_kind: &str) -> Option<LocalSameFormatProviderSpec> {
    match plan_kind {
        CLAUDE_CHAT_SYNC_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "claude:chat",
            decision_kind: CLAUDE_CHAT_SYNC_PLAN_KIND,
            report_kind: "claude_chat_sync_success",
            family: LocalSameFormatProviderFamily::Standard,
            require_streaming: false,
        }),
        CLAUDE_CLI_SYNC_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "claude:cli",
            decision_kind: CLAUDE_CLI_SYNC_PLAN_KIND,
            report_kind: "claude_cli_sync_success",
            family: LocalSameFormatProviderFamily::Standard,
            require_streaming: false,
        }),
        GEMINI_CHAT_SYNC_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "gemini:chat",
            decision_kind: GEMINI_CHAT_SYNC_PLAN_KIND,
            report_kind: "gemini_chat_sync_success",
            family: LocalSameFormatProviderFamily::Gemini,
            require_streaming: false,
        }),
        GEMINI_CLI_SYNC_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "gemini:cli",
            decision_kind: GEMINI_CLI_SYNC_PLAN_KIND,
            report_kind: "gemini_cli_sync_success",
            family: LocalSameFormatProviderFamily::Gemini,
            require_streaming: false,
        }),
        _ => None,
    }
}

fn resolve_stream_spec(plan_kind: &str) -> Option<LocalSameFormatProviderSpec> {
    match plan_kind {
        CLAUDE_CHAT_STREAM_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "claude:chat",
            decision_kind: CLAUDE_CHAT_STREAM_PLAN_KIND,
            report_kind: "claude_chat_stream_success",
            family: LocalSameFormatProviderFamily::Standard,
            require_streaming: true,
        }),
        CLAUDE_CLI_STREAM_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "claude:cli",
            decision_kind: CLAUDE_CLI_STREAM_PLAN_KIND,
            report_kind: "claude_cli_stream_success",
            family: LocalSameFormatProviderFamily::Standard,
            require_streaming: true,
        }),
        GEMINI_CHAT_STREAM_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "gemini:chat",
            decision_kind: GEMINI_CHAT_STREAM_PLAN_KIND,
            report_kind: "gemini_chat_stream_success",
            family: LocalSameFormatProviderFamily::Gemini,
            require_streaming: true,
        }),
        GEMINI_CLI_STREAM_PLAN_KIND => Some(LocalSameFormatProviderSpec {
            api_format: "gemini:cli",
            decision_kind: GEMINI_CLI_STREAM_PLAN_KIND,
            report_kind: "gemini_cli_stream_success",
            family: LocalSameFormatProviderFamily::Gemini,
            require_streaming: true,
        }),
        _ => None,
    }
}

async fn build_local_sync_plan_and_reports(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    spec: LocalSameFormatProviderSpec,
) -> Result<Vec<LocalSyncPlanAndReport>, GatewayError> {
    let Some(input) = resolve_local_same_format_provider_decision_input(
        state, parts, trace_id, decision, body_json, spec,
    )
    .await
    else {
        return Ok(Vec::new());
    };

    let attempts =
        materialize_local_same_format_provider_candidate_attempts(state, trace_id, &input, spec)
            .await?;

    let mut plans = Vec::new();
    for attempt in attempts {
        let Some(payload) = maybe_build_local_same_format_provider_decision_payload_for_candidate(
            state, parts, trace_id, body_json, &input, attempt, spec,
        )
        .await
        else {
            continue;
        };

        let built = match spec.family {
            LocalSameFormatProviderFamily::Standard => {
                build_standard_sync_plan_from_decision(parts, body_json, payload)
            }
            LocalSameFormatProviderFamily::Gemini => {
                build_gemini_sync_plan_from_decision(parts, body_json, payload)
            }
        };

        match built {
            Ok(Some(value)) => plans.push(value),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    api_format = spec.api_format,
                    error = ?err,
                    "gateway local same-format sync decision plan build failed"
                );
            }
        }
    }

    Ok(plans)
}

async fn build_local_stream_plan_and_reports(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    spec: LocalSameFormatProviderSpec,
) -> Result<Vec<LocalStreamPlanAndReport>, GatewayError> {
    let Some(input) = resolve_local_same_format_provider_decision_input(
        state, parts, trace_id, decision, body_json, spec,
    )
    .await
    else {
        return Ok(Vec::new());
    };

    let attempts =
        materialize_local_same_format_provider_candidate_attempts(state, trace_id, &input, spec)
            .await?;

    let mut plans = Vec::new();
    for attempt in attempts {
        let Some(payload) = maybe_build_local_same_format_provider_decision_payload_for_candidate(
            state, parts, trace_id, body_json, &input, attempt, spec,
        )
        .await
        else {
            continue;
        };

        let built = match spec.family {
            LocalSameFormatProviderFamily::Standard => {
                build_standard_stream_plan_from_decision(parts, body_json, payload, false)
            }
            LocalSameFormatProviderFamily::Gemini => {
                build_gemini_stream_plan_from_decision(parts, body_json, payload)
            }
        };

        match built {
            Ok(Some(value)) => plans.push(value),
            Ok(None) => {}
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    api_format = spec.api_format,
                    error = ?err,
                    "gateway local same-format stream decision plan build failed"
                );
            }
        }
    }

    Ok(plans)
}

async fn resolve_local_same_format_provider_decision_input(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    spec: LocalSameFormatProviderSpec,
) -> Option<LocalSameFormatProviderDecisionInput> {
    let Some(auth_context) = decision.auth_context.clone().filter(|auth_context| {
        !auth_context.user_id.trim().is_empty() && !auth_context.api_key_id.trim().is_empty()
    }) else {
        return None;
    };

    let requested_model = match spec.family {
        LocalSameFormatProviderFamily::Standard => body_json
            .get("model")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)?,
        LocalSameFormatProviderFamily::Gemini => extract_gemini_model_from_path(parts.uri.path())?,
    };

    let auth_snapshot = match state
        .read_auth_api_key_snapshot(
            &auth_context.user_id,
            &auth_context.api_key_id,
            current_unix_secs(),
        )
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return None,
        Err(err) => {
            warn!(
                trace_id = %trace_id,
                api_format = spec.api_format,
                error = ?err,
                "gateway local same-format decision auth snapshot read failed"
            );
            return None;
        }
    };

    Some(LocalSameFormatProviderDecisionInput {
        auth_context,
        requested_model,
        auth_snapshot,
    })
}

async fn materialize_local_same_format_provider_candidate_attempts(
    state: &AppState,
    trace_id: &str,
    input: &LocalSameFormatProviderDecisionInput,
    spec: LocalSameFormatProviderSpec,
) -> Result<Vec<LocalSameFormatProviderCandidateAttempt>, GatewayError> {
    let candidates = list_selectable_candidates(
        state,
        spec.api_format,
        &input.requested_model,
        spec.require_streaming,
        Some(&input.auth_snapshot),
        current_unix_secs(),
    )
    .await?;

    let created_at_unix_secs = current_unix_secs();
    let mut attempts = Vec::with_capacity(candidates.len());
    for (candidate_index, candidate) in candidates.into_iter().enumerate() {
        let generated_candidate_id = Uuid::new_v4().to_string();
        let extra_data = json!({
            "provider_api_format": spec.api_format,
            "client_api_format": spec.api_format,
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
                    api_format = spec.api_format,
                    error = ?err,
                    "gateway local same-format decision request candidate upsert failed"
                );
                generated_candidate_id.clone()
            }
        };

        attempts.push(LocalSameFormatProviderCandidateAttempt {
            candidate,
            candidate_index: candidate_index as u32,
            candidate_id,
        });
    }

    Ok(attempts)
}

async fn maybe_build_local_same_format_provider_decision_payload_for_candidate(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    body_json: &serde_json::Value,
    input: &LocalSameFormatProviderDecisionInput,
    attempt: LocalSameFormatProviderCandidateAttempt,
    spec: LocalSameFormatProviderSpec,
) -> Option<GatewayControlSyncDecisionResponse> {
    let LocalSameFormatProviderCandidateAttempt {
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
            mark_skipped_local_same_format_provider_candidate(
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
                api_format = spec.api_format,
                error = ?err,
                "gateway local same-format decision provider transport read failed"
            );
            mark_skipped_local_same_format_provider_candidate(
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

    let is_antigravity = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("antigravity");
    let is_claude_code = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("claude_code");
    let is_vertex = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("vertex_ai");
    let transport_supported = match spec.family {
        _ if transport
            .provider
            .provider_type
            .trim()
            .eq_ignore_ascii_case("kiro") =>
        {
            supports_local_kiro_request_transport_with_network(&transport)
        }
        _ if is_antigravity => true,
        _ if is_claude_code => {
            supports_local_claude_code_transport_with_network(&transport, spec.api_format)
        }
        _ if is_vertex => supports_local_vertex_api_key_gemini_transport_with_network(&transport),
        LocalSameFormatProviderFamily::Standard => {
            supports_local_standard_transport_with_network(&transport, spec.api_format)
        }
        LocalSameFormatProviderFamily::Gemini => {
            supports_local_gemini_transport_with_network(&transport, spec.api_format)
        }
    };
    if !transport_supported {
        mark_skipped_local_same_format_provider_candidate(
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

    let is_kiro = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("kiro");
    let vertex_query_auth = if is_vertex {
        resolve_local_vertex_api_key_query_auth(&transport)
    } else {
        None
    };
    let should_try_oauth_auth = is_kiro
        || matches!(spec.family, LocalSameFormatProviderFamily::Standard)
            && resolve_local_standard_auth(&transport).is_none()
        || matches!(spec.family, LocalSameFormatProviderFamily::Gemini)
            && !is_vertex
            && resolve_local_gemini_auth(&transport).is_none();
    let oauth_auth = if should_try_oauth_auth {
        match state.resolve_local_oauth_request_auth(&transport).await {
            Ok(Some(LocalResolvedOAuthRequestAuth::Kiro(auth))) => {
                Some(LocalResolvedOAuthRequestAuth::Kiro(auth))
            }
            Ok(Some(LocalResolvedOAuthRequestAuth::Header { name, value })) => {
                Some(LocalResolvedOAuthRequestAuth::Header { name, value })
            }
            Ok(None) => None,
            Err(err) => {
                warn!(
                    trace_id = %trace_id,
                    api_format = spec.api_format,
                    provider_type = %transport.provider.provider_type,
                    error = ?err,
                    "gateway local same-format oauth auth resolution failed"
                );
                None
            }
        }
    } else {
        None
    };
    let kiro_auth = match oauth_auth.as_ref() {
        Some(LocalResolvedOAuthRequestAuth::Kiro(auth)) => Some(auth),
        _ => None,
    };
    let auth = if let Some(auth) = kiro_auth.as_ref() {
        Some((auth.name.to_string(), auth.value.clone()))
    } else if let Some(LocalResolvedOAuthRequestAuth::Header { name, value }) = oauth_auth.as_ref()
    {
        Some((name.clone(), value.clone()))
    } else if is_vertex {
        None
    } else {
        match spec.family {
            LocalSameFormatProviderFamily::Standard => resolve_local_standard_auth(&transport),
            LocalSameFormatProviderFamily::Gemini => resolve_local_gemini_auth(&transport),
        }
    };
    let (auth_header, auth_value) = match auth {
        Some((name, value)) => (Some(name), Some(value)),
        None if is_vertex && vertex_query_auth.is_some() => (None, None),
        None => {
            mark_skipped_local_same_format_provider_candidate(
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
        }
    };
    if is_vertex && vertex_query_auth.is_none() {
        mark_skipped_local_same_format_provider_candidate(
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
    }
    let mapped_model = candidate.selected_provider_model_name.trim().to_string();
    if mapped_model.is_empty() {
        mark_skipped_local_same_format_provider_candidate(
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

    let Some(base_provider_request_body) = build_same_format_provider_request_body(
        body_json,
        &mapped_model,
        spec,
        transport.endpoint.body_rules.as_ref(),
        is_kiro || is_antigravity || spec.require_streaming,
        kiro_auth,
        is_claude_code,
    ) else {
        mark_skipped_local_same_format_provider_candidate(
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

    let antigravity_auth = if is_antigravity {
        match classify_local_antigravity_request_support(
            &transport,
            &base_provider_request_body,
            AntigravityEnvelopeRequestType::Agent,
        ) {
            AntigravityRequestSideSupport::Supported(spec) => Some(spec.auth),
            AntigravityRequestSideSupport::Unsupported(_) => {
                mark_skipped_local_same_format_provider_candidate(
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
        }
    } else {
        None
    };
    let provider_request_body = if let Some(antigravity_auth) = antigravity_auth.as_ref() {
        match build_antigravity_safe_v1internal_request(
            antigravity_auth,
            trace_id,
            &mapped_model,
            &base_provider_request_body,
            AntigravityEnvelopeRequestType::Agent,
        ) {
            AntigravityRequestEnvelopeSupport::Supported(envelope) => envelope,
            AntigravityRequestEnvelopeSupport::Unsupported(_) => {
                mark_skipped_local_same_format_provider_candidate(
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
            }
        }
    } else {
        base_provider_request_body
    };
    let upstream_is_stream = is_kiro || is_antigravity || spec.require_streaming;
    let report_kind = if is_kiro && !spec.require_streaming {
        "claude_cli_sync_finalize"
    } else if is_antigravity && !spec.require_streaming {
        match spec.api_format {
            "gemini:chat" => "gemini_chat_sync_finalize",
            "gemini:cli" => "gemini_cli_sync_finalize",
            _ => spec.report_kind,
        }
    } else {
        spec.report_kind
    };

    let Some(upstream_url) = build_same_format_upstream_url(
        parts,
        &transport,
        &mapped_model,
        spec,
        upstream_is_stream,
        kiro_auth,
    ) else {
        mark_skipped_local_same_format_provider_candidate(
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

    let Some(provider_request_headers) = (if let Some(kiro_auth) = kiro_auth.as_ref() {
        build_kiro_provider_headers(
            &parts.headers,
            &provider_request_body,
            body_json,
            transport.endpoint.header_rules.as_ref(),
            auth_header.as_deref().unwrap_or_default(),
            auth_value.as_deref().unwrap_or_default(),
            &kiro_auth.auth_config,
            kiro_auth.machine_id.as_str(),
        )
    } else {
        let extra_headers = antigravity_auth
            .as_ref()
            .map(build_antigravity_static_identity_headers)
            .unwrap_or_default();
        let mut provider_request_headers = if is_claude_code {
            build_claude_code_passthrough_headers(
                &parts.headers,
                auth_header.as_deref().unwrap_or_default(),
                auth_value.as_deref().unwrap_or_default(),
                &extra_headers,
                upstream_is_stream,
                transport.key.fingerprint.as_ref(),
            )
        } else if is_vertex {
            build_passthrough_headers(&parts.headers, &extra_headers, Some("application/json"))
        } else {
            build_openai_passthrough_headers(
                &parts.headers,
                auth_header.as_deref().unwrap_or_default(),
                auth_value.as_deref().unwrap_or_default(),
                &extra_headers,
                Some("application/json"),
            )
        };
        let protected_headers = auth_header
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| vec![value, "content-type"])
            .unwrap_or_else(|| vec!["content-type"]);
        if !apply_local_header_rules(
            &mut provider_request_headers,
            transport.endpoint.header_rules.as_ref(),
            &protected_headers,
            &provider_request_body,
            Some(body_json),
        ) {
            None
        } else {
            if let (Some(auth_header), Some(auth_value)) =
                (auth_header.as_deref(), auth_value.as_deref())
            {
                ensure_upstream_auth_header(&mut provider_request_headers, auth_header, auth_value);
            }
            if upstream_is_stream {
                provider_request_headers
                    .insert("accept".to_string(), "text/event-stream".to_string());
            }
            Some(provider_request_headers)
        }
    }) else {
        mark_skipped_local_same_format_provider_candidate(
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
    };
    let prompt_cache_key = provider_request_body
        .get("prompt_cache_key")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let proxy = resolve_transport_proxy_snapshot(&transport);
    let tls_profile = resolve_transport_tls_profile(&transport);

    Some(GatewayControlSyncDecisionResponse {
        action: if spec.require_streaming {
            EXECUTOR_STREAM_DECISION_ACTION.to_string()
        } else {
            EXECUTOR_SYNC_DECISION_ACTION.to_string()
        },
        decision_kind: Some(spec.decision_kind.to_string()),
        request_id: Some(trace_id.to_string()),
        candidate_id: Some(candidate_id.clone()),
        provider_name: Some(transport.provider.name.clone()),
        provider_id: Some(candidate.provider_id.clone()),
        endpoint_id: Some(candidate.endpoint_id.clone()),
        key_id: Some(candidate.key_id.clone()),
        upstream_base_url: Some(transport.endpoint.base_url.clone()),
        upstream_url: Some(upstream_url.clone()),
        provider_request_method: None,
        auth_header,
        auth_value,
        provider_api_format: Some(spec.api_format.to_string()),
        client_api_format: Some(spec.api_format.to_string()),
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
            "provider_api_format": spec.api_format,
            "client_api_format": spec.api_format,
            "mapped_model": mapped_model,
            "upstream_url": upstream_url,
            "provider_request_method": serde_json::Value::Null,
            "provider_request_headers": provider_request_headers,
            "provider_request_body": provider_request_body,
            "original_headers": collect_control_headers(&parts.headers),
            "original_request_body": body_json,
            "has_envelope": is_kiro || is_antigravity,
            "envelope_name": if is_kiro {
                Some(KIRO_ENVELOPE_NAME)
            } else if is_antigravity {
                Some(ANTIGRAVITY_ENVELOPE_NAME)
            } else {
                None
            },
            "needs_conversion": false,
        })),
        auth_context: Some(input.auth_context.clone()),
    })
}

fn build_same_format_provider_request_body(
    body_json: &Value,
    mapped_model: &str,
    spec: LocalSameFormatProviderSpec,
    body_rules: Option<&Value>,
    upstream_is_stream: bool,
    kiro_auth: Option<&crate::gateway::provider_transport::KiroRequestAuth>,
    is_claude_code: bool,
) -> Option<Value> {
    if let Some(kiro_auth) = kiro_auth {
        return build_kiro_provider_request_body(
            body_json,
            mapped_model,
            &kiro_auth.auth_config,
            body_rules,
        );
    }

    let request_body_object = body_json.as_object()?;
    let mut provider_request_body = serde_json::Map::from_iter(
        request_body_object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    match spec.family {
        LocalSameFormatProviderFamily::Standard => {
            provider_request_body
                .insert("model".to_string(), Value::String(mapped_model.to_string()));
            if upstream_is_stream {
                provider_request_body.insert("stream".to_string(), Value::Bool(true));
            }
        }
        LocalSameFormatProviderFamily::Gemini => {
            provider_request_body.remove("model");
        }
    }
    let mut provider_request_body = Value::Object(provider_request_body);
    if is_claude_code {
        sanitize_claude_code_request_body(&mut provider_request_body);
    }
    if !apply_local_body_rules(&mut provider_request_body, body_rules, Some(body_json)) {
        return None;
    }
    Some(provider_request_body)
}

fn build_same_format_upstream_url(
    parts: &http::request::Parts,
    transport: &crate::gateway::provider_transport::GatewayProviderTransportSnapshot,
    mapped_model: &str,
    spec: LocalSameFormatProviderSpec,
    upstream_is_stream: bool,
    kiro_auth: Option<&crate::gateway::provider_transport::KiroRequestAuth>,
) -> Option<String> {
    if let Some(kiro_auth) = kiro_auth {
        return build_kiro_generate_assistant_response_url(
            &transport.endpoint.base_url,
            parts.uri.query(),
            Some(kiro_auth.auth_config.effective_api_region()),
        );
    }
    if transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("claude_code")
    {
        return Some(build_claude_code_messages_url(
            &transport.endpoint.base_url,
            parts.uri.query(),
        ));
    }
    if transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("vertex_ai")
    {
        let auth = resolve_local_vertex_api_key_query_auth(transport)?;
        return build_vertex_api_key_gemini_content_url(
            mapped_model,
            upstream_is_stream,
            &auth.value,
            parts.uri.query(),
        );
    }
    if transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("antigravity")
    {
        let query = parts.uri.query().map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect::<BTreeMap<String, String>>()
        });
        return build_antigravity_v1internal_url(
            &transport.endpoint.base_url,
            if upstream_is_stream {
                AntigravityRequestUrlAction::StreamGenerateContent
            } else {
                AntigravityRequestUrlAction::GenerateContent
            },
            query.as_ref(),
        );
    }

    let custom_path = transport
        .endpoint
        .custom_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(path) = custom_path {
        let blocked_keys = match spec.family {
            LocalSameFormatProviderFamily::Standard => &[][..],
            LocalSameFormatProviderFamily::Gemini => &["key"][..],
        };
        let url = build_passthrough_path_url(
            &transport.endpoint.base_url,
            path,
            parts.uri.query(),
            blocked_keys,
        )?;
        return Some(maybe_add_gemini_stream_alt_sse(url, spec));
    }

    let url = match spec.family {
        LocalSameFormatProviderFamily::Standard => Some(build_claude_messages_url(
            &transport.endpoint.base_url,
            parts.uri.query(),
        )),
        LocalSameFormatProviderFamily::Gemini => build_gemini_content_url(
            &transport.endpoint.base_url,
            mapped_model,
            spec.require_streaming,
            parts.uri.query(),
        ),
    }?;

    Some(maybe_add_gemini_stream_alt_sse(url, spec))
}

fn maybe_add_gemini_stream_alt_sse(
    upstream_url: String,
    spec: LocalSameFormatProviderSpec,
) -> String {
    if spec.family != LocalSameFormatProviderFamily::Gemini || !spec.require_streaming {
        return upstream_url;
    }

    let has_alt = upstream_url
        .split_once('?')
        .map(|(_, query)| {
            form_urlencoded::parse(query.as_bytes())
                .any(|(key, _)| key.as_ref().eq_ignore_ascii_case("alt"))
        })
        .unwrap_or(false);
    if has_alt {
        return upstream_url;
    }

    if upstream_url.contains('?') {
        format!("{upstream_url}&alt=sse")
    } else {
        format!("{upstream_url}?alt=sse")
    }
}

async fn mark_skipped_local_same_format_provider_candidate(
    state: &AppState,
    input: &LocalSameFormatProviderDecisionInput,
    trace_id: &str,
    candidate: &GatewayMinimalCandidateSelectionCandidate,
    candidate_index: u32,
    candidate_id: &str,
    skip_reason: &'static str,
) {
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
            finished_at_unix_secs: Some(current_unix_secs()),
        })
        .await
    {
        warn!(
            trace_id = %trace_id,
            candidate_id = %candidate_id,
            skip_reason,
            error = ?err,
            "gateway local same-format decision failed to persist skipped candidate"
        );
    }
}

async fn mark_unused_local_provider_candidates<T>(state: &AppState, remaining: Vec<T>)
where
    T: LocalSameFormatProviderPlanAndReport,
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

fn extract_gemini_model_from_path(path: &str) -> Option<String> {
    let (_, suffix) = path.split_once("/models/")?;
    let model = suffix
        .split_once(':')
        .map(|(value, _)| value)
        .unwrap_or(suffix);
    let model = model.trim();
    if model.is_empty() {
        None
    } else {
        Some(model.to_string())
    }
}

trait LocalSameFormatProviderPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan;

    fn report_context(&self) -> Option<&serde_json::Value>;
}

impl LocalSameFormatProviderPlanAndReport for LocalSyncPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan {
        &self.plan
    }

    fn report_context(&self) -> Option<&serde_json::Value> {
        self.report_context.as_ref()
    }
}

impl LocalSameFormatProviderPlanAndReport for LocalStreamPlanAndReport {
    fn plan(&self) -> &aether_contracts::ExecutionPlan {
        &self.plan
    }

    fn report_context(&self) -> Option<&serde_json::Value> {
        self.report_context.as_ref()
    }
}
