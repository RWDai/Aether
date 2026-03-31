use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use axum::body::Bytes;

use super::super::*;

fn is_codex_cli_upstream_url(url: &str) -> bool {
    let url = url.trim().to_ascii_lowercase();
    url.contains("/codex") && (url.contains("/backend-api/") || url.contains("/backendapi/"))
}

pub(in crate::gateway::executor) fn should_bypass_direct_executor_decision(
    payload: &GatewayControlSyncDecisionResponse,
) -> bool {
    let provider_api_format = payload
        .provider_api_format
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(
        provider_api_format.as_str(),
        "openai:cli" | "openai:compact"
    ) {
        return false;
    }

    payload
        .upstream_url
        .as_deref()
        .or(payload.upstream_base_url.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(is_codex_cli_upstream_url)
}

pub(in crate::gateway::executor) fn should_bypass_direct_executor_plan(
    plan: &ExecutionPlan,
) -> bool {
    let provider_api_format = plan.provider_api_format.trim().to_ascii_lowercase();
    if !matches!(
        provider_api_format.as_str(),
        "openai:cli" | "openai:compact"
    ) {
        return false;
    }

    is_codex_cli_upstream_url(&plan.url)
}

pub(in crate::gateway::executor) fn allows_remote_python_control_fallback(plan_kind: &str) -> bool {
    matches!(
        plan_kind,
        GEMINI_FILES_GET_PLAN_KIND
            | GEMINI_FILES_UPLOAD_PLAN_KIND
            | GEMINI_FILES_LIST_PLAN_KIND
            | GEMINI_FILES_DELETE_PLAN_KIND
            | GEMINI_FILES_DOWNLOAD_PLAN_KIND
            | OPENAI_VIDEO_CONTENT_PLAN_KIND
            | OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    )
}

pub(in crate::gateway::executor) fn build_direct_plan_bypass_cache_key(
    plan_kind: &str,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    decision: &GatewayControlDecision,
) -> String {
    let mut hasher = DefaultHasher::new();
    plan_kind.hash(&mut hasher);
    parts.method.as_str().hash(&mut hasher);
    parts.uri.path().hash(&mut hasher);
    parts.uri.query().unwrap_or_default().hash(&mut hasher);
    decision
        .route_family
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    decision
        .route_kind
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    decision
        .auth_endpoint_signature
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, http::header::AUTHORIZATION.as_str())
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, "x-api-key")
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, "api-key")
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, http::header::CONTENT_TYPE.as_str())
        .unwrap_or_default()
        .hash(&mut hasher);
    body_bytes.hash(&mut hasher);
    format!("{plan_kind}:{:x}", hasher.finish())
}

pub(in crate::gateway::executor) fn should_skip_direct_plan(
    state: &AppState,
    cache_key: &str,
) -> bool {
    state
        .direct_plan_bypass_cache
        .should_skip(cache_key, DIRECT_PLAN_BYPASS_TTL)
}

pub(in crate::gateway::executor) fn mark_direct_plan_bypass(state: &AppState, cache_key: String) {
    state.direct_plan_bypass_cache.mark(
        cache_key,
        DIRECT_PLAN_BYPASS_TTL,
        DIRECT_PLAN_BYPASS_MAX_ENTRIES,
    );
}
