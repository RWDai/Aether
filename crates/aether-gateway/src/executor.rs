use std::collections::BTreeMap;
use std::io::Error as IoError;
use std::time::Duration;

use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionTimeouts, ProxySnapshot, RequestBody, StreamFrame,
    StreamFramePayload,
};
use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::http::Response;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;
use tracing::warn;

use crate::gateway::constants::*;
use crate::gateway::headers::{collect_control_headers, header_value_str, is_json_request};
use crate::gateway::{
    attach_control_metadata_headers, build_client_response, build_client_response_from_parts,
    local_finalize::maybe_build_local_core_sync_finalize_response,
    local_stream::maybe_build_local_stream_rewriter, resolve_executor_auth_context, usage,
    AppState, GatewayControlAuthContext, GatewayControlDecision, GatewayError,
};

const GEMINI_FILES_GET_PLAN_KIND: &str = "gemini_files_get";
const GEMINI_FILES_UPLOAD_PLAN_KIND: &str = "gemini_files_upload";
const GEMINI_FILES_LIST_PLAN_KIND: &str = "gemini_files_list";
const GEMINI_FILES_DELETE_PLAN_KIND: &str = "gemini_files_delete";
const GEMINI_FILES_DOWNLOAD_PLAN_KIND: &str = "gemini_files_download";
const OPENAI_VIDEO_CONTENT_PLAN_KIND: &str = "openai_video_content";
const OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND: &str = "openai_video_cancel_sync";
const OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND: &str = "openai_video_remix_sync";
const OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND: &str = "openai_video_delete_sync";
const GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND: &str = "gemini_video_create_sync";
const GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND: &str = "gemini_video_cancel_sync";
const OPENAI_CHAT_STREAM_PLAN_KIND: &str = "openai_chat_stream";
const CLAUDE_CHAT_STREAM_PLAN_KIND: &str = "claude_chat_stream";
const GEMINI_CHAT_STREAM_PLAN_KIND: &str = "gemini_chat_stream";
const OPENAI_CLI_STREAM_PLAN_KIND: &str = "openai_cli_stream";
const OPENAI_COMPACT_STREAM_PLAN_KIND: &str = "openai_compact_stream";
const CLAUDE_CLI_STREAM_PLAN_KIND: &str = "claude_cli_stream";
const GEMINI_CLI_STREAM_PLAN_KIND: &str = "gemini_cli_stream";
const OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND: &str = "openai_video_create_sync";
const OPENAI_CHAT_SYNC_PLAN_KIND: &str = "openai_chat_sync";
const OPENAI_CLI_SYNC_PLAN_KIND: &str = "openai_cli_sync";
const OPENAI_COMPACT_SYNC_PLAN_KIND: &str = "openai_compact_sync";
const CLAUDE_CHAT_SYNC_PLAN_KIND: &str = "claude_chat_sync";
const GEMINI_CHAT_SYNC_PLAN_KIND: &str = "gemini_chat_sync";
const CLAUDE_CLI_SYNC_PLAN_KIND: &str = "claude_cli_sync";
const GEMINI_CLI_SYNC_PLAN_KIND: &str = "gemini_cli_sync";
const EXECUTOR_SYNC_ACTION: &str = "executor_sync";
const EXECUTOR_SYNC_DECISION_ACTION: &str = "executor_sync_decision";
const EXECUTOR_STREAM_ACTION: &str = "executor_stream";
const EXECUTOR_STREAM_DECISION_ACTION: &str = "executor_stream_decision";
const MAX_ERROR_BODY_BYTES: usize = 16_384;
const MAX_STREAM_PREFETCH_FRAMES: usize = 5;
const MAX_STREAM_PREFETCH_BYTES: usize = 16_384;
const DIRECT_PLAN_BYPASS_TTL: Duration = Duration::from_secs(30);
const DIRECT_PLAN_BYPASS_MAX_ENTRIES: usize = 512;

fn allow_control_execute_fallback(state: &AppState, parts: &http::request::Parts) -> bool {
    state.executor_base_url.is_none()
        || header_value_str(&parts.headers, CONTROL_EXECUTE_FALLBACK_HEADER)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
}

#[derive(Debug, Serialize)]
struct GatewayControlPlanRequest {
    trace_id: String,
    method: String,
    path: String,
    query_string: Option<String>,
    headers: BTreeMap<String, String>,
    body_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct GatewayControlPlanResponse {
    pub(crate) action: String,
    #[serde(default)]
    pub(crate) plan_kind: Option<String>,
    #[serde(default)]
    pub(crate) plan: Option<ExecutionPlan>,
    #[serde(default)]
    pub(crate) report_kind: Option<String>,
    #[serde(default)]
    pub(crate) report_context: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct GatewayControlSyncDecisionResponse {
    action: String,
    #[serde(default)]
    decision_kind: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    candidate_id: Option<String>,
    #[serde(default)]
    provider_name: Option<String>,
    #[serde(default)]
    provider_id: Option<String>,
    #[serde(default)]
    endpoint_id: Option<String>,
    #[serde(default)]
    key_id: Option<String>,
    #[serde(default)]
    upstream_base_url: Option<String>,
    #[serde(default)]
    upstream_url: Option<String>,
    #[serde(default)]
    provider_request_method: Option<String>,
    #[serde(default)]
    auth_header: Option<String>,
    #[serde(default)]
    auth_value: Option<String>,
    #[serde(default)]
    provider_api_format: Option<String>,
    #[serde(default)]
    client_api_format: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    mapped_model: Option<String>,
    #[serde(default)]
    prompt_cache_key: Option<String>,
    #[serde(default)]
    extra_headers: BTreeMap<String, String>,
    #[serde(default)]
    provider_request_headers: BTreeMap<String, String>,
    #[serde(default)]
    provider_request_body: Option<serde_json::Value>,
    #[serde(default)]
    provider_request_body_base64: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    proxy: Option<ProxySnapshot>,
    #[serde(default)]
    tls_profile: Option<String>,
    #[serde(default)]
    timeouts: Option<ExecutionTimeouts>,
    #[serde(default)]
    upstream_is_stream: bool,
    #[serde(default)]
    report_kind: Option<String>,
    #[serde(default)]
    report_context: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) auth_context: Option<GatewayControlAuthContext>,
}

#[path = "executor/decision/mod.rs"]
mod decision;
#[path = "executor/plan_builders.rs"]
mod plan_builders;
#[path = "executor/request_candidates.rs"]
pub(crate) mod request_candidates;
#[path = "executor/stream.rs"]
mod stream;
#[path = "executor/submission.rs"]
mod submission;
#[path = "executor/sync.rs"]
mod sync;

pub(crate) use decision::maybe_build_stream_decision_payload_via_local_path;
pub(crate) use decision::maybe_build_stream_plan_payload_via_local_path;
pub(crate) use decision::maybe_build_sync_decision_payload_via_local_path;
pub(crate) use decision::maybe_build_sync_plan_payload_via_local_path;
pub(crate) use stream::execute_executor_stream;
pub(crate) use stream::maybe_execute_via_executor_stream;
pub(crate) use sync::execute_executor_sync;
pub(crate) use sync::maybe_execute_via_executor_sync;
#[allow(unused_imports)]
pub(crate) use sync::{
    maybe_build_local_sync_finalize_response, maybe_build_local_video_error_response,
    maybe_build_local_video_success_outcome, resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind, LocalVideoSyncSuccessOutcome,
};
pub(crate) use usage::{GatewayStreamReportRequest, GatewaySyncReportRequest};

fn decision_has_exact_provider_request(payload: &GatewayControlSyncDecisionResponse) -> bool {
    !payload.provider_request_headers.is_empty()
        && (payload.provider_request_body.is_some()
            || payload
                .provider_request_body_base64
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false))
}

fn generic_decision_missing_exact_provider_request(
    payload: &GatewayControlSyncDecisionResponse,
) -> bool {
    if decision_has_exact_provider_request(payload) {
        return false;
    }

    warn!(
        decision_kind = payload.decision_kind.as_deref().unwrap_or_default(),
        provider_api_format = payload.provider_api_format.as_deref().unwrap_or_default(),
        client_api_format = payload.client_api_format.as_deref().unwrap_or_default(),
        "gateway generic decision missing exact provider request; falling back to plan"
    );
    true
}

#[cfg(test)]
#[path = "executor/tests.rs"]
mod tests;
