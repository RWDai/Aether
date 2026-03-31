#[path = "api/mod.rs"]
mod api;
#[path = "async_task/mod.rs"]
mod async_task;
#[path = "audit/mod.rs"]
mod audit;
#[path = "auth/mod.rs"]
mod auth;
#[path = "billing/mod.rs"]
mod billing;
#[path = "cache/mod.rs"]
mod cache;
#[path = "constants.rs"]
mod constants;
#[path = "control/mod.rs"]
mod control;
#[path = "data/mod.rs"]
mod data;
#[path = "error.rs"]
mod error;
#[path = "executor.rs"]
mod executor;
#[path = "fallback_metrics.rs"]
mod fallback_metrics;
#[path = "handlers.rs"]
mod handlers;
#[path = "headers.rs"]
mod headers;
#[path = "hooks/mod.rs"]
mod hooks;
#[path = "kiro_stream.rs"]
mod kiro_stream;
#[path = "local_finalize.rs"]
mod local_finalize;
#[path = "local_stream.rs"]
mod local_stream;
#[path = "maintenance/mod.rs"]
mod maintenance;
#[path = "middleware/mod.rs"]
mod middleware;
#[path = "model_fetch/mod.rs"]
mod model_fetch;
#[path = "provider_transport/mod.rs"]
mod provider_transport;
#[path = "rate_limit.rs"]
mod rate_limit;
#[path = "response.rs"]
mod response;
#[path = "gateway/router.rs"]
mod router;
#[path = "scheduler/mod.rs"]
mod scheduler;
#[path = "gateway/state.rs"]
mod state;
#[path = "usage/mod.rs"]
mod usage;
#[path = "wallet/mod.rs"]
mod wallet;

use aether_data::repository::proxy_nodes::{
    ProxyNodeHeartbeatMutation, ProxyNodeTunnelStatusMutation, StoredProxyNode,
    StoredProxyNodeEvent,
};
use aether_http::{build_http_client, HttpClientConfig};
use aether_runtime::{
    prometheus_response, service_up_sample, AdmissionPermit, ConcurrencyError, ConcurrencyGate,
    ConcurrencySnapshot, DistributedConcurrencyError, DistributedConcurrencyGate,
    DistributedConcurrencySnapshot, MetricKind, MetricLabel, MetricSample,
};
use axum::http::header::{HeaderName, HeaderValue};
use axum::routing::any;
use axum::Router;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::task::JoinHandle;

use aether_crypto::encrypt_python_fernet_plaintext;
pub(crate) use async_task::video as video_tasks;
pub(crate) use async_task::VideoTaskService;
pub use async_task::VideoTaskTruthSourceMode;
use async_task::{
    cancel_video_task, get_video_task_detail, get_video_task_stats, get_video_task_video,
    list_video_tasks, spawn_video_task_poller, VideoTaskPollerConfig,
};
use audit::{
    get_auth_api_key_snapshot, get_decision_trace, get_request_candidate_trace,
    list_recent_shadow_results,
};
pub(crate) use auth::{
    request_model_local_rejection, resolve_executor_auth_context,
    should_buffer_request_for_local_auth, trusted_auth_local_rejection, GatewayControlAuthContext,
    GatewayLocalAuthRejection,
};
use cache::{
    AuthApiKeyLastUsedCache, AuthContextCache, DirectPlanBypassCache, SchedulerAffinityCache,
};
pub(crate) use control::{
    allows_control_execute_emergency, maybe_execute_via_control, resolve_control_route,
    resolve_public_request_context, GatewayControlDecision, GatewayPublicRequestContext,
};
pub use data::GatewayDataConfig;
use data::GatewayDataState;
pub(crate) use error::GatewayError;
pub(crate) use executor::{
    execute_executor_stream, execute_executor_sync,
    maybe_build_stream_decision_payload_via_local_path,
    maybe_build_stream_plan_payload_via_local_path,
    maybe_build_sync_decision_payload_via_local_path, maybe_build_sync_plan_payload_via_local_path,
    maybe_execute_via_executor_stream, maybe_execute_via_executor_sync,
};
pub(crate) use fallback_metrics::{GatewayFallbackMetricKind, GatewayFallbackReason};
use handlers::proxy_request;
pub(crate) use hooks::record_shadow_result_non_blocking;
use hooks::{get_request_audit_bundle, get_request_usage_audit};
use maintenance::{
    spawn_audit_cleanup_worker, spawn_db_maintenance_worker,
    spawn_gemini_file_mapping_cleanup_worker, spawn_provider_checkin_worker,
    spawn_request_candidate_cleanup_worker,
};
use model_fetch::spawn_model_fetch_worker;
pub(crate) use model_fetch::{perform_model_fetch_once, ModelFetchRunSummary};
pub use rate_limit::FrontdoorUserRpmConfig;
pub(crate) use rate_limit::{FrontdoorUserRpmLimiter, FrontdoorUserRpmOutcome};
pub(crate) use response::{
    attach_control_metadata_headers, build_client_response, build_client_response_from_parts,
    build_local_auth_rejection_response, build_local_http_error_response,
    build_local_overloaded_response, build_local_user_rpm_limited_response,
};
pub use usage::UsageRuntimeConfig;
pub(crate) use wallet::{local_rejection_from_wallet_access, resolve_wallet_auth_gate};

pub use router::{
    build_router, build_router_with_control, build_router_with_endpoints, build_router_with_state,
    serve_tcp, serve_tcp_with_endpoints,
};
pub(crate) use router::{metrics, RequestAdmissionError};
pub(crate) use state::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingRuleRecord,
    AdminBillingRuleWriteInput, AdminWalletMutationOutcome, AdminWalletPaymentOrderRecord,
    AdminWalletRefundRecord, AdminWalletTransactionRecord, LocalMutationOutcome,
    LocalProviderDeleteTaskState,
};
pub use state::{AppState, FrontdoorCorsConfig};

fn insert_header_if_missing(
    headers: &mut http::HeaderMap,
    key: &'static str,
    value: &str,
) -> Result<(), GatewayError> {
    if headers.contains_key(key) {
        return Ok(());
    }
    let name = HeaderName::from_static(key);
    let value =
        HeaderValue::from_str(value).map_err(|err| GatewayError::Internal(err.to_string()))?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests;
