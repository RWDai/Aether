mod candidate;
mod failover;
mod health;
mod route;

pub(crate) use candidate::{
    list_selectable_candidates,
    list_selectable_candidates_for_required_capability_without_requested_model,
    read_minimal_candidate_selection, GatewayMinimalCandidateSelectionCandidate,
};
pub(crate) use failover::{
    resolve_core_stream_direct_finalize_report_kind,
    resolve_core_stream_error_finalize_report_kind, resolve_core_sync_error_finalize_report_kind,
    should_fallback_to_control_stream, should_fallback_to_control_sync,
    should_finalize_sync_response, should_retry_next_local_candidate_stream,
    should_retry_next_local_candidate_sync,
};
pub(crate) use health::{
    count_recent_rpm_requests_for_provider_key, count_recent_rpm_requests_for_provider_key_since,
    is_provider_key_circuit_open, provider_key_health_score, provider_key_rpm_allows_request_since,
    PROVIDER_KEY_RPM_WINDOW_SECS,
};
pub(crate) use route::{
    is_matching_stream_request, resolve_direct_executor_stream_plan_kind,
    resolve_direct_executor_sync_plan_kind, supports_stream_scheduler_decision_kind,
    supports_sync_scheduler_decision_kind,
};
