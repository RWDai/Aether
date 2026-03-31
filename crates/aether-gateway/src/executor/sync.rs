use super::decision::maybe_execute_via_sync_decision_path;
use super::*;

#[path = "sync/execution.rs"]
mod execution;

pub(crate) use execution::execute_executor_sync;

#[allow(unused_imports)]
pub(crate) use execution::{
    maybe_build_local_sync_finalize_response, maybe_build_local_video_error_response,
    maybe_build_local_video_success_outcome, resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind, LocalVideoSyncSuccessOutcome,
};

pub(crate) async fn maybe_execute_via_executor_sync(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(executor_base_url) = state.executor_base_url.as_deref() else {
        return Ok(None);
    };
    let Some(decision) = decision else {
        return Ok(None);
    };
    let control_base_url = state.control_base_url.as_deref().unwrap_or("");

    maybe_execute_via_sync_decision_path(
        state,
        control_base_url,
        executor_base_url,
        parts,
        body_bytes,
        trace_id,
        decision,
    )
    .await
}
