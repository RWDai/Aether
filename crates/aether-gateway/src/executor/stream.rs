use super::decision::maybe_execute_via_stream_decision_path;
use super::*;

#[path = "stream/error.rs"]
mod error;
#[path = "stream/execution.rs"]
mod execution;

pub(crate) use execution::execute_executor_stream;

pub(crate) async fn maybe_execute_via_executor_stream(
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

    maybe_execute_via_stream_decision_path(
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
