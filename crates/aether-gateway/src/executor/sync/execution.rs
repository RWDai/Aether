use base64::Engine as _;

use super::super::submission::submit_local_core_error_or_sync_finalize;
use super::*;
use crate::gateway::executor::request_candidates::{
    current_unix_secs as current_request_candidate_unix_secs,
    ensure_execution_request_candidate_slot, execution_error_details,
    record_local_request_candidate_status,
};
use crate::gateway::scheduler::{
    resolve_core_sync_error_finalize_report_kind, should_fallback_to_control_sync,
    should_finalize_sync_response, should_retry_next_local_candidate_sync,
};
use crate::gateway::usage::{spawn_sync_report, submit_sync_report};
use crate::gateway::video_tasks::VideoTaskSyncReportMode;

#[path = "execution/policy.rs"]
mod policy;
#[path = "execution/response.rs"]
mod response;

use policy::decode_execution_result_body;
pub(crate) use response::{
    maybe_build_local_sync_finalize_response, maybe_build_local_video_error_response,
    maybe_build_local_video_success_outcome, resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind, LocalVideoSyncSuccessOutcome,
};

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
pub(crate) async fn execute_executor_sync(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    request_path: &str,
    mut plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    mut report_context: Option<serde_json::Value>,
) -> Result<Option<Response<Body>>, GatewayError> {
    ensure_execution_request_candidate_slot(state, &mut plan, &mut report_context).await;
    let plan_request_id = plan.request_id.as_str();
    let plan_candidate_id = plan.candidate_id.as_deref();
    let response = match state
        .client
        .post(format!("{executor_base_url}/v1/execute/sync"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&plan)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway direct executor sync unavailable");
            return Ok(None);
        }
    };

    if response.status() != http::StatusCode::OK {
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(response.status().as_u16()),
            Some("executor_http_error".to_string()),
            Some(format!("executor returned HTTP {}", response.status())),
            None,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        return Ok(Some(attach_control_metadata_headers(
            build_client_response(response, trace_id, Some(decision))?,
            Some(plan_request_id),
            plan_candidate_id,
        )?));
    }

    let result: ExecutionResult = response
        .json()
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let result_body_json = result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref());
    let (result_error_type, result_error_message) =
        execution_error_details(result.error.as_ref(), result_body_json);
    let result_latency_ms = result
        .telemetry
        .as_ref()
        .and_then(|telemetry| telemetry.elapsed_ms);
    if should_retry_next_local_candidate_sync(plan_kind, report_context.as_ref(), &result) {
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(result.status_code),
            result_error_type.clone(),
            result_error_message.clone(),
            result_latency_ms,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        warn!(
            trace_id = %trace_id,
            request_id = %plan_request_id,
            status_code = result.status_code,
            "gateway local sync decision retrying next candidate after retryable executor result"
        );
        return Ok(None);
    }
    let request_id = (!result.request_id.trim().is_empty())
        .then_some(result.request_id.as_str())
        .or(Some(plan_request_id));
    let candidate_id = result.candidate_id.as_deref().or(plan_candidate_id);
    let mut headers = result.headers.clone();
    let (body_bytes, body_json, body_base64) = decode_execution_result_body(&result, &mut headers)?;
    let has_body_bytes = body_base64.is_some();
    let explicit_finalize = should_finalize_sync_response(report_kind.as_deref());
    let mapped_error_finalize_kind =
        resolve_core_sync_error_finalize_report_kind(plan_kind, &result, body_json.as_ref());
    let finalize_report_kind = if explicit_finalize {
        report_kind.clone()
    } else {
        mapped_error_finalize_kind.clone()
    };

    if should_fallback_to_control_sync(
        plan_kind,
        &result,
        body_json.as_ref(),
        has_body_bytes,
        explicit_finalize,
        mapped_error_finalize_kind.is_some(),
    ) {
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(result.status_code),
            result_error_type.clone(),
            result_error_message.clone(),
            result_latency_ms,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        return Ok(None);
    }

    state
        .usage_runtime
        .record_pending(state.data.as_ref(), &plan, report_context.as_ref())
        .await;
    state
        .usage_runtime
        .record_sync_terminal(
            state.data.as_ref(),
            &plan,
            report_context.as_ref(),
            &GatewaySyncReportRequest {
                trace_id: trace_id.to_string(),
                report_kind: finalize_report_kind
                    .clone()
                    .or_else(|| report_kind.clone())
                    .unwrap_or_default(),
                report_context: report_context.clone(),
                status_code: result.status_code,
                headers: headers.clone(),
                body_json: body_json.clone(),
                client_body_json: None,
                body_base64: body_base64.clone(),
                telemetry: result.telemetry.clone(),
            },
        )
        .await;
    let terminal_unix_secs = current_request_candidate_unix_secs();
    record_local_request_candidate_status(
        state,
        &plan,
        report_context.as_ref(),
        if result.status_code >= 400 {
            aether_data::repository::candidates::RequestCandidateStatus::Failed
        } else {
            aether_data::repository::candidates::RequestCandidateStatus::Success
        },
        Some(result.status_code),
        result_error_type.clone(),
        result_error_message.clone(),
        result_latency_ms,
        Some(terminal_unix_secs),
        Some(terminal_unix_secs),
    )
    .await;

    if let Some(finalize_report_kind) = finalize_report_kind {
        let payload = GatewaySyncReportRequest {
            trace_id: trace_id.to_string(),
            report_kind: finalize_report_kind,
            report_context,
            status_code: result.status_code,
            headers: headers.clone(),
            body_json: body_json.clone(),
            client_body_json: None,
            body_base64: body_base64.clone(),
            telemetry: result.telemetry.clone(),
        };
        if let Some(outcome) =
            maybe_build_local_core_sync_finalize_response(trace_id, decision, &payload)?
        {
            if let Some(report_payload) = outcome.background_report {
                spawn_sync_report(
                    state.clone(),
                    control_base_url.to_string(),
                    trace_id.to_string(),
                    report_payload,
                );
            } else {
                warn!(
                    trace_id = %trace_id,
                    report_kind = %payload.report_kind,
                    "gateway local core finalize produced response without background success report mapping"
                );
            }
            return Ok(Some(attach_control_metadata_headers(
                outcome.response,
                request_id,
                candidate_id,
            )?));
        }
        if let Some(outcome) = maybe_build_local_video_success_outcome(
            trace_id,
            decision,
            &payload,
            &state.video_tasks,
            &plan,
        )? {
            if let Some(snapshot) = outcome.local_task_snapshot.clone() {
                state.video_tasks.record_snapshot(snapshot.clone());
                let _ = state.upsert_video_task_snapshot(&snapshot).await?;
            }
            match outcome.report_mode {
                VideoTaskSyncReportMode::InlineSync => {
                    submit_sync_report(state, control_base_url, trace_id, outcome.report_payload)
                        .await?;
                }
                VideoTaskSyncReportMode::Background => {
                    spawn_sync_report(
                        state.clone(),
                        control_base_url.to_string(),
                        trace_id.to_string(),
                        outcome.report_payload,
                    );
                }
            }
            return Ok(Some(attach_control_metadata_headers(
                outcome.response,
                request_id,
                candidate_id,
            )?));
        }
        if let Some(response) =
            maybe_build_local_sync_finalize_response(trace_id, decision, &payload)?
        {
            state
                .video_tasks
                .apply_finalize_mutation(request_path, payload.report_kind.as_str());
            if let Some(snapshot) = state
                .video_tasks
                .snapshot_for_route(decision.route_family.as_deref(), request_path)
            {
                let _ = state.upsert_video_task_snapshot(&snapshot).await?;
            }
            if let Some(success_report_kind) =
                resolve_local_sync_success_background_report_kind(payload.report_kind.as_str())
            {
                let mut report_payload = payload.clone();
                report_payload.report_kind = success_report_kind;
                spawn_sync_report(
                    state.clone(),
                    control_base_url.to_string(),
                    trace_id.to_string(),
                    report_payload,
                );
            } else {
                warn!(
                    trace_id = %trace_id,
                    report_kind = %payload.report_kind,
                    "gateway local video finalize produced response without background success report mapping"
                );
            }
            return Ok(Some(attach_control_metadata_headers(
                response,
                request_id,
                candidate_id,
            )?));
        }
        if let Some(response) =
            maybe_build_local_video_error_response(trace_id, decision, &payload)?
        {
            if let Some(error_report_kind) =
                resolve_local_sync_error_background_report_kind(payload.report_kind.as_str())
            {
                let mut report_payload = payload.clone();
                report_payload.report_kind = error_report_kind;
                spawn_sync_report(
                    state.clone(),
                    control_base_url.to_string(),
                    trace_id.to_string(),
                    report_payload,
                );
            } else {
                warn!(
                    trace_id = %trace_id,
                    report_kind = %payload.report_kind,
                    "gateway local video finalize produced response without background error report mapping"
                );
            }
            return Ok(Some(attach_control_metadata_headers(
                response,
                request_id,
                candidate_id,
            )?));
        }
        let response = submit_local_core_error_or_sync_finalize(
            state,
            control_base_url,
            trace_id,
            decision,
            payload,
        )
        .await?;
        return Ok(Some(attach_control_metadata_headers(
            response,
            request_id,
            candidate_id,
        )?));
    }

    if let Some(report_kind) = report_kind {
        let report = GatewaySyncReportRequest {
            trace_id: trace_id.to_string(),
            report_kind,
            report_context,
            status_code: result.status_code,
            headers: headers.clone(),
            body_json: body_json.clone(),
            client_body_json: None,
            body_base64: body_base64.clone(),
            telemetry: result.telemetry.clone(),
        };
        spawn_sync_report(
            state.clone(),
            control_base_url.to_string(),
            trace_id.to_string(),
            report,
        );
    }

    if let Some(request_id) = request_id.map(str::trim).filter(|value| !value.is_empty()) {
        headers.insert(
            CONTROL_REQUEST_ID_HEADER.to_string(),
            request_id.to_string(),
        );
    }

    if let Some(candidate_id) = candidate_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.insert(
            CONTROL_CANDIDATE_ID_HEADER.to_string(),
            candidate_id.to_string(),
        );
    }

    Ok(Some(build_client_response_from_parts(
        result.status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}
