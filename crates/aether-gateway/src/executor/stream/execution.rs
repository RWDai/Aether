use aether_contracts::{ExecutionError, ExecutionTelemetry};
use base64::Engine as _;
use futures_util::TryStreamExt;
use serde_json::{Map, Value};
use tracing::debug;

use super::super::submission::{
    resolve_core_error_background_report_kind, submit_local_core_error_or_sync_finalize,
};
use super::super::*;
use super::error::{
    build_executor_error_response, collect_error_body, decode_stream_error_body,
    inspect_prefetched_stream_body, read_next_frame, StreamPrefetchInspection,
};
use crate::gateway::executor::request_candidates::{
    current_unix_secs as current_request_candidate_unix_secs,
    ensure_execution_request_candidate_slot, record_local_request_candidate_status,
    record_report_request_candidate_status,
};
use crate::gateway::scheduler::{
    resolve_core_stream_direct_finalize_report_kind,
    resolve_core_stream_error_finalize_report_kind, should_fallback_to_control_stream,
    should_retry_next_local_candidate_stream,
};
use crate::gateway::usage::{submit_stream_report, submit_sync_report};

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
pub(crate) async fn execute_executor_stream(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    mut plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    mut report_context: Option<serde_json::Value>,
) -> Result<Option<Response<Body>>, GatewayError> {
    ensure_execution_request_candidate_slot(state, &mut plan, &mut report_context).await;
    let request_id = plan.request_id.as_str();
    let candidate_id = plan.candidate_id.as_deref();
    let response = match state
        .client
        .post(format!("{executor_base_url}/v1/execute/stream"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&plan)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway direct executor stream unavailable");
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
            Some(request_id),
            candidate_id,
        )?));
    }

    let stream = response
        .bytes_stream()
        .map_err(|err| IoError::other(err.to_string()));
    let reader = StreamReader::new(stream);
    let mut lines = FramedRead::new(reader, LinesCodec::new());

    let first_frame = read_next_frame(&mut lines).await?.ok_or_else(|| {
        GatewayError::Internal("executor stream ended before headers frame".to_string())
    })?;
    let StreamFramePayload::Headers {
        status_code,
        mut headers,
    } = first_frame.payload
    else {
        return Err(GatewayError::Internal(
            "executor stream must start with headers frame".to_string(),
        ));
    };

    if should_retry_next_local_candidate_stream(plan_kind, report_context.as_ref(), status_code) {
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(status_code),
            Some("retryable_upstream_status".to_string()),
            Some(format!(
                "executor stream returned retryable status {status_code}"
            )),
            None,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        warn!(
            trace_id = %trace_id,
            request_id,
            status_code,
            "gateway local stream decision retrying next candidate after retryable executor status"
        );
        return Ok(None);
    }

    let stream_error_finalize_kind =
        resolve_core_stream_error_finalize_report_kind(plan_kind, status_code);

    if should_fallback_to_control_stream(
        plan_kind,
        status_code,
        stream_error_finalize_kind.is_some(),
    ) {
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(status_code),
            Some("control_fallback".to_string()),
            Some(format!(
                "stream decision fell back to control after status {status_code}"
            )),
            None,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        return Ok(None);
    }

    if status_code >= 400 {
        let error_body = collect_error_body(&mut lines).await?;
        let (body_json, body_base64) = decode_stream_error_body(&headers, &error_body);
        let usage_report_kind = stream_error_finalize_kind
            .clone()
            .or_else(|| report_kind.clone())
            .unwrap_or_default();
        let usage_payload = GatewaySyncReportRequest {
            trace_id: trace_id.to_string(),
            report_kind: usage_report_kind,
            report_context: report_context.clone(),
            status_code,
            headers: headers.clone(),
            body_json: body_json.clone(),
            client_body_json: None,
            body_base64: body_base64.clone(),
            telemetry: None,
        };
        state
            .usage_runtime
            .record_sync_terminal(
                state.data.as_ref(),
                &plan,
                report_context.as_ref(),
                &usage_payload,
            )
            .await;
        let terminal_unix_secs = current_request_candidate_unix_secs();
        record_local_request_candidate_status(
            state,
            &plan,
            report_context.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Failed,
            Some(status_code),
            Some("executor_stream_error".to_string()),
            Some(format!(
                "executor stream returned error status {status_code}"
            )),
            None,
            Some(terminal_unix_secs),
            Some(terminal_unix_secs),
        )
        .await;
        if let Some(report_kind) = stream_error_finalize_kind {
            let payload = GatewaySyncReportRequest {
                trace_id: trace_id.to_string(),
                report_kind,
                report_context,
                status_code,
                headers: headers.clone(),
                body_json,
                client_body_json: None,
                body_base64,
                telemetry: None,
            };
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
                Some(request_id),
                candidate_id,
            )?));
        }
        return Ok(Some(attach_control_metadata_headers(
            build_executor_error_response(
                trace_id,
                decision,
                plan_kind,
                status_code,
                headers,
                error_body,
            )?,
            Some(request_id),
            candidate_id,
        )?));
    }

    let direct_stream_finalize_kind = resolve_core_stream_direct_finalize_report_kind(plan_kind);
    let mut local_stream_rewriter = maybe_build_local_stream_rewriter(report_context.as_ref());
    if local_stream_rewriter.is_some() {
        headers.remove("content-encoding");
        headers.remove("content-length");
        headers.insert("content-type".to_string(), "text/event-stream".to_string());
    }
    let mut prefetched_chunks: Vec<Bytes> = Vec::new();
    let mut prefetched_body = Vec::new();
    let mut prefetched_inspection_body = Vec::new();
    let mut prefetched_telemetry: Option<ExecutionTelemetry> = None;
    let mut reached_eof = false;
    if let Some(ref report_kind) = direct_stream_finalize_kind {
        while prefetched_chunks.len() < MAX_STREAM_PREFETCH_FRAMES
            && prefetched_inspection_body.len() < MAX_STREAM_PREFETCH_BYTES
        {
            let Some(frame) = (match read_next_frame(&mut lines).await {
                Ok(frame) => frame,
                Err(err) => {
                    let failure = build_stream_failure_report(
                        "executor_stream_frame_decode_error",
                        format!("failed to decode executor stream frame: {err:?}"),
                        502,
                    );
                    return handle_prefetch_stream_failure(
                        state,
                        control_base_url,
                        trace_id,
                        decision,
                        &plan,
                        report_context.clone(),
                        request_id,
                        candidate_id,
                        report_kind,
                        &headers,
                        prefetched_telemetry.clone(),
                        &prefetched_body,
                        failure,
                    )
                    .await;
                }
            }) else {
                reached_eof = true;
                break;
            };
            match frame.payload {
                StreamFramePayload::Data { chunk_b64, text } => {
                    let chunk = if let Some(chunk_b64) = chunk_b64 {
                        match base64::engine::general_purpose::STANDARD.decode(chunk_b64) {
                            Ok(decoded) => decoded,
                            Err(err) => {
                                let failure = build_stream_failure_report(
                                    "executor_stream_chunk_decode_error",
                                    format!("failed to decode executor stream chunk: {err}"),
                                    502,
                                );
                                return handle_prefetch_stream_failure(
                                    state,
                                    control_base_url,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context.clone(),
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    &headers,
                                    prefetched_telemetry.clone(),
                                    &prefetched_body,
                                    failure,
                                )
                                .await;
                            }
                        }
                    } else if let Some(text) = text {
                        text.into_bytes()
                    } else {
                        Vec::new()
                    };

                    if chunk.is_empty() {
                        continue;
                    }

                    prefetched_inspection_body.extend_from_slice(&chunk);

                    let inspection =
                        inspect_prefetched_stream_body(&headers, &prefetched_inspection_body);
                    match inspection {
                        StreamPrefetchInspection::EmbeddedError(body_json) => {
                            let payload = GatewaySyncReportRequest {
                                trace_id: trace_id.to_string(),
                                report_kind: report_kind.clone(),
                                report_context: report_context.clone(),
                                status_code,
                                headers: headers.clone(),
                                body_json: Some(body_json),
                                client_body_json: None,
                                body_base64: None,
                                telemetry: prefetched_telemetry.clone(),
                            };
                            state
                                .usage_runtime
                                .record_sync_terminal(
                                    state.data.as_ref(),
                                    &plan,
                                    report_context.as_ref(),
                                    &payload,
                                )
                                .await;
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
                                Some(request_id),
                                candidate_id,
                            )?));
                        }
                        StreamPrefetchInspection::NeedMore => {}
                        StreamPrefetchInspection::NonError => {}
                    }

                    let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut() {
                        match rewriter.push_chunk(&chunk) {
                            Ok(rewritten_chunk) => rewritten_chunk,
                            Err(err) => {
                                let failure = build_stream_failure_report(
                                    "executor_stream_rewrite_error",
                                    format!("failed to rewrite executor stream chunk: {err:?}"),
                                    502,
                                );
                                return handle_prefetch_stream_failure(
                                    state,
                                    control_base_url,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context.clone(),
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    &headers,
                                    prefetched_telemetry.clone(),
                                    &prefetched_body,
                                    failure,
                                )
                                .await;
                            }
                        }
                    } else {
                        chunk
                    };
                    if !rewritten_chunk.is_empty() {
                        prefetched_body.extend_from_slice(&rewritten_chunk);
                        prefetched_chunks.push(Bytes::from(rewritten_chunk));
                    }

                    if matches!(inspection, StreamPrefetchInspection::NonError) {
                        break;
                    }
                }
                StreamFramePayload::Telemetry {
                    telemetry: frame_telemetry,
                } => {
                    prefetched_telemetry = Some(frame_telemetry);
                }
                StreamFramePayload::Eof { .. } => {
                    reached_eof = true;
                    break;
                }
                StreamFramePayload::Error { error } => {
                    warn!(trace_id = %trace_id, error = %error.message, "executor stream emitted error frame during prefetch");
                    return handle_prefetch_stream_failure(
                        state,
                        control_base_url,
                        trace_id,
                        decision,
                        &plan,
                        report_context.clone(),
                        request_id,
                        candidate_id,
                        report_kind,
                        &headers,
                        prefetched_telemetry.clone(),
                        &prefetched_body,
                        build_stream_failure_from_execution_error(&error),
                    )
                    .await;
                }
                StreamFramePayload::Headers { .. } => {}
            }
        }
    }

    let candidate_started_unix_secs = current_request_candidate_unix_secs();
    state
        .usage_runtime
        .record_pending(state.data.as_ref(), &plan, report_context.as_ref())
        .await;
    state
        .usage_runtime
        .record_stream_started(
            state.data.as_ref(),
            &plan,
            report_context.as_ref(),
            status_code,
            &headers,
            prefetched_telemetry.as_ref(),
        )
        .await;
    record_local_request_candidate_status(
        state,
        &plan,
        report_context.as_ref(),
        aether_data::repository::candidates::RequestCandidateStatus::Streaming,
        Some(status_code),
        None,
        None,
        prefetched_telemetry
            .as_ref()
            .and_then(|telemetry| telemetry.elapsed_ms),
        Some(candidate_started_unix_secs),
        None,
    )
    .await;

    let (tx, mut rx) = mpsc::channel::<Result<Bytes, IoError>>(16);
    let state_for_report = state.clone();
    let plan_for_report = plan.clone();
    let trace_id_owned = trace_id.to_string();
    let control_base_url_owned = control_base_url.to_string();
    let headers_for_report = headers.clone();
    let report_kind_owned = report_kind.clone();
    let report_context_owned = report_context.clone();
    let prefetched_body_for_report = prefetched_body.clone();
    let prefetched_chunks_for_body = prefetched_chunks.clone();
    let initial_telemetry = prefetched_telemetry.clone();
    let initial_reached_eof = reached_eof;
    let direct_stream_finalize_kind_owned = direct_stream_finalize_kind.clone();
    let candidate_started_unix_secs_for_report = candidate_started_unix_secs;
    tokio::spawn(async move {
        let mut buffered_body = prefetched_body_for_report;
        let mut telemetry: Option<ExecutionTelemetry> = initial_telemetry;
        let reached_eof = initial_reached_eof;
        let mut downstream_dropped = false;
        let mut terminal_failure: Option<StreamFailureReport> = None;

        if !reached_eof {
            loop {
                let next_frame = match read_next_frame(&mut lines).await {
                    Ok(frame) => frame,
                    Err(err) => {
                        warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to decode executor stream frame");
                        terminal_failure = Some(build_stream_failure_report(
                            "executor_stream_frame_decode_error",
                            format!("failed to decode executor stream frame: {err:?}"),
                            502,
                        ));
                        break;
                    }
                };
                let Some(frame) = next_frame else {
                    break;
                };
                match frame.payload {
                    StreamFramePayload::Data { chunk_b64, text } => {
                        let chunk = if let Some(chunk_b64) = chunk_b64 {
                            match base64::engine::general_purpose::STANDARD.decode(chunk_b64) {
                                Ok(decoded) => decoded,
                                Err(err) => {
                                    warn!(trace_id = %trace_id_owned, error = %err, "gateway failed to decode executor chunk");
                                    terminal_failure = Some(build_stream_failure_report(
                                        "executor_stream_chunk_decode_error",
                                        format!("failed to decode executor stream chunk: {err}"),
                                        502,
                                    ));
                                    break;
                                }
                            }
                        } else if let Some(text) = text {
                            text.into_bytes()
                        } else {
                            Vec::new()
                        };

                        if chunk.is_empty() {
                            continue;
                        }

                        let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut()
                        {
                            match rewriter.push_chunk(&chunk) {
                                Ok(rewritten_chunk) => rewritten_chunk,
                                Err(err) => {
                                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to rewrite executor stream chunk");
                                    terminal_failure = Some(build_stream_failure_report(
                                        "executor_stream_rewrite_error",
                                        format!("failed to rewrite executor stream chunk: {err:?}"),
                                        502,
                                    ));
                                    break;
                                }
                            }
                        } else {
                            chunk
                        };

                        if rewritten_chunk.is_empty() {
                            continue;
                        }

                        buffered_body.extend_from_slice(&rewritten_chunk);
                        if tx.send(Ok(Bytes::from(rewritten_chunk))).await.is_err() {
                            warn!(
                                trace_id = %trace_id_owned,
                                "gateway stream downstream dropped; stopping executor stream forwarding"
                            );
                            downstream_dropped = true;
                            break;
                        }
                    }
                    StreamFramePayload::Telemetry {
                        telemetry: frame_telemetry,
                    } => {
                        telemetry = Some(frame_telemetry);
                    }
                    StreamFramePayload::Eof { .. } => {
                        break;
                    }
                    StreamFramePayload::Error { error } => {
                        warn!(trace_id = %trace_id_owned, error = %error.message, "executor stream emitted error frame");
                        terminal_failure = Some(build_stream_failure_from_execution_error(&error));
                        break;
                    }
                    StreamFramePayload::Headers { .. } => {}
                }
            }
        }

        if downstream_dropped {
            debug!(
                trace_id = %trace_id_owned,
                "gateway skipped local stream flush after downstream disconnect"
            );
        } else if let Some(rewriter) = local_stream_rewriter.as_mut() {
            match rewriter.finish() {
                Ok(flushed_chunk) if !flushed_chunk.is_empty() => {
                    buffered_body.extend_from_slice(&flushed_chunk);
                    if tx.send(Ok(Bytes::from(flushed_chunk))).await.is_err() {
                        warn!(
                            trace_id = %trace_id_owned,
                            "gateway stream downstream dropped while flushing local stream rewrite"
                        );
                        downstream_dropped = true;
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to flush local stream rewrite");
                    terminal_failure.get_or_insert_with(|| {
                        build_stream_failure_report(
                            "executor_stream_rewrite_flush_error",
                            format!("failed to flush local stream rewrite: {err:?}"),
                            502,
                        )
                    });
                }
            }
        }

        drop(tx);

        if downstream_dropped {
            debug!(
                trace_id = %trace_id_owned,
                "gateway skipped stream report because downstream disconnected before completion"
            );
            state_for_report
                .usage_runtime
                .record_stream_terminal(
                    state_for_report.data.as_ref(),
                    &plan_for_report,
                    report_context_owned.as_ref(),
                    &GatewayStreamReportRequest {
                        trace_id: trace_id_owned.clone(),
                        report_kind: report_kind_owned.clone().unwrap_or_default(),
                        report_context: report_context_owned.clone(),
                        status_code: 499,
                        headers: headers_for_report.clone(),
                        body_base64: (!buffered_body.is_empty()).then(|| {
                            base64::engine::general_purpose::STANDARD.encode(&buffered_body)
                        }),
                        telemetry: telemetry.clone(),
                    },
                    true,
                )
                .await;
            record_local_request_candidate_status(
                &state_for_report,
                &plan_for_report,
                report_context_owned.as_ref(),
                aether_data::repository::candidates::RequestCandidateStatus::Cancelled,
                Some(499),
                Some("downstream_disconnect".to_string()),
                Some("client disconnected before stream completion".to_string()),
                telemetry.as_ref().and_then(|value| value.elapsed_ms),
                Some(candidate_started_unix_secs_for_report),
                Some(current_request_candidate_unix_secs()),
            )
            .await;
            return;
        }

        if let Some(failure) = terminal_failure {
            submit_midstream_stream_failure(
                &state_for_report,
                &control_base_url_owned,
                &trace_id_owned,
                &plan_for_report,
                direct_stream_finalize_kind_owned.as_deref(),
                report_context_owned.as_ref(),
                &headers_for_report,
                telemetry.clone(),
                &buffered_body,
                candidate_started_unix_secs_for_report,
                failure,
            )
            .await;
            return;
        }

        let usage_payload = GatewayStreamReportRequest {
            trace_id: trace_id_owned.clone(),
            report_kind: report_kind_owned.clone().unwrap_or_default(),
            report_context: report_context_owned.clone(),
            status_code,
            headers: headers_for_report.clone(),
            body_base64: (!buffered_body.is_empty())
                .then(|| base64::engine::general_purpose::STANDARD.encode(&buffered_body)),
            telemetry: telemetry.clone(),
        };
        state_for_report
            .usage_runtime
            .record_stream_terminal(
                state_for_report.data.as_ref(),
                &plan_for_report,
                report_context_owned.as_ref(),
                &usage_payload,
                false,
            )
            .await;
        record_local_request_candidate_status(
            &state_for_report,
            &plan_for_report,
            report_context_owned.as_ref(),
            aether_data::repository::candidates::RequestCandidateStatus::Success,
            Some(status_code),
            None,
            None,
            telemetry.as_ref().and_then(|value| value.elapsed_ms),
            Some(candidate_started_unix_secs_for_report),
            Some(current_request_candidate_unix_secs()),
        )
        .await;

        if let Some(report_kind) = report_kind_owned {
            let mut report = usage_payload;
            report.report_kind = report_kind;
            if let Err(err) = submit_stream_report(
                &state_for_report,
                &control_base_url_owned,
                &trace_id_owned,
                report,
            )
            .await
            {
                warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to submit stream execution report");
            }
        }
    });

    let body_stream = stream! {
        for chunk in prefetched_chunks_for_body {
            yield Ok(chunk);
        }
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };

    headers.insert(
        CONTROL_REQUEST_ID_HEADER.to_string(),
        request_id.to_string(),
    );

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
        status_code,
        &headers,
        Body::from_stream(body_stream),
        trace_id,
        Some(decision),
    )?))
}

#[derive(Debug, Clone)]
struct StreamFailureReport {
    status_code: u16,
    error_type: String,
    error_message: String,
    body_json: Value,
}

fn build_stream_failure_report(
    error_type: impl Into<String>,
    error_message: impl Into<String>,
    status_code: u16,
) -> StreamFailureReport {
    let error_type = error_type.into();
    let error_message = error_message.into();
    StreamFailureReport {
        status_code,
        body_json: Value::Object(Map::from_iter([(
            "error".to_string(),
            Value::Object(Map::from_iter([
                ("type".to_string(), Value::String(error_type.clone())),
                ("message".to_string(), Value::String(error_message.clone())),
                ("code".to_string(), Value::from(status_code)),
            ])),
        )])),
        error_type,
        error_message,
    }
}

fn build_stream_failure_from_execution_error(error: &ExecutionError) -> StreamFailureReport {
    let status_code = error.upstream_status.unwrap_or(502);
    let error_type = serde_json::to_value(&error.kind)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "internal".to_string());
    let phase = serde_json::to_value(&error.phase).unwrap_or(Value::Null);
    let mut error_object = Map::from_iter([
        ("type".to_string(), Value::String(error_type.clone())),
        ("message".to_string(), Value::String(error.message.clone())),
        ("code".to_string(), Value::from(status_code)),
        ("phase".to_string(), phase),
        ("retryable".to_string(), Value::Bool(error.retryable)),
        (
            "failover_recommended".to_string(),
            Value::Bool(error.failover_recommended),
        ),
    ]);
    if let Some(upstream_status) = error.upstream_status {
        error_object.insert("upstream_status".to_string(), Value::from(upstream_status));
    }

    StreamFailureReport {
        status_code,
        error_type,
        error_message: error.message.trim().to_string(),
        body_json: Value::Object(Map::from_iter([(
            "error".to_string(),
            Value::Object(error_object),
        )])),
    }
}

fn build_stream_failure_sync_payload(
    trace_id: &str,
    report_kind: String,
    report_context: Option<Value>,
    headers: &std::collections::BTreeMap<String, String>,
    telemetry: Option<ExecutionTelemetry>,
    buffered_body: &[u8],
    failure: &StreamFailureReport,
) -> GatewaySyncReportRequest {
    let mut response_headers = headers.clone();
    response_headers.remove("content-encoding");
    response_headers.remove("content-length");
    response_headers.insert("content-type".to_string(), "application/json".to_string());

    GatewaySyncReportRequest {
        trace_id: trace_id.to_string(),
        report_kind,
        report_context,
        status_code: failure.status_code,
        headers: response_headers,
        body_json: Some(failure.body_json.clone()),
        client_body_json: None,
        body_base64: (!buffered_body.is_empty())
            .then(|| base64::engine::general_purpose::STANDARD.encode(buffered_body)),
        telemetry,
    }
}

async fn record_stream_sync_failure(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    payload: &GatewaySyncReportRequest,
    failure: &StreamFailureReport,
    started_at_unix_secs: Option<u64>,
) {
    state
        .usage_runtime
        .record_sync_terminal(state.data.as_ref(), plan, report_context, payload)
        .await;
    let terminal_unix_secs = current_request_candidate_unix_secs();
    record_report_request_candidate_status(
        state,
        report_context,
        aether_data::repository::candidates::RequestCandidateStatus::Failed,
        Some(failure.status_code),
        Some(failure.error_type.clone()),
        Some(failure.error_message.clone()),
        payload
            .telemetry
            .as_ref()
            .and_then(|telemetry| telemetry.elapsed_ms),
        started_at_unix_secs.or(Some(terminal_unix_secs)),
        Some(terminal_unix_secs),
    )
    .await;
}

#[allow(clippy::too_many_arguments)] // internal helper for prefetch error handling
async fn handle_prefetch_stream_failure(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan: &ExecutionPlan,
    report_context: Option<Value>,
    request_id: &str,
    candidate_id: Option<&str>,
    report_kind: &str,
    headers: &std::collections::BTreeMap<String, String>,
    telemetry: Option<ExecutionTelemetry>,
    buffered_body: &[u8],
    failure: StreamFailureReport,
) -> Result<Option<Response<Body>>, GatewayError> {
    let payload = build_stream_failure_sync_payload(
        trace_id,
        report_kind.to_string(),
        report_context.clone(),
        headers,
        telemetry,
        buffered_body,
        &failure,
    );
    record_stream_sync_failure(
        state,
        plan,
        report_context.as_ref(),
        &payload,
        &failure,
        None,
    )
    .await;

    let response = submit_local_core_error_or_sync_finalize(
        state,
        control_base_url,
        trace_id,
        decision,
        payload,
    )
    .await?;
    Ok(Some(attach_control_metadata_headers(
        response,
        Some(request_id),
        candidate_id,
    )?))
}

async fn submit_midstream_stream_failure(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    plan: &ExecutionPlan,
    direct_stream_finalize_kind: Option<&str>,
    report_context: Option<&Value>,
    headers: &std::collections::BTreeMap<String, String>,
    telemetry: Option<ExecutionTelemetry>,
    buffered_body: &[u8],
    started_at_unix_secs: u64,
    failure: StreamFailureReport,
) {
    let Some(report_kind) =
        direct_stream_finalize_kind.and_then(resolve_core_error_background_report_kind)
    else {
        return;
    };

    let payload = build_stream_failure_sync_payload(
        trace_id,
        report_kind,
        report_context.cloned(),
        headers,
        telemetry,
        buffered_body,
        &failure,
    );
    record_stream_sync_failure(
        state,
        plan,
        report_context,
        &payload,
        &failure,
        Some(started_at_unix_secs),
    )
    .await;
    if let Err(err) = submit_sync_report(state, control_base_url, trace_id, payload).await {
        warn!(
            trace_id = %trace_id,
            error = ?err,
            "gateway failed to submit sync execution report for terminal stream failure"
        );
    }
}
