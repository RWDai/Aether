use base64::Engine as _;

#[path = "cli/claude.rs"]
mod claude;
#[path = "cli/gemini.rs"]
mod gemini;
#[path = "cli/openai.rs"]
mod openai;

use super::chat::aggregate_gemini_stream_sync_response;
use super::common::{
    build_local_success_outcome, build_local_success_outcome_with_conversion_report,
    local_finalize_allows_envelope, unwrap_local_finalize_response_value,
    LocalCoreSyncFinalizeOutcome,
};
use super::*;

pub(crate) use claude::convert_claude_cli_response_to_openai_cli;
pub(crate) use gemini::convert_gemini_cli_response_to_openai_cli;
#[cfg(test)]
pub(crate) use openai::aggregate_openai_cli_stream_sync_response;

pub(super) fn maybe_build_local_openai_cli_cross_format_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) =
        maybe_build_local_openai_cli_antigravity_cross_format_stream_sync_response(
            trace_id, decision, payload,
        )?
    {
        return Ok(Some(response));
    }
    openai::maybe_build_local_openai_cli_cross_format_stream_sync_response(
        trace_id, decision, payload,
    )
}

pub(super) fn maybe_build_local_openai_cli_cross_format_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) = maybe_build_local_openai_cli_antigravity_cross_format_sync_response(
        trace_id, decision, payload,
    )? {
        return Ok(Some(response));
    }
    openai::maybe_build_local_openai_cli_cross_format_sync_response(trace_id, decision, payload)
}

pub(super) fn maybe_build_local_openai_cli_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) =
        openai::maybe_build_local_openai_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) = maybe_build_local_openai_cli_openai_family_stream_sync_response(
        trace_id, decision, payload,
    )? {
        return Ok(Some(response));
    }
    maybe_build_local_openai_cli_sync_response(trace_id, decision, payload)
}

pub(super) fn maybe_build_local_claude_cli_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) =
        claude::maybe_build_local_claude_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    maybe_build_local_provider_cli_sync_response(
        trace_id,
        decision,
        payload,
        "claude_cli_sync_finalize",
        "claude:cli",
    )
}

pub(super) fn maybe_build_local_gemini_cli_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) =
        gemini::maybe_build_local_gemini_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    maybe_build_local_provider_cli_sync_response(
        trace_id,
        decision,
        payload,
        "gemini_cli_sync_finalize",
        "gemini:cli",
    )
}

fn maybe_build_local_openai_cli_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if !local_finalize_allows_envelope(report_context)
        || !is_openai_cli_family_api_format(provider_api_format.as_str())
        || !is_openai_cli_family_api_format(client_api_format.as_str())
    {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let Some(body_json) = unwrap_local_finalize_response_value(body_json.clone(), report_context)?
    else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome(
        trace_id, decision, payload, body_json,
    )?))
}

fn maybe_build_local_openai_cli_openai_family_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if !local_finalize_allows_envelope(report_context)
        || !is_openai_cli_family_api_format(provider_api_format.as_str())
        || !is_openai_cli_family_api_format(client_api_format.as_str())
        || provider_api_format == client_api_format
    {
        return Ok(None);
    }

    let Some(body_base64) = payload.body_base64.as_deref() else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let Some(body_json) = openai::aggregate_openai_cli_stream_sync_response(&body_bytes) else {
        return Ok(None);
    };
    let Some(body_json) = unwrap_local_finalize_response_value(body_json, report_context)? else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome(
        trace_id, decision, payload, body_json,
    )?))
}

fn maybe_build_local_provider_cli_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    expected_report_kind: &str,
    expected_api_format: &str,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if payload.report_kind != expected_report_kind || payload.status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if !local_finalize_allows_envelope(report_context)
        || provider_api_format != expected_api_format
        || client_api_format != expected_api_format
        || needs_conversion
    {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let Some(body_json) = unwrap_local_finalize_response_value(body_json.clone(), report_context)?
    else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome(
        trace_id, decision, payload, body_json,
    )?))
}

fn maybe_build_local_openai_cli_antigravity_cross_format_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if provider_api_format != "gemini:cli"
        || !is_openai_cli_family_api_format(client_api_format.as_str())
        || !is_antigravity_v1internal_envelope(report_context)
        || !local_finalize_allows_envelope(report_context)
    {
        return Ok(None);
    }

    let Some(body_base64) = payload.body_base64.as_deref() else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let Some(aggregated) = aggregate_gemini_stream_sync_response(&body_bytes) else {
        return Ok(None);
    };
    let Some(provider_body_json) =
        unwrap_cli_conversion_response_value(aggregated, report_context)?
    else {
        return Ok(None);
    };
    let Some(converted) =
        convert_gemini_cli_response_to_openai_cli(&provider_body_json, report_context)
    else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome_with_conversion_report(
        trace_id,
        decision,
        payload,
        converted,
        provider_body_json,
    )?))
}

fn maybe_build_local_openai_cli_antigravity_cross_format_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if provider_api_format != "gemini:cli"
        || !is_openai_cli_family_api_format(client_api_format.as_str())
        || !is_antigravity_v1internal_envelope(report_context)
        || !local_finalize_allows_envelope(report_context)
    {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let Some(provider_body_json) =
        unwrap_cli_conversion_response_value(body_json.clone(), report_context)?
    else {
        return Ok(None);
    };
    let Some(converted) =
        convert_gemini_cli_response_to_openai_cli(&provider_body_json, report_context)
    else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome_with_conversion_report(
        trace_id,
        decision,
        payload,
        converted,
        provider_body_json,
    )?))
}

fn unwrap_cli_conversion_response_value(
    data: Value,
    report_context: &Value,
) -> Result<Option<Value>, GatewayError> {
    if !is_antigravity_v1internal_envelope(report_context) {
        return unwrap_local_finalize_response_value(data, report_context);
    }

    let mut unwrapped = if let Some(response) = data
        .get("response")
        .and_then(Value::as_object)
        .filter(|response| !response.contains_key("response"))
    {
        let mut response = response.clone();
        if let Some(response_id) = data.get("responseId").cloned() {
            response
                .entry("responseId".to_string())
                .or_insert(response_id);
        }
        Value::Object(response)
    } else {
        data
    };

    if let Some(object) = unwrapped.as_object_mut() {
        if !object.contains_key("responseId") {
            if let Some(response_id) = object.get("_v1internal_response_id").cloned() {
                object.insert("responseId".to_string(), response_id);
            }
        }
    }

    Ok(Some(unwrapped))
}

fn is_antigravity_v1internal_envelope(report_context: &Value) -> bool {
    report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && report_context
            .get("envelope_name")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("antigravity:v1internal"))
}

fn is_openai_cli_family_api_format(api_format: &str) -> bool {
    matches!(api_format, "openai:cli" | "openai:compact")
}
