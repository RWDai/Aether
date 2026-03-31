use super::*;
use crate::gateway::usage::spawn_sync_report;
use base64::Engine as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalSyncErrorKind {
    InvalidRequest,
    Authentication,
    PermissionDenied,
    NotFound,
    RateLimit,
    ContextLengthExceeded,
    Overloaded,
    ServerError,
}

#[derive(Clone, Debug)]
struct LocalSyncErrorDetails {
    message: String,
    code: Option<String>,
    kind: LocalSyncErrorKind,
}

pub(super) fn maybe_build_local_core_error_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !is_core_error_finalize_kind(payload.report_kind.as_str()) {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let mut body_json = body_json.clone();
    if let Some(report_context) = payload.report_context.as_ref() {
        if let Some(unwrapped) =
            crate::gateway::local_finalize::unwrap_local_finalize_response_value(
                body_json.clone(),
                report_context,
            )?
        {
            body_json = unwrapped;
        }
    }

    let Some(body_object) = body_json.as_object() else {
        return Ok(None);
    };
    if !body_object.contains_key("error")
        && !body_object
            .get("type")
            .and_then(|value| value.as_str())
            .is_some_and(|value| value == "error")
    {
        return Ok(None);
    }

    let Some(response_body_json) = build_best_effort_local_core_error_body(payload, &body_json)?
    else {
        return Ok(None);
    };

    let mut response_headers = payload.headers.clone();
    response_headers.remove("content-encoding");
    response_headers.remove("content-length");
    response_headers.insert("content-type".to_string(), "application/json".to_string());

    let body_bytes = serde_json::to_vec(&response_body_json)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    response_headers.insert("content-length".to_string(), body_bytes.len().to_string());

    Ok(Some(build_client_response_from_parts(
        resolve_local_sync_error_status_code(payload.status_code, &body_json),
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn maybe_resolve_local_sync_response_body_json(
    payload: &GatewaySyncReportRequest,
) -> Result<Option<serde_json::Value>, GatewayError> {
    if let Some(client_body_json) = payload.client_body_json.clone() {
        return Ok(Some(client_body_json));
    }

    let Some(mut body_json) = payload.body_json.clone() else {
        return Ok(None);
    };

    if let Some(report_context) = payload.report_context.as_ref() {
        if let Some(unwrapped) =
            crate::gateway::local_finalize::unwrap_local_finalize_response_value(
                body_json.clone(),
                report_context,
            )?
        {
            body_json = unwrapped;
        }
    }

    if is_core_error_finalize_kind(payload.report_kind.as_str()) {
        if let Some(converted) = build_best_effort_local_core_error_body(payload, &body_json)? {
            return Ok(Some(converted));
        }
    }

    Ok(Some(body_json))
}

fn build_local_sync_response_from_json(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    body_json: serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let status_code = if is_core_error_finalize_kind(payload.report_kind.as_str())
        || has_nested_error(&body_json)
    {
        resolve_local_sync_error_status_code(payload.status_code, &body_json)
    } else {
        payload.status_code
    };

    let mut response_headers = payload.headers.clone();
    response_headers.remove("content-encoding");
    response_headers.remove("content-length");
    response_headers.insert("content-type".to_string(), "application/json".to_string());

    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    response_headers.insert("content-length".to_string(), body_bytes.len().to_string());

    build_client_response_from_parts(
        status_code,
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

fn build_local_sync_response_from_bytes(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    body_bytes: Vec<u8>,
) -> Result<Response<Body>, GatewayError> {
    let mut response_headers = payload.headers.clone();
    response_headers.remove("content-length");
    if body_bytes.is_empty() {
        response_headers.remove("content-encoding");
    }
    response_headers.insert("content-length".to_string(), body_bytes.len().to_string());

    build_client_response_from_parts(
        payload.status_code,
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

fn build_local_core_sync_finalize_fallback_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Response<Body>, GatewayError> {
    if let Some(body_json) = maybe_resolve_local_sync_response_body_json(payload)? {
        return build_local_sync_response_from_json(trace_id, decision, payload, body_json);
    }

    if let Some(body_base64) = payload.body_base64.as_ref() {
        let body_bytes = base64::engine::general_purpose::STANDARD
            .decode(body_base64)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return build_local_sync_response_from_bytes(trace_id, decision, payload, body_bytes);
    }

    build_local_sync_response_from_bytes(trace_id, decision, payload, Vec::new())
}

pub(crate) fn build_best_effort_local_core_error_body(
    payload: &GatewaySyncReportRequest,
    body_json: &serde_json::Value,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let default_api_format = infer_default_api_format(payload.report_kind.as_str())
        .unwrap_or_default()
        .to_string();
    let client_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("client_api_format"))
        .and_then(|value| value.as_str())
        .unwrap_or(default_api_format.as_str())
        .trim()
        .to_ascii_lowercase();
    let provider_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("provider_api_format"))
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| client_api_format.clone());

    if client_api_format.is_empty() {
        return Ok(None);
    }
    if client_api_format == provider_api_format {
        return Ok(Some(body_json.clone()));
    }

    let details = extract_local_sync_error_details(payload.status_code, body_json);
    let mut error_object = serde_json::Map::new();
    error_object.insert(
        "message".to_string(),
        serde_json::Value::String(details.message.clone()),
    );

    let response_body = match client_api_format.as_str() {
        "openai:chat" | "openai:cli" | "openai:compact" => {
            error_object.insert(
                "type".to_string(),
                serde_json::Value::String(map_local_sync_error_kind_to_openai_type(details.kind)),
            );
            if let Some(code) = details.code.clone().filter(|value| !value.is_empty()) {
                error_object.insert("code".to_string(), serde_json::Value::String(code));
            }
            serde_json::Value::Object(serde_json::Map::from_iter([(
                "error".to_string(),
                serde_json::Value::Object(error_object),
            )]))
        }
        "claude:chat" | "claude:cli" => {
            error_object.insert(
                "type".to_string(),
                serde_json::Value::String(map_local_sync_error_kind_to_claude_type(details.kind)),
            );
            if let Some(code) = details.code.clone().filter(|value| !value.is_empty()) {
                error_object.insert("code".to_string(), serde_json::Value::String(code));
            }
            serde_json::Value::Object(serde_json::Map::from_iter([
                (
                    "type".to_string(),
                    serde_json::Value::String("error".to_string()),
                ),
                ("error".to_string(), serde_json::Value::Object(error_object)),
            ]))
        }
        "gemini:chat" | "gemini:cli" => serde_json::Value::Object(serde_json::Map::from_iter([(
            "error".to_string(),
            serde_json::Value::Object(serde_json::Map::from_iter([
                (
                    "code".to_string(),
                    serde_json::Value::from(map_local_sync_error_kind_to_gemini_code(details.kind)),
                ),
                (
                    "message".to_string(),
                    serde_json::Value::String(details.message),
                ),
                (
                    "status".to_string(),
                    serde_json::Value::String(map_local_sync_error_kind_to_gemini_status(
                        details.kind,
                    )),
                ),
            ])),
        )])),
        _ => return Ok(None),
    };

    Ok(Some(response_body))
}

fn is_core_error_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_chat_sync_finalize"
            | "claude_chat_sync_finalize"
            | "gemini_chat_sync_finalize"
            | "openai_cli_sync_finalize"
            | "openai_compact_sync_finalize"
            | "claude_cli_sync_finalize"
            | "gemini_cli_sync_finalize"
    )
}

fn infer_default_api_format(report_kind: &str) -> Option<&'static str> {
    match report_kind {
        "openai_chat_sync_finalize" => Some("openai:chat"),
        "claude_chat_sync_finalize" => Some("claude:chat"),
        "gemini_chat_sync_finalize" => Some("gemini:chat"),
        "openai_cli_sync_finalize" => Some("openai:cli"),
        "openai_compact_sync_finalize" => Some("openai:compact"),
        "claude_cli_sync_finalize" => Some("claude:cli"),
        "gemini_cli_sync_finalize" => Some("gemini:cli"),
        _ => None,
    }
}

pub(crate) fn resolve_core_error_background_report_kind(report_kind: &str) -> Option<String> {
    let mapped = match report_kind {
        "openai_chat_sync_finalize" => "openai_chat_sync_error",
        "claude_chat_sync_finalize" => "claude_chat_sync_error",
        "gemini_chat_sync_finalize" => "gemini_chat_sync_error",
        "openai_cli_sync_finalize" => "openai_cli_sync_error",
        "openai_compact_sync_finalize" => "openai_compact_sync_error",
        "claude_cli_sync_finalize" => "claude_cli_sync_error",
        "gemini_cli_sync_finalize" => "gemini_cli_sync_error",
        _ => return None,
    };

    Some(mapped.to_string())
}

#[cfg(test)]
pub(crate) fn resolve_core_success_background_report_kind(report_kind: &str) -> Option<String> {
    let mapped = match report_kind {
        "openai_chat_sync_finalize" => "openai_chat_sync_success",
        "claude_chat_sync_finalize" => "claude_chat_sync_success",
        "gemini_chat_sync_finalize" => "gemini_chat_sync_success",
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize" => "openai_cli_sync_success",
        "claude_cli_sync_finalize" => "claude_cli_sync_success",
        "gemini_cli_sync_finalize" => "gemini_cli_sync_success",
        _ => return None,
    };

    Some(mapped.to_string())
}

fn resolve_local_sync_error_status_code(status_code: u16, body_json: &serde_json::Value) -> u16 {
    if (400..600).contains(&status_code) {
        return status_code;
    }

    let Some(error_object) = body_json.get("error").and_then(|value| value.as_object()) else {
        return 400;
    };

    for key in ["code", "status"] {
        let Some(value) = error_object.get(key) else {
            continue;
        };
        if let Some(number) = value.as_u64() {
            if (400..600).contains(&number) {
                return number as u16;
            }
        }
        if let Some(text) = value.as_str() {
            if let Ok(number) = text.parse::<u16>() {
                if (400..600).contains(&number) {
                    return number;
                }
            }
        }
    }

    400
}

fn extract_local_sync_error_details(
    status_code: u16,
    body_json: &serde_json::Value,
) -> LocalSyncErrorDetails {
    let resolved_status_code = resolve_local_sync_error_status_code(status_code, body_json);
    let body_object = body_json.as_object();
    let error_object = body_object
        .and_then(|object| object.get("error"))
        .and_then(|value| value.as_object());

    let message = first_non_empty_error_text(
        error_object,
        body_object,
        &["message", "detail", "reason", "status", "type", "__type"],
    )
    .unwrap_or_else(|| format!("HTTP {resolved_status_code}"));
    let code = first_non_empty_error_text(error_object, body_object, &["code", "status"]);
    let raw_type = first_non_empty_error_text(error_object, body_object, &["type", "__type"]);
    let raw_status = first_non_empty_error_text(error_object, body_object, &["status"]);
    let kind = classify_local_sync_error_kind(
        resolved_status_code,
        raw_type.as_deref(),
        raw_status.as_deref(),
        code.as_deref(),
        message.as_str(),
    );

    LocalSyncErrorDetails {
        message,
        code,
        kind,
    }
}

fn first_non_empty_error_text(
    error_object: Option<&serde_json::Map<String, serde_json::Value>>,
    body_object: Option<&serde_json::Map<String, serde_json::Value>>,
    keys: &[&str],
) -> Option<String> {
    for object in [error_object, body_object].into_iter().flatten() {
        for key in keys {
            let Some(value) = object.get(*key) else {
                continue;
            };
            match value {
                serde_json::Value::String(text) if !text.trim().is_empty() => {
                    return Some(text.trim().to_string());
                }
                serde_json::Value::Number(number) => return Some(number.to_string()),
                _ => {}
            }
        }
    }
    None
}

fn classify_local_sync_error_kind(
    status_code: u16,
    raw_type: Option<&str>,
    raw_status: Option<&str>,
    raw_code: Option<&str>,
    message: &str,
) -> LocalSyncErrorKind {
    let mut fingerprint = String::new();
    for segment in [raw_type, raw_status, raw_code, Some(message)] {
        if let Some(segment) = segment.map(str::trim).filter(|value| !value.is_empty()) {
            if !fingerprint.is_empty() {
                fingerprint.push(' ');
            }
            fingerprint.push_str(&segment.to_ascii_lowercase());
        }
    }

    if status_code == 429
        || fingerprint.contains("rate_limit")
        || fingerprint.contains("rate limited")
        || fingerprint.contains("resource_exhausted")
        || fingerprint.contains("throttl")
    {
        return LocalSyncErrorKind::RateLimit;
    }
    if fingerprint.contains("contextlength")
        || fingerprint.contains("contentlengthexceeded")
        || fingerprint.contains("context window")
        || fingerprint.contains("context length")
        || fingerprint.contains("max_tokens")
        || (fingerprint.contains("context") && fingerprint.contains("token"))
    {
        return LocalSyncErrorKind::ContextLengthExceeded;
    }
    if status_code == 401
        || fingerprint.contains("unauth")
        || fingerprint.contains("authentication")
    {
        return LocalSyncErrorKind::Authentication;
    }
    if status_code == 403 || fingerprint.contains("permission") || fingerprint.contains("forbidden")
    {
        return LocalSyncErrorKind::PermissionDenied;
    }
    if status_code == 404 || fingerprint.contains("not_found") || fingerprint.contains("not found")
    {
        return LocalSyncErrorKind::NotFound;
    }
    if status_code == 503 || fingerprint.contains("overload") || fingerprint.contains("unavailable")
    {
        return LocalSyncErrorKind::Overloaded;
    }
    if (500..600).contains(&status_code) {
        return LocalSyncErrorKind::ServerError;
    }
    LocalSyncErrorKind::InvalidRequest
}

fn map_local_sync_error_kind_to_openai_type(kind: LocalSyncErrorKind) -> String {
    match kind {
        LocalSyncErrorKind::InvalidRequest => "invalid_request_error",
        LocalSyncErrorKind::Authentication => "authentication_error",
        LocalSyncErrorKind::PermissionDenied => "permission_error",
        LocalSyncErrorKind::NotFound => "not_found_error",
        LocalSyncErrorKind::RateLimit => "rate_limit_error",
        LocalSyncErrorKind::ContextLengthExceeded => "context_length_exceeded",
        LocalSyncErrorKind::Overloaded | LocalSyncErrorKind::ServerError => "server_error",
    }
    .to_string()
}

fn map_local_sync_error_kind_to_claude_type(kind: LocalSyncErrorKind) -> String {
    match kind {
        LocalSyncErrorKind::InvalidRequest | LocalSyncErrorKind::ContextLengthExceeded => {
            "invalid_request_error"
        }
        LocalSyncErrorKind::Authentication => "authentication_error",
        LocalSyncErrorKind::PermissionDenied => "permission_error",
        LocalSyncErrorKind::NotFound => "not_found_error",
        LocalSyncErrorKind::RateLimit => "rate_limit_error",
        LocalSyncErrorKind::Overloaded | LocalSyncErrorKind::ServerError => "api_error",
    }
    .to_string()
}

fn map_local_sync_error_kind_to_gemini_code(kind: LocalSyncErrorKind) -> u16 {
    match kind {
        LocalSyncErrorKind::InvalidRequest | LocalSyncErrorKind::ContextLengthExceeded => 400,
        LocalSyncErrorKind::Authentication => 401,
        LocalSyncErrorKind::PermissionDenied => 403,
        LocalSyncErrorKind::NotFound => 404,
        LocalSyncErrorKind::RateLimit => 429,
        LocalSyncErrorKind::Overloaded => 503,
        LocalSyncErrorKind::ServerError => 500,
    }
}

fn map_local_sync_error_kind_to_gemini_status(kind: LocalSyncErrorKind) -> String {
    match kind {
        LocalSyncErrorKind::InvalidRequest | LocalSyncErrorKind::ContextLengthExceeded => {
            "INVALID_ARGUMENT"
        }
        LocalSyncErrorKind::Authentication => "UNAUTHENTICATED",
        LocalSyncErrorKind::PermissionDenied => "PERMISSION_DENIED",
        LocalSyncErrorKind::NotFound => "NOT_FOUND",
        LocalSyncErrorKind::RateLimit => "RESOURCE_EXHAUSTED",
        LocalSyncErrorKind::Overloaded => "UNAVAILABLE",
        LocalSyncErrorKind::ServerError => "INTERNAL",
    }
    .to_string()
}

pub(super) fn strip_utf8_bom_and_ws(mut body: &[u8]) -> &[u8] {
    loop {
        while let Some(first) = body.first() {
            if first.is_ascii_whitespace() {
                body = &body[1..];
            } else {
                break;
            }
        }
        if body.starts_with(&[0xEF, 0xBB, 0xBF]) {
            body = &body[3..];
        } else {
            break;
        }
    }
    body
}

pub(super) fn has_nested_error(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    if object.contains_key("error") {
        return true;
    }
    if object
        .get("type")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value == "error")
    {
        return true;
    }

    object
        .get("chunks")
        .and_then(|value| value.as_array())
        .is_some_and(|chunks| {
            chunks.iter().any(|chunk| {
                chunk.as_object().is_some_and(|chunk_object| {
                    chunk_object.contains_key("error")
                        || chunk_object
                            .get("type")
                            .and_then(|value| value.as_str())
                            .is_some_and(|value| value == "error")
                })
            })
        })
}

pub(super) async fn submit_local_core_error_or_sync_finalize(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: GatewaySyncReportRequest,
) -> Result<Response<Body>, GatewayError> {
    let response = if let Some(response) =
        maybe_build_local_core_error_response(trace_id, decision, &payload)?
    {
        response
    } else {
        build_local_core_sync_finalize_fallback_response(trace_id, decision, &payload)?
    };

    if let Some(error_report_kind) =
        resolve_core_error_background_report_kind(payload.report_kind.as_str())
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
            "gateway built local core finalize response without background error report mapping"
        );
    }

    Ok(response)
}
