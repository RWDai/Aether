use aether_contracts::ExecutionResult;

fn is_local_candidate_attempt(report_context: Option<&serde_json::Value>) -> bool {
    report_context
        .and_then(serde_json::Value::as_object)
        .and_then(|context| context.get("candidate_index"))
        .and_then(serde_json::Value::as_u64)
        .is_some()
}

fn is_retryable_local_upstream_status(status_code: u16) -> bool {
    status_code == 429 || status_code >= 500
}

pub(crate) fn should_retry_next_local_candidate_sync(
    plan_kind: &str,
    report_context: Option<&serde_json::Value>,
    result: &ExecutionResult,
) -> bool {
    is_local_candidate_attempt(report_context)
        && plan_kind == "openai_chat_sync"
        && is_retryable_local_upstream_status(result.status_code)
}

pub(crate) fn should_fallback_to_control_sync(
    plan_kind: &str,
    result: &ExecutionResult,
    body_json: Option<&serde_json::Value>,
    has_body_bytes: bool,
    explicit_finalize: bool,
    mapped_error_finalize: bool,
) -> bool {
    if explicit_finalize
        && matches!(
            plan_kind,
            "openai_video_delete_sync" | "openai_video_cancel_sync" | "gemini_video_cancel_sync"
        )
    {
        return false;
    }

    if !matches!(
        plan_kind,
        "openai_video_create_sync"
            | "openai_video_remix_sync"
            | "gemini_video_create_sync"
            | "openai_chat_sync"
            | "openai_cli_sync"
            | "openai_compact_sync"
            | "claude_chat_sync"
            | "gemini_chat_sync"
            | "claude_cli_sync"
            | "gemini_cli_sync"
    ) {
        return false;
    }

    if explicit_finalize {
        return result.status_code < 400 && body_json.is_none() && !has_body_bytes;
    }

    if mapped_error_finalize {
        return false;
    }

    if result.status_code >= 400 {
        return true;
    }

    let Some(body_json) = body_json else {
        return true;
    };

    body_json.get("error").is_some()
}

pub(crate) fn should_finalize_sync_response(report_kind: Option<&str>) -> bool {
    report_kind.is_some_and(|kind| kind.ends_with("_finalize"))
}

pub(crate) fn resolve_core_sync_error_finalize_report_kind(
    plan_kind: &str,
    result: &ExecutionResult,
    body_json: Option<&serde_json::Value>,
) -> Option<String> {
    let has_embedded_error = body_json.is_some_and(|value| value.get("error").is_some());
    if result.status_code < 400 && !has_embedded_error {
        return None;
    }

    let report_kind = match plan_kind {
        "openai_chat_sync" => "openai_chat_sync_finalize",
        "openai_cli_sync" => "openai_cli_sync_finalize",
        "openai_compact_sync" => "openai_compact_sync_finalize",
        "claude_chat_sync" => "claude_chat_sync_finalize",
        "gemini_chat_sync" => "gemini_chat_sync_finalize",
        "claude_cli_sync" => "claude_cli_sync_finalize",
        "gemini_cli_sync" => "gemini_cli_sync_finalize",
        _ => return None,
    };

    Some(report_kind.to_string())
}

pub(crate) fn should_retry_next_local_candidate_stream(
    plan_kind: &str,
    report_context: Option<&serde_json::Value>,
    status_code: u16,
) -> bool {
    is_local_candidate_attempt(report_context)
        && plan_kind == "openai_chat_stream"
        && is_retryable_local_upstream_status(status_code)
}

pub(crate) fn should_fallback_to_control_stream(
    plan_kind: &str,
    status_code: u16,
    mapped_error_finalize: bool,
) -> bool {
    if mapped_error_finalize {
        return false;
    }

    matches!(
        plan_kind,
        "openai_chat_stream"
            | "claude_chat_stream"
            | "gemini_chat_stream"
            | "openai_cli_stream"
            | "openai_compact_stream"
            | "claude_cli_stream"
            | "gemini_cli_stream"
    ) && status_code >= 400
}

pub(crate) fn resolve_core_stream_error_finalize_report_kind(
    plan_kind: &str,
    status_code: u16,
) -> Option<String> {
    if status_code < 400 {
        return None;
    }

    let report_kind = match plan_kind {
        "openai_chat_stream" => "openai_chat_sync_finalize",
        "claude_chat_stream" => "claude_chat_sync_finalize",
        "gemini_chat_stream" => "gemini_chat_sync_finalize",
        "openai_cli_stream" => "openai_cli_sync_finalize",
        "openai_compact_stream" => "openai_compact_sync_finalize",
        "claude_cli_stream" => "claude_cli_sync_finalize",
        "gemini_cli_stream" => "gemini_cli_sync_finalize",
        _ => return None,
    };

    Some(report_kind.to_string())
}

pub(crate) fn resolve_core_stream_direct_finalize_report_kind(plan_kind: &str) -> Option<String> {
    let report_kind = match plan_kind {
        "openai_chat_stream" => "openai_chat_sync_finalize",
        "claude_chat_stream" => "claude_chat_sync_finalize",
        "gemini_chat_stream" => "gemini_chat_sync_finalize",
        "openai_cli_stream" => "openai_cli_sync_finalize",
        "openai_compact_stream" => "openai_compact_sync_finalize",
        "claude_cli_stream" => "claude_cli_sync_finalize",
        "gemini_cli_stream" => "gemini_cli_sync_finalize",
        _ => return None,
    };

    Some(report_kind.to_string())
}

#[cfg(test)]
mod tests {
    use aether_contracts::ExecutionResult;

    use super::{
        resolve_core_stream_error_finalize_report_kind,
        resolve_core_sync_error_finalize_report_kind, should_fallback_to_control_stream,
        should_fallback_to_control_sync, should_retry_next_local_candidate_stream,
        should_retry_next_local_candidate_sync,
    };

    #[test]
    fn sync_failover_marks_chat_errors() {
        let result = ExecutionResult {
            request_id: "req-1".to_string(),
            candidate_id: None,
            status_code: 502,
            headers: Default::default(),
            body: None,
            telemetry: None,
            error: None,
        };

        assert!(should_fallback_to_control_sync(
            "openai_chat_sync",
            &result,
            None,
            false,
            false,
            false,
        ));
        assert_eq!(
            resolve_core_sync_error_finalize_report_kind("openai_chat_sync", &result, None),
            Some("openai_chat_sync_finalize".to_string())
        );
    }

    #[test]
    fn stream_failover_marks_chat_errors() {
        assert!(should_fallback_to_control_stream(
            "openai_chat_stream",
            502,
            false,
        ));
        assert_eq!(
            resolve_core_stream_error_finalize_report_kind("openai_chat_stream", 502),
            Some("openai_chat_sync_finalize".to_string())
        );
    }

    #[test]
    fn sync_retry_next_candidate_is_local_openai_chat_only() {
        let result = ExecutionResult {
            request_id: "req-1".to_string(),
            candidate_id: None,
            status_code: 502,
            headers: Default::default(),
            body: None,
            telemetry: None,
            error: None,
        };
        let local_report_context = serde_json::json!({
            "candidate_index": 0,
            "retry_index": 0,
        });

        assert!(should_retry_next_local_candidate_sync(
            "openai_chat_sync",
            Some(&local_report_context),
            &result,
        ));
        assert!(!should_retry_next_local_candidate_sync(
            "openai_chat_sync",
            None,
            &result,
        ));
        assert!(!should_retry_next_local_candidate_sync(
            "claude_chat_sync",
            None,
            &result,
        ));
    }

    #[test]
    fn sync_retry_next_candidate_treats_rate_limit_as_retryable() {
        let result = ExecutionResult {
            request_id: "req-1".to_string(),
            candidate_id: None,
            status_code: 429,
            headers: Default::default(),
            body: None,
            telemetry: None,
            error: None,
        };
        let local_report_context = serde_json::json!({
            "candidate_index": 0,
            "retry_index": 0,
        });

        assert!(should_retry_next_local_candidate_sync(
            "openai_chat_sync",
            Some(&local_report_context),
            &result,
        ));
    }

    #[test]
    fn stream_retry_next_candidate_is_local_openai_chat_only() {
        let local_report_context = serde_json::json!({
            "candidate_index": 0,
            "retry_index": 0,
        });

        assert!(should_retry_next_local_candidate_stream(
            "openai_chat_stream",
            Some(&local_report_context),
            502,
        ));
        assert!(!should_retry_next_local_candidate_stream(
            "openai_chat_stream",
            None,
            502,
        ));
        assert!(!should_retry_next_local_candidate_stream(
            "claude_chat_stream",
            None,
            502,
        ));
    }

    #[test]
    fn stream_retry_next_candidate_treats_rate_limit_as_retryable() {
        let local_report_context = serde_json::json!({
            "candidate_index": 0,
            "retry_index": 0,
        });

        assert!(should_retry_next_local_candidate_stream(
            "openai_chat_stream",
            Some(&local_report_context),
            429,
        ));
    }
}
