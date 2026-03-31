const ADMIN_BILLING_RUST_BACKEND_DETAIL: &str =
    "Admin billing routes require Rust maintenance backend";

fn default_admin_billing_preset_mode() -> String {
    "merge".to_string()
}

#[derive(Debug, Deserialize)]
struct AdminBillingPresetApplyRequest {
    preset: String,
    #[serde(default = "default_admin_billing_preset_mode")]
    mode: String,
}

fn build_admin_billing_presets_payload() -> serde_json::Value {
    json!({
        "items": [
            {
                "name": "aether-core",
                "version": "1.0",
                "description": "Aether built-in dimension collectors for common api_formats/task_types.",
                "collector_count": build_admin_billing_aether_core_collectors().len(),
            }
        ],
    })
}

fn default_admin_billing_rule_task_type() -> String {
    "chat".to_string()
}

fn default_admin_billing_true() -> bool {
    true
}

fn default_admin_billing_json_object() -> serde_json::Value {
    json!({})
}

fn default_admin_billing_collector_value_type() -> String {
    "float".to_string()
}

#[derive(Debug, Deserialize)]
struct AdminBillingRuleUpsertRequest {
    name: String,
    #[serde(default = "default_admin_billing_rule_task_type")]
    task_type: String,
    #[serde(default)]
    global_model_id: Option<String>,
    #[serde(default)]
    model_id: Option<String>,
    expression: String,
    #[serde(default = "default_admin_billing_json_object")]
    variables: serde_json::Value,
    #[serde(default = "default_admin_billing_json_object")]
    dimension_mappings: serde_json::Value,
    #[serde(default = "default_admin_billing_true")]
    is_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct AdminBillingCollectorUpsertRequest {
    api_format: String,
    task_type: String,
    dimension_name: String,
    source_type: String,
    #[serde(default)]
    source_path: Option<String>,
    #[serde(default = "default_admin_billing_collector_value_type")]
    value_type: String,
    #[serde(default)]
    transform_expression: Option<String>,
    #[serde(default)]
    default_value: Option<String>,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_admin_billing_true")]
    is_enabled: bool,
}

fn build_admin_billing_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_BILLING_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn build_admin_billing_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn build_admin_billing_read_only_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::CONFLICT,
        Json(json!({
            "detail": detail,
            "error_code": "read_only_mode",
        })),
    )
        .into_response()
}

fn build_admin_billing_not_found_response(detail: &'static str) -> Response<Body> {
    (http::StatusCode::NOT_FOUND, Json(json!({ "detail": detail }))).into_response()
}

fn build_admin_billing_aether_core_collectors(
) -> Vec<crate::gateway::AdminBillingCollectorWriteInput> {
    vec![
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "input_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usage.prompt_tokens".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "output_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usage.completion_tokens".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "CLAUDE:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "input_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usage.input_tokens".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "CLAUDE:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "output_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usage.output_tokens".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "input_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usageMetadata.promptTokenCount".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "chat".to_string(),
            dimension_name: "output_tokens".to_string(),
            source_type: "response".to_string(),
            source_path: Some("usageMetadata.candidatesTokenCount".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_resolution_key".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.size".to_string()),
            value_type: "string".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_resolution_key".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.resolution".to_string()),
            value_type: "string".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_size_bytes".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.video_size_bytes".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_duration_seconds".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.video_duration_seconds".to_string()),
            value_type: "float".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "OPENAI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_duration_seconds".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.duration_seconds".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_resolution_key".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.size".to_string()),
            value_type: "string".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_resolution_key".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.resolution".to_string()),
            value_type: "string".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_size_bytes".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.video_size_bytes".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_duration_seconds".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.video_duration_seconds".to_string()),
            value_type: "float".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 10,
            is_enabled: true,
        },
        crate::gateway::AdminBillingCollectorWriteInput {
            api_format: "GEMINI:CHAT".to_string(),
            task_type: "video".to_string(),
            dimension_name: "video_duration_seconds".to_string(),
            source_type: "metadata".to_string(),
            source_path: Some("task.duration_seconds".to_string()),
            value_type: "int".to_string(),
            transform_expression: None,
            default_value: None,
            priority: 0,
            is_enabled: true,
        },
    ]
}

fn resolve_admin_billing_preset_collectors(
    preset: &str,
) -> Option<(&'static str, Vec<crate::gateway::AdminBillingCollectorWriteInput>)> {
    let normalized = preset.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "aether-core" | "default" => Some(("aether-core", build_admin_billing_aether_core_collectors())),
        _ => None,
    }
}

fn parse_admin_billing_preset_apply_request(
    request_body: Option<&axum::body::Bytes>,
) -> Result<(String, String), Response<Body>> {
    let Some(request_body) = request_body else {
        return Err(build_admin_billing_bad_request_response("请求体不能为空"));
    };
    let request = match serde_json::from_slice::<AdminBillingPresetApplyRequest>(request_body) {
        Ok(value) => value,
        Err(err) => {
            return Err(build_admin_billing_bad_request_response(format!(
                "Invalid request body: {err}"
            )))
        }
    };
    let preset = match normalize_admin_billing_required_text(&request.preset, "preset", 100) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let mode = request.mode.trim().to_ascii_lowercase();
    if !matches!(mode.as_str(), "merge" | "overwrite") {
        return Err(build_admin_billing_bad_request_response(
            "mode must be one of merge, overwrite",
        ));
    }
    Ok((preset, mode))
}

fn build_admin_billing_rule_payload_from_record(
    record: &crate::gateway::AdminBillingRuleRecord,
) -> serde_json::Value {
    json!({
        "id": record.id,
        "name": record.name,
        "task_type": record.task_type,
        "global_model_id": record.global_model_id,
        "model_id": record.model_id,
        "expression": record.expression,
        "variables": record.variables,
        "dimension_mappings": record.dimension_mappings,
        "is_enabled": record.is_enabled,
        "created_at": unix_secs_to_rfc3339(record.created_at_unix_secs),
        "updated_at": unix_secs_to_rfc3339(record.updated_at_unix_secs),
    })
}

fn build_admin_billing_collector_payload_from_record(
    record: &crate::gateway::AdminBillingCollectorRecord,
) -> serde_json::Value {
    json!({
        "id": record.id,
        "api_format": record.api_format,
        "task_type": record.task_type,
        "dimension_name": record.dimension_name,
        "source_type": record.source_type,
        "source_path": record.source_path,
        "value_type": record.value_type,
        "transform_expression": record.transform_expression,
        "default_value": record.default_value,
        "priority": record.priority,
        "is_enabled": record.is_enabled,
        "created_at": unix_secs_to_rfc3339(record.created_at_unix_secs),
        "updated_at": unix_secs_to_rfc3339(record.updated_at_unix_secs),
    })
}

fn admin_billing_rule_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/billing/rules/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_billing_collector_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/billing/collectors/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_billing_parse_page(query: Option<&str>) -> Result<u32, String> {
    match query_param_value(query, "page") {
        None => Ok(1),
        Some(value) => {
            let parsed = value
                .parse::<u32>()
                .map_err(|_| "page must be between 1 and 100000".to_string())?;
            if !(1..=100_000).contains(&parsed) {
                return Err("page must be between 1 and 100000".to_string());
            }
            Ok(parsed)
        }
    }
}

fn admin_billing_parse_page_size(query: Option<&str>) -> Result<u32, String> {
    match query_param_value(query, "page_size") {
        None => Ok(50),
        Some(value) => {
            let parsed = value
                .parse::<u32>()
                .map_err(|_| "page_size must be between 1 and 200".to_string())?;
            if !(1..=200).contains(&parsed) {
                return Err("page_size must be between 1 and 200".to_string());
            }
            Ok(parsed)
        }
    }
}

fn admin_billing_optional_filter(query: Option<&str>, key: &str) -> Option<String> {
    query_param_value(query, key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn admin_billing_optional_bool_filter(
    query: Option<&str>,
    key: &str,
) -> Result<Option<bool>, String> {
    match query_param_value(query, key) {
        None => Ok(None),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(Some(true)),
            "false" | "0" | "no" => Ok(Some(false)),
            _ => Err(format!("{key} must be a boolean")),
        },
    }
}

fn admin_billing_pages(total: u64, page_size: u32) -> u64 {
    if total == 0 {
        0
    } else {
        total.div_ceil(u64::from(page_size))
    }
}

fn normalize_admin_billing_required_text(
    value: &str,
    field: &str,
    max_len: usize,
) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{field} must be a non-empty string"));
    }
    if value.len() > max_len {
        return Err(format!("{field} exceeds maximum length {max_len}"));
    }
    Ok(value.to_string())
}

fn normalize_admin_billing_optional_text(
    value: Option<String>,
    max_len: usize,
) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > max_len {
        return Err(format!("field exceeds maximum length {max_len}"));
    }
    Ok(Some(trimmed.to_string()))
}

fn admin_billing_validate_safe_expression(expression: &str) -> Result<(), String> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Err("expression must not be empty".to_string());
    }
    if expression.contains("__") {
        return Err("Dunder names are not allowed".to_string());
    }

    let allowed_chars =
        Regex::new(r"^[A-Za-z0-9_+\-*/%().,\s]+$").expect("regex should compile");
    if !allowed_chars.is_match(expression) {
        return Err("Expression contains unsupported characters".to_string());
    }

    let identifier = Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").expect("regex should compile");
    const ALLOWED_FUNCTIONS: &[&str] = &["min", "max", "abs", "round", "int", "float"];
    for matched in identifier.find_iter(expression) {
        let name = matched.as_str();
        let next_non_ws = expression[matched.end()..]
            .chars()
            .find(|value| !value.is_whitespace());
        if next_non_ws == Some('(') && !ALLOWED_FUNCTIONS.iter().any(|value| value == &name) {
            return Err(format!("Function not allowed: {name}"));
        }
    }
    Ok(())
}

fn parse_admin_billing_rule_request(
    request_body: Option<&axum::body::Bytes>,
) -> Result<crate::gateway::AdminBillingRuleWriteInput, Response<Body>> {
    let Some(request_body) = request_body else {
        return Err(build_admin_billing_bad_request_response("请求体不能为空"));
    };
    let request = match serde_json::from_slice::<AdminBillingRuleUpsertRequest>(request_body) {
        Ok(value) => value,
        Err(err) => {
            return Err(build_admin_billing_bad_request_response(format!(
                "Invalid request body: {err}"
            )))
        }
    };

    let name = match normalize_admin_billing_required_text(&request.name, "name", 100) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let task_type = request.task_type.trim().to_ascii_lowercase();
    if !matches!(task_type.as_str(), "chat" | "video" | "image" | "audio") {
        return Err(build_admin_billing_bad_request_response(
            "task_type must be one of chat, video, image, audio",
        ));
    }
    let global_model_id = match normalize_admin_billing_optional_text(request.global_model_id, 64) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let model_id = match normalize_admin_billing_optional_text(request.model_id, 64) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    if global_model_id.is_some() == model_id.is_some() {
        return Err(build_admin_billing_bad_request_response(
            "Exactly one of global_model_id or model_id must be provided",
        ));
    }
    let expression = request.expression.trim().to_string();
    if let Err(detail) = admin_billing_validate_safe_expression(&expression) {
        return Err(build_admin_billing_bad_request_response(format!(
            "Invalid expression: {detail}"
        )));
    }

    let Some(variables) = request.variables.as_object() else {
        return Err(build_admin_billing_bad_request_response(
            "variables must be a JSON object",
        ));
    };
    for (key, value) in variables {
        if key.trim().is_empty() {
            return Err(build_admin_billing_bad_request_response(
                "variables keys must be non-empty strings",
            ));
        }
        if value.is_boolean() || !value.is_number() {
            return Err(build_admin_billing_bad_request_response(format!(
                "variables['{key}'] must be a number"
            )));
        }
    }

    let Some(dimension_mappings) = request.dimension_mappings.as_object() else {
        return Err(build_admin_billing_bad_request_response(
            "dimension_mappings must be a JSON object",
        ));
    };
    for (key, value) in dimension_mappings {
        if key.trim().is_empty() {
            return Err(build_admin_billing_bad_request_response(
                "dimension_mappings keys must be non-empty strings",
            ));
        }
        let Some(mapping) = value.as_object() else {
            return Err(build_admin_billing_bad_request_response(format!(
                "dimension_mappings['{key}'] must be an object"
            )));
        };
        if !mapping.contains_key("source") {
            return Err(build_admin_billing_bad_request_response(format!(
                "dimension_mappings['{key}'].source is required"
            )));
        }
    }

    Ok(crate::gateway::AdminBillingRuleWriteInput {
        name,
        task_type,
        global_model_id,
        model_id,
        expression,
        variables: serde_json::Value::Object(variables.clone()),
        dimension_mappings: serde_json::Value::Object(dimension_mappings.clone()),
        is_enabled: request.is_enabled,
    })
}

fn admin_billing_optional_epoch_value(
    row: &sqlx::postgres::PgRow,
    field: &str,
) -> Result<Option<String>, GatewayError> {
    let value = row
        .try_get::<Option<i64>, _>(field)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    match value {
        None => Ok(None),
        Some(value) if value < 0 => Ok(None),
        Some(value) => Ok(unix_secs_to_rfc3339(value as u64)),
    }
}

fn admin_billing_rule_payload(row: &sqlx::postgres::PgRow) -> Result<serde_json::Value, GatewayError> {
    Ok(json!({
        "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "name": row.try_get::<String, _>("name").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "task_type": row.try_get::<String, _>("task_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "global_model_id": row.try_get::<Option<String>, _>("global_model_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "model_id": row.try_get::<Option<String>, _>("model_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "expression": row.try_get::<String, _>("expression").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "variables": row.try_get::<Option<serde_json::Value>, _>("variables").map_err(|err| GatewayError::Internal(err.to_string()))?.unwrap_or_else(|| json!({})),
        "dimension_mappings": row.try_get::<Option<serde_json::Value>, _>("dimension_mappings").map_err(|err| GatewayError::Internal(err.to_string()))?.unwrap_or_else(|| json!({})),
        "is_enabled": row.try_get::<bool, _>("is_enabled").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "created_at": admin_billing_optional_epoch_value(row, "created_at_unix_secs")?,
        "updated_at": admin_billing_optional_epoch_value(row, "updated_at_unix_secs")?,
    }))
}

fn admin_billing_collector_payload(
    row: &sqlx::postgres::PgRow,
) -> Result<serde_json::Value, GatewayError> {
    Ok(json!({
        "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "api_format": row.try_get::<String, _>("api_format").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "task_type": row.try_get::<String, _>("task_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "dimension_name": row.try_get::<String, _>("dimension_name").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "source_type": row.try_get::<String, _>("source_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "source_path": row.try_get::<Option<String>, _>("source_path").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "value_type": row.try_get::<String, _>("value_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "transform_expression": row.try_get::<Option<String>, _>("transform_expression").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "default_value": row.try_get::<Option<String>, _>("default_value").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "priority": row.try_get::<i32, _>("priority").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "is_enabled": row.try_get::<bool, _>("is_enabled").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "created_at": admin_billing_optional_epoch_value(row, "created_at_unix_secs")?,
        "updated_at": admin_billing_optional_epoch_value(row, "updated_at_unix_secs")?,
    }))
}

async fn build_admin_list_billing_rules_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let page = match admin_billing_parse_page(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };
    let page_size = match admin_billing_parse_page_size(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };
    let task_type = admin_billing_optional_filter(query, "task_type");
    let is_enabled = match admin_billing_optional_bool_filter(query, "is_enabled") {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some((records, record_total)) = state
        .list_admin_billing_rules(task_type.as_deref(), is_enabled, page, page_size)
        .await?
    {
        total = record_total;
        items = records
            .iter()
            .map(build_admin_billing_rule_payload_from_record)
            .collect::<Vec<_>>();
    } else if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM billing_rules
WHERE ($1::TEXT IS NULL OR task_type = $1)
  AND ($2::BOOL IS NULL OR is_enabled = $2)
            "#,
        )
        .bind(task_type.as_deref())
        .bind(is_enabled)
        .fetch_one(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        total = count_row
            .try_get::<i64, _>("total")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64;

        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id,
  name,
  task_type,
  global_model_id,
  model_id,
  expression,
  variables,
  dimension_mappings,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM billing_rules
WHERE ($1::TEXT IS NULL OR task_type = $1)
  AND ($2::BOOL IS NULL OR is_enabled = $2)
ORDER BY updated_at DESC
OFFSET $3
LIMIT $4
            "#,
        )
        .bind(task_type.as_deref())
        .bind(is_enabled)
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::from(page_size))
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .iter()
            .map(admin_billing_rule_payload)
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": admin_billing_pages(total, page_size),
    }))
    .into_response())
}

async fn build_admin_get_billing_rule_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(rule_id) = admin_billing_rule_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_billing_bad_request_response("缺少 rule_id"));
    };
    if let Some(record) = state.read_admin_billing_rule(&rule_id).await? {
        return Ok(Json(build_admin_billing_rule_payload_from_record(&record)).into_response());
    }
    let Some(pool) = state.postgres_pool() else {
        return Ok(build_admin_billing_not_found_response("Billing rule not found"));
    };

    let row = sqlx::query(
        r#"
SELECT
  id,
  name,
  task_type,
  global_model_id,
  model_id,
  expression,
  variables,
  dimension_mappings,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM billing_rules
WHERE id = $1
        "#,
    )
    .bind(&rule_id)
    .fetch_optional(&pool)
    .await
    .map_err(|err| GatewayError::Internal(err.to_string()))?;

    match row {
        Some(row) => Ok(Json(admin_billing_rule_payload(&row)?).into_response()),
        None => Ok(build_admin_billing_not_found_response("Billing rule not found")),
    }
}

async fn build_admin_list_dimension_collectors_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let page = match admin_billing_parse_page(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };
    let page_size = match admin_billing_parse_page_size(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };
    let api_format = admin_billing_optional_filter(query, "api_format");
    let task_type = admin_billing_optional_filter(query, "task_type");
    let dimension_name = admin_billing_optional_filter(query, "dimension_name");
    let is_enabled = match admin_billing_optional_bool_filter(query, "is_enabled") {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_billing_bad_request_response(detail)),
    };

    if let Some((items, total)) = state
        .list_admin_billing_collectors(
            api_format.as_deref(),
            task_type.as_deref(),
            dimension_name.as_deref(),
            is_enabled,
            page,
            page_size,
        )
        .await?
    {
        return Ok(Json(json!({
            "items": items
                .iter()
                .map(build_admin_billing_collector_payload_from_record)
                .collect::<Vec<_>>(),
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": admin_billing_pages(total, page_size),
        }))
        .into_response());
    }

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM dimension_collectors
WHERE ($1::TEXT IS NULL OR api_format = $1)
  AND ($2::TEXT IS NULL OR task_type = $2)
  AND ($3::TEXT IS NULL OR dimension_name = $3)
  AND ($4::BOOL IS NULL OR is_enabled = $4)
            "#,
        )
        .bind(api_format.as_deref())
        .bind(task_type.as_deref())
        .bind(dimension_name.as_deref())
        .bind(is_enabled)
        .fetch_one(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        total = count_row
            .try_get::<i64, _>("total")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64;

        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM dimension_collectors
WHERE ($1::TEXT IS NULL OR api_format = $1)
  AND ($2::TEXT IS NULL OR task_type = $2)
  AND ($3::TEXT IS NULL OR dimension_name = $3)
  AND ($4::BOOL IS NULL OR is_enabled = $4)
ORDER BY updated_at DESC, priority DESC, id ASC
OFFSET $5
LIMIT $6
            "#,
        )
        .bind(api_format.as_deref())
        .bind(task_type.as_deref())
        .bind(dimension_name.as_deref())
        .bind(is_enabled)
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::from(page_size))
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .iter()
            .map(admin_billing_collector_payload)
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": admin_billing_pages(total, page_size),
    }))
    .into_response())
}

async fn build_admin_get_dimension_collector_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(collector_id) = admin_billing_collector_id_from_path(&request_context.request_path)
    else {
        return Ok(build_admin_billing_bad_request_response("缺少 collector_id"));
    };

    if let Some(record) = state.read_admin_billing_collector(&collector_id).await? {
        return Ok(Json(build_admin_billing_collector_payload_from_record(&record)).into_response());
    }

    let Some(pool) = state.postgres_pool() else {
        return Ok(build_admin_billing_not_found_response(
            "Dimension collector not found",
        ));
    };

    let row = sqlx::query(
        r#"
SELECT
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM dimension_collectors
WHERE id = $1
        "#,
    )
    .bind(&collector_id)
    .fetch_optional(&pool)
    .await
    .map_err(|err| GatewayError::Internal(err.to_string()))?;

    match row {
        Some(row) => Ok(Json(admin_billing_collector_payload(&row)?).into_response()),
        None => Ok(build_admin_billing_not_found_response(
            "Dimension collector not found",
        )),
    }
}

async fn parse_admin_billing_collector_request(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
    existing_id: Option<&str>,
) -> Result<crate::gateway::AdminBillingCollectorWriteInput, Response<Body>> {
    let Some(request_body) = request_body else {
        return Err(build_admin_billing_bad_request_response("请求体不能为空"));
    };
    let request =
        match serde_json::from_slice::<AdminBillingCollectorUpsertRequest>(request_body) {
            Ok(value) => value,
            Err(err) => {
                return Err(build_admin_billing_bad_request_response(format!(
                    "Invalid request body: {err}"
                )))
            }
        };

    let api_format = match normalize_admin_billing_required_text(&request.api_format, "api_format", 50)
    {
        Ok(value) => value.to_ascii_uppercase(),
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let task_type = match normalize_admin_billing_required_text(&request.task_type, "task_type", 20)
    {
        Ok(value) => value.to_ascii_lowercase(),
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let dimension_name =
        match normalize_admin_billing_required_text(&request.dimension_name, "dimension_name", 100)
        {
            Ok(value) => value,
            Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
        };
    let source_type = request.source_type.trim().to_ascii_lowercase();
    if !matches!(
        source_type.as_str(),
        "request" | "response" | "metadata" | "computed"
    ) {
        return Err(build_admin_billing_bad_request_response(
            "source_type must be one of request, response, metadata, computed",
        ));
    }
    let value_type = request.value_type.trim().to_ascii_lowercase();
    if !matches!(value_type.as_str(), "float" | "int" | "string") {
        return Err(build_admin_billing_bad_request_response(
            "value_type must be one of float, int, string",
        ));
    }
    let source_path = match normalize_admin_billing_optional_text(request.source_path, 200) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };
    let transform_expression =
        match normalize_admin_billing_optional_text(request.transform_expression, 4096) {
            Ok(value) => value,
            Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
        };
    let default_value = match normalize_admin_billing_optional_text(request.default_value, 100) {
        Ok(value) => value,
        Err(detail) => return Err(build_admin_billing_bad_request_response(detail)),
    };

    if source_type == "computed" {
        if source_path.is_some() {
            return Err(build_admin_billing_bad_request_response(
                "computed collector must have source_path=null",
            ));
        }
        if transform_expression.is_none() {
            return Err(build_admin_billing_bad_request_response(
                "computed collector must have transform_expression",
            ));
        }
    } else if source_path.is_none() {
        return Err(build_admin_billing_bad_request_response(
            "non-computed collector must have source_path",
        ));
    }

    if let Some(transform_expression) = transform_expression.as_deref() {
        if let Err(detail) = admin_billing_validate_safe_expression(transform_expression) {
            return Err(build_admin_billing_bad_request_response(format!(
                "Invalid transform_expression: {detail}"
            )));
        }
    }

    if default_value.is_some() && request.is_enabled {
        match state
            .admin_billing_enabled_default_value_exists(
                &api_format,
                &task_type,
                &dimension_name,
                existing_id,
            )
            .await
        {
            Ok(true) => {
                return Err(build_admin_billing_bad_request_response(
                    "default_value already exists for this (api_format, task_type, dimension_name)",
                ))
            }
            Ok(false) => {}
            Err(err) => {
                let detail = match err {
                    GatewayError::Internal(message) => message,
                    other => format!("{other:?}"),
                };
                return Err(
                    (
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "detail": detail })),
                    )
                        .into_response(),
                )
            }
        }
    }

    Ok(crate::gateway::AdminBillingCollectorWriteInput {
        api_format,
        task_type,
        dimension_name,
        source_type,
        source_path,
        value_type,
        transform_expression,
        default_value,
        priority: request.priority,
        is_enabled: request.is_enabled,
    })
}

async fn build_admin_create_billing_rule_response(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let input = match parse_admin_billing_rule_request(request_body) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    match state.create_admin_billing_rule(&input).await? {
        crate::gateway::LocalMutationOutcome::Applied(record) => {
            Ok(Json(build_admin_billing_rule_payload_from_record(&record)).into_response())
        }
        crate::gateway::LocalMutationOutcome::Invalid(detail) => {
            Ok(build_admin_billing_bad_request_response(detail))
        }
        crate::gateway::LocalMutationOutcome::NotFound => {
            Ok(build_admin_billing_not_found_response("Billing rule not found"))
        }
        crate::gateway::LocalMutationOutcome::Unavailable => {
            Ok(build_admin_billing_read_only_response(
                "当前为只读模式，无法创建计费规则",
            ))
        }
    }
}

async fn build_admin_update_billing_rule_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(rule_id) = admin_billing_rule_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_billing_bad_request_response("缺少 rule_id"));
    };
    let input = match parse_admin_billing_rule_request(request_body) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    match state.update_admin_billing_rule(&rule_id, &input).await? {
        crate::gateway::LocalMutationOutcome::Applied(record) => {
            Ok(Json(build_admin_billing_rule_payload_from_record(&record)).into_response())
        }
        crate::gateway::LocalMutationOutcome::NotFound => {
            Ok(build_admin_billing_not_found_response("Billing rule not found"))
        }
        crate::gateway::LocalMutationOutcome::Invalid(detail) => {
            Ok(build_admin_billing_bad_request_response(detail))
        }
        crate::gateway::LocalMutationOutcome::Unavailable => {
            Ok(build_admin_billing_read_only_response(
                "当前为只读模式，无法更新计费规则",
            ))
        }
    }
}

async fn build_admin_create_dimension_collector_response(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let input = match parse_admin_billing_collector_request(state, request_body, None).await {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    match state.create_admin_billing_collector(&input).await? {
        crate::gateway::LocalMutationOutcome::Applied(record) => {
            Ok(Json(build_admin_billing_collector_payload_from_record(&record)).into_response())
        }
        crate::gateway::LocalMutationOutcome::Invalid(detail) => {
            Ok(build_admin_billing_bad_request_response(detail))
        }
        crate::gateway::LocalMutationOutcome::NotFound => Ok(
            build_admin_billing_not_found_response("Dimension collector not found"),
        ),
        crate::gateway::LocalMutationOutcome::Unavailable => {
            Ok(build_admin_billing_read_only_response(
                "当前为只读模式，无法创建维度采集器",
            ))
        }
    }
}

async fn build_admin_update_dimension_collector_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(collector_id) = admin_billing_collector_id_from_path(&request_context.request_path)
    else {
        return Ok(build_admin_billing_bad_request_response("缺少 collector_id"));
    };
    let input = match parse_admin_billing_collector_request(
        state,
        request_body,
        Some(&collector_id),
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    match state
        .update_admin_billing_collector(&collector_id, &input)
        .await?
    {
        crate::gateway::LocalMutationOutcome::Applied(record) => {
            Ok(Json(build_admin_billing_collector_payload_from_record(&record)).into_response())
        }
        crate::gateway::LocalMutationOutcome::NotFound => Ok(
            build_admin_billing_not_found_response("Dimension collector not found"),
        ),
        crate::gateway::LocalMutationOutcome::Invalid(detail) => {
            Ok(build_admin_billing_bad_request_response(detail))
        }
        crate::gateway::LocalMutationOutcome::Unavailable => {
            Ok(build_admin_billing_read_only_response(
                "当前为只读模式，无法更新维度采集器",
            ))
        }
    }
}

async fn build_admin_apply_billing_preset_response(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let (preset, mode) = match parse_admin_billing_preset_apply_request(request_body) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let Some((resolved_preset, collectors)) = resolve_admin_billing_preset_collectors(&preset) else {
        let payload = json!({
            "ok": false,
            "preset": preset,
            "mode": mode,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "errors": ["Unknown preset: available presets are aether-core"],
        });
        return Ok(Json(payload).into_response());
    };

    match state
        .apply_admin_billing_preset(resolved_preset, &mode, &collectors)
        .await?
    {
        crate::gateway::LocalMutationOutcome::Applied(result) => Ok(Json(json!({
            "ok": result.errors.is_empty(),
            "preset": result.preset,
            "mode": result.mode,
            "created": result.created,
            "updated": result.updated,
            "skipped": result.skipped,
            "errors": result.errors,
        }))
        .into_response()),
        crate::gateway::LocalMutationOutcome::Unavailable => {
            Ok(build_admin_billing_read_only_response(
                "当前为只读模式，无法应用计费预设",
            ))
        }
        crate::gateway::LocalMutationOutcome::Invalid(detail) => {
            Ok(build_admin_billing_bad_request_response(detail))
        }
        crate::gateway::LocalMutationOutcome::NotFound => {
            Ok(build_admin_billing_not_found_response("Billing preset not found"))
        }
    }
}

async fn maybe_build_local_admin_billing_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("billing_manage") {
        return Ok(None);
    }

    let path = request_context.request_path.as_str();
    let is_billing_route = (request_context.request_method == http::Method::GET
        && matches!(
            path,
            "/api/admin/billing/presets" | "/api/admin/billing/presets/"
        ))
        || (request_context.request_method == http::Method::POST
            && matches!(
                path,
                "/api/admin/billing/presets/apply" | "/api/admin/billing/presets/apply/"
            ))
        || (request_context.request_method == http::Method::GET
            && matches!(path, "/api/admin/billing/rules" | "/api/admin/billing/rules/"))
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/billing/rules/")
            && path.matches('/').count() == 5)
        || (request_context.request_method == http::Method::POST
            && matches!(path, "/api/admin/billing/rules" | "/api/admin/billing/rules/"))
        || (request_context.request_method == http::Method::PUT
            && path.starts_with("/api/admin/billing/rules/")
            && path.matches('/').count() == 5)
        || (request_context.request_method == http::Method::GET
            && matches!(
                path,
                "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
            ))
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/billing/collectors/")
            && path.matches('/').count() == 5)
        || (request_context.request_method == http::Method::POST
            && matches!(
                path,
                "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
            ))
        || (request_context.request_method == http::Method::PUT
            && path.starts_with("/api/admin/billing/collectors/")
            && path.matches('/').count() == 5);

    if !is_billing_route {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("list_presets")
            if request_context.request_method == http::Method::GET
                && matches!(
                    path,
                    "/api/admin/billing/presets" | "/api/admin/billing/presets/"
                ) =>
        {
            Ok(Some(Json(build_admin_billing_presets_payload()).into_response()))
        }
        Some("apply_preset")
            if request_context.request_method == http::Method::POST
                && matches!(
                    path,
                    "/api/admin/billing/presets/apply" | "/api/admin/billing/presets/apply/"
                ) =>
        {
            Ok(Some(
                build_admin_apply_billing_preset_response(state, request_body).await?,
            ))
        }
        Some("list_rules")
            if request_context.request_method == http::Method::GET
                && matches!(path, "/api/admin/billing/rules" | "/api/admin/billing/rules/") =>
        {
            Ok(Some(
                build_admin_list_billing_rules_response(state, request_context).await?,
            ))
        }
        Some("get_rule")
            if request_context.request_method == http::Method::GET
                && path.starts_with("/api/admin/billing/rules/") =>
        {
            Ok(Some(
                build_admin_get_billing_rule_response(state, request_context).await?,
            ))
        }
        Some("list_collectors")
            if request_context.request_method == http::Method::GET
                && matches!(
                    path,
                    "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
                ) =>
        {
            Ok(Some(
                build_admin_list_dimension_collectors_response(state, request_context).await?,
            ))
        }
        Some("get_collector")
            if request_context.request_method == http::Method::GET
                && path.starts_with("/api/admin/billing/collectors/") =>
        {
            Ok(Some(
                build_admin_get_dimension_collector_response(state, request_context).await?,
            ))
        }
        Some("create_rule")
            if request_context.request_method == http::Method::POST
                && matches!(path, "/api/admin/billing/rules" | "/api/admin/billing/rules/") =>
        {
            Ok(Some(
                build_admin_create_billing_rule_response(state, request_body).await?,
            ))
        }
        Some("update_rule")
            if request_context.request_method == http::Method::PUT
                && path.starts_with("/api/admin/billing/rules/") =>
        {
            Ok(Some(
                build_admin_update_billing_rule_response(state, request_context, request_body)
                    .await?,
            ))
        }
        Some("create_collector")
            if request_context.request_method == http::Method::POST
                && matches!(
                    path,
                    "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
                ) =>
        {
            Ok(Some(
                build_admin_create_dimension_collector_response(state, request_body).await?,
            ))
        }
        Some("update_collector")
            if request_context.request_method == http::Method::PUT
                && path.starts_with("/api/admin/billing/collectors/") =>
        {
            Ok(Some(
                build_admin_update_dimension_collector_response(
                    state,
                    request_context,
                    request_body,
                )
                .await?,
            ))
        }
        _ => Ok(Some(build_admin_billing_maintenance_response())),
    }
}
