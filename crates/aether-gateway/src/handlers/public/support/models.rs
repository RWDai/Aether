fn models_api_format(request_context: &GatewayPublicRequestContext) -> Option<&str> {
    request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.auth_endpoint_signature.as_deref())
        .filter(|signature| matches!(*signature, "openai:chat" | "claude:chat" | "gemini:chat"))
}

fn models_detail_id(request_path: &str) -> Option<String> {
    let raw = if let Some(value) = request_path.strip_prefix("/v1/models/") {
        value
    } else if let Some(value) = request_path.strip_prefix("/v1beta/models/") {
        value
    } else {
        return None;
    };
    let normalized = raw.trim().trim_start_matches("models/").trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn auth_snapshot_allows_provider_for_models(
    auth_snapshot: Option<&crate::gateway::data::StoredGatewayAuthApiKeySnapshot>,
    provider_id: &str,
    provider_name: &str,
) -> bool {
    let Some(allowed) = auth_snapshot.and_then(
        crate::gateway::data::StoredGatewayAuthApiKeySnapshot::effective_allowed_providers,
    ) else {
        return true;
    };

    allowed.iter().any(|value| {
        value.trim().eq_ignore_ascii_case(provider_id.trim())
            || value.trim().eq_ignore_ascii_case(provider_name.trim())
    })
}

fn auth_snapshot_allows_model_for_models(
    auth_snapshot: Option<&crate::gateway::data::StoredGatewayAuthApiKeySnapshot>,
    global_model_name: &str,
) -> bool {
    let Some(allowed) = auth_snapshot
        .and_then(crate::gateway::data::StoredGatewayAuthApiKeySnapshot::effective_allowed_models)
    else {
        return true;
    };
    allowed.iter().any(|value| value == global_model_name)
}

fn mapping_scope_matches_for_models(
    mapping: &aether_data::repository::candidate_selection::StoredProviderModelMapping,
    api_format: &str,
) -> bool {
    let Some(api_formats) = mapping.api_formats.as_ref() else {
        return true;
    };
    api_formats
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(api_format))
}

fn candidate_model_names_for_models(
    row: &StoredMinimalCandidateSelectionRow,
    api_format: &str,
) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::from([row.model_provider_model_name.clone()]);
    if let Some(mappings) = row.model_provider_model_mappings.as_ref() {
        for mapping in mappings {
            if mapping_scope_matches_for_models(mapping, api_format) {
                names.insert(mapping.name.clone());
            }
        }
    }
    names
}

pub(super) fn matches_model_mapping_for_models(pattern: &str, model_name: &str) -> bool {
    let Ok(compiled) = Regex::new(&format!("^(?:{pattern})$")) else {
        return false;
    };
    compiled.is_match(model_name)
}

fn row_exposes_global_model_for_models(
    row: &StoredMinimalCandidateSelectionRow,
    api_format: &str,
) -> bool {
    let Some(key_allowed_models) = row.key_allowed_models.as_ref() else {
        return true;
    };
    if key_allowed_models.is_empty() {
        return false;
    }
    if key_allowed_models
        .iter()
        .any(|value| value == &row.global_model_name)
    {
        return true;
    }

    let candidate_models = candidate_model_names_for_models(row, api_format);
    for allowed_model in key_allowed_models {
        if candidate_models.contains(allowed_model) {
            return true;
        }
    }

    let Some(global_model_mappings) = row.global_model_mappings.as_ref() else {
        return false;
    };
    for allowed_model in key_allowed_models {
        for pattern in global_model_mappings {
            if matches_model_mapping_for_models(pattern, allowed_model) {
                return true;
            }
        }
    }

    false
}

fn build_models_auth_error_response(api_format: &str) -> Response<Body> {
    match api_format {
        "claude:chat" => (
            http::StatusCode::UNAUTHORIZED,
            Json(json!({
                "type": "error",
                "error": {
                    "type": "authentication_error",
                    "message": "Invalid API key provided",
                },
            })),
        )
            .into_response(),
        "gemini:chat" => (
            http::StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "code": 401,
                    "message": "API key not valid. Please pass a valid API key.",
                    "status": "UNAUTHENTICATED",
                }
            })),
        )
            .into_response(),
        _ => (
            http::StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "message": "Incorrect API key provided. You can find your API key at https://platform.openai.com/account/api-keys.",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": "invalid_api_key",
                }
            })),
        )
            .into_response(),
    }
}

fn build_models_not_found_response(model_id: &str, api_format: &str) -> Response<Body> {
    match api_format {
        "claude:chat" => (
            http::StatusCode::NOT_FOUND,
            Json(json!({
                "type": "error",
                "error": {
                    "type": "not_found_error",
                    "message": format!("Model '{model_id}' not found"),
                },
            })),
        )
            .into_response(),
        "gemini:chat" => (
            http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "code": 404,
                    "message": format!("models/{model_id} is not found"),
                    "status": "NOT_FOUND",
                }
            })),
        )
            .into_response(),
        _ => (
            http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": format!("The model '{model_id}' does not exist"),
                    "type": "invalid_request_error",
                    "param": "model",
                    "code": "model_not_found",
                }
            })),
        )
            .into_response(),
    }
}

fn build_empty_models_list_response(api_format: &str) -> Response<Body> {
    match api_format {
        "claude:chat" => Json(json!({
            "data": [],
            "has_more": false,
            "first_id": serde_json::Value::Null,
            "last_id": serde_json::Value::Null,
        }))
        .into_response(),
        "gemini:chat" => Json(json!({ "models": [] })).into_response(),
        _ => Json(json!({ "object": "list", "data": [] })).into_response(),
    }
}

fn build_openai_models_list_response(
    rows: &[StoredMinimalCandidateSelectionRow],
) -> Response<Body> {
    Json(json!({
        "object": "list",
        "data": rows.iter().map(|row| {
            json!({
                "id": row.global_model_name,
                "object": "model",
                "created": 0,
                "owned_by": row.provider_name,
            })
        }).collect::<Vec<_>>(),
    }))
    .into_response()
}

fn build_openai_model_detail_response(row: &StoredMinimalCandidateSelectionRow) -> Response<Body> {
    Json(json!({
        "id": row.global_model_name,
        "object": "model",
        "created": 0,
        "owned_by": row.provider_name,
    }))
    .into_response()
}

fn build_claude_models_list_response(
    rows: &[StoredMinimalCandidateSelectionRow],
    before_id: Option<&str>,
    after_id: Option<&str>,
    limit: usize,
) -> Response<Body> {
    let model_data = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.global_model_name,
                "type": "model",
                "display_name": row.global_model_name,
                "created_at": serde_json::Value::Null,
            })
        })
        .collect::<Vec<_>>();

    let mut start_idx = 0usize;
    if let Some(after_id) = after_id {
        if let Some(index) = model_data.iter().position(|item| item["id"] == after_id) {
            start_idx = index.saturating_add(1);
        }
    }
    let mut end_idx = model_data.len();
    if let Some(before_id) = before_id {
        if let Some(index) = model_data.iter().position(|item| item["id"] == before_id) {
            end_idx = index;
        }
    }
    let window = &model_data[start_idx.min(end_idx)..end_idx];
    let paginated = window.iter().take(limit).cloned().collect::<Vec<_>>();
    let first_id = paginated
        .first()
        .and_then(|item| item.get("id"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let last_id = paginated
        .last()
        .and_then(|item| item.get("id"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    Json(json!({
        "data": paginated,
        "has_more": window.len() > limit,
        "first_id": first_id,
        "last_id": last_id,
    }))
    .into_response()
}

fn build_claude_model_detail_response(row: &StoredMinimalCandidateSelectionRow) -> Response<Body> {
    Json(json!({
        "id": row.global_model_name,
        "type": "model",
        "display_name": row.global_model_name,
        "created_at": serde_json::Value::Null,
    }))
    .into_response()
}

fn build_gemini_model_value(row: &StoredMinimalCandidateSelectionRow) -> serde_json::Value {
    json!({
        "name": format!("models/{}", row.global_model_name),
        "baseModelId": row.global_model_name,
        "version": "001",
        "displayName": row.global_model_name,
        "description": format!("Model {}", row.global_model_name),
        "inputTokenLimit": 128000,
        "outputTokenLimit": 8192,
        "supportedGenerationMethods": ["generateContent", "countTokens"],
        "temperature": 1.0,
        "maxTemperature": 2.0,
        "topP": 0.95,
        "topK": 64,
    })
}

fn build_gemini_models_list_response(
    rows: &[StoredMinimalCandidateSelectionRow],
    page_size: usize,
    page_token: Option<&str>,
) -> Response<Body> {
    let start_idx = page_token
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let end_idx = start_idx.saturating_add(page_size);
    let window = rows
        .iter()
        .skip(start_idx)
        .take(page_size)
        .map(build_gemini_model_value)
        .collect::<Vec<_>>();
    let mut payload = json!({ "models": window });
    if end_idx < rows.len() {
        payload["nextPageToken"] = serde_json::Value::String(end_idx.to_string());
    }
    Json(payload).into_response()
}

fn build_gemini_model_detail_response(row: &StoredMinimalCandidateSelectionRow) -> Response<Body> {
    Json(build_gemini_model_value(row)).into_response()
}

async fn maybe_build_local_models_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("models") {
        return None;
    }
    let api_format = models_api_format(request_context)?;
    if !state.has_minimal_candidate_selection_reader() {
        return None;
    }

    let auth_context = decision.auth_context.as_ref()?;
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let auth_snapshot = state
        .read_auth_api_key_snapshot(
            &auth_context.user_id,
            &auth_context.api_key_id,
            now_unix_secs,
        )
        .await
        .ok()
        .flatten();
    let auth_snapshot = auth_snapshot.as_ref();

    let filter_rows = |rows: Vec<StoredMinimalCandidateSelectionRow>| {
        let mut filtered = rows
            .into_iter()
            .filter(|row| {
                auth_snapshot_allows_provider_for_models(
                    auth_snapshot,
                    &row.provider_id,
                    &row.provider_name,
                )
            })
            .filter(|row| {
                auth_snapshot_allows_model_for_models(auth_snapshot, &row.global_model_name)
            })
            .filter(|row| row_exposes_global_model_for_models(row, api_format))
            .collect::<Vec<_>>();
        filtered.sort_by(|left, right| left.global_model_name.cmp(&right.global_model_name));
        let mut deduped = Vec::new();
        let mut last_model_name: Option<String> = None;
        for row in filtered {
            if last_model_name.as_deref() == Some(row.global_model_name.as_str()) {
                continue;
            }
            last_model_name = Some(row.global_model_name.clone());
            deduped.push(row);
        }
        deduped
    };

    match decision.route_kind.as_deref() {
        Some("list") => {
            let rows = state
                .list_minimal_candidate_selection_rows_for_api_format(api_format)
                .await
                .ok()?;
            let rows = filter_rows(rows);
            if rows.is_empty() {
                return Some(build_empty_models_list_response(api_format));
            }
            let response = match api_format {
                "claude:chat" => {
                    let before_id = query_param_value(
                        request_context.request_query_string.as_deref(),
                        "before_id",
                    );
                    let after_id = query_param_value(
                        request_context.request_query_string.as_deref(),
                        "after_id",
                    );
                    let limit =
                        query_param_value(request_context.request_query_string.as_deref(), "limit")
                            .and_then(|value| value.parse::<usize>().ok())
                            .filter(|value| *value > 0)
                            .unwrap_or(20);
                    build_claude_models_list_response(
                        &rows,
                        before_id.as_deref(),
                        after_id.as_deref(),
                        limit,
                    )
                }
                "gemini:chat" => {
                    let page_size = query_param_value(
                        request_context.request_query_string.as_deref(),
                        "pageSize",
                    )
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or(50);
                    let page_token = query_param_value(
                        request_context.request_query_string.as_deref(),
                        "pageToken",
                    );
                    build_gemini_models_list_response(&rows, page_size, page_token.as_deref())
                }
                _ => build_openai_models_list_response(&rows),
            };
            Some(response)
        }
        Some("detail") => {
            let model_id = models_detail_id(&request_context.request_path)?;
            let rows = state
                .list_minimal_candidate_selection_rows_for_api_format_and_global_model(
                    api_format, &model_id,
                )
                .await
                .ok()?;
            let rows = filter_rows(rows);
            let Some(row) = rows.first() else {
                return Some(build_models_not_found_response(&model_id, api_format));
            };
            let response = match api_format {
                "claude:chat" => build_claude_model_detail_response(row),
                "gemini:chat" => build_gemini_model_detail_response(row),
                _ => build_openai_model_detail_response(row),
            };
            Some(response)
        }
        _ => Some(build_models_auth_error_response(api_format)),
    }
}
