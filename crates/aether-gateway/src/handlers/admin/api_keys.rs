#[derive(Debug, Default, serde::Deserialize)]
struct AdminStandaloneApiKeyCreateRequest {
    name: Option<String>,
    allowed_providers: Option<Vec<String>>,
    allowed_api_formats: Option<Vec<String>>,
    allowed_models: Option<Vec<String>>,
    rate_limit: Option<i32>,
    initial_balance_usd: Option<f64>,
    unlimited_balance: Option<bool>,
    expire_days: Option<i32>,
    expires_at: Option<String>,
    auto_delete_on_expiry: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminStandaloneApiKeyUpdateRequest {
    name: Option<String>,
    allowed_providers: Option<Vec<String>>,
    allowed_api_formats: Option<Vec<String>>,
    allowed_models: Option<Vec<String>>,
    rate_limit: Option<i32>,
    initial_balance_usd: Option<f64>,
    unlimited_balance: Option<bool>,
    expire_days: Option<i32>,
    expires_at: Option<String>,
    auto_delete_on_expiry: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminStandaloneApiKeyToggleRequest {
    is_active: Option<bool>,
}

#[derive(Debug, Default)]
struct AdminStandaloneApiKeyFieldPresence {
    allowed_providers: bool,
    allowed_api_formats: bool,
    allowed_models: bool,
}

const ADMIN_API_KEYS_RUST_BACKEND_DETAIL: &str =
    "Admin standalone API key routes require Rust maintenance backend";

fn build_admin_api_keys_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_API_KEYS_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn build_admin_api_keys_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn build_admin_api_keys_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "API密钥不存在" })),
    )
        .into_response()
}

fn admin_api_keys_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/api-keys/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_api_keys_operator_id(request_context: &GatewayPublicRequestContext) -> Option<String> {
    request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.admin_principal.as_ref())
        .map(|principal| principal.user_id.clone())
}

fn admin_api_keys_parse_skip(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "skip") {
        None => Ok(0),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "skip must be a non-negative integer".to_string()),
    }
}

fn admin_api_keys_parse_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        None => Ok(100),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be a positive integer".to_string())?;
            if parsed == 0 || parsed > 500 {
                return Err("limit must be between 1 and 500".to_string());
            }
            Ok(parsed)
        }
    }
}

fn build_admin_api_key_list_item_payload(
    state: &AppState,
    record: &aether_data::repository::auth::StoredAuthApiKeyExportRecord,
    total_tokens: u64,
) -> serde_json::Value {
    json!({
        "id": record.api_key_id,
        "user_id": record.user_id,
        "name": record.name,
        "key_display": masked_user_api_key_display(state, record.key_encrypted.as_deref()),
        "is_active": record.is_active,
        "is_standalone": true,
        "total_requests": record.total_requests,
        "total_tokens": total_tokens,
        "total_cost_usd": record.total_cost_usd,
        "rate_limit": record.rate_limit,
        "allowed_providers": record.allowed_providers,
        "allowed_api_formats": record.allowed_api_formats,
        "allowed_models": record.allowed_models,
        "last_used_at": serde_json::Value::Null,
        "expires_at": format_optional_unix_secs_iso8601(record.expires_at_unix_secs),
        "created_at": serde_json::Value::Null,
        "updated_at": serde_json::Value::Null,
        "auto_delete_on_expiry": record.auto_delete_on_expiry,
    })
}

fn build_admin_api_key_detail_payload(
    state: &AppState,
    record: &aether_data::repository::auth::StoredAuthApiKeyExportRecord,
    total_tokens: u64,
    wallet: Option<&aether_data::repository::wallet::StoredWalletSnapshot>,
) -> serde_json::Value {
    json!({
        "id": record.api_key_id,
        "user_id": record.user_id,
        "name": record.name,
        "key_display": masked_user_api_key_display(state, record.key_encrypted.as_deref()),
        "is_active": record.is_active,
        "is_standalone": true,
        "total_requests": record.total_requests,
        "total_tokens": total_tokens,
        "total_cost_usd": record.total_cost_usd,
        "rate_limit": record.rate_limit,
        "allowed_providers": record.allowed_providers,
        "allowed_api_formats": record.allowed_api_formats,
        "allowed_models": record.allowed_models,
        "last_used_at": serde_json::Value::Null,
        "expires_at": format_optional_unix_secs_iso8601(record.expires_at_unix_secs),
        "created_at": serde_json::Value::Null,
        "updated_at": serde_json::Value::Null,
        "wallet": serialize_admin_system_users_export_wallet(wallet),
    })
}

async fn admin_api_key_total_tokens_by_ids(
    state: &AppState,
    api_key_ids: &[String],
) -> Result<std::collections::BTreeMap<String, u64>, GatewayError> {
    if api_key_ids.is_empty() || !state.has_usage_data_reader() {
        return Ok(std::collections::BTreeMap::new());
    }

    state
        .summarize_usage_total_tokens_by_api_key_ids(api_key_ids)
        .await
}

async fn build_admin_list_api_keys_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let skip = match admin_api_keys_parse_skip(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
    };
    let limit = match admin_api_keys_parse_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
    };
    let is_active = query_param_optional_bool(query, "is_active");

    let total = state
        .count_auth_api_key_export_standalone_records(is_active)
        .await? as usize;
    let paged_records = state
        .list_auth_api_key_export_standalone_records_page(
            &aether_data::repository::auth::StandaloneApiKeyExportListQuery {
                skip,
                limit,
                is_active,
            },
        )
        .await?;
    let api_key_ids = paged_records
        .iter()
        .map(|record| record.api_key_id.clone())
        .collect::<Vec<_>>();
    let total_tokens_by_api_key_id = admin_api_key_total_tokens_by_ids(state, &api_key_ids).await?;

    let api_keys = paged_records
        .iter()
        .map(|record| {
            build_admin_api_key_list_item_payload(
                state,
                record,
                total_tokens_by_api_key_id
                    .get(&record.api_key_id)
                    .copied()
                    .unwrap_or(0),
            )
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "api_keys": api_keys,
        "total": total,
        "limit": limit,
        "skip": skip,
    }))
    .into_response())
}

async fn build_admin_api_key_detail_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(api_key_id) = admin_api_keys_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_api_keys_maintenance_response());
    };

    if state
        .read_auth_api_key_snapshots_by_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .any(|snapshot| snapshot.api_key_id == api_key_id && !snapshot.api_key_is_standalone)
    {
        return Ok(build_admin_api_keys_bad_request_response(
            "仅支持查看独立密钥",
        ));
    }

    let Some(record) = state
        .find_auth_api_key_export_standalone_record_by_id(&api_key_id)
        .await?
    else {
        return Ok(build_admin_api_keys_not_found_response());
    };

    if query_param_bool(
        request_context.request_query_string.as_deref(),
        "include_key",
        false,
    ) {
        let Some(ciphertext) = record
            .key_encrypted
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(build_admin_api_keys_bad_request_response(
                "该密钥没有存储完整密钥信息",
            ));
        };
        let Some(key) = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
        else {
            return Ok((
                http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "detail": "解密密钥失败" })),
            )
                .into_response());
        };
        return Ok(Json(json!({ "key": key })).into_response());
    }

    let wallet = state
        .list_wallet_snapshots_by_api_key_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id.as_str()));
    let total_tokens_by_api_key_id =
        admin_api_key_total_tokens_by_ids(state, std::slice::from_ref(&api_key_id)).await?;
    let total_tokens = total_tokens_by_api_key_id
        .get(&api_key_id)
        .copied()
        .unwrap_or(0);

    Ok(Json(build_admin_api_key_detail_payload(
        state,
        &record,
        total_tokens,
        wallet.as_ref(),
    ))
    .into_response())
}

async fn build_admin_create_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_api_keys_maintenance_response());
    }

    let Some(operator_id) = admin_api_keys_operator_id(request_context) else {
        return Ok(build_admin_api_keys_maintenance_response());
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败"));
    };
    let payload = match serde_json::from_slice::<AdminStandaloneApiKeyCreateRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败")),
    };
    if payload.initial_balance_usd.is_some()
        || payload.unlimited_balance.is_some()
        || payload.expire_days.is_some()
        || payload.expires_at.is_some()
        || payload.auto_delete_on_expiry.is_some()
    {
        return Ok(build_admin_api_keys_bad_request_response(
            "当前仅支持 name、rate_limit、allowed_providers、allowed_api_formats、allowed_models 字段",
        ));
    }

    let name = match normalize_admin_optional_api_key_name(payload.name) {
        Ok(Some(value)) => value,
        Ok(None) => default_admin_user_api_key_name(),
        Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
    };
    let allowed_providers =
        match normalize_admin_user_string_list(payload.allowed_providers, "allowed_providers") {
            Ok(value) => value,
            Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
        };
    let allowed_api_formats = match normalize_admin_user_api_formats(payload.allowed_api_formats) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
    };
    let allowed_models =
        match normalize_admin_user_string_list(payload.allowed_models, "allowed_models") {
            Ok(value) => value,
            Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
        };
    let rate_limit = payload.rate_limit.unwrap_or(0);
    if rate_limit < 0 {
        return Ok(build_admin_api_keys_bad_request_response(
            "rate_limit 必须大于等于 0",
        ));
    }

    let plaintext_key = generate_admin_user_api_key_plaintext();
    let Some(key_encrypted) = encrypt_catalog_secret_with_fallbacks(state, &plaintext_key) else {
        return Ok((
            http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": "API密钥加密失败" })),
        )
            .into_response());
    };

    let Some(created) = state
        .create_standalone_api_key(aether_data::repository::auth::CreateStandaloneApiKeyRecord {
            user_id: operator_id,
            api_key_id: uuid::Uuid::new_v4().to_string(),
            key_hash: hash_admin_user_api_key(&plaintext_key),
            key_encrypted: Some(key_encrypted),
            name: Some(name),
            allowed_providers,
            allowed_api_formats,
            allowed_models,
            rate_limit,
            concurrent_limit: 5,
        })
        .await?
    else {
        return Ok(build_admin_api_keys_maintenance_response());
    };

    Ok(Json(json!({
        "id": created.api_key_id,
        "key": plaintext_key,
        "name": created.name,
        "key_display": masked_user_api_key_display(state, created.key_encrypted.as_deref()),
        "is_standalone": true,
        "is_active": created.is_active,
        "rate_limit": created.rate_limit,
        "allowed_providers": created.allowed_providers,
        "allowed_api_formats": created.allowed_api_formats,
        "allowed_models": created.allowed_models,
        "expires_at": format_optional_unix_secs_iso8601(created.expires_at_unix_secs),
        "wallet": serde_json::Value::Null,
        "message": "独立余额Key创建成功，请妥善保存完整密钥，后续将无法查看",
    }))
    .into_response())
}

async fn build_admin_update_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_api_keys_maintenance_response());
    }

    let Some(api_key_id) = admin_api_keys_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_api_keys_maintenance_response());
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败"));
    };
    let raw_payload = match serde_json::from_slice::<serde_json::Value>(request_body) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败")),
    };
    let field_presence = AdminStandaloneApiKeyFieldPresence {
        allowed_providers: raw_payload.contains_key("allowed_providers"),
        allowed_api_formats: raw_payload.contains_key("allowed_api_formats"),
        allowed_models: raw_payload.contains_key("allowed_models"),
    };
    let payload = match serde_json::from_value::<AdminStandaloneApiKeyUpdateRequest>(
        serde_json::Value::Object(raw_payload),
    ) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败")),
    };
    if payload.initial_balance_usd.is_some()
        || payload.unlimited_balance.is_some()
        || payload.expire_days.is_some()
        || payload.expires_at.is_some()
        || payload.auto_delete_on_expiry.is_some()
    {
        return Ok(build_admin_api_keys_bad_request_response(
            "当前仅支持 name、rate_limit、allowed_providers、allowed_api_formats、allowed_models 字段",
        ));
    }

    let name = match normalize_admin_optional_api_key_name(payload.name) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
    };
    if payload.rate_limit.is_some_and(|value| value < 0) {
        return Ok(build_admin_api_keys_bad_request_response(
            "rate_limit 必须大于等于 0",
        ));
    }
    let allowed_providers = if field_presence.allowed_providers {
        match normalize_admin_user_string_list(payload.allowed_providers, "allowed_providers") {
            Ok(value) => Some(value),
            Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
        }
    } else {
        None
    };
    let allowed_api_formats = if field_presence.allowed_api_formats {
        match normalize_admin_user_api_formats(payload.allowed_api_formats) {
            Ok(value) => Some(value),
            Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
        }
    } else {
        None
    };
    let allowed_models = if field_presence.allowed_models {
        match normalize_admin_user_string_list(payload.allowed_models, "allowed_models") {
            Ok(value) => Some(value),
            Err(detail) => return Ok(build_admin_api_keys_bad_request_response(detail)),
        }
    } else {
        None
    };

    let Some(updated) = state
        .update_standalone_api_key_basic(
            aether_data::repository::auth::UpdateStandaloneApiKeyBasicRecord {
                api_key_id: api_key_id.clone(),
                name,
                rate_limit: payload.rate_limit,
                allowed_providers,
                allowed_api_formats,
                allowed_models,
            },
        )
        .await?
    else {
        return Ok(build_admin_api_keys_not_found_response());
    };

    let wallet = state
        .list_wallet_snapshots_by_api_key_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id.as_str()));
    let total_tokens_by_api_key_id =
        admin_api_key_total_tokens_by_ids(state, std::slice::from_ref(&api_key_id)).await?;
    let total_tokens = total_tokens_by_api_key_id
        .get(&api_key_id)
        .copied()
        .unwrap_or(0);
    let mut payload =
        build_admin_api_key_detail_payload(state, &updated, total_tokens, wallet.as_ref());
    payload["message"] = json!("API密钥已更新");
    Ok(Json(payload).into_response())
}

async fn build_admin_toggle_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_api_keys_maintenance_response());
    }

    let Some(api_key_id) = admin_api_keys_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_api_keys_maintenance_response());
    };

    let requested_active = match request_body {
        None => None,
        Some(request_body) if request_body.is_empty() => None,
        Some(request_body) => match serde_json::from_slice::<AdminStandaloneApiKeyToggleRequest>(
            request_body,
        ) {
            Ok(value) => value.is_active,
            Err(_) => return Ok(build_admin_api_keys_bad_request_response("请求数据验证失败")),
        },
    };

    let Some(snapshot) = state
        .read_auth_api_key_snapshots_by_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .find(|snapshot| snapshot.api_key_id == api_key_id)
    else {
        return Ok(build_admin_api_keys_not_found_response());
    };
    if !snapshot.api_key_is_standalone {
        return Ok(build_admin_api_keys_bad_request_response("仅支持独立密钥"));
    }

    let is_active = requested_active.unwrap_or(!snapshot.api_key_is_active);
    let Some(updated) = state
        .set_standalone_api_key_active(&api_key_id, is_active)
        .await?
    else {
        return Ok(build_admin_api_keys_not_found_response());
    };

    Ok(Json(json!({
        "id": updated.api_key_id,
        "is_active": updated.is_active,
        "message": if updated.is_active { "API密钥已启用" } else { "API密钥已禁用" },
    }))
    .into_response())
}

async fn build_admin_delete_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_api_keys_maintenance_response());
    }

    let Some(api_key_id) = admin_api_keys_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_api_keys_maintenance_response());
    };

    match state.delete_standalone_api_key(&api_key_id).await? {
        true => Ok(Json(json!({ "message": "API密钥已删除" })).into_response()),
        false => Ok(build_admin_api_keys_not_found_response()),
    }
}

async fn maybe_build_local_admin_api_keys_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("api_keys_manage") {
        return Ok(None);
    }

    let path = request_context.request_path.as_str();
    let is_api_keys_route = matches!(path, "/api/admin/api-keys" | "/api/admin/api-keys/")
        || (path.starts_with("/api/admin/api-keys/") && path.matches('/').count() == 4);

    if !is_api_keys_route {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("list_api_keys")
            if request_context.request_method == http::Method::GET
                && matches!(path, "/api/admin/api-keys" | "/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_list_api_keys_response(state, request_context).await?,
            ))
        }
        Some("api_key_detail")
            if request_context.request_method == http::Method::GET
                && path.starts_with("/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_api_key_detail_response(state, request_context).await?,
            ))
        }
        Some("create_api_key")
            if request_context.request_method == http::Method::POST
                && matches!(path, "/api/admin/api-keys" | "/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_create_api_key_response(state, request_context, request_body).await?,
            ))
        }
        Some("update_api_key")
            if request_context.request_method == http::Method::PUT
                && path.starts_with("/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_update_api_key_response(state, request_context, request_body).await?,
            ))
        }
        Some("toggle_api_key")
            if request_context.request_method == http::Method::PATCH
                && path.starts_with("/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_toggle_api_key_response(state, request_context, request_body).await?,
            ))
        }
        Some("delete_api_key")
            if request_context.request_method == http::Method::DELETE
                && path.starts_with("/api/admin/api-keys/") =>
        {
            Ok(Some(
                build_admin_delete_api_key_response(state, request_context).await?,
            ))
        }
        _ => Ok(Some(build_admin_api_keys_maintenance_response())),
    }
}
