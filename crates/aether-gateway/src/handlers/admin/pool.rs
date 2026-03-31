const ADMIN_POOL_RUST_BACKEND_DETAIL: &str = "Admin pool routes require Rust maintenance backend";
const ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL: &str =
    "Admin pool overview requires provider catalog reader";
const ADMIN_POOL_PROVIDER_CATALOG_WRITER_UNAVAILABLE_DETAIL: &str =
    "Admin pool cleanup requires provider catalog writer";
const ADMIN_POOL_BANNED_KEY_CLEANUP_EMPTY_MESSAGE: &str = "未发现可清理的异常账号";

#[derive(Debug, Default, serde::Deserialize)]
struct AdminPoolResolveSelectionRequest {
    #[serde(default)]
    search: String,
    #[serde(default)]
    quick_selectors: Vec<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminPoolBatchActionRequest {
    #[serde(default)]
    key_ids: Vec<String>,
    #[serde(default)]
    action: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminPoolBatchImportRequest {
    #[serde(default)]
    keys: Vec<AdminPoolBatchImportItem>,
    #[serde(default)]
    proxy_node_id: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminPoolBatchImportItem {
    #[serde(default)]
    name: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    auth_type: String,
}

fn build_admin_pool_error_response(
    status: http::StatusCode,
    detail: impl Into<String>,
) -> Response<Body> {
    (status, Json(json!({ "detail": detail.into() }))).into_response()
}

fn admin_pool_batch_delete_task_parts(request_path: &str) -> Option<(String, String)> {
    let raw = request_path.strip_prefix("/api/admin/pool/")?;
    let (provider_id, suffix) = raw.split_once("/keys/batch-delete-task/")?;
    let provider_id = provider_id.trim();
    let task_id = suffix.trim().trim_matches('/');
    if provider_id.is_empty() || provider_id.contains('/') || task_id.is_empty() || task_id.contains('/')
    {
        return None;
    }
    Some((provider_id.to_string(), task_id.to_string()))
}

fn admin_pool_key_proxy_value(proxy_node_id: Option<&str>) -> Option<serde_json::Value> {
    proxy_node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| json!({ "node_id": value, "enabled": true }))
}

fn build_admin_pool_batch_delete_task_payload(
    task: &LocalProviderDeleteTaskState,
) -> serde_json::Value {
    json!({
        "task_id": task.task_id,
        "provider_id": task.provider_id,
        "status": task.status,
        "stage": task.stage,
        "total_keys": task.total_keys,
        "deleted_keys": task.deleted_keys,
        "total_endpoints": task.total_endpoints,
        "deleted_endpoints": task.deleted_endpoints,
        "message": task.message,
    })
}

fn parse_admin_pool_page(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "page") {
        None => Ok(1),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "page must be an integer between 1 and 10000".to_string())?;
            if (1..=10_000).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("page must be an integer between 1 and 10000".to_string())
            }
        }
    }
}

fn parse_admin_pool_page_size(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "page_size") {
        None => Ok(50),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "page_size must be an integer between 1 and 200".to_string())?;
            if (1..=200).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("page_size must be an integer between 1 and 200".to_string())
            }
        }
    }
}

fn parse_admin_pool_search(query: Option<&str>) -> Option<String> {
    query_param_value(query, "search")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_admin_pool_status_filter(query: Option<&str>) -> Result<String, String> {
    let value = query_param_value(query, "status")
        .unwrap_or_else(|| "all".to_string())
        .trim()
        .to_ascii_lowercase();
    match value.as_str() {
        "all" | "active" | "inactive" | "cooldown" => Ok(value),
        _ => Err("status must be one of: all, active, cooldown, inactive".to_string()),
    }
}

fn admin_pool_api_formats(key: &StoredProviderCatalogKey) -> Vec<String> {
    key.api_formats
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn admin_pool_string_list(value: Option<&serde_json::Value>) -> Option<Vec<String>> {
    let values = value
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn admin_pool_json_object(
    value: Option<&serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    value
        .and_then(serde_json::Value::as_object)
        .cloned()
        .filter(|value| !value.is_empty())
}

fn admin_pool_resolved_api_formats(
    endpoints: &[StoredProviderCatalogEndpoint],
    existing_keys: &[StoredProviderCatalogKey],
) -> Vec<String> {
    let mut formats = Vec::new();
    let mut seen = BTreeSet::new();
    for endpoint in endpoints.iter().filter(|endpoint| endpoint.is_active) {
        let api_format = endpoint.api_format.trim();
        if api_format.is_empty() || !seen.insert(api_format.to_string()) {
            continue;
        }
        formats.push(api_format.to_string());
    }
    if !formats.is_empty() {
        return formats;
    }

    for key in existing_keys {
        for api_format in admin_pool_api_formats(key) {
            if !seen.insert(api_format.clone()) {
                continue;
            }
            formats.push(api_format);
        }
    }
    formats
}

fn build_admin_pool_key_payload(
    key: &StoredProviderCatalogKey,
    runtime: &AdminProviderPoolRuntimeState,
    pool_config: Option<AdminProviderPoolConfig>,
) -> serde_json::Value {
    let cooldown_reason = runtime.cooldown_reason_by_key.get(&key.id).cloned();
    let cooldown_ttl_seconds = cooldown_reason
        .as_ref()
        .and_then(|_| runtime.cooldown_ttl_by_key.get(&key.id).copied());
    let health_score = admin_pool_health_score(key);
    let circuit_breaker_open = admin_pool_circuit_breaker_open(key);
    let (scheduling_status, scheduling_reason, scheduling_label, scheduling_reasons) =
        admin_pool_scheduling_payload(
            key,
            cooldown_reason.as_deref(),
            cooldown_ttl_seconds,
            health_score,
            circuit_breaker_open,
        );

    json!({
        "key_id": key.id,
        "key_name": key.name,
        "is_active": key.is_active,
        "auth_type": key.auth_type,
        "status_snapshot": key.status_snapshot.clone().unwrap_or_else(|| json!({})),
        "health_score": health_score,
        "circuit_breaker_open": circuit_breaker_open,
        "api_formats": admin_pool_api_formats(key),
        "rate_multipliers": admin_pool_json_object(key.rate_multipliers.as_ref()),
        "internal_priority": key.internal_priority,
        "rpm_limit": key.rpm_limit,
        "cache_ttl_minutes": key.cache_ttl_minutes,
        "max_probe_interval_minutes": key.max_probe_interval_minutes,
        "note": key.note,
        "allowed_models": admin_pool_string_list(key.allowed_models.as_ref()),
        "capabilities": admin_pool_json_object(key.capabilities.as_ref()),
        "auto_fetch_models": key.auto_fetch_models,
        "locked_models": admin_pool_string_list(key.locked_models.as_ref()),
        "model_include_patterns": admin_pool_string_list(key.model_include_patterns.as_ref()),
        "model_exclude_patterns": admin_pool_string_list(key.model_exclude_patterns.as_ref()),
        "proxy": key.proxy.clone(),
        "fingerprint": key.fingerprint.clone(),
        "cooldown_reason": cooldown_reason,
        "cooldown_ttl_seconds": cooldown_ttl_seconds,
        "cost_window_usage": runtime.cost_window_usage_by_key.get(&key.id).copied().unwrap_or(0),
        "cost_limit": pool_config.map(|config| config.cost_limit_per_key_tokens),
        "request_count": key.request_count.unwrap_or(0),
        "total_tokens": 0,
        "total_cost_usd": "0.00000000",
        "sticky_sessions": runtime.sticky_sessions_by_key.get(&key.id).copied().unwrap_or(0),
        "lru_score": runtime.lru_score_by_key.get(&key.id).copied(),
        "created_at": key.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "last_used_at": key.last_used_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "scheduling_status": scheduling_status,
        "scheduling_reason": scheduling_reason,
        "scheduling_label": scheduling_label,
        "scheduling_reasons": scheduling_reasons,
    })
}

fn admin_pool_health_score(key: &StoredProviderCatalogKey) -> f64 {
    let scores = key
        .health_by_format
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .map(|formats| {
            formats
                .values()
                .filter_map(serde_json::Value::as_object)
                .filter_map(|item| item.get("health_score"))
                .filter_map(serde_json::Value::as_f64)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if scores.is_empty() {
        1.0
    } else {
        scores.into_iter().fold(1.0, f64::min)
    }
}

fn admin_pool_circuit_breaker_open(key: &StoredProviderCatalogKey) -> bool {
    key.circuit_breaker_by_format
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .map(|formats| {
            formats
                .values()
                .filter_map(serde_json::Value::as_object)
                .any(|item| {
                    item.get("open")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                })
        })
        .unwrap_or(false)
}

fn admin_pool_scheduling_payload(
    key: &StoredProviderCatalogKey,
    cooldown_reason: Option<&str>,
    cooldown_ttl_seconds: Option<u64>,
    health_score: f64,
    circuit_breaker_open: bool,
) -> (String, String, String, Vec<serde_json::Value>) {
    if !key.is_active {
        return (
            "blocked".to_string(),
            "inactive".to_string(),
            "已禁用".to_string(),
            vec![json!({
                "code": "inactive",
                "label": "已禁用",
                "blocking": true,
                "source": "manual",
                "ttl_seconds": serde_json::Value::Null,
                "detail": serde_json::Value::Null,
            })],
        );
    }
    if let Some(reason) = cooldown_reason {
        return (
            "degraded".to_string(),
            "cooldown".to_string(),
            "冷却中".to_string(),
            vec![json!({
                "code": "cooldown",
                "label": "冷却中",
                "blocking": true,
                "source": "pool",
                "ttl_seconds": cooldown_ttl_seconds,
                "detail": reason,
            })],
        );
    }
    if circuit_breaker_open {
        return (
            "degraded".to_string(),
            "circuit_breaker".to_string(),
            "熔断中".to_string(),
            vec![json!({
                "code": "circuit_breaker",
                "label": "熔断中",
                "blocking": true,
                "source": "health",
                "ttl_seconds": serde_json::Value::Null,
                "detail": serde_json::Value::Null,
            })],
        );
    }
    if health_score < 0.5 {
        return (
            "degraded".to_string(),
            "health_low".to_string(),
            "健康度较低".to_string(),
            vec![json!({
                "code": "health_low",
                "label": "健康度较低",
                "blocking": false,
                "source": "health",
                "ttl_seconds": serde_json::Value::Null,
                "detail": serde_json::Value::Null,
            })],
        );
    }
    (
        "available".to_string(),
        "available".to_string(),
        "可用".to_string(),
        Vec::new(),
    )
}

fn admin_pool_provider_id_from_path(request_path: &str) -> Option<String> {
    let raw = request_path.strip_prefix("/api/admin/pool/")?;
    let mut segments = raw.split('/');
    let provider_id = segments.next()?.trim();
    let keys_segment = segments.next()?.trim();
    if provider_id.is_empty() || keys_segment != "keys" {
        None
    } else {
        Some(provider_id.to_string())
    }
}

fn admin_pool_reason_indicates_ban(reason: &str) -> bool {
    let normalized = reason.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && [
            "banned",
            "forbidden",
            "blocked",
            "suspend",
            "deactivated",
            "disabled",
            "verification",
            "workspace",
            "受限",
            "封",
            "禁",
        ]
        .iter()
        .any(|hint| normalized.contains(hint))
}

fn admin_pool_normalize_text(value: impl AsRef<str>) -> String {
    value.as_ref().trim().to_ascii_lowercase()
}

fn admin_pool_parse_auth_config_json(
    state: &AppState,
    key: &StoredProviderCatalogKey,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let ciphertext = key.encrypted_auth_config.as_deref()?.trim();
    if ciphertext.is_empty() {
        return None;
    }
    let plaintext = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)?;
    serde_json::from_str::<serde_json::Value>(&plaintext)
        .ok()?
        .as_object()
        .cloned()
}

fn admin_pool_derive_oauth_plan_type(
    state: &AppState,
    key: &StoredProviderCatalogKey,
    provider_type: &str,
) -> Option<String> {
    let normalize = |value: &str| {
        let mut text = value.trim().to_string();
        if text.is_empty() {
            return None;
        }
        let provider_type = provider_type.trim().to_ascii_lowercase();
        if !provider_type.is_empty() && text.to_ascii_lowercase().starts_with(&provider_type) {
            text = text[provider_type.len()..]
                .trim_matches(|ch: char| [' ', ':', '-', '_'].contains(&ch))
                .to_string();
        }
        if text.is_empty() { None } else { Some(text.to_ascii_lowercase()) }
    };

    if key.auth_type.trim() != "oauth" {
        return None;
    }

    if let Some(auth_config) = admin_pool_parse_auth_config_json(state, key) {
        for plan_key in ["plan_type", "tier", "plan", "subscription_plan"] {
            if let Some(value) = auth_config.get(plan_key).and_then(serde_json::Value::as_str) {
                if let Some(normalized) = normalize(value) {
                    return Some(normalized);
                }
            }
        }
    }

    let upstream_metadata = key.upstream_metadata.as_ref()?.as_object()?;
    let provider_bucket = upstream_metadata
        .get(&provider_type.trim().to_ascii_lowercase())
        .and_then(serde_json::Value::as_object);
    for source in provider_bucket.into_iter().chain(std::iter::once(upstream_metadata)) {
        for plan_key in ["plan_type", "tier", "subscription_title", "subscription_plan"] {
            if let Some(value) = source.get(plan_key).and_then(serde_json::Value::as_str) {
                if let Some(normalized) = normalize(value) {
                    return Some(normalized);
                }
            }
        }
    }

    None
}

fn admin_pool_has_proxy(key: &StoredProviderCatalogKey) -> bool {
    match key.proxy.as_ref() {
        Some(serde_json::Value::Object(values)) => !values.is_empty(),
        Some(serde_json::Value::String(value)) => !value.trim().is_empty(),
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::Number(_)) => true,
        Some(serde_json::Value::Array(values)) => !values.is_empty(),
        _ => false,
    }
}

fn admin_pool_is_oauth_invalid(key: &StoredProviderCatalogKey) -> bool {
    if key.auth_type.trim() != "oauth" {
        return false;
    }
    if key
        .oauth_invalid_reason
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return true;
    }
    key.expires_at_unix_secs
        .is_some_and(|value| value > 0 && value <= chrono::Utc::now().timestamp().max(0) as u64)
}

fn admin_pool_matches_quick_selector(
    state: &AppState,
    key: &StoredProviderCatalogKey,
    provider_type: &str,
    selector: &str,
) -> bool {
    match selector {
        "banned" => admin_pool_key_is_known_banned(key),
        "oauth_invalid" => admin_pool_is_oauth_invalid(key),
        "proxy_unset" => !admin_pool_has_proxy(key),
        "proxy_set" => admin_pool_has_proxy(key),
        "disabled" => !key.is_active,
        "enabled" => key.is_active,
        "plan_free" => admin_pool_derive_oauth_plan_type(state, key, provider_type)
            .is_some_and(|value| value.contains("free")),
        "plan_team" => admin_pool_derive_oauth_plan_type(state, key, provider_type)
            .is_some_and(|value| value.contains("team")),
        "no_5h_limit" | "no_weekly_limit" => false,
        _ => false,
    }
}

fn admin_pool_matches_search(
    state: &AppState,
    key: &StoredProviderCatalogKey,
    provider_type: &str,
    search: Option<&str>,
) -> bool {
    let Some(search) = search else {
        return true;
    };
    let search = admin_pool_normalize_text(search);
    if search.is_empty() {
        return true;
    }

    let oauth_plan_type = admin_pool_derive_oauth_plan_type(state, key, provider_type);
    let mut search_fields = vec![
        key.id.clone(),
        key.name.clone(),
        key.auth_type.clone(),
        if key.is_active {
            "已启用".to_string()
        } else {
            "已禁用".to_string()
        },
        if admin_pool_has_proxy(key) {
            "独立代理".to_string()
        } else {
            "未配置代理".to_string()
        },
    ];
    if let Some(reason) = key.oauth_invalid_reason.as_ref() {
        search_fields.push(reason.clone());
    }
    if let Some(note) = key.note.as_ref() {
        search_fields.push(note.clone());
    }
    if let Some(plan_type) = oauth_plan_type {
        search_fields.push(plan_type);
    }

    search_fields
        .into_iter()
        .any(|value| admin_pool_normalize_text(&value).contains(&search))
}

fn admin_pool_key_is_known_banned(key: &StoredProviderCatalogKey) -> bool {
    if key
        .oauth_invalid_reason
        .as_deref()
        .is_some_and(admin_pool_reason_indicates_ban)
    {
        return true;
    }

    let Some(account) = key
        .status_snapshot
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|snapshot| snapshot.get("account"))
        .and_then(serde_json::Value::as_object)
    else {
        return false;
    };

    if !account
        .get("blocked")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }

    account
        .get("code")
        .and_then(serde_json::Value::as_str)
        .is_some_and(admin_pool_reason_indicates_ban)
        || account
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .is_some_and(admin_pool_reason_indicates_ban)
}

fn admin_pool_sort_keys(keys: &mut [StoredProviderCatalogKey]) {
    keys.sort_by(|left, right| {
        left.internal_priority
            .cmp(&right.internal_priority)
            .then(left.name.cmp(&right.name))
            .then(left.id.cmp(&right.id))
    });
}

async fn build_admin_pool_overview_payload(state: &AppState) -> Result<serde_json::Value, GatewayError> {
    let providers = state.list_provider_catalog_providers(false).await?;
    let pool_enabled_providers = providers
        .into_iter()
        .filter_map(|provider| admin_provider_pool_config(&provider).map(|config| (provider, config)))
        .collect::<Vec<_>>();
    let provider_ids = pool_enabled_providers
        .iter()
        .map(|(provider, _)| provider.id.clone())
        .collect::<Vec<_>>();
    let key_stats = if provider_ids.is_empty() {
        Vec::new()
    } else {
        state
            .list_provider_catalog_key_stats_by_provider_ids(&provider_ids)
            .await?
    };
    let key_stats_by_provider = key_stats
        .into_iter()
        .map(|item| (item.provider_id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let redis_runner = state.redis_kv_runner();

    let mut items = Vec::with_capacity(pool_enabled_providers.len());
    for (provider, _pool_config) in pool_enabled_providers {
        let stats = key_stats_by_provider.get(&provider.id);
        let total_keys = stats.map(|item| item.total_keys as usize).unwrap_or(0);
        let active_keys = stats.map(|item| item.active_keys as usize).unwrap_or(0);
        let cooldown_count = if let Some(runner) = redis_runner.as_ref() {
            read_admin_provider_pool_cooldown_count(runner, &provider.id).await
        } else {
            0
        };

        items.push(json!({
            "provider_id": provider.id,
            "provider_name": provider.name,
            "provider_type": provider.provider_type,
            "total_keys": total_keys,
            "active_keys": active_keys,
            "cooldown_count": cooldown_count,
            "pool_enabled": true,
        }));
    }

    Ok(json!({ "items": items }))
}

async fn build_admin_pool_cleanup_banned_keys_response(
    state: &AppState,
    provider_id: String,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }
    if !state.has_provider_catalog_data_writer() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_WRITER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let banned_keys = state
        .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
        .await?
        .into_iter()
        .filter(admin_pool_key_is_known_banned)
        .collect::<Vec<_>>();
    if banned_keys.is_empty() {
        return Ok(
            Json(json!({
                "affected": 0,
                "message": ADMIN_POOL_BANNED_KEY_CLEANUP_EMPTY_MESSAGE,
            }))
            .into_response(),
        );
    }

    let deleted_key_ids = banned_keys
        .iter()
        .map(|key| key.id.clone())
        .collect::<Vec<_>>();
    for key in &banned_keys {
        clear_admin_provider_pool_cooldown(state, &provider.id, &key.id).await;
        reset_admin_provider_pool_cost(state, &provider.id, &key.id).await;
    }

    let mut affected = 0usize;
    for key_id in &deleted_key_ids {
        if state.delete_provider_catalog_key(key_id).await? {
            affected += 1;
        }
    }
    state
        .cleanup_deleted_provider_catalog_refs(&provider.id, &[], &deleted_key_ids)
        .await?;

    Ok(Json(json!({
        "affected": affected,
        "message": format!("已清理 {affected} 个异常账号"),
    }))
    .into_response())
}

async fn build_admin_pool_list_keys_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider_id) = admin_pool_provider_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "provider_id 无效",
        ));
    };
    let query = request_context.request_query_string.as_deref();
    let page = match parse_admin_pool_page(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_pool_error_response(http::StatusCode::BAD_REQUEST, detail)),
    };
    let page_size = match parse_admin_pool_page_size(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_pool_error_response(http::StatusCode::BAD_REQUEST, detail)),
    };
    let search = parse_admin_pool_search(query).map(|value| value.to_ascii_lowercase());
    let status = match parse_admin_pool_status_filter(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_pool_error_response(http::StatusCode::BAD_REQUEST, detail)),
    };

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let pool_config = admin_provider_pool_config(&provider);
    let page_offset = page.saturating_sub(1).saturating_mul(page_size);

    let (keys, total) = if status == "cooldown" {
        let cooldown_key_ids = if let Some(runner) = state.redis_kv_runner() {
            read_admin_provider_pool_cooldown_key_ids(&runner, &provider.id).await
        } else {
            Vec::new()
        };
        let mut keys = if cooldown_key_ids.is_empty() {
            Vec::new()
        } else {
            state.list_provider_catalog_keys_by_ids(&cooldown_key_ids).await?
        };
        if let Some(keyword) = search.as_ref() {
            keys.retain(|key| {
                key.name.to_ascii_lowercase().contains(keyword)
                    || key.id.to_ascii_lowercase().contains(keyword)
            });
        }
        admin_pool_sort_keys(&mut keys);
        let total = keys.len();
        let keys = keys
            .into_iter()
            .skip(page_offset)
            .take(page_size)
            .collect::<Vec<_>>();
        (keys, total)
    } else {
        let key_page = state
            .list_provider_catalog_key_page(&aether_data::repository::provider_catalog::ProviderCatalogKeyListQuery {
                provider_id: provider.id.clone(),
                search: search.clone(),
                is_active: match status.as_str() {
                    "active" => Some(true),
                    "inactive" => Some(false),
                    _ => None,
                },
                offset: page_offset,
                limit: page_size,
            })
            .await?;
        (key_page.items, key_page.total)
    };

    let key_ids = keys.iter().map(|key| key.id.clone()).collect::<Vec<_>>();
    let runtime = match (state.redis_kv_runner(), pool_config) {
        (Some(runner), Some(pool_config)) if !key_ids.is_empty() => {
            read_admin_provider_pool_runtime_state(&runner, &provider.id, &key_ids, pool_config)
                .await
        }
        _ => AdminProviderPoolRuntimeState::default(),
    };

    let items = keys
        .into_iter()
        .map(|key| build_admin_pool_key_payload(&key, &runtime, pool_config))
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "total": total,
        "page": page,
        "page_size": page_size,
        "keys": items,
    }))
    .into_response())
}

async fn build_admin_pool_resolve_selection_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider_id) = admin_pool_provider_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "provider_id 无效",
        ));
    };

    let payload = match request_body {
        None => AdminPoolResolveSelectionRequest::default(),
        Some(body) if body.is_empty() => AdminPoolResolveSelectionRequest::default(),
        Some(body) => match serde_json::from_slice::<AdminPoolResolveSelectionRequest>(body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(build_admin_pool_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "Invalid JSON request body",
                ))
            }
        },
    };

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let provider_type = provider.provider_type.clone();
    let search = payload.search.trim();
    let mut quick_selectors = payload
        .quick_selectors
        .into_iter()
        .map(|value| admin_pool_normalize_text(value))
        .filter(|value| {
            matches!(
                value.as_str(),
                "banned"
                    | "no_5h_limit"
                    | "no_weekly_limit"
                    | "plan_free"
                    | "plan_team"
                    | "oauth_invalid"
                    | "proxy_unset"
                    | "proxy_set"
                    | "disabled"
                    | "enabled"
            )
        })
        .collect::<Vec<_>>();
    quick_selectors.sort();
    quick_selectors.dedup();

    let mut keys = state
        .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
        .await?
        .into_iter()
        .filter(|key| admin_pool_matches_search(state, key, &provider_type, Some(search)))
        .filter(|key| {
            quick_selectors.is_empty()
                || quick_selectors.iter().all(|selector| {
                    admin_pool_matches_quick_selector(state, key, &provider_type, selector)
                })
        })
        .collect::<Vec<_>>();

    keys.sort_by(|left, right| {
        left.internal_priority
            .cmp(&right.internal_priority)
            .then_with(|| left.name.cmp(&right.name))
    });

    let items = keys
        .iter()
        .map(|key| {
            json!({
                "key_id": key.id,
                "key_name": key.name,
                "auth_type": key.auth_type,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "total": items.len(),
        "items": items,
    }))
    .into_response())
}

async fn build_admin_pool_batch_import_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }
    if !state.has_provider_catalog_data_writer() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_WRITER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider_id) = admin_pool_provider_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "provider_id 无效",
        ));
    };
    let payload = match request_body {
        Some(body) if !body.is_empty() => {
            match serde_json::from_slice::<AdminPoolBatchImportRequest>(body) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(build_admin_pool_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "Invalid JSON request body",
                    ));
                }
            }
        }
        _ => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                "Invalid JSON request body",
            ));
        }
    };

    if payload.keys.len() > 500 {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "keys length must be less than or equal to 500",
        ));
    }

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let endpoints = state
        .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider.id))
        .await?;
    let existing_keys = state
        .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
        .await?;
    let api_formats = admin_pool_resolved_api_formats(&endpoints, &existing_keys);
    if api_formats.is_empty() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "Provider 没有可用 endpoint 或现有 key，无法推断 api_formats",
        ));
    }

    let proxy = admin_pool_key_proxy_value(payload.proxy_node_id.as_deref());
    let mut imported = 0usize;
    let skipped = 0usize;
    let mut errors = Vec::new();
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    for (index, item) in payload.keys.iter().enumerate() {
        let api_key = item.api_key.trim();
        if api_key.is_empty() {
            errors.push(json!({
                "index": index,
                "reason": "api_key is empty",
            }));
            continue;
        }

        let Some(encrypted_api_key) = encrypt_catalog_secret_with_fallbacks(state, api_key) else {
            errors.push(json!({
                "index": index,
                "reason": "gateway 未配置 provider key 加密密钥",
            }));
            continue;
        };

        let auth_type = item.auth_type.trim().to_ascii_lowercase();
        let auth_type = if auth_type.is_empty() {
            "api_key".to_string()
        } else {
            auth_type
        };
        let name = item.name.trim();
        let mut record = match StoredProviderCatalogKey::new(
            Uuid::new_v4().to_string(),
            provider.id.clone(),
            if name.is_empty() {
                format!("imported-{index}")
            } else {
                name.to_string()
            },
            auth_type,
            None,
            true,
        ) {
            Ok(value) => value,
            Err(err) => {
                errors.push(json!({
                    "index": index,
                    "reason": err.to_string(),
                }));
                continue;
            }
        };
        record = match record.with_transport_fields(
            Some(json!(api_formats)),
            encrypted_api_key,
            None,
            None,
            None,
            None,
            None,
            proxy.clone(),
            None,
        ) {
            Ok(value) => value,
            Err(err) => {
                errors.push(json!({
                    "index": index,
                    "reason": err.to_string(),
                }));
                continue;
            }
        };
        record.request_count = Some(0);
        record.success_count = Some(0);
        record.error_count = Some(0);
        record.total_response_time_ms = Some(0);
        record.health_by_format = Some(json!({}));
        record.circuit_breaker_by_format = Some(json!({}));
        record.created_at_unix_secs = Some(now_unix_secs);
        record.updated_at_unix_secs = Some(now_unix_secs);

        let Some(_) = state.create_provider_catalog_key(&record).await? else {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                ADMIN_POOL_PROVIDER_CATALOG_WRITER_UNAVAILABLE_DETAIL,
            ));
        };
        imported += 1;
    }

    Ok(Json(json!({
        "imported": imported,
        "skipped": skipped,
        "errors": errors,
    }))
    .into_response())
}

async fn build_admin_pool_batch_action_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }
    if !state.has_provider_catalog_data_writer() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_WRITER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider_id) = admin_pool_provider_id_from_path(&request_context.request_path) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            "Provider 不存在",
        ));
    };
    let payload = match request_body {
        Some(body) if !body.is_empty() => match serde_json::from_slice::<AdminPoolBatchActionRequest>(body)
        {
            Ok(value) => value,
            Err(_) => {
                return Ok(build_admin_pool_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "Invalid JSON request body",
                ))
            }
        },
        _ => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                "Invalid JSON request body",
            ))
        }
    };

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let action = payload.action.trim().to_ascii_lowercase();
    let action_label = match action.as_str() {
        "enable" => "enabled",
        "disable" => "disabled",
        "clear_proxy" => "proxy cleared",
        "set_proxy" => "proxy set",
        "delete" => "deleted",
        _ => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                format!(
                    "Invalid action: {action}. Supported locally: enable, disable, clear_proxy, set_proxy, delete"
                ),
            ))
        }
    };

    let key_ids = payload
        .key_ids
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if key_ids.is_empty() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "key_ids should not be empty",
        ));
    }

    let proxy_payload = if action == "set_proxy" {
        match payload.payload {
            Some(serde_json::Value::Object(map)) if !map.is_empty() => {
                Some(serde_json::Value::Object(map))
            }
            _ => {
                return Ok(build_admin_pool_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "set_proxy action requires a non-empty payload with proxy config",
                ))
            }
        }
    } else {
        None
    };

    let keys = state
        .read_provider_catalog_keys_by_ids(&key_ids)
        .await?
        .into_iter()
        .filter(|key| key.provider_id == provider.id)
        .collect::<Vec<_>>();

    if action == "delete" {
        let deleted_key_ids = keys.iter().map(|key| key.id.clone()).collect::<Vec<_>>();
        for key in &keys {
            clear_admin_provider_pool_cooldown(state, &provider.id, &key.id).await;
            reset_admin_provider_pool_cost(state, &provider.id, &key.id).await;
        }

        let mut affected = 0usize;
        for key_id in &deleted_key_ids {
            if state.delete_provider_catalog_key(key_id).await? {
                affected = affected.saturating_add(1);
            }
        }
        state
            .cleanup_deleted_provider_catalog_refs(&provider.id, &[], &deleted_key_ids)
            .await?;

        return Ok(Json(json!({
            "affected": affected,
            "message": format!("{affected} keys {action_label}"),
        }))
        .into_response());
    }

    let mut affected = 0usize;
    for mut key in keys {
        match action.as_str() {
            "enable" => key.is_active = true,
            "disable" => key.is_active = false,
            "clear_proxy" => key.proxy = None,
            "set_proxy" => key.proxy = proxy_payload.clone(),
            _ => unreachable!(),
        }
        if state.update_provider_catalog_key(&key).await?.is_some() {
            affected = affected.saturating_add(1);
        }
    }

    Ok(Json(json!({
        "affected": affected,
        "message": format!("{affected} keys {action_label}"),
    }))
    .into_response())
}

async fn build_admin_pool_batch_delete_task_status_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((provider_id, task_id)) =
        admin_pool_batch_delete_task_parts(&request_context.request_path)
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            "批量删除任务不存在",
        ));
    };
    let Some(task) = state.get_provider_delete_task(&task_id) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            "批量删除任务不存在",
        ));
    };
    if task.provider_id != provider_id {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::NOT_FOUND,
                "批量删除任务不存在",
            ));
        }

    Ok(Json(build_admin_pool_batch_delete_task_payload(&task)).into_response())
}

fn build_admin_pool_scheduling_presets_payload() -> serde_json::Value {
    json!([
        {
            "name": "lru",
            "label": "LRU 轮转",
            "description": "最久未使用的 Key 优先",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": "distribution_mode",
            "evidence_hint": "依据 LRU 时间戳（最近未使用优先）",
        },
        {
            "name": "cache_affinity",
            "label": "缓存亲和",
            "description": "优先复用最近使用过的 Key，利用 Prompt Caching",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": "distribution_mode",
            "evidence_hint": "依据 LRU 时间戳（最近使用优先，与 LRU 轮转相反）",
        },
        {
            "name": "cost_first",
            "label": "成本优先",
            "description": "优先选择窗口消耗更低的账号",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据窗口成本/Token 用量，缺失时回退配额使用率",
        },
        {
            "name": "free_first",
            "label": "Free 优先",
            "description": "优先消耗 Free 账号（依赖 plan_type）",
            "providers": ["codex", "kiro"],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据 plan_type（Free 账号优先调度）",
        },
        {
            "name": "health_first",
            "label": "健康优先",
            "description": "优先选择健康分更高、失败更少的账号",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据 health_by_format 聚合分（含熔断/失败衰减）",
        },
        {
            "name": "latency_first",
            "label": "延迟优先",
            "description": "优先选择最近延迟更低的账号",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据号池延迟窗口均值（latency_window_seconds）",
        },
        {
            "name": "load_balance",
            "label": "负载均衡",
            "description": "随机分散 Key 使用，均匀分摊负载",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": "distribution_mode",
            "evidence_hint": "每次随机分值，实现完全均匀分散",
        },
        {
            "name": "plus_first",
            "label": "Plus 优先",
            "description": "优先消耗 Plus/Pro 账号（依赖 plan_type）",
            "providers": ["codex", "kiro"],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据 plan_type（Plus/Pro 账号优先调度）",
        },
        {
            "name": "priority_first",
            "label": "优先级优先",
            "description": "按账号优先级顺序调度（数字越小越优先）",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据 internal_priority（支持拖拽/手工编辑）",
        },
        {
            "name": "quota_balanced",
            "label": "额度平均",
            "description": "优先选额度消耗最少的账号",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据账号配额使用率；无配额时回退到窗口成本使用",
        },
        {
            "name": "recent_refresh",
            "label": "额度刷新优先",
            "description": "优先选即将刷新额度的账号",
            "providers": ["codex", "kiro"],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据账号额度重置倒计时（next_reset / reset_seconds）",
        },
        {
            "name": "single_account",
            "label": "单号优先",
            "description": "集中使用同一账号（反向 LRU）",
            "providers": [],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": "distribution_mode",
            "evidence_hint": "先按账号优先级（internal_priority），同级再按反向 LRU 集中",
        },
        {
            "name": "team_first",
            "label": "Team 优先",
            "description": "优先消耗 Team 账号（依赖 plan_type）",
            "providers": ["codex", "kiro"],
            "modes": serde_json::Value::Null,
            "default_mode": serde_json::Value::Null,
            "mutex_group": serde_json::Value::Null,
            "evidence_hint": "依据 plan_type（Team 账号优先调度）",
        }
    ])
}

fn is_admin_pool_route(request_context: &GatewayPublicRequestContext) -> bool {
    let normalized_path = request_context.request_path.trim_end_matches('/');
    let path = if normalized_path.is_empty() {
        request_context.request_path.as_str()
    } else {
        normalized_path
    };

    (request_context.request_method == http::Method::GET
        && path == "/api/admin/pool/overview")
        || (request_context.request_method == http::Method::GET
            && path == "/api/admin/pool/scheduling-presets")
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/pool/")
            && path.ends_with("/keys")
            && path.matches('/').count() == 5)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/pool/")
            && path.ends_with("/keys/batch-import")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/pool/")
            && path.ends_with("/keys/batch-action")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/pool/")
            && path.ends_with("/keys/resolve-selection")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/pool/")
            && path.contains("/keys/batch-delete-task/")
            && path.matches('/').count() == 7)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/pool/")
            && path.ends_with("/keys/cleanup-banned")
            && path.matches('/').count() == 6)
}

async fn maybe_build_local_admin_pool_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("pool_manage") {
        return Ok(None);
    }

    if !is_admin_pool_route(request_context) {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("overview")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.trim_end_matches('/'),
                    "/api/admin/pool/overview"
                ) =>
        {
            if !state.has_provider_catalog_data_reader() {
                return Ok(Some(build_admin_pool_error_response(
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
                )));
            }
            return Ok(Some(
                Json(build_admin_pool_overview_payload(state).await?).into_response(),
            ));
        }
        Some("scheduling_presets")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.trim_end_matches('/'),
                    "/api/admin/pool/scheduling-presets"
                ) =>
        {
            return Ok(Some(Json(build_admin_pool_scheduling_presets_payload()).into_response()));
        }
        Some("cleanup_banned_keys") if request_context.request_method == http::Method::POST => {
            let Some(provider_id) = admin_pool_provider_id_from_path(&request_context.request_path)
            else {
                return Ok(Some(build_admin_pool_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "provider_id 无效",
                )));
            };
            return Ok(Some(
                build_admin_pool_cleanup_banned_keys_response(state, provider_id).await?,
            ));
        }
        Some("list_keys") => {
            return Ok(Some(
                build_admin_pool_list_keys_response(state, request_context).await?,
            ));
        }
        Some("batch_import_keys") => {
            return Ok(Some(
                build_admin_pool_batch_import_response(state, request_context, request_body)
                    .await?,
            ));
        }
        Some("batch_action_keys") => {
            return Ok(Some(
                build_admin_pool_batch_action_response(state, request_context, request_body)
                    .await?,
            ));
        }
        Some("resolve_selection") => {
            return Ok(Some(
                build_admin_pool_resolve_selection_response(state, request_context, request_body)
                    .await?,
            ));
        }
        Some("batch_delete_task_status") => {
            return Ok(Some(
                build_admin_pool_batch_delete_task_status_response(state, request_context).await?,
            ));
        }
        _ => {}
    }

    Ok(Some(build_admin_pool_error_response(
        http::StatusCode::NOT_FOUND,
        format!(
            "Unsupported admin pool route {} {}",
            request_context.request_method, request_context.request_path
        ),
    )))
}
