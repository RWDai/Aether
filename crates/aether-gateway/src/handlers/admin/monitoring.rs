use super::*;

const ADMIN_MONITORING_RUST_BACKEND_DETAIL: &str =
    "Admin monitoring routes require Rust maintenance backend";
const ADMIN_MONITORING_CACHE_AFFINITY_REDIS_REQUIRED_DETAIL: &str =
    "Redis未初始化，无法获取缓存亲和性";
const ADMIN_MONITORING_REDIS_REQUIRED_DETAIL: &str = "Redis 未启用";
const ADMIN_MONITORING_CACHE_AFFINITY_DEFAULT_TTL_SECS: u64 = 300;
const ADMIN_MONITORING_CACHE_RESERVATION_RATIO: f64 = 0.1;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_PROBE_PHASE_REQUESTS: u64 = 100;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_PROBE_RESERVATION: f64 = 0.1;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_STABLE_MIN_RESERVATION: f64 = 0.1;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_STABLE_MAX_RESERVATION: f64 = 0.35;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_LOW_LOAD_THRESHOLD: f64 = 0.5;
const ADMIN_MONITORING_DYNAMIC_RESERVATION_HIGH_LOAD_THRESHOLD: f64 = 0.8;
const ADMIN_MONITORING_REDIS_CACHE_CATEGORIES: &[(&str, &str, &str, &str)] = &[
    (
        "upstream_models",
        "上游模型",
        "upstream_models:*",
        "Provider 上游获取的模型列表缓存",
    ),
    ("model_id", "模型 ID", "model:id:*", "Model 按 ID 缓存"),
    (
        "model_provider_global",
        "模型映射",
        "model:provider_global:*",
        "Provider-GlobalModel 模型映射缓存",
    ),
    (
        "provider_mapping_preview",
        "映射预览",
        "admin:providers:mapping-preview:*",
        "Provider 详情页 mapping-preview 缓存",
    ),
    (
        "global_model",
        "全局模型",
        "global_model:*",
        "GlobalModel 缓存（ID/名称/解析）",
    ),
    (
        "models_list",
        "模型列表",
        "models:list:*",
        "/v1/models 端点模型列表缓存",
    ),
    ("user", "用户", "user:*", "用户信息缓存（ID/Email）"),
    (
        "apikey",
        "API Key",
        "apikey:*",
        "API Key 认证缓存（Hash/Auth）",
    ),
    (
        "api_key_id",
        "API Key ID",
        "api_key:id:*",
        "API Key 按 ID 缓存",
    ),
    (
        "cache_affinity",
        "缓存亲和性",
        "cache_affinity:*",
        "请求路由亲和性缓存",
    ),
    (
        "provider_billing",
        "Provider 计费",
        "provider:billing_type:*",
        "Provider 计费类型缓存",
    ),
    (
        "provider_rate",
        "Provider 费率",
        "provider_api_key:rate_multiplier:*",
        "ProviderAPIKey 费率倍数缓存",
    ),
    (
        "provider_balance",
        "Provider 余额",
        "provider_ops:balance:*",
        "Provider 余额查询缓存",
    ),
    ("health", "健康检查", "health:*", "端点健康状态缓存"),
    (
        "endpoint_status",
        "端点状态",
        "endpoint_status:*",
        "用户端点状态缓存",
    ),
    ("dashboard", "仪表盘", "dashboard:*", "仪表盘统计缓存"),
    (
        "activity_heatmap",
        "活动热力图",
        "activity_heatmap:*",
        "用户活动热力图缓存",
    ),
    (
        "gemini_files",
        "Gemini 文件映射",
        "gemini_files:*",
        "Gemini Files API 文件-Key 映射缓存",
    ),
    (
        "provider_oauth",
        "OAuth 状态",
        "provider_oauth_state:*",
        "Provider OAuth 授权流程临时状态",
    ),
    (
        "oauth_refresh_lock",
        "OAuth 刷新锁",
        "provider_oauth_refresh_lock:*",
        "OAuth Token 刷新分布式锁",
    ),
    (
        "concurrency_lock",
        "并发锁",
        "concurrency:*",
        "请求并发控制锁",
    ),
];

struct AdminMonitoringCacheSnapshot {
    scheduler_name: String,
    scheduling_mode: String,
    provider_priority_mode: String,
    storage_type: &'static str,
    total_affinities: usize,
    cache_hits: usize,
    cache_misses: usize,
    cache_hit_rate: f64,
    provider_switches: usize,
    key_switches: usize,
    cache_invalidations: usize,
}

struct AdminMonitoringResilienceSnapshot {
    timestamp: chrono::DateTime<chrono::Utc>,
    health_score: i64,
    status: &'static str,
    error_statistics: serde_json::Value,
    recent_errors: Vec<serde_json::Value>,
    recommendations: Vec<String>,
    previous_stats: serde_json::Value,
}

#[derive(Debug, Clone)]
struct AdminMonitoringCacheAffinityRecord {
    raw_key: String,
    affinity_key: String,
    api_format: String,
    model_name: String,
    provider_id: Option<String>,
    endpoint_id: Option<String>,
    key_id: Option<String>,
    created_at: Option<serde_json::Value>,
    expire_at: Option<serde_json::Value>,
    request_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdminMonitoringRoute {
    AuditLogs,
    SystemStatus,
    SuspiciousActivities,
    UserBehavior,
    ResilienceStatus,
    ResilienceErrorStats,
    ResilienceCircuitHistory,
    TraceRequest,
    TraceProviderStats,
    CacheStats,
    CacheAffinity,
    CacheAffinities,
    CacheUsersDelete,
    CacheAffinityDelete,
    CacheFlush,
    CacheProviderDelete,
    CacheConfig,
    CacheMetrics,
    CacheModelMappingStats,
    CacheModelMappingDelete,
    CacheModelMappingDeleteModel,
    CacheModelMappingDeleteProvider,
    CacheRedisKeys,
    CacheRedisKeysDelete,
}

fn admin_monitoring_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_MONITORING_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn admin_monitoring_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn admin_monitoring_not_found_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn parse_admin_monitoring_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        None => Ok(0),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
    }
}

fn parse_admin_monitoring_days(query: Option<&str>) -> Result<i64, String> {
    match query_param_value(query, "days") {
        None => Ok(7),
        Some(value) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| "days must be an integer between 1 and 365".to_string())?;
            if (1..=365).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("days must be an integer between 1 and 365".to_string())
            }
        }
    }
}

fn parse_admin_monitoring_hours(query: Option<&str>) -> Result<i64, String> {
    match query_param_value(query, "hours") {
        None => Ok(24),
        Some(value) => {
            let parsed = value
                .parse::<i64>()
                .map_err(|_| "hours must be an integer between 1 and 168".to_string())?;
            if (1..=168).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("hours must be an integer between 1 and 168".to_string())
            }
        }
    }
}

fn parse_admin_monitoring_username_filter(query: Option<&str>) -> Option<String> {
    query_param_value(query, "username")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_admin_monitoring_event_type_filter(query: Option<&str>) -> Option<String> {
    query_param_value(query, "event_type")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn admin_monitoring_escape_like_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '%' => escaped.push_str("\\%"),
            '_' => escaped.push_str("\\_"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn build_admin_monitoring_audit_logs_payload(
    items: Vec<serde_json::Value>,
    total: usize,
    limit: usize,
    offset: usize,
    username: Option<String>,
    event_type: Option<String>,
    days: i64,
) -> Response<Body> {
    let count = items.len();
    Json(json!({
        "items": items,
        "meta": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": count,
        },
        "filters": {
            "username": username,
            "event_type": event_type,
            "days": days,
        },
    }))
    .into_response()
}

fn build_admin_monitoring_suspicious_activities_payload(
    activities: Vec<serde_json::Value>,
    hours: i64,
) -> Response<Body> {
    let count = activities.len();
    Json(json!({
        "activities": activities,
        "count": count,
        "time_range_hours": hours,
    }))
    .into_response()
}

fn build_admin_monitoring_user_behavior_payload(
    user_id: String,
    days: i64,
    event_counts: std::collections::BTreeMap<String, u64>,
    failed_requests: u64,
    success_requests: u64,
    suspicious_activities: u64,
) -> Response<Body> {
    let total_requests = success_requests.saturating_add(failed_requests);
    let success_rate = if total_requests == 0 {
        0.0
    } else {
        success_requests as f64 / total_requests as f64
    };

    Json(json!({
        "user_id": user_id,
        "period_days": days,
        "event_counts": event_counts,
        "failed_requests": failed_requests,
        "success_requests": success_requests,
        "success_rate": success_rate,
        "suspicious_activities": suspicious_activities,
        "analysis_time": chrono::Utc::now().to_rfc3339(),
    }))
    .into_response()
}

async fn count_admin_monitoring_cache_affinity_entries(state: &AppState) -> usize {
    let Some(runner) = state.redis_kv_runner() else {
        return 0;
    };
    let mut connection = match runner.client().get_multiplexed_async_connection().await {
        Ok(value) => value,
        Err(_) => return 0,
    };
    let pattern = runner.keyspace().key("cache_affinity:*");
    let mut cursor = 0u64;
    let mut total = 0usize;
    loop {
        let (next_cursor, keys) = match redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(200)
            .query_async::<(u64, Vec<String>)>(&mut connection)
            .await
        {
            Ok(value) => value,
            Err(_) => return total,
        };
        total += keys.len();
        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }
    total
}

async fn scan_admin_monitoring_namespaced_keys(
    runner: &aether_data::redis::RedisKvRunner,
    pattern: &str,
) -> Result<Vec<String>, GatewayError> {
    let mut connection = runner
        .client()
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| {
            GatewayError::Internal(format!("admin monitoring redis connect failed: {err}"))
        })?;
    let namespaced_pattern = runner.keyspace().key(pattern);
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (next_cursor, batch) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&namespaced_pattern)
            .arg("COUNT")
            .arg(200)
            .query_async::<(u64, Vec<String>)>(&mut connection)
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis scan failed: {err}"))
            })?;
        keys.extend(batch);
        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }
    Ok(keys)
}

#[cfg(test)]
fn load_admin_monitoring_cache_affinity_entries_for_tests(
    state: &AppState,
) -> Vec<(String, String)> {
    state.list_admin_monitoring_cache_affinity_entries_for_tests()
}

#[cfg(not(test))]
fn load_admin_monitoring_cache_affinity_entries_for_tests(
    _state: &AppState,
) -> Vec<(String, String)> {
    Vec::new()
}

#[cfg(test)]
fn load_admin_monitoring_redis_keys_for_tests(state: &AppState) -> Vec<String> {
    state.list_admin_monitoring_redis_keys_for_tests()
}

#[cfg(not(test))]
fn load_admin_monitoring_redis_keys_for_tests(_state: &AppState) -> Vec<String> {
    Vec::new()
}

#[cfg(test)]
fn delete_admin_monitoring_redis_keys_for_tests(state: &AppState, raw_keys: &[String]) -> usize {
    state.remove_admin_monitoring_redis_keys_for_tests(raw_keys)
}

#[cfg(not(test))]
fn delete_admin_monitoring_redis_keys_for_tests(_state: &AppState, _raw_keys: &[String]) -> usize {
    0
}

fn admin_monitoring_test_key_matches_pattern(key: &str, pattern: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => key.starts_with(prefix),
        None => key == pattern,
    }
}

fn admin_monitoring_has_test_redis_keys(state: &AppState) -> bool {
    !load_admin_monitoring_redis_keys_for_tests(state).is_empty()
}

async fn list_admin_monitoring_namespaced_keys(
    state: &AppState,
    pattern: &str,
) -> Result<Vec<String>, GatewayError> {
    if let Some(runner) = state.redis_kv_runner() {
        return scan_admin_monitoring_namespaced_keys(&runner, pattern).await;
    }

    let mut keys = load_admin_monitoring_redis_keys_for_tests(state)
        .into_iter()
        .filter(|key| admin_monitoring_test_key_matches_pattern(key, pattern))
        .collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

async fn delete_admin_monitoring_namespaced_keys(
    state: &AppState,
    raw_keys: &[String],
) -> Result<usize, GatewayError> {
    if raw_keys.is_empty() {
        return Ok(0);
    }

    if let Some(runner) = state.redis_kv_runner() {
        let mut connection = runner
            .client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis connect failed: {err}"))
            })?;
        let deleted = redis::cmd("DEL")
            .arg(raw_keys)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis delete failed: {err}"))
            })?;
        return Ok(usize::try_from(deleted).unwrap_or(0));
    }

    Ok(delete_admin_monitoring_redis_keys_for_tests(
        state, raw_keys,
    ))
}

async fn list_admin_monitoring_cache_affinity_records(
    state: &AppState,
) -> Result<Vec<AdminMonitoringCacheAffinityRecord>, GatewayError> {
    list_admin_monitoring_cache_affinity_records_matching(state, None).await
}

async fn list_admin_monitoring_cache_affinity_records_by_affinity_keys(
    state: &AppState,
    affinity_keys: &std::collections::BTreeSet<String>,
) -> Result<Vec<AdminMonitoringCacheAffinityRecord>, GatewayError> {
    if affinity_keys.is_empty() {
        return Ok(Vec::new());
    }
    list_admin_monitoring_cache_affinity_records_matching(state, Some(affinity_keys)).await
}

async fn list_admin_monitoring_cache_affinity_records_matching(
    state: &AppState,
    affinity_keys: Option<&std::collections::BTreeSet<String>>,
) -> Result<Vec<AdminMonitoringCacheAffinityRecord>, GatewayError> {
    let mut records = Vec::new();
    let mut seen_raw_keys = std::collections::BTreeSet::new();

    if let Some(runner) = state.redis_kv_runner() {
        let mut connection = runner
            .client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis connect failed: {err}"))
            })?;
        let patterns = affinity_keys
            .map(|keys| {
                keys.iter()
                    .map(|affinity_key| {
                        runner
                            .keyspace()
                            .key(&format!("cache_affinity:{affinity_key}:*"))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![runner.keyspace().key("cache_affinity:*")]);

        for pattern in patterns {
            let mut cursor = 0u64;
            loop {
                let (next_cursor, keys) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(200)
                    .query_async::<(u64, Vec<String>)>(&mut connection)
                    .await
                    .map_err(|err| {
                        GatewayError::Internal(format!("admin monitoring redis scan failed: {err}"))
                    })?;
                if !keys.is_empty() {
                    let values = redis::cmd("MGET")
                        .arg(&keys)
                        .query_async::<Vec<Option<String>>>(&mut connection)
                        .await
                        .map_err(|err| {
                            GatewayError::Internal(format!(
                                "admin monitoring redis mget failed: {err}"
                            ))
                        })?;
                    for (key, raw_value) in keys.into_iter().zip(values.into_iter()) {
                        let Some(raw_value) = raw_value else {
                            continue;
                        };
                        let Some(record) =
                            parse_admin_monitoring_cache_affinity_record(&key, &raw_value)
                        else {
                            continue;
                        };
                        if affinity_keys
                            .is_some_and(|keys| !keys.contains(&record.affinity_key))
                        {
                            continue;
                        }
                        if seen_raw_keys.insert(record.raw_key.clone()) {
                            records.push(record);
                        }
                    }
                }
                if next_cursor == 0 {
                    break;
                }
                cursor = next_cursor;
            }
        }
        return Ok(records);
    }

    for (key, raw_value) in load_admin_monitoring_cache_affinity_entries_for_tests(state) {
        let Some(record) = parse_admin_monitoring_cache_affinity_record(&key, &raw_value) else {
            continue;
        };
        if affinity_keys.is_some_and(|keys| !keys.contains(&record.affinity_key)) {
            continue;
        }
        if seen_raw_keys.insert(record.raw_key.clone()) {
            records.push(record);
        }
    }

    Ok(records)
}

async fn build_admin_monitoring_cache_snapshot(
    state: &AppState,
) -> Result<AdminMonitoringCacheSnapshot, GatewayError> {
    let scheduling_mode = state
        .read_system_config_json_value("scheduling_mode")
        .await?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "cache_affinity".to_string());
    let provider_priority_mode = state
        .read_system_config_json_value("provider_priority_mode")
        .await?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "provider".to_string());

    let now = chrono::Utc::now();
    let usage = if state.has_usage_data_reader() {
        state
            .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
                created_from_unix_secs: Some(
                    (now - chrono::Duration::hours(24)).timestamp().max(0) as u64,
                ),
                ..Default::default()
            })
            .await?
    } else {
        Vec::new()
    };
    let cache_hits = usage
        .iter()
        .filter(|item| item.cache_read_input_tokens > 0)
        .count();
    let cache_misses = usage.len().saturating_sub(cache_hits);
    let cache_hit_rate = if usage.is_empty() {
        0.0
    } else {
        round_to(cache_hits as f64 / usage.len() as f64, 4)
    };
    let total_affinities = count_admin_monitoring_cache_affinity_entries(state).await;
    let storage_type = if state.redis_kv_runner().is_some() {
        "redis"
    } else {
        "memory"
    };
    let scheduler_name = if scheduling_mode == "cache_affinity" {
        "cache_aware".to_string()
    } else {
        "random".to_string()
    };

    Ok(AdminMonitoringCacheSnapshot {
        scheduler_name,
        scheduling_mode,
        provider_priority_mode,
        storage_type,
        total_affinities,
        cache_hits,
        cache_misses,
        cache_hit_rate,
        provider_switches: 0,
        key_switches: 0,
        cache_invalidations: 0,
    })
}

fn build_admin_monitoring_resilience_recommendations(
    total_errors: usize,
    health_score: i64,
    open_breaker_labels: &[String],
) -> Vec<String> {
    let mut recommendations = Vec::new();
    if health_score < 50 {
        recommendations.push("系统健康状况严重，请立即检查错误日志".to_string());
    }
    if total_errors > 100 {
        recommendations.push("错误频率过高，建议检查系统配置和外部依赖".to_string());
    }
    if !open_breaker_labels.is_empty() {
        recommendations.push(format!(
            "以下服务熔断器已打开：{}",
            open_breaker_labels.join(", ")
        ));
    }
    if health_score > 90 {
        recommendations.push("系统运行良好".to_string());
    }
    recommendations
}

fn admin_monitoring_usage_is_error(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> bool {
    item.status_code.is_some_and(|value| value >= 400)
        || item.status.trim().eq_ignore_ascii_case("failed")
        || item.status.trim().eq_ignore_ascii_case("error")
        || item.error_message.is_some()
        || item.error_category.is_some()
}

fn admin_monitoring_trace_request_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/monitoring/trace/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_monitoring_user_behavior_user_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/monitoring/user-behavior/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_monitoring_trace_provider_id_from_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/monitoring/trace/stats/provider/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn parse_admin_monitoring_attempted_only(query: Option<&str>) -> Result<bool, String> {
    match query_param_value(query, "attempted_only") {
        None => Ok(false),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err("attempted_only must be a boolean".to_string()),
        },
    }
}

fn parse_admin_monitoring_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        None => Ok(100),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be an integer between 1 and 1000".to_string())?;
            if (1..=1000).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("limit must be an integer between 1 and 1000".to_string())
            }
        }
    }
}

fn parse_admin_monitoring_keyword_filter(query: Option<&str>) -> Option<String> {
    query_param_value(query, "keyword")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn admin_monitoring_cache_path_identifier_from_path(
    request_path: &str,
    prefix: &str,
) -> Option<String> {
    let value = request_path
        .strip_prefix(prefix)?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_monitoring_cache_affinity_user_identifier_from_path(request_path: &str) -> Option<String> {
    admin_monitoring_cache_path_identifier_from_path(
        request_path,
        "/api/admin/monitoring/cache/affinity/",
    )
}

fn admin_monitoring_cache_users_user_identifier_from_path(request_path: &str) -> Option<String> {
    admin_monitoring_cache_path_identifier_from_path(
        request_path,
        "/api/admin/monitoring/cache/users/",
    )
}

fn admin_monitoring_cache_provider_id_from_path(request_path: &str) -> Option<String> {
    admin_monitoring_cache_path_identifier_from_path(
        request_path,
        "/api/admin/monitoring/cache/providers/",
    )
}

fn admin_monitoring_cache_model_name_from_path(request_path: &str) -> Option<String> {
    admin_monitoring_cache_path_identifier_from_path(
        request_path,
        "/api/admin/monitoring/cache/model-mapping/",
    )
}

fn admin_monitoring_cache_redis_category_from_path(request_path: &str) -> Option<String> {
    admin_monitoring_cache_path_identifier_from_path(
        request_path,
        "/api/admin/monitoring/cache/redis-keys/",
    )
}

fn admin_monitoring_cache_model_mapping_provider_params_from_path(
    request_path: &str,
) -> Option<(String, String)> {
    let suffix = request_path
        .strip_prefix("/api/admin/monitoring/cache/model-mapping/provider/")?
        .trim()
        .trim_matches('/');
    let segments = suffix.split('/').collect::<Vec<_>>();
    if segments.len() != 2 || segments.iter().any(|segment| segment.trim().is_empty()) {
        return None;
    }
    Some((
        segments[0].trim().to_string(),
        segments[1].trim().to_string(),
    ))
}

fn admin_monitoring_cache_affinity_delete_params_from_path(
    request_path: &str,
) -> Option<(String, String, String, String)> {
    let suffix = request_path
        .strip_prefix("/api/admin/monitoring/cache/affinity/")?
        .trim()
        .trim_matches('/');
    let segments = suffix.split('/').collect::<Vec<_>>();
    if segments.len() != 4 || segments.iter().any(|segment| segment.trim().is_empty()) {
        return None;
    }
    Some((
        segments[0].trim().to_string(),
        segments[1].trim().to_string(),
        segments[2].trim().to_string(),
        segments[3].trim().to_string(),
    ))
}

fn admin_monitoring_cache_affinity_unavailable_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_MONITORING_CACHE_AFFINITY_REDIS_REQUIRED_DETAIL })),
    )
        .into_response()
}

fn admin_monitoring_redis_unavailable_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_MONITORING_REDIS_REQUIRED_DETAIL })),
    )
        .into_response()
}

fn admin_monitoring_cache_affinity_not_found_response(user_identifier: &str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({
            "detail": format!(
                "无法识别的用户标识符: {user_identifier}。支持用户名、邮箱、User ID或API Key ID"
            )
        })),
    )
        .into_response()
}

fn admin_monitoring_cache_users_not_found_response(user_identifier: &str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({
            "detail": format!(
                "无法识别的标识符: {user_identifier}。支持用户名、邮箱、User ID或API Key ID"
            )
        })),
    )
        .into_response()
}

fn admin_monitoring_masked_user_api_key_prefix(
    state: &AppState,
    ciphertext: Option<&str>,
) -> Option<String> {
    let Some(ciphertext) = ciphertext.map(str::trim).filter(|value| !value.is_empty()) else {
        return None;
    };
    let full_key = admin_monitoring_try_decrypt_secret(state, ciphertext)?;
    let prefix_len = full_key.len().min(10);
    let prefix = &full_key[..prefix_len];
    let suffix = if full_key.len() >= 4 {
        &full_key[full_key.len().saturating_sub(4)..]
    } else {
        ""
    };
    Some(format!("{prefix}...{suffix}"))
}

fn admin_monitoring_masked_provider_key_prefix(
    state: &AppState,
    key: &aether_data::repository::provider_catalog::StoredProviderCatalogKey,
) -> Option<String> {
    match key.auth_type.trim() {
        "service_account" | "vertex_ai" => Some("[Service Account]".to_string()),
        "oauth" => Some("[OAuth Token]".to_string()),
        _ => {
            let full_key = admin_monitoring_try_decrypt_secret(state, &key.encrypted_api_key)?;
            if full_key.len() <= 12 {
                Some(format!("{full_key}***"))
            } else {
                Some(format!(
                    "{}***{}",
                    &full_key[..8],
                    &full_key[full_key.len().saturating_sub(4)..]
                ))
            }
        }
    }
}

fn admin_monitoring_try_decrypt_secret(state: &AppState, ciphertext: &str) -> Option<String> {
    let ciphertext = ciphertext.trim();
    if ciphertext.is_empty() {
        return None;
    }
    let encryption_key = state.encryption_key().map(str::trim).unwrap_or("");
    if !encryption_key.is_empty() {
        if let Ok(value) = decrypt_python_fernet_ciphertext(encryption_key, ciphertext) {
            return Some(value);
        }
    }
    for env_key in ["AETHER_GATEWAY_DATA_ENCRYPTION_KEY", "ENCRYPTION_KEY"] {
        let Ok(candidate) = std::env::var(env_key) else {
            continue;
        };
        let candidate = candidate.trim();
        if candidate.is_empty() || candidate == encryption_key {
            continue;
        }
        if let Ok(value) = decrypt_python_fernet_ciphertext(candidate, ciphertext) {
            return Some(value);
        }
    }
    #[cfg(test)]
    if encryption_key != DEVELOPMENT_ENCRYPTION_KEY {
        if let Ok(value) = decrypt_python_fernet_ciphertext(DEVELOPMENT_ENCRYPTION_KEY, ciphertext)
        {
            return Some(value);
        }
    }
    None
}

fn admin_monitoring_cache_affinity_sort_value(value: Option<&serde_json::Value>) -> f64 {
    let Some(value) = value else {
        return 0.0;
    };
    if let Some(number) = value.as_f64() {
        return number;
    }
    if let Some(number) = value.as_i64() {
        return number as f64;
    }
    if let Some(number) = value.as_u64() {
        return number as f64;
    }
    if let Some(text) = value.as_str() {
        if let Ok(number) = text.parse::<f64>() {
            return number;
        }
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
            return parsed.timestamp() as f64;
        }
    }
    0.0
}

fn parse_admin_monitoring_cache_affinity_key(raw_key: &str) -> Option<(String, String, String)> {
    let parts = raw_key.split(':').collect::<Vec<_>>();
    let start = parts
        .iter()
        .position(|segment| *segment == "cache_affinity")?;
    let affinity_key = parts.get(start + 1)?.trim();
    if affinity_key.is_empty() {
        return None;
    }
    let api_format = parts
        .get(start + 2)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string();
    let model_name = parts
        .get(start + 3..)
        .filter(|segments| !segments.is_empty())
        .map(|segments| segments.join(":"))
        .unwrap_or_else(|| "unknown".to_string());
    Some((affinity_key.to_string(), api_format, model_name))
}

fn parse_admin_monitoring_cache_affinity_record(
    raw_key: &str,
    raw_value: &str,
) -> Option<AdminMonitoringCacheAffinityRecord> {
    let payload = serde_json::from_str::<serde_json::Value>(raw_value).ok()?;
    let object = payload.as_object()?;
    let (affinity_key, parsed_api_format, parsed_model_name) =
        parse_admin_monitoring_cache_affinity_key(raw_key)?;
    let api_format = object
        .get("api_format")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(parsed_api_format.as_str())
        .to_string();
    let model_name = object
        .get("model_name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(parsed_model_name.as_str())
        .to_string();
    let request_count = object
        .get("request_count")
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        })
        .unwrap_or(0);
    Some(AdminMonitoringCacheAffinityRecord {
        raw_key: raw_key.to_string(),
        affinity_key,
        api_format,
        model_name,
        provider_id: object
            .get("provider_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        endpoint_id: object
            .get("endpoint_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        key_id: object
            .get("key_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        created_at: object.get("created_at").cloned(),
        expire_at: object.get("expire_at").cloned(),
        request_count,
    })
}

async fn admin_monitoring_list_export_api_key_records_by_ids(
    state: &AppState,
    api_key_ids: &[String],
) -> Result<
    std::collections::BTreeMap<String, aether_data::repository::auth::StoredAuthApiKeyExportRecord>,
    GatewayError,
> {
    if api_key_ids.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    Ok(state
        .list_auth_api_key_export_records_by_ids(api_key_ids)
        .await?
        .into_iter()
        .map(|record| (record.api_key_id.clone(), record))
        .collect())
}

async fn admin_monitoring_list_user_summaries_by_ids(
    state: &AppState,
    user_ids: &[String],
) -> Result<
    std::collections::BTreeMap<String, aether_data::repository::users::StoredUserSummary>,
    GatewayError,
> {
    if user_ids.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    Ok(state
        .list_users_by_ids(user_ids)
        .await?
        .into_iter()
        .map(|user| (user.id.clone(), user))
        .collect())
}

async fn admin_monitoring_load_affinity_identity_maps(
    state: &AppState,
    affinities: &[AdminMonitoringCacheAffinityRecord],
) -> Result<
    (
        std::collections::BTreeMap<
            String,
            aether_data::repository::auth::StoredAuthApiKeyExportRecord,
        >,
        std::collections::BTreeMap<String, aether_data::repository::users::StoredUserSummary>,
    ),
    GatewayError,
> {
    let api_key_ids = affinities
        .iter()
        .map(|item| item.affinity_key.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let api_key_by_id = admin_monitoring_list_export_api_key_records_by_ids(state, &api_key_ids).await?;
    let user_ids = api_key_by_id
        .values()
        .map(|record| record.user_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let user_by_id = admin_monitoring_list_user_summaries_by_ids(state, &user_ids).await?;
    Ok((api_key_by_id, user_by_id))
}

async fn admin_monitoring_find_user_summary_by_id(
    state: &AppState,
    user_id: &str,
) -> Result<Option<aether_data::repository::users::StoredUserSummary>, GatewayError> {
    if user_id.trim().is_empty() {
        return Ok(None);
    }

    let user_ids = [user_id.to_string()];
    Ok(state.list_users_by_ids(&user_ids).await?.into_iter().next())
}

fn admin_monitoring_scheduler_affinity_cache_key(
    record: &AdminMonitoringCacheAffinityRecord,
) -> Option<String> {
    let affinity_key = record.affinity_key.trim();
    let api_format = record.api_format.trim().to_ascii_lowercase();
    let model_name = record.model_name.trim();
    if affinity_key.is_empty() || api_format.is_empty() || model_name.is_empty() {
        return None;
    }
    Some(format!(
        "scheduler_affinity:{affinity_key}:{api_format}:{model_name}"
    ))
}

fn clear_admin_monitoring_scheduler_affinity_entries(
    state: &AppState,
    records: &[AdminMonitoringCacheAffinityRecord],
) {
    let scheduler_keys = records
        .iter()
        .filter_map(admin_monitoring_scheduler_affinity_cache_key)
        .collect::<std::collections::BTreeSet<_>>();
    for scheduler_key in scheduler_keys {
        let _ = state.remove_scheduler_affinity_cache_entry(&scheduler_key);
    }
}

#[cfg(test)]
fn delete_admin_monitoring_cache_affinity_entries_for_tests(
    state: &AppState,
    raw_keys: &[String],
) -> usize {
    state.remove_admin_monitoring_cache_affinity_entries_for_tests(raw_keys)
}

#[cfg(not(test))]
fn delete_admin_monitoring_cache_affinity_entries_for_tests(
    _state: &AppState,
    _raw_keys: &[String],
) -> usize {
    0
}

async fn delete_admin_monitoring_cache_affinity_raw_keys(
    state: &AppState,
    raw_keys: &[String],
) -> Result<usize, GatewayError> {
    if raw_keys.is_empty() {
        return Ok(0);
    }

    if let Some(runner) = state.redis_kv_runner() {
        let mut connection = runner
            .client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis connect failed: {err}"))
            })?;
        let deleted = redis::cmd("DEL")
            .arg(raw_keys)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin monitoring redis delete failed: {err}"))
            })?;
        return Ok(usize::try_from(deleted).unwrap_or(0));
    }

    Ok(delete_admin_monitoring_cache_affinity_entries_for_tests(
        state, raw_keys,
    ))
}

fn parse_admin_monitoring_circuit_history_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        None => Ok(50),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be an integer between 1 and 200".to_string())?;
            if (1..=200).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("limit must be an integer between 1 and 200".to_string())
            }
        }
    }
}

async fn build_admin_monitoring_trace_request_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(request_id) =
        admin_monitoring_trace_request_id_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 request_id"));
    };
    let attempted_only = match parse_admin_monitoring_attempted_only(
        request_context.request_query_string.as_deref(),
    ) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let Some(trace) = state
        .read_decision_trace(&request_id, attempted_only)
        .await?
    else {
        return Ok(admin_monitoring_not_found_response("Request not found"));
    };

    let candidates = trace
        .candidates
        .iter()
        .map(|item| {
            let candidate = &item.candidate;
            json!({
                "id": candidate.id,
                "request_id": candidate.request_id,
                "candidate_index": candidate.candidate_index,
                "retry_index": candidate.retry_index,
                "provider_id": candidate.provider_id,
                "provider_name": item.provider_name,
                "provider_website": item.provider_website,
                "endpoint_id": candidate.endpoint_id,
                "endpoint_name": item.endpoint_api_format,
                "key_id": candidate.key_id,
                "key_name": item.provider_key_name,
                "key_account_label": serde_json::Value::Null,
                "key_preview": serde_json::Value::Null,
                "key_auth_type": item.provider_key_auth_type,
                "key_oauth_plan_type": serde_json::Value::Null,
                "key_capabilities": item.provider_key_capabilities,
                "required_capabilities": candidate.required_capabilities,
                "status": candidate.status,
                "skip_reason": candidate.skip_reason,
                "is_cached": candidate.is_cached,
                "status_code": candidate.status_code,
                "error_type": candidate.error_type,
                "error_message": candidate.error_message,
                "latency_ms": candidate.latency_ms,
                "concurrent_requests": candidate.concurrent_requests,
                "extra_data": candidate.extra_data,
                "created_at": unix_secs_to_rfc3339(candidate.created_at_unix_secs),
                "started_at": candidate.started_at_unix_secs.and_then(unix_secs_to_rfc3339),
                "finished_at": candidate.finished_at_unix_secs.and_then(unix_secs_to_rfc3339),
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "request_id": trace.request_id,
        "total_candidates": trace.total_candidates,
        "final_status": trace.final_status,
        "total_latency_ms": trace.total_latency_ms,
        "candidates": candidates,
    }))
    .into_response())
}

fn build_admin_monitoring_circuit_history_items(
    keys: &[aether_data::repository::provider_catalog::StoredProviderCatalogKey],
    provider_name_by_id: &BTreeMap<String, String>,
    limit: usize,
) -> Vec<serde_json::Value> {
    let mut items = Vec::new();

    for key in keys {
        let health_by_format = key
            .health_by_format
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        let circuit_by_format = key
            .circuit_breaker_by_format
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();

        for (api_format, circuit_value) in circuit_by_format {
            let Some(circuit) = circuit_value.as_object() else {
                continue;
            };
            let is_open = circuit
                .get("open")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let is_half_open = circuit
                .get("half_open_until")
                .and_then(serde_json::Value::as_str)
                .is_some();

            if !is_open && !is_half_open {
                continue;
            }

            let health = health_by_format
                .get(&api_format)
                .and_then(serde_json::Value::as_object);
            let timestamp = circuit
                .get("open_at")
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    circuit
                        .get("half_open_until")
                        .and_then(serde_json::Value::as_str)
                })
                .or_else(|| {
                    health.and_then(|value| {
                        value
                            .get("last_failure_at")
                            .and_then(serde_json::Value::as_str)
                    })
                })
                .map(ToOwned::to_owned);
            let event = if is_half_open { "half_open" } else { "opened" };
            let reason = circuit
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    health
                        .and_then(|value| {
                            value
                                .get("consecutive_failures")
                                .and_then(serde_json::Value::as_i64)
                        })
                        .filter(|value| *value > 0)
                        .map(|value| format!("连续失败 {value} 次"))
                })
                .or_else(|| {
                    Some(if is_half_open {
                        "熔断器处于半开状态".to_string()
                    } else {
                        "熔断器处于打开状态".to_string()
                    })
                });
            let recovery_seconds = circuit
                .get("recovery_seconds")
                .and_then(serde_json::Value::as_i64)
                .or_else(|| {
                    let open_at = circuit
                        .get("open_at")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok());
                    let next_probe_at = circuit
                        .get("next_probe_at")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok());
                    match (open_at, next_probe_at) {
                        (Some(open_at), Some(next_probe_at)) => {
                            Some((next_probe_at - open_at).num_seconds().max(0))
                        }
                        _ => None,
                    }
                });

            items.push(json!({
                "event": event,
                "key_id": key.id,
                "provider_id": key.provider_id,
                "provider_name": provider_name_by_id.get(&key.provider_id).cloned(),
                "key_name": key.name,
                "api_format": api_format,
                "reason": reason,
                "recovery_seconds": recovery_seconds,
                "timestamp": timestamp,
            }));
        }
    }

    items.sort_by(|left, right| {
        let left_ts = left
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let right_ts = right
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        right_ts.cmp(left_ts)
    });
    items.truncate(limit);
    items
}

async fn build_admin_monitoring_resilience_circuit_history_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let limit = match parse_admin_monitoring_circuit_history_limit(
        request_context.request_query_string.as_deref(),
    ) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let providers = state.list_provider_catalog_providers(false).await?;
    let provider_ids = providers
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let provider_name_by_id = providers
        .iter()
        .map(|item| (item.id.clone(), item.name.clone()))
        .collect::<BTreeMap<_, _>>();
    let keys = if provider_ids.is_empty() {
        Vec::new()
    } else {
        state
            .list_provider_catalog_keys_by_provider_ids(&provider_ids)
            .await?
    };

    let items = build_admin_monitoring_circuit_history_items(&keys, &provider_name_by_id, limit);
    let count = items.len();
    Ok(Json(json!({
        "items": items,
        "count": count,
    }))
    .into_response())
}

async fn build_admin_monitoring_trace_provider_stats_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(provider_id) =
        admin_monitoring_trace_provider_id_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 provider_id"));
    };
    let limit = match parse_admin_monitoring_limit(request_context.request_query_string.as_deref())
    {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let candidates = state
        .read_request_candidates_by_provider_id(&provider_id, limit)
        .await?;
    let total_attempts = candidates.len();
    let success_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Success
        })
        .count();
    let failed_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Failed
        })
        .count();
    let cancelled_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Cancelled
        })
        .count();
    let skipped_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Skipped
        })
        .count();
    let pending_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Pending
        })
        .count();
    let available_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Available
        })
        .count();
    let unused_count = candidates
        .iter()
        .filter(|item| {
            item.status == aether_data::repository::candidates::RequestCandidateStatus::Unused
        })
        .count();
    let completed_count = success_count + failed_count;
    let failure_rate = if completed_count == 0 {
        0.0
    } else {
        ((failed_count as f64 / completed_count as f64) * 10000.0).round() / 100.0
    };
    let latency_values = candidates
        .iter()
        .filter_map(|item| item.latency_ms.map(|value| value as f64))
        .collect::<Vec<_>>();
    let avg_latency_ms = if latency_values.is_empty() {
        0.0
    } else {
        let total = latency_values.iter().sum::<f64>();
        ((total / latency_values.len() as f64) * 100.0).round() / 100.0
    };

    Ok(Json(json!({
        "provider_id": provider_id,
        "total_attempts": total_attempts,
        "success_count": success_count,
        "failed_count": failed_count,
        "cancelled_count": cancelled_count,
        "skipped_count": skipped_count,
        "pending_count": pending_count,
        "available_count": available_count,
        "unused_count": unused_count,
        "failure_rate": failure_rate,
        "avg_latency_ms": avg_latency_ms,
    }))
    .into_response())
}

async fn build_admin_monitoring_audit_logs_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let username = parse_admin_monitoring_username_filter(query);
    let event_type = parse_admin_monitoring_event_type_filter(query);
    let limit = match parse_admin_monitoring_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };
    let offset = match parse_admin_monitoring_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };
    let days = match parse_admin_monitoring_days(query) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let Some(pool) = state.postgres_pool() else {
        return Ok(build_admin_monitoring_audit_logs_payload(
            Vec::new(),
            0,
            limit,
            offset,
            username,
            event_type,
            days,
        ));
    };

    let cutoff_time = chrono::Utc::now() - chrono::Duration::days(days);
    let username_pattern = username
        .as_deref()
        .map(admin_monitoring_escape_like_pattern)
        .map(|value| format!("%{value}%"));

    let total = sqlx::query_scalar::<_, i64>(
        r#"
SELECT COUNT(*)
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= $1
  AND ($2::text IS NULL OR u.username ILIKE $2 ESCAPE '\')
  AND ($3::text IS NULL OR a.event_type = $3)
"#,
    )
    .bind(cutoff_time)
    .bind(username_pattern.as_deref())
    .bind(event_type.as_deref())
    .fetch_one(&pool)
    .await
    .map_err(|err| GatewayError::Internal(format!("admin audit logs count failed: {err}")))?;

    let rows = sqlx::query(
        r#"
SELECT
  a.id,
  a.event_type,
  a.user_id,
  u.email AS user_email,
  u.username AS user_username,
  a.description,
  a.ip_address,
  a.status_code,
  a.error_message,
  a.event_metadata AS metadata,
  a.created_at
FROM audit_logs AS a
LEFT JOIN users AS u ON a.user_id = u.id
WHERE a.created_at >= $1
  AND ($2::text IS NULL OR u.username ILIKE $2 ESCAPE '\')
  AND ($3::text IS NULL OR a.event_type = $3)
ORDER BY a.created_at DESC
LIMIT $4 OFFSET $5
"#,
    )
    .bind(cutoff_time)
    .bind(username_pattern.as_deref())
    .bind(event_type.as_deref())
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .bind(i64::try_from(offset).unwrap_or(i64::MAX))
    .fetch_all(&pool)
    .await
    .map_err(|err| GatewayError::Internal(format!("admin audit logs read failed: {err}")))?;

    let items = rows
        .into_iter()
        .map(|row| {
            let created_at = row
                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                .ok()
                .map(|value| value.to_rfc3339());
            json!({
                "id": row.try_get::<String, _>("id").ok(),
                "event_type": row.try_get::<String, _>("event_type").ok(),
                "user_id": row.try_get::<Option<String>, _>("user_id").ok().flatten(),
                "user_email": row.try_get::<Option<String>, _>("user_email").ok().flatten(),
                "user_username": row.try_get::<Option<String>, _>("user_username").ok().flatten(),
                "description": row.try_get::<Option<String>, _>("description").ok().flatten(),
                "ip_address": row.try_get::<Option<String>, _>("ip_address").ok().flatten(),
                "status_code": row.try_get::<Option<i32>, _>("status_code").ok().flatten(),
                "error_message": row.try_get::<Option<String>, _>("error_message").ok().flatten(),
                "metadata": row.try_get::<Option<serde_json::Value>, _>("metadata").ok().flatten(),
                "created_at": created_at,
            })
        })
        .collect::<Vec<_>>();

    Ok(build_admin_monitoring_audit_logs_payload(
        items,
        usize::try_from(total.max(0)).unwrap_or(usize::MAX),
        limit,
        offset,
        username,
        event_type,
        days,
    ))
}

async fn build_admin_monitoring_suspicious_activities_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let hours = match parse_admin_monitoring_hours(query) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let Some(pool) = state.postgres_pool() else {
        return Ok(build_admin_monitoring_suspicious_activities_payload(
            Vec::new(),
            hours,
        ));
    };

    let cutoff_time = chrono::Utc::now() - chrono::Duration::hours(hours);
    let rows = sqlx::query(
        r#"
SELECT
  id,
  event_type,
  user_id,
  description,
  ip_address,
  event_metadata AS metadata,
  created_at
FROM audit_logs
WHERE created_at >= $1
  AND event_type = ANY($2)
ORDER BY created_at DESC
LIMIT 100
"#,
    )
    .bind(cutoff_time)
    .bind(vec![
        "suspicious_activity",
        "unauthorized_access",
        "login_failed",
        "request_rate_limited",
    ])
    .fetch_all(&pool)
    .await
    .map_err(|err| {
        GatewayError::Internal(format!("admin suspicious activities read failed: {err}"))
    })?;

    let activities = rows
        .into_iter()
        .map(|row| {
            let created_at = row
                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                .ok()
                .map(|value| value.to_rfc3339());
            json!({
                "id": row.try_get::<String, _>("id").ok(),
                "event_type": row.try_get::<String, _>("event_type").ok(),
                "user_id": row.try_get::<Option<String>, _>("user_id").ok().flatten(),
                "description": row.try_get::<Option<String>, _>("description").ok().flatten(),
                "ip_address": row.try_get::<Option<String>, _>("ip_address").ok().flatten(),
                "metadata": row.try_get::<Option<serde_json::Value>, _>("metadata").ok().flatten(),
                "created_at": created_at,
            })
        })
        .collect::<Vec<_>>();

    Ok(build_admin_monitoring_suspicious_activities_payload(
        activities, hours,
    ))
}

async fn build_admin_monitoring_user_behavior_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) =
        admin_monitoring_user_behavior_user_id_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 user_id"));
    };
    let days = match parse_admin_monitoring_days(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };

    let Some(pool) = state.postgres_pool() else {
        return Ok(build_admin_monitoring_user_behavior_payload(
            user_id,
            days,
            std::collections::BTreeMap::new(),
            0,
            0,
            0,
        ));
    };

    let cutoff_time = chrono::Utc::now() - chrono::Duration::days(days);

    let event_rows = sqlx::query(
        r#"
SELECT event_type, COUNT(*)::bigint AS count
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
GROUP BY event_type
"#,
    )
    .bind(&user_id)
    .bind(cutoff_time)
    .fetch_all(&pool)
    .await
    .map_err(|err| GatewayError::Internal(format!("admin user behavior read failed: {err}")))?;

    let event_counts = event_rows
        .into_iter()
        .filter_map(|row| {
            let event_type = row.try_get::<String, _>("event_type").ok()?;
            let count = row
                .try_get::<i64, _>("count")
                .ok()
                .and_then(|value| u64::try_from(value.max(0)).ok())
                .unwrap_or(0);
            Some((event_type, count))
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let failed_requests = event_counts
        .get("request_failed")
        .copied()
        .unwrap_or_default();
    let success_requests = event_counts
        .get("request_success")
        .copied()
        .unwrap_or_default();
    let suspicious_activities = event_counts
        .get("suspicious_activity")
        .copied()
        .unwrap_or_default()
        .saturating_add(
            event_counts
                .get("unauthorized_access")
                .copied()
                .unwrap_or_default(),
        );

    Ok(build_admin_monitoring_user_behavior_payload(
        user_id,
        days,
        event_counts,
        failed_requests,
        success_requests,
        suspicious_activities,
    ))
}

async fn build_admin_monitoring_resilience_status_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    let snapshot = build_admin_monitoring_resilience_snapshot(state).await?;

    Ok(Json(json!({
        "timestamp": snapshot.timestamp.to_rfc3339(),
        "health_score": snapshot.health_score,
        "status": snapshot.status,
        "error_statistics": snapshot.error_statistics,
        "recent_errors": snapshot.recent_errors,
        "recommendations": snapshot.recommendations,
    }))
    .into_response())
}

async fn build_admin_monitoring_resilience_snapshot(
    state: &AppState,
) -> Result<AdminMonitoringResilienceSnapshot, GatewayError> {
    let now = chrono::Utc::now();
    let recent_error_from = std::cmp::max(
        now - chrono::Duration::hours(24),
        chrono::DateTime::<chrono::Utc>::from_timestamp(
            state
                .admin_monitoring_error_stats_reset_at()
                .unwrap_or_default() as i64,
            0,
        )
        .unwrap_or_else(|| {
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("unix epoch should exist")
        }),
    );

    let providers = state.list_provider_catalog_providers(false).await?;
    let provider_ids = providers
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let provider_name_by_id = providers
        .iter()
        .map(|item| (item.id.clone(), item.name.clone()))
        .collect::<BTreeMap<_, _>>();
    let keys = if provider_ids.is_empty() {
        Vec::new()
    } else {
        state
            .list_provider_catalog_keys_by_provider_ids(&provider_ids)
            .await?
    };

    let active_keys = keys.iter().filter(|item| item.is_active).count();
    let mut degraded_keys = 0usize;
    let mut unhealthy_keys = 0usize;
    let mut open_circuit_breakers = 0usize;
    let mut open_breaker_labels = Vec::new();
    let mut circuit_breakers = serde_json::Map::new();
    let mut previous_circuit_breakers = serde_json::Map::new();

    for key in &keys {
        let (
            health_score,
            consecutive_failures,
            last_failure_at,
            circuit_breaker_open,
            circuit_by_format,
        ) = provider_key_health_summary(key);
        if health_score < 0.8 {
            degraded_keys += 1;
        }
        if health_score < 0.5 {
            unhealthy_keys += 1;
        }

        let open_formats = circuit_by_format
            .iter()
            .filter_map(|(api_format, value)| {
                value
                    .get("open")
                    .and_then(serde_json::Value::as_bool)
                    .filter(|open| *open)
                    .map(|_| api_format.clone())
            })
            .collect::<Vec<_>>();

        if circuit_breaker_open {
            open_circuit_breakers += 1;
            let provider_label = provider_name_by_id
                .get(&key.provider_id)
                .cloned()
                .unwrap_or_else(|| key.provider_id.clone());
            open_breaker_labels.push(format!("{provider_label}/{}", key.name));
        }

        if circuit_breaker_open || consecutive_failures > 0 || health_score < 1.0 {
            circuit_breakers.insert(
                key.id.clone(),
                json!({
                    "state": if circuit_breaker_open { "open" } else { "closed" },
                    "provider_id": key.provider_id,
                    "provider_name": provider_name_by_id.get(&key.provider_id).cloned(),
                    "key_name": key.name,
                    "health_score": health_score,
                    "consecutive_failures": consecutive_failures,
                    "last_failure_at": last_failure_at,
                    "open_formats": open_formats,
                }),
            );
            previous_circuit_breakers.insert(
                key.id.clone(),
                json!({
                    "state": if circuit_breaker_open { "open" } else { "closed" },
                    "failure_count": consecutive_failures,
                }),
            );
        }
    }

    let mut recent_usage_errors = state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(recent_error_from.timestamp().max(0) as u64),
            ..Default::default()
        })
        .await?
        .into_iter()
        .filter(admin_monitoring_usage_is_error)
        .collect::<Vec<_>>();
    recent_usage_errors
        .sort_by(|left, right| right.created_at_unix_secs.cmp(&left.created_at_unix_secs));

    let total_errors = recent_usage_errors.len();
    let mut error_breakdown = std::collections::BTreeMap::<String, usize>::new();
    for item in &recent_usage_errors {
        let error_type = item
            .error_category
            .clone()
            .unwrap_or_else(|| item.status.clone());
        let operation = format!(
            "{}:{}",
            item.provider_name,
            item.api_format
                .clone()
                .unwrap_or_else(|| item.model.clone())
        );
        *error_breakdown
            .entry(format!("{error_type}:{operation}"))
            .or_default() += 1;
    }
    let recent_errors = recent_usage_errors
        .iter()
        .take(10)
        .map(|item| {
            let error_type = item
                .error_category
                .clone()
                .unwrap_or_else(|| item.status.clone());
            let operation = format!(
                "{}:{}",
                item.provider_name,
                item.api_format
                    .clone()
                    .unwrap_or_else(|| item.model.clone())
            );
            json!({
                "error_id": item.id,
                "error_type": error_type,
                "operation": operation,
                "timestamp": unix_secs_to_rfc3339(item.created_at_unix_secs),
                "context": {
                    "request_id": item.request_id,
                    "provider_id": item.provider_id,
                    "provider_name": item.provider_name,
                    "model": item.model,
                    "api_format": item.api_format,
                    "status_code": item.status_code,
                    "error_message": item.error_message,
                }
            })
        })
        .collect::<Vec<_>>();

    let health_score = (100_i64
        - i64::try_from(total_errors)
            .unwrap_or(i64::MAX)
            .saturating_mul(2)
        - i64::try_from(open_circuit_breakers)
            .unwrap_or(i64::MAX)
            .saturating_mul(20))
    .clamp(0, 100);
    let status = if health_score > 80 {
        "healthy"
    } else if health_score > 50 {
        "degraded"
    } else {
        "critical"
    };
    let recommendations = build_admin_monitoring_resilience_recommendations(
        total_errors,
        health_score,
        &open_breaker_labels,
    );

    Ok(AdminMonitoringResilienceSnapshot {
        timestamp: now,
        health_score,
        status,
        error_statistics: json!({
            "total_errors": total_errors,
            "active_keys": active_keys,
            "degraded_keys": degraded_keys,
            "unhealthy_keys": unhealthy_keys,
            "open_circuit_breakers": open_circuit_breakers,
            "circuit_breakers": circuit_breakers,
        }),
        recent_errors,
        recommendations,
        previous_stats: json!({
            "total_errors": total_errors,
            "error_breakdown": error_breakdown,
            "recent_errors": total_errors,
            "circuit_breakers": previous_circuit_breakers,
        }),
    })
}

async fn build_admin_monitoring_reset_error_stats_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let snapshot = build_admin_monitoring_resilience_snapshot(state).await?;
    let reset_at = chrono::Utc::now();
    state.mark_admin_monitoring_error_stats_reset(reset_at.timestamp().max(0) as u64);

    let reset_by = if let Some(user_id) = request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.admin_principal.as_ref())
        .map(|principal| principal.user_id.clone())
    {
        state
            .find_user_auth_by_id(&user_id)
            .await?
            .and_then(|user| user.email.or(Some(user.username)))
            .or(Some(user_id))
    } else {
        None
    };

    Ok(Json(json!({
        "message": "错误统计已重置",
        "previous_stats": snapshot.previous_stats,
        "reset_by": reset_by,
        "reset_at": reset_at.to_rfc3339(),
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_stats_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    let snapshot = build_admin_monitoring_cache_snapshot(state).await?;

    Ok(Json(json!({
        "status": "ok",
        "data": {
            "scheduler": snapshot.scheduler_name,
            "total_affinities": snapshot.total_affinities,
            "cache_hit_rate": snapshot.cache_hit_rate,
            "provider_switches": snapshot.provider_switches,
            "key_switches": snapshot.key_switches,
            "cache_hits": snapshot.cache_hits,
            "cache_misses": snapshot.cache_misses,
            "scheduler_metrics": {
                "cache_hits": snapshot.cache_hits,
                "cache_misses": snapshot.cache_misses,
                "cache_hit_rate": snapshot.cache_hit_rate,
                "total_batches": 0,
                "last_batch_size": 0,
                "total_candidates": 0,
                "last_candidate_count": 0,
                "concurrency_denied": 0,
                "avg_candidates_per_batch": 0.0,
                "scheduling_mode": snapshot.scheduling_mode,
                "provider_priority_mode": snapshot.provider_priority_mode,
            },
            "affinity_stats": {
                "storage_type": snapshot.storage_type,
                "total_affinities": snapshot.total_affinities,
                "cache_hits": snapshot.cache_hits,
                "cache_misses": snapshot.cache_misses,
                "cache_hit_rate": snapshot.cache_hit_rate,
                "cache_invalidations": snapshot.cache_invalidations,
                "provider_switches": snapshot.provider_switches,
                "key_switches": snapshot.key_switches,
                "config": {
                    "default_ttl": ADMIN_MONITORING_CACHE_AFFINITY_DEFAULT_TTL_SECS,
                }
            }
        }
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_metrics_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    let snapshot = build_admin_monitoring_cache_snapshot(state).await?;
    let metrics = [
        (
            "cache_scheduler_total_batches",
            "Number of scheduling batches processed",
            0.0,
        ),
        (
            "cache_scheduler_last_batch_size",
            "Size of the most recent scheduling batch",
            0.0,
        ),
        (
            "cache_scheduler_total_candidates",
            "Total candidates seen during scheduling",
            0.0,
        ),
        (
            "cache_scheduler_last_candidate_count",
            "Number of candidates in the most recent batch",
            0.0,
        ),
        (
            "cache_scheduler_cache_hits",
            "Cache hits counted during scheduling",
            snapshot.cache_hits as f64,
        ),
        (
            "cache_scheduler_cache_misses",
            "Cache misses counted during scheduling",
            snapshot.cache_misses as f64,
        ),
        (
            "cache_scheduler_cache_hit_rate",
            "Cache hit rate during scheduling",
            snapshot.cache_hit_rate,
        ),
        (
            "cache_scheduler_concurrency_denied",
            "Times candidate rejected due to concurrency limits",
            0.0,
        ),
        (
            "cache_scheduler_avg_candidates_per_batch",
            "Average candidates per batch",
            0.0,
        ),
        (
            "cache_affinity_total",
            "Total cache affinities stored",
            snapshot.total_affinities as f64,
        ),
        (
            "cache_affinity_hits",
            "Affinity cache hits",
            snapshot.cache_hits as f64,
        ),
        (
            "cache_affinity_misses",
            "Affinity cache misses",
            snapshot.cache_misses as f64,
        ),
        (
            "cache_affinity_hit_rate",
            "Affinity cache hit rate",
            snapshot.cache_hit_rate,
        ),
        (
            "cache_affinity_invalidations",
            "Affinity invalidations",
            snapshot.cache_invalidations as f64,
        ),
        (
            "cache_affinity_provider_switches",
            "Affinity provider switches",
            snapshot.provider_switches as f64,
        ),
        (
            "cache_affinity_key_switches",
            "Affinity key switches",
            snapshot.key_switches as f64,
        ),
    ];

    let mut lines = Vec::with_capacity(metrics.len() * 3 + 1);
    for (name, help_text, value) in metrics {
        lines.push(format!("# HELP {name} {help_text}"));
        lines.push(format!("# TYPE {name} gauge"));
        lines.push(format!("{name} {value}"));
    }
    lines.push(format!(
        "cache_scheduler_info{{scheduler=\"{}\"}} 1",
        snapshot.scheduler_name
    ));

    Ok((
        [(
            http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        lines.join("\n") + "\n",
    )
        .into_response())
}

async fn build_admin_monitoring_cache_config_response() -> Result<Response<Body>, GatewayError> {
    Ok(Json(json!({
        "status": "ok",
        "data": {
            "cache_ttl_seconds": ADMIN_MONITORING_CACHE_AFFINITY_DEFAULT_TTL_SECS,
            "cache_reservation_ratio": ADMIN_MONITORING_CACHE_RESERVATION_RATIO,
            "dynamic_reservation": {
                "enabled": true,
                "config": {
                    "probe_phase_requests": ADMIN_MONITORING_DYNAMIC_RESERVATION_PROBE_PHASE_REQUESTS,
                    "probe_reservation": ADMIN_MONITORING_DYNAMIC_RESERVATION_PROBE_RESERVATION,
                    "stable_min_reservation": ADMIN_MONITORING_DYNAMIC_RESERVATION_STABLE_MIN_RESERVATION,
                    "stable_max_reservation": ADMIN_MONITORING_DYNAMIC_RESERVATION_STABLE_MAX_RESERVATION,
                    "low_load_threshold": ADMIN_MONITORING_DYNAMIC_RESERVATION_LOW_LOAD_THRESHOLD,
                    "high_load_threshold": ADMIN_MONITORING_DYNAMIC_RESERVATION_HIGH_LOAD_THRESHOLD,
                },
                "description": {
                    "probe_phase_requests": "探测阶段请求数阈值",
                    "probe_reservation": "探测阶段预留比例",
                    "stable_min_reservation": "稳定阶段最小预留比例",
                    "stable_max_reservation": "稳定阶段最大预留比例",
                    "low_load_threshold": "低负载阈值（低于此值使用最小预留）",
                    "high_load_threshold": "高负载阈值（高于此值根据置信度使用较高预留）",
                },
            },
            "description": {
                "cache_ttl": "缓存亲和性有效期（秒）",
                "cache_reservation_ratio": "静态预留比例（已被动态预留替代）",
                "dynamic_reservation": "动态预留机制配置",
            },
        }
    }))
    .into_response())
}

async fn build_admin_monitoring_model_mapping_stats_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(Json(json!({
            "status": "ok",
            "data": {
                "available": false,
                "message": "Redis 未启用，模型映射缓存不可用",
            }
        }))
        .into_response());
    };

    let model_id_keys = list_admin_monitoring_namespaced_keys(state, "model:id:*").await?;
    let global_model_id_keys =
        list_admin_monitoring_namespaced_keys(state, "global_model:id:*").await?;
    let global_model_name_keys =
        list_admin_monitoring_namespaced_keys(state, "global_model:name:*").await?;
    let global_model_resolve_keys =
        list_admin_monitoring_namespaced_keys(state, "global_model:resolve:*").await?;
    let provider_global_keys =
        list_admin_monitoring_namespaced_keys(state, "model:provider_global:*")
            .await?
            .into_iter()
            .filter(|key| !key.starts_with("model:provider_global:hits:"))
            .collect::<Vec<_>>();

    let total_keys = model_id_keys.len()
        + global_model_id_keys.len()
        + global_model_name_keys.len()
        + global_model_resolve_keys.len()
        + provider_global_keys.len();

    Ok(Json(json!({
        "status": "ok",
        "data": {
            "available": true,
            "ttl_seconds": 300,
            "total_keys": total_keys,
            "breakdown": {
                "model_by_id": model_id_keys.len(),
                "model_by_provider_global": provider_global_keys.len(),
                "global_model_by_id": global_model_id_keys.len(),
                "global_model_by_name": global_model_name_keys.len(),
                "global_model_resolve": global_model_resolve_keys.len(),
            },
            "mappings": [],
            "provider_model_mappings": serde_json::Value::Null,
            "unmapped": serde_json::Value::Null,
        }
    }))
    .into_response())
}

async fn build_admin_monitoring_redis_cache_categories_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(Json(json!({
            "status": "ok",
            "data": {
                "available": false,
                "message": "Redis 未启用",
            }
        }))
        .into_response());
    };

    let mut categories = Vec::with_capacity(ADMIN_MONITORING_REDIS_CACHE_CATEGORIES.len());
    let mut total_keys = 0usize;

    for (key, name, pattern, description) in ADMIN_MONITORING_REDIS_CACHE_CATEGORIES {
        let count = list_admin_monitoring_namespaced_keys(state, pattern)
            .await?
            .len();
        total_keys += count;
        categories.push(json!({
            "key": key,
            "name": name,
            "pattern": pattern,
            "description": description,
            "count": count,
        }));
    }

    Ok(Json(json!({
        "status": "ok",
        "data": {
            "available": true,
            "categories": categories,
            "total_keys": total_keys,
        }
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_affinities_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let limit = match parse_admin_monitoring_limit(request_context.request_query_string.as_deref())
    {
        Ok(value) => value,
        Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
    };
    let offset =
        match parse_admin_monitoring_offset(request_context.request_query_string.as_deref()) {
            Ok(value) => value,
            Err(detail) => return Ok(admin_monitoring_bad_request_response(detail)),
        };
    let keyword =
        parse_admin_monitoring_keyword_filter(request_context.request_query_string.as_deref());

    let mut matched_user_id = None::<String>;
    let mut matched_api_key_id = None::<String>;
    let filtered_affinities = if let Some(keyword_value) = keyword.as_deref() {
        let direct_affinity_keys = std::iter::once(keyword_value.to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let direct_affinities = list_admin_monitoring_cache_affinity_records_by_affinity_keys(
            state,
            &direct_affinity_keys,
        )
        .await?;
        if !direct_affinities.is_empty() {
            matched_api_key_id = Some(keyword_value.to_string());
            matched_user_id = admin_monitoring_list_export_api_key_records_by_ids(
                state,
                &[keyword_value.to_string()],
            )
            .await?
            .get(keyword_value)
            .map(|item| item.user_id.clone());
            direct_affinities
        } else if let Some(user) = state.find_user_auth_by_identifier(keyword_value).await? {
            matched_user_id = Some(user.id.clone());
            let user_api_key_ids = state
                .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&user.id))
                .await?
                .into_iter()
                .map(|item| item.api_key_id)
                .collect::<std::collections::BTreeSet<_>>();
            list_admin_monitoring_cache_affinity_records_by_affinity_keys(state, &user_api_key_ids)
                .await?
        } else {
            list_admin_monitoring_cache_affinity_records(state).await?
        }
    } else {
        list_admin_monitoring_cache_affinity_records(state).await?
    };
    let (api_key_by_id, user_by_id) =
        admin_monitoring_load_affinity_identity_maps(state, &filtered_affinities).await?;

    let provider_ids = filtered_affinities
        .iter()
        .filter_map(|item| item.provider_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let endpoint_ids = filtered_affinities
        .iter()
        .filter_map(|item| item.endpoint_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let key_ids = filtered_affinities
        .iter()
        .filter_map(|item| item.key_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let provider_by_id = state
        .data
        .list_provider_catalog_providers_by_ids(&provider_ids)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<std::collections::BTreeMap<_, _>>();
    let endpoint_by_id = state
        .data
        .list_provider_catalog_endpoints_by_ids(&endpoint_ids)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<std::collections::BTreeMap<_, _>>();
    let key_by_id = state
        .data
        .list_provider_catalog_keys_by_ids(&key_ids)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<std::collections::BTreeMap<_, _>>();

    let keyword_lower = keyword.as_ref().map(|value| value.to_ascii_lowercase());
    let mut items = Vec::new();
    for affinity in filtered_affinities {
        let user_api_key = api_key_by_id.get(&affinity.affinity_key);
        let user_id = user_api_key.map(|item| item.user_id.clone());
        let user = user_id.as_ref().and_then(|id| user_by_id.get(id));
        let provider = affinity
            .provider_id
            .as_ref()
            .and_then(|id| provider_by_id.get(id));
        let endpoint = affinity
            .endpoint_id
            .as_ref()
            .and_then(|id| endpoint_by_id.get(id));
        let key = affinity.key_id.as_ref().and_then(|id| key_by_id.get(id));

        let user_api_key_name = user_api_key.and_then(|item| item.name.clone());
        let user_api_key_prefix = user_api_key.and_then(|item| {
            admin_monitoring_masked_user_api_key_prefix(state, item.key_encrypted.as_deref())
        });
        let provider_name = provider.map(|item| item.name.clone());
        let endpoint_url = endpoint
            .map(|item| item.base_url.clone())
            .filter(|value| !value.trim().is_empty());
        let key_name = key.map(|item| item.name.clone());
        let key_prefix =
            key.and_then(|item| admin_monitoring_masked_provider_key_prefix(state, item));
        let user_id_text = user_id.clone();
        let username = user.map(|item| item.username.clone());
        let email = user.and_then(|item| item.email.clone());
        let provider_id = affinity.provider_id.clone();
        let key_id = affinity.key_id.clone();

        if let Some(keyword_value) = keyword_lower.as_deref() {
            if matched_user_id.is_none() && matched_api_key_id.is_none() {
                let searchable = [
                    Some(affinity.affinity_key.as_str()),
                    user_api_key_name.as_deref(),
                    user_id_text.as_deref(),
                    username.as_deref(),
                    email.as_deref(),
                    provider_id.as_deref(),
                    key_id.as_deref(),
                ];
                if !searchable
                    .into_iter()
                    .flatten()
                    .any(|value| value.to_ascii_lowercase().contains(keyword_value))
                {
                    continue;
                }
            }
        }

        items.push(json!({
            "affinity_key": affinity.affinity_key,
            "user_api_key_name": user_api_key_name,
            "user_api_key_prefix": user_api_key_prefix,
            "is_standalone": user_api_key.map(|item| item.is_standalone).unwrap_or(false),
            "user_id": user_id_text,
            "username": username,
            "email": email,
            "provider_id": provider_id,
            "provider_name": provider_name,
            "endpoint_id": affinity.endpoint_id,
            "endpoint_url": endpoint_url,
            "key_id": key_id,
            "key_name": key_name,
            "key_prefix": key_prefix,
            "rate_multipliers": key.and_then(|item| item.rate_multipliers.clone()),
            "global_model_id": affinity.model_name,
            "model_name": affinity.model_name,
            "model_display_name": serde_json::Value::Null,
            "api_format": affinity.api_format,
            "created_at": affinity.created_at,
            "expire_at": affinity.expire_at,
            "request_count": affinity.request_count,
        }));
    }

    items.sort_by(|left, right| {
        admin_monitoring_cache_affinity_sort_value(right.get("expire_at"))
            .partial_cmp(&admin_monitoring_cache_affinity_sort_value(
                left.get("expire_at"),
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let total = items.len();
    let paged_items = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let paged_count = paged_items.len();

    Ok(Json(json!({
        "status": "ok",
        "data": {
            "items": paged_items,
            "meta": {
                "total": total,
                "limit": limit,
                "offset": offset,
                "count": paged_count,
            },
            "matched_user_id": matched_user_id,
        }
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_affinity_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_identifier) =
        admin_monitoring_cache_affinity_user_identifier_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 user_identifier"));
    };
    let direct_api_key_by_id =
        admin_monitoring_list_export_api_key_records_by_ids(state, &[user_identifier.clone()])
            .await?;
    let direct_affinity_keys = std::iter::once(user_identifier.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let direct_affinities = list_admin_monitoring_cache_affinity_records_by_affinity_keys(
        state,
        &direct_affinity_keys,
    )
    .await?;

    let (resolved_user_id, username, email, filtered_affinities) =
        if !direct_affinities.is_empty() || direct_api_key_by_id.contains_key(&user_identifier) {
            let user_id = direct_api_key_by_id
                .get(&user_identifier)
                .map(|item| item.user_id.clone());
            let user = match user_id.as_deref() {
                Some(user_id) => admin_monitoring_find_user_summary_by_id(state, user_id).await?,
                None => None,
            };
            (
                user_id,
                user.as_ref().map(|item| item.username.clone()),
                user.and_then(|item| item.email),
                direct_affinities,
            )
        } else if let Some(user) = state.find_user_auth_by_identifier(&user_identifier).await? {
            let user_api_key_ids = state
                .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&user.id))
                .await?
                .into_iter()
                .map(|item| item.api_key_id)
                .collect::<std::collections::BTreeSet<_>>();
            let affinities =
                list_admin_monitoring_cache_affinity_records_by_affinity_keys(state, &user_api_key_ids)
                    .await?;
            (
                Some(user.id),
                Some(user.username),
                user.email,
                affinities,
            )
        } else {
            return Ok(admin_monitoring_cache_affinity_not_found_response(
                &user_identifier,
            ));
        };

    if filtered_affinities.is_empty() {
        let display_name = username.clone().unwrap_or_else(|| user_identifier.clone());
        return Ok(Json(json!({
            "status": "not_found",
            "message": format!(
                "用户 {} ({}) 没有缓存亲和性",
                display_name,
                email.clone().unwrap_or_else(|| "null".to_string()),
            ),
            "user_info": {
                "user_id": resolved_user_id,
                "username": username,
                "email": email,
            },
            "affinities": [],
        }))
        .into_response());
    }

    let mut affinities = filtered_affinities
        .into_iter()
        .map(|item| {
            json!({
                "provider_id": item.provider_id,
                "endpoint_id": item.endpoint_id,
                "key_id": item.key_id,
                "api_format": item.api_format,
                "model_name": item.model_name,
                "created_at": item.created_at,
                "expire_at": item.expire_at,
                "request_count": item.request_count,
            })
        })
        .collect::<Vec<_>>();
    affinities.sort_by(|left, right| {
        admin_monitoring_cache_affinity_sort_value(right.get("expire_at"))
            .partial_cmp(&admin_monitoring_cache_affinity_sort_value(
                left.get("expire_at"),
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let total_endpoints = affinities.len();

    Ok(Json(json!({
        "status": "ok",
        "user_info": {
            "user_id": resolved_user_id,
            "username": username,
            "email": email,
        },
        "affinities": affinities,
        "total_endpoints": total_endpoints,
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_users_delete_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_identifier) =
        admin_monitoring_cache_users_user_identifier_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 user_identifier"));
    };

    if state.redis_kv_runner().is_none()
        && load_admin_monitoring_cache_affinity_entries_for_tests(state).is_empty()
    {
        return Ok(admin_monitoring_cache_affinity_unavailable_response());
    }

    let direct_api_key_by_id =
        admin_monitoring_list_export_api_key_records_by_ids(state, &[user_identifier.clone()])
            .await?;

    if let Some(api_key) = direct_api_key_by_id.get(&user_identifier) {
        let target_affinity_keys = std::iter::once(user_identifier.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let target_affinities = list_admin_monitoring_cache_affinity_records_by_affinity_keys(
            state,
            &target_affinity_keys,
        )
        .await?;
        let raw_keys = target_affinities
            .iter()
            .map(|item| item.raw_key.clone())
            .collect::<Vec<_>>();
        let _ = delete_admin_monitoring_cache_affinity_raw_keys(state, &raw_keys).await?;
        clear_admin_monitoring_scheduler_affinity_entries(state, &target_affinities);

        let user = admin_monitoring_find_user_summary_by_id(state, &api_key.user_id).await?;
        let api_key_name = api_key
            .name
            .clone()
            .unwrap_or_else(|| user_identifier.clone());
        return Ok(Json(json!({
            "status": "ok",
            "message": format!("已清除 API Key {api_key_name} 的缓存亲和性"),
            "user_info": {
                "user_id": Some(api_key.user_id.clone()),
                "username": user.as_ref().map(|item| item.username.clone()),
                "email": user.and_then(|item| item.email),
                "api_key_id": user_identifier,
                "api_key_name": api_key.name.clone(),
            },
        }))
        .into_response());
    }

    let Some(user) = state.find_user_auth_by_identifier(&user_identifier).await? else {
        return Ok(admin_monitoring_cache_users_not_found_response(
            &user_identifier,
        ));
    };

    let user_api_key_ids = state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&user.id))
        .await?
        .into_iter()
        .map(|item| item.api_key_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let target_affinities =
        list_admin_monitoring_cache_affinity_records_by_affinity_keys(state, &user_api_key_ids)
            .await?;
    let raw_keys = target_affinities
        .iter()
        .map(|item| item.raw_key.clone())
        .collect::<Vec<_>>();
    let _ = delete_admin_monitoring_cache_affinity_raw_keys(state, &raw_keys).await?;
    clear_admin_monitoring_scheduler_affinity_entries(state, &target_affinities);

    Ok(Json(json!({
        "status": "ok",
        "message": format!("已清除用户 {} 的所有缓存亲和性", user.username),
        "user_info": {
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
        },
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_affinity_delete_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((affinity_key, endpoint_id, model_id, api_format)) =
        admin_monitoring_cache_affinity_delete_params_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response(
            "缺少 affinity_key、endpoint_id、model_id 或 api_format",
        ));
    };

    if state.redis_kv_runner().is_none()
        && load_admin_monitoring_cache_affinity_entries_for_tests(state).is_empty()
    {
        return Ok(admin_monitoring_cache_affinity_unavailable_response());
    }

    let target_affinity_keys =
        std::iter::once(affinity_key.clone()).collect::<std::collections::BTreeSet<_>>();
    let target_affinity = list_admin_monitoring_cache_affinity_records_by_affinity_keys(
        state,
        &target_affinity_keys,
    )
    .await?
    .into_iter()
    .find(|item| {
        item.affinity_key == affinity_key
            && item.endpoint_id.as_deref() == Some(endpoint_id.as_str())
            && item.model_name == model_id
            && item.api_format.eq_ignore_ascii_case(&api_format)
    });
    let Some(target_affinity) = target_affinity else {
        return Ok(admin_monitoring_not_found_response(
            "未找到指定的缓存亲和性记录",
        ));
    };

    let _ = delete_admin_monitoring_cache_affinity_raw_keys(
        state,
        std::slice::from_ref(&target_affinity.raw_key),
    )
    .await?;
    clear_admin_monitoring_scheduler_affinity_entries(
        state,
        std::slice::from_ref(&target_affinity),
    );

    let mut api_key_by_id = admin_monitoring_list_export_api_key_records_by_ids(
        state,
        std::slice::from_ref(&affinity_key),
    )
    .await?;
    let api_key_name = api_key_by_id
        .remove(&affinity_key)
        .and_then(|item| item.name)
        .unwrap_or_else(|| affinity_key.chars().take(8).collect::<String>());

    Ok(Json(json!({
        "status": "ok",
        "message": format!("已清除缓存亲和性: {api_key_name}"),
        "affinity_key": affinity_key,
        "endpoint_id": endpoint_id,
        "model_id": model_id,
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_flush_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    let raw_affinities = list_admin_monitoring_cache_affinity_records(state).await?;
    if state.redis_kv_runner().is_none() && raw_affinities.is_empty() {
        return Ok(admin_monitoring_cache_affinity_unavailable_response());
    }

    let raw_keys = raw_affinities
        .iter()
        .map(|item| item.raw_key.clone())
        .collect::<Vec<_>>();
    let deleted = delete_admin_monitoring_cache_affinity_raw_keys(state, &raw_keys).await?;
    clear_admin_monitoring_scheduler_affinity_entries(state, &raw_affinities);

    Ok(Json(json!({
        "status": "ok",
        "message": "已清除全部缓存亲和性",
        "deleted_affinities": deleted,
    }))
    .into_response())
}

async fn build_admin_monitoring_cache_provider_delete_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(provider_id) =
        admin_monitoring_cache_provider_id_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 provider_id"));
    };

    let raw_affinities = list_admin_monitoring_cache_affinity_records(state).await?;
    if state.redis_kv_runner().is_none() && raw_affinities.is_empty() {
        return Ok(admin_monitoring_cache_affinity_unavailable_response());
    }

    let target_affinities = raw_affinities
        .into_iter()
        .filter(|item| item.provider_id.as_deref() == Some(provider_id.as_str()))
        .collect::<Vec<_>>();
    if target_affinities.is_empty() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({
                "detail": format!("未找到 provider {provider_id} 的缓存亲和性记录")
            })),
        )
            .into_response());
    }

    let raw_keys = target_affinities
        .iter()
        .map(|item| item.raw_key.clone())
        .collect::<Vec<_>>();
    let deleted = delete_admin_monitoring_cache_affinity_raw_keys(state, &raw_keys).await?;
    clear_admin_monitoring_scheduler_affinity_entries(state, &target_affinities);

    Ok(Json(json!({
        "status": "ok",
        "message": format!("已清除 provider {provider_id} 的缓存亲和性"),
        "provider_id": provider_id,
        "deleted_affinities": deleted,
    }))
    .into_response())
}

async fn build_admin_monitoring_model_mapping_delete_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(admin_monitoring_redis_unavailable_response());
    }

    let mut raw_keys = list_admin_monitoring_namespaced_keys(state, "model:*").await?;
    raw_keys.extend(list_admin_monitoring_namespaced_keys(state, "global_model:*").await?);
    raw_keys.sort();
    raw_keys.dedup();
    let deleted_count = delete_admin_monitoring_namespaced_keys(state, &raw_keys).await?;

    Ok(Json(json!({
        "status": "ok",
        "message": "已清除所有模型映射缓存",
        "deleted_count": deleted_count,
    }))
    .into_response())
}

async fn build_admin_monitoring_model_mapping_delete_model_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(model_name) =
        admin_monitoring_cache_model_name_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 model_name"));
    };
    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(admin_monitoring_redis_unavailable_response());
    }

    let candidate_keys = [
        format!("global_model:resolve:{model_name}"),
        format!("global_model:name:{model_name}"),
    ];
    let mut existing_keys = Vec::new();
    for key in candidate_keys {
        let matches = list_admin_monitoring_namespaced_keys(state, key.as_str()).await?;
        existing_keys.extend(matches);
    }
    existing_keys.sort();
    existing_keys.dedup();

    let deleted_count = delete_admin_monitoring_namespaced_keys(state, &existing_keys).await?;
    let deleted_keys = if deleted_count == 0 {
        Vec::new()
    } else {
        existing_keys
    };

    Ok(Json(json!({
        "status": "ok",
        "message": format!("已清除模型 {model_name} 的映射缓存"),
        "model_name": model_name,
        "deleted_keys": deleted_keys,
    }))
    .into_response())
}

async fn build_admin_monitoring_model_mapping_delete_provider_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((provider_id, global_model_id)) =
        admin_monitoring_cache_model_mapping_provider_params_from_path(
            &request_context.request_path,
        )
    else {
        return Ok(admin_monitoring_bad_request_response(
            "缺少 provider_id 或 global_model_id",
        ));
    };
    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(admin_monitoring_redis_unavailable_response());
    }

    let candidate_keys = [
        format!("model:provider_global:{provider_id}:{global_model_id}"),
        format!("model:provider_global:hits:{provider_id}:{global_model_id}"),
    ];
    let mut existing_keys = Vec::new();
    for key in candidate_keys {
        let matches = list_admin_monitoring_namespaced_keys(state, key.as_str()).await?;
        existing_keys.extend(matches);
    }
    existing_keys.sort();
    existing_keys.dedup();

    let _ = delete_admin_monitoring_namespaced_keys(state, &existing_keys).await?;

    Ok(Json(json!({
        "status": "ok",
        "message": "已清除 Provider 模型映射缓存",
        "provider_id": provider_id,
        "global_model_id": global_model_id,
        "deleted_keys": existing_keys,
    }))
    .into_response())
}

async fn build_admin_monitoring_redis_keys_delete_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(category) =
        admin_monitoring_cache_redis_category_from_path(&request_context.request_path)
    else {
        return Ok(admin_monitoring_bad_request_response("缺少 category"));
    };

    let Some((cat_key, name, pattern, _description)) = ADMIN_MONITORING_REDIS_CACHE_CATEGORIES
        .iter()
        .find(|(cat_key, _, _, _)| *cat_key == category)
    else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("未知的缓存分类: {category}") })),
        )
            .into_response());
    };

    if state.redis_kv_runner().is_none() && !admin_monitoring_has_test_redis_keys(state) {
        return Ok(admin_monitoring_redis_unavailable_response());
    }

    let raw_keys = list_admin_monitoring_namespaced_keys(state, pattern).await?;
    let deleted_count = delete_admin_monitoring_namespaced_keys(state, &raw_keys).await?;

    Ok(Json(json!({
        "status": "ok",
        "message": format!("已清除 {name} 缓存"),
        "category": cat_key,
        "deleted_count": deleted_count,
    }))
    .into_response())
}

async fn build_admin_monitoring_system_status_response(
    state: &AppState,
) -> Result<Response<Body>, GatewayError> {
    let now = chrono::Utc::now();
    let today_start = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight should be valid")
        .and_utc();
    let recent_error_from = now - chrono::Duration::hours(1);
    let now_unix_secs = now.timestamp().max(0) as u64;

    let user_summary = state.summarize_export_users().await?;
    let total_users = user_summary.total;
    let active_users = user_summary.active;

    let providers = state
        .data
        .list_provider_catalog_providers(false)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let total_providers = providers.len();
    let active_providers = providers.iter().filter(|item| item.is_active).count();

    let user_api_key_summary = state
        .summarize_auth_api_key_export_non_standalone_records(now_unix_secs)
        .await?;
    let standalone_api_key_summary = state
        .summarize_auth_api_key_export_standalone_records(now_unix_secs)
        .await?;
    let total_api_keys = user_api_key_summary
        .total
        .saturating_add(standalone_api_key_summary.total);
    let active_api_keys = user_api_key_summary
        .active
        .saturating_add(standalone_api_key_summary.active);

    let today_usage = state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(today_start.timestamp().max(0) as u64),
            ..Default::default()
        })
        .await?;
    let today_requests = today_usage.len();
    let today_tokens = today_usage
        .iter()
        .map(|item| item.total_tokens)
        .sum::<u64>();
    let today_cost = today_usage
        .iter()
        .map(|item| item.total_cost_usd)
        .sum::<f64>();

    let recent_errors = state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(recent_error_from.timestamp().max(0) as u64),
            ..Default::default()
        })
        .await?
        .into_iter()
        .filter(admin_monitoring_usage_is_error)
        .count();

    Ok(Json(json!({
        "timestamp": now.to_rfc3339(),
        "users": {
            "total": total_users,
            "active": active_users,
        },
        "providers": {
            "total": total_providers,
            "active": active_providers,
        },
        "api_keys": {
            "total": total_api_keys,
            "active": active_api_keys,
        },
        "today_stats": {
            "requests": today_requests,
            "tokens": today_tokens,
            "cost_usd": format!("${today_cost:.4}"),
        },
        "recent_errors": recent_errors,
    }))
    .into_response())
}

async fn maybe_build_local_admin_monitoring_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(route) = match_admin_monitoring_route(
        &request_context.request_method,
        request_context.request_path.as_str(),
    ) else {
        return Ok(None);
    };

    match route {
        AdminMonitoringRoute::AuditLogs => Ok(Some(
            build_admin_monitoring_audit_logs_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::ResilienceStatus => Ok(Some(
            build_admin_monitoring_resilience_status_response(state).await?,
        )),
        AdminMonitoringRoute::ResilienceErrorStats => Ok(Some(
            build_admin_monitoring_reset_error_stats_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::ResilienceCircuitHistory => Ok(Some(
            build_admin_monitoring_resilience_circuit_history_response(state, request_context)
                .await?,
        )),
        AdminMonitoringRoute::CacheStats => Ok(Some(
            build_admin_monitoring_cache_stats_response(state).await?,
        )),
        AdminMonitoringRoute::CacheAffinities => Ok(Some(
            build_admin_monitoring_cache_affinities_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheAffinity => Ok(Some(
            build_admin_monitoring_cache_affinity_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheUsersDelete => Ok(Some(
            build_admin_monitoring_cache_users_delete_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheAffinityDelete => Ok(Some(
            build_admin_monitoring_cache_affinity_delete_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheFlush => Ok(Some(
            build_admin_monitoring_cache_flush_response(state).await?,
        )),
        AdminMonitoringRoute::CacheProviderDelete => Ok(Some(
            build_admin_monitoring_cache_provider_delete_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheModelMappingDelete => Ok(Some(
            build_admin_monitoring_model_mapping_delete_response(state).await?,
        )),
        AdminMonitoringRoute::CacheModelMappingDeleteModel => Ok(Some(
            build_admin_monitoring_model_mapping_delete_model_response(state, request_context)
                .await?,
        )),
        AdminMonitoringRoute::CacheModelMappingDeleteProvider => Ok(Some(
            build_admin_monitoring_model_mapping_delete_provider_response(state, request_context)
                .await?,
        )),
        AdminMonitoringRoute::CacheRedisKeysDelete => Ok(Some(
            build_admin_monitoring_redis_keys_delete_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::CacheMetrics => Ok(Some(
            build_admin_monitoring_cache_metrics_response(state).await?,
        )),
        AdminMonitoringRoute::CacheConfig => {
            Ok(Some(build_admin_monitoring_cache_config_response().await?))
        }
        AdminMonitoringRoute::CacheModelMappingStats => Ok(Some(
            build_admin_monitoring_model_mapping_stats_response(state).await?,
        )),
        AdminMonitoringRoute::CacheRedisKeys => Ok(Some(
            build_admin_monitoring_redis_cache_categories_response(state).await?,
        )),
        AdminMonitoringRoute::SystemStatus => Ok(Some(
            build_admin_monitoring_system_status_response(state).await?,
        )),
        AdminMonitoringRoute::SuspiciousActivities => Ok(Some(
            build_admin_monitoring_suspicious_activities_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::UserBehavior => Ok(Some(
            build_admin_monitoring_user_behavior_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::TraceRequest => Ok(Some(
            build_admin_monitoring_trace_request_response(state, request_context).await?,
        )),
        AdminMonitoringRoute::TraceProviderStats => Ok(Some(
            build_admin_monitoring_trace_provider_stats_response(state, request_context).await?,
        )),
        // All routes handled explicitly; no fallback needed.
    }
}

fn match_admin_monitoring_route(method: &http::Method, path: &str) -> Option<AdminMonitoringRoute> {
    let path = normalize_admin_monitoring_path(path);

    match *method {
        http::Method::GET => match path {
            "/api/admin/monitoring/audit-logs" => Some(AdminMonitoringRoute::AuditLogs),
            "/api/admin/monitoring/system-status" => Some(AdminMonitoringRoute::SystemStatus),
            "/api/admin/monitoring/suspicious-activities" => {
                Some(AdminMonitoringRoute::SuspiciousActivities)
            }
            "/api/admin/monitoring/resilience-status" => {
                Some(AdminMonitoringRoute::ResilienceStatus)
            }
            "/api/admin/monitoring/resilience/circuit-history" => {
                Some(AdminMonitoringRoute::ResilienceCircuitHistory)
            }
            "/api/admin/monitoring/cache/stats" => Some(AdminMonitoringRoute::CacheStats),
            "/api/admin/monitoring/cache/affinities" => Some(AdminMonitoringRoute::CacheAffinities),
            "/api/admin/monitoring/cache/config" => Some(AdminMonitoringRoute::CacheConfig),
            "/api/admin/monitoring/cache/metrics" => Some(AdminMonitoringRoute::CacheMetrics),
            "/api/admin/monitoring/cache/model-mapping/stats" => {
                Some(AdminMonitoringRoute::CacheModelMappingStats)
            }
            "/api/admin/monitoring/cache/redis-keys" => Some(AdminMonitoringRoute::CacheRedisKeys),
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/user-behavior/", 1) => {
                Some(AdminMonitoringRoute::UserBehavior)
            }
            _ if matches_dynamic_segments(
                path,
                "/api/admin/monitoring/trace/stats/provider/",
                1,
            ) =>
            {
                Some(AdminMonitoringRoute::TraceProviderStats)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/trace/", 1) => {
                Some(AdminMonitoringRoute::TraceRequest)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/cache/affinity/", 1) => {
                Some(AdminMonitoringRoute::CacheAffinity)
            }
            _ => None,
        },
        http::Method::DELETE => match path {
            "/api/admin/monitoring/resilience/error-stats" => {
                Some(AdminMonitoringRoute::ResilienceErrorStats)
            }
            "/api/admin/monitoring/cache" => Some(AdminMonitoringRoute::CacheFlush),
            "/api/admin/monitoring/cache/model-mapping" => {
                Some(AdminMonitoringRoute::CacheModelMappingDelete)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/cache/users/", 1) => {
                Some(AdminMonitoringRoute::CacheUsersDelete)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/cache/providers/", 1) => {
                Some(AdminMonitoringRoute::CacheProviderDelete)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/cache/redis-keys/", 1) => {
                Some(AdminMonitoringRoute::CacheRedisKeysDelete)
            }
            _ if matches_dynamic_segments(
                path,
                "/api/admin/monitoring/cache/model-mapping/provider/",
                2,
            ) =>
            {
                Some(AdminMonitoringRoute::CacheModelMappingDeleteProvider)
            }
            _ if matches_dynamic_segments(
                path,
                "/api/admin/monitoring/cache/model-mapping/",
                1,
            ) =>
            {
                Some(AdminMonitoringRoute::CacheModelMappingDeleteModel)
            }
            _ if matches_dynamic_segments(path, "/api/admin/monitoring/cache/affinity/", 4) => {
                Some(AdminMonitoringRoute::CacheAffinityDelete)
            }
            _ => None,
        },
        _ => None,
    }
}

fn normalize_admin_monitoring_path(path: &str) -> &str {
    let normalized = path.trim_end_matches('/');
    if normalized.is_empty() {
        "/"
    } else {
        normalized
    }
}

fn matches_dynamic_segments(path: &str, prefix: &str, dynamic_segments: usize) -> bool {
    let Some(suffix) = path.strip_prefix(prefix) else {
        return false;
    };

    let segments = suffix
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    segments.len() == dynamic_segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
    use axum::body::to_bytes;
    use axum::http::Uri;
    use std::sync::Arc;

    use aether_data::repository::auth::{
        InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeyExportRecord,
    };
    use aether_data::repository::candidates::{
        InMemoryRequestCandidateRepository, RequestCandidateStatus, StoredRequestCandidate,
    };
    use aether_data::repository::provider_catalog::{
        InMemoryProviderCatalogReadRepository, StoredProviderCatalogEndpoint,
        StoredProviderCatalogKey, StoredProviderCatalogProvider,
    };
    use aether_data::repository::usage::{InMemoryUsageReadRepository, StoredRequestUsageAudit};
    use aether_data::repository::users::{
        InMemoryUserReadRepository, StoredUserAuthRecord, StoredUserExportRow,
    };

    fn request_context(method: http::Method, uri: &str) -> GatewayPublicRequestContext {
        GatewayPublicRequestContext::from_request_parts(
            "trace-123",
            &method,
            &uri.parse::<Uri>().expect("uri should parse"),
            &http::HeaderMap::new(),
            None,
        )
    }

    #[test]
    fn admin_monitoring_matches_typical_routes() {
        assert_eq!(
            match_admin_monitoring_route(&http::Method::GET, "/api/admin/monitoring/audit-logs"),
            Some(AdminMonitoringRoute::AuditLogs)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::GET,
                "/api/admin/monitoring/trace/request-1"
            ),
            Some(AdminMonitoringRoute::TraceRequest)
        );
        assert_eq!(
            match_admin_monitoring_route(&http::Method::GET, "/api/admin/monitoring/cache/stats"),
            Some(AdminMonitoringRoute::CacheStats)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::GET,
                "/api/admin/monitoring/resilience-status"
            ),
            Some(AdminMonitoringRoute::ResilienceStatus)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::GET,
                "/api/admin/monitoring/user-behavior/user-1"
            ),
            Some(AdminMonitoringRoute::UserBehavior)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::GET,
                "/api/admin/monitoring/trace/stats/provider/provider-1"
            ),
            Some(AdminMonitoringRoute::TraceProviderStats)
        );
    }

    #[test]
    fn admin_monitoring_matches_cache_delete_shapes_and_trailing_slashes() {
        assert_eq!(
            match_admin_monitoring_route(&http::Method::DELETE, "/api/admin/monitoring/cache/"),
            Some(AdminMonitoringRoute::CacheFlush)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::DELETE,
                "/api/admin/monitoring/cache/model-mapping/provider/provider-1/model-1"
            ),
            Some(AdminMonitoringRoute::CacheModelMappingDeleteProvider)
        );
        assert_eq!(
            match_admin_monitoring_route(
                &http::Method::DELETE,
                "/api/admin/monitoring/cache/affinity/a/b/c/d"
            ),
            Some(AdminMonitoringRoute::CacheAffinityDelete)
        );
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_delete_requires_redis_without_runtime_or_test_entries()
    {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::DELETE,
            "/api/admin/monitoring/cache/model-mapping",
        );
        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("monitoring route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(
            payload,
            json!({ "detail": ADMIN_MONITORING_REDIS_REQUIRED_DETAIL })
        );
    }

    #[tokio::test]
    async fn admin_monitoring_user_behavior_returns_empty_local_payload_without_postgres() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/user-behavior/user-123?days=30",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("user behavior route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["user_id"], json!("user-123"));
        assert_eq!(payload["period_days"], json!(30));
        assert_eq!(payload["event_counts"], json!({}));
        assert_eq!(payload["failed_requests"], json!(0));
        assert_eq!(payload["success_requests"], json!(0));
        assert_eq!(payload["success_rate"], json!(0.0));
        assert_eq!(payload["suspicious_activities"], json!(0));
        assert!(payload["analysis_time"].as_str().is_some());
    }

    #[tokio::test]
    async fn admin_monitoring_audit_logs_returns_empty_local_payload_without_postgres() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/audit-logs?username=alice&event_type=login_failed&days=14&limit=20&offset=5",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("monitoring route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["items"], json!([]));
        assert_eq!(payload["meta"]["total"], json!(0));
        assert_eq!(payload["meta"]["limit"], json!(20));
        assert_eq!(payload["meta"]["offset"], json!(5));
        assert_eq!(payload["meta"]["count"], json!(0));
        assert_eq!(payload["filters"]["username"], json!("alice"));
        assert_eq!(payload["filters"]["event_type"], json!("login_failed"));
        assert_eq!(payload["filters"]["days"], json!(14));
    }

    #[tokio::test]
    async fn admin_monitoring_suspicious_activities_returns_empty_local_payload_without_postgres() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/suspicious-activities?hours=48",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("monitoring route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["activities"], json!([]));
        assert_eq!(payload["count"], json!(0));
        assert_eq!(payload["time_range_hours"], json!(48));
    }

    #[tokio::test]
    async fn admin_monitoring_resilience_status_returns_local_payload() {
        let now = chrono::Utc::now().timestamp();
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![],
            vec![sample_key().with_health_fields(
                Some(json!({
                    "openai:chat": {
                        "health_score": 0.25,
                        "consecutive_failures": 3,
                        "last_failure_at": "2026-03-30T12:00:00+00:00"
                    }
                })),
                Some(json!({
                    "openai:chat": {
                        "open": true
                    }
                })),
            )],
        ));
        let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
            sample_usage(
                "request-recent-failed",
                "provider-1",
                "OpenAI",
                10,
                0.10,
                "failed",
                Some(502),
                now - 120,
            ),
            sample_usage(
                "request-old-failed",
                "provider-1",
                "OpenAI",
                12,
                0.15,
                "failed",
                Some(500),
                now - 172_800,
            ),
        ]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_provider_catalog_and_usage_reader_for_tests(
                    provider_catalog,
                    usage_repository,
                ),
            );
        let context = request_context(http::Method::GET, "/api/admin/monitoring/resilience-status");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["health_score"], json!(78));
        assert_eq!(payload["status"], json!("degraded"));
        assert_eq!(payload["error_statistics"]["total_errors"], json!(1));
        assert_eq!(
            payload["error_statistics"]["open_circuit_breakers"],
            json!(1)
        );
        assert_eq!(
            payload["error_statistics"]["circuit_breakers"]["provider-key-1"]["state"],
            json!("open")
        );
        assert_eq!(payload["recent_errors"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            payload["recent_errors"][0]["error_id"],
            json!("usage-request-recent-failed")
        );
        let recommendations = payload["recommendations"]
            .as_array()
            .expect("recommendations should be array");
        assert!(recommendations.iter().any(|item| item
            .as_str()
            .is_some_and(|value| value.contains("prod-key"))));
        assert!(payload["timestamp"].as_str().is_some());
    }

    #[tokio::test]
    async fn admin_monitoring_cache_stats_returns_local_payload() {
        let now = chrono::Utc::now().timestamp();
        let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
            sample_usage(
                "request-cache-hit",
                "provider-1",
                "OpenAI",
                20,
                0.20,
                "success",
                Some(200),
                now - 60,
            )
            .with_cache_input_tokens(10, 5),
            sample_usage(
                "request-cache-miss",
                "provider-1",
                "OpenAI",
                15,
                0.10,
                "success",
                Some(200),
                now - 120,
            ),
        ]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_usage_reader_for_tests(
                    usage_repository,
                )
                .with_system_config_values_for_tests([
                    ("scheduling_mode".to_string(), json!("cache_affinity")),
                    ("provider_priority_mode".to_string(), json!("provider")),
                ]),
            );
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/stats");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["data"]["scheduler"], json!("cache_aware"));
        assert_eq!(payload["data"]["total_affinities"], json!(0));
        assert_eq!(payload["data"]["cache_hits"], json!(1));
        assert_eq!(payload["data"]["cache_misses"], json!(1));
        assert_eq!(payload["data"]["cache_hit_rate"], json!(0.5));
        assert_eq!(
            payload["data"]["scheduler_metrics"]["scheduling_mode"],
            json!("cache_affinity")
        );
        assert_eq!(
            payload["data"]["affinity_stats"]["storage_type"],
            json!("memory")
        );
        assert_eq!(
            payload["data"]["affinity_stats"]["config"]["default_ttl"],
            json!(300)
        );
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinities_returns_empty_payload_without_runtime_or_test_entries(
    ) {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/affinities");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["data"]["items"], json!([]));
        assert_eq!(payload["data"]["meta"]["total"], json!(0));
        assert_eq!(payload["data"]["meta"]["count"], json!(0));
        assert_eq!(payload["data"]["matched_user_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinity_returns_not_found_without_runtime_or_test_entries() {
        let user_repository = Arc::new(
            InMemoryUserReadRepository::seed_auth_users(vec![sample_monitoring_auth_user(
                "user-1",
            )])
            .with_export_users(vec![sample_monitoring_export_user("user-1")]),
        );
        let auth_repository = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::default().with_export_records(vec![
                sample_monitoring_export_api_key("user-1", "user-key-1"),
            ]),
        );
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_user_reader_for_tests(user_repository)
                    .with_auth_api_key_reader(auth_repository),
            );
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/cache/affinity/alice",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("not_found"));
        assert_eq!(payload["user_info"]["user_id"], json!("user-1"));
        assert_eq!(payload["affinities"], json!([]));
        assert_eq!(
            payload["message"],
            json!("用户 alice (alice@example.com) 没有缓存亲和性")
        );
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinities_and_affinity_return_local_payload_from_test_store() {
        let user_repository = Arc::new(
            InMemoryUserReadRepository::seed_auth_users(vec![sample_monitoring_auth_user(
                "user-1",
            )])
            .with_export_users(vec![sample_monitoring_export_user("user-1")]),
        );
        let auth_repository = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::default().with_export_records(vec![
                sample_monitoring_export_api_key("user-1", "user-key-1"),
            ]),
        );
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![sample_monitoring_catalog_endpoint()],
            vec![sample_monitoring_catalog_key()],
        ));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog,
                )
                .with_user_reader(user_repository)
                .with_auth_api_key_reader(auth_repository),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-2:openai:model-beta",
                json!({
                    "provider_id": "provider-2",
                    "endpoint_id": "endpoint-2",
                    "key_id": "provider-key-2",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 4,
                }),
            );

        let list_context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/cache/affinities?keyword=alice&limit=20&offset=0",
        );
        let list_response = maybe_build_local_admin_monitoring_response(&state, &list_context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");
        assert_eq!(list_response.status(), http::StatusCode::OK);
        let list_body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let list_payload: serde_json::Value =
            serde_json::from_slice(&list_body).expect("json body should parse");
        assert_eq!(list_payload["status"], json!("ok"));
        assert_eq!(list_payload["data"]["meta"]["total"], json!(1));
        assert_eq!(list_payload["data"]["matched_user_id"], json!("user-1"));
        assert_eq!(
            list_payload["data"]["items"][0]["affinity_key"],
            json!("user-key-1")
        );
        assert_eq!(list_payload["data"]["items"][0]["username"], json!("alice"));
        assert_eq!(
            list_payload["data"]["items"][0]["provider_name"],
            json!("OpenAI")
        );
        assert_eq!(
            list_payload["data"]["items"][0]["endpoint_url"],
            json!("https://api.openai.example/v1")
        );
        assert_eq!(
            list_payload["data"]["items"][0]["key_name"],
            json!("prod-key")
        );
        assert_eq!(list_payload["data"]["items"][0]["request_count"], json!(7));

        let detail_context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/cache/affinity/alice",
        );
        let detail_response = maybe_build_local_admin_monitoring_response(&state, &detail_context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");
        assert_eq!(detail_response.status(), http::StatusCode::OK);
        let detail_body = to_bytes(detail_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let detail_payload: serde_json::Value =
            serde_json::from_slice(&detail_body).expect("json body should parse");
        assert_eq!(detail_payload["status"], json!("ok"));
        assert_eq!(detail_payload["user_info"]["user_id"], json!("user-1"));
        assert_eq!(
            detail_payload["affinities"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            detail_payload["affinities"][0]["api_format"],
            json!("openai")
        );
        assert_eq!(detail_payload["total_endpoints"], json!(1));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_users_delete_returns_local_payload_from_test_store() {
        let user_repository = Arc::new(
            InMemoryUserReadRepository::seed_auth_users(vec![sample_monitoring_auth_user(
                "user-1",
            )])
            .with_export_users(vec![sample_monitoring_export_user("user-1")]),
        );
        let auth_repository = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::default().with_export_records(vec![
                sample_monitoring_export_api_key("user-1", "user-key-1"),
            ]),
        );
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_user_reader_for_tests(user_repository)
                    .with_auth_api_key_reader(auth_repository),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-2:openai:model-beta",
                json!({
                    "provider_id": "provider-2",
                    "endpoint_id": "endpoint-2",
                    "key_id": "provider-key-2",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 4,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/users/alice",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(
            payload["message"],
            json!("已清除用户 alice 的所有缓存亲和性")
        );
        assert_eq!(payload["user_info"]["user_id"], json!("user-1"));
        let remaining = state.list_admin_monitoring_cache_affinity_entries_for_tests();
        assert_eq!(remaining.len(), 1);
        assert!(remaining
            .iter()
            .any(|(key, _)| key == "cache_affinity:user-key-2:openai:model-beta"));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_users_delete_returns_not_found_for_unknown_identifier() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/users/unknown",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(
            payload["detail"],
            json!("无法识别的标识符: unknown。支持用户名、邮箱、User ID或API Key ID")
        );
    }

    #[tokio::test]
    async fn admin_monitoring_cache_flush_returns_local_payload_from_test_store() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-2:openai:model-beta",
                json!({
                    "provider_id": "provider-2",
                    "endpoint_id": "endpoint-2",
                    "key_id": "provider-key-2",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 4,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(http::Method::DELETE, "/api/admin/monitoring/cache"),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["message"], json!("已清除全部缓存亲和性"));
        assert_eq!(payload["deleted_affinities"], json!(2));
        assert!(state
            .list_admin_monitoring_cache_affinity_entries_for_tests()
            .is_empty());
    }

    #[tokio::test]
    async fn admin_monitoring_cache_provider_delete_returns_local_payload_from_test_store() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-2:openai:model-beta",
                json!({
                    "provider_id": "provider-2",
                    "endpoint_id": "endpoint-2",
                    "key_id": "provider-key-2",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 4,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/providers/provider-1",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(
            payload["message"],
            json!("已清除 provider provider-1 的缓存亲和性")
        );
        assert_eq!(payload["provider_id"], json!("provider-1"));
        assert_eq!(payload["deleted_affinities"], json!(1));
        assert_eq!(
            state
                .list_admin_monitoring_cache_affinity_entries_for_tests()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_delete_returns_local_payload_from_test_store() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_redis_key_for_tests("model:id:model-1", json!({"id": "model-1"}))
            .with_admin_monitoring_redis_key_for_tests(
                "model:provider_global:provider-1:model-alpha",
                json!({"provider_id": "provider-1", "global_model_id": "model-alpha"}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "global_model:name:model-alpha",
                json!({"name": "model-alpha"}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "global_model:resolve:model-alpha",
                json!({"id": "model-alpha"}),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/model-mapping",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["message"], json!("已清除所有模型映射缓存"));
        assert_eq!(payload["deleted_count"], json!(4));
        assert!(state
            .list_admin_monitoring_redis_keys_for_tests()
            .is_empty());
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_delete_model_returns_local_payload_from_test_store() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_redis_key_for_tests(
                "global_model:name:model-alpha",
                json!({"name": "model-alpha"}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "global_model:resolve:model-alpha",
                json!({"id": "model-alpha"}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "global_model:name:model-beta",
                json!({"name": "model-beta"}),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/model-mapping/model-alpha",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["model_name"], json!("model-alpha"));
        assert_eq!(
            payload["deleted_keys"],
            json!([
                "global_model:name:model-alpha",
                "global_model:resolve:model-alpha"
            ])
        );
        assert_eq!(
            state.list_admin_monitoring_redis_keys_for_tests(),
            vec!["global_model:name:model-beta".to_string()]
        );
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_delete_provider_returns_local_payload_from_test_store()
    {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_redis_key_for_tests(
                "model:provider_global:provider-1:model-alpha",
                json!({"provider_id": "provider-1"}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "model:provider_global:hits:provider-1:model-alpha",
                json!(12),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "model:provider_global:provider-2:model-alpha",
                json!({"provider_id": "provider-2"}),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/model-mapping/provider/provider-1/model-alpha",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["provider_id"], json!("provider-1"));
        assert_eq!(payload["global_model_id"], json!("model-alpha"));
        assert_eq!(
            payload["deleted_keys"],
            json!([
                "model:provider_global:hits:provider-1:model-alpha",
                "model:provider_global:provider-1:model-alpha"
            ])
        );
        assert_eq!(
            state.list_admin_monitoring_redis_keys_for_tests(),
            vec!["model:provider_global:provider-2:model-alpha".to_string()]
        );
    }

    #[tokio::test]
    async fn admin_monitoring_redis_keys_delete_returns_local_payload_from_test_store() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_redis_key_for_tests(
                "dashboard:summary:user-1",
                json!({"ok": true}),
            )
            .with_admin_monitoring_redis_key_for_tests(
                "dashboard:stats:user-1",
                json!({"ok": true}),
            )
            .with_admin_monitoring_redis_key_for_tests("user:user-1", json!({"ok": true}));

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/redis-keys/dashboard",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["category"], json!("dashboard"));
        assert_eq!(payload["deleted_count"], json!(2));
        assert_eq!(payload["message"], json!("已清除 仪表盘 缓存"));
        assert_eq!(
            state.list_admin_monitoring_redis_keys_for_tests(),
            vec!["user:user-1".to_string()]
        );
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinity_delete_returns_local_payload_from_test_store() {
        let user_repository = Arc::new(
            InMemoryUserReadRepository::seed_auth_users(vec![sample_monitoring_auth_user(
                "user-1",
            )])
            .with_export_users(vec![sample_monitoring_export_user("user-1")]),
        );
        let auth_repository = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::default().with_export_records(vec![
                sample_monitoring_export_api_key("user-1", "user-key-1"),
            ]),
        );
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_user_reader_for_tests(user_repository)
                    .with_auth_api_key_reader(auth_repository),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            )
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-2:openai:model-beta",
                json!({
                    "provider_id": "provider-2",
                    "endpoint_id": "endpoint-2",
                    "key_id": "provider-key-2",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 4,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/affinity/user-key-1/endpoint-1/model-alpha/openai",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["message"], json!("已清除缓存亲和性: Alice Key"));
        assert_eq!(payload["affinity_key"], json!("user-key-1"));
        assert_eq!(payload["endpoint_id"], json!("endpoint-1"));
        assert_eq!(payload["model_id"], json!("model-alpha"));
        let remaining = state.list_admin_monitoring_cache_affinity_entries_for_tests();
        assert_eq!(remaining.len(), 1);
        assert!(remaining
            .iter()
            .any(|(key, _)| key == "cache_affinity:user-key-2:openai:model-beta"));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinity_delete_returns_not_found_for_mismatched_endpoint() {
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_admin_monitoring_cache_affinity_entry_for_tests(
                "cache_affinity:user-key-1:openai:model-alpha",
                json!({
                    "provider_id": "provider-1",
                    "endpoint_id": "endpoint-1",
                    "key_id": "provider-key-1",
                    "created_at": 1710000000,
                    "expire_at": 1710000300,
                    "request_count": 7,
                }),
            );

        let response = maybe_build_local_admin_monitoring_response(
            &state,
            &request_context(
                http::Method::DELETE,
                "/api/admin/monitoring/cache/affinity/user-key-1/endpoint-2/model-alpha/openai",
            ),
        )
        .await
        .expect("handler should not error")
        .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["detail"], json!("未找到指定的缓存亲和性记录"));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_metrics_returns_local_payload() {
        let now = chrono::Utc::now().timestamp();
        let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
            sample_usage(
                "request-cache-hit",
                "provider-1",
                "OpenAI",
                20,
                0.20,
                "success",
                Some(200),
                now - 60,
            )
            .with_cache_input_tokens(10, 5),
            sample_usage(
                "request-cache-miss",
                "provider-1",
                "OpenAI",
                15,
                0.10,
                "success",
                Some(200),
                now - 120,
            ),
        ]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_usage_reader_for_tests(
                    usage_repository,
                )
                .with_system_config_values_for_tests([
                    ("scheduling_mode".to_string(), json!("cache_affinity")),
                    ("provider_priority_mode".to_string(), json!("provider")),
                ]),
            );
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/metrics");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get(http::header::CONTENT_TYPE),
            Some(&http::HeaderValue::from_static(
                "text/plain; version=0.0.4; charset=utf-8"
            ))
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload = String::from_utf8(body.to_vec()).expect("body should be utf8");
        assert!(payload
            .contains("# HELP cache_scheduler_cache_hits Cache hits counted during scheduling"));
        assert!(payload.contains("cache_scheduler_cache_hits 1"));
        assert!(payload.contains("cache_scheduler_cache_misses 1"));
        assert!(payload.contains("cache_scheduler_cache_hit_rate 0.5"));
        assert!(payload.contains("cache_affinity_total 0"));
        assert!(payload.contains("cache_scheduler_info{scheduler=\"cache_aware\"} 1"));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_config_returns_local_payload() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/config");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["data"]["cache_ttl_seconds"], json!(300));
        assert_eq!(payload["data"]["cache_reservation_ratio"], json!(0.1));
        assert_eq!(
            payload["data"]["dynamic_reservation"]["enabled"],
            json!(true)
        );
        assert_eq!(
            payload["data"]["dynamic_reservation"]["config"]["probe_phase_requests"],
            json!(100)
        );
        assert_eq!(
            payload["data"]["dynamic_reservation"]["config"]["stable_max_reservation"],
            json!(0.35)
        );
        assert_eq!(
            payload["data"]["description"]["dynamic_reservation"],
            json!("动态预留机制配置")
        );
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_stats_returns_local_payload_without_redis() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/cache/model-mapping/stats",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["data"]["available"], json!(false));
        assert_eq!(
            payload["data"]["message"],
            json!("Redis 未启用，模型映射缓存不可用")
        );
    }

    #[tokio::test]
    async fn admin_monitoring_reset_error_stats_returns_local_payload_and_clears_future_snapshot() {
        let now = chrono::Utc::now().timestamp();
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![],
            vec![sample_key().with_health_fields(
                Some(json!({
                    "openai:chat": {
                        "health_score": 0.25,
                        "consecutive_failures": 3,
                        "last_failure_at": "2026-03-30T12:00:00+00:00"
                    }
                })),
                Some(json!({
                    "openai:chat": {
                        "open": true
                    }
                })),
            )],
        ));
        let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![sample_usage(
            "request-recent-failed",
            "provider-1",
            "OpenAI",
            10,
            0.10,
            "failed",
            Some(502),
            now - 120,
        )]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_provider_catalog_and_usage_reader_for_tests(
                    provider_catalog,
                    usage_repository,
                ),
            );

        let reset_context = request_context(
            http::Method::DELETE,
            "/api/admin/monitoring/resilience/error-stats",
        );
        let reset_response = maybe_build_local_admin_monitoring_response(&state, &reset_context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(reset_response.status(), http::StatusCode::OK);
        let body = to_bytes(reset_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["message"], json!("错误统计已重置"));
        assert_eq!(payload["previous_stats"]["total_errors"], json!(1));
        assert_eq!(payload["previous_stats"]["recent_errors"], json!(1));
        assert_eq!(
            payload["previous_stats"]["circuit_breakers"]["provider-key-1"]["state"],
            json!("open")
        );
        assert_eq!(payload["reset_by"], serde_json::Value::Null);
        assert!(payload["reset_at"].as_str().is_some());

        let status_context =
            request_context(http::Method::GET, "/api/admin/monitoring/resilience-status");
        let status_response = maybe_build_local_admin_monitoring_response(&state, &status_context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(status_response.status(), http::StatusCode::OK);
        let body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["error_statistics"]["total_errors"], json!(0));
        assert_eq!(payload["recent_errors"], json!([]));
        assert_eq!(
            payload["error_statistics"]["open_circuit_breakers"],
            json!(1)
        );
    }

    #[tokio::test]
    async fn admin_monitoring_redis_keys_returns_local_payload_without_redis() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/redis-keys");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["status"], json!("ok"));
        assert_eq!(payload["data"]["available"], json!(false));
        assert_eq!(payload["data"]["message"], json!("Redis 未启用"));
    }

    #[tokio::test]
    async fn admin_monitoring_redis_keys_delete_returns_unavailable_without_redis() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::DELETE,
            "/api/admin/monitoring/cache/redis-keys/upstream_models",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["detail"], json!("Redis 未启用"));
    }

    #[tokio::test]
    async fn admin_monitoring_circuit_history_returns_local_payload() {
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![],
            vec![sample_key().with_health_fields(
                Some(json!({
                    "openai:chat": {
                        "health_score": 0.25,
                        "consecutive_failures": 3,
                        "last_failure_at": "2026-03-30T12:00:00+00:00"
                    }
                })),
                Some(json!({
                    "openai:chat": {
                        "open": true,
                        "open_at": "2026-03-30T12:00:00+00:00",
                        "next_probe_at": "2026-03-30T12:05:00+00:00",
                        "reason": "错误率过高"
                    }
                })),
            )],
        ));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog,
                ),
            );
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/resilience/circuit-history?limit=10",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["count"], json!(1));
        assert_eq!(payload["items"][0]["event"], json!("opened"));
        assert_eq!(payload["items"][0]["key_id"], json!("provider-key-1"));
        assert_eq!(payload["items"][0]["provider_name"], json!("OpenAI"));
        assert_eq!(payload["items"][0]["api_format"], json!("openai:chat"));
        assert_eq!(payload["items"][0]["reason"], json!("错误率过高"));
        assert_eq!(payload["items"][0]["recovery_seconds"], json!(300));
        assert_eq!(
            payload["items"][0]["timestamp"],
            json!("2026-03-30T12:00:00+00:00")
        );
    }

    fn sample_usage(
        request_id: &str,
        provider_id: &str,
        provider_name: &str,
        total_tokens: i32,
        total_cost_usd: f64,
        status: &str,
        status_code: Option<i32>,
        created_at_unix_secs: i64,
    ) -> StoredRequestUsageAudit {
        let is_error = status_code.is_some_and(|value| value >= 400)
            || status.trim().eq_ignore_ascii_case("failed")
            || status.trim().eq_ignore_ascii_case("error");
        StoredRequestUsageAudit::new(
            format!("usage-{request_id}"),
            request_id.to_string(),
            Some("user-1".to_string()),
            Some("api-key-1".to_string()),
            Some("alice".to_string()),
            Some("monitoring-key".to_string()),
            provider_name.to_string(),
            "gpt-4.1".to_string(),
            None,
            Some(provider_id.to_string()),
            Some("endpoint-1".to_string()),
            Some("provider-key-1".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            false,
            false,
            total_tokens / 2,
            total_tokens / 2,
            total_tokens,
            total_cost_usd,
            total_cost_usd,
            status_code,
            is_error.then(|| "boom".to_string()),
            is_error.then(|| "upstream_error".to_string()),
            Some(120),
            Some(30),
            status.to_string(),
            "billed".to_string(),
            created_at_unix_secs,
            created_at_unix_secs,
            Some(created_at_unix_secs),
        )
        .expect("usage should build")
    }

    fn sample_candidate(
        id: &str,
        request_id: &str,
        candidate_index: i32,
        status: RequestCandidateStatus,
        started_at_unix_secs: Option<i64>,
        latency_ms: Option<i32>,
        status_code: Option<i32>,
    ) -> StoredRequestCandidate {
        StoredRequestCandidate::new(
            id.to_string(),
            request_id.to_string(),
            Some("user-1".to_string()),
            Some("api-key-1".to_string()),
            Some("alice".to_string()),
            Some("default".to_string()),
            candidate_index,
            0,
            Some("provider-1".to_string()),
            Some("endpoint-1".to_string()),
            Some("provider-key-1".to_string()),
            status,
            None,
            false,
            status_code,
            None,
            None,
            latency_ms,
            Some(1),
            None,
            Some(json!({"cache_1h": true})),
            100 + i64::from(candidate_index),
            started_at_unix_secs,
            started_at_unix_secs.map(|value| value + 1),
        )
        .expect("candidate should build")
    }

    fn sample_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-1".to_string(),
            "OpenAI".to_string(),
            Some("https://openai.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
    }

    fn sample_inactive_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-2".to_string(),
            "Anthropic".to_string(),
            Some("https://anthropic.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(false, false, false, None, None, None, None, None, None)
    }

    fn sample_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-1".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
    }

    fn sample_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "provider-key-1".to_string(),
            "provider-1".to_string(),
            "prod-key".to_string(),
            "api_key".to_string(),
            Some(json!({"cache_1h": true})),
            true,
        )
        .expect("key should build")
    }

    fn sample_monitoring_auth_user(user_id: &str) -> StoredUserAuthRecord {
        StoredUserAuthRecord::new(
            user_id.to_string(),
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            None,
            "user".to_string(),
            "local".to_string(),
            None,
            None,
            None,
            true,
            false,
            None,
            None,
        )
        .expect("auth user should build")
    }

    fn sample_monitoring_export_user(user_id: &str) -> StoredUserExportRow {
        StoredUserExportRow::new(
            user_id.to_string(),
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            None,
            "user".to_string(),
            "local".to_string(),
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .expect("export user should build")
    }

    fn sample_monitoring_export_api_key(
        user_id: &str,
        api_key_id: &str,
    ) -> StoredAuthApiKeyExportRecord {
        StoredAuthApiKeyExportRecord::new(
            user_id.to_string(),
            api_key_id.to_string(),
            format!("hash-{api_key_id}"),
            Some(
                encrypt_python_fernet_plaintext(
                    DEVELOPMENT_ENCRYPTION_KEY,
                    "sk-user-monitoring-1234",
                )
                .expect("user key should encrypt"),
            ),
            Some("Alice Key".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            true,
            None,
            false,
            0,
            0.0,
            false,
        )
        .expect("export api key should build")
    }

    fn sample_monitoring_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        sample_endpoint()
            .with_transport_fields(
                "https://api.openai.example/v1".to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("endpoint transport fields should build")
    }

    fn sample_monitoring_catalog_key() -> StoredProviderCatalogKey {
        sample_key()
            .with_transport_fields(
                None,
                encrypt_python_fernet_plaintext(
                    DEVELOPMENT_ENCRYPTION_KEY,
                    "sk-upstream-monitoring-5678",
                )
                .expect("provider key should encrypt"),
                None,
                Some(json!({"cache": 1.0})),
                None,
                None,
                None,
                None,
                None,
            )
            .expect("provider key transport fields should build")
    }

    #[tokio::test]
    async fn admin_monitoring_trace_request_returns_local_not_found_when_missing() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/trace/request-123?attempted_only=true",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("trace route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload, json!({ "detail": "Request not found" }));
    }

    #[tokio::test]
    async fn admin_monitoring_cache_affinity_returns_bad_request_when_identifier_missing() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(http::Method::GET, "/api/admin/monitoring/cache/affinity/");

        let response = build_admin_monitoring_cache_affinity_response(&state, &context)
            .await
            .expect("handler should not error");

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload, json!({ "detail": "缺少 user_identifier" }));
    }

    #[tokio::test]
    async fn admin_monitoring_model_mapping_delete_model_returns_bad_request_when_missing_model_name(
    ) {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(
            http::Method::DELETE,
            "/api/admin/monitoring/cache/model-mapping/",
        );

        let response = build_admin_monitoring_model_mapping_delete_model_response(&state, &context)
            .await
            .expect("handler should not error");

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload, json!({ "detail": "缺少 model_name" }));
    }

    #[tokio::test]
    async fn admin_monitoring_redis_keys_delete_returns_bad_request_when_category_missing() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");
        let context = request_context(http::Method::DELETE, "/api/admin/monitoring/cache/redis-keys/");

        let response = build_admin_monitoring_redis_keys_delete_response(&state, &context)
            .await
            .expect("handler should not error");

        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload, json!({ "detail": "缺少 category" }));
    }

    #[tokio::test]
    async fn admin_monitoring_trace_request_returns_local_trace_payload() {
        let request_candidates = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
            sample_candidate(
                "cand-unused",
                "req-1",
                0,
                RequestCandidateStatus::Pending,
                None,
                None,
                None,
            ),
            sample_candidate(
                "cand-used",
                "req-1",
                1,
                RequestCandidateStatus::Failed,
                Some(101),
                Some(33),
                Some(502),
            ),
        ]));
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![sample_endpoint()],
            vec![sample_key()],
        ));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_decision_trace_data_readers_for_tests(request_candidates, provider_catalog);
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/trace/req-1?attempted_only=true",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("trace route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["request_id"], json!("req-1"));
        assert_eq!(payload["total_candidates"], json!(1));
        assert_eq!(payload["final_status"], json!("failed"));
        assert_eq!(payload["total_latency_ms"], json!(33));
        assert_eq!(payload["candidates"][0]["id"], json!("cand-used"));
        assert_eq!(payload["candidates"][0]["provider_name"], json!("OpenAI"));
        assert_eq!(
            payload["candidates"][0]["provider_website"],
            json!("https://openai.com")
        );
        assert_eq!(
            payload["candidates"][0]["endpoint_name"],
            json!("openai:chat")
        );
        assert_eq!(payload["candidates"][0]["key_name"], json!("prod-key"));
        assert_eq!(payload["candidates"][0]["key_auth_type"], json!("api_key"));
        assert_eq!(payload["candidates"][0]["latency_ms"], json!(33));
        assert_eq!(payload["candidates"][0]["status_code"], json!(502));
    }

    #[tokio::test]
    async fn admin_monitoring_system_status_returns_local_payload() {
        let now = chrono::Utc::now().timestamp();
        let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider(), sample_inactive_provider()],
            vec![],
            vec![],
        ));
        let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
            sample_usage(
                "request-today-ok",
                "provider-1",
                "OpenAI",
                20,
                0.25,
                "success",
                Some(200),
                now - 300,
            ),
            sample_usage(
                "request-today-failed",
                "provider-1",
                "OpenAI",
                10,
                0.10,
                "failed",
                Some(502),
                now - 120,
            ),
            sample_usage(
                "request-old",
                "provider-1",
                "OpenAI",
                99,
                9.99,
                "success",
                Some(200),
                now - 172_800,
            ),
        ]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_provider_catalog_and_usage_reader_for_tests(
                    provider_catalog,
                    usage_repository,
                ),
            );
        let context = request_context(http::Method::GET, "/api/admin/monitoring/system-status");

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("system status route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["users"]["total"], json!(0));
        assert_eq!(payload["users"]["active"], json!(0));
        assert_eq!(payload["providers"]["total"], json!(2));
        assert_eq!(payload["providers"]["active"], json!(1));
        assert_eq!(payload["api_keys"]["total"], json!(0));
        assert_eq!(payload["api_keys"]["active"], json!(0));
        assert_eq!(payload["today_stats"]["requests"], json!(2));
        assert_eq!(payload["today_stats"]["tokens"], json!(30));
        assert_eq!(payload["today_stats"]["cost_usd"], json!("$0.3500"));
        assert_eq!(payload["recent_errors"], json!(1));
        assert!(payload["timestamp"].as_str().is_some());
    }

    #[tokio::test]
    async fn admin_monitoring_trace_provider_stats_returns_local_payload() {
        let request_candidates = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
            sample_candidate(
                "cand-1",
                "req-a",
                0,
                RequestCandidateStatus::Success,
                Some(101),
                Some(20),
                Some(200),
            ),
            sample_candidate(
                "cand-2",
                "req-b",
                0,
                RequestCandidateStatus::Failed,
                Some(201),
                Some(40),
                Some(502),
            ),
            sample_candidate(
                "cand-3",
                "req-c",
                0,
                RequestCandidateStatus::Cancelled,
                Some(301),
                Some(60),
                Some(499),
            ),
            sample_candidate(
                "cand-4",
                "req-d",
                0,
                RequestCandidateStatus::Available,
                None,
                None,
                None,
            ),
            sample_candidate(
                "cand-5",
                "req-e",
                0,
                RequestCandidateStatus::Unused,
                None,
                None,
                None,
            ),
        ]));
        let state = AppState::new("http://127.0.0.1:9", None)
            .expect("state should build")
            .with_request_candidate_data_reader_for_tests(request_candidates);
        let context = request_context(
            http::Method::GET,
            "/api/admin/monitoring/trace/stats/provider/provider-1?limit=10",
        );

        let response = maybe_build_local_admin_monitoring_response(&state, &context)
            .await
            .expect("handler should not error")
            .expect("provider stats route should be handled locally");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(payload["provider_id"], json!("provider-1"));
        assert_eq!(payload["total_attempts"], json!(5));
        assert_eq!(payload["success_count"], json!(1));
        assert_eq!(payload["failed_count"], json!(1));
        assert_eq!(payload["cancelled_count"], json!(1));
        assert_eq!(payload["skipped_count"], json!(0));
        assert_eq!(payload["pending_count"], json!(0));
        assert_eq!(payload["available_count"], json!(1));
        assert_eq!(payload["unused_count"], json!(1));
        assert_eq!(payload["failure_rate"], json!(50.0));
        assert_eq!(payload["avg_latency_ms"], json!(40.0));
    }

    #[tokio::test]
    async fn admin_monitoring_ignores_non_owned_routes() {
        let state = AppState::new("http://127.0.0.1:9", None).expect("state should build");

        let non_monitoring = request_context(http::Method::GET, "/api/admin/stats/time-series");
        assert!(
            maybe_build_local_admin_monitoring_response(&state, &non_monitoring)
                .await
                .expect("handler should not error")
                .is_none()
        );

        let wrong_method = request_context(
            http::Method::POST,
            "/api/admin/monitoring/cache/model-mapping",
        );
        assert!(
            maybe_build_local_admin_monitoring_response(&state, &wrong_method)
                .await
                .expect("handler should not error")
                .is_none()
        );
    }
}
