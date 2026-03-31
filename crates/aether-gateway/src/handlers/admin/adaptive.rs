#[derive(Debug, Deserialize)]
struct AdminAdaptiveToggleModeRequest {
    enabled: bool,
    #[serde(default)]
    fixed_limit: Option<u32>,
}

fn admin_adaptive_effective_limit(
    key: &aether_data::repository::provider_catalog::StoredProviderCatalogKey,
) -> Option<u32> {
    if key.rpm_limit.is_none() {
        key.learned_rpm_limit
    } else {
        key.rpm_limit
    }
}

fn admin_adaptive_adjustment_items(
    value: Option<&serde_json::Value>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_object)
        .cloned()
        .collect()
}

fn admin_adaptive_key_payload(
    key: &aether_data::repository::provider_catalog::StoredProviderCatalogKey,
) -> serde_json::Value {
    json!({
        "id": key.id,
        "name": key.name,
        "provider_id": key.provider_id,
        "api_formats": json_string_list(key.api_formats.as_ref()),
        "is_adaptive": key.rpm_limit.is_none(),
        "rpm_limit": key.rpm_limit,
        "effective_limit": admin_adaptive_effective_limit(key),
        "learned_rpm_limit": key.learned_rpm_limit,
        "concurrent_429_count": key.concurrent_429_count.unwrap_or(0),
        "rpm_429_count": key.rpm_429_count.unwrap_or(0),
    })
}

fn admin_adaptive_key_not_found_response(key_id: &str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": format!("Key {key_id} 不存在") })),
    )
        .into_response()
}

fn admin_adaptive_dispatcher_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "Adaptive route not found" })),
    )
        .into_response()
}

fn admin_adaptive_key_id_from_path(path: &str) -> Option<String> {
    let normalized = path.trim_end_matches('/');
    let mut segments = normalized.split('/').filter(|segment| !segment.is_empty());
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("api"), Some("admin"), Some("adaptive"), Some("keys"), Some(key_id))
            if !key_id.is_empty() =>
        {
            Some(key_id.to_string())
        }
        _ => None,
    }
}

async fn admin_adaptive_find_key(
    state: &AppState,
    key_id: &str,
) -> Result<Option<aether_data::repository::provider_catalog::StoredProviderCatalogKey>, GatewayError>
{
    Ok(state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&key_id.to_string()))
        .await?
        .into_iter()
        .next())
}

async fn admin_adaptive_load_candidate_keys(
    state: &AppState,
    provider_id: Option<&str>,
) -> Result<Vec<aether_data::repository::provider_catalog::StoredProviderCatalogKey>, GatewayError>
{
    if let Some(provider_id) = provider_id.filter(|value| !value.trim().is_empty()) {
        return state
            .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider_id.to_string()))
            .await;
    }

    let provider_ids = state
        .list_provider_catalog_providers(false)
        .await?
        .into_iter()
        .map(|provider| provider.id)
        .collect::<Vec<_>>();
    if provider_ids.is_empty() {
        return Ok(vec![]);
    }
    state
        .list_provider_catalog_keys_by_provider_ids(&provider_ids)
        .await
}

async fn maybe_build_local_admin_adaptive_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("adaptive_manage") {
        return Ok(None);
    }

    if !state.has_provider_catalog_data_reader() {
        return Ok(Some(build_proxy_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "maintenance_mode",
            "Admin adaptive routes require Rust maintenance backend",
            Some(json!({
                "error": "Admin adaptive routes require Rust maintenance backend",
            })),
        )));
    }

    if decision.route_kind.as_deref() == Some("list_keys")
        && request_context.request_method == http::Method::GET
        && matches!(
            request_context.request_path.as_str(),
            "/api/admin/adaptive/keys" | "/api/admin/adaptive/keys/"
        )
    {
        let provider_id = query_param_value(request_context.request_query_string.as_deref(), "provider_id");
        let payload = admin_adaptive_load_candidate_keys(state, provider_id.as_deref())
            .await?
            .into_iter()
            .filter(|key| key.rpm_limit.is_none())
            .map(|key| admin_adaptive_key_payload(&key))
            .collect::<Vec<_>>();
        return Ok(Some(Json(payload).into_response()));
    }

    if decision.route_kind.as_deref() == Some("summary")
        && request_context.request_method == http::Method::GET
        && matches!(
            request_context.request_path.as_str(),
            "/api/admin/adaptive/summary" | "/api/admin/adaptive/summary/"
        )
    {
        let keys = admin_adaptive_load_candidate_keys(state, None).await?;
        let adaptive_keys = keys
            .into_iter()
            .filter(|key| key.rpm_limit.is_none())
            .collect::<Vec<_>>();

        let total_keys = adaptive_keys.len() as u64;
        let total_concurrent_429 = adaptive_keys
            .iter()
            .map(|key| u64::from(key.concurrent_429_count.unwrap_or(0)))
            .sum::<u64>();
        let total_rpm_429 = adaptive_keys
            .iter()
            .map(|key| u64::from(key.rpm_429_count.unwrap_or(0)))
            .sum::<u64>();
        let mut recent_adjustments = vec![];
        let mut total_adjustments = 0usize;
        for key in adaptive_keys {
            let history = admin_adaptive_adjustment_items(key.adjustment_history.as_ref());
            total_adjustments += history.len();
            for adjustment in history.into_iter().rev().take(3) {
                let mut payload = adjustment;
                payload.insert("key_id".to_string(), json!(key.id));
                payload.insert("key_name".to_string(), json!(key.name));
                recent_adjustments.push(serde_json::Value::Object(payload));
            }
        }
        recent_adjustments.sort_by(|left, right| {
            let lhs = left
                .get("timestamp")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let rhs = right
                .get("timestamp")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            rhs.cmp(lhs)
        });

        return Ok(Some(
            Json(json!({
                "total_adaptive_keys": total_keys,
                "total_concurrent_429_errors": total_concurrent_429,
                "total_rpm_429_errors": total_rpm_429,
                "total_adjustments": total_adjustments,
                "recent_adjustments": recent_adjustments.into_iter().take(10).collect::<Vec<_>>(),
            }))
            .into_response(),
        ));
    }

    let key_id = admin_adaptive_key_id_from_path(&request_context.request_path);
    if key_id.is_none() {
        return Ok(Some(admin_adaptive_dispatcher_not_found_response()));
    }
    let key_id = key_id.expect("checked is_some above");

    if decision.route_kind.as_deref() == Some("get_stats")
        && request_context.request_method == http::Method::GET
        && request_context.request_path.ends_with("/stats")
    {
        let Some(key) = admin_adaptive_find_key(state, &key_id).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        let status_snapshot = key
            .status_snapshot
            .as_ref()
            .and_then(serde_json::Value::as_object);
        let adjustments = admin_adaptive_adjustment_items(key.adjustment_history.as_ref());
        let adjustment_count = adjustments.len();
        let recent_adjustments = adjustments
            .into_iter()
            .rev()
            .take(10)
            .map(serde_json::Value::Object)
            .collect::<Vec<_>>();

        return Ok(Some(
            Json(json!({
                "adaptive_mode": key.rpm_limit.is_none(),
                "rpm_limit": key.rpm_limit,
                "effective_limit": admin_adaptive_effective_limit(&key),
                "learned_limit": key.learned_rpm_limit,
                "concurrent_429_count": key.concurrent_429_count.unwrap_or(0),
                "rpm_429_count": key.rpm_429_count.unwrap_or(0),
                "last_429_at": key.last_429_at_unix_secs.and_then(unix_secs_to_rfc3339),
                "last_429_type": key.last_429_type,
                "adjustment_count": adjustment_count,
                "recent_adjustments": recent_adjustments,
                "learning_confidence": status_snapshot.and_then(|value| value.get("learning_confidence")).cloned(),
                "enforcement_active": status_snapshot.and_then(|value| value.get("enforcement_active")).cloned(),
                "observation_count": status_snapshot
                    .and_then(|value| value.get("observation_count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                "header_observation_count": status_snapshot
                    .and_then(|value| value.get("header_observation_count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                "latest_upstream_limit": status_snapshot
                    .and_then(|value| value.get("latest_upstream_limit"))
                    .and_then(serde_json::Value::as_u64),
            }))
            .into_response(),
        ));
    }

    if !state.has_provider_catalog_data_writer() {
        return Ok(Some(build_proxy_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "maintenance_mode",
            "Admin adaptive routes require Rust maintenance backend",
            Some(json!({
                "error": "Admin adaptive routes require Rust maintenance backend",
            })),
        )));
    }

    if decision.route_kind.as_deref() == Some("toggle_mode")
        && request_context.request_method == http::Method::PATCH
        && request_context.request_path.ends_with("/mode")
    {
        let Some(request_body) = request_body else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "请求数据验证失败",
                None,
            )));
        };
        let body = match serde_json::from_slice::<AdminAdaptiveToggleModeRequest>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "请求数据验证失败",
                    None,
                )));
            }
        };
        let Some(mut key) = admin_adaptive_find_key(state, &key_id).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        let message = if body.enabled {
            key.rpm_limit = None;
            "已切换为自适应模式，系统将自动学习并调整 RPM 限制".to_string()
        } else {
            let Some(fixed_limit) = body.fixed_limit else {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({
                            "detail": "禁用自适应模式时必须提供 fixed_limit 参数",
                        })),
                    )
                        .into_response(),
                ));
            };
            if !(1..=100).contains(&fixed_limit) {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({
                            "detail": "fixed_limit 超出范围（1-100）",
                        })),
                    )
                        .into_response(),
                ));
            }
            key.rpm_limit = Some(fixed_limit);
            format!("已切换为固定限制模式，RPM 限制设为 {fixed_limit}")
        };
        let Some(updated) = state.update_provider_catalog_key(&key).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        return Ok(Some(
            Json(json!({
                "message": message,
                "key_id": updated.id,
                "is_adaptive": updated.rpm_limit.is_none(),
                "rpm_limit": updated.rpm_limit,
                "effective_limit": admin_adaptive_effective_limit(&updated),
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("set_limit")
        && request_context.request_method == http::Method::PATCH
        && request_context.request_path.ends_with("/limit")
    {
        let Some(limit_value) =
            query_param_value(request_context.request_query_string.as_deref(), "limit")
        else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "缺少 limit 参数" })),
                )
                    .into_response(),
            ));
        };
        let Ok(limit) = limit_value.parse::<u32>() else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "limit 必须是数字" })),
                )
                    .into_response(),
            ));
        };
        if !(1..=100).contains(&limit) {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "limit 超出范围（1-100）" })),
                )
                    .into_response(),
            ));
        }

        let Some(mut key) = admin_adaptive_find_key(state, &key_id).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        let was_adaptive = key.rpm_limit.is_none();
        key.rpm_limit = Some(limit);
        let Some(updated) = state.update_provider_catalog_key(&key).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        return Ok(Some(
            Json(json!({
                "message": format!("已设置为固定限制模式，RPM 限制为 {limit}"),
                "key_id": updated.id,
                "is_adaptive": false,
                "rpm_limit": updated.rpm_limit,
                "previous_mode": if was_adaptive { "adaptive" } else { "fixed" },
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("reset_learning")
        && request_context.request_method == http::Method::DELETE
        && request_context.request_path.ends_with("/learning")
    {
        let Some(mut key) = admin_adaptive_find_key(state, &key_id).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        key.learned_rpm_limit = None;
        key.concurrent_429_count = None;
        key.rpm_429_count = None;
        key.last_429_at_unix_secs = None;
        key.last_429_type = None;
        key.adjustment_history = None;
        let Some(updated) = state.update_provider_catalog_key(&key).await? else {
            return Ok(Some(admin_adaptive_key_not_found_response(&key_id)));
        };
        return Ok(Some(
            Json(json!({
                "message": "学习状态已重置",
                "key_id": updated.id,
            }))
            .into_response(),
        ));
    }

    Ok(Some(admin_adaptive_dispatcher_not_found_response()))
}
