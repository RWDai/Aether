#[derive(Debug, Deserialize)]
struct AdminProviderStrategyBillingRequest {
    billing_type: String,
    #[serde(default)]
    monthly_quota_usd: Option<f64>,
    #[serde(default = "default_provider_strategy_quota_reset_day")]
    quota_reset_day: u64,
    #[serde(default)]
    quota_last_reset_at: Option<String>,
    #[serde(default)]
    quota_expires_at: Option<String>,
    #[serde(default)]
    rpm_limit: Option<i32>,
    #[serde(default = "default_provider_strategy_provider_priority")]
    provider_priority: i32,
}

fn default_provider_strategy_quota_reset_day() -> u64 {
    30
}

fn default_provider_strategy_provider_priority() -> i32 {
    100
}

async fn maybe_build_local_admin_provider_strategy_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };
    if decision.route_family.as_deref() != Some("provider_strategy_manage") {
        return Ok(None);
    }

    if decision.route_kind.as_deref() == Some("list_strategies")
        && request_context.request_method == http::Method::GET
        && is_admin_provider_strategy_strategies_root(&request_context.request_path)
    {
        return Ok(Some(
            Json(json!({
                "strategies": [{
                    "name": "sticky_priority",
                    "priority": 110,
                    "version": "1.0.0",
                    "description": "粘性优先级负载均衡策略，正常时始终使用同一提供商",
                    "author": "System",
                }],
                "total": 1,
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("update_provider_billing")
        && request_context.request_method == http::Method::PUT
    {
        if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "maintenance_mode",
                "Admin provider strategy routes require Rust maintenance backend",
                Some(json!({
                    "error": "Admin provider strategy routes require Rust maintenance backend",
                })),
            )));
        }

        let Some(provider_id) =
            admin_provider_id_for_provider_strategy_billing(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体不能为空" })),
                )
                    .into_response(),
            ));
        };

        let payload =
            match serde_json::from_slice::<AdminProviderStrategyBillingRequest>(request_body) {
                Ok(payload) => payload,
                Err(_) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": "请求数据验证失败" })),
                        )
                            .into_response(),
                    ));
                }
            };

        let Some(existing) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        let billing_type = match normalize_provider_billing_type(&payload.billing_type) {
            Ok(value) => value,
            Err(message) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": message })),
                    )
                        .into_response(),
                ));
            }
        };
        if payload
            .monthly_quota_usd
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "monthly_quota_usd 必须是非负数" })),
                )
                    .into_response(),
            ));
        }
        if !(1..=365).contains(&payload.quota_reset_day) {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "quota_reset_day 必须是 1 到 365 之间的整数" })),
                )
                    .into_response(),
            ));
        }
        if !(0..=10_000).contains(&payload.provider_priority) {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "provider_priority 必须在 0 到 10000 之间" })),
                )
                    .into_response(),
            ));
        }

        let quota_last_reset_at_unix_secs = match payload.quota_last_reset_at.as_deref() {
            Some(value) => match parse_optional_rfc3339_unix_secs(value, "quota_last_reset_at") {
                Ok(value) => Some(value),
                Err(message) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": message })),
                        )
                            .into_response(),
                    ));
                }
            },
            None => existing.quota_last_reset_at_unix_secs,
        };
        let quota_expires_at_unix_secs = match payload.quota_expires_at.as_deref() {
            Some(value) => match parse_optional_rfc3339_unix_secs(value, "quota_expires_at") {
                Ok(value) => Some(value),
                Err(message) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": message })),
                        )
                            .into_response(),
                    ));
                }
            },
            None => existing.quota_expires_at_unix_secs,
        };

        let synced_monthly_used_usd = match quota_last_reset_at_unix_secs {
            Some(quota_last_reset_at_unix_secs) if state.has_usage_data_reader() => Some(
                state
                    .summarize_provider_usage_since(&provider_id, quota_last_reset_at_unix_secs)
                    .await?
                    .total_cost_usd,
            ),
            _ => existing.monthly_used_usd,
        };

        let _ignored_rpm_limit = payload.rpm_limit;
        let updated = existing
            .clone()
            .with_billing_fields(
                Some(billing_type.clone()),
                payload.monthly_quota_usd,
                synced_monthly_used_usd,
                Some(payload.quota_reset_day),
                quota_last_reset_at_unix_secs,
                quota_expires_at_unix_secs,
            )
            .with_routing_fields(payload.provider_priority);
        let Some(updated) = state.update_provider_catalog_provider(&updated).await? else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        return Ok(Some(
            Json(json!({
                "message": "Provider billing config updated successfully",
                "provider": {
                    "id": updated.id,
                    "name": updated.name,
                    "billing_type": billing_type,
                    "provider_priority": updated.provider_priority,
                },
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("get_provider_stats")
        && request_context.request_method == http::Method::GET
    {
        if !state.has_provider_catalog_data_reader() || !state.has_usage_data_reader() {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "maintenance_mode",
                "Admin provider strategy stats require Rust maintenance backend",
                Some(json!({
                    "error": "Admin provider strategy stats require Rust maintenance backend",
                })),
            )));
        }

        let Some(provider_id) =
            admin_provider_id_for_provider_strategy_stats(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        let hours = query_param_value(request_context.request_query_string.as_deref(), "hours")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(24);
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let since_unix_secs = now_unix_secs.saturating_sub(hours.saturating_mul(3600));

        let Some(provider) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        let summary = state
            .summarize_provider_usage_since(&provider_id, since_unix_secs)
            .await?;
        let monthly_used_usd = provider.monthly_used_usd.unwrap_or(0.0);
        let quota_remaining_usd = provider
            .monthly_quota_usd
            .map(|value| value - monthly_used_usd);
        let success_rate = if summary.total_requests > 0 {
            summary.successful_requests as f64 / summary.total_requests as f64
        } else {
            0.0
        };

        return Ok(Some(
            Json(json!({
                "provider_id": provider_id,
                "provider_name": provider.name,
                "period_hours": hours,
                "billing_info": {
                    "billing_type": provider.billing_type,
                    "monthly_quota_usd": provider.monthly_quota_usd,
                    "monthly_used_usd": monthly_used_usd,
                    "quota_remaining_usd": quota_remaining_usd,
                    "quota_expires_at": provider.quota_expires_at_unix_secs.and_then(unix_secs_to_rfc3339),
                },
                "usage_stats": {
                    "total_requests": summary.total_requests,
                    "successful_requests": summary.successful_requests,
                    "failed_requests": summary.failed_requests,
                    "success_rate": success_rate,
                    "avg_response_time_ms": (summary.avg_response_time_ms * 100.0).round() / 100.0,
                    "total_cost_usd": (summary.total_cost_usd * 10_000.0).round() / 10_000.0,
                },
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("reset_provider_quota")
        && request_context.request_method == http::Method::DELETE
    {
        if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "maintenance_mode",
                "Admin provider strategy routes require Rust maintenance backend",
                Some(json!({
                    "error": "Admin provider strategy routes require Rust maintenance backend",
                })),
            )));
        }

        let Some(provider_id) =
            admin_provider_id_for_provider_strategy_quota(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        let Some(provider) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        if provider.billing_type.as_deref() != Some("monthly_quota") {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "Only monthly quota providers can be reset" })),
                )
                    .into_response(),
            ));
        }

        let previous_used = provider.monthly_used_usd.unwrap_or(0.0);
        let mut updated = provider.clone();
        updated.monthly_used_usd = Some(0.0);
        let Some(updated) = state.update_provider_catalog_provider(&updated).await? else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider not found" })),
                )
                    .into_response(),
            ));
        };

        return Ok(Some(
            Json(json!({
                "message": "Provider quota reset successfully",
                "provider_name": updated.name,
                "previous_used": previous_used,
                "current_used": 0.0,
            }))
            .into_response(),
        ));
    }

    Ok(Some(admin_provider_strategy_dispatcher_not_found_response()))
}

fn admin_provider_strategy_dispatcher_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "Provider strategy route not found" })),
    )
        .into_response()
}
