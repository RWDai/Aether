const USER_MONITORING_MAINTENANCE_DETAIL: &str =
    "User monitoring routes require Rust maintenance backend";

fn normalize_rate_limit_value(value: Option<i32>) -> u32 {
    value
        .map(|raw| raw.max(0))
        .and_then(|raw| u32::try_from(raw).ok())
        .unwrap_or(0)
}

fn parse_user_monitoring_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
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
        None => Ok(50),
    }
}

fn parse_user_monitoring_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
        None => Ok(0),
    }
}

fn parse_user_monitoring_days(query: Option<&str>) -> Result<i64, String> {
    match query_param_value(query, "days") {
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
        None => Ok(30),
    }
}

fn build_user_monitoring_audit_logs_payload(
    items: Vec<serde_json::Value>,
    total: usize,
    limit: usize,
    offset: usize,
    event_type: Option<String>,
    days: i64,
) -> Response<Body> {
    Json(json!({
        "items": items,
        "meta": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": items.len(),
        },
        "filters": {
            "event_type": event_type,
            "days": days,
        }
    }))
    .into_response()
}

async fn handle_user_audit_logs(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };

    let query = request_context.request_query_string.as_deref();
    let event_type = query_param_value(query, "event_type").map(|value| value.trim().to_string());
    let event_type = event_type.filter(|value| !value.is_empty());
    let limit = match parse_user_monitoring_limit(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let offset = match parse_user_monitoring_offset(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let days = match parse_user_monitoring_days(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };

    let Some(pool) = state.postgres_pool() else {
        return build_user_monitoring_audit_logs_payload(
            Vec::new(),
            0,
            limit,
            offset,
            event_type,
            days,
        );
    };

    let cutoff_time = Utc::now() - chrono::Duration::days(days);
    let total = if let Some(ref event_type) = event_type {
        match sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
  AND event_type = $3
"#,
        )
        .bind(&auth.user.id)
        .bind(cutoff_time)
        .bind(event_type)
        .fetch_one(&pool)
        .await
        {
            Ok(value) => usize::try_from(value.max(0)).unwrap_or(usize::MAX),
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user audit logs count failed: {err}"),
                    false,
                )
            }
        }
    } else {
        match sqlx::query_scalar::<_, i64>(
            r#"
SELECT COUNT(*)
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
"#,
        )
        .bind(&auth.user.id)
        .bind(cutoff_time)
        .fetch_one(&pool)
        .await
        {
            Ok(value) => usize::try_from(value.max(0)).unwrap_or(usize::MAX),
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user audit logs count failed: {err}"),
                    false,
                )
            }
        }
    };

    let rows = if let Some(ref event_type) = event_type {
        match sqlx::query(
            r#"
SELECT id, event_type, description, ip_address, status_code, created_at
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
  AND event_type = $3
ORDER BY created_at DESC
LIMIT $4 OFFSET $5
"#,
        )
        .bind(&auth.user.id)
        .bind(cutoff_time)
        .bind(event_type)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user audit logs read failed: {err}"),
                    false,
                )
            }
        }
    } else {
        match sqlx::query(
            r#"
SELECT id, event_type, description, ip_address, status_code, created_at
FROM audit_logs
WHERE user_id = $1
  AND created_at >= $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4
"#,
        )
        .bind(&auth.user.id)
        .bind(cutoff_time)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
        .fetch_all(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user audit logs read failed: {err}"),
                    false,
                )
            }
        }
    };

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
                "description": row.try_get::<String, _>("description").ok(),
                "ip_address": row.try_get::<Option<String>, _>("ip_address").ok().flatten(),
                "status_code": row.try_get::<Option<i32>, _>("status_code").ok().flatten(),
                "created_at": created_at,
            })
        })
        .collect::<Vec<_>>();

    build_user_monitoring_audit_logs_payload(items, total, limit, offset, event_type, days)
}

async fn handle_user_rate_limit_status(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };

    let limiter = state.frontdoor_user_rpm();
    let now = Utc::now();
    let now_unix_secs = u64::try_from(now.timestamp()).unwrap_or(0);
    let system_default_limit = match limiter.current_system_default_limit(state).await {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = ?err, "user rate limit status system default read failed");
            0
        }
    };
    let bucket = limiter.current_bucket(now_unix_secs);
    let reset_time =
        (now + chrono::Duration::seconds(i64::try_from(limiter.retry_after(now_unix_secs)).unwrap_or(0)))
            .to_rfc3339();
    let window = format!("{}s", limiter.config().bucket_seconds());

    let export_records = match state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user rate limit status read failed: {err:?}"),
                false,
            )
        }
    };

    let mut api_keys = Vec::new();
    for record in export_records {
        if !record.is_active {
            continue;
        }

        let snapshot = match state
            .read_auth_api_key_snapshot(&auth.user.id, &record.api_key_id, now_unix_secs)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user api key snapshot read failed: {err:?}"),
                    false,
                )
            }
        };
        let is_standalone = snapshot
            .as_ref()
            .map(|value| value.api_key_is_standalone)
            .unwrap_or(record.is_standalone);
        let user_limit = if is_standalone {
            match record.rate_limit {
                Some(value) => normalize_rate_limit_value(Some(value)),
                None => system_default_limit,
            }
        } else {
            normalize_rate_limit_value(
                snapshot
                    .as_ref()
                    .and_then(|value| value.user_rate_limit)
                    .or(Some(i32::try_from(system_default_limit).unwrap_or(i32::MAX))),
            )
        };
        let key_limit = if is_standalone {
            0
        } else {
            normalize_rate_limit_value(
                snapshot
                    .as_ref()
                    .and_then(|value| value.api_key_rate_limit)
                    .or(record.rate_limit),
            )
        };

        let user_scope_key = if is_standalone {
            limiter.standalone_scope_key(&record.api_key_id, bucket)
        } else {
            limiter.user_scope_key(&auth.user.id, bucket)
        };
        let key_scope_key = limiter.key_scope_key(&record.api_key_id, bucket);

        let user_count = if user_limit > 0 {
            match limiter.get_scope_count(state, &user_scope_key, bucket).await {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(error = ?err, scope_key = %user_scope_key, "user rpm scope read failed");
                    0
                }
            }
        } else {
            0
        };
        let key_count = if key_limit > 0 {
            match limiter.get_scope_count(state, &key_scope_key, bucket).await {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(error = ?err, scope_key = %key_scope_key, "api key rpm scope read failed");
                    0
                }
            }
        } else {
            0
        };

        let user_remaining = if user_limit > 0 {
            Some(user_limit.saturating_sub(user_count))
        } else {
            None
        };
        let key_remaining = if key_limit > 0 {
            Some(key_limit.saturating_sub(key_count))
        } else {
            None
        };

        let primary_scope = match (user_remaining, key_remaining) {
            (Some(user_remaining), Some(key_remaining)) => {
                if user_remaining <= key_remaining {
                    Some(("user", user_limit, user_remaining))
                } else {
                    Some(("key", key_limit, key_remaining))
                }
            }
            (Some(user_remaining), None) => Some(("user", user_limit, user_remaining)),
            (None, Some(key_remaining)) => Some(("key", key_limit, key_remaining)),
            (None, None) => None,
        };

        api_keys.push(json!({
            "api_key_name": record
                .name
                .clone()
                .unwrap_or_else(|| format!("Key-{}", record.api_key_id)),
            "limit": primary_scope.map(|(_, limit, _)| limit),
            "remaining": primary_scope.map(|(_, _, remaining)| remaining),
            "scope": primary_scope.map(|(scope, _, _)| scope),
            "reset_time": primary_scope.map(|_| reset_time.clone()),
            "window": primary_scope.map(|_| window.clone()),
            "user_limit": if user_limit > 0 { Some(user_limit) } else { None::<u32> },
            "user_remaining": user_remaining,
            "key_limit": if key_limit > 0 { Some(key_limit) } else { None::<u32> },
            "key_remaining": key_remaining,
        }));
    }

    Json(json!({
        "user_id": auth.user.id,
        "api_keys": api_keys,
    }))
    .into_response()
}

async fn maybe_build_local_user_monitoring_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("monitoring_user_legacy") {
        return None;
    }

    match decision.route_kind.as_deref() {
        Some("audit_logs")
            if request_context.request_method == http::Method::GET
                && request_context.request_path == "/api/monitoring/my-audit-logs" =>
        {
            Some(handle_user_audit_logs(state, request_context, headers).await)
        }
        Some("rate_limit_status")
            if request_context.request_method == http::Method::GET
                && request_context.request_path == "/api/monitoring/rate-limit-status" =>
        {
            Some(handle_user_rate_limit_status(state, request_context, headers).await)
        }
        _ => Some(build_public_support_maintenance_response(USER_MONITORING_MAINTENANCE_DETAIL)),
    }
}
