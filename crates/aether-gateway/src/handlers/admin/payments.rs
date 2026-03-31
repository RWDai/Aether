const ADMIN_PAYMENTS_RUST_BACKEND_DETAIL: &str =
    "Admin payments routes require Rust maintenance backend";

#[derive(Debug, Default, serde::Deserialize)]
struct AdminPaymentOrderCreditRequest {
    #[serde(default)]
    gateway_order_id: Option<String>,
    #[serde(default)]
    pay_amount: Option<f64>,
    #[serde(default)]
    pay_currency: Option<String>,
    #[serde(default)]
    exchange_rate: Option<f64>,
    #[serde(default)]
    gateway_response: Option<serde_json::Value>,
}

fn build_admin_payments_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_PAYMENTS_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn build_admin_payments_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn build_admin_payments_backend_unavailable_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn build_admin_payment_order_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "Payment order not found" })),
    )
        .into_response()
}

fn build_admin_payment_orders_page_response(
    items: Vec<serde_json::Value>,
    total: u64,
    limit: usize,
    offset: usize,
) -> Response<Body> {
    Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response()
}

fn parse_admin_payments_limit(query: Option<&str>) -> Result<usize, String> {
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

fn parse_admin_payments_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
        None => Ok(0),
    }
}

fn admin_payment_order_id_from_detail_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/admin/payments/orders/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn admin_payment_order_id_from_suffix_path(request_path: &str, suffix: &str) -> Option<String> {
    request_path
        .trim()
        .trim_end_matches('/')
        .strip_prefix("/api/admin/payments/orders/")?
        .strip_suffix(suffix)
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty() && !value.contains('/'))
}

fn normalize_admin_payment_optional_string(
    value: Option<String>,
    field_name: &str,
    max_len: usize,
) -> Result<Option<String>, String> {
    match value {
        None => Ok(None),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            if trimmed.chars().count() > max_len {
                return Err(format!("{field_name} 长度不能超过 {max_len}"));
            }
            Ok(Some(trimmed.to_string()))
        }
    }
}

fn normalize_admin_payment_currency(value: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = normalize_admin_payment_optional_string(value, "pay_currency", 3)? else {
        return Ok(None);
    };
    let normalized = value.to_ascii_uppercase();
    if normalized.len() != 3 {
        return Err("pay_currency 必须是 3 位货币代码".to_string());
    }
    Ok(Some(normalized))
}

fn normalize_admin_payment_positive_number(
    value: Option<f64>,
    field_name: &str,
) -> Result<Option<f64>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{field_name} 必须为大于 0 的有限数字"));
    }
    Ok(Some(value))
}

fn admin_payment_operator_id(request_context: &GatewayPublicRequestContext) -> Option<String> {
    request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.admin_principal.as_ref())
        .map(|principal| principal.user_id.clone())
}

fn admin_payment_effective_status(status: &str, expires_at_unix_secs: Option<u64>) -> String {
    let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
    if status == "pending" && expires_at_unix_secs.is_some_and(|value| value < now_unix_secs) {
        "expired".to_string()
    } else {
        status.to_string()
    }
}

fn build_admin_payment_order_payload(
    record: &crate::gateway::AdminWalletPaymentOrderRecord,
) -> serde_json::Value {
    json!({
        "id": record.id,
        "order_no": record.order_no,
        "wallet_id": record.wallet_id,
        "user_id": record.user_id,
        "amount_usd": record.amount_usd,
        "pay_amount": record.pay_amount,
        "pay_currency": record.pay_currency,
        "exchange_rate": record.exchange_rate,
        "refunded_amount_usd": record.refunded_amount_usd,
        "refundable_amount_usd": record.refundable_amount_usd,
        "payment_method": record.payment_method,
        "gateway_order_id": record.gateway_order_id,
        "gateway_response": record.gateway_response,
        "status": admin_payment_effective_status(&record.status, record.expires_at_unix_secs),
        "created_at": unix_secs_to_rfc3339(record.created_at_unix_secs),
        "paid_at": record.paid_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "credited_at": record.credited_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "expires_at": record.expires_at_unix_secs.and_then(unix_secs_to_rfc3339),
    })
}

fn build_admin_payment_callback_payload(
    row: &sqlx::postgres::PgRow,
) -> Result<serde_json::Value, GatewayError> {
    Ok(json!({
        "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "payment_order_id": row.try_get::<Option<String>, _>("payment_order_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "payment_method": row.try_get::<String, _>("payment_method").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "callback_key": row.try_get::<String, _>("callback_key").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "order_no": row.try_get::<Option<String>, _>("order_no").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "gateway_order_id": row.try_get::<Option<String>, _>("gateway_order_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "payload_hash": row.try_get::<Option<String>, _>("payload_hash").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "signature_valid": row.try_get::<bool, _>("signature_valid").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "status": row.try_get::<String, _>("status").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "payload": row.try_get::<Option<serde_json::Value>, _>("payload").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "error_message": row.try_get::<Option<String>, _>("error_message").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "created_at": row
            .try_get::<Option<i64>, _>("created_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .and_then(|value| u64::try_from(value).ok())
            .and_then(unix_secs_to_rfc3339),
        "processed_at": row
            .try_get::<Option<i64>, _>("processed_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .and_then(|value| u64::try_from(value).ok())
            .and_then(unix_secs_to_rfc3339),
    }))
}

fn build_admin_payment_callback_payload_from_record(
    record: &crate::gateway::state::AdminPaymentCallbackRecord,
) -> serde_json::Value {
    json!({
        "id": record.id,
        "payment_order_id": record.payment_order_id,
        "payment_method": record.payment_method,
        "callback_key": record.callback_key,
        "order_no": record.order_no,
        "gateway_order_id": record.gateway_order_id,
        "payload_hash": record.payload_hash,
        "signature_valid": record.signature_valid,
        "status": record.status,
        "payload": record.payload,
        "error_message": record.error_message,
        "created_at": unix_secs_to_rfc3339(record.created_at_unix_secs),
        "processed_at": record.processed_at_unix_secs.and_then(unix_secs_to_rfc3339),
    })
}

async fn build_admin_payment_list_orders_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_payments_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let offset = match parse_admin_payments_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let status = query_param_value(query, "status");
    let payment_method = query_param_value(query, "payment_method");

    let Some((items, total)) = state
        .list_admin_payment_orders(status.as_deref(), payment_method.as_deref(), limit, offset)
        .await?
    else {
        return Ok(build_admin_payment_orders_page_response(
            Vec::new(),
            0,
            limit,
            offset,
        ));
    };

    Ok(build_admin_payment_orders_page_response(
        items
            .iter()
            .map(build_admin_payment_order_payload)
            .collect::<Vec<_>>(),
        total,
        limit,
        offset,
    ))
}

async fn build_admin_payment_get_order_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(order_id) = admin_payment_order_id_from_detail_path(&request_context.request_path)
    else {
        return Ok(build_admin_payment_order_not_found_response());
    };
    match state.read_admin_payment_order(&order_id).await? {
        crate::gateway::AdminWalletMutationOutcome::Applied(order) => Ok(Json(json!({
            "order": build_admin_payment_order_payload(&order),
        }))
        .into_response()),
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_payment_order_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_payments_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_payments_backend_unavailable_response(
                "Payment order read backend unavailable",
            ))
        }
    }
}

async fn build_admin_payment_expire_order_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(order_id) =
        admin_payment_order_id_from_suffix_path(&request_context.request_path, "/expire")
    else {
        return Ok(build_admin_payment_order_not_found_response());
    };
    match state.admin_expire_payment_order(&order_id).await? {
        crate::gateway::AdminWalletMutationOutcome::Applied((order, expired)) => Ok(Json(json!({
            "order": build_admin_payment_order_payload(&order),
            "expired": expired,
        }))
        .into_response()),
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_payment_order_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_payments_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_payments_backend_unavailable_response(
                "Payment order write backend unavailable",
            ))
        }
    }
}

async fn build_admin_payment_credit_order_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(order_id) =
        admin_payment_order_id_from_suffix_path(&request_context.request_path, "/credit")
    else {
        return Ok(build_admin_payment_order_not_found_response());
    };
    let payload = match request_body {
        Some(body) if !body.is_empty() => {
            match serde_json::from_slice::<AdminPaymentOrderCreditRequest>(body) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(build_admin_payments_bad_request_response(
                        "请求数据验证失败",
                    ));
                }
            }
        }
        _ => AdminPaymentOrderCreditRequest::default(),
    };
    let gateway_order_id = match normalize_admin_payment_optional_string(
        payload.gateway_order_id,
        "gateway_order_id",
        128,
    ) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let pay_amount = match normalize_admin_payment_positive_number(payload.pay_amount, "pay_amount")
    {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let pay_currency = match normalize_admin_payment_currency(payload.pay_currency) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let exchange_rate =
        match normalize_admin_payment_positive_number(payload.exchange_rate, "exchange_rate") {
            Ok(value) => value,
            Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
        };
    if payload
        .gateway_response
        .as_ref()
        .is_some_and(|value| !value.is_object())
    {
        return Ok(build_admin_payments_bad_request_response(
            "gateway_response 必须为对象",
        ));
    }
    let operator_id = admin_payment_operator_id(request_context);
    match state
        .admin_credit_payment_order(
            &order_id,
            gateway_order_id.as_deref(),
            pay_amount,
            pay_currency.as_deref(),
            exchange_rate,
            payload.gateway_response,
            operator_id.as_deref(),
        )
        .await?
    {
        crate::gateway::AdminWalletMutationOutcome::Applied((order, credited)) => Ok(Json(json!({
            "order": build_admin_payment_order_payload(&order),
            "credited": credited,
        }))
        .into_response()),
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_payment_order_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_payments_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_payments_backend_unavailable_response(
                "Payment order write backend unavailable",
            ))
        }
    }
}

async fn build_admin_payment_fail_order_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(order_id) =
        admin_payment_order_id_from_suffix_path(&request_context.request_path, "/fail")
    else {
        return Ok(build_admin_payment_order_not_found_response());
    };
    match state.admin_fail_payment_order(&order_id).await? {
        crate::gateway::AdminWalletMutationOutcome::Applied(order) => Ok(Json(json!({
            "order": build_admin_payment_order_payload(&order),
        }))
        .into_response()),
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_payment_order_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_payments_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_payments_backend_unavailable_response(
                "Payment order write backend unavailable",
            ))
        }
    }
}

async fn build_admin_payment_callbacks_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_payments_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let offset = match parse_admin_payments_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let payment_method = query_param_value(query, "payment_method");

    if let Some((items, total)) = state
        .list_admin_payment_callbacks(payment_method.as_deref(), limit, offset)
        .await?
    {
        return Ok(Json(json!({
            "items": items
                .iter()
                .map(build_admin_payment_callback_payload_from_record)
                .collect::<Vec<_>>(),
            "total": total,
            "limit": limit,
            "offset": offset,
        }))
        .into_response());
    }

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM payment_callbacks
WHERE ($1::TEXT IS NULL OR payment_method = $1)
            "#,
        )
        .bind(payment_method.as_deref())
        .fetch_one(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        total = count_row
            .try_get::<i64, _>("total")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64;
        let rows = sqlx::query(
            r#"
SELECT
  id,
  payment_order_id,
  payment_method,
  callback_key,
  order_no,
  gateway_order_id,
  payload_hash,
  signature_valid,
  status,
  payload,
  error_message,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs
FROM payment_callbacks
WHERE ($1::TEXT IS NULL OR payment_method = $1)
ORDER BY created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(payment_method.as_deref())
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        items = rows
            .iter()
            .map(build_admin_payment_callback_payload)
            .collect::<Result<Vec<_>, GatewayError>>()?;
    } else {
        return Ok(Json(json!({
            "items": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
        }))
        .into_response());
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn maybe_build_local_admin_payments_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("payments_manage") {
        return Ok(None);
    }

    let normalized_path = request_context.request_path.trim_end_matches('/');
    let path = if normalized_path.is_empty() {
        request_context.request_path.as_str()
    } else {
        normalized_path
    };
    let is_payments_route = (request_context.request_method == http::Method::GET
        && path == "/api/admin/payments/orders")
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/payments/orders/")
            && path.matches('/').count() == 5)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/payments/orders/")
            && path.ends_with("/expire")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/payments/orders/")
            && path.ends_with("/credit")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::POST
            && path.starts_with("/api/admin/payments/orders/")
            && path.ends_with("/fail")
            && path.matches('/').count() == 6)
        || (request_context.request_method == http::Method::GET
            && path == "/api/admin/payments/callbacks");

    if !is_payments_route {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("list_orders") => Ok(Some(
            build_admin_payment_list_orders_response(state, request_context).await?,
        )),
        Some("get_order") => Ok(Some(
            build_admin_payment_get_order_response(state, request_context).await?,
        )),
        Some("expire_order") => Ok(Some(
            build_admin_payment_expire_order_response(state, request_context).await?,
        )),
        Some("credit_order") => Ok(Some(
            build_admin_payment_credit_order_response(state, request_context, request_body).await?,
        )),
        Some("fail_order") => Ok(Some(
            build_admin_payment_fail_order_response(state, request_context).await?,
        )),
        Some("list_callbacks") => Ok(Some(
            build_admin_payment_callbacks_response(state, request_context).await?,
        )),
        _ => Ok(Some(build_admin_payments_maintenance_response())),
    }
}
