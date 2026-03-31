use sqlx::Row;

const ADMIN_WALLETS_RUST_BACKEND_DETAIL: &str =
    "Admin wallets routes require Rust maintenance backend";
const ADMIN_WALLETS_API_KEY_REFUND_DETAIL: &str = "独立密钥钱包不支持退款审批";
const ADMIN_WALLETS_API_KEY_RECHARGE_DETAIL: &str = "独立密钥钱包不支持充值，请使用调账";
const ADMIN_WALLETS_API_KEY_GIFT_ADJUST_DETAIL: &str = "独立密钥钱包不支持赠款调账";

#[derive(Debug, serde::Deserialize)]
struct AdminWalletRechargeRequest {
    amount_usd: f64,
    #[serde(default = "default_admin_wallet_payment_method")]
    payment_method: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminWalletAdjustRequest {
    amount_usd: f64,
    #[serde(default = "default_admin_wallet_balance_type")]
    balance_type: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminWalletRefundFailRequest {
    reason: String,
}

#[derive(Debug, serde::Deserialize)]
struct AdminWalletRefundCompleteRequest {
    #[serde(default)]
    gateway_refund_id: Option<String>,
    #[serde(default)]
    payout_reference: Option<String>,
    #[serde(default)]
    payout_proof: Option<serde_json::Value>,
}

fn default_admin_wallet_payment_method() -> String {
    "admin_manual".to_string()
}

fn default_admin_wallet_balance_type() -> String {
    "recharge".to_string()
}

fn build_admin_wallets_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_WALLETS_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn build_admin_wallets_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn build_admin_wallet_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "Wallet not found" })),
    )
        .into_response()
}

fn build_admin_wallet_refund_not_found_response() -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": "Refund request not found" })),
    )
        .into_response()
}

fn build_admin_wallet_payment_order_payload(
    order_id: String,
    order_no: String,
    amount_usd: f64,
    payment_method: String,
    status: String,
    created_at: Option<String>,
    credited_at: Option<String>,
) -> serde_json::Value {
    json!({
        "id": order_id,
        "order_no": order_no,
        "amount_usd": amount_usd,
        "payment_method": payment_method,
        "status": status,
        "created_at": created_at,
        "credited_at": credited_at,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_admin_wallet_transaction_payload(
    wallet: &aether_data::repository::wallet::StoredWalletSnapshot,
    owner: &AdminWalletOwnerSummary,
    transaction_id: String,
    category: &str,
    reason_code: &str,
    amount: f64,
    balance_before: f64,
    balance_after: f64,
    recharge_balance_before: f64,
    recharge_balance_after: f64,
    gift_balance_before: f64,
    gift_balance_after: f64,
    link_type: Option<&str>,
    link_id: Option<&str>,
    operator_id: Option<&str>,
    description: Option<&str>,
    created_at: Option<String>,
) -> serde_json::Value {
    json!({
        "id": transaction_id,
        "wallet_id": wallet.id,
        "owner_type": owner.owner_type,
        "owner_name": owner.owner_name.clone(),
        "wallet_status": wallet.status,
        "category": category,
        "reason_code": reason_code,
        "amount": amount,
        "balance_before": balance_before,
        "balance_after": balance_after,
        "recharge_balance_before": recharge_balance_before,
        "recharge_balance_after": recharge_balance_after,
        "gift_balance_before": gift_balance_before,
        "gift_balance_after": gift_balance_after,
        "link_type": link_type,
        "link_id": link_id,
        "operator_id": operator_id,
        "operator_name": serde_json::Value::Null,
        "operator_email": serde_json::Value::Null,
        "description": description,
        "created_at": created_at,
    })
}

fn admin_wallet_build_order_no(now: chrono::DateTime<chrono::Utc>) -> String {
    format!(
        "po_{}_{}",
        now.format("%Y%m%d%H%M%S%6f"),
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

fn normalize_admin_wallet_description(value: Option<String>) -> Result<Option<String>, String> {
    match value {
        None => Ok(None),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            Ok(Some(trimmed.chars().take(500).collect()))
        }
    }
}

fn normalize_admin_wallet_required_text(
    value: String,
    field_name: &str,
    max_len: usize,
) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{field_name} 不能为空"));
    }
    if trimmed.chars().count() > max_len {
        return Err(format!("{field_name} 长度不能超过 {max_len}"));
    }
    Ok(trimmed.to_string())
}

fn normalize_admin_wallet_optional_text(
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

fn normalize_admin_wallet_payment_method(value: String) -> Result<String, String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err("payment_method 不能为空".to_string());
    }
    Ok(normalized.chars().take(30).collect())
}

fn normalize_admin_wallet_balance_type(value: String) -> Result<String, String> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "recharge" | "gift" => Ok(normalized),
        _ => Err("balance_type 必须为 recharge 或 gift".to_string()),
    }
}

fn normalize_admin_wallet_positive_amount(value: f64, field_name: &str) -> Result<f64, String> {
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{field_name} 必须为大于 0 的有限数字"));
    }
    Ok(value)
}

fn normalize_admin_wallet_non_zero_amount(value: f64, field_name: &str) -> Result<f64, String> {
    if !value.is_finite() || value == 0.0 {
        return Err(format!("{field_name} 不能为 0，且必须为有限数字"));
    }
    Ok(value)
}

fn admin_wallet_operator_id(request_context: &GatewayPublicRequestContext) -> Option<String> {
    request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.admin_principal.as_ref())
        .map(|principal| principal.user_id.clone())
}

fn admin_wallet_recharge_reason_code(payment_method: &str) -> &'static str {
    match payment_method {
        "card_code" | "gift_code" | "card_recharge" => "topup_card_code",
        _ => "topup_admin_manual",
    }
}

fn admin_wallet_apply_manual_recharge_to_snapshot(
    wallet: &mut aether_data::repository::wallet::StoredWalletSnapshot,
    amount_usd: f64,
) -> (f64, f64, f64, f64, f64, f64) {
    let recharge_before = wallet.balance;
    let gift_before = wallet.gift_balance;
    let balance_before = recharge_before + gift_before;

    wallet.balance += amount_usd;
    wallet.total_recharged += amount_usd;
    wallet.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;

    let recharge_after = wallet.balance;
    let gift_after = wallet.gift_balance;
    let balance_after = recharge_after + gift_after;

    (
        balance_before,
        balance_after,
        recharge_before,
        recharge_after,
        gift_before,
        gift_after,
    )
}

fn admin_wallet_apply_adjust_to_snapshot(
    wallet: &mut aether_data::repository::wallet::StoredWalletSnapshot,
    amount_usd: f64,
    balance_type: &str,
) -> Result<(f64, f64, f64, f64, f64, f64), String> {
    if amount_usd == 0.0 {
        return Err("adjust amount must not be zero".to_string());
    }
    if balance_type == "gift" && wallet.api_key_id.is_some() {
        return Err(ADMIN_WALLETS_API_KEY_GIFT_ADJUST_DETAIL.to_string());
    }

    let recharge_before = wallet.balance;
    let gift_before = wallet.gift_balance;
    let balance_before = recharge_before + gift_before;

    let mut recharge_after = recharge_before;
    let mut gift_after = gift_before;

    if amount_usd > 0.0 {
        if balance_type == "gift" {
            gift_after += amount_usd;
        } else {
            recharge_after += amount_usd;
        }
    } else {
        let mut remaining = -amount_usd;
        let consume_positive_bucket = |balance: &mut f64, remaining: &mut f64| {
            if *remaining <= 0.0 {
                return;
            }
            let available = balance.max(0.0);
            let consumed = available.min(*remaining);
            *balance -= consumed;
            *remaining -= consumed;
        };

        if balance_type == "gift" {
            consume_positive_bucket(&mut gift_after, &mut remaining);
            consume_positive_bucket(&mut recharge_after, &mut remaining);
        } else {
            consume_positive_bucket(&mut recharge_after, &mut remaining);
            consume_positive_bucket(&mut gift_after, &mut remaining);
        }

        if remaining > 0.0 {
            recharge_after -= remaining;
        }
        if gift_after < 0.0 {
            return Err("gift balance cannot be negative".to_string());
        }
    }

    wallet.balance = recharge_after;
    wallet.gift_balance = gift_after;
    wallet.total_adjusted += amount_usd;
    wallet.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;

    Ok((
        balance_before,
        recharge_after + gift_after,
        recharge_before,
        recharge_after,
        gift_before,
        gift_after,
    ))
}

fn admin_wallet_id_from_detail_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/admin/wallets/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn admin_wallet_id_from_suffix_path(request_path: &str, suffix: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/admin/wallets/")?
        .strip_suffix(suffix)
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty() && !value.contains('/'))
}

fn admin_wallet_refund_ids_from_suffix_path(
    request_path: &str,
    suffix: &str,
) -> Option<(String, String)> {
    let trimmed = request_path
        .strip_prefix("/api/admin/wallets/")?
        .strip_suffix(suffix)?
        .trim()
        .trim_matches('/');
    let mut segments = trimmed.split('/');
    let wallet_id = segments.next()?.trim();
    let literal = segments.next()?.trim();
    let refund_id = segments.next()?.trim();
    if literal != "refunds"
        || wallet_id.is_empty()
        || refund_id.is_empty()
        || wallet_id.contains('/')
        || refund_id.contains('/')
        || segments.next().is_some()
    {
        return None;
    }
    Some((wallet_id.to_string(), refund_id.to_string()))
}

fn parse_admin_wallets_limit(query: Option<&str>) -> Result<usize, String> {
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

fn parse_admin_wallets_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
        None => Ok(0),
    }
}

fn parse_admin_wallets_owner_type_filter(query: Option<&str>) -> Option<String> {
    match query_param_value(query, "owner_type") {
        Some(value) if value.eq_ignore_ascii_case("user") => Some("user".to_string()),
        Some(value) if value.eq_ignore_ascii_case("api_key") => Some("api_key".to_string()),
        _ => None,
    }
}

fn wallet_owner_summary_from_fields(
    user_id: Option<&str>,
    user_name: Option<String>,
    api_key_id: Option<&str>,
    api_key_name: Option<String>,
) -> AdminWalletOwnerSummary {
    if user_id.is_some() {
        return AdminWalletOwnerSummary {
            owner_type: "user",
            owner_name: user_name,
        };
    }
    if let Some(api_key_id) = api_key_id {
        return AdminWalletOwnerSummary {
            owner_type: "api_key",
            owner_name: api_key_name
                .filter(|value| !value.trim().is_empty())
                .or_else(|| Some(format!("Key-{}", &api_key_id[..api_key_id.len().min(8)]))),
        };
    }
    AdminWalletOwnerSummary {
        owner_type: "orphaned",
        owner_name: None,
    }
}

fn optional_epoch_value(
    row: &sqlx::postgres::PgRow,
    key: &str,
) -> Result<Option<String>, GatewayError> {
    Ok(row
        .try_get::<Option<i64>, _>(key)
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339))
}

#[derive(Clone)]
struct AdminWalletOwnerSummary {
    owner_type: &'static str,
    owner_name: Option<String>,
}

async fn resolve_admin_wallet_owner_summary(
    state: &AppState,
    wallet: &aether_data::repository::wallet::StoredWalletSnapshot,
) -> Result<AdminWalletOwnerSummary, GatewayError> {
    if let Some(user_id) = wallet.user_id.as_deref() {
        let user = state.find_user_auth_by_id(user_id).await?;
        Ok(AdminWalletOwnerSummary {
            owner_type: "user",
            owner_name: user.map(|record| record.username),
        })
    } else if let Some(api_key_id) = wallet.api_key_id.as_deref() {
        let api_key_ids = vec![api_key_id.to_string()];
        let snapshots = state
            .read_auth_api_key_snapshots_by_ids(&api_key_ids)
            .await?;
        let owner_name = snapshots
            .into_iter()
            .find(|snapshot| snapshot.api_key_id == api_key_id)
            .and_then(|snapshot| snapshot.api_key_name)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| Some(format!("Key-{}", &api_key_id[..api_key_id.len().min(8)])));
        Ok(AdminWalletOwnerSummary {
            owner_type: "api_key",
            owner_name,
        })
    } else {
        Ok(AdminWalletOwnerSummary {
            owner_type: "orphaned",
            owner_name: None,
        })
    }
}

fn build_admin_wallet_summary_payload(
    wallet: &aether_data::repository::wallet::StoredWalletSnapshot,
    owner: &AdminWalletOwnerSummary,
) -> serde_json::Value {
    json!({
        "id": wallet.id.clone(),
        "user_id": wallet.user_id.clone(),
        "api_key_id": wallet.api_key_id.clone(),
        "owner_type": owner.owner_type,
        "owner_name": owner.owner_name.clone(),
        "balance": wallet.balance + wallet.gift_balance,
        "recharge_balance": wallet.balance,
        "gift_balance": wallet.gift_balance,
        "refundable_balance": wallet.balance,
        "currency": wallet.currency.clone(),
        "status": wallet.status.clone(),
        "limit_mode": wallet.limit_mode.clone(),
        "unlimited": wallet.limit_mode.eq_ignore_ascii_case("unlimited"),
        "total_recharged": wallet.total_recharged,
        "total_consumed": wallet.total_consumed,
        "total_refunded": wallet.total_refunded,
        "total_adjusted": wallet.total_adjusted,
        "created_at": serde_json::Value::Null,
        "updated_at": unix_secs_to_rfc3339(wallet.updated_at_unix_secs),
    })
}

fn build_admin_wallet_refund_payload(
    wallet: &aether_data::repository::wallet::StoredWalletSnapshot,
    owner: &AdminWalletOwnerSummary,
    refund: &crate::gateway::AdminWalletRefundRecord,
) -> serde_json::Value {
    json!({
        "id": refund.id.clone(),
        "refund_no": refund.refund_no.clone(),
        "wallet_id": refund.wallet_id.clone(),
        "owner_type": owner.owner_type,
        "owner_name": owner.owner_name.clone(),
        "wallet_status": wallet.status.clone(),
        "user_id": refund.user_id.clone(),
        "payment_order_id": refund.payment_order_id.clone(),
        "source_type": refund.source_type.clone(),
        "source_id": refund.source_id.clone(),
        "refund_mode": refund.refund_mode.clone(),
        "amount_usd": refund.amount_usd,
        "status": refund.status.clone(),
        "reason": refund.reason.clone(),
        "failure_reason": refund.failure_reason.clone(),
        "gateway_refund_id": refund.gateway_refund_id.clone(),
        "payout_method": refund.payout_method.clone(),
        "payout_reference": refund.payout_reference.clone(),
        "payout_proof": refund.payout_proof.clone(),
        "requested_by": refund.requested_by.clone(),
        "approved_by": refund.approved_by.clone(),
        "processed_by": refund.processed_by.clone(),
        "created_at": unix_secs_to_rfc3339(refund.created_at_unix_secs),
        "updated_at": unix_secs_to_rfc3339(refund.updated_at_unix_secs),
        "processed_at": refund.processed_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "completed_at": refund.completed_at_unix_secs.and_then(unix_secs_to_rfc3339),
    })
}

async fn build_admin_wallet_detail_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) = admin_wallet_id_from_detail_path(&request_context.request_path) else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };

    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let mut payload = build_admin_wallet_summary_payload(&wallet, &owner);
    if let Some(object) = payload.as_object_mut() {
        object.insert("pending_refund_count".to_string(), serde_json::Value::Null);
    }
    Ok(Json(payload).into_response())
}

async fn build_admin_wallet_list_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let limit = match parse_admin_wallets_limit(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let status = query_param_value(request_context.request_query_string.as_deref(), "status");

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallets
WHERE ($1::TEXT IS NULL OR status = $1)
            "#,
        )
        .bind(status.as_deref())
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
  w.id,
  w.user_id,
  w.api_key_id,
  CAST(w.balance AS DOUBLE PRECISION) AS balance,
  CAST(w.gift_balance AS DOUBLE PRECISION) AS gift_balance,
  w.limit_mode,
  w.currency,
  w.status,
  CAST(w.total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(w.total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(w.total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(w.total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  users.username AS user_name,
  api_keys.name AS api_key_name,
  CAST(EXTRACT(EPOCH FROM w.created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM w.updated_at) AS BIGINT) AS updated_at_unix_secs
FROM wallets w
LEFT JOIN users ON users.id = w.user_id
LEFT JOIN api_keys ON api_keys.id = w.api_key_id
WHERE ($1::TEXT IS NULL OR w.status = $1)
ORDER BY w.updated_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(status.as_deref())
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .into_iter()
            .map(|row| {
                let wallet_id = row
                    .try_get::<String, _>("id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let user_id = row
                    .try_get::<Option<String>, _>("user_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let api_key_id = row
                    .try_get::<Option<String>, _>("api_key_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let owner = wallet_owner_summary_from_fields(
                    user_id.as_deref(),
                    row.try_get::<Option<String>, _>("user_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                    api_key_id.as_deref(),
                    row.try_get::<Option<String>, _>("api_key_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                );
                Ok(json!({
                    "id": wallet_id,
                    "user_id": user_id,
                    "api_key_id": api_key_id,
                    "owner_type": owner.owner_type,
                    "owner_name": owner.owner_name,
                    "balance": row.try_get::<f64, _>("balance").map_err(|err| GatewayError::Internal(err.to_string()))?
                        + row.try_get::<f64, _>("gift_balance").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "recharge_balance": row.try_get::<f64, _>("balance").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gift_balance": row.try_get::<f64, _>("gift_balance").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "refundable_balance": row.try_get::<f64, _>("balance").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "currency": row.try_get::<String, _>("currency").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "status": row.try_get::<String, _>("status").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "limit_mode": row.try_get::<String, _>("limit_mode").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "unlimited": row.try_get::<String, _>("limit_mode").map_err(|err| GatewayError::Internal(err.to_string()))?.eq_ignore_ascii_case("unlimited"),
                    "total_recharged": row.try_get::<f64, _>("total_recharged").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "total_consumed": row.try_get::<f64, _>("total_consumed").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "total_refunded": row.try_get::<f64, _>("total_refunded").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "total_adjusted": row.try_get::<f64, _>("total_adjusted").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "created_at": optional_epoch_value(&row, "created_at_unix_secs")?,
                    "updated_at": optional_epoch_value(&row, "updated_at_unix_secs")?,
                }))
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn build_admin_wallet_ledger_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_wallets_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let category = query_param_value(query, "category");
    let reason_code = query_param_value(query, "reason_code");
    let owner_type = parse_admin_wallets_owner_type_filter(query);

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallet_transactions tx
JOIN wallets w ON w.id = tx.wallet_id
WHERE ($1::TEXT IS NULL OR tx.category = $1)
  AND ($2::TEXT IS NULL OR tx.reason_code = $2)
  AND (
    $3::TEXT IS NULL
    OR ($3 = 'user' AND w.user_id IS NOT NULL)
    OR ($3 = 'api_key' AND w.api_key_id IS NOT NULL)
  )
            "#,
        )
        .bind(category.as_deref())
        .bind(reason_code.as_deref())
        .bind(owner_type.as_deref())
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
  tx.id,
  tx.wallet_id,
  tx.category,
  tx.reason_code,
  CAST(tx.amount AS DOUBLE PRECISION) AS amount,
  CAST(tx.balance_before AS DOUBLE PRECISION) AS balance_before,
  CAST(tx.balance_after AS DOUBLE PRECISION) AS balance_after,
  CAST(tx.recharge_balance_before AS DOUBLE PRECISION) AS recharge_balance_before,
  CAST(tx.recharge_balance_after AS DOUBLE PRECISION) AS recharge_balance_after,
  CAST(tx.gift_balance_before AS DOUBLE PRECISION) AS gift_balance_before,
  CAST(tx.gift_balance_after AS DOUBLE PRECISION) AS gift_balance_after,
  tx.link_type,
  tx.link_id,
  tx.operator_id,
  tx.description,
  w.user_id,
  w.api_key_id,
  w.status AS wallet_status,
  wallet_users.username AS wallet_user_name,
  api_keys.name AS api_key_name,
  operator_users.username AS operator_name,
  operator_users.email AS operator_email,
  CAST(EXTRACT(EPOCH FROM tx.created_at) AS BIGINT) AS created_at_unix_secs
FROM wallet_transactions tx
JOIN wallets w ON w.id = tx.wallet_id
LEFT JOIN users wallet_users ON wallet_users.id = w.user_id
LEFT JOIN api_keys ON api_keys.id = w.api_key_id
LEFT JOIN users operator_users ON operator_users.id = tx.operator_id
WHERE ($1::TEXT IS NULL OR tx.category = $1)
  AND ($2::TEXT IS NULL OR tx.reason_code = $2)
  AND (
    $3::TEXT IS NULL
    OR ($3 = 'user' AND w.user_id IS NOT NULL)
    OR ($3 = 'api_key' AND w.api_key_id IS NOT NULL)
  )
ORDER BY tx.created_at DESC
OFFSET $4
LIMIT $5
            "#,
        )
        .bind(category.as_deref())
        .bind(reason_code.as_deref())
        .bind(owner_type.as_deref())
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .into_iter()
            .map(|row| {
                let user_id = row
                    .try_get::<Option<String>, _>("user_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let api_key_id = row
                    .try_get::<Option<String>, _>("api_key_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let owner = wallet_owner_summary_from_fields(
                    user_id.as_deref(),
                    row.try_get::<Option<String>, _>("wallet_user_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                    api_key_id.as_deref(),
                    row.try_get::<Option<String>, _>("api_key_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                );
                Ok(json!({
                    "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "wallet_id": row.try_get::<String, _>("wallet_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "owner_type": owner.owner_type,
                    "owner_name": owner.owner_name,
                    "wallet_status": row.try_get::<String, _>("wallet_status").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "category": row.try_get::<String, _>("category").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "reason_code": row.try_get::<String, _>("reason_code").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "amount": row.try_get::<f64, _>("amount").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "balance_before": row.try_get::<f64, _>("balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "balance_after": row.try_get::<f64, _>("balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "recharge_balance_before": row.try_get::<f64, _>("recharge_balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "recharge_balance_after": row.try_get::<f64, _>("recharge_balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gift_balance_before": row.try_get::<f64, _>("gift_balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gift_balance_after": row.try_get::<f64, _>("gift_balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "link_type": row.try_get::<Option<String>, _>("link_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "link_id": row.try_get::<Option<String>, _>("link_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_id": row.try_get::<Option<String>, _>("operator_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_name": row.try_get::<Option<String>, _>("operator_name").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_email": row.try_get::<Option<String>, _>("operator_email").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "description": row.try_get::<Option<String>, _>("description").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "created_at": optional_epoch_value(&row, "created_at_unix_secs")?,
                }))
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn build_admin_wallet_refund_requests_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_wallets_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let status = query_param_value(query, "status");
    let owner_type = parse_admin_wallets_owner_type_filter(query);
    if owner_type.as_deref() == Some("api_key") {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some((refunds, refund_total)) = state
        .list_admin_wallet_refund_requests(status.as_deref(), limit, offset)
        .await?
    {
        total = refund_total;
        let mut local_items = Vec::with_capacity(refunds.len());
        for refund in refunds {
            let Some(wallet) = state
                .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
                    &refund.wallet_id,
                ))
                .await?
            else {
                continue;
            };
            let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
            local_items.push(build_admin_wallet_refund_payload(&wallet, &owner, &refund));
        }
        items = local_items;
    } else if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM refund_requests rr
JOIN wallets w ON w.id = rr.wallet_id
WHERE ($1::TEXT IS NULL OR rr.status = $1)
  AND w.user_id IS NOT NULL
            "#,
        )
        .bind(status.as_deref())
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
  rr.id,
  rr.refund_no,
  rr.wallet_id,
  rr.user_id,
  rr.payment_order_id,
  rr.source_type,
  rr.source_id,
  rr.refund_mode,
  CAST(rr.amount_usd AS DOUBLE PRECISION) AS amount_usd,
  rr.status,
  rr.reason,
  rr.failure_reason,
  rr.gateway_refund_id,
  rr.payout_method,
  rr.payout_reference,
  rr.payout_proof,
  rr.requested_by,
  rr.approved_by,
  rr.processed_by,
  w.user_id AS wallet_user_id,
  w.api_key_id AS wallet_api_key_id,
  w.status AS wallet_status,
  wallet_users.username AS wallet_user_name,
  api_keys.name AS api_key_name,
  CAST(EXTRACT(EPOCH FROM rr.created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM rr.updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM rr.processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM rr.completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests rr
JOIN wallets w ON w.id = rr.wallet_id
LEFT JOIN users wallet_users ON wallet_users.id = w.user_id
LEFT JOIN api_keys ON api_keys.id = w.api_key_id
WHERE ($1::TEXT IS NULL OR rr.status = $1)
  AND w.user_id IS NOT NULL
ORDER BY rr.created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(status.as_deref())
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .into_iter()
            .map(|row| {
                let wallet_user_id = row
                    .try_get::<Option<String>, _>("wallet_user_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let wallet_api_key_id = row
                    .try_get::<Option<String>, _>("wallet_api_key_id")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                let owner = wallet_owner_summary_from_fields(
                    wallet_user_id.as_deref(),
                    row.try_get::<Option<String>, _>("wallet_user_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                    wallet_api_key_id.as_deref(),
                    row.try_get::<Option<String>, _>("api_key_name")
                        .map_err(|err| GatewayError::Internal(err.to_string()))?,
                );
                Ok(json!({
                    "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "refund_no": row.try_get::<String, _>("refund_no").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "wallet_id": row.try_get::<String, _>("wallet_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "owner_type": owner.owner_type,
                    "owner_name": owner.owner_name,
                    "wallet_status": row.try_get::<String, _>("wallet_status").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "user_id": row.try_get::<Option<String>, _>("user_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payment_order_id": row.try_get::<Option<String>, _>("payment_order_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "source_type": row.try_get::<String, _>("source_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "source_id": row.try_get::<Option<String>, _>("source_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "refund_mode": row.try_get::<String, _>("refund_mode").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "amount_usd": row.try_get::<f64, _>("amount_usd").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "status": row.try_get::<String, _>("status").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "reason": row.try_get::<Option<String>, _>("reason").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "failure_reason": row.try_get::<Option<String>, _>("failure_reason").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gateway_refund_id": row.try_get::<Option<String>, _>("gateway_refund_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_method": row.try_get::<Option<String>, _>("payout_method").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_reference": row.try_get::<Option<String>, _>("payout_reference").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_proof": row.try_get::<Option<serde_json::Value>, _>("payout_proof").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "requested_by": row.try_get::<Option<String>, _>("requested_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "approved_by": row.try_get::<Option<String>, _>("approved_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "processed_by": row.try_get::<Option<String>, _>("processed_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "created_at": optional_epoch_value(&row, "created_at_unix_secs")?,
                    "updated_at": optional_epoch_value(&row, "updated_at_unix_secs")?,
                    "processed_at": optional_epoch_value(&row, "processed_at_unix_secs")?,
                    "completed_at": optional_epoch_value(&row, "completed_at_unix_secs")?,
                }))
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn build_admin_wallet_transactions_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/transactions")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let limit = match parse_admin_wallets_limit(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let wallet_payload = build_admin_wallet_summary_payload(&wallet, &owner);

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some((transactions, transaction_total)) = state
        .list_admin_wallet_transactions(&wallet.id, limit, offset)
        .await?
    {
        total = transaction_total;
        let mut local_items = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let (operator_name, operator_email) = match transaction.operator_id.as_deref() {
                Some(operator_id) => state
                    .find_user_auth_by_id(operator_id)
                    .await?
                    .map(|user| (Some(user.username), user.email))
                    .unwrap_or((None, None)),
                None => (None, None),
            };
            local_items.push(json!({
                "id": transaction.id,
                "wallet_id": transaction.wallet_id,
                "owner_type": owner.owner_type,
                "owner_name": owner.owner_name.clone(),
                "wallet_status": wallet.status.clone(),
                "category": transaction.category,
                "reason_code": transaction.reason_code,
                "amount": transaction.amount,
                "balance_before": transaction.balance_before,
                "balance_after": transaction.balance_after,
                "recharge_balance_before": transaction.recharge_balance_before,
                "recharge_balance_after": transaction.recharge_balance_after,
                "gift_balance_before": transaction.gift_balance_before,
                "gift_balance_after": transaction.gift_balance_after,
                "link_type": transaction.link_type,
                "link_id": transaction.link_id,
                "operator_id": transaction.operator_id,
                "operator_name": operator_name,
                "operator_email": operator_email,
                "description": transaction.description,
                "created_at": unix_secs_to_rfc3339(transaction.created_at_unix_secs),
            }));
        }
        items = local_items;
    } else if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallet_transactions
WHERE wallet_id = $1
            "#,
        )
        .bind(&wallet.id)
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
  tx.id,
  tx.wallet_id,
  tx.category,
  tx.reason_code,
  CAST(tx.amount AS DOUBLE PRECISION) AS amount,
  CAST(tx.balance_before AS DOUBLE PRECISION) AS balance_before,
  CAST(tx.balance_after AS DOUBLE PRECISION) AS balance_after,
  CAST(tx.recharge_balance_before AS DOUBLE PRECISION) AS recharge_balance_before,
  CAST(tx.recharge_balance_after AS DOUBLE PRECISION) AS recharge_balance_after,
  CAST(tx.gift_balance_before AS DOUBLE PRECISION) AS gift_balance_before,
  CAST(tx.gift_balance_after AS DOUBLE PRECISION) AS gift_balance_after,
  tx.link_type,
  tx.link_id,
  tx.operator_id,
  tx.description,
  operator_users.username AS operator_name,
  operator_users.email AS operator_email,
  CAST(EXTRACT(EPOCH FROM tx.created_at) AS BIGINT) AS created_at_unix_secs
FROM wallet_transactions tx
LEFT JOIN users operator_users
  ON operator_users.id = tx.operator_id
WHERE tx.wallet_id = $1
ORDER BY tx.created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(&wallet.id)
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .into_iter()
            .map(|row| {
                let created_at_unix_secs = row
                    .try_get::<Option<i64>, _>("created_at_unix_secs")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
                    .and_then(|value| u64::try_from(value).ok())
                    .and_then(unix_secs_to_rfc3339);
                Ok(json!({
                    "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "wallet_id": row.try_get::<String, _>("wallet_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "owner_type": owner.owner_type,
                    "owner_name": owner.owner_name.clone(),
                    "wallet_status": wallet.status.clone(),
                    "category": row.try_get::<String, _>("category").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "reason_code": row.try_get::<String, _>("reason_code").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "amount": row.try_get::<f64, _>("amount").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "balance_before": row.try_get::<f64, _>("balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "balance_after": row.try_get::<f64, _>("balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "recharge_balance_before": row.try_get::<f64, _>("recharge_balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "recharge_balance_after": row.try_get::<f64, _>("recharge_balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gift_balance_before": row.try_get::<f64, _>("gift_balance_before").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gift_balance_after": row.try_get::<f64, _>("gift_balance_after").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "link_type": row.try_get::<Option<String>, _>("link_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "link_id": row.try_get::<Option<String>, _>("link_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_id": row.try_get::<Option<String>, _>("operator_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_name": row.try_get::<Option<String>, _>("operator_name").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "operator_email": row.try_get::<Option<String>, _>("operator_email").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "description": row.try_get::<Option<String>, _>("description").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "created_at": created_at_unix_secs,
                }))
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "wallet": wallet_payload,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn build_admin_wallet_refunds_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/refunds")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let limit = match parse_admin_wallets_limit(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let offset = match parse_admin_wallets_offset(request_context.request_query_string.as_deref()) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let wallet_payload = build_admin_wallet_summary_payload(&wallet, &owner);
    let mut total = 0_u64;
    let mut items = Vec::new();

    if let Some((refunds, refund_total)) = state
        .list_admin_wallet_refunds(&wallet.id, limit, offset)
        .await?
    {
        total = refund_total;
        items = refunds
            .into_iter()
            .map(|refund| build_admin_wallet_refund_payload(&wallet, &owner, &refund))
            .collect();
    } else if let Some(pool) = state.postgres_pool() {
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM refund_requests
WHERE wallet_id = $1
            "#,
        )
        .bind(&wallet.id)
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
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests
WHERE wallet_id = $1
ORDER BY created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(&wallet.id)
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        items = rows
            .into_iter()
            .map(|row| {
                let created_at = row
                    .try_get::<Option<i64>, _>("created_at_unix_secs")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
                    .and_then(|value| u64::try_from(value).ok())
                    .and_then(unix_secs_to_rfc3339);
                let updated_at = row
                    .try_get::<Option<i64>, _>("updated_at_unix_secs")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
                    .and_then(|value| u64::try_from(value).ok())
                    .and_then(unix_secs_to_rfc3339);
                let processed_at = row
                    .try_get::<Option<i64>, _>("processed_at_unix_secs")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
                    .and_then(|value| u64::try_from(value).ok())
                    .and_then(unix_secs_to_rfc3339);
                let completed_at = row
                    .try_get::<Option<i64>, _>("completed_at_unix_secs")
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
                    .and_then(|value| u64::try_from(value).ok())
                    .and_then(unix_secs_to_rfc3339);
                Ok(json!({
                    "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "refund_no": row.try_get::<String, _>("refund_no").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "wallet_id": row.try_get::<String, _>("wallet_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "owner_type": owner.owner_type,
                    "owner_name": owner.owner_name.clone(),
                    "wallet_status": wallet.status.clone(),
                    "user_id": row.try_get::<Option<String>, _>("user_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payment_order_id": row.try_get::<Option<String>, _>("payment_order_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "source_type": row.try_get::<String, _>("source_type").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "source_id": row.try_get::<Option<String>, _>("source_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "refund_mode": row.try_get::<String, _>("refund_mode").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "amount_usd": row.try_get::<f64, _>("amount_usd").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "status": row.try_get::<String, _>("status").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "reason": row.try_get::<Option<String>, _>("reason").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "failure_reason": row.try_get::<Option<String>, _>("failure_reason").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "gateway_refund_id": row.try_get::<Option<String>, _>("gateway_refund_id").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_method": row.try_get::<Option<String>, _>("payout_method").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_reference": row.try_get::<Option<String>, _>("payout_reference").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "payout_proof": row.try_get::<Option<serde_json::Value>, _>("payout_proof").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "requested_by": row.try_get::<Option<String>, _>("requested_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "approved_by": row.try_get::<Option<String>, _>("approved_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "processed_by": row.try_get::<Option<String>, _>("processed_by").map_err(|err| GatewayError::Internal(err.to_string()))?,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "processed_at": processed_at,
                    "completed_at": completed_at,
                }))
            })
            .collect::<Result<Vec<_>, GatewayError>>()?;
    }

    Ok(Json(json!({
        "wallet": wallet_payload,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}

async fn build_admin_wallet_adjust_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/adjust")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_wallets_bad_request_response("请求体不能为空"));
    };
    let payload = match serde_json::from_slice::<AdminWalletAdjustRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_wallets_bad_request_response("请求体格式无效")),
    };
    let amount_usd = match normalize_admin_wallet_non_zero_amount(payload.amount_usd, "amount_usd")
    {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let balance_type = match normalize_admin_wallet_balance_type(payload.balance_type) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let description = match normalize_admin_wallet_description(payload.description) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(existing_wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if existing_wallet.api_key_id.is_some() && balance_type == "gift" {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_GIFT_ADJUST_DETAIL,
        ));
    }
    let operator_id = admin_wallet_operator_id(request_context);
    let has_postgres = state.postgres_pool().is_some();
    let Some((wallet, transaction)) = state
        .admin_adjust_wallet_balance(
            &wallet_id,
            amount_usd,
            &balance_type,
            operator_id.as_deref(),
            description.as_deref(),
        )
        .await?
    else {
        return if has_postgres {
            Ok(build_admin_wallet_not_found_response())
        } else {
            Ok(build_admin_wallets_maintenance_response())
        };
    };
    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    let wallet_payload = build_admin_wallet_summary_payload(&wallet, &owner);
    let transaction_payload = build_admin_wallet_transaction_payload(
        &wallet,
        &owner,
        transaction.id,
        &transaction.category,
        &transaction.reason_code,
        transaction.amount,
        transaction.balance_before,
        transaction.balance_after,
        transaction.recharge_balance_before,
        transaction.recharge_balance_after,
        transaction.gift_balance_before,
        transaction.gift_balance_after,
        transaction.link_type.as_deref(),
        transaction.link_id.as_deref(),
        transaction.operator_id.as_deref(),
        transaction.description.as_deref(),
        unix_secs_to_rfc3339(transaction.created_at_unix_secs),
    );
    Ok(Json(json!({
        "wallet": wallet_payload,
        "transaction": transaction_payload,
    }))
    .into_response())
}

async fn build_admin_wallet_recharge_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(wallet_id) =
        admin_wallet_id_from_suffix_path(&request_context.request_path, "/recharge")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 无效"));
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_wallets_bad_request_response("请求体不能为空"));
    };
    let payload = match serde_json::from_slice::<AdminWalletRechargeRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_wallets_bad_request_response("请求体格式无效")),
    };
    let amount_usd = match normalize_admin_wallet_positive_amount(payload.amount_usd, "amount_usd")
    {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let payment_method = match normalize_admin_wallet_payment_method(payload.payment_method) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let description = match normalize_admin_wallet_description(payload.description) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(existing_wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if existing_wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_RECHARGE_DETAIL,
        ));
    }
    let operator_id = admin_wallet_operator_id(request_context);
    let has_postgres = state.postgres_pool().is_some();
    let Some((wallet, payment_order)) = state
        .admin_create_manual_wallet_recharge(
            &wallet_id,
            amount_usd,
            &payment_method,
            operator_id.as_deref(),
            description.as_deref(),
        )
        .await?
    else {
        return if has_postgres {
            Ok(build_admin_wallet_not_found_response())
        } else {
            Ok(build_admin_wallets_maintenance_response())
        };
    };
    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    Ok(Json(json!({
        "wallet": build_admin_wallet_summary_payload(&wallet, &owner),
        "payment_order": build_admin_wallet_payment_order_payload(
            payment_order.id,
            payment_order.order_no,
            payment_order.amount_usd,
            payment_order.payment_method,
            payment_order.status,
            unix_secs_to_rfc3339(payment_order.created_at_unix_secs),
            payment_order
                .credited_at_unix_secs
                .and_then(unix_secs_to_rfc3339),
        ),
    }))
    .into_response())
}

async fn build_admin_wallet_process_refund_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((wallet_id, refund_id)) =
        admin_wallet_refund_ids_from_suffix_path(&request_context.request_path, "/process")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 或 refund_id 无效"));
    };

    let Some(existing_wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if existing_wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let operator_id = admin_wallet_operator_id(request_context);
    match state
        .admin_process_wallet_refund(&wallet_id, &refund_id, operator_id.as_deref())
        .await?
    {
        crate::gateway::AdminWalletMutationOutcome::Applied((wallet, refund, transaction)) => {
            let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
            Ok(Json(json!({
                "wallet": build_admin_wallet_summary_payload(&wallet, &owner),
                "refund": build_admin_wallet_refund_payload(&wallet, &owner, &refund),
                "transaction": build_admin_wallet_transaction_payload(
                    &wallet,
                    &owner,
                    transaction.id,
                    &transaction.category,
                    &transaction.reason_code,
                    transaction.amount,
                    transaction.balance_before,
                    transaction.balance_after,
                    transaction.recharge_balance_before,
                    transaction.recharge_balance_after,
                    transaction.gift_balance_before,
                    transaction.gift_balance_after,
                    transaction.link_type.as_deref(),
                    transaction.link_id.as_deref(),
                    transaction.operator_id.as_deref(),
                    transaction.description.as_deref(),
                    unix_secs_to_rfc3339(transaction.created_at_unix_secs),
                ),
            }))
            .into_response())
        }
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_wallet_refund_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_wallets_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_wallets_maintenance_response())
        }
    }
}

async fn build_admin_wallet_complete_refund_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some((wallet_id, refund_id)) =
        admin_wallet_refund_ids_from_suffix_path(&request_context.request_path, "/complete")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 或 refund_id 无效"));
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_wallets_bad_request_response("请求体不能为空"));
    };
    let payload = match serde_json::from_slice::<AdminWalletRefundCompleteRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_wallets_bad_request_response("请求体格式无效")),
    };
    let gateway_refund_id = match normalize_admin_wallet_optional_text(
        payload.gateway_refund_id,
        "gateway_refund_id",
        128,
    ) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    let payout_reference = match normalize_admin_wallet_optional_text(
        payload.payout_reference,
        "payout_reference",
        255,
    ) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };
    if payload
        .payout_proof
        .as_ref()
        .is_some_and(|value| !value.is_object())
    {
        return Ok(build_admin_wallets_bad_request_response(
            "payout_proof 必须为对象",
        ));
    }

    let Some(wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
    match state
        .admin_complete_wallet_refund(
            &wallet_id,
            &refund_id,
            gateway_refund_id.as_deref(),
            payout_reference.as_deref(),
            payload.payout_proof,
        )
        .await?
    {
        crate::gateway::AdminWalletMutationOutcome::Applied(refund) => Ok(Json(json!({
            "refund": build_admin_wallet_refund_payload(&wallet, &owner, &refund),
        }))
        .into_response()),
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_wallet_refund_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            let detail = if detail == "refund status must be processing before completion" {
                "只有 processing 状态的退款可以标记完成".to_string()
            } else {
                detail
            };
            Ok(build_admin_wallets_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_wallets_maintenance_response())
        }
    }
}

async fn build_admin_wallet_fail_refund_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some((wallet_id, refund_id)) =
        admin_wallet_refund_ids_from_suffix_path(&request_context.request_path, "/fail")
    else {
        return Ok(build_admin_wallets_bad_request_response("wallet_id 或 refund_id 无效"));
    };
    let Some(request_body) = request_body else {
        return Ok(build_admin_wallets_bad_request_response("请求体不能为空"));
    };
    let payload = match serde_json::from_slice::<AdminWalletRefundFailRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return Ok(build_admin_wallets_bad_request_response("请求体格式无效")),
    };
    let reason = match normalize_admin_wallet_required_text(payload.reason, "reason", 500) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_wallets_bad_request_response(detail)),
    };

    let Some(existing_wallet) = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::WalletId(
            &wallet_id,
        ))
        .await?
    else {
        return Ok(build_admin_wallet_not_found_response());
    };
    if existing_wallet.api_key_id.is_some() {
        return Ok(build_admin_wallets_bad_request_response(
            ADMIN_WALLETS_API_KEY_REFUND_DETAIL,
        ));
    }

    let operator_id = admin_wallet_operator_id(request_context);
    match state
        .admin_fail_wallet_refund(&wallet_id, &refund_id, &reason, operator_id.as_deref())
        .await?
    {
        crate::gateway::AdminWalletMutationOutcome::Applied((wallet, refund, transaction)) => {
            let owner = resolve_admin_wallet_owner_summary(state, &wallet).await?;
            Ok(Json(json!({
                "wallet": build_admin_wallet_summary_payload(&wallet, &owner),
                "refund": build_admin_wallet_refund_payload(&wallet, &owner, &refund),
                "transaction": transaction.map(|transaction| build_admin_wallet_transaction_payload(
                    &wallet,
                    &owner,
                    transaction.id,
                    &transaction.category,
                    &transaction.reason_code,
                    transaction.amount,
                    transaction.balance_before,
                    transaction.balance_after,
                    transaction.recharge_balance_before,
                    transaction.recharge_balance_after,
                    transaction.gift_balance_before,
                    transaction.gift_balance_after,
                    transaction.link_type.as_deref(),
                    transaction.link_id.as_deref(),
                    transaction.operator_id.as_deref(),
                    transaction.description.as_deref(),
                    unix_secs_to_rfc3339(transaction.created_at_unix_secs),
                )).unwrap_or(serde_json::Value::Null),
            }))
            .into_response())
        }
        crate::gateway::AdminWalletMutationOutcome::NotFound => {
            Ok(build_admin_wallet_refund_not_found_response())
        }
        crate::gateway::AdminWalletMutationOutcome::Invalid(detail) => {
            Ok(build_admin_wallets_bad_request_response(detail))
        }
        crate::gateway::AdminWalletMutationOutcome::Unavailable => {
            Ok(build_admin_wallets_maintenance_response())
        }
    }
}

async fn maybe_build_local_admin_wallets_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("wallets_manage") {
        return Ok(None);
    }

    let path = request_context.request_path.as_str();
    let is_wallets_route = (request_context.request_method == http::Method::GET
        && matches!(path, "/api/admin/wallets" | "/api/admin/wallets/"))
        || (request_context.request_method == http::Method::GET
            && matches!(
                path,
                "/api/admin/wallets/ledger" | "/api/admin/wallets/ledger/"
            ))
        || (request_context.request_method == http::Method::GET
            && matches!(
                path,
                "/api/admin/wallets/refund-requests" | "/api/admin/wallets/refund-requests/"
            ))
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/wallets/")
            && path.ends_with("/transactions"))
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/wallets/")
            && path.ends_with("/refunds"))
        || (request_context.request_method == http::Method::GET
            && path.starts_with("/api/admin/wallets/")
            && !path.ends_with("/transactions")
            && !path.ends_with("/refunds")
            && path.matches('/').count() == 4)
        || (request_context.request_method == http::Method::POST
            && matches!(
                decision.route_kind.as_deref(),
                Some(
                    "adjust_balance"
                        | "recharge_balance"
                        | "process_refund"
                        | "complete_refund"
                        | "fail_refund"
                )
            ));

    if !is_wallets_route {
        return Ok(None);
    }

    if decision.route_kind.as_deref() == Some("wallet_detail")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_detail_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("list_wallets")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_list_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("ledger")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_ledger_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("list_refund_requests")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_refund_requests_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("list_wallet_transactions")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_transactions_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("list_wallet_refunds")
        && request_context.request_method == http::Method::GET
    {
        return Ok(Some(
            build_admin_wallet_refunds_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("adjust_balance")
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            build_admin_wallet_adjust_response(state, request_context, request_body).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("recharge_balance")
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            build_admin_wallet_recharge_response(state, request_context, request_body).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("process_refund")
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            build_admin_wallet_process_refund_response(state, request_context).await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("complete_refund")
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            build_admin_wallet_complete_refund_response(state, request_context, request_body)
                .await?,
        ));
    }
    if decision.route_kind.as_deref() == Some("fail_refund")
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            build_admin_wallet_fail_refund_response(state, request_context, request_body).await?,
        ));
    }

    Ok(Some(build_admin_wallets_maintenance_response()))
}
