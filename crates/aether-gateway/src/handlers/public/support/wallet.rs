use sqlx::Row;

const WALLET_LEGACY_TIMEZONE: &str = "Asia/Shanghai";
const WALLET_SAFE_GATEWAY_RESPONSE_KEYS: &[&str] = &[
    "gateway",
    "display_name",
    "gateway_order_id",
    "payment_url",
    "qr_code",
    "expires_at",
    "manual_credit",
];

#[derive(Debug, Deserialize)]
struct WalletCreateRefundRequest {
    amount_usd: f64,
    #[serde(default)]
    payment_order_id: Option<String>,
    #[serde(default)]
    source_type: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    refund_mode: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
struct NormalizedWalletCreateRefundRequest {
    amount_usd: f64,
    payment_order_id: Option<String>,
    source_type: Option<String>,
    source_id: Option<String>,
    refund_mode: Option<String>,
    reason: Option<String>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WalletCreateRechargeRequest {
    amount_usd: f64,
    payment_method: String,
    #[serde(default)]
    pay_amount: Option<f64>,
    #[serde(default)]
    pay_currency: Option<String>,
    #[serde(default)]
    exchange_rate: Option<f64>,
}

#[derive(Debug, Clone)]
struct NormalizedWalletCreateRechargeRequest {
    amount_usd: f64,
    payment_method: String,
    pay_amount: Option<f64>,
    pay_currency: Option<String>,
    exchange_rate: Option<f64>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct WalletTestRefundRecord {
    wallet_id: String,
    user_id: String,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct WalletTestRechargeRecord {
    user_id: String,
    payload: serde_json::Value,
}

#[cfg(test)]
fn wallet_test_refund_store() -> &'static std::sync::Mutex<Vec<WalletTestRefundRecord>> {
    static STORE: std::sync::OnceLock<std::sync::Mutex<Vec<WalletTestRefundRecord>>> =
        std::sync::OnceLock::new();
    STORE.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

#[cfg(test)]
fn wallet_test_recharge_store() -> &'static std::sync::Mutex<Vec<WalletTestRechargeRecord>> {
    static STORE: std::sync::OnceLock<std::sync::Mutex<Vec<WalletTestRechargeRecord>>> =
        std::sync::OnceLock::new();
    STORE.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn wallet_normalize_optional_string_field(
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > max_chars {
        return Err("输入验证失败");
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_wallet_create_refund_request(
    payload: WalletCreateRefundRequest,
) -> Result<NormalizedWalletCreateRefundRequest, &'static str> {
    if !payload.amount_usd.is_finite() || payload.amount_usd <= 0.0 {
        return Err("输入验证失败");
    }

    Ok(NormalizedWalletCreateRefundRequest {
        amount_usd: payload.amount_usd,
        payment_order_id: wallet_normalize_optional_string_field(payload.payment_order_id, 100)?,
        source_type: wallet_normalize_optional_string_field(payload.source_type, 30)?,
        source_id: wallet_normalize_optional_string_field(payload.source_id, 100)?,
        refund_mode: wallet_normalize_optional_string_field(payload.refund_mode, 30)?,
        reason: wallet_normalize_optional_string_field(payload.reason, 500)?,
        idempotency_key: wallet_normalize_optional_string_field(payload.idempotency_key, 128)?,
    })
}

fn normalize_wallet_create_recharge_request(
    payload: WalletCreateRechargeRequest,
) -> Result<NormalizedWalletCreateRechargeRequest, &'static str> {
    if !payload.amount_usd.is_finite() || payload.amount_usd <= 0.0 {
        return Err("输入验证失败");
    }
    let payment_method = payload.payment_method.trim().to_ascii_lowercase();
    if payment_method.is_empty() || payment_method.chars().count() > 30 {
        return Err("输入验证失败");
    }
    if matches!(payload.pay_amount, Some(value) if !value.is_finite() || value <= 0.0) {
        return Err("输入验证失败");
    }
    if matches!(payload.exchange_rate, Some(value) if !value.is_finite() || value <= 0.0) {
        return Err("输入验证失败");
    }
    let pay_currency = wallet_normalize_optional_string_field(payload.pay_currency, 3)?;
    if matches!(pay_currency.as_deref(), Some(value) if value.chars().count() != 3) {
        return Err("输入验证失败");
    }

    Ok(NormalizedWalletCreateRechargeRequest {
        amount_usd: payload.amount_usd,
        payment_method,
        pay_amount: payload.pay_amount,
        pay_currency,
        exchange_rate: payload.exchange_rate,
    })
}

fn wallet_default_refund_mode_for_payment_method(payment_method: &str) -> &'static str {
    if matches!(
        payment_method,
        "admin_manual" | "card_recharge" | "card_code" | "gift_code"
    ) {
        return "offline_payout";
    }
    "original_channel"
}

fn wallet_build_refund_no(now: chrono::DateTime<chrono::Utc>) -> String {
    format!(
        "rf_{}_{}",
        now.format("%Y%m%d%H%M%S%6f"),
        &Uuid::new_v4().simple().to_string()[..8]
    )
}

fn wallet_build_order_no(now: chrono::DateTime<chrono::Utc>) -> String {
    format!(
        "po_{}_{}",
        now.format("%Y%m%d%H%M%S%6f"),
        &Uuid::new_v4().simple().to_string()[..12]
    )
}

fn wallet_checkout_payload(
    payment_method: &str,
    order_no: &str,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> Result<(String, serde_json::Value), String> {
    let expires_at = expires_at.to_rfc3339();
    match payment_method {
        "alipay" => {
            let gateway_order_id = format!("ali_{order_no}");
            Ok((
                gateway_order_id.clone(),
                json!({
                    "gateway": "alipay",
                    "display_name": "支付宝",
                    "gateway_order_id": gateway_order_id,
                    "payment_url": format!("/pay/mock/alipay/{order_no}"),
                    "qr_code": format!("mock://alipay/{order_no}"),
                    "expires_at": expires_at,
                }),
            ))
        }
        "wechat" => {
            let gateway_order_id = format!("wx_{order_no}");
            Ok((
                gateway_order_id.clone(),
                json!({
                    "gateway": "wechat",
                    "display_name": "微信支付",
                    "gateway_order_id": gateway_order_id,
                    "payment_url": format!("/pay/mock/wechat/{order_no}"),
                    "qr_code": format!("mock://wechat/{order_no}"),
                    "expires_at": expires_at,
                }),
            ))
        }
        "manual" => {
            let gateway_order_id = format!("manual_{order_no}");
            Ok((
                gateway_order_id.clone(),
                json!({
                    "gateway": "manual",
                    "display_name": "人工打款",
                    "gateway_order_id": gateway_order_id,
                    "payment_url": serde_json::Value::Null,
                    "qr_code": serde_json::Value::Null,
                    "instructions": "请线下确认到账后由管理员处理",
                    "expires_at": expires_at,
                }),
            ))
        }
        _ => Err(format!("unsupported payment_method: {payment_method}")),
    }
}

fn build_wallet_payload(
    wallet: Option<&aether_data::repository::wallet::StoredWalletSnapshot>,
) -> serde_json::Value {
    let wallet_payload = build_auth_wallet_summary_payload(wallet);
    json!({
        "wallet": wallet_payload.clone(),
        "unlimited": wallet_payload.get("unlimited").cloned().unwrap_or(json!(false)),
        "limit_mode": wallet_payload
            .get("limit_mode")
            .cloned()
            .unwrap_or_else(|| json!("finite")),
        "balance": wallet_payload.get("balance").cloned().unwrap_or(json!(0.0)),
        "recharge_balance": wallet_payload
            .get("recharge_balance")
            .cloned()
            .unwrap_or(json!(0.0)),
        "gift_balance": wallet_payload
            .get("gift_balance")
            .cloned()
            .unwrap_or(json!(0.0)),
        "refundable_balance": wallet_payload
            .get("refundable_balance")
            .cloned()
            .unwrap_or(json!(0.0)),
        "currency": wallet_payload
            .get("currency")
            .cloned()
            .unwrap_or_else(|| json!("USD")),
    })
}

fn build_wallet_balance_payload(
    wallet: Option<&aether_data::repository::wallet::StoredWalletSnapshot>,
) -> serde_json::Value {
    let mut payload = build_wallet_payload(wallet);
    payload["pending_refund_count"] = json!(0);
    payload
}

fn parse_wallet_limit(query: Option<&str>) -> Result<usize, String> {
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

fn parse_wallet_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
        None => Ok(0),
    }
}

fn wallet_fixed_offset() -> chrono::FixedOffset {
    chrono::FixedOffset::east_opt(8 * 3600).expect("Asia/Shanghai offset should be valid")
}

fn wallet_today_billing_date_string() -> String {
    Utc::now()
        .with_timezone(&wallet_fixed_offset())
        .date_naive()
        .to_string()
}

fn build_wallet_daily_usage_payload(
    id: Option<String>,
    date: String,
    timezone: String,
    total_cost: f64,
    total_requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    first_finalized_at: Option<String>,
    last_finalized_at: Option<String>,
    aggregated_at: Option<String>,
    is_today: bool,
) -> serde_json::Value {
    json!({
        "id": id,
        "date": date,
        "timezone": timezone,
        "total_cost": round_to(total_cost, 6),
        "total_requests": total_requests,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "cache_read_tokens": cache_read_tokens,
        "first_finalized_at": first_finalized_at,
        "last_finalized_at": last_finalized_at,
        "aggregated_at": aggregated_at,
        "is_today": is_today,
    })
}

fn build_wallet_zero_today_entry() -> serde_json::Value {
    build_wallet_daily_usage_payload(
        None,
        wallet_today_billing_date_string(),
        WALLET_LEGACY_TIMEZONE.to_string(),
        0.0,
        0,
        0,
        0,
        0,
        0,
        None,
        None,
        Some(Utc::now().to_rfc3339()),
        true,
    )
}

fn wallet_refund_id_from_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/wallet/refunds/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn wallet_order_id_from_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/wallet/recharge/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn sanitize_wallet_gateway_response(value: Option<serde_json::Value>) -> serde_json::Value {
    let Some(value) = value else {
        return json!({});
    };
    let Some(object) = value.as_object() else {
        return json!({});
    };
    let mut sanitized = serde_json::Map::new();
    for key in WALLET_SAFE_GATEWAY_RESPONSE_KEYS {
        if let Some(item) = object.get(*key) {
            sanitized.insert((*key).to_string(), item.clone());
        }
    }
    serde_json::Value::Object(sanitized)
}

fn build_wallet_payment_order_payload(
    id: String,
    order_no: String,
    wallet_id: String,
    user_id: Option<String>,
    amount_usd: f64,
    pay_amount: Option<f64>,
    pay_currency: Option<String>,
    exchange_rate: Option<f64>,
    refunded_amount_usd: f64,
    refundable_amount_usd: f64,
    payment_method: String,
    gateway_order_id: Option<String>,
    gateway_response: Option<serde_json::Value>,
    status: String,
    created_at: Option<String>,
    paid_at: Option<String>,
    credited_at: Option<String>,
    expires_at: Option<String>,
) -> serde_json::Value {
    json!({
        "id": id,
        "order_no": order_no,
        "wallet_id": wallet_id,
        "user_id": user_id,
        "amount_usd": amount_usd,
        "pay_amount": pay_amount,
        "pay_currency": pay_currency,
        "exchange_rate": exchange_rate,
        "refunded_amount_usd": refunded_amount_usd,
        "refundable_amount_usd": refundable_amount_usd,
        "payment_method": payment_method,
        "gateway_order_id": gateway_order_id,
        "gateway_response": sanitize_wallet_gateway_response(gateway_response),
        "status": status,
        "created_at": created_at,
        "paid_at": paid_at,
        "credited_at": credited_at,
        "expires_at": expires_at,
    })
}

fn wallet_payment_order_payload_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<serde_json::Value, GatewayError> {
    let created_at = row
        .try_get::<Option<i64>, _>("created_at_unix_secs")
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339);
    let paid_at = row
        .try_get::<Option<i64>, _>("paid_at_unix_secs")
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339);
    let credited_at = row
        .try_get::<Option<i64>, _>("credited_at_unix_secs")
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339);
    let expires_at = row
        .try_get::<Option<i64>, _>("expires_at_unix_secs")
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339);
    Ok(build_wallet_payment_order_payload(
        row.try_get::<String, _>("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<String, _>("order_no")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<String, _>("wallet_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<String>, _>("user_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<f64, _>("amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<f64>, _>("pay_amount")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<String>, _>("pay_currency")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<f64>, _>("exchange_rate")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<f64, _>("refunded_amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<f64, _>("refundable_amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<String, _>("payment_method")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<String>, _>("gateway_order_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<Option<serde_json::Value>, _>("gateway_response")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get::<String, _>("effective_status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        created_at,
        paid_at,
        credited_at,
        expires_at,
    ))
}

fn wallet_transaction_payload_from_row(row: &sqlx::postgres::PgRow) -> Result<serde_json::Value, GatewayError> {
    let created_at = row
        .try_get::<Option<i64>, _>("created_at_unix_secs")
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .and_then(|value| u64::try_from(value).ok())
        .and_then(unix_secs_to_rfc3339);
    Ok(json!({
        "id": row.try_get::<String, _>("id").map_err(|err| GatewayError::Internal(err.to_string()))?,
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
        "description": row.try_get::<Option<String>, _>("description").map_err(|err| GatewayError::Internal(err.to_string()))?,
        "created_at": created_at,
    }))
}

fn wallet_refund_payload_from_row(row: &sqlx::postgres::PgRow) -> Result<serde_json::Value, GatewayError> {
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
        "created_at": created_at,
        "updated_at": updated_at,
        "processed_at": processed_at,
        "completed_at": completed_at,
    }))
}

#[cfg(test)]
fn wallet_test_refunds_for_wallet(wallet_id: &str) -> Vec<serde_json::Value> {
    let mut items = wallet_test_refund_store()
        .lock()
        .expect("wallet test refund store should lock")
        .iter()
        .filter(|entry| entry.wallet_id == wallet_id)
        .map(|entry| entry.payload.clone())
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right["created_at"]
            .as_str()
            .cmp(&left["created_at"].as_str())
    });
    items
}

#[cfg(test)]
fn wallet_test_refund_by_id(wallet_id: &str, refund_id: &str) -> Option<serde_json::Value> {
    wallet_test_refund_store()
        .lock()
        .expect("wallet test refund store should lock")
        .iter()
        .find(|entry| {
            entry.wallet_id == wallet_id && entry.payload["id"].as_str() == Some(refund_id)
        })
        .map(|entry| entry.payload.clone())
}

#[cfg(test)]
fn wallet_test_refund_by_idempotency(
    user_id: &str,
    idempotency_key: &str,
) -> Option<serde_json::Value> {
    wallet_test_refund_store()
        .lock()
        .expect("wallet test refund store should lock")
        .iter()
        .find(|entry| {
            entry.user_id == user_id && entry.idempotency_key.as_deref() == Some(idempotency_key)
        })
        .map(|entry| entry.payload.clone())
}

#[cfg(test)]
fn wallet_test_reserved_refund_amount(wallet_id: &str) -> f64 {
    wallet_test_refund_store()
        .lock()
        .expect("wallet test refund store should lock")
        .iter()
        .filter(|entry| {
            entry.wallet_id == wallet_id
                && matches!(
                    entry.payload["status"].as_str(),
                    Some("pending_approval" | "approved")
                )
        })
        .map(|entry| entry.payload["amount_usd"].as_f64().unwrap_or_default())
        .sum::<f64>()
}

#[cfg(test)]
fn wallet_test_recharge_orders_for_user(
    user_id: &str,
    limit: usize,
    offset: usize,
) -> (Vec<serde_json::Value>, u64) {
    let mut items = wallet_test_recharge_store()
        .lock()
        .expect("wallet test recharge store should lock")
        .iter()
        .filter(|entry| entry.user_id == user_id)
        .map(|entry| entry.payload.clone())
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right["created_at"]
            .as_str()
            .cmp(&left["created_at"].as_str())
    });
    let total = items.len() as u64;
    let items = items.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
    (items, total)
}

#[cfg(test)]
fn wallet_test_recharge_order_by_id(
    user_id: &str,
    order_id: &str,
) -> Option<serde_json::Value> {
    wallet_test_recharge_store()
        .lock()
        .expect("wallet test recharge store should lock")
        .iter()
        .find(|entry| {
            entry.user_id == user_id && entry.payload["id"].as_str() == Some(order_id)
        })
        .map(|entry| entry.payload.clone())
}

fn wallet_flow_sort_key(
    item_type: &str,
    payload: &serde_json::Value,
) -> (String, u8, String) {
    match item_type {
        "daily_usage" => {
            let data = payload.get("data").unwrap_or(payload);
            let date = data
                .get("date")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let sort_dt = data
                .get("last_finalized_at")
                .and_then(serde_json::Value::as_str)
                .or_else(|| data.get("aggregated_at").and_then(serde_json::Value::as_str))
                .unwrap_or("");
            (date.to_string(), 1, sort_dt.to_string())
        }
        _ => {
            let data = payload.get("data").unwrap_or(payload);
            let created_at = data
                .get("created_at")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let local_date = chrono::DateTime::parse_from_rfc3339(created_at)
                .ok()
                .map(|value| value.with_timezone(&wallet_fixed_offset()).date_naive().to_string())
                .unwrap_or_default();
            (local_date, 0, created_at.to_string())
        }
    }
}

async fn handle_wallet_balance(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let wallet = state
        .read_wallet_snapshot_for_auth(&auth.user.id, "", false)
        .await
        .ok()
        .flatten();
    build_auth_json_response(
        http::StatusCode::OK,
        build_wallet_balance_payload(wallet.as_ref()),
        None,
    )
}

async fn handle_wallet_today_cost(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_usage_data_reader() {
        return build_public_support_maintenance_response("Wallet routes require Rust maintenance backend");
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let today = Utc::now().date_naive();
    let Some(start_of_day) = today.and_hms_opt(0, 0, 0) else {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "wallet today start is invalid",
            false,
        );
    };
    let start_unix_secs = u64::try_from(
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(start_of_day, chrono::Utc)
            .timestamp(),
    )
    .unwrap_or_default();
    let end_unix_secs = start_unix_secs.saturating_add(24 * 3600);

    let items = match state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(start_unix_secs),
            created_until_unix_secs: Some(end_unix_secs),
            user_id: Some(auth.user.id.clone()),
            provider_name: None,
            model: None,
        })
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet today cost lookup failed: {err:?}"),
                false,
            )
        }
    };

    let settled = items
        .into_iter()
        .filter(|item| item.billing_status == "settled" && item.total_cost_usd > 0.0)
        .collect::<Vec<_>>();
    let total_cost = settled.iter().map(|item| item.total_cost_usd).sum::<f64>();
    let total_requests = settled.len() as u64;
    let input_tokens = settled.iter().map(|item| item.input_tokens).sum::<u64>();
    let output_tokens = settled.iter().map(|item| item.output_tokens).sum::<u64>();
    let cache_creation_tokens = settled
        .iter()
        .map(|item| item.cache_creation_input_tokens)
        .sum::<u64>();
    let cache_read_tokens = settled
        .iter()
        .map(|item| item.cache_read_input_tokens)
        .sum::<u64>();
    let first_finalized_at = settled
        .iter()
        .filter_map(|item| item.finalized_at_unix_secs)
        .min()
        .and_then(unix_secs_to_rfc3339);
    let last_finalized_at = settled
        .iter()
        .filter_map(|item| item.finalized_at_unix_secs)
        .max()
        .and_then(unix_secs_to_rfc3339);

    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "id": serde_json::Value::Null,
            "date": today.to_string(),
            "timezone": "UTC",
            "total_cost": round_to(total_cost, 6),
            "total_requests": total_requests,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_tokens": cache_creation_tokens,
            "cache_read_tokens": cache_read_tokens,
            "first_finalized_at": first_finalized_at,
            "last_finalized_at": last_finalized_at,
            "aggregated_at": Utc::now().to_rfc3339(),
            "is_today": true,
        }),
        None,
    )
}

async fn handle_wallet_transactions(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_wallet_limit(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let offset = match parse_wallet_offset(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(wallet) = wallet else {
        return build_auth_json_response(
            http::StatusCode::OK,
            json!({
                "items": [],
                "total": 0,
                "limit": limit,
                "offset": offset,
            })
            .as_object()
            .cloned()
            .map(|mut value| {
                if let Some(wallet_payload) = build_wallet_payload(None).as_object() {
                    value.extend(wallet_payload.clone());
                }
                serde_json::Value::Object(value)
            })
            .unwrap_or_else(|| json!({})),
            None,
        );
    };

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = match sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallet_transactions
WHERE wallet_id = $1
            "#,
        )
        .bind(&wallet.id)
        .fetch_one(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet transaction count failed: {err}"),
                    false,
                )
            }
        };
        total = count_row
            .try_get::<i64, _>("total")
            .ok()
            .unwrap_or_default()
            .max(0) as u64;
        let rows = match sqlx::query(
            r#"
SELECT
  id,
  category,
  reason_code,
  CAST(amount AS DOUBLE PRECISION) AS amount,
  CAST(balance_before AS DOUBLE PRECISION) AS balance_before,
  CAST(balance_after AS DOUBLE PRECISION) AS balance_after,
  CAST(recharge_balance_before AS DOUBLE PRECISION) AS recharge_balance_before,
  CAST(recharge_balance_after AS DOUBLE PRECISION) AS recharge_balance_after,
  CAST(gift_balance_before AS DOUBLE PRECISION) AS gift_balance_before,
  CAST(gift_balance_after AS DOUBLE PRECISION) AS gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs
FROM wallet_transactions
WHERE wallet_id = $1
ORDER BY created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(&wallet.id)
        .bind(i64::try_from(offset).ok().unwrap_or_default())
        .bind(i64::try_from(limit).ok().unwrap_or_default())
        .fetch_all(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet transaction query failed: {err}"),
                    false,
                )
            }
        };
        items = match rows
            .iter()
            .map(wallet_transaction_payload_from_row)
            .collect::<Result<Vec<_>, GatewayError>>()
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet transaction payload failed: {err:?}"),
                    false,
                )
            }
        };
    }
    let mut payload = json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(wallet_payload) = build_wallet_payload(Some(&wallet)).as_object() {
            object.extend(wallet_payload.clone());
        }
    }
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_refunds_list(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_wallet_limit(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let offset = match parse_wallet_offset(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(wallet) = wallet else {
        let mut payload = json!({
            "items": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
        });
        if let Some(object) = payload.as_object_mut() {
            if let Some(wallet_payload) = build_wallet_payload(None).as_object() {
                object.extend(wallet_payload.clone());
            }
        }
        return build_auth_json_response(http::StatusCode::OK, payload, None);
    };

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = match sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM refund_requests
WHERE wallet_id = $1
            "#,
        )
        .bind(&wallet.id)
        .fetch_one(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet refund count failed: {err}"),
                    false,
                )
            }
        };
        total = count_row
            .try_get::<i64, _>("total")
            .ok()
            .unwrap_or_default()
            .max(0) as u64;
        let rows = match sqlx::query(
            r#"
SELECT
  id,
  refund_no,
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
        .bind(i64::try_from(offset).ok().unwrap_or_default())
        .bind(i64::try_from(limit).ok().unwrap_or_default())
        .fetch_all(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet refund query failed: {err}"),
                    false,
                )
            }
        };
        items = match rows
            .iter()
            .map(wallet_refund_payload_from_row)
            .collect::<Result<Vec<_>, GatewayError>>()
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet refund payload failed: {err:?}"),
                    false,
                )
            }
        };
    }
    #[cfg(test)]
    if state.postgres_pool().is_none() {
        let all_items = wallet_test_refunds_for_wallet(&wallet.id);
        total = all_items.len() as u64;
        items = all_items.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
    }

    let mut payload = json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(wallet_payload) = build_wallet_payload(Some(&wallet)).as_object() {
            object.extend(wallet_payload.clone());
        }
    }
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_refund_detail(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(refund_id) = wallet_refund_id_from_path(&request_context.request_path) else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Refund request not found", false);
    };
    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(wallet) = wallet else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Refund request not found", false);
    };
    let Some(pool) = state.postgres_pool() else {
        #[cfg(test)]
        if let Some(payload) = wallet_test_refund_by_id(&wallet.id, &refund_id) {
            return build_auth_json_response(http::StatusCode::OK, payload, None);
        }
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Refund request not found", false);
    };
    let row = match sqlx::query(
        r#"
SELECT
  id,
  refund_no,
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
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests
WHERE wallet_id = $1 AND id = $2
LIMIT 1
        "#,
    )
    .bind(&wallet.id)
    .bind(&refund_id)
    .fetch_optional(&pool)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund detail query failed: {err}"),
                false,
            )
        }
    };
    let Some(row) = row else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Refund request not found", false);
    };
    let payload = match wallet_refund_payload_from_row(&row) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund detail payload failed: {err:?}"),
                false,
            )
        }
    };
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_create_refund(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let payload = match serde_json::from_slice::<WalletCreateRefundRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false),
    };
    let payload = match normalize_wallet_create_refund_request(payload) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
        }
    };

    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(wallet) = wallet else {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "当前账户尚未开通钱包，无法申请退款",
            false,
        );
    };

    let Some(pool) = state.postgres_pool() else {
        #[cfg(test)]
        {
            if let Some(idempotency_key) = payload.idempotency_key.as_deref() {
                if let Some(existing) = wallet_test_refund_by_idempotency(&auth.user.id, idempotency_key)
                {
                    return build_auth_json_response(http::StatusCode::OK, existing, None);
                }
            }
            let reserved_amount = wallet_test_reserved_refund_amount(&wallet.id);
            if payload.amount_usd > (wallet.balance - reserved_amount) {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "refund amount exceeds available refundable recharge balance",
                    false,
                );
            }
            let now = Utc::now();
            let created = json!({
                "id": Uuid::new_v4().to_string(),
                "refund_no": wallet_build_refund_no(now),
                "payment_order_id": serde_json::Value::Null,
                "source_type": payload.source_type.as_deref().unwrap_or("wallet_balance"),
                "source_id": payload.source_id,
                "refund_mode": payload.refund_mode.as_deref().unwrap_or("offline_payout"),
                "amount_usd": payload.amount_usd,
                "status": "pending_approval",
                "reason": payload.reason,
                "failure_reason": serde_json::Value::Null,
                "gateway_refund_id": serde_json::Value::Null,
                "payout_method": serde_json::Value::Null,
                "payout_reference": serde_json::Value::Null,
                "payout_proof": serde_json::Value::Null,
                "created_at": now.to_rfc3339(),
                "updated_at": now.to_rfc3339(),
                "processed_at": serde_json::Value::Null,
                "completed_at": serde_json::Value::Null,
            });
            wallet_test_refund_store()
                .lock()
                .expect("wallet test refund store should lock")
                .push(WalletTestRefundRecord {
                    wallet_id: wallet.id,
                    user_id: auth.user.id,
                    idempotency_key: payload.idempotency_key,
                    payload: created.clone(),
                });
            return build_auth_json_response(http::StatusCode::OK, created, None);
        }
        #[cfg(not(test))]
        return build_public_support_maintenance_response(
            "Wallet refund routes require Rust maintenance backend",
        );
    };

    let mut tx = match pool.begin().await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund transaction failed: {err}"),
                false,
            )
        }
    };

    let locked_wallet_row = match sqlx::query(
        r#"
SELECT
  id,
  CAST(balance AS DOUBLE PRECISION) AS balance
FROM wallets
WHERE id = $1
LIMIT 1
FOR UPDATE
        "#,
    )
    .bind(&wallet.id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund wallet lock failed: {err}"),
                false,
            );
        }
    };
    let Some(locked_wallet_row) = locked_wallet_row else {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "当前账户尚未开通钱包，无法申请退款",
            false,
        );
    };
    let wallet_recharge_balance = locked_wallet_row
        .try_get::<f64, _>("balance")
        .ok()
        .unwrap_or_default();
    let wallet_reserved_row = match sqlx::query(
        r#"
SELECT COALESCE(CAST(SUM(amount_usd) AS DOUBLE PRECISION), 0) AS total
FROM refund_requests
WHERE wallet_id = $1
  AND status IN ('pending_approval', 'approved')
        "#,
    )
    .bind(&wallet.id)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund reserved amount lookup failed: {err}"),
                false,
            );
        }
    };
    let wallet_reserved_amount = wallet_reserved_row
        .try_get::<f64, _>("total")
        .ok()
        .unwrap_or_default();
    if payload.amount_usd > (wallet_recharge_balance - wallet_reserved_amount) {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "refund amount exceeds available refundable recharge balance",
            false,
        );
    }

    let mut payment_order_id = None;
    let mut source_type = payload
        .source_type
        .clone()
        .unwrap_or_else(|| "wallet_balance".to_string());
    let mut source_id = payload.source_id.clone();
    let mut refund_mode = payload
        .refund_mode
        .clone()
        .unwrap_or_else(|| "offline_payout".to_string());
    if let Some(order_id) = payload.payment_order_id.as_deref() {
        let order_row = match sqlx::query(
            r#"
SELECT
  id,
  wallet_id,
  status,
  payment_method,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd
FROM payment_orders
WHERE id = $1
  AND wallet_id = $2
LIMIT 1
FOR UPDATE
            "#,
        )
        .bind(order_id)
        .bind(&wallet.id)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet refund payment order lookup failed: {err}"),
                    false,
                );
            }
        };
        let Some(order_row) = order_row else {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::NOT_FOUND,
                "Payment order not found",
                false,
            );
        };
        let status = order_row
            .try_get::<String, _>("status")
            .ok()
            .unwrap_or_default();
        if status != "credited" {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "payment order is not refundable",
                false,
            );
        }

        let order_reserved_row = match sqlx::query(
            r#"
SELECT COALESCE(CAST(SUM(amount_usd) AS DOUBLE PRECISION), 0) AS total
FROM refund_requests
WHERE payment_order_id = $1
  AND status IN ('pending_approval', 'approved')
            "#,
        )
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet refund payment order reserve lookup failed: {err}"),
                    false,
                );
            }
        };
        let refundable_amount = order_row
            .try_get::<f64, _>("refundable_amount_usd")
            .ok()
            .unwrap_or_default();
        let reserved_amount = order_reserved_row
            .try_get::<f64, _>("total")
            .ok()
            .unwrap_or_default();
        if payload.amount_usd > (refundable_amount - reserved_amount) {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "refund amount exceeds available refundable amount",
                false,
            );
        }

        payment_order_id = Some(order_id.to_string());
        source_type = "payment_order".to_string();
        source_id = Some(order_id.to_string());
        if payload.refund_mode.is_none() {
            let payment_method = order_row
                .try_get::<String, _>("payment_method")
                .ok()
                .unwrap_or_default();
            refund_mode = wallet_default_refund_mode_for_payment_method(&payment_method).to_string();
        }
    }

    let now = Utc::now();
    let refund_id = Uuid::new_v4().to_string();
    let refund_no = wallet_build_refund_no(now);
    let insert_result = sqlx::query(
        r#"
INSERT INTO refund_requests (
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  amount_usd,
  status,
  reason,
  requested_by,
  idempotency_key,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'pending_approval',
  $10,
  $11,
  $12,
  NOW(),
  NOW()
)
RETURNING
  id,
  refund_no,
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
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
        "#,
    )
    .bind(&refund_id)
    .bind(&refund_no)
    .bind(&wallet.id)
    .bind(&auth.user.id)
    .bind(payment_order_id.as_deref())
    .bind(&source_type)
    .bind(source_id.as_deref())
    .bind(&refund_mode)
    .bind(payload.amount_usd)
    .bind(payload.reason.as_deref())
    .bind(&auth.user.id)
    .bind(payload.idempotency_key.as_deref())
    .fetch_one(&mut *tx)
    .await;

    let row = match insert_result {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            if let Some(idempotency_key) = payload.idempotency_key.as_deref() {
                let existing = match sqlx::query(
                    r#"
SELECT
  id,
  refund_no,
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
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests
WHERE user_id = $1
  AND idempotency_key = $2
LIMIT 1
                    "#,
                )
                .bind(&auth.user.id)
                .bind(idempotency_key)
                .fetch_optional(&pool)
                .await
                {
                    Ok(value) => value,
                    Err(read_err) => {
                        return build_auth_error_response(
                            http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("wallet refund idempotency lookup failed: {read_err}"),
                            false,
                        );
                    }
                };
                if let Some(existing) = existing {
                    let payload = match wallet_refund_payload_from_row(&existing) {
                        Ok(value) => value,
                        Err(payload_err) => {
                            return build_auth_error_response(
                                http::StatusCode::INTERNAL_SERVER_ERROR,
                                format!("wallet refund payload failed: {payload_err:?}"),
                                false,
                            );
                        }
                    };
                    return build_auth_json_response(http::StatusCode::OK, payload, None);
                }
            }
            if err
                .as_database_error()
                .and_then(|value| value.code())
                .as_deref()
                == Some("23505")
            {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "退款申请重复，请勿重复提交",
                    false,
                );
            }
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund create failed: {err}"),
                false,
            );
        }
    };

    if let Err(err) = tx.commit().await {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("wallet refund commit failed: {err}"),
            false,
        );
    }

    let payload = match wallet_refund_payload_from_row(&row) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet refund payload failed: {err:?}"),
                false,
            );
        }
    };
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_flow(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_wallet_limit(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let offset = match parse_wallet_offset(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(wallet) = wallet else {
        let mut payload = json!({
            "today_entry": serde_json::Value::Null,
            "items": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
        });
        if let Some(object) = payload.as_object_mut() {
            if let Some(wallet_payload) = build_wallet_payload(None).as_object() {
                object.extend(wallet_payload.clone());
            }
        }
        return build_auth_json_response(http::StatusCode::OK, payload, None);
    };

    let mut today_entry = build_wallet_zero_today_entry();
    let mut items = Vec::new();
    let mut total = 0_u64;
    if let Some(pool) = state.postgres_pool() {
        let today_row = sqlx::query(
            r#"
SELECT
  id,
  billing_date::text AS billing_date,
  billing_timezone,
  CAST(total_cost_usd AS DOUBLE PRECISION) AS total_cost_usd,
  total_requests,
  input_tokens,
  output_tokens,
  cache_creation_tokens,
  cache_read_tokens,
  CAST(EXTRACT(EPOCH FROM first_finalized_at) AS BIGINT) AS first_finalized_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM last_finalized_at) AS BIGINT) AS last_finalized_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM aggregated_at) AS BIGINT) AS aggregated_at_unix_secs
FROM wallet_daily_usage_ledgers
WHERE wallet_id = $1
  AND billing_timezone = $2
  AND billing_date = (timezone($2, now()))::date
LIMIT 1
            "#,
        )
        .bind(&wallet.id)
        .bind(WALLET_LEGACY_TIMEZONE)
        .fetch_optional(&pool)
        .await;
        if let Ok(Some(row)) = today_row {
            today_entry = build_wallet_daily_usage_payload(
                row.try_get::<Option<String>, _>("id").ok().flatten(),
                row.try_get::<String, _>("billing_date").ok().unwrap_or_else(wallet_today_billing_date_string),
                row.try_get::<String, _>("billing_timezone").ok().unwrap_or_else(|| WALLET_LEGACY_TIMEZONE.to_string()),
                row.try_get::<f64, _>("total_cost_usd").ok().unwrap_or_default(),
                row.try_get::<i64, _>("total_requests").ok().unwrap_or_default().max(0) as u64,
                row.try_get::<i64, _>("input_tokens").ok().unwrap_or_default().max(0) as u64,
                row.try_get::<i64, _>("output_tokens").ok().unwrap_or_default().max(0) as u64,
                row.try_get::<i64, _>("cache_creation_tokens").ok().unwrap_or_default().max(0) as u64,
                row.try_get::<i64, _>("cache_read_tokens").ok().unwrap_or_default().max(0) as u64,
                row.try_get::<Option<i64>, _>("first_finalized_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                row.try_get::<Option<i64>, _>("last_finalized_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                row.try_get::<Option<i64>, _>("aggregated_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                true,
            );
        }

        let fetch_size = offset.saturating_add(limit).min(5200);
        let tx_count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallet_transactions
WHERE wallet_id = $1
            "#,
        )
        .bind(&wallet.id)
        .fetch_one(&pool)
        .await;
        let tx_total = tx_count_row
            .ok()
            .and_then(|row| row.try_get::<i64, _>("total").ok())
            .unwrap_or_default()
            .max(0) as u64;
        let tx_rows = sqlx::query(
            r#"
SELECT
  id,
  category,
  reason_code,
  CAST(amount AS DOUBLE PRECISION) AS amount,
  CAST(balance_before AS DOUBLE PRECISION) AS balance_before,
  CAST(balance_after AS DOUBLE PRECISION) AS balance_after,
  CAST(recharge_balance_before AS DOUBLE PRECISION) AS recharge_balance_before,
  CAST(recharge_balance_after AS DOUBLE PRECISION) AS recharge_balance_after,
  CAST(gift_balance_before AS DOUBLE PRECISION) AS gift_balance_before,
  CAST(gift_balance_after AS DOUBLE PRECISION) AS gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs
FROM wallet_transactions
WHERE wallet_id = $1
ORDER BY created_at DESC
LIMIT $2
            "#,
        )
        .bind(&wallet.id)
        .bind(i64::try_from(fetch_size).ok().unwrap_or(50))
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        let daily_count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM wallet_daily_usage_ledgers
WHERE wallet_id = $1
  AND billing_timezone = $2
  AND billing_date < (timezone($2, now()))::date
            "#,
        )
        .bind(&wallet.id)
        .bind(WALLET_LEGACY_TIMEZONE)
        .fetch_one(&pool)
        .await;
        let daily_total = daily_count_row
            .ok()
            .and_then(|row| row.try_get::<i64, _>("total").ok())
            .unwrap_or_default()
            .max(0) as u64;
        let daily_rows = sqlx::query(
            r#"
SELECT
  id,
  billing_date::text AS billing_date,
  billing_timezone,
  CAST(total_cost_usd AS DOUBLE PRECISION) AS total_cost_usd,
  total_requests,
  input_tokens,
  output_tokens,
  cache_creation_tokens,
  cache_read_tokens,
  CAST(EXTRACT(EPOCH FROM first_finalized_at) AS BIGINT) AS first_finalized_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM last_finalized_at) AS BIGINT) AS last_finalized_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM aggregated_at) AS BIGINT) AS aggregated_at_unix_secs
FROM wallet_daily_usage_ledgers
WHERE wallet_id = $1
  AND billing_timezone = $2
  AND billing_date < (timezone($2, now()))::date
ORDER BY billing_date DESC
LIMIT $3
            "#,
        )
        .bind(&wallet.id)
        .bind(WALLET_LEGACY_TIMEZONE)
        .bind(i64::try_from(fetch_size).ok().unwrap_or(50))
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut merged = tx_rows
            .iter()
            .filter_map(|row| wallet_transaction_payload_from_row(row).ok())
            .map(|data| json!({ "type": "transaction", "data": data }))
            .collect::<Vec<_>>();
        merged.extend(daily_rows.iter().map(|row| {
            json!({
                "type": "daily_usage",
                "data": build_wallet_daily_usage_payload(
                    row.try_get::<Option<String>, _>("id").ok().flatten(),
                    row.try_get::<String, _>("billing_date").ok().unwrap_or_default(),
                    row.try_get::<String, _>("billing_timezone").ok().unwrap_or_else(|| WALLET_LEGACY_TIMEZONE.to_string()),
                    row.try_get::<f64, _>("total_cost_usd").ok().unwrap_or_default(),
                    row.try_get::<i64, _>("total_requests").ok().unwrap_or_default().max(0) as u64,
                    row.try_get::<i64, _>("input_tokens").ok().unwrap_or_default().max(0) as u64,
                    row.try_get::<i64, _>("output_tokens").ok().unwrap_or_default().max(0) as u64,
                    row.try_get::<i64, _>("cache_creation_tokens").ok().unwrap_or_default().max(0) as u64,
                    row.try_get::<i64, _>("cache_read_tokens").ok().unwrap_or_default().max(0) as u64,
                    row.try_get::<Option<i64>, _>("first_finalized_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                    row.try_get::<Option<i64>, _>("last_finalized_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                    row.try_get::<Option<i64>, _>("aggregated_at_unix_secs").ok().flatten().and_then(|value| u64::try_from(value).ok()).and_then(unix_secs_to_rfc3339),
                    false,
                )
            })
        }));
        merged.sort_by(|left, right| {
            let left_type = left.get("type").and_then(serde_json::Value::as_str).unwrap_or("");
            let right_type = right.get("type").and_then(serde_json::Value::as_str).unwrap_or("");
            wallet_flow_sort_key(right_type, right).cmp(&wallet_flow_sort_key(left_type, left))
        });
        items = merged.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
        total = tx_total.saturating_add(daily_total);
    }

    let mut payload = json!({
        "today_entry": today_entry,
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(wallet_payload) = build_wallet_payload(Some(&wallet)).as_object() {
            object.extend(wallet_payload.clone());
        }
    }
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_create_recharge(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let payload = match serde_json::from_slice::<WalletCreateRechargeRequest>(request_body) {
        Ok(value) => value,
        Err(_) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false),
    };
    let payload = match normalize_wallet_create_recharge_request(payload) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if payload.payment_method == "admin_manual" {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "admin_manual is reserved for admin recharge",
            false,
        );
    }

    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };

    let Some(pool) = state.postgres_pool() else {
        #[cfg(test)]
        {
            let Some(wallet) = wallet else {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "wallet not available",
                    false,
                );
            };
            if wallet.status != "active" {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "wallet is not active",
                    false,
                );
            }
            let now = Utc::now();
            let order_id = Uuid::new_v4().to_string();
            let order_no = wallet_build_order_no(now);
            let expires_at = now + chrono::Duration::minutes(30);
            let (gateway_order_id, gateway_response) =
                match wallet_checkout_payload(&payload.payment_method, &order_no, expires_at) {
                    Ok(value) => value,
                    Err(detail) => {
                        return build_auth_error_response(
                            http::StatusCode::BAD_REQUEST,
                            detail,
                            false,
                        );
                    }
                };
            let order_payload = build_wallet_payment_order_payload(
                order_id,
                order_no,
                wallet.id.clone(),
                Some(auth.user.id.clone()),
                payload.amount_usd,
                payload.pay_amount,
                payload.pay_currency.clone(),
                payload.exchange_rate,
                0.0,
                0.0,
                payload.payment_method,
                Some(gateway_order_id),
                Some(gateway_response.clone()),
                "pending".to_string(),
                Some(now.to_rfc3339()),
                None,
                None,
                Some(expires_at.to_rfc3339()),
            );
            wallet_test_recharge_store()
                .lock()
                .expect("wallet test recharge store should lock")
                .push(WalletTestRechargeRecord {
                    user_id: auth.user.id,
                    payload: order_payload.clone(),
                });
            return build_auth_json_response(
                http::StatusCode::OK,
                json!({
                    "order": order_payload,
                    "payment_instructions": sanitize_wallet_gateway_response(Some(gateway_response)),
                }),
                None,
            );
        }
        #[cfg(not(test))]
        return build_public_support_maintenance_response(
            "Wallet routes require Rust maintenance backend",
        );
    };

    let mut tx = match pool.begin().await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge transaction failed: {err}"),
                false,
            )
        }
    };

    let wallet_row = match sqlx::query(
        r#"
SELECT id, status
FROM wallets
WHERE user_id = $1
LIMIT 1
FOR UPDATE
        "#,
    )
    .bind(&auth.user.id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(Some(value)) => Some(value),
        Ok(None) => {
            let wallet_id = wallet
                .as_ref()
                .map(|value| value.id.clone())
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            match sqlx::query(
                r#"
INSERT INTO wallets (
  id,
  user_id,
  balance,
  gift_balance,
  limit_mode,
  currency,
  status,
  total_recharged,
  total_consumed,
  total_refunded,
  total_adjusted,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  0,
  0,
  'finite',
  'USD',
  'active',
  0,
  0,
  0,
  0,
  NOW(),
  NOW()
)
ON CONFLICT (user_id) DO UPDATE
SET updated_at = wallets.updated_at
RETURNING id, status
                "#,
            )
            .bind(&wallet_id)
            .bind(&auth.user.id)
            .fetch_one(&mut *tx)
            .await
            {
                Ok(value) => Some(value),
                Err(err) => {
                    let _ = tx.rollback().await;
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("wallet recharge wallet bootstrap failed: {err}"),
                        false,
                    );
                }
            }
        }
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge wallet lookup failed: {err}"),
                false,
            );
        }
    };
    let Some(wallet_row) = wallet_row else {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "wallet not available",
            false,
        );
    };
    let wallet_id = wallet_row
        .try_get::<String, _>("id")
        .ok()
        .unwrap_or_default();
    let wallet_status = wallet_row
        .try_get::<String, _>("status")
        .ok()
        .unwrap_or_default();
    if wallet_status != "active" {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "wallet is not active",
            false,
        );
    }

    let now = Utc::now();
    let order_id = Uuid::new_v4().to_string();
    let order_no = wallet_build_order_no(now);
    let expires_at = now + chrono::Duration::minutes(30);
    let (gateway_order_id, gateway_response) =
        match wallet_checkout_payload(&payload.payment_method, &order_no, expires_at) {
            Ok(value) => value,
            Err(detail) => {
                let _ = tx.rollback().await;
                return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
            }
        };

    let row = match sqlx::query(
        r#"
INSERT INTO payment_orders (
  id,
  order_no,
  wallet_id,
  user_id,
  amount_usd,
  pay_amount,
  pay_currency,
  exchange_rate,
  refunded_amount_usd,
  refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  created_at,
  expires_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  0,
  0,
  $9,
  $10,
  $11,
  'pending',
  NOW(),
  to_timestamp($12)
)
RETURNING
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status AS effective_status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
        "#,
    )
    .bind(&order_id)
    .bind(&order_no)
    .bind(&wallet_id)
    .bind(&auth.user.id)
    .bind(payload.amount_usd)
    .bind(payload.pay_amount)
    .bind(payload.pay_currency.as_deref())
    .bind(payload.exchange_rate)
    .bind(&payload.payment_method)
    .bind(&gateway_order_id)
    .bind(&gateway_response)
    .bind(expires_at.timestamp())
    .fetch_one(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge create failed: {err}"),
                false,
            );
        }
    };

    if let Err(err) = tx.commit().await {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("wallet recharge commit failed: {err}"),
            false,
        );
    }

    let order_payload = match wallet_payment_order_payload_from_row(&row) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge payload failed: {err:?}"),
                false,
            );
        }
    };
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "order": order_payload,
            "payment_instructions": sanitize_wallet_gateway_response(Some(gateway_response)),
        }),
        None,
    )
}

async fn handle_wallet_recharge_list(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_wallet_limit(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let offset = match parse_wallet_offset(query) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let wallet = match state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet lookup failed: {err:?}"),
                false,
            )
        }
    };

    let mut total = 0_u64;
    let mut items = Vec::new();
    if let Some(pool) = state.postgres_pool() {
        let count_row = match sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM payment_orders
WHERE user_id = $1
            "#,
        )
        .bind(&auth.user.id)
        .fetch_one(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet recharge count failed: {err}"),
                    false,
                )
            }
        };
        total = count_row
            .try_get::<i64, _>("total")
            .ok()
            .unwrap_or_default()
            .max(0) as u64;
        let rows = match sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  CASE
    WHEN status = 'pending' AND expires_at IS NOT NULL AND expires_at < now() THEN 'expired'
    ELSE status
  END AS effective_status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE user_id = $1
ORDER BY created_at DESC
OFFSET $2
LIMIT $3
            "#,
        )
        .bind(&auth.user.id)
        .bind(i64::try_from(offset).ok().unwrap_or_default())
        .bind(i64::try_from(limit).ok().unwrap_or_default())
        .fetch_all(&pool)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet recharge query failed: {err}"),
                    false,
                )
            }
        };
        items = match rows
            .iter()
            .map(wallet_payment_order_payload_from_row)
            .collect::<Result<Vec<_>, GatewayError>>()
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("wallet recharge payload failed: {err:?}"),
                    false,
                )
            }
        };
    } else {
        #[cfg(test)]
        {
            let (test_items, test_total) =
                wallet_test_recharge_orders_for_user(&auth.user.id, limit, offset);
            items = test_items;
            total = test_total;
        }
    }

    let mut payload = json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    });
    if let Some(object) = payload.as_object_mut() {
        if let Some(wallet_payload) = build_wallet_payload(wallet.as_ref()).as_object() {
            object.extend(wallet_payload.clone());
        }
    }
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn handle_wallet_recharge_detail(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(order_id) = wallet_order_id_from_path(&request_context.request_path) else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Payment order not found", false);
    };
    let Some(pool) = state.postgres_pool() else {
        #[cfg(test)]
        {
            if let Some(order) = wallet_test_recharge_order_by_id(&auth.user.id, &order_id) {
                return build_auth_json_response(http::StatusCode::OK, json!({ "order": order }), None);
            }
        }
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Payment order not found", false);
    };
    let row = match sqlx::query(
        r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  CASE
    WHEN status = 'pending' AND expires_at IS NOT NULL AND expires_at < now() THEN 'expired'
    ELSE status
  END AS effective_status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE id = $1 AND user_id = $2
LIMIT 1
        "#,
    )
    .bind(&order_id)
    .bind(&auth.user.id)
    .fetch_optional(&pool)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge detail query failed: {err}"),
                false,
            )
        }
    };
    let Some(row) = row else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "Payment order not found", false);
    };
    let payload = match wallet_payment_order_payload_from_row(&row) {
        Ok(value) => json!({ "order": value }),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("wallet recharge detail payload failed: {err:?}"),
                false,
            )
        }
    };
    build_auth_json_response(http::StatusCode::OK, payload, None)
}

async fn maybe_build_local_wallet_legacy_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("wallet_legacy") {
        return None;
    }

    if decision.route_kind.as_deref() == Some("balance")
        && request_context.request_path == "/api/wallet/balance"
    {
        return Some(handle_wallet_balance(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("today_cost")
        && request_context.request_path == "/api/wallet/today-cost"
    {
        return Some(handle_wallet_today_cost(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("transactions")
        && request_context.request_path == "/api/wallet/transactions"
    {
        return Some(handle_wallet_transactions(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("flow")
        && request_context.request_path == "/api/wallet/flow"
    {
        return Some(handle_wallet_flow(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("list_refunds")
        && request_context.request_path == "/api/wallet/refunds"
    {
        return Some(handle_wallet_refunds_list(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("refund_detail")
        && request_context.request_path.starts_with("/api/wallet/refunds/")
    {
        return Some(handle_wallet_refund_detail(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("create_refund")
        && request_context.request_path == "/api/wallet/refunds"
    {
        return Some(handle_wallet_create_refund(state, request_context, headers, request_body).await);
    }

    if decision.route_kind.as_deref() == Some("create_recharge_order")
        && request_context.request_path == "/api/wallet/recharge"
    {
        return Some(
            handle_wallet_create_recharge(state, request_context, headers, request_body).await,
        );
    }

    if decision.route_kind.as_deref() == Some("list_recharge_orders")
        && request_context.request_path == "/api/wallet/recharge"
    {
        return Some(handle_wallet_recharge_list(state, request_context, headers).await);
    }

    if decision.route_kind.as_deref() == Some("recharge_detail")
        && request_context.request_path.starts_with("/api/wallet/recharge/")
    {
        return Some(handle_wallet_recharge_detail(state, request_context, headers).await);
    }

    None
}
