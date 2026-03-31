use hmac::{Hmac, Mac};

const PAYMENT_CALLBACK_TOKEN_HEADER: &str = "x-payment-callback-token";
const PAYMENT_CALLBACK_SIGNATURE_HEADER: &str = "x-payment-callback-signature";

#[derive(Debug, Deserialize)]
struct PaymentCallbackRequest {
    callback_key: String,
    #[serde(default)]
    order_no: Option<String>,
    #[serde(default)]
    gateway_order_id: Option<String>,
    amount_usd: f64,
    #[serde(default)]
    pay_amount: Option<f64>,
    #[serde(default)]
    pay_currency: Option<String>,
    #[serde(default)]
    exchange_rate: Option<f64>,
    #[serde(default)]
    payload: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone)]
struct NormalizedPaymentCallbackRequest {
    callback_key: String,
    order_no: Option<String>,
    gateway_order_id: Option<String>,
    amount_usd: f64,
    pay_amount: Option<f64>,
    pay_currency: Option<String>,
    exchange_rate: Option<f64>,
    payload: serde_json::Value,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct PaymentTestCallbackRecord {
    callback_key: String,
    payment_order_id: Option<String>,
    status: String,
}

#[cfg(test)]
fn payment_test_callback_store() -> &'static std::sync::Mutex<Vec<PaymentTestCallbackRecord>> {
    static STORE: std::sync::OnceLock<std::sync::Mutex<Vec<PaymentTestCallbackRecord>>> =
        std::sync::OnceLock::new();
    STORE.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn payment_callback_secret() -> Option<String> {
    std::env::var("PAYMENT_CALLBACK_SECRET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn payment_callback_payment_method_from_path(path: &str) -> Option<String> {
    let normalized = path.trim_end_matches('/');
    let prefix = "/api/payment/callback/";
    normalized
        .strip_prefix(prefix)
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn normalize_payment_callback_optional_string(
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, &'static str> {
    wallet_normalize_optional_string_field(value, max_chars)
}

fn normalize_payment_callback_request(
    payload: PaymentCallbackRequest,
) -> Result<NormalizedPaymentCallbackRequest, &'static str> {
    let callback_key = payload.callback_key.trim();
    if callback_key.is_empty() || callback_key.chars().count() > 128 {
        return Err("输入验证失败");
    }
    if !payload.amount_usd.is_finite() || payload.amount_usd <= 0.0 {
        return Err("输入验证失败");
    }
    if matches!(payload.pay_amount, Some(value) if !value.is_finite() || value <= 0.0) {
        return Err("输入验证失败");
    }
    if matches!(payload.exchange_rate, Some(value) if !value.is_finite() || value <= 0.0) {
        return Err("输入验证失败");
    }
    let order_no = normalize_payment_callback_optional_string(payload.order_no, 64)?;
    let gateway_order_id =
        normalize_payment_callback_optional_string(payload.gateway_order_id, 128)?;
    let pay_currency = normalize_payment_callback_optional_string(payload.pay_currency, 3)?;
    if matches!(pay_currency.as_deref(), Some(value) if value.chars().count() != 3) {
        return Err("输入验证失败");
    }

    let payload_value = payload.payload.map(serde_json::Value::Object).unwrap_or_else(|| {
        json!({
            "callback_key": callback_key,
            "order_no": order_no,
            "gateway_order_id": gateway_order_id,
            "amount_usd": payload.amount_usd,
            "pay_amount": payload.pay_amount,
            "pay_currency": pay_currency,
            "exchange_rate": payload.exchange_rate,
            "payload": serde_json::Value::Null,
        })
    });

    Ok(NormalizedPaymentCallbackRequest {
        callback_key: callback_key.to_string(),
        order_no,
        gateway_order_id,
        amount_usd: payload.amount_usd,
        pay_amount: payload.pay_amount,
        pay_currency,
        exchange_rate: payload.exchange_rate,
        payload: payment_callback_canonicalize_json(&payload_value),
    })
}

fn payment_callback_canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut items = map.iter().collect::<Vec<_>>();
            items.sort_by(|left, right| left.0.cmp(right.0));
            let mut object = serde_json::Map::new();
            for (key, value) in items {
                object.insert(
                    key.clone(),
                    payment_callback_canonicalize_json(value),
                );
            }
            serde_json::Value::Object(object)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(payment_callback_canonicalize_json)
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn payment_callback_signature_hex(
    payload: &serde_json::Value,
    secret: &str,
) -> Result<String, String> {
    let canonical = serde_json::to_string(payload)
        .map_err(|err| format!("payment callback canonicalization failed: {err}"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|err| format!("payment callback hmac init failed: {err}"))?;
    mac.update(canonical.as_bytes());
    let bytes = mac.finalize().into_bytes();
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn payment_callback_signature_matches(
    payload: &serde_json::Value,
    provided_signature: &str,
    secret: &str,
) -> Result<bool, String> {
    let expected = payment_callback_signature_hex(payload, secret)?;
    let provided = provided_signature
        .trim()
        .strip_prefix("sha256=")
        .unwrap_or(provided_signature.trim())
        .to_ascii_lowercase();
    Ok(provided == expected)
}

fn payment_callback_payload_hash(payload: &serde_json::Value) -> Result<String, String> {
    let encoded = serde_json::to_vec(payload)
        .map_err(|err| format!("payment callback payload encode failed: {err}"))?;
    let digest = Sha256::digest(&encoded);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn payment_callback_mark_failed_response(
    duplicate: bool,
    error: &str,
    payment_method: &str,
    request_path: &str,
) -> Response<Body> {
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "ok": false,
            "duplicate": duplicate,
            "error": error,
            "payment_method": payment_method,
            "request_path": request_path,
        }),
        None,
    )
}

async fn update_payment_callback_failure(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    callback_id: &str,
    payload: &NormalizedPaymentCallbackRequest,
    callback_payload_hash: &str,
    signature_valid: bool,
    error: &str,
) {
    let _ = sqlx::query(
        r#"
UPDATE payment_callbacks
SET signature_valid = $2,
    status = 'failed',
    error_message = $3,
    payload_hash = $4,
    payload = $5,
    processed_at = NOW(),
    order_no = COALESCE($6, order_no),
    gateway_order_id = COALESCE($7, gateway_order_id)
WHERE id = $1
        "#,
    )
    .bind(callback_id)
    .bind(signature_valid)
    .bind(error)
    .bind(callback_payload_hash)
    .bind(&payload.payload)
    .bind(payload.order_no.as_deref())
    .bind(payload.gateway_order_id.as_deref())
    .execute(&mut **tx)
    .await;
}

async fn handle_payment_callback_with_postgres(
    state: &AppState,
    payment_method: &str,
    request_context: &GatewayPublicRequestContext,
    payload: &NormalizedPaymentCallbackRequest,
    signature_valid: bool,
) -> Response<Body> {
    let Some(pool) = state.postgres_pool() else {
        return build_public_support_maintenance_response(
            "Payment callback routes require Rust maintenance backend",
        );
    };
    let mut tx = match pool.begin().await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback transaction failed: {err}"),
                false,
            );
        }
    };

    let existing_callback = match sqlx::query(
        r#"
SELECT id, payment_order_id, status, order_no, gateway_order_id
FROM payment_callbacks
WHERE callback_key = $1
LIMIT 1
        "#,
    )
    .bind(&payload.callback_key)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback lookup failed: {err}"),
                false,
            );
        }
    };

    let callback_payload_hash = match payment_callback_payload_hash(&payload.payload) {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                err,
                false,
            );
        }
    };

    let duplicate = existing_callback.is_some();
    let callback_id = if let Some(row) = existing_callback.as_ref() {
        let status = row
            .try_get::<String, _>("status")
            .ok()
            .unwrap_or_default();
        if status == "processed" {
            let order_id = row
                .try_get::<Option<String>, _>("payment_order_id")
                .ok()
                .flatten();
            let _ = tx.rollback().await;
            return build_auth_json_response(
                http::StatusCode::OK,
                json!({
                    "ok": true,
                    "duplicate": true,
                    "credited": false,
                    "order_id": order_id,
                    "payment_method": payment_method,
                    "request_path": request_context.request_path,
                }),
                None,
            );
        }
        row.try_get::<String, _>("id").ok().unwrap_or_default()
    } else {
        let callback_id = Uuid::new_v4().to_string();
        let insert_result = sqlx::query(
            r#"
INSERT INTO payment_callbacks (
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
  created_at,
  processed_at
)
VALUES (
  $1,
  NULL,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  'received',
  $8,
  NULL,
  NOW(),
  NULL
)
            "#,
        )
        .bind(&callback_id)
        .bind(payment_method)
        .bind(&payload.callback_key)
        .bind(payload.order_no.as_deref())
        .bind(payload.gateway_order_id.as_deref())
        .bind(&callback_payload_hash)
        .bind(signature_valid)
        .bind(&payload.payload)
        .execute(&mut *tx)
        .await;
        if let Err(err) = insert_result {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback create failed: {err}"),
                false,
            );
        }
        callback_id
    };

    if !signature_valid {
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            "invalid callback signature",
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            "invalid callback signature",
            payment_method,
            &request_context.request_path,
        );
    }

    let lookup_order_no = payload.order_no.clone().or_else(|| {
        existing_callback
            .as_ref()
            .and_then(|row| row.try_get::<Option<String>, _>("order_no").ok().flatten())
    });
    let lookup_gateway_order_id = payload.gateway_order_id.clone().or_else(|| {
        existing_callback.as_ref().and_then(|row| {
            row.try_get::<Option<String>, _>("gateway_order_id")
                .ok()
                .flatten()
        })
    });

    let order_row = if let Some(order_no) = lookup_order_no.as_deref() {
        match sqlx::query(
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
  status AS effective_status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE order_no = $1
LIMIT 1
FOR UPDATE
            "#,
        )
        .bind(order_no)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("payment callback order lookup failed: {err}"),
                    false,
                );
            }
        }
    } else if let Some(gateway_order_id) = lookup_gateway_order_id.as_deref() {
        match sqlx::query(
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
  status AS effective_status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE gateway_order_id = $1
LIMIT 1
FOR UPDATE
            "#,
        )
        .bind(gateway_order_id)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(value) => value,
            Err(err) => {
                let _ = tx.rollback().await;
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("payment callback order lookup failed: {err}"),
                    false,
                );
            }
        }
    } else {
        None
    };

    let Some(order_row) = order_row else {
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            "payment order not found",
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            "payment order not found",
            payment_method,
            &request_context.request_path,
        );
    };

    let order_id = order_row
        .try_get::<String, _>("id")
        .ok()
        .unwrap_or_default();
    let order_no = order_row
        .try_get::<String, _>("order_no")
        .ok()
        .unwrap_or_default();
    let order_wallet_id = order_row
        .try_get::<String, _>("wallet_id")
        .ok()
        .unwrap_or_default();
    let order_amount_usd = order_row
        .try_get::<f64, _>("amount_usd")
        .ok()
        .unwrap_or_default();
    let order_status = order_row
        .try_get::<String, _>("effective_status")
        .ok()
        .unwrap_or_default();
    let expires_at_unix_secs = order_row
        .try_get::<Option<i64>, _>("expires_at_unix_secs")
        .ok()
        .flatten();
    if (payload.amount_usd - order_amount_usd).abs() > f64::EPSILON {
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            "callback amount mismatch",
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            "callback amount mismatch",
            payment_method,
            &request_context.request_path,
        );
    }
    if order_status == "credited" {
        let _ = sqlx::query(
            r#"
UPDATE payment_callbacks
SET payment_order_id = $2,
    signature_valid = true,
    status = 'processed',
    error_message = NULL,
    payload_hash = $3,
    payload = $4,
    processed_at = NOW(),
    order_no = $5,
    gateway_order_id = COALESCE($6, gateway_order_id)
WHERE id = $1
            "#,
        )
        .bind(&callback_id)
        .bind(&order_id)
        .bind(&callback_payload_hash)
        .bind(&payload.payload)
        .bind(&order_no)
        .bind(payload.gateway_order_id.as_deref())
        .execute(&mut *tx)
        .await;
        let _ = tx.commit().await;
        return build_auth_json_response(
            http::StatusCode::OK,
            json!({
                "ok": true,
                "duplicate": duplicate,
                "credited": false,
                "order_id": order_id,
                "order_no": order_no,
                "status": "credited",
                "wallet_id": order_wallet_id,
                "payment_method": payment_method,
                "request_path": request_context.request_path,
            }),
            None,
        );
    }
    if matches!(order_status.as_str(), "failed" | "expired" | "refunded") {
        let error = format!("payment order is not creditable: {order_status}");
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            &error,
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            &error,
            payment_method,
            &request_context.request_path,
        );
    }
    if order_status == "pending" {
        let now = Utc::now().timestamp();
        if expires_at_unix_secs.is_some_and(|value| value < now) {
            let _ = sqlx::query(
                "UPDATE payment_orders SET status = 'expired' WHERE id = $1",
            )
            .bind(&order_id)
            .execute(&mut *tx)
            .await;
            update_payment_callback_failure(
                &mut tx,
                &callback_id,
                payload,
                &callback_payload_hash,
                signature_valid,
                "payment order expired",
            )
            .await;
            let _ = tx.commit().await;
            return payment_callback_mark_failed_response(
                duplicate,
                "payment order expired",
                payment_method,
                &request_context.request_path,
            );
        }
    }

    let wallet_row = match sqlx::query(
        r#"
SELECT
  id,
  status,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged
FROM wallets
WHERE id = $1
LIMIT 1
FOR UPDATE
        "#,
    )
    .bind(&order_wallet_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback wallet lookup failed: {err}"),
                false,
            );
        }
    };
    let Some(wallet_row) = wallet_row else {
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            "wallet not found",
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            "wallet not found",
            payment_method,
            &request_context.request_path,
        );
    };
    let wallet_status = wallet_row
        .try_get::<String, _>("status")
        .ok()
        .unwrap_or_default();
    if wallet_status != "active" {
        update_payment_callback_failure(
            &mut tx,
            &callback_id,
            payload,
            &callback_payload_hash,
            signature_valid,
            "wallet is not active",
        )
        .await;
        let _ = tx.commit().await;
        return payment_callback_mark_failed_response(
            duplicate,
            "wallet is not active",
            payment_method,
            &request_context.request_path,
        );
    }

    let before_recharge = wallet_row
        .try_get::<f64, _>("balance")
        .ok()
        .unwrap_or_default();
    let before_gift = wallet_row
        .try_get::<f64, _>("gift_balance")
        .ok()
        .unwrap_or_default();
    let before_total = before_recharge + before_gift;
    let after_recharge = before_recharge + order_amount_usd;
    let after_total = after_recharge + before_gift;
    let wallet_tx_id = Uuid::new_v4().to_string();

    if let Err(err) = sqlx::query(
        r#"
UPDATE wallets
SET balance = $2,
    total_recharged = total_recharged + $3,
    updated_at = NOW()
WHERE id = $1
        "#,
    )
    .bind(&order_wallet_id)
    .bind(after_recharge)
    .bind(order_amount_usd)
    .execute(&mut *tx)
    .await
    {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("payment callback wallet update failed: {err}"),
            false,
        );
    }

    if let Err(err) = sqlx::query(
        r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'recharge',
  'topup_gateway',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'payment_order',
  $10,
  NULL,
  $11,
  NOW()
)
        "#,
    )
    .bind(&wallet_tx_id)
    .bind(&order_wallet_id)
    .bind(order_amount_usd)
    .bind(before_total)
    .bind(after_total)
    .bind(before_recharge)
    .bind(after_recharge)
    .bind(before_gift)
    .bind(before_gift)
    .bind(&order_id)
    .bind(format!("充值到账({payment_method})"))
    .execute(&mut *tx)
    .await
    {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("payment callback wallet transaction failed: {err}"),
            false,
        );
    }

    let updated_order_row = match sqlx::query(
        r#"
UPDATE payment_orders
SET gateway_order_id = COALESCE($2, gateway_order_id),
    gateway_response = $3,
    pay_amount = COALESCE($4, pay_amount),
    pay_currency = COALESCE($5, pay_currency),
    exchange_rate = COALESCE($6, exchange_rate),
    status = 'credited',
    paid_at = COALESCE(paid_at, NOW()),
    credited_at = NOW(),
    refundable_amount_usd = amount_usd
WHERE id = $1
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
    .bind(payload.gateway_order_id.as_deref())
    .bind(&payload.payload)
    .bind(payload.pay_amount)
    .bind(payload.pay_currency.as_deref())
    .bind(payload.exchange_rate)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(err) => {
            let _ = tx.rollback().await;
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback order update failed: {err}"),
                false,
            );
        }
    };

    if let Err(err) = sqlx::query(
        r#"
UPDATE payment_callbacks
SET payment_order_id = $2,
    signature_valid = true,
    status = 'processed',
    error_message = NULL,
    payload_hash = $3,
    payload = $4,
    processed_at = NOW(),
    order_no = $5,
    gateway_order_id = COALESCE($6, gateway_order_id)
WHERE id = $1
        "#,
    )
    .bind(&callback_id)
    .bind(&order_id)
    .bind(&callback_payload_hash)
    .bind(&payload.payload)
    .bind(&order_no)
    .bind(payload.gateway_order_id.as_deref())
    .execute(&mut *tx)
    .await
    {
        let _ = tx.rollback().await;
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("payment callback update failed: {err}"),
            false,
        );
    }

    if let Err(err) = tx.commit().await {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("payment callback commit failed: {err}"),
            false,
        );
    }

    let updated_order_payload = match wallet_payment_order_payload_from_row(&updated_order_row) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("payment callback order payload failed: {err:?}"),
                false,
            );
        }
    };
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "ok": true,
            "duplicate": duplicate,
            "credited": true,
            "order_id": order_id,
            "order_no": order_no,
            "status": updated_order_payload["status"],
            "wallet_id": order_wallet_id,
            "payment_method": payment_method,
            "request_path": request_context.request_path,
        }),
        None,
    )
}

#[cfg(test)]
async fn handle_payment_callback_with_test_store(
    payment_method: &str,
    request_context: &GatewayPublicRequestContext,
    payload: &NormalizedPaymentCallbackRequest,
    signature_valid: bool,
) -> Response<Body> {
    let mut callback_store = payment_test_callback_store()
        .lock()
        .expect("payment test callback store should lock");
    if let Some(existing) = callback_store
        .iter()
        .find(|entry| entry.callback_key == payload.callback_key && entry.status == "processed")
    {
        return build_auth_json_response(
            http::StatusCode::OK,
            json!({
                "ok": true,
                "duplicate": true,
                "credited": false,
                "order_id": existing.payment_order_id,
                "payment_method": payment_method,
                "request_path": request_context.request_path,
            }),
            None,
        );
    }
    let duplicate = callback_store
        .iter()
        .any(|entry| entry.callback_key == payload.callback_key);
    if !signature_valid {
        callback_store.push(PaymentTestCallbackRecord {
            callback_key: payload.callback_key.clone(),
            payment_order_id: None,
            status: "failed".to_string(),
        });
        return payment_callback_mark_failed_response(
            duplicate,
            "invalid callback signature",
            payment_method,
            &request_context.request_path,
        );
    }

    let mut recharge_store = wallet_test_recharge_store()
        .lock()
        .expect("wallet test recharge store should lock");
    let order = recharge_store.iter_mut().find(|entry| {
        entry.payload["order_no"].as_str() == payload.order_no.as_deref()
            || entry.payload["gateway_order_id"].as_str() == payload.gateway_order_id.as_deref()
    });
    let Some(order) = order else {
        callback_store.push(PaymentTestCallbackRecord {
            callback_key: payload.callback_key.clone(),
            payment_order_id: None,
            status: "failed".to_string(),
        });
        return payment_callback_mark_failed_response(
            duplicate,
            "payment order not found",
            payment_method,
            &request_context.request_path,
        );
    };

    let order_amount = order.payload["amount_usd"].as_f64().unwrap_or_default();
    if (payload.amount_usd - order_amount).abs() > f64::EPSILON {
        callback_store.push(PaymentTestCallbackRecord {
            callback_key: payload.callback_key.clone(),
            payment_order_id: order.payload["id"].as_str().map(ToOwned::to_owned),
            status: "failed".to_string(),
        });
        return payment_callback_mark_failed_response(
            duplicate,
            "callback amount mismatch",
            payment_method,
            &request_context.request_path,
        );
    }

    let current_status = order.payload["status"].as_str().unwrap_or_default().to_string();
    if current_status == "credited" {
        let order_id = order.payload["id"].as_str().map(ToOwned::to_owned);
        callback_store.push(PaymentTestCallbackRecord {
            callback_key: payload.callback_key.clone(),
            payment_order_id: order_id.clone(),
            status: "processed".to_string(),
        });
        return build_auth_json_response(
            http::StatusCode::OK,
            json!({
                "ok": true,
                "duplicate": duplicate,
                "credited": false,
                "order_id": order_id,
                "order_no": order.payload["order_no"],
                "status": "credited",
                "wallet_id": order.payload["wallet_id"],
                "payment_method": payment_method,
                "request_path": request_context.request_path,
            }),
            None,
        );
    }

    let now = Utc::now().to_rfc3339();
    order.payload["status"] = json!("credited");
    order.payload["gateway_response"] =
        sanitize_wallet_gateway_response(Some(payload.payload.clone()));
    order.payload["pay_amount"] = match payload.pay_amount {
        Some(value) => json!(value),
        None => order.payload["pay_amount"].clone(),
    };
    order.payload["pay_currency"] = match payload.pay_currency.as_deref() {
        Some(value) => json!(value),
        None => order.payload["pay_currency"].clone(),
    };
    order.payload["exchange_rate"] = match payload.exchange_rate {
        Some(value) => json!(value),
        None => order.payload["exchange_rate"].clone(),
    };
    if let Some(gateway_order_id) = payload.gateway_order_id.as_deref() {
        order.payload["gateway_order_id"] = json!(gateway_order_id);
    }
    order.payload["refundable_amount_usd"] = json!(order_amount);
    order.payload["paid_at"] = json!(now.clone());
    order.payload["credited_at"] = json!(now);
    let order_id = order.payload["id"].as_str().map(ToOwned::to_owned);
    let order_no = order.payload["order_no"].as_str().map(ToOwned::to_owned);
    let wallet_id = order.payload["wallet_id"].as_str().map(ToOwned::to_owned);
    callback_store.push(PaymentTestCallbackRecord {
        callback_key: payload.callback_key.clone(),
        payment_order_id: order_id.clone(),
        status: "processed".to_string(),
    });
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "ok": true,
            "duplicate": duplicate,
            "credited": true,
            "order_id": order_id,
            "order_no": order_no,
            "status": "credited",
            "wallet_id": wallet_id,
            "payment_method": payment_method,
            "request_path": request_context.request_path,
        }),
        None,
    )
}

async fn maybe_build_local_payment_callback_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("payment_callback_legacy")
        || decision.route_kind.as_deref() != Some("callback")
    {
        return None;
    }

    let Some(secret) = payment_callback_secret() else {
        return Some(build_auth_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "payment callback is disabled",
            false,
        ));
    };
    let Some(provided_token) = crate::gateway::headers::header_value_str(
        headers,
        PAYMENT_CALLBACK_TOKEN_HEADER,
    ) else {
        return Some(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "invalid payment callback token",
            false,
        ));
    };
    if provided_token.trim() != secret {
        return Some(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "invalid payment callback token",
            false,
        ));
    }
    let Some(signature) = crate::gateway::headers::header_value_str(
        headers,
        PAYMENT_CALLBACK_SIGNATURE_HEADER,
    ) else {
        return Some(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "missing payment callback signature",
            false,
        ));
    };
    let Some(request_body) = request_body else {
        return Some(build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "缺少请求体",
            false,
        ));
    };
    let raw_payload = match serde_json::from_slice::<PaymentCallbackRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return Some(build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "输入验证失败",
                false,
            ));
        }
    };
    let payload = match normalize_payment_callback_request(raw_payload) {
        Ok(value) => value,
        Err(detail) => {
            return Some(build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                detail,
                false,
            ));
        }
    };
    let Some(payment_method) =
        payment_callback_payment_method_from_path(&request_context.request_path)
    else {
        return Some(build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "payment_method is required",
            false,
        ));
    };
    let signature_valid = match payment_callback_signature_matches(&payload.payload, &signature, &secret)
    {
        Ok(value) => value,
        Err(err) => {
            return Some(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                err,
                false,
            ));
        }
    };

    if state.postgres_pool().is_some() {
        return Some(
            handle_payment_callback_with_postgres(
                state,
                &payment_method,
                request_context,
                &payload,
                signature_valid,
            )
            .await,
        );
    }

    #[cfg(test)]
    {
        return Some(
            handle_payment_callback_with_test_store(
                &payment_method,
                request_context,
                &payload,
                signature_valid,
            )
            .await,
        );
    }

    #[cfg(not(test))]
    {
        Some(build_public_support_maintenance_response(
            "Payment callback routes require Rust maintenance backend",
        ))
    }
}
