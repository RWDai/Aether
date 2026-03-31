const ADMIN_PROVIDER_OPS_SENSITIVE_FIELDS: &[&str] = &[
    "api_key",
    "password",
    "refresh_token",
    "session_token",
    "session_cookie",
    "token_cookie",
    "auth_cookie",
    "cookie_string",
    "cookie",
];
const ADMIN_PROVIDER_OPS_CONNECT_RUST_ONLY_MESSAGE: &str = "Provider 连接仅支持 Rust executor";
const ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE: &str = "Provider 操作仅支持 Rust executor";
const ADMIN_PROVIDER_OPS_VERIFY_RUST_ONLY_MESSAGE: &str = "认证验证仅支持 Rust executor";

#[derive(Debug, Deserialize)]
struct AdminProviderOpsSaveConfigRequest {
    #[serde(default = "default_admin_provider_ops_architecture_id")]
    architecture_id: String,
    #[serde(default)]
    base_url: Option<String>,
    connector: AdminProviderOpsConnectorConfigRequest,
    #[serde(default)]
    actions: BTreeMap<String, AdminProviderOpsActionConfigRequest>,
    #[serde(default)]
    schedule: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderOpsConnectorConfigRequest {
    auth_type: String,
    #[serde(default)]
    config: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    credentials: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderOpsActionConfigRequest {
    #[serde(default = "default_admin_provider_ops_action_enabled")]
    enabled: bool,
    #[serde(default)]
    config: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderOpsConnectRequest {
    #[serde(default)]
    credentials: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderOpsExecuteActionRequest {
    #[serde(default)]
    config: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone)]
struct AdminProviderOpsCheckinOutcome {
    success: Option<bool>,
    message: String,
    cookie_expired: bool,
}

fn default_admin_provider_ops_architecture_id() -> String {
    "generic_api".to_string()
}

fn default_admin_provider_ops_action_enabled() -> bool {
    true
}

fn admin_provider_ops_config_object(
    provider: &StoredProviderCatalogProvider,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    provider
        .config
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|config| config.get("provider_ops"))
        .and_then(serde_json::Value::as_object)
}

fn admin_provider_ops_connector_object(
    provider_ops_config: &serde_json::Map<String, serde_json::Value>,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    provider_ops_config
        .get("connector")
        .and_then(serde_json::Value::as_object)
}

fn admin_provider_ops_masked_secret(
    state: &AppState,
    field: &str,
    ciphertext: &str,
) -> serde_json::Value {
    let plaintext = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
        .unwrap_or_else(|| ciphertext.to_string());
    if plaintext.is_empty() {
        return serde_json::Value::String(String::new());
    }

    let masked = if field == "password" {
        "********".to_string()
    } else if plaintext.len() > 12 {
        format!(
            "{}****{}",
            &plaintext[..4],
            &plaintext[plaintext.len().saturating_sub(4)..]
        )
    } else if plaintext.len() > 8 {
        format!(
            "{}****{}",
            &plaintext[..2],
            &plaintext[plaintext.len().saturating_sub(2)..]
        )
    } else {
        "*".repeat(plaintext.len())
    };

    serde_json::Value::String(masked)
}

fn admin_provider_ops_masked_credentials(
    state: &AppState,
    raw_credentials: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(credentials) = raw_credentials.and_then(serde_json::Value::as_object) else {
        return json!({});
    };

    let mut masked = serde_json::Map::new();
    for (key, value) in credentials {
        if ADMIN_PROVIDER_OPS_SENSITIVE_FIELDS.contains(&key.as_str()) {
            if let Some(ciphertext) = value.as_str().filter(|value| !value.is_empty()) {
                masked.insert(
                    key.clone(),
                    admin_provider_ops_masked_secret(state, key, ciphertext),
                );
                continue;
            }
        }
        masked.insert(key.clone(), value.clone());
    }
    serde_json::Value::Object(masked)
}

fn admin_provider_ops_is_supported_auth_type(auth_type: &str) -> bool {
    matches!(
        auth_type,
        "api_key" | "session_login" | "oauth" | "cookie" | "none"
    )
}

fn admin_provider_ops_uses_python_verify_fallback(
    architecture_id: &str,
    config: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let _ = architecture_id;
    config
        .get("proxy_enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || config
            .get("proxy_node_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
}

fn admin_provider_ops_decrypted_credentials(
    state: &AppState,
    raw_credentials: Option<&serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let Some(credentials) = raw_credentials.and_then(serde_json::Value::as_object) else {
        return serde_json::Map::new();
    };

    let mut decrypted = serde_json::Map::new();
    for (key, value) in credentials {
        if ADMIN_PROVIDER_OPS_SENSITIVE_FIELDS.contains(&key.as_str()) {
            if let Some(ciphertext) = value.as_str() {
                let plaintext =
                    decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
                        .unwrap_or_else(|| ciphertext.to_string());
                decrypted.insert(key.clone(), serde_json::Value::String(plaintext));
                continue;
            }
        }
        decrypted.insert(key.clone(), value.clone());
    }
    decrypted
}

fn admin_provider_ops_sensitive_placeholder_or_empty(value: Option<&serde_json::Value>) -> bool {
    match value {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(raw)) => {
            raw.is_empty() || raw.chars().all(|ch| ch == '*')
        }
        Some(serde_json::Value::Array(items)) => items.is_empty(),
        Some(serde_json::Value::Object(map)) => map.is_empty(),
        _ => false,
    }
}

fn admin_provider_ops_merge_credentials(
    state: &AppState,
    provider: &StoredProviderCatalogProvider,
    mut request_credentials: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let saved_credentials = admin_provider_ops_decrypted_credentials(
        state,
        admin_provider_ops_config_object(provider)
            .and_then(admin_provider_ops_connector_object)
            .and_then(|connector| connector.get("credentials")),
    );

    for field in ADMIN_PROVIDER_OPS_SENSITIVE_FIELDS {
        if admin_provider_ops_sensitive_placeholder_or_empty(request_credentials.get(*field))
            && saved_credentials.contains_key(*field)
        {
            if let Some(saved_value) = saved_credentials.get(*field) {
                request_credentials.insert((*field).to_string(), saved_value.clone());
            }
        }
    }

    for (key, value) in saved_credentials {
        if key.starts_with('_') && !request_credentials.contains_key(&key) {
            request_credentials.insert(key, value);
        }
    }

    request_credentials
}

fn admin_provider_ops_encrypt_credentials(
    state: &AppState,
    credentials: serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut encrypted = serde_json::Map::new();
    for (key, value) in credentials {
        if ADMIN_PROVIDER_OPS_SENSITIVE_FIELDS.contains(&key.as_str()) {
            if let Some(plaintext) = value.as_str() {
                if plaintext.is_empty() {
                    encrypted.insert(key, value);
                } else {
                    let ciphertext = encrypt_catalog_secret_with_fallbacks(state, plaintext)
                        .ok_or_else(|| "gateway 未配置 Provider Ops 加密密钥".to_string())?;
                    encrypted.insert(key, serde_json::Value::String(ciphertext));
                }
                continue;
            }
        }
        encrypted.insert(key, value);
    }
    Ok(encrypted)
}

fn build_admin_provider_ops_saved_config_value(
    state: &AppState,
    provider: &StoredProviderCatalogProvider,
    payload: AdminProviderOpsSaveConfigRequest,
) -> Result<serde_json::Value, String> {
    let auth_type = payload.connector.auth_type.trim().to_string();
    if auth_type.is_empty() || !admin_provider_ops_is_supported_auth_type(auth_type.as_str()) {
        return Err("connector.auth_type 必须是合法的认证类型".to_string());
    }

    let merged_credentials =
        admin_provider_ops_merge_credentials(state, provider, payload.connector.credentials);
    let encrypted_credentials = admin_provider_ops_encrypt_credentials(state, merged_credentials)?;

    let actions = payload
        .actions
        .into_iter()
        .map(|(action_type, config)| {
            (
                action_type,
                json!({
                    "enabled": config.enabled,
                    "config": config.config,
                }),
            )
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();

    Ok(json!({
        "architecture_id": payload.architecture_id,
        "base_url": payload.base_url,
        "connector": {
            "auth_type": auth_type,
            "config": payload.connector.config,
            "credentials": encrypted_credentials,
        },
        "actions": actions,
        "schedule": payload.schedule,
    }))
}

fn resolve_admin_provider_ops_base_url(
    provider: &StoredProviderCatalogProvider,
    endpoints: &[StoredProviderCatalogEndpoint],
    provider_ops_config: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<String> {
    let from_saved_config = provider_ops_config
        .and_then(|config| config.get("base_url"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if from_saved_config.is_some() {
        return from_saved_config;
    }

    if let Some(base_url) = endpoints
        .iter()
        .find_map(|endpoint| {
            let value = endpoint.base_url.trim();
            (!value.is_empty()).then(|| value.to_string())
        })
    {
        return Some(base_url);
    }

    let from_provider_config = provider
        .config
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|config| config.get("base_url"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if from_provider_config.is_some() {
        return from_provider_config;
    }

    provider
        .website
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn build_admin_provider_ops_status_payload(
    provider_id: &str,
    provider: Option<&StoredProviderCatalogProvider>,
) -> serde_json::Value {
    let provider_ops_config = provider.and_then(admin_provider_ops_config_object);
    let auth_type = provider_ops_config
        .and_then(admin_provider_ops_connector_object)
        .and_then(|connector| connector.get("auth_type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| {
            if provider_ops_config.is_some() {
                "api_key"
            } else {
                "none"
            }
        });
    let mut enabled_actions = provider_ops_config
        .and_then(|config| config.get("actions"))
        .and_then(serde_json::Value::as_object)
        .map(|actions| {
            actions
                .iter()
                .filter_map(|(action_type, config)| {
                    let enabled = config
                        .as_object()
                        .and_then(|config| config.get("enabled"))
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(true);
                    enabled.then(|| serde_json::Value::String(action_type.clone()))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    enabled_actions.sort_by(|left, right| left.as_str().cmp(&right.as_str()));

    json!({
        "provider_id": provider_id,
        "is_configured": provider_ops_config.is_some(),
        "architecture_id": provider_ops_config.map(|config| {
            config
                .get("architecture_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("generic_api")
        }),
        "connection_status": {
            "status": "disconnected",
            "auth_type": auth_type,
            "connected_at": serde_json::Value::Null,
            "expires_at": serde_json::Value::Null,
            "last_error": serde_json::Value::Null,
        },
        "enabled_actions": enabled_actions,
    })
}

fn build_admin_provider_ops_config_payload(
    state: &AppState,
    provider_id: &str,
    provider: Option<&StoredProviderCatalogProvider>,
    endpoints: &[StoredProviderCatalogEndpoint],
) -> serde_json::Value {
    let Some(provider) = provider else {
        return json!({
            "provider_id": provider_id,
            "is_configured": false,
        });
    };
    let Some(provider_ops_config) = admin_provider_ops_config_object(provider) else {
        return json!({
            "provider_id": provider_id,
            "is_configured": false,
        });
    };
    let connector = admin_provider_ops_connector_object(provider_ops_config);

    json!({
        "provider_id": provider_id,
        "is_configured": true,
        "architecture_id": provider_ops_config
            .get("architecture_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("generic_api"),
        "base_url": resolve_admin_provider_ops_base_url(
            provider,
            endpoints,
            Some(provider_ops_config),
        ),
        "connector": {
            "auth_type": connector
                .and_then(|connector| connector.get("auth_type"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("api_key"),
            "config": connector
                .and_then(|connector| connector.get("config"))
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| json!({})),
            "credentials": admin_provider_ops_masked_credentials(
                state,
                connector.and_then(|connector| connector.get("credentials")),
            ),
        },
    })
}

fn admin_provider_ops_normalized_verify_architecture_id(architecture_id: &str) -> &str {
    match architecture_id.trim() {
        "" => "generic_api",
        "generic_api" | "new_api" | "cubence" | "yescode" | "nekocode" | "anyrouter"
        | "sub2api" => architecture_id.trim(),
        _ => "generic_api",
    }
}

fn admin_provider_ops_extract_cookie_value(cookie_input: &str, key: &str) -> String {
    if cookie_input.contains(&format!("{key}=")) {
        for part in cookie_input.split(';') {
            let trimmed = part.trim();
            if let Some(value) = trimmed.strip_prefix(&format!("{key}=")) {
                return value.trim().to_string();
            }
        }
    }
    cookie_input.trim().to_string()
}

fn admin_provider_ops_yescode_cookie_header(cookie_input: &str) -> String {
    if cookie_input.contains("yescode_auth=") {
        let mut parts = Vec::new();
        for part in cookie_input.split(';') {
            let trimmed = part.trim();
            if let Some(value) = trimmed.strip_prefix("yescode_auth=") {
                parts.push(format!("yescode_auth={}", value.trim()));
            } else if let Some(value) = trimmed.strip_prefix("yescode_csrf=") {
                parts.push(format!("yescode_csrf={}", value.trim()));
            }
        }
        return parts.join("; ");
    }
    format!("yescode_auth={}", cookie_input.trim())
}

const ADMIN_PROVIDER_OPS_ANYROUTER_XOR_KEY: &str = "3000176000856006061501533003690027800375";
const ADMIN_PROVIDER_OPS_ANYROUTER_UNSBOX_TABLE: [usize; 40] = [
    0xF, 0x23, 0x1D, 0x18, 0x21, 0x10, 0x1, 0x26, 0xA, 0x9, 0x13, 0x1F, 0x28, 0x1B, 0x16, 0x17,
    0x19, 0xD, 0x6, 0xB, 0x27, 0x12, 0x14, 0x8, 0xE, 0x15, 0x20, 0x1A, 0x2, 0x1E, 0x7, 0x4, 0x11,
    0x5, 0x3, 0x1C, 0x22, 0x25, 0xC, 0x24,
];

fn admin_provider_ops_anyrouter_compute_acw_sc_v2(arg1: &str) -> Option<String> {
    if arg1.len() != 40 || !arg1.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    let chars = arg1.chars().collect::<Vec<_>>();
    let unsboxed = ADMIN_PROVIDER_OPS_ANYROUTER_UNSBOX_TABLE
        .iter()
        .map(|index| chars.get(index.saturating_sub(1)).copied())
        .collect::<Option<String>>()?;

    let mut result = String::with_capacity(40);
    for i in (0..40).step_by(2) {
        let a = u8::from_str_radix(&unsboxed[i..i + 2], 16).ok()?;
        let b = u8::from_str_radix(&ADMIN_PROVIDER_OPS_ANYROUTER_XOR_KEY[i..i + 2], 16).ok()?;
        result.push_str(&format!("{:02x}", a ^ b));
    }
    Some(result)
}

fn admin_provider_ops_anyrouter_parse_session_user_id(cookie_input: &str) -> Option<String> {
    let session_cookie = admin_provider_ops_extract_cookie_value(cookie_input, "session");
    let decoded = URL_SAFE_NO_PAD.decode(session_cookie.as_bytes()).ok()?;
    let text = String::from_utf8_lossy(&decoded);
    let mut parts = text.split('|');
    let _timestamp = parts.next()?;
    let gob_b64 = parts.next()?;
    let gob_data = URL_SAFE_NO_PAD.decode(gob_b64.as_bytes()).ok()?;

    let id_pattern = b"\x02id\x03int";
    let id_idx = gob_data.windows(id_pattern.len()).position(|window| window == id_pattern)?;
    let value_start = id_idx + id_pattern.len() + 2;
    let first_byte = *gob_data.get(value_start)?;
    if first_byte != 0 {
        return None;
    }
    let marker = *gob_data.get(value_start + 1)?;
    if marker < 0x80 {
        return None;
    }
    let length = 256usize.saturating_sub(marker as usize);
    let end = value_start + 2 + length;
    let bytes = gob_data.get(value_start + 2..end)?;
    let val = bytes
        .iter()
        .fold(0u64, |acc, byte| (acc << 8) | (*byte as u64));
    Some((val >> 1).to_string())
}

async fn admin_provider_ops_anyrouter_acw_cookie(
    state: &AppState,
    base_url: &str,
) -> Option<String> {
    let response = state
        .client
        .get(base_url.trim_end_matches('/'))
        .header(
            reqwest::header::USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .ok()?;
    let body = response.text().await.ok()?;
    let compiled = Regex::new(r"var\s+arg1\s*=\s*'([0-9a-fA-F]{40})'").ok()?;
    let captures = compiled.captures(&body)?;
    let arg1 = captures.get(1)?.as_str();
    admin_provider_ops_anyrouter_compute_acw_sc_v2(arg1)
        .map(|value| format!("acw_sc__v2={value}"))
}

fn admin_provider_ops_verify_failure(message: impl Into<String>) -> serde_json::Value {
    json!({
        "success": false,
        "message": message.into(),
    })
}

fn admin_provider_ops_verify_success(
    data: serde_json::Value,
    updated_credentials: Option<serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    let mut payload = serde_json::Map::from_iter([
        ("success".to_string(), serde_json::Value::Bool(true)),
        ("data".to_string(), data),
    ]);
    if let Some(credentials) = updated_credentials.filter(|value| !value.is_empty()) {
        payload.insert(
            "updated_credentials".to_string(),
            serde_json::Value::Object(credentials),
        );
    }
    serde_json::Value::Object(payload)
}

fn admin_provider_ops_verify_user_payload(
    username: Option<String>,
    display_name: Option<String>,
    email: Option<String>,
    quota: Option<f64>,
    extra: Option<serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    let resolved_username = username.filter(|value| !value.trim().is_empty());
    let resolved_display_name = display_name
        .filter(|value| !value.trim().is_empty())
        .or_else(|| resolved_username.clone());
    let mut payload = serde_json::Map::new();
    payload.insert(
        "username".to_string(),
        resolved_username
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    payload.insert(
        "display_name".to_string(),
        resolved_display_name
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    payload.insert(
        "email".to_string(),
        email.map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    payload.insert(
        "quota".to_string(),
        quota
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
    );
    if let Some(extra) = extra.filter(|value| !value.is_empty()) {
        payload.insert("extra".to_string(), serde_json::Value::Object(extra));
    }
    serde_json::Value::Object(payload)
}

fn admin_provider_ops_value_as_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(raw)) => raw.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn admin_provider_ops_json_object(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    value.as_object()
}

fn admin_provider_ops_frontend_updated_credentials(
    credentials: serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let filtered = credentials
        .into_iter()
        .filter(|(key, value)| {
            !key.starts_with('_')
                && !matches!(value, serde_json::Value::Null)
                && !value
                    .as_str()
                    .is_some_and(|raw| raw.trim().is_empty())
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();
    (!filtered.is_empty()).then_some(filtered)
}

fn admin_provider_ops_generic_verify_payload(
    status: http::StatusCode,
    response_json: &serde_json::Value,
) -> serde_json::Value {
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_verify_failure("认证失败：无效的凭据");
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_verify_failure("认证失败：权限不足");
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_verify_failure(format!("验证失败：HTTP {}", status.as_u16()));
    }

    let user_data = if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && response_json
            .get("data")
            .is_some_and(serde_json::Value::is_object)
    {
        response_json.get("data")
    } else if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        return admin_provider_ops_verify_failure(
            response_json
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("验证失败"),
        );
    } else {
        Some(response_json)
    };

    let Some(user_data) = user_data.and_then(admin_provider_ops_json_object) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };

    let mut extra = serde_json::Map::new();
    for (key, value) in user_data {
        if matches!(
            key.as_str(),
            "username" | "display_name" | "email" | "quota" | "used_quota" | "request_count"
        ) {
            continue;
        }
        extra.insert(key.clone(), value.clone());
    }

    admin_provider_ops_verify_success(
        admin_provider_ops_verify_user_payload(
            user_data
                .get("username")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("display_name")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("email")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            admin_provider_ops_value_as_f64(user_data.get("quota")),
            Some(extra),
        ),
        None,
    )
}

fn admin_provider_ops_cubence_verify_payload(
    status: http::StatusCode,
    response_json: &serde_json::Value,
) -> serde_json::Value {
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_verify_failure("Cookie 已失效，请重新配置");
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_verify_failure("Cookie 已失效或无权限");
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_verify_failure(format!("验证失败：HTTP {}", status.as_u16()));
    }

    let Some(payload) = admin_provider_ops_json_object(response_json) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };
    let user_info = payload
        .get("user")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let balance_info = payload
        .get("balance")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();

    let mut extra = serde_json::Map::new();
    if let Some(role) = user_info.get("role") {
        extra.insert("role".to_string(), role.clone());
    }
    if let Some(invite_code) = user_info.get("invite_code") {
        extra.insert("invite_code".to_string(), invite_code.clone());
    }

    admin_provider_ops_verify_success(
        admin_provider_ops_verify_user_payload(
            user_info
                .get("username")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_info
                .get("username")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            None,
            admin_provider_ops_value_as_f64(balance_info.get("total_balance_dollar")),
            Some(extra),
        ),
        None,
    )
}

fn admin_provider_ops_yescode_verify_payload(
    status: http::StatusCode,
    response_json: &serde_json::Value,
) -> serde_json::Value {
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_verify_failure("Cookie 已失效，请重新配置");
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_verify_failure("Cookie 已失效或无权限");
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_verify_failure(format!("验证失败：HTTP {}", status.as_u16()));
    }

    let Some(payload) = admin_provider_ops_json_object(response_json) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };
    let Some(username) = payload
        .get("username")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };

    let pay_as_you_go =
        admin_provider_ops_value_as_f64(payload.get("pay_as_you_go_balance")).unwrap_or(0.0);
    let subscription =
        admin_provider_ops_value_as_f64(payload.get("subscription_balance")).unwrap_or(0.0);
    let plan = payload
        .get("subscription_plan")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let weekly_limit = admin_provider_ops_value_as_f64(
        payload
            .get("weekly_limit")
            .or_else(|| plan.get("weekly_limit")),
    );
    let weekly_spent = admin_provider_ops_value_as_f64(
        payload
            .get("weekly_spent_balance")
            .or_else(|| payload.get("current_week_spend")),
    )
    .unwrap_or(0.0);
    let subscription_available = weekly_limit
        .map(|limit| (limit - weekly_spent).max(0.0).min(subscription))
        .unwrap_or(subscription);

    admin_provider_ops_verify_success(
        admin_provider_ops_verify_user_payload(
            Some(username.clone()),
            Some(username),
            payload
                .get("email")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            Some(pay_as_you_go + subscription_available),
            None,
        ),
        None,
    )
}

fn admin_provider_ops_nekocode_verify_payload(
    status: http::StatusCode,
    response_json: &serde_json::Value,
) -> serde_json::Value {
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_verify_failure("Cookie 已失效，请重新配置");
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_verify_failure("Cookie 已失效或无权限");
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_verify_failure(format!("验证失败：HTTP {}", status.as_u16()));
    }

    let user_data = if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && response_json
            .get("data")
            .is_some_and(serde_json::Value::is_object)
    {
        response_json.get("data")
    } else {
        Some(response_json)
    };
    let Some(user_data) = user_data.and_then(admin_provider_ops_json_object) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };

    admin_provider_ops_verify_success(
        admin_provider_ops_verify_user_payload(
            user_data
                .get("username")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("display_name")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("email")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            admin_provider_ops_value_as_f64(user_data.get("balance")),
            None,
        ),
        None,
    )
}

async fn admin_provider_ops_sub2api_exchange_token(
    state: &AppState,
    base_url: &str,
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, Option<serde_json::Map<String, serde_json::Value>>), String> {
    let email = credentials
        .get("email")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let password = credentials
        .get("password")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let refresh_token = credentials
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default();

    let (path, body, default_error, previous_refresh_token) = if !email.is_empty() && !password.is_empty() {
        (
            "/api/v1/auth/login",
            json!({ "email": email, "password": password }),
            "登录失败",
            None,
        )
    } else if !refresh_token.is_empty() {
        (
            "/api/v1/auth/refresh",
            json!({ "refresh_token": refresh_token }),
            "Refresh Token 无效或已过期",
            Some(refresh_token),
        )
    } else {
        return Err("请填写账号密码或 Refresh Token".to_string());
    };

    let response = match state
        .client
        .post(format!("{}{path}", base_url.trim_end_matches('/')))
        .json(&body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_timeout() => return Err("连接超时".to_string()),
        Err(err) if err.is_connect() => return Err(format!("连接失败: {err}")),
        Err(err) => return Err(format!("验证失败: {err}")),
    };

    let status = response.status();
    let response_json = match response.bytes().await {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    };
    let payload = response_json
        .as_object()
        .cloned()
        .unwrap_or_default();
    if status != http::StatusCode::OK
        || payload
            .get("code")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(-1)
            != 0
    {
        let message = payload
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(default_error);
        return Err(message.to_string());
    }

    let Some(token_data) = payload.get("data").and_then(serde_json::Value::as_object) else {
        return Err("响应格式无效".to_string());
    };
    let access_token = token_data
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "响应格式无效".to_string())?;

    let mut updated_credentials = serde_json::Map::new();
    if let Some(new_refresh_token) = token_data
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if previous_refresh_token != Some(new_refresh_token) {
            updated_credentials.insert(
                "refresh_token".to_string(),
                serde_json::Value::String(new_refresh_token.to_string()),
            );
        }
    }

    Ok((
        access_token.to_string(),
        admin_provider_ops_frontend_updated_credentials(updated_credentials),
    ))
}

fn admin_provider_ops_sub2api_verify_payload(
    status: http::StatusCode,
    response_json: &serde_json::Value,
    updated_credentials: Option<serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_verify_failure("认证失败：无效的凭据");
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_verify_failure("认证失败：权限不足");
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_verify_failure(format!("验证失败：HTTP {}", status.as_u16()));
    }

    let Some(payload) = admin_provider_ops_json_object(response_json) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };
    if payload
        .get("code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(-1)
        != 0
    {
        return admin_provider_ops_verify_failure(
            payload
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("验证失败"),
        );
    }

    let Some(user_data) = payload.get("data").and_then(serde_json::Value::as_object) else {
        return admin_provider_ops_verify_failure("响应格式无效");
    };
    let balance = admin_provider_ops_value_as_f64(user_data.get("balance")).unwrap_or(0.0);
    let points = admin_provider_ops_value_as_f64(user_data.get("points")).unwrap_or(0.0);
    let mut extra = serde_json::Map::new();
    for key in ["balance", "points", "status", "concurrency"] {
        if let Some(value) = user_data.get(key) {
            extra.insert(key.to_string(), value.clone());
        }
    }

    admin_provider_ops_verify_success(
        admin_provider_ops_verify_user_payload(
            user_data
                .get("username")
                .or_else(|| user_data.get("email"))
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("username")
                .or_else(|| user_data.get("email"))
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            user_data
                .get("email")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            Some(balance + points),
            Some(extra),
        ),
        updated_credentials,
    )
}

async fn admin_provider_ops_local_sub2api_verify_response(
    state: &AppState,
    base_url: &str,
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return admin_provider_ops_verify_failure("请提供 API 地址");
    }

    let (access_token, updated_credentials) =
        match admin_provider_ops_sub2api_exchange_token(state, base_url, credentials).await {
            Ok(value) => value,
            Err(message) => return admin_provider_ops_verify_failure(message),
        };

    let response = match state
        .client
        .get(format!("{base_url}/api/v1/auth/me?timezone=Asia/Shanghai"))
        .bearer_auth(access_token)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_timeout() => return admin_provider_ops_verify_failure("连接超时"),
        Err(err) if err.is_connect() => {
            return admin_provider_ops_verify_failure(format!("连接失败: {err}"));
        }
        Err(err) => return admin_provider_ops_verify_failure(format!("验证失败: {err}")),
    };

    let status = response.status();
    let response_json = match response.bytes().await {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    };
    admin_provider_ops_sub2api_verify_payload(status, &response_json, updated_credentials)
}

fn admin_provider_ops_insert_header(
    headers: &mut reqwest::header::HeaderMap,
    name: &str,
    value: &str,
) -> Result<(), String> {
    let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| format!("无效的请求头: {name}"))?;
    let header_value =
        reqwest::header::HeaderValue::from_str(value).map_err(|_| format!("无效的请求头值: {name}"))?;
    headers.insert(header_name, header_value);
    Ok(())
}

fn admin_provider_ops_verify_headers(
    architecture_id: &str,
    config: &serde_json::Map<String, serde_json::Value>,
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> Result<reqwest::header::HeaderMap, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    match architecture_id {
        "generic_api" => {
            let api_key = credentials
                .get("api_key")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim();
            if !api_key.is_empty() {
                let auth_method = config
                    .get("auth_method")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("bearer");
                if auth_method == "header" {
                    let header_name = config
                        .get("header_name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("X-API-Key");
                    admin_provider_ops_insert_header(&mut headers, header_name, api_key)?;
                } else {
                    admin_provider_ops_insert_header(
                        &mut headers,
                        "Authorization",
                        &format!("Bearer {api_key}"),
                    )?;
                }
            }
        }
        "new_api" => {
            for (name, value) in [
                (
                    "User-Agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.249 Electron/38.7.0 Safari/537.36",
                ),
                ("Accept", "application/json"),
                ("Accept-Encoding", "gzip, deflate, br"),
                ("Accept-Language", "zh-CN"),
                ("sec-ch-ua", "\"Not=A?Brand\";v=\"24\", \"Chromium\";v=\"140\""),
                ("sec-ch-ua-mobile", "?0"),
                ("sec-ch-ua-platform", "\"macOS\""),
                ("Sec-Fetch-Site", "cross-site"),
                ("Sec-Fetch-Mode", "cors"),
                ("Sec-Fetch-Dest", "empty"),
            ] {
                admin_provider_ops_insert_header(&mut headers, name, value)?;
            }
            if let Some(api_key) = credentials.get("api_key").and_then(serde_json::Value::as_str) {
                if !api_key.trim().is_empty() {
                    admin_provider_ops_insert_header(
                        &mut headers,
                        "Authorization",
                        &format!("Bearer {}", api_key.trim()),
                    )?;
                }
            }
            if let Some(user_id) = credentials.get("user_id").and_then(serde_json::Value::as_str) {
                if !user_id.trim().is_empty() {
                    admin_provider_ops_insert_header(&mut headers, "New-Api-User", user_id.trim())?;
                }
            }
            if let Some(cookie) = credentials.get("cookie").and_then(serde_json::Value::as_str) {
                if !cookie.trim().is_empty() {
                    admin_provider_ops_insert_header(&mut headers, "Cookie", cookie.trim())?;
                }
            }
        }
        "cubence" => {
            if let Some(token_cookie) = credentials
                .get("token_cookie")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                let token = admin_provider_ops_extract_cookie_value(token_cookie, "token");
                admin_provider_ops_insert_header(&mut headers, "Cookie", &format!("token={token}"))?;
            }
        }
        "yescode" => {
            if let Some(auth_cookie) = credentials
                .get("auth_cookie")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                admin_provider_ops_insert_header(
                    &mut headers,
                    "Cookie",
                    &admin_provider_ops_yescode_cookie_header(auth_cookie),
                )?;
            }
        }
        "nekocode" => {
            if let Some(session_cookie) = credentials
                .get("session_cookie")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                let session =
                    admin_provider_ops_extract_cookie_value(session_cookie, "session");
                admin_provider_ops_insert_header(
                    &mut headers,
                    "Cookie",
                    &format!("session={session}"),
                )?;
            }
        }
        "anyrouter" => {
            let mut cookies = Vec::new();
            if let Some(acw_cookie) = config
                .get("acw_cookie")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                cookies.push(acw_cookie.to_string());
            }
            if let Some(session_cookie) = credentials
                .get("session_cookie")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                let session =
                    admin_provider_ops_extract_cookie_value(session_cookie, "session");
                cookies.push(format!("session={session}"));
                if let Some(user_id) =
                    admin_provider_ops_anyrouter_parse_session_user_id(session_cookie)
                {
                    admin_provider_ops_insert_header(
                        &mut headers,
                        "New-Api-User",
                        user_id.trim(),
                    )?;
                }
            }
            if !cookies.is_empty() {
                admin_provider_ops_insert_header(&mut headers, "Cookie", &cookies.join("; "))?;
            }
        }
        _ => {}
    }
    Ok(headers)
}

async fn admin_provider_ops_local_verify_response(
    state: &AppState,
    base_url: &str,
    architecture_id: &str,
    config: &serde_json::Map<String, serde_json::Value>,
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    if architecture_id == "sub2api" {
        return admin_provider_ops_local_sub2api_verify_response(state, base_url, credentials).await;
    }

    let mut resolved_config = config.clone();
    if architecture_id == "anyrouter" {
        if let Some(acw_cookie) = admin_provider_ops_anyrouter_acw_cookie(state, base_url).await {
            resolved_config.insert("acw_cookie".to_string(), serde_json::Value::String(acw_cookie));
        }
    }

    let verify_path = match architecture_id {
        "anyrouter" => "/api/user/self",
        "cubence" => "/api/v1/dashboard/overview",
        "yescode" => "/api/v1/auth/profile",
        "nekocode" => "/api/user/self",
        "new_api" | "generic_api" => "/api/user/self",
        _ => return admin_provider_ops_verify_failure(ADMIN_PROVIDER_OPS_VERIFY_RUST_ONLY_MESSAGE),
    };
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return admin_provider_ops_verify_failure("请提供 API 地址");
    }

    let headers = match admin_provider_ops_verify_headers(architecture_id, &resolved_config, credentials) {
        Ok(headers) => headers,
        Err(message) => return admin_provider_ops_verify_failure(message),
    };

    let response = match state
        .client
        .get(format!("{base_url}{verify_path}"))
        .headers(headers)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_timeout() => return admin_provider_ops_verify_failure("连接超时"),
        Err(err) if err.is_connect() => {
            return admin_provider_ops_verify_failure(format!("连接失败: {err}"));
        }
        Err(err) => return admin_provider_ops_verify_failure(format!("验证失败: {err}")),
    };

    let status = response.status();
    let response_json = match response.bytes().await {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    };

    match architecture_id {
        "cubence" => admin_provider_ops_cubence_verify_payload(status, &response_json),
        "yescode" => admin_provider_ops_yescode_verify_payload(status, &response_json),
        "nekocode" => admin_provider_ops_nekocode_verify_payload(status, &response_json),
        _ => admin_provider_ops_generic_verify_payload(status, &response_json),
    }
}

fn admin_provider_ops_is_valid_action_type(action_type: &str) -> bool {
    matches!(
        action_type,
        "query_balance"
            | "checkin"
            | "claim_quota"
            | "refresh_token"
            | "get_usage"
            | "get_models"
            | "custom"
    )
}

fn admin_provider_ops_action_response(
    status: &str,
    action_type: &str,
    data: serde_json::Value,
    message: Option<String>,
    response_time_ms: Option<u64>,
    cache_ttl_seconds: u64,
) -> serde_json::Value {
    json!({
        "status": status,
        "action_type": action_type,
        "data": data,
        "message": message,
        "executed_at": chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "response_time_ms": response_time_ms,
        "cache_ttl_seconds": cache_ttl_seconds,
    })
}

fn admin_provider_ops_action_error(
    status: &str,
    action_type: &str,
    message: impl Into<String>,
    response_time_ms: Option<u64>,
) -> serde_json::Value {
    admin_provider_ops_action_response(
        status,
        action_type,
        serde_json::Value::Null,
        Some(message.into()),
        response_time_ms,
        0,
    )
}

fn admin_provider_ops_action_not_configured(
    action_type: &str,
    message: impl Into<String>,
) -> serde_json::Value {
    admin_provider_ops_action_error("not_configured", action_type, message, None)
}

fn admin_provider_ops_action_not_supported(
    action_type: &str,
    message: impl Into<String>,
) -> serde_json::Value {
    admin_provider_ops_action_error("not_supported", action_type, message, None)
}

fn admin_provider_ops_balance_data(
    total_granted: Option<f64>,
    total_used: Option<f64>,
    total_available: Option<f64>,
    currency: &str,
    extra: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    json!({
        "total_granted": total_granted,
        "total_used": total_used,
        "total_available": total_available,
        "expires_at": serde_json::Value::Null,
        "currency": currency,
        "extra": extra,
    })
}

fn admin_provider_ops_checkin_data(
    reward: Option<f64>,
    streak_days: Option<i64>,
    next_reward: Option<f64>,
    message: Option<String>,
    extra: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    json!({
        "reward": reward,
        "streak_days": streak_days,
        "next_reward": next_reward,
        "message": message,
        "extra": extra,
    })
}

fn admin_provider_ops_action_config_object<'a>(
    provider_ops_config: &'a serde_json::Map<String, serde_json::Value>,
    action_type: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    provider_ops_config
        .get("actions")
        .and_then(serde_json::Value::as_object)
        .and_then(|actions| actions.get(action_type))
        .and_then(serde_json::Value::as_object)
        .and_then(|action| action.get("config"))
        .and_then(serde_json::Value::as_object)
}

fn admin_provider_ops_default_action_config(
    architecture_id: &str,
    action_type: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let value = match (architecture_id, action_type) {
        ("generic_api", "query_balance") => {
            json!({ "endpoint": "/api/user/balance", "method": "GET" })
        }
        ("generic_api", "checkin") => json!({ "endpoint": "/api/user/checkin", "method": "POST" }),
        ("new_api", "query_balance") => json!({
            "endpoint": "/api/user/self",
            "method": "GET",
            "quota_divisor": 500000,
            "checkin_endpoint": "/api/user/checkin",
            "currency": "USD",
        }),
        ("new_api", "checkin") => json!({ "endpoint": "/api/user/checkin", "method": "POST" }),
        ("cubence", "query_balance") => {
            json!({ "endpoint": "/api/v1/dashboard/overview", "method": "GET", "currency": "USD" })
        }
        ("yescode", "query_balance") => {
            json!({ "endpoint": "/api/v1/user/balance", "method": "GET", "currency": "USD" })
        }
        ("nekocode", "query_balance") => {
            json!({ "endpoint": "/api/usage/summary", "method": "GET", "currency": "USD" })
        }
        _ => return None,
    };
    value.as_object().cloned()
}

fn admin_provider_ops_json_object_map(
    value: serde_json::Value,
) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn admin_provider_ops_resolved_action_config(
    architecture_id: &str,
    provider_ops_config: &serde_json::Map<String, serde_json::Value>,
    action_type: &str,
    request_config: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut resolved = admin_provider_ops_default_action_config(architecture_id, action_type)
        .unwrap_or_default();
    if let Some(saved) = admin_provider_ops_action_config_object(provider_ops_config, action_type) {
        for (key, value) in saved {
            resolved.insert(key.clone(), value.clone());
        }
    }
    if let Some(overrides) = request_config {
        for (key, value) in overrides {
            resolved.insert(key.clone(), value.clone());
        }
    }
    (!resolved.is_empty()).then_some(resolved)
}

fn admin_provider_ops_request_url(
    base_url: &str,
    action_config: &serde_json::Map<String, serde_json::Value>,
    default_endpoint: &str,
) -> String {
    let endpoint = action_config
        .get("endpoint")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_endpoint);
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("{}{}", base_url.trim_end_matches('/'), endpoint)
    }
}

fn admin_provider_ops_request_method(
    action_config: &serde_json::Map<String, serde_json::Value>,
    default_method: &str,
) -> reqwest::Method {
    action_config
        .get("method")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| reqwest::Method::from_bytes(value.trim().as_bytes()).ok())
        .unwrap_or_else(|| reqwest::Method::from_bytes(default_method.as_bytes()).unwrap_or(reqwest::Method::GET))
}

fn admin_provider_ops_message_contains_any(message: &str, indicators: &[&str]) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    indicators
        .iter()
        .any(|indicator| normalized.contains(&indicator.to_ascii_lowercase()))
}

fn admin_provider_ops_checkin_already_done(message: &str) -> bool {
    admin_provider_ops_message_contains_any(
        message,
        &["already", "已签到", "已经签到", "今日已签", "重复签到"],
    )
}

fn admin_provider_ops_checkin_auth_failure(message: &str) -> bool {
    admin_provider_ops_message_contains_any(
        message,
        &[
            "未登录",
            "请登录",
            "login",
            "unauthorized",
            "无权限",
            "权限不足",
            "turnstile",
            "captcha",
            "验证码",
        ],
    )
}

fn admin_provider_ops_checkin_payload(
    response_json: &serde_json::Value,
    fallback_message: Option<String>,
) -> serde_json::Value {
    let details = response_json
        .get("data")
        .and_then(serde_json::Value::as_object)
        .or_else(|| response_json.as_object());
    let reward = details.and_then(|value| {
        admin_provider_ops_value_as_f64(
            value.get("reward")
                .or_else(|| value.get("quota"))
                .or_else(|| value.get("amount")),
        )
    });
    let streak_days = details
        .and_then(|value| value.get("streak_days").or_else(|| value.get("streak")))
        .and_then(serde_json::Value::as_i64);
    let next_reward = details.and_then(|value| {
        admin_provider_ops_value_as_f64(value.get("next_reward").or_else(|| value.get("next")))
    });
    let message = fallback_message.or_else(|| {
        response_json
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
    });
    let mut extra = serde_json::Map::new();
    if let Some(details) = details {
        for (key, value) in details {
            if matches!(key.as_str(), "reward" | "quota" | "amount" | "streak_days" | "streak" | "next_reward" | "next" | "message") {
                continue;
            }
            extra.insert(key.clone(), value.clone());
        }
    }
    admin_provider_ops_checkin_data(reward, streak_days, next_reward, message, extra)
}

fn admin_provider_ops_parse_rfc3339_unix_secs(value: Option<&serde_json::Value>) -> Option<i64> {
    let raw = value?.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.timestamp())
}

fn admin_provider_ops_is_cookie_auth_architecture(architecture_id: &str) -> bool {
    matches!(architecture_id, "cubence" | "yescode" | "nekocode")
}

fn admin_provider_ops_should_use_rust_only_action_stub(
    architecture_id: &str,
    config: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    !matches!(
        architecture_id,
        "generic_api" | "new_api" | "cubence" | "yescode" | "nekocode"
    ) || admin_provider_ops_uses_python_verify_fallback(architecture_id, config)
}

async fn admin_provider_ops_probe_new_api_checkin(
    state: &AppState,
    base_url: &str,
    action_config: &serde_json::Map<String, serde_json::Value>,
    headers: &reqwest::header::HeaderMap,
    has_cookie: bool,
) -> Option<AdminProviderOpsCheckinOutcome> {
    let endpoint = action_config
        .get("checkin_endpoint")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("/api/user/checkin");
    let url = admin_provider_ops_request_url(
        base_url,
        &admin_provider_ops_json_object_map(json!({ "endpoint": endpoint })),
        endpoint,
    );
    let response = match state
        .client
        .request(reqwest::Method::POST, url)
        .headers(headers.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return None,
    };

    if response.status() == http::StatusCode::NOT_FOUND {
        return None;
    }
    if matches!(
        response.status(),
        http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
    ) {
        return has_cookie.then(|| AdminProviderOpsCheckinOutcome {
            success: None,
            message: "Cookie 已失效".to_string(),
            cookie_expired: true,
        });
    }

    let response_json = match response.bytes().await {
        Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| json!({})),
        Err(_) => json!({}),
    };
    let message = response_json
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Some(AdminProviderOpsCheckinOutcome {
            success: Some(true),
            message: if message.is_empty() {
                "签到成功".to_string()
            } else {
                message
            },
            cookie_expired: false,
        });
    }
    if admin_provider_ops_checkin_already_done(&message) {
        return Some(AdminProviderOpsCheckinOutcome {
            success: None,
            message: if message.is_empty() {
                "今日已签到".to_string()
            } else {
                message
            },
            cookie_expired: false,
        });
    }
    if admin_provider_ops_checkin_auth_failure(&message) {
        return has_cookie.then(|| AdminProviderOpsCheckinOutcome {
            success: None,
            message: if message.is_empty() {
                "Cookie 已失效".to_string()
            } else {
                message
            },
            cookie_expired: true,
        });
    }
    Some(AdminProviderOpsCheckinOutcome {
        success: Some(false),
        message: if message.is_empty() {
            "签到失败".to_string()
        } else {
            message
        },
        cookie_expired: false,
    })
}

async fn admin_provider_ops_run_checkin_action(
    state: &AppState,
    base_url: &str,
    architecture_id: &str,
    action_config: &serde_json::Map<String, serde_json::Value>,
    headers: &reqwest::header::HeaderMap,
    has_cookie: bool,
) -> serde_json::Value {
    let start = std::time::Instant::now();
    if !matches!(architecture_id, "generic_api" | "new_api") {
        return admin_provider_ops_action_not_supported(
            "checkin",
            ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE,
        );
    }

    let url = admin_provider_ops_request_url(base_url, action_config, "/api/user/checkin");
    let method = admin_provider_ops_request_method(action_config, "POST");
    let response = match state
        .client
        .request(method, url)
        .headers(headers.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_timeout() => {
            return admin_provider_ops_action_error("network_error", "checkin", "请求超时", None)
        }
        Err(err) => {
            return admin_provider_ops_action_error(
                "network_error",
                "checkin",
                format!("网络错误: {err}"),
                None,
            )
        }
    };
    let response_time_ms = Some(start.elapsed().as_millis() as u64);
    let status = response.status();
    let response_json = match response.bytes().await {
        Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return admin_provider_ops_action_error(
                    "parse_error",
                    "checkin",
                    "响应不是有效的 JSON",
                    response_time_ms,
                )
            }
        },
        Err(err) => {
            return admin_provider_ops_action_error(
                "network_error",
                "checkin",
                format!("网络错误: {err}"),
                response_time_ms,
            )
        }
    };

    if status == http::StatusCode::NOT_FOUND {
        return admin_provider_ops_action_error(
            "not_supported",
            "checkin",
            "功能未开放",
            response_time_ms,
        );
    }
    if status == http::StatusCode::TOO_MANY_REQUESTS {
        return admin_provider_ops_action_error(
            "rate_limited",
            "checkin",
            "请求频率限制",
            response_time_ms,
        );
    }
    if status == http::StatusCode::UNAUTHORIZED {
        return admin_provider_ops_action_error(
            if has_cookie { "auth_expired" } else { "auth_failed" },
            "checkin",
            if has_cookie {
                "Cookie 已失效，请重新配置"
            } else {
                "认证失败"
            },
            response_time_ms,
        );
    }
    if status == http::StatusCode::FORBIDDEN {
        return admin_provider_ops_action_error(
            if has_cookie { "auth_expired" } else { "auth_failed" },
            "checkin",
            if has_cookie {
                "Cookie 已失效或无权限"
            } else {
                "无权限访问"
            },
            response_time_ms,
        );
    }
    if status != http::StatusCode::OK {
        return admin_provider_ops_action_error(
            "unknown_error",
            "checkin",
            format!("HTTP {}: {}", status.as_u16(), status.canonical_reason().unwrap_or("Unknown")),
            response_time_ms,
        );
    }

    let message = response_json
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return admin_provider_ops_action_response(
            "success",
            "checkin",
            admin_provider_ops_checkin_payload(&response_json, Some(message)),
            None,
            response_time_ms,
            3600,
        );
    }
    if admin_provider_ops_checkin_already_done(&message) {
        return admin_provider_ops_action_response(
            "already_done",
            "checkin",
            admin_provider_ops_checkin_payload(&response_json, Some(message)),
            None,
            response_time_ms,
            3600,
        );
    }
    if admin_provider_ops_checkin_auth_failure(&message) {
        return admin_provider_ops_action_error(
            if has_cookie { "auth_expired" } else { "auth_failed" },
            "checkin",
            if message.is_empty() {
                if has_cookie {
                    "Cookie 已失效"
                } else {
                    "认证失败"
                }
            } else {
                message.as_str()
            },
            response_time_ms,
        );
    }
    admin_provider_ops_action_error(
        "unknown_error",
        "checkin",
        if message.is_empty() { "签到失败" } else { message.as_str() },
        response_time_ms,
    )
}

fn admin_provider_ops_new_api_balance_payload(
    action_config: &serde_json::Map<String, serde_json::Value>,
    response_json: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let user_data = if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && response_json
            .get("data")
            .is_some_and(serde_json::Value::is_object)
    {
        response_json.get("data")
    } else if response_json
        .get("success")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        return Err(
            response_json
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("业务状态码表示失败")
                .to_string(),
        );
    } else {
        Some(response_json)
    };
    let Some(user_data) = user_data.and_then(serde_json::Value::as_object) else {
        return Err("响应格式无效".to_string());
    };
    let quota_divisor = admin_provider_ops_value_as_f64(action_config.get("quota_divisor"))
        .filter(|value| *value > 0.0)
        .unwrap_or(500000.0);
    let total_available =
        admin_provider_ops_value_as_f64(user_data.get("quota")).map(|value| value / quota_divisor);
    let total_used = admin_provider_ops_value_as_f64(user_data.get("used_quota"))
        .map(|value| value / quota_divisor);
    Ok(admin_provider_ops_balance_data(
        None,
        total_used,
        total_available,
        action_config
            .get("currency")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("USD"),
        serde_json::Map::new(),
    ))
}

fn admin_provider_ops_cubence_balance_payload(
    action_config: &serde_json::Map<String, serde_json::Value>,
    response_json: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let response_data = response_json
        .get("data")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "响应格式无效".to_string())?;
    let balance_data = response_data
        .get("balance")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let subscription_limits = response_data
        .get("subscription_limits")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut extra = serde_json::Map::new();
    if let Some(five_hour) = subscription_limits
        .get("five_hour")
        .and_then(serde_json::Value::as_object)
    {
        extra.insert(
            "five_hour_limit".to_string(),
            json!({
                "limit": five_hour.get("limit"),
                "used": five_hour.get("used"),
                "remaining": five_hour.get("remaining"),
                "resets_at": five_hour.get("resets_at"),
            }),
        );
    }
    if let Some(weekly) = subscription_limits
        .get("weekly")
        .and_then(serde_json::Value::as_object)
    {
        extra.insert(
            "weekly_limit".to_string(),
            json!({
                "limit": weekly.get("limit"),
                "used": weekly.get("used"),
                "remaining": weekly.get("remaining"),
                "resets_at": weekly.get("resets_at"),
            }),
        );
    }
    for key in [
        "normal_balance_dollar",
        "subscription_balance_dollar",
        "charity_balance_dollar",
    ] {
        if let Some(value) = balance_data.get(key) {
            extra.insert(
                key.trim_end_matches("_dollar").replace("_dollar", ""),
                value.clone(),
            );
        }
    }
    if let Some(value) = balance_data.get("normal_balance_dollar") {
        extra.insert("normal_balance".to_string(), value.clone());
    }
    if let Some(value) = balance_data.get("subscription_balance_dollar") {
        extra.insert("subscription_balance".to_string(), value.clone());
    }
    if let Some(value) = balance_data.get("charity_balance_dollar") {
        extra.insert("charity_balance".to_string(), value.clone());
    }
    Ok(admin_provider_ops_balance_data(
        None,
        None,
        admin_provider_ops_value_as_f64(balance_data.get("total_balance_dollar")),
        action_config
            .get("currency")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("USD"),
        extra,
    ))
}

fn admin_provider_ops_yescode_balance_extra(
    combined_data: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let pay_as_you_go =
        admin_provider_ops_value_as_f64(combined_data.get("pay_as_you_go_balance")).unwrap_or(0.0);
    let subscription =
        admin_provider_ops_value_as_f64(combined_data.get("subscription_balance")).unwrap_or(0.0);
    let plan = combined_data
        .get("subscription_plan")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let daily_balance = admin_provider_ops_value_as_f64(plan.get("daily_balance")).unwrap_or(subscription);
    let weekly_limit = admin_provider_ops_value_as_f64(
        combined_data
            .get("weekly_limit")
            .or_else(|| plan.get("weekly_limit")),
    );
    let weekly_spent =
        admin_provider_ops_value_as_f64(combined_data.get("weekly_spent_balance")).unwrap_or(0.0);
    let subscription_available = weekly_limit
        .map(|limit| (limit - weekly_spent).max(0.0).min(subscription))
        .unwrap_or(subscription);

    let mut extra = serde_json::Map::new();
    extra.insert("pay_as_you_go_balance".to_string(), json!(pay_as_you_go));
    extra.insert("daily_limit".to_string(), json!(daily_balance));
    if let Some(limit) = weekly_limit {
        extra.insert("weekly_limit".to_string(), json!(limit));
    }
    extra.insert("weekly_spent".to_string(), json!(weekly_spent));
    if let Some(last_week_reset) =
        admin_provider_ops_parse_rfc3339_unix_secs(combined_data.get("last_week_reset"))
    {
        extra.insert(
            "weekly_resets_at".to_string(),
            json!(last_week_reset + 7 * 24 * 3600),
        );
    }
    if let Some(last_daily_add) =
        admin_provider_ops_parse_rfc3339_unix_secs(combined_data.get("last_daily_balance_add"))
    {
        extra.insert("daily_resets_at".to_string(), json!(last_daily_add + 24 * 3600));
    }
    let daily_spent = if let Some(limit) = weekly_limit {
        daily_balance - daily_balance.min(subscription_available.min(limit.max(0.0)))
    } else {
        (daily_balance - subscription).max(0.0)
    };
    extra.insert("daily_spent".to_string(), json!(daily_spent));
    extra.insert("_subscription_available".to_string(), json!(subscription_available));
    extra.insert(
        "_total_available".to_string(),
        json!(pay_as_you_go + subscription_available),
    );
    extra
}

async fn admin_provider_ops_yescode_balance_payload(
    state: &AppState,
    base_url: &str,
    headers: &reqwest::header::HeaderMap,
    action_config: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let start = std::time::Instant::now();
    let balance_url = format!("{}/api/v1/user/balance", base_url.trim_end_matches('/'));
    let profile_url = format!("{}/api/v1/auth/profile", base_url.trim_end_matches('/'));
    let balance_future = state
        .client
        .request(reqwest::Method::GET, balance_url)
        .headers(headers.clone())
        .send();
    let profile_future = state
        .client
        .request(reqwest::Method::GET, profile_url)
        .headers(headers.clone())
        .send();
    let (balance_result, profile_result) = tokio::join!(balance_future, profile_future);
    let response_time_ms = Some(start.elapsed().as_millis() as u64);

    let mut combined = serde_json::Map::new();
    let mut has_any = false;

    if let Ok(balance_response) = balance_result {
        if balance_response.status() == http::StatusCode::OK {
            if let Ok(bytes) = balance_response.bytes().await {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(object) = value.as_object() {
                        has_any = true;
                        combined.insert(
                            "_balance_data".to_string(),
                            serde_json::Value::Object(object.clone()),
                        );
                        combined.insert(
                            "pay_as_you_go_balance".to_string(),
                            object
                                .get("pay_as_you_go_balance")
                                .cloned()
                                .unwrap_or_else(|| json!(0)),
                        );
                        combined.insert(
                            "subscription_balance".to_string(),
                            object
                                .get("subscription_balance")
                                .cloned()
                                .unwrap_or_else(|| json!(0)),
                        );
                        if let Some(limit) = object.get("weekly_limit") {
                            combined.insert("weekly_limit".to_string(), limit.clone());
                        }
                        combined.insert(
                            "weekly_spent_balance".to_string(),
                            object
                                .get("weekly_spent_balance")
                                .cloned()
                                .unwrap_or_else(|| json!(0)),
                        );
                    }
                }
            }
        }
    }

    if let Ok(profile_response) = profile_result {
        if profile_response.status() == http::StatusCode::OK {
            if let Ok(bytes) = profile_response.bytes().await {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(object) = value.as_object() {
                        has_any = true;
                        combined.insert(
                            "_profile_data".to_string(),
                            serde_json::Value::Object(object.clone()),
                        );
                        for key in [
                            "username",
                            "email",
                            "last_week_reset",
                            "last_daily_balance_add",
                            "subscription_plan",
                        ] {
                            if let Some(value) = object.get(key) {
                                combined.insert(key.to_string(), value.clone());
                            }
                        }
                        combined
                            .entry("pay_as_you_go_balance".to_string())
                            .or_insert_with(|| {
                                object
                                    .get("pay_as_you_go_balance")
                                    .cloned()
                                    .unwrap_or_else(|| json!(0))
                            });
                        combined
                            .entry("subscription_balance".to_string())
                            .or_insert_with(|| {
                                object
                                    .get("subscription_balance")
                                    .cloned()
                                    .unwrap_or_else(|| json!(0))
                            });
                        combined.entry("weekly_spent_balance".to_string()).or_insert_with(|| {
                            object
                                .get("current_week_spend")
                                .cloned()
                                .unwrap_or_else(|| json!(0))
                        });
                        if !combined.contains_key("weekly_limit") {
                            if let Some(limit) = object
                                .get("subscription_plan")
                                .and_then(serde_json::Value::as_object)
                                .and_then(|plan| plan.get("weekly_limit"))
                            {
                                combined.insert("weekly_limit".to_string(), limit.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    if !has_any {
        return admin_provider_ops_action_error(
            "auth_failed",
            "query_balance",
            "Cookie 已失效，请重新配置",
            response_time_ms,
        );
    }

    let mut extra = admin_provider_ops_yescode_balance_extra(&combined);
    let total_available = admin_provider_ops_value_as_f64(extra.get("_total_available"));
    extra.remove("_subscription_available");
    extra.remove("_total_available");
    admin_provider_ops_action_response(
        "success",
        "query_balance",
        admin_provider_ops_balance_data(
            None,
            None,
            total_available,
            action_config
                .get("currency")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("USD"),
            extra,
        ),
        None,
        response_time_ms,
        86400,
    )
}

fn admin_provider_ops_nekocode_balance_payload(
    response_json: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let response_data = response_json
        .get("data")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "响应格式无效".to_string())?;
    let subscription = response_data
        .get("subscription")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let balance = admin_provider_ops_value_as_f64(response_data.get("balance"));
    let daily_quota_limit =
        admin_provider_ops_value_as_f64(subscription.get("daily_quota_limit"));
    let daily_remaining_quota =
        admin_provider_ops_value_as_f64(subscription.get("daily_remaining_quota"));
    let daily_used = match (daily_quota_limit, daily_remaining_quota) {
        (Some(limit), Some(remaining)) => Some(limit - remaining),
        _ => None,
    };
    let mut extra = serde_json::Map::new();
    for key in [
        "plan_name",
        "status",
        "daily_quota_limit",
        "daily_remaining_quota",
        "effective_start_date",
        "effective_end_date",
    ] {
        if let Some(value) = subscription.get(key) {
            extra.insert(
                match key {
                    "status" => "subscription_status",
                    other => other,
                }
                .to_string(),
                value.clone(),
            );
        }
    }
    if let Some(value) = daily_used {
        extra.insert("daily_used_quota".to_string(), json!(value));
    }
    if let Some(month_data) = response_data.get("month").and_then(serde_json::Value::as_object) {
        extra.insert(
            "month_stats".to_string(),
            json!({
                "total_input_tokens": month_data.get("total_input_tokens"),
                "total_output_tokens": month_data.get("total_output_tokens"),
                "total_quota": month_data.get("total_quota"),
                "total_requests": month_data.get("total_requests"),
            }),
        );
    }
    if let Some(today_data) = response_data.get("today").and_then(serde_json::Value::as_object) {
        if let Some(stats) = today_data.get("stats") {
            extra.insert("today_stats".to_string(), stats.clone());
        }
    }
    Ok(admin_provider_ops_balance_data(
        daily_quota_limit,
        daily_used,
        balance,
        "USD",
        extra,
    ))
}

fn admin_provider_ops_attach_balance_checkin_outcome(
    action_payload: &mut serde_json::Value,
    outcome: &AdminProviderOpsCheckinOutcome,
) {
    if let Some(data) = action_payload.get_mut("data").and_then(serde_json::Value::as_object_mut) {
        let extra = data
            .entry("extra".to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if let Some(extra) = extra.as_object_mut() {
            if outcome.cookie_expired {
                extra.insert("cookie_expired".to_string(), serde_json::Value::Bool(true));
                extra.insert(
                    "cookie_expired_message".to_string(),
                    serde_json::Value::String(outcome.message.clone()),
                );
            } else {
                extra.insert(
                    "checkin_success".to_string(),
                    outcome
                        .success
                        .map(serde_json::Value::Bool)
                        .unwrap_or(serde_json::Value::Null),
                );
                extra.insert(
                    "checkin_message".to_string(),
                    serde_json::Value::String(outcome.message.clone()),
                );
            }
        }
    }
    if outcome.cookie_expired {
        if let Some(object) = action_payload.as_object_mut() {
            object.insert("status".to_string(), json!("auth_expired"));
        }
    }
}

async fn admin_provider_ops_run_query_balance_action(
    state: &AppState,
    base_url: &str,
    architecture_id: &str,
    connector_config: &serde_json::Map<String, serde_json::Value>,
    action_config: &serde_json::Map<String, serde_json::Value>,
    headers: &reqwest::header::HeaderMap,
    credentials: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    if architecture_id == "yescode" {
        return admin_provider_ops_yescode_balance_payload(state, base_url, headers, action_config)
            .await;
    }

    let mut balance_checkin = None;
    if matches!(architecture_id, "generic_api" | "new_api") {
        let has_cookie = credentials
            .get("cookie")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty());
        balance_checkin = admin_provider_ops_probe_new_api_checkin(
            state,
            base_url,
            action_config,
            headers,
            has_cookie,
        )
        .await;
    }

    let start = std::time::Instant::now();
    let url = admin_provider_ops_request_url(base_url, action_config, "/api/user/balance");
    let method = admin_provider_ops_request_method(action_config, "GET");
    let response = match state
        .client
        .request(method, url)
        .headers(headers.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) if err.is_timeout() => {
            return admin_provider_ops_action_error(
                "network_error",
                "query_balance",
                "请求超时",
                None,
            )
        }
        Err(err) => {
            return admin_provider_ops_action_error(
                "network_error",
                "query_balance",
                format!("网络错误: {err}"),
                None,
            )
        }
    };
    let response_time_ms = Some(start.elapsed().as_millis() as u64);
    let status = response.status();
    let response_json = match response.bytes().await {
        Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return admin_provider_ops_action_error(
                    "parse_error",
                    "query_balance",
                    "响应不是有效的 JSON",
                    response_time_ms,
                )
            }
        },
        Err(err) => {
            return admin_provider_ops_action_error(
                "network_error",
                "query_balance",
                format!("网络错误: {err}"),
                response_time_ms,
            )
        }
    };

    if status != http::StatusCode::OK {
        let cookie_auth = admin_provider_ops_is_cookie_auth_architecture(architecture_id);
        let payload = match status {
            http::StatusCode::UNAUTHORIZED => admin_provider_ops_action_error(
                "auth_failed",
                "query_balance",
                if cookie_auth {
                    "Cookie 已失效，请重新配置"
                } else {
                    "认证失败"
                },
                response_time_ms,
            ),
            http::StatusCode::FORBIDDEN => admin_provider_ops_action_error(
                "auth_failed",
                "query_balance",
                if cookie_auth {
                    "Cookie 已失效或无权限"
                } else {
                    "无权限访问"
                },
                response_time_ms,
            ),
            http::StatusCode::NOT_FOUND => admin_provider_ops_action_error(
                "not_supported",
                "query_balance",
                "功能未开放",
                response_time_ms,
            ),
            http::StatusCode::TOO_MANY_REQUESTS => admin_provider_ops_action_error(
                "rate_limited",
                "query_balance",
                "请求频率限制",
                response_time_ms,
            ),
            _ => admin_provider_ops_action_error(
                "unknown_error",
                "query_balance",
                format!(
                    "HTTP {}: {}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown")
                ),
                response_time_ms,
            ),
        };
        return payload;
    }

    let data = match architecture_id {
        "generic_api" | "new_api" => {
            match admin_provider_ops_new_api_balance_payload(action_config, &response_json) {
                Ok(data) => data,
                Err(message) => {
                    return admin_provider_ops_action_error(
                        "unknown_error",
                        "query_balance",
                        message,
                        response_time_ms,
                    )
                }
            }
        }
        "cubence" => match admin_provider_ops_cubence_balance_payload(action_config, &response_json) {
            Ok(data) => data,
            Err(message) => {
                return admin_provider_ops_action_error(
                    "parse_error",
                    "query_balance",
                    message,
                    response_time_ms,
                )
            }
        },
        "nekocode" => match admin_provider_ops_nekocode_balance_payload(&response_json) {
            Ok(data) => data,
            Err(message) => {
                return admin_provider_ops_action_error(
                    "parse_error",
                    "query_balance",
                    message,
                    response_time_ms,
                )
            }
        },
        _ => {
            return admin_provider_ops_action_not_supported(
                "query_balance",
                ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE,
            )
        }
    };

    let mut payload = admin_provider_ops_action_response(
        "success",
        "query_balance",
        data,
        None,
        response_time_ms,
        86400,
    );
    if let Some(outcome) = balance_checkin.as_ref() {
        admin_provider_ops_attach_balance_checkin_outcome(&mut payload, outcome);
    }
    let _ = connector_config;
    payload
}

pub(crate) async fn admin_provider_ops_local_action_response(
    state: &AppState,
    _provider_id: &str,
    provider: Option<&StoredProviderCatalogProvider>,
    endpoints: &[StoredProviderCatalogEndpoint],
    action_type: &str,
    request_config: Option<&serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    let Some(provider) = provider else {
        return admin_provider_ops_action_not_configured(action_type, "未配置操作设置");
    };
    let Some(provider_ops_config) = admin_provider_ops_config_object(provider) else {
        return admin_provider_ops_action_not_configured(action_type, "未配置操作设置");
    };
    let architecture_id = provider_ops_config
        .get("architecture_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("generic_api");
    let connector_config = admin_provider_ops_connector_object(provider_ops_config)
        .and_then(|connector| connector.get("config"))
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if admin_provider_ops_should_use_rust_only_action_stub(architecture_id, &connector_config) {
        return admin_provider_ops_action_not_supported(
            action_type,
            ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE,
        );
    }

    let Some(base_url) =
        resolve_admin_provider_ops_base_url(provider, endpoints, Some(provider_ops_config))
    else {
        return admin_provider_ops_action_not_configured(action_type, "Provider 未配置 base_url");
    };
    let credentials = admin_provider_ops_decrypted_credentials(
        state,
        admin_provider_ops_config_object(provider)
            .and_then(admin_provider_ops_connector_object)
            .and_then(|connector| connector.get("credentials")),
    );
    let headers = match admin_provider_ops_verify_headers(architecture_id, &connector_config, &credentials) {
        Ok(headers) => headers,
        Err(message) => return admin_provider_ops_action_not_configured(action_type, message),
    };
    let Some(action_config) = admin_provider_ops_resolved_action_config(
        architecture_id,
        provider_ops_config,
        action_type,
        request_config,
    ) else {
        return admin_provider_ops_action_not_supported(
            action_type,
            ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE,
        );
    };

    match action_type {
        "query_balance" => {
            admin_provider_ops_run_query_balance_action(
                state,
                &base_url,
                architecture_id,
                &connector_config,
                &action_config,
                &headers,
                &credentials,
            )
            .await
        }
        "checkin" => {
            let has_cookie = credentials
                .get("cookie")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());
            admin_provider_ops_run_checkin_action(
                state,
                &base_url,
                architecture_id,
                &action_config,
                &headers,
                has_cookie,
            )
            .await
        }
        _ => admin_provider_ops_action_not_supported(
            action_type,
            ADMIN_PROVIDER_OPS_ACTION_RUST_ONLY_MESSAGE,
        ),
    }
}

async fn maybe_build_local_admin_provider_ops_providers_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };
    if decision.route_family.as_deref() != Some("provider_ops_manage") {
        return Ok(None);
    }

    let route_kind = decision.route_kind.as_deref().unwrap_or_default();
    if !state.has_provider_catalog_data_reader() && route_kind != "disconnect_provider" {
        return Ok(None);
    }
    if route_kind == "batch_balance" {
        let requested_provider_ids = match request_body {
            Some(body) if !body.is_empty() => {
                let raw_value = match serde_json::from_slice::<serde_json::Value>(body) {
                    Ok(value) => value,
                    Err(_) => {
                        return Ok(Some(
                            (
                                http::StatusCode::BAD_REQUEST,
                                Json(json!({ "detail": "请求体必须是 provider_id 数组" })),
                            )
                                .into_response(),
                        ));
                    }
                };
                let ids = if let Some(items) = raw_value.as_array() {
                    items.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                } else if let Some(items) = raw_value
                    .get("provider_ids")
                    .and_then(serde_json::Value::as_array)
                {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                } else {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": "请求体必须是 provider_id 数组" })),
                        )
                            .into_response(),
                    ));
                };
                Some(ids)
            }
            _ => None,
        };

        let provider_ids = if let Some(provider_ids) = requested_provider_ids {
            provider_ids
        } else {
            state
                .list_provider_catalog_providers(true)
                .await?
                .into_iter()
                .filter(|provider| {
                    provider
                        .config
                        .as_ref()
                        .and_then(serde_json::Value::as_object)
                        .is_some_and(|config| config.contains_key("provider_ops"))
                })
                .map(|provider| provider.id)
                .collect::<Vec<_>>()
        };

        if provider_ids.is_empty() {
            return Ok(Some(Json(json!({})).into_response()));
        }

        let providers = state
            .read_provider_catalog_providers_by_ids(&provider_ids)
            .await?;
        let endpoints = state
            .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
            .await?;
        let mut payload = serde_json::Map::new();
        for provider_id in &provider_ids {
            let provider = providers.iter().find(|provider| provider.id == *provider_id);
            let provider_endpoints = endpoints
                .iter()
                .filter(|endpoint| endpoint.provider_id == *provider_id)
                .cloned()
                .collect::<Vec<_>>();
            let result = admin_provider_ops_local_action_response(
                state,
                provider_id,
                provider,
                &provider_endpoints,
                "query_balance",
                None,
            )
            .await;
            payload.insert(provider_id.clone(), result);
        }
        return Ok(Some(Json(serde_json::Value::Object(payload)).into_response()));
    }

    let action_route = if route_kind == "execute_provider_action" {
        admin_provider_ops_action_route_parts(&request_context.request_path)
    } else {
        None
    };
    let provider_id = if matches!(
        route_kind,
        "get_provider_status"
            | "get_provider_config"
            | "save_provider_config"
            | "delete_provider_config"
            | "verify_provider"
            | "connect_provider"
            | "disconnect_provider"
            | "get_provider_balance"
            | "refresh_provider_balance"
            | "provider_checkin"
            | "execute_provider_action"
    ) {
        admin_provider_id_for_provider_ops_config(&request_context.request_path)
            .or_else(|| admin_provider_id_for_provider_ops_status(&request_context.request_path))
            .or_else(|| admin_provider_id_for_provider_ops_verify(&request_context.request_path))
            .or_else(|| admin_provider_id_for_provider_ops_connect(&request_context.request_path))
            .or_else(|| admin_provider_id_for_provider_ops_balance(&request_context.request_path))
            .or_else(|| admin_provider_id_for_provider_ops_checkin(&request_context.request_path))
            .or_else(|| action_route.as_ref().map(|(provider_id, _)| provider_id.clone()))
            .or_else(|| {
                admin_provider_id_for_provider_ops_disconnect(&request_context.request_path)
            })
    } else {
        None
    };
    let Some(provider_id) = provider_id else {
        return Ok(None);
    };

    if decision.route_kind.as_deref() != Some(route_kind) {
        return Ok(None);
    }

    if route_kind == "save_provider_config" {
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体不能为空" })),
                )
                    .into_response(),
            ));
        };
        let raw_value = match serde_json::from_slice::<serde_json::Value>(request_body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };
        if !raw_value.is_object() {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                )
                    .into_response(),
            ));
        }
        let payload = match serde_json::from_value::<AdminProviderOpsSaveConfigRequest>(raw_value) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };
        let Some(existing_provider) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider 不存在" })),
                )
                    .into_response(),
            ));
        };
        let provider_ops_config =
            match build_admin_provider_ops_saved_config_value(state, &existing_provider, payload) {
                Ok(config) => config,
                Err(detail) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": detail })),
                        )
                            .into_response(),
                    ));
                }
            };
        let mut updated_provider = existing_provider.clone();
        let mut provider_config = updated_provider
            .config
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        provider_config.insert("provider_ops".to_string(), provider_ops_config);
        updated_provider.config = Some(serde_json::Value::Object(provider_config));
        updated_provider.updated_at_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs());
        let Some(_updated) = state.update_provider_catalog_provider(&updated_provider).await? else {
            return Ok(None);
        };
        return Ok(Some(
            Json(json!({
                "success": true,
                "message": "配置保存成功",
            }))
            .into_response(),
        ));
    }

    if route_kind == "verify_provider" {
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体不能为空" })),
                )
                    .into_response(),
            ));
        };
        let raw_value = match serde_json::from_slice::<serde_json::Value>(request_body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };
        if !raw_value.is_object() {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                )
                    .into_response(),
            ));
        }
        let payload = match serde_json::from_value::<AdminProviderOpsSaveConfigRequest>(raw_value) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };

        let existing_provider = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next();
        let endpoints = if existing_provider.is_some() {
            state
                .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
                .await?
        } else {
            Vec::new()
        };
        let base_url = payload
            .base_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                existing_provider.as_ref().and_then(|provider| {
                    resolve_admin_provider_ops_base_url(
                        provider,
                        &endpoints,
                        admin_provider_ops_config_object(provider),
                    )
                })
            });
        let Some(base_url) = base_url else {
            return Ok(Some(
                Json(admin_provider_ops_verify_failure("请提供 API 地址")).into_response(),
            ));
        };

        let architecture_id =
            admin_provider_ops_normalized_verify_architecture_id(&payload.architecture_id);

        let credentials = existing_provider.as_ref().map_or_else(
            || payload.connector.credentials.clone(),
            |provider| {
                admin_provider_ops_merge_credentials(
                    state,
                    provider,
                    payload.connector.credentials.clone(),
                )
            },
        );
        let payload = admin_provider_ops_local_verify_response(
            state,
            &base_url,
            architecture_id,
            &payload.connector.config,
            &credentials,
        )
        .await;
        return Ok(Some(Json(payload).into_response()));
    }

    if route_kind == "connect_provider" {
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体不能为空" })),
                )
                    .into_response(),
            ));
        };
        let raw_value = match serde_json::from_slice::<serde_json::Value>(request_body) {
            Ok(value) => value,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };
        if !raw_value.is_object() {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                )
                    .into_response(),
            ));
        }
        let payload = match serde_json::from_value::<AdminProviderOpsConnectRequest>(raw_value) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        };

        let Some(existing_provider) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "Provider 不存在" })),
                )
                    .into_response(),
            ));
        };
        let Some(provider_ops_config) = admin_provider_ops_config_object(&existing_provider) else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "未配置操作设置" })),
                )
                    .into_response(),
            ));
        };
        let endpoints = state
            .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
            .await?;
        if resolve_admin_provider_ops_base_url(
            &existing_provider,
            &endpoints,
            Some(provider_ops_config),
        )
        .is_none()
        {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "Provider 未配置 base_url" })),
                )
                    .into_response(),
            ));
        }

        let actual_credentials = payload.credentials.filter(|value| !value.is_empty()).unwrap_or_else(|| {
            admin_provider_ops_decrypted_credentials(
                state,
                admin_provider_ops_config_object(&existing_provider)
                    .and_then(admin_provider_ops_connector_object)
                    .and_then(|connector| connector.get("credentials")),
            )
        });
        if actual_credentials.is_empty() {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "未提供凭据" })),
                )
                    .into_response(),
            ));
        }

        return Ok(Some(
            (
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": ADMIN_PROVIDER_OPS_CONNECT_RUST_ONLY_MESSAGE })),
            )
                .into_response(),
        ));
    }

    if matches!(
        route_kind,
        "get_provider_balance"
            | "refresh_provider_balance"
            | "provider_checkin"
            | "execute_provider_action"
    ) {
        let action_type = if route_kind == "provider_checkin" {
            "checkin".to_string()
        } else if matches!(route_kind, "get_provider_balance" | "refresh_provider_balance") {
            "query_balance".to_string()
        } else {
            let Some((_, action_type)) = action_route else {
                return Ok(None);
            };
            if !admin_provider_ops_is_valid_action_type(&action_type) {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": format!("无效的操作类型: {action_type}") })),
                    )
                        .into_response(),
                ));
            }
            action_type
        };

        let request_config = if route_kind == "execute_provider_action" {
            match request_body {
                Some(body) if !body.is_empty() => {
                    let raw_value = match serde_json::from_slice::<serde_json::Value>(body) {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(Some(
                                (
                                    http::StatusCode::BAD_REQUEST,
                                    Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                                )
                                    .into_response(),
                            ));
                        }
                    };
                    let payload =
                        match serde_json::from_value::<AdminProviderOpsExecuteActionRequest>(
                            raw_value,
                        ) {
                            Ok(payload) => payload,
                            Err(_) => {
                                return Ok(Some(
                                    (
                                        http::StatusCode::BAD_REQUEST,
                                        Json(
                                            json!({ "detail": "请求体必须是合法的 JSON 对象" }),
                                        ),
                                    )
                                        .into_response(),
                                ));
                            }
                        };
                    payload.config
                }
                _ => None,
            }
        } else {
            None
        };

        let providers = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?;
        let provider = providers.first();
        let endpoints = if provider.is_some() {
            state
                .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
                .await?
        } else {
            Vec::new()
        };
        let payload = admin_provider_ops_local_action_response(
            state,
            &provider_id,
            provider,
            &endpoints,
            &action_type,
            request_config.as_ref(),
        )
        .await;
        return Ok(Some(Json(payload).into_response()));
    }

    if route_kind == "delete_provider_config" {
        let Some(existing_provider) = state
            .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider 不存在" })),
                )
                    .into_response(),
            ));
        };
        let mut updated_provider = existing_provider.clone();
        let mut provider_config = updated_provider
            .config
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        if provider_config.remove("provider_ops").is_some() {
            updated_provider.config = Some(serde_json::Value::Object(provider_config));
            updated_provider.updated_at_unix_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs());
            let Some(_updated) = state.update_provider_catalog_provider(&updated_provider).await?
            else {
                return Ok(None);
            };
        }
        return Ok(Some(
            Json(json!({
                "success": true,
                "message": "配置已删除",
            }))
            .into_response(),
        ));
    }

    if route_kind == "disconnect_provider" {
        return Ok(Some(
            Json(json!({
                "success": true,
                "message": "已断开连接",
            }))
            .into_response(),
        ));
    }

    let providers = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?;
    let provider = providers.first();
    let endpoints = if route_kind == "get_provider_config" && provider.is_some() {
        state
            .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
            .await?
    } else {
        Vec::new()
    };

    let payload = if route_kind == "get_provider_status" {
        build_admin_provider_ops_status_payload(&provider_id, provider)
    } else {
        build_admin_provider_ops_config_payload(state, &provider_id, provider, &endpoints)
    };

    Ok(Some(Json(payload).into_response()))
}
