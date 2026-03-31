const USERS_ME_MAINTENANCE_DETAIL: &str = "User self-service routes require Rust maintenance backend";
const USERS_ME_AVAILABLE_MODELS_FETCH_LIMIT: usize = 1000;
const USERS_ME_MANAGEMENT_TOKEN_PREFIX: &str = "ae_";
const USERS_ME_MANAGEMENT_TOKEN_RANDOM_LENGTH: usize = 40;
const USERS_ME_MANAGEMENT_TOKEN_DISPLAY_PREFIX_LEN: usize = 7;
const USERS_ME_MANAGEMENT_TOKEN_FETCH_LIMIT: usize = 10_000;
const USERS_ME_MANAGEMENT_TOKEN_DEFAULT_MAX_PER_USER: usize = 20;
const USERS_ME_MANAGEMENT_TOKEN_MAX_PER_USER_ENV: &str = "MANAGEMENT_TOKEN_MAX_PER_USER";

#[derive(Debug, Clone)]
struct UsersMeManagementTokenCreateInput {
    name: String,
    description: Option<String>,
    allowed_ips: Option<serde_json::Value>,
    expires_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct UsersMeManagementTokenUpdateInput {
    name: Option<String>,
    description: Option<String>,
    clear_description: bool,
    allowed_ips: Option<serde_json::Value>,
    clear_allowed_ips: bool,
    expires_at_unix_secs: Option<u64>,
    clear_expires_at: bool,
}

impl UsersMeManagementTokenUpdateInput {
    fn is_noop(&self) -> bool {
        self.name.is_none()
            && self.description.is_none()
            && !self.clear_description
            && self.allowed_ips.is_none()
            && !self.clear_allowed_ips
            && self.expires_at_unix_secs.is_none()
            && !self.clear_expires_at
    }
}

#[derive(Debug, Deserialize)]
struct UsersMeUpdateProfileRequest {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UsersMeChangePasswordRequest {
    #[serde(default, alias = "current_password")]
    old_password: Option<String>,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct UsersMeUpdateSessionLabelRequest {
    device_label: String,
}

#[derive(Debug, Deserialize)]
struct UsersMeCreateApiKeyRequest {
    name: String,
    #[serde(default)]
    rate_limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct UsersMeUpdateApiKeyRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    rate_limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct UsersMePatchApiKeyRequest {
    #[serde(default)]
    is_active: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum UsersMeApiKeyProviderValue {
    ProviderId(String),
    ProviderConfig {
        provider_id: String,
        #[serde(default)]
        priority: Option<i32>,
        #[serde(default)]
        weight: Option<f64>,
        #[serde(default)]
        enabled: Option<bool>,
    },
}

#[derive(Debug, Deserialize)]
struct UsersMeUpdateApiKeyProvidersRequest {
    #[serde(default)]
    allowed_providers: Option<Vec<UsersMeApiKeyProviderValue>>,
    #[serde(default)]
    providers: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct UsersMeUpdateApiKeyCapabilitiesRequest {
    #[serde(default)]
    force_capabilities: Option<serde_json::Value>,
    #[serde(default)]
    capabilities: Option<Vec<String>>,
}

fn build_users_me_available_model_payload(
    model: aether_data::repository::global_models::StoredPublicGlobalModel,
) -> serde_json::Value {
    json!({
        "id": model.id,
        "name": model.name,
        "display_name": model.display_name,
        "is_active": model.is_active,
        "default_price_per_request": model.default_price_per_request,
        "default_tiered_pricing": model.default_tiered_pricing,
        "supported_capabilities": model.supported_capabilities,
        "config": model.config,
        "usage_count": model.usage_count,
    })
}

fn user_configurable_capability_names() -> BTreeSet<&'static str> {
    PUBLIC_CAPABILITY_DEFINITIONS
        .iter()
        .filter(|capability| capability.config_mode == "user_configurable")
        .map(|capability| capability.name)
        .collect()
}

fn known_capability_names() -> BTreeSet<&'static str> {
    PUBLIC_CAPABILITY_DEFINITIONS
        .iter()
        .map(|capability| capability.name)
        .collect()
}

fn normalize_user_model_capability_settings_input(
    value: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    match value {
        Some(serde_json::Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

fn validate_user_model_capability_settings(
    value: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let Some(value) = normalize_user_model_capability_settings_input(value) else {
        return Ok(None);
    };
    let Some(settings) = value.as_object() else {
        return Err("model_capability_settings 必须是对象类型".to_string());
    };

    let user_configurable = user_configurable_capability_names();
    let known_capabilities = known_capability_names();
    for (model_name, capabilities) in settings {
        let Some(capabilities) = capabilities.as_object() else {
            return Err(format!("模型 {model_name} 的能力配置必须是对象类型"));
        };
        for (capability_name, capability_value) in capabilities {
            if !known_capabilities.contains(capability_name.as_str()) {
                return Err(format!("未知的能力类型: {capability_name}"));
            }
            if !user_configurable.contains(capability_name.as_str()) {
                return Err(format!("能力 {capability_name} 不支持用户配置"));
            }
            if !capability_value.is_boolean() {
                return Err(format!("能力 {capability_name} 的值必须是布尔类型"));
            }
        }
    }

    Ok(Some(value))
}

fn parse_users_me_available_models_query(
    query: Option<&str>,
) -> (usize, usize, Option<String>) {
    let skip = query_param_value(query, "skip")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let limit = query_param_value(query, "limit")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=1000).contains(value))
        .unwrap_or(100);
    let search = query_param_value(query, "search");
    (skip, limit, search)
}

fn users_me_session_id_from_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/users/me/sessions/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn users_me_api_key_id_from_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/users/me/api-keys/")?
        .trim()
        .trim_matches('/')
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| !value.contains('/'))
        .map(ToOwned::to_owned)
}

fn users_me_management_tokens_root(request_path: &str) -> bool {
    matches!(
        request_path,
        "/api/me/management-tokens" | "/api/me/management-tokens/"
    )
}

fn users_me_management_token_id_from_path(request_path: &str) -> Option<String> {
    let raw = request_path
        .strip_prefix("/api/me/management-tokens/")?
        .trim()
        .trim_matches('/');
    if raw.is_empty() || raw.contains('/') {
        return None;
    }
    Some(raw.to_string())
}

fn users_me_management_token_status_id_from_path(request_path: &str) -> Option<String> {
    let raw = request_path
        .strip_prefix("/api/me/management-tokens/")?
        .trim()
        .trim_matches('/');
    let token_id = raw.strip_suffix("/status")?.trim_matches('/');
    if token_id.is_empty() || token_id.contains('/') {
        return None;
    }
    Some(token_id.to_string())
}

fn users_me_management_token_regenerate_id_from_path(request_path: &str) -> Option<String> {
    let raw = request_path
        .strip_prefix("/api/me/management-tokens/")?
        .trim()
        .trim_matches('/');
    let token_id = raw.strip_suffix("/regenerate")?.trim_matches('/');
    if token_id.is_empty() || token_id.contains('/') {
        return None;
    }
    Some(token_id.to_string())
}

fn users_me_management_token_max_per_user() -> usize {
    std::env::var(USERS_ME_MANAGEMENT_TOKEN_MAX_PER_USER_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(USERS_ME_MANAGEMENT_TOKEN_DEFAULT_MAX_PER_USER)
}

fn generate_users_me_management_token_plaintext() -> String {
    let first = Uuid::new_v4().simple().to_string();
    let second = Uuid::new_v4().simple().to_string();
    let mut random_part = String::with_capacity(USERS_ME_MANAGEMENT_TOKEN_RANDOM_LENGTH);
    random_part.push_str(&first);
    random_part.push_str(&second);
    random_part.truncate(USERS_ME_MANAGEMENT_TOKEN_RANDOM_LENGTH);
    format!("{USERS_ME_MANAGEMENT_TOKEN_PREFIX}{random_part}")
}

fn hash_users_me_management_token(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn users_me_management_token_prefix(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| {
        value[..value
            .len()
            .min(USERS_ME_MANAGEMENT_TOKEN_DISPLAY_PREFIX_LEN)]
            .to_string()
    })
}

fn users_me_management_token_limit(query: Option<&str>) -> usize {
    query_param_value(query, "limit")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(50)
}

fn users_me_management_token_skip(query: Option<&str>) -> usize {
    query_param_value(query, "skip")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

fn users_me_validate_ip_or_cidr(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    if value.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    let Some((host, prefix)) = value.split_once('/') else {
        return false;
    };
    let Ok(ip) = host.trim().parse::<std::net::IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.trim().parse::<u8>() else {
        return false;
    };
    match ip {
        std::net::IpAddr::V4(_) => prefix <= 32,
        std::net::IpAddr::V6(_) => prefix <= 128,
    }
}

fn users_me_parse_management_token_allowed_ips(
    value: Option<&serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                return Err("IP 白名单不能为空列表，如需取消限制请不提供此字段".to_string());
            }
            let mut normalized = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let Some(raw) = item.as_str() else {
                    return Err("IP 白名单必须是字符串数组".to_string());
                };
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(format!("IP 白名单第 {} 项为空", index + 1));
                }
                if !users_me_validate_ip_or_cidr(trimmed) {
                    return Err(format!("无效的 IP 地址或 CIDR: {raw}"));
                }
                normalized.push(trimmed.to_string());
            }
            Ok(Some(json!(normalized)))
        }
        _ => Err("IP 白名单必须是字符串数组".to_string()),
    }
}

fn users_me_parse_management_token_expires_at(
    value: &serde_json::Value,
    allow_past: bool,
) -> Result<Option<u64>, String> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let parsed = chrono::DateTime::parse_from_rfc3339(raw)
                .map(|value| value.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M"))
                        .map(|value| value.and_utc())
                })
                .map_err(|_| format!("无效的时间格式: {raw}"))?;
            let unix_secs = parsed.timestamp();
            if !allow_past && unix_secs <= Utc::now().timestamp() {
                return Err("过期时间必须在未来".to_string());
            }
            u64::try_from(unix_secs)
                .map(Some)
                .map_err(|_| format!("无效的时间格式: {raw}"))
        }
        _ => Err("expires_at 必须是字符串或 null".to_string()),
    }
}

fn users_me_parse_management_token_create_input(
    request_body: &[u8],
) -> Result<UsersMeManagementTokenCreateInput, String> {
    let payload = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(request_body)
        .map_err(|_| "输入验证失败".to_string())?;
    let name = match payload.get("name") {
        Some(serde_json::Value::String(value)) if (1..=100).contains(&value.chars().count()) => {
            value.clone()
        }
        _ => return Err("输入验证失败".to_string()),
    };
    let description = match payload.get("description") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(value)) if value.chars().count() <= 500 => {
            Some(value.clone())
        }
        _ => return Err("输入验证失败".to_string()),
    };
    let allowed_ips = users_me_parse_management_token_allowed_ips(payload.get("allowed_ips"))?;
    let expires_at_unix_secs = match payload.get("expires_at") {
        Some(value) => users_me_parse_management_token_expires_at(value, false)?,
        None => None,
    };

    Ok(UsersMeManagementTokenCreateInput {
        name,
        description,
        allowed_ips,
        expires_at_unix_secs,
    })
}

fn users_me_parse_management_token_update_input(
    request_body: &[u8],
) -> Result<UsersMeManagementTokenUpdateInput, String> {
    let payload = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(request_body)
        .map_err(|_| "输入验证失败".to_string())?;
    let mut input = UsersMeManagementTokenUpdateInput::default();

    if let Some(value) = payload.get("name") {
        match value {
            serde_json::Value::Null => {}
            serde_json::Value::String(value) if (1..=100).contains(&value.chars().count()) => {
                input.name = Some(value.clone());
            }
            _ => return Err("输入验证失败".to_string()),
        }
    }

    if let Some(value) = payload.get("description") {
        match value {
            serde_json::Value::Null => input.clear_description = true,
            serde_json::Value::String(value) if value.chars().count() <= 500 => {
                if value.is_empty() {
                    input.clear_description = true;
                } else {
                    input.description = Some(value.clone());
                }
            }
            _ => return Err("输入验证失败".to_string()),
        }
    }

    if let Some(value) = payload.get("allowed_ips") {
        if value.is_null() {
            input.clear_allowed_ips = true;
        } else {
            input.allowed_ips = users_me_parse_management_token_allowed_ips(Some(value))?;
        }
    }

    if let Some(value) = payload.get("expires_at") {
        if value.is_null()
            || value
                .as_str()
                .is_some_and(|value| value.trim().is_empty())
        {
            input.clear_expires_at = true;
        } else {
            input.expires_at_unix_secs =
                users_me_parse_management_token_expires_at(value, false)?;
        }
    }

    Ok(input)
}

fn format_users_me_optional_datetime_iso8601(
    value: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<String> {
    value.map(|value| value.to_rfc3339())
}

fn format_users_me_optional_unix_secs_iso8601(value: Option<u64>) -> Option<String> {
    let secs = value?;
    let secs = i64::try_from(secs).ok()?;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).map(|value| value.to_rfc3339())
}

fn format_users_me_required_session_datetime_iso8601(
    session: &crate::gateway::data::StoredUserSessionRecord,
) -> Option<String> {
    session
        .created_at
        .or(session.updated_at)
        .or(session.last_seen_at)
        .map(|value| value.to_rfc3339())
}

fn users_me_masked_api_key_display(state: &AppState, ciphertext: Option<&str>) -> String {
    let Some(ciphertext) = ciphertext.map(str::trim).filter(|value| !value.is_empty()) else {
        return "sk-****".to_string();
    };
    let Some(full_key) = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
    else {
        return "sk-****".to_string();
    };
    let prefix_len = full_key.len().min(10);
    let prefix = &full_key[..prefix_len];
    let suffix = if full_key.len() >= 4 {
        &full_key[full_key.len() - 4..]
    } else {
        ""
    };
    format!("{prefix}...{suffix}")
}

fn build_users_me_session_payload(
    session: crate::gateway::data::StoredUserSessionRecord,
    current_session_id: &str,
) -> serde_json::Value {
    json!({
        "id": session.id,
        "device_label": session
            .device_label
            .clone()
            .unwrap_or_else(|| "未知设备".to_string()),
        "device_type": "unknown",
        "browser_name": serde_json::Value::Null,
        "browser_version": serde_json::Value::Null,
        "os_name": serde_json::Value::Null,
        "os_version": serde_json::Value::Null,
        "device_model": serde_json::Value::Null,
        "ip_address": session.ip_address,
        "last_seen_at": format_users_me_optional_datetime_iso8601(session.last_seen_at),
        "created_at": format_users_me_required_session_datetime_iso8601(&session),
        "is_current": session.id == current_session_id,
        "revoked_at": format_users_me_optional_datetime_iso8601(session.revoked_at),
        "revoke_reason": session.revoke_reason,
    })
}

fn build_users_me_api_key_list_payload(
    state: &AppState,
    record: &aether_data::repository::auth::StoredAuthApiKeyExportRecord,
    is_locked: bool,
) -> serde_json::Value {
    json!({
        "id": record.api_key_id,
        "name": record.name,
        "key_display": users_me_masked_api_key_display(state, record.key_encrypted.as_deref()),
        "is_active": record.is_active,
        "is_locked": is_locked,
        "last_used_at": serde_json::Value::Null,
        "created_at": serde_json::Value::Null,
        "total_requests": record.total_requests,
        "total_cost_usd": record.total_cost_usd,
        "rate_limit": record.rate_limit,
        "allowed_providers": record.allowed_providers,
        "force_capabilities": record.force_capabilities,
    })
}

fn build_users_me_api_key_detail_payload(
    state: &AppState,
    record: &aether_data::repository::auth::StoredAuthApiKeyExportRecord,
    is_locked: bool,
) -> serde_json::Value {
    json!({
        "id": record.api_key_id,
        "name": record.name,
        "key_display": users_me_masked_api_key_display(state, record.key_encrypted.as_deref()),
        "is_active": record.is_active,
        "is_locked": is_locked,
        "allowed_providers": record.allowed_providers,
        "force_capabilities": record.force_capabilities,
        "rate_limit": record.rate_limit,
        "last_used_at": serde_json::Value::Null,
        "expires_at": format_users_me_optional_unix_secs_iso8601(record.expires_at_unix_secs),
        "created_at": serde_json::Value::Null,
    })
}

fn build_users_me_preferences_payload(
    preferences: &crate::gateway::data::StoredUserPreferenceRecord,
) -> serde_json::Value {
    json!({
        "avatar_url": preferences.avatar_url,
        "bio": preferences.bio,
        "default_provider_id": preferences.default_provider_id,
        "default_provider": preferences.default_provider_name,
        "theme": preferences.theme,
        "language": preferences.language,
        "timezone": preferences.timezone,
        "notifications": {
            "email": preferences.email_notifications,
            "usage_alerts": preferences.usage_alerts,
            "announcements": preferences.announcement_notifications,
        },
    })
}

fn parse_users_me_optional_string_field(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<String>, String> {
    match payload.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        _ => Err("输入验证失败".to_string()),
    }
}

fn normalize_users_me_optional_non_empty_string(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn normalize_users_me_required_api_key_name(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("API密钥名称不能为空".to_string());
    }
    Ok(trimmed.chars().take(100).collect())
}

fn generate_users_me_api_key_plaintext() -> String {
    let first = uuid::Uuid::new_v4().simple().to_string();
    let second = uuid::Uuid::new_v4().simple().to_string();
    format!("sk-{}{}", first, &second[..16])
}

fn hash_users_me_api_key(value: &str) -> String {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn normalize_users_me_api_key_providers(
    payload: UsersMeUpdateApiKeyProvidersRequest,
) -> Result<Option<Vec<String>>, String> {
    let values = if let Some(values) = payload.allowed_providers {
        values
            .into_iter()
            .map(|value| match value {
                UsersMeApiKeyProviderValue::ProviderId(provider_id) => provider_id,
                UsersMeApiKeyProviderValue::ProviderConfig { provider_id, .. } => provider_id,
            })
            .collect::<Vec<_>>()
    } else if let Some(values) = payload.providers {
        values
    } else {
        return Ok(None);
    };

    let mut normalized = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let provider_id = value.trim();
        if provider_id.is_empty() {
            return Err("提供商ID不能为空".to_string());
        }
        if seen.insert(provider_id.to_string()) {
            normalized.push(provider_id.to_string());
        }
    }
    Ok(Some(normalized))
}

fn normalize_users_me_api_key_force_capabilities(
    payload: UsersMeUpdateApiKeyCapabilitiesRequest,
) -> Result<Option<serde_json::Value>, String> {
    if let Some(capabilities) = payload.capabilities {
        let mut map = serde_json::Map::new();
        for capability in capabilities {
            let capability = capability.trim();
            if capability.is_empty() {
                return Err("能力名称不能为空".to_string());
            }
            map.insert(capability.to_string(), serde_json::Value::Bool(true));
        }
        return validate_users_me_force_capabilities(Some(serde_json::Value::Object(map)));
    }
    validate_users_me_force_capabilities(payload.force_capabilities)
}

fn validate_users_me_force_capabilities(
    value: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, String> {
    let Some(value) = normalize_user_model_capability_settings_input(value) else {
        return Ok(None);
    };
    let Some(map) = value.as_object() else {
        return Err("force_capabilities 必须是对象类型".to_string());
    };

    let user_configurable = user_configurable_capability_names();
    let known_capabilities = known_capability_names();
    for (capability_name, capability_value) in map {
        if !known_capabilities.contains(capability_name.as_str()) {
            return Err(format!("未知的能力类型: {capability_name}"));
        }
        if !user_configurable.contains(capability_name.as_str()) {
            return Err(format!("能力 {capability_name} 不支持用户配置"));
        }
        if !capability_value.is_boolean() {
            return Err(format!("能力 {capability_name} 的值必须是布尔类型"));
        }
    }

    Ok(Some(value))
}

fn parse_users_me_optional_bool_field(
    payload: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<bool>, String> {
    match payload.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        _ => Err("输入验证失败".to_string()),
    }
}

fn parse_users_me_optional_provider_id_field(
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<String>, String> {
    match payload.get("default_provider_id") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Err("输入验证失败".to_string())
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(serde_json::Value::Number(value)) => Ok(Some(value.to_string())),
        _ => Err("输入验证失败".to_string()),
    }
}

fn validate_users_me_preference_theme(theme: &str) -> Result<(), String> {
    if matches!(theme, "light" | "dark" | "auto" | "system") {
        Ok(())
    } else {
        Err("Invalid theme. Must be 'light', 'dark', 'auto', or 'system'".to_string())
    }
}

fn users_me_allowed_provider_names(
    user: &aether_data::repository::users::StoredUserAuthRecord,
) -> Option<BTreeSet<String>> {
    if user.role.eq_ignore_ascii_case("admin") {
        return None;
    }

    user.allowed_providers
        .as_ref()
        .map(|providers| {
            providers
                .iter()
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty())
                .collect::<BTreeSet<_>>()
        })
        .filter(|providers| !providers.is_empty())
}

fn parse_users_me_usage_limit(query: Option<&str>) -> Result<usize, String> {
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
        None => Ok(100),
    }
}

fn parse_users_me_usage_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
        None => Ok(0),
    }
}

fn parse_users_me_usage_hours(query: Option<&str>) -> Result<u32, String> {
    match query_param_value(query, "hours") {
        Some(value) => parse_bounded_u32("hours", &value, 1, 720),
        None => Ok(24),
    }
}

fn parse_users_me_usage_timeline_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be an integer between 100 and 20000".to_string())?;
            if (100..=20_000).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("limit must be an integer between 100 and 20000".to_string())
            }
        }
        None => Ok(2_000),
    }
}

fn parse_users_me_usage_ids(query: Option<&str>) -> Option<BTreeSet<String>> {
    let ids = query_param_value(query, "ids")?;
    let values = ids
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    (!values.is_empty()).then_some(values)
}

fn users_me_usage_total_input_context(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> u64 {
    item.input_tokens
        .saturating_add(item.cache_read_input_tokens)
}

fn users_me_usage_effective_unix_secs(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> u64 {
    item.finalized_at_unix_secs.unwrap_or(item.created_at_unix_secs)
}

fn users_me_usage_cache_hit_rate(total_input_context: u64, cache_read_tokens: u64) -> f64 {
    if total_input_context == 0 {
        0.0
    } else {
        round_to(cache_read_tokens as f64 / total_input_context as f64 * 100.0, 2)
    }
}

fn users_me_usage_matches_search(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    search: Option<&str>,
) -> bool {
    let Some(search) = search.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };

    let model = item.model.to_ascii_lowercase();
    let api_key_name = item
        .api_key_name
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    search.split_whitespace().all(|keyword| {
        let keyword = keyword.to_ascii_lowercase();
        model.contains(&keyword) || api_key_name.contains(&keyword)
    })
}

fn build_users_me_usage_api_key_payload(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> serde_json::Value {
    match item.api_key_id.as_deref() {
        Some(api_key_id) => json!({
            "id": api_key_id,
            "name": item.api_key_name.clone(),
            "display": item
                .api_key_name
                .clone()
                .unwrap_or_else(|| api_key_id.to_string()),
        }),
        None => serde_json::Value::Null,
    }
}

fn build_users_me_usage_record_payload(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    include_actual_cost: bool,
) -> serde_json::Value {
    let mut payload = json!({
        "id": item.id,
        "model": item.model,
        "target_model": serde_json::Value::Null,
        "api_format": item.api_format,
        "endpoint_api_format": item.endpoint_api_format,
        "has_format_conversion": item.has_format_conversion,
        "input_tokens": item.input_tokens,
        "output_tokens": item.output_tokens,
        "total_tokens": item.total_tokens,
        "cost": round_to(item.total_cost_usd, 6),
        "response_time_ms": item.response_time_ms,
        "first_byte_time_ms": item.first_byte_time_ms,
        "is_stream": item.is_stream,
        "status": item.status,
        "created_at": unix_secs_to_rfc3339(item.created_at_unix_secs),
        "cache_creation_input_tokens": item.cache_creation_input_tokens,
        "cache_read_input_tokens": item.cache_read_input_tokens,
        "status_code": item.status_code,
        "error_message": item.error_message,
        "input_price_per_1m": serde_json::Value::Null,
        "output_price_per_1m": item.output_price_per_1m,
        "cache_creation_price_per_1m": serde_json::Value::Null,
        "cache_read_price_per_1m": serde_json::Value::Null,
        "api_key": build_users_me_usage_api_key_payload(item),
    });

    if item.target_model.is_some() {
        payload["target_model"] = json!(item.target_model.clone());
    }
    if include_actual_cost {
        payload["actual_cost"] = json!(round_to(item.actual_total_cost_usd, 6));
        payload["rate_multiplier"] = serde_json::Value::Null;
    }
    payload
}

fn build_users_me_usage_active_payload(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> serde_json::Value {
    let mut payload = json!({
        "id": item.id,
        "status": item.status,
        "input_tokens": item.input_tokens,
        "output_tokens": item.output_tokens,
        "cache_creation_input_tokens": item.cache_creation_input_tokens,
        "cache_read_input_tokens": item.cache_read_input_tokens,
        "cost": round_to(item.total_cost_usd, 6),
        "actual_cost": round_to(item.actual_total_cost_usd, 6),
        "rate_multiplier": serde_json::Value::Null,
        "response_time_ms": item.response_time_ms,
        "first_byte_time_ms": item.first_byte_time_ms,
        "api_format": item.api_format,
        "endpoint_api_format": item.endpoint_api_format,
        "has_format_conversion": item.has_format_conversion,
        "target_model": item.target_model,
    });
    if item.api_format.is_none() {
        payload.as_object_mut().expect("object").remove("api_format");
    }
    if item.endpoint_api_format.is_none() {
        payload
            .as_object_mut()
            .expect("object")
            .remove("endpoint_api_format");
    }
    if item.target_model.is_none() {
        payload.as_object_mut().expect("object").remove("target_model");
    }
    payload
}

fn build_users_me_usage_summary_by_model(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
    include_actual_cost: bool,
) -> Vec<serde_json::Value> {
    let mut grouped: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for item in items {
        let entry = grouped.entry(item.model.clone()).or_insert_with(|| {
            json!({
                "model": item.model,
                "requests": 0_u64,
                "input_tokens": 0_u64,
                "output_tokens": 0_u64,
                "total_tokens": 0_u64,
                "cache_read_tokens": 0_u64,
                "cache_creation_tokens": 0_u64,
                "total_input_context": 0_u64,
                "cache_hit_rate": 0.0,
                "total_cost_usd": 0.0,
            })
        });
        entry["requests"] = json!(entry["requests"].as_u64().unwrap_or(0).saturating_add(1));
        entry["input_tokens"] =
            json!(entry["input_tokens"].as_u64().unwrap_or(0).saturating_add(item.input_tokens));
        entry["output_tokens"] = json!(
            entry["output_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.output_tokens)
        );
        entry["total_tokens"] = json!(
            entry["total_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.total_tokens)
        );
        entry["cache_read_tokens"] = json!(
            entry["cache_read_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_read_input_tokens)
        );
        entry["cache_creation_tokens"] = json!(
            entry["cache_creation_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_creation_input_tokens)
        );
        entry["total_input_context"] = json!(
            entry["total_input_context"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(users_me_usage_total_input_context(item))
        );
        entry["total_cost_usd"] = json!(
            entry["total_cost_usd"].as_f64().unwrap_or(0.0) + item.total_cost_usd
        );
        if include_actual_cost {
            if entry.get("actual_total_cost_usd").is_none() {
                entry["actual_total_cost_usd"] = json!(0.0);
            }
            entry["actual_total_cost_usd"] = json!(
                entry["actual_total_cost_usd"].as_f64().unwrap_or(0.0)
                    + item.actual_total_cost_usd
            );
        }
    }

    let mut values = grouped.into_values().collect::<Vec<_>>();
    for value in &mut values {
        let total_input_context = value["total_input_context"].as_u64().unwrap_or(0);
        let cache_read_tokens = value["cache_read_tokens"].as_u64().unwrap_or(0);
        value["cache_hit_rate"] =
            json!(users_me_usage_cache_hit_rate(total_input_context, cache_read_tokens));
        value["total_cost_usd"] =
            json!(round_to(value["total_cost_usd"].as_f64().unwrap_or(0.0), 6));
        if include_actual_cost && value.get("actual_total_cost_usd").is_some() {
            value["actual_total_cost_usd"] = json!(round_to(
                value["actual_total_cost_usd"].as_f64().unwrap_or(0.0),
                6,
            ));
        }
    }
    values.sort_by(|left, right| {
        right["requests"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&left["requests"].as_u64().unwrap_or(0))
            .then_with(|| {
                left["model"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["model"].as_str().unwrap_or_default())
            })
    });
    values
}

fn build_users_me_usage_summary_by_provider(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
) -> Vec<serde_json::Value> {
    let mut grouped: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for item in items {
        let entry = grouped.entry(item.provider_name.clone()).or_insert_with(|| {
            json!({
                "provider": item.provider_name,
                "requests": 0_u64,
                "total_tokens": 0_u64,
                "total_input_context": 0_u64,
                "output_tokens": 0_u64,
                "cache_read_tokens": 0_u64,
                "cache_creation_tokens": 0_u64,
                "cache_hit_rate": 0.0,
                "total_cost_usd": 0.0,
                "success_rate": 0.0,
                "avg_response_time_ms": 0.0,
                "_success_count": 0_u64,
                "_response_time_sum_ms": 0.0,
                "_response_time_count": 0_u64,
            })
        });
        entry["requests"] = json!(entry["requests"].as_u64().unwrap_or(0).saturating_add(1));
        entry["total_tokens"] = json!(
            entry["total_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.total_tokens)
        );
        entry["total_input_context"] = json!(
            entry["total_input_context"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(users_me_usage_total_input_context(item))
        );
        entry["output_tokens"] = json!(
            entry["output_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.output_tokens)
        );
        entry["cache_read_tokens"] = json!(
            entry["cache_read_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_read_input_tokens)
        );
        entry["cache_creation_tokens"] = json!(
            entry["cache_creation_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_creation_input_tokens)
        );
        entry["total_cost_usd"] = json!(
            entry["total_cost_usd"].as_f64().unwrap_or(0.0) + item.total_cost_usd
        );

        let is_success = item.status != "failed"
            && item.status_code.is_none_or(|status| status < 400)
            && item.error_message.is_none();
        if is_success {
            entry["_success_count"] = json!(
                entry["_success_count"]
                    .as_u64()
                    .unwrap_or(0)
                    .saturating_add(1)
            );
            if let Some(response_time_ms) = item.response_time_ms {
                entry["_response_time_sum_ms"] = json!(
                    entry["_response_time_sum_ms"].as_f64().unwrap_or(0.0)
                        + response_time_ms as f64
                );
                entry["_response_time_count"] = json!(
                    entry["_response_time_count"]
                        .as_u64()
                        .unwrap_or(0)
                        .saturating_add(1)
                );
            }
        }
    }

    let mut values = grouped.into_values().collect::<Vec<_>>();
    for value in &mut values {
        let total_input_context = value["total_input_context"].as_u64().unwrap_or(0);
        let cache_read_tokens = value["cache_read_tokens"].as_u64().unwrap_or(0);
        let success_count = value["_success_count"].as_u64().unwrap_or(0);
        let requests = value["requests"].as_u64().unwrap_or(0);
        let response_time_count = value["_response_time_count"].as_u64().unwrap_or(0);
        let response_time_sum_ms = value["_response_time_sum_ms"].as_f64().unwrap_or(0.0);
        value["cache_hit_rate"] =
            json!(users_me_usage_cache_hit_rate(total_input_context, cache_read_tokens));
        value["total_cost_usd"] =
            json!(round_to(value["total_cost_usd"].as_f64().unwrap_or(0.0), 6));
        value["success_rate"] = json!(if requests == 0 {
            100.0
        } else {
            round_to(success_count as f64 / requests as f64 * 100.0, 2)
        });
        value["avg_response_time_ms"] = json!(if response_time_count == 0 {
            0.0
        } else {
            round_to(response_time_sum_ms / response_time_count as f64, 2)
        });
        value.as_object_mut().expect("object").remove("_success_count");
        value
            .as_object_mut()
            .expect("object")
            .remove("_response_time_sum_ms");
        value
            .as_object_mut()
            .expect("object")
            .remove("_response_time_count");
    }
    values.sort_by(|left, right| {
        right["requests"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&left["requests"].as_u64().unwrap_or(0))
            .then_with(|| {
                left["provider"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["provider"].as_str().unwrap_or_default())
            })
    });
    values
}

fn build_users_me_usage_summary_by_api_format(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
) -> Vec<serde_json::Value> {
    let mut grouped: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for item in items.iter().filter(|item| item.api_format.is_some()) {
        let api_format = item.api_format.clone().unwrap_or_else(|| "unknown".to_string());
        let entry = grouped.entry(api_format.clone()).or_insert_with(|| {
            json!({
                "api_format": api_format,
                "request_count": 0_u64,
                "total_tokens": 0_u64,
                "total_input_context": 0_u64,
                "output_tokens": 0_u64,
                "cache_read_tokens": 0_u64,
                "cache_creation_tokens": 0_u64,
                "cache_hit_rate": 0.0,
                "total_cost_usd": 0.0,
                "avg_response_time_ms": 0.0,
                "_response_time_sum_ms": 0.0,
                "_response_time_count": 0_u64,
            })
        });
        entry["request_count"] = json!(
            entry["request_count"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(1)
        );
        entry["total_tokens"] = json!(
            entry["total_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.total_tokens)
        );
        entry["total_input_context"] = json!(
            entry["total_input_context"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(users_me_usage_total_input_context(item))
        );
        entry["output_tokens"] = json!(
            entry["output_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.output_tokens)
        );
        entry["cache_read_tokens"] = json!(
            entry["cache_read_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_read_input_tokens)
        );
        entry["cache_creation_tokens"] = json!(
            entry["cache_creation_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(item.cache_creation_input_tokens)
        );
        entry["total_cost_usd"] = json!(
            entry["total_cost_usd"].as_f64().unwrap_or(0.0) + item.total_cost_usd
        );
        if let Some(response_time_ms) = item.response_time_ms {
            entry["_response_time_sum_ms"] = json!(
                entry["_response_time_sum_ms"].as_f64().unwrap_or(0.0)
                    + response_time_ms as f64
            );
            entry["_response_time_count"] = json!(
                entry["_response_time_count"]
                    .as_u64()
                    .unwrap_or(0)
                    .saturating_add(1)
            );
        }
    }

    let mut values = grouped.into_values().collect::<Vec<_>>();
    for value in &mut values {
        let total_input_context = value["total_input_context"].as_u64().unwrap_or(0);
        let cache_read_tokens = value["cache_read_tokens"].as_u64().unwrap_or(0);
        let response_time_count = value["_response_time_count"].as_u64().unwrap_or(0);
        let response_time_sum_ms = value["_response_time_sum_ms"].as_f64().unwrap_or(0.0);
        value["cache_hit_rate"] =
            json!(users_me_usage_cache_hit_rate(total_input_context, cache_read_tokens));
        value["total_cost_usd"] =
            json!(round_to(value["total_cost_usd"].as_f64().unwrap_or(0.0), 6));
        value["avg_response_time_ms"] = json!(if response_time_count == 0 {
            0.0
        } else {
            round_to(response_time_sum_ms / response_time_count as f64, 2)
        });
        value
            .as_object_mut()
            .expect("object")
            .remove("_response_time_sum_ms");
        value
            .as_object_mut()
            .expect("object")
            .remove("_response_time_count");
    }
    values.sort_by(|left, right| {
        right["request_count"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&left["request_count"].as_u64().unwrap_or(0))
            .then_with(|| {
                left["api_format"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["api_format"].as_str().unwrap_or_default())
            })
    });
    values
}

async fn resolve_users_me_allowed_global_model_ids(
    state: &AppState,
    user: &aether_data::repository::users::StoredUserAuthRecord,
) -> Result<Option<BTreeSet<String>>, Response<Body>> {
    let Some(allowed_providers) = user
        .allowed_providers
        .as_ref()
        .filter(|providers| !providers.is_empty())
    else {
        return Ok(None);
    };

    if !state.has_provider_catalog_data_reader() {
        return Err(build_public_support_maintenance_response(
            USERS_ME_MAINTENANCE_DETAIL,
        ));
    }

    let allowed_provider_names = allowed_providers
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    let providers = match state.list_provider_catalog_providers(true).await {
        Ok(value) => value,
        Err(err) => {
            return Err(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user provider lookup failed: {err:?}"),
                false,
            ))
        }
    };
    let provider_ids = providers
        .into_iter()
        .filter(|provider| {
            allowed_provider_names.contains(&provider.id.to_ascii_lowercase())
                || allowed_provider_names.contains(&provider.name.to_ascii_lowercase())
        })
        .map(|provider| provider.id)
        .collect::<Vec<_>>();
    if provider_ids.is_empty() {
        return Ok(Some(BTreeSet::new()));
    }

    let refs = match state
        .list_active_global_model_ids_by_provider_ids(&provider_ids)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return Err(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user provider model lookup failed: {err:?}"),
                false,
            ))
        }
    };
    Ok(Some(
        refs.into_iter()
            .map(|entry| entry.global_model_id)
            .collect::<BTreeSet<_>>(),
    ))
}

async fn handle_users_me_available_models(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_global_model_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let (skip, limit, search) =
        parse_users_me_available_models_query(request_context.request_query_string.as_deref());

    let provider_model_ids = if auth.user.role.eq_ignore_ascii_case("admin") {
        None
    } else {
        match resolve_users_me_allowed_global_model_ids(state, &auth.user).await {
            Ok(value) => value,
            Err(response) => return response,
        }
    };
    let allowed_models = if auth.user.role.eq_ignore_ascii_case("admin") {
        None
    } else {
        auth.user
            .allowed_models
            .as_ref()
            .map(|models| {
                models
                    .iter()
                    .map(|value| value.trim().to_ascii_lowercase())
                    .filter(|value| !value.is_empty())
                    .collect::<BTreeSet<_>>()
            })
            .filter(|models| !models.is_empty())
    };

    let page = if provider_model_ids.is_none() && allowed_models.is_none() {
        match state
            .list_public_global_models(&aether_data::repository::global_models::PublicGlobalModelQuery {
                offset: skip,
                limit,
                is_active: Some(true),
                search,
            })
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("available model lookup failed: {err:?}"),
                    false,
                )
            }
        }
    } else {
        let page = match state
            .list_public_global_models(&aether_data::repository::global_models::PublicGlobalModelQuery {
                offset: 0,
                limit: USERS_ME_AVAILABLE_MODELS_FETCH_LIMIT,
                is_active: Some(true),
                search,
            })
            .await
        {
            Ok(value) => value,
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("available model lookup failed: {err:?}"),
                    false,
                )
            }
        };

        let filtered = page
            .items
            .into_iter()
            .filter(|model| {
                allowed_models
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(&model.name.to_ascii_lowercase()))
            })
            .filter(|model| {
                provider_model_ids
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(&model.id))
            })
            .collect::<Vec<_>>();
        let total = filtered.len();
        let items = filtered.into_iter().skip(skip).take(limit).collect::<Vec<_>>();
        aether_data::repository::global_models::StoredPublicGlobalModelPage { items, total }
    };

    Json(json!({
        "models": page
            .items
            .into_iter()
            .map(build_users_me_available_model_payload)
            .collect::<Vec<_>>(),
        "total": page.total,
    }))
    .into_response()
}

async fn handle_users_me_detail_put(
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
    let payload = match serde_json::from_slice::<UsersMeUpdateProfileRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };

    let email = normalize_users_me_optional_non_empty_string(payload.email);
    let username = normalize_users_me_optional_non_empty_string(payload.username);

    if let Some(email) = email.as_deref() {
        match state.is_other_user_auth_email_taken(email, &auth.user.id).await {
            Ok(true) => {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "邮箱已被使用",
                    false,
                )
            }
            Ok(false) => {}
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user email uniqueness lookup failed: {err:?}"),
                    false,
                )
            }
        }
    }

    if let Some(username) = username.as_deref() {
        match state
            .is_other_user_auth_username_taken(username, &auth.user.id)
            .await
        {
            Ok(true) => {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "用户名已被使用",
                    false,
                )
            }
            Ok(false) => {}
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user username uniqueness lookup failed: {err:?}"),
                    false,
                )
            }
        }
    }

    match state
        .update_local_auth_user_profile(&auth.user.id, email, username)
        .await
    {
        Ok(Some(_)) => Json(json!({ "message": "个人信息更新成功" })).into_response(),
        Ok(None) => build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("user profile update failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_password_patch(
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
    let payload = match serde_json::from_slice::<UsersMeChangePasswordRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };

    if auth.user.auth_source.eq_ignore_ascii_case("ldap") {
        return build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "LDAP 用户不能在此修改密码",
            false,
        );
    }

    let current_password_hash = auth
        .user
        .password_hash
        .as_deref()
        .filter(|value| !value.is_empty());
    if let Some(current_password_hash) = current_password_hash {
        let Some(old_password) = payload.old_password.as_deref() else {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "请输入当前密码",
                false,
            );
        };
        let old_password_matches =
            bcrypt::verify(old_password, current_password_hash).unwrap_or(false);
        if !old_password_matches {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "旧密码错误", false);
        }
        let new_password_matches =
            bcrypt::verify(&payload.new_password, current_password_hash).unwrap_or(false);
        if new_password_matches {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "新密码不能与当前密码相同",
                false,
            );
        }
    }

    let password_policy = match auth_password_policy_level(state).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("password policy lookup failed: {err:?}"),
                false,
            )
        }
    };
    if let Err(detail) = validate_auth_register_password(&payload.new_password, &password_policy) {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
    }

    let password_hash = match bcrypt::hash(&payload.new_password, bcrypt::DEFAULT_COST) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("password hash failed: {err:?}"),
                false,
            )
        }
    };
    let updated_at = chrono::Utc::now();
    match state
        .update_local_auth_user_password_hash(&auth.user.id, password_hash, updated_at)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL)
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user password update failed: {err:?}"),
                false,
            )
        }
    }

    let sessions = match state.list_user_sessions(&auth.user.id).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session lookup failed: {err:?}"),
                false,
            )
        }
    };
    for session in sessions {
        if session.id == auth.session_id {
            continue;
        }
        if let Err(err) = state
            .revoke_user_session(&auth.user.id, &session.id, updated_at, "password_changed")
            .await
        {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session revoke failed: {err:?}"),
                false,
            );
        }
    }

    let action = if current_password_hash.is_some() {
        "修改"
    } else {
        "设置"
    };
    Json(json!({ "message": format!("密码{action}成功") })).into_response()
}

async fn handle_users_me_sessions_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let sessions = match state.list_user_sessions(&auth.user.id).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session lookup failed: {err:?}"),
                false,
            )
        }
    };

    Json(
        sessions
            .into_iter()
            .map(|session| build_users_me_session_payload(session, &auth.session_id))
            .collect::<Vec<_>>(),
    )
    .into_response()
}

async fn handle_users_me_delete_other_sessions(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let sessions = match state.list_user_sessions(&auth.user.id).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session lookup failed: {err:?}"),
                false,
            )
        }
    };

    let now = chrono::Utc::now();
    let mut revoked_count = 0_u64;
    for session in sessions {
        if session.id == auth.session_id {
            continue;
        }
        match state
            .revoke_user_session(&auth.user.id, &session.id, now, "logout_other_sessions")
            .await
        {
            Ok(true) => revoked_count += 1,
            Ok(false) => {}
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("user session revoke failed: {err:?}"),
                    false,
                )
            }
        }
    }

    Json(json!({
        "message": "其他设备已退出登录",
        "revoked_count": revoked_count,
    }))
    .into_response()
}

async fn handle_users_me_delete_session(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(session_id) = users_me_session_id_from_path(&request_context.request_path) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    let session = match state.find_user_session(&auth.user.id, &session_id).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return build_auth_error_response(
                http::StatusCode::NOT_FOUND,
                "会话不存在",
                false,
            )
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session lookup failed: {err:?}"),
                false,
            )
        }
    };
    if session.is_revoked() || session.is_expired(chrono::Utc::now()) {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "会话不存在", false);
    }

    match state
        .revoke_user_session(
            &auth.user.id,
            &session_id,
            chrono::Utc::now(),
            "user_session_revoked",
        )
        .await
    {
        Ok(true) => Json(json!({ "message": "设备已退出登录" })).into_response(),
        Ok(false) => build_auth_error_response(http::StatusCode::NOT_FOUND, "会话不存在", false),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("user session revoke failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_update_session(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(session_id) = users_me_session_id_from_path(&request_context.request_path) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false);
    };
    let payload = match serde_json::from_slice::<UsersMeUpdateSessionLabelRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };
    let device_label = payload.device_label.trim();
    if device_label.is_empty() {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "设备名称不能为空", false);
    }
    let device_label = device_label.chars().take(120).collect::<String>();

    let session = match state.find_user_session(&auth.user.id, &session_id).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return build_auth_error_response(http::StatusCode::NOT_FOUND, "会话不存在", false)
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session lookup failed: {err:?}"),
                false,
            )
        }
    };
    if session.is_revoked() || session.is_expired(chrono::Utc::now()) {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "会话不存在", false);
    }

    let now = chrono::Utc::now();
    match state
        .update_user_session_device_label(&auth.user.id, &session_id, &device_label, now)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return build_auth_error_response(http::StatusCode::NOT_FOUND, "会话不存在", false)
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user session update failed: {err:?}"),
                false,
            )
        }
    }

    let mut updated = session;
    updated.device_label = Some(device_label);
    updated.updated_at = Some(now);
    Json(build_users_me_session_payload(updated, &auth.session_id)).into_response()
}

async fn handle_users_me_api_keys_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let mut records = match state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key lookup failed: {err:?}"),
                false,
            )
        }
    };
    records.retain(|record| !record.is_standalone);
    records.sort_by(|left, right| left.api_key_id.cmp(&right.api_key_id));

    let snapshot_ids = records
        .iter()
        .map(|record| record.api_key_id.clone())
        .collect::<Vec<_>>();
    let snapshot_by_id = match state.read_auth_api_key_snapshots_by_ids(&snapshot_ids).await {
        Ok(value) => value
            .into_iter()
            .map(|snapshot| (snapshot.api_key_id.clone(), snapshot))
            .collect::<BTreeMap<_, _>>(),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key snapshot lookup failed: {err:?}"),
                false,
            )
        }
    };

    Json(
        records
            .iter()
            .map(|record| {
                let is_locked = snapshot_by_id
                    .get(&record.api_key_id)
                    .map(|snapshot| snapshot.api_key_is_locked)
                    .unwrap_or(false);
                build_users_me_api_key_list_payload(state, record, is_locked)
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

async fn handle_users_me_api_key_detail_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(api_key_id) = users_me_api_key_id_from_path(&request_context.request_path) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };
    let include_key =
        query_param_optional_bool(request_context.request_query_string.as_deref(), "include_key")
            .unwrap_or(false);

    let records = match state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&auth.user.id))
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(record) = records
        .into_iter()
        .find(|record| !record.is_standalone && record.api_key_id == api_key_id)
    else {
        return build_auth_error_response(http::StatusCode::NOT_FOUND, "API密钥不存在", false);
    };

    if include_key {
        let Some(ciphertext) = record.key_encrypted.as_deref().map(str::trim) else {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "该密钥没有存储完整密钥信息",
                false,
            );
        };
        if ciphertext.is_empty() {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "该密钥没有存储完整密钥信息",
                false,
            );
        }
        let Some(full_key) =
            decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
        else {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "解密密钥失败",
                false,
            );
        };
        return Json(json!({ "key": full_key })).into_response();
    }

    let snapshot_ids = vec![api_key_id.clone()];
    let is_locked = match state.read_auth_api_key_snapshots_by_ids(&snapshot_ids).await {
        Ok(value) => value
            .into_iter()
            .find(|snapshot| snapshot.api_key_id == api_key_id)
            .map(|snapshot| snapshot.api_key_is_locked)
            .unwrap_or(false),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key snapshot lookup failed: {err:?}"),
                false,
            )
        }
    };

    Json(build_users_me_api_key_detail_payload(state, &record, is_locked)).into_response()
}

async fn resolve_users_me_api_key_snapshot(
    state: &AppState,
    user_id: &str,
    request_path: &str,
) -> Result<crate::gateway::data::StoredGatewayAuthApiKeySnapshot, Response<Body>> {
    let Some(api_key_id) = users_me_api_key_id_from_path(request_path) else {
        return Err(build_public_support_maintenance_response(
            USERS_ME_MAINTENANCE_DETAIL,
        ));
    };
    let snapshot = match state
        .read_auth_api_key_snapshot(
            user_id,
            &api_key_id,
            chrono::Utc::now().timestamp().max(0) as u64,
        )
        .await
    {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return Err(build_auth_error_response(
                http::StatusCode::NOT_FOUND,
                "API密钥不存在",
                false,
            ))
        }
        Err(err) => {
            return Err(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key snapshot lookup failed: {err:?}"),
                false,
            ))
        }
    };
    if snapshot.api_key_is_standalone {
        return Err(build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "API密钥不存在",
            false,
        ));
    }
    Ok(snapshot)
}

fn ensure_users_me_api_key_mutable(
    snapshot: &crate::gateway::data::StoredGatewayAuthApiKeySnapshot,
) -> Result<(), Response<Body>> {
    if snapshot.api_key_is_locked {
        return Err(build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "该密钥已被管理员锁定，无法修改",
            false,
        ));
    }
    Ok(())
}

async fn handle_users_me_api_key_create(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false);
    };
    let payload = match serde_json::from_slice::<UsersMeCreateApiKeyRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };
    let name = match normalize_users_me_required_api_key_name(&payload.name) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let rate_limit = payload.rate_limit.unwrap_or(0);
    if rate_limit < 0 {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "rate_limit 必须大于等于 0",
            false,
        );
    }

    let plaintext_key = generate_users_me_api_key_plaintext();
    let Some(key_encrypted) = encrypt_catalog_secret_with_fallbacks(state, &plaintext_key) else {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "API密钥加密失败",
            false,
        );
    };
    let record = aether_data::repository::auth::CreateUserApiKeyRecord {
        user_id: auth.user.id.clone(),
        api_key_id: uuid::Uuid::new_v4().to_string(),
        key_hash: hash_users_me_api_key(&plaintext_key),
        key_encrypted: Some(key_encrypted),
        name: Some(name.clone()),
        rate_limit,
        concurrent_limit: 5,
    };
    let Some(created) = (match state.create_user_api_key(record).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key create failed: {err:?}"),
                false,
            )
        }
    }) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    Json(json!({
        "id": created.api_key_id,
        "name": created.name,
        "key": plaintext_key,
        "key_display": users_me_masked_api_key_display(state, created.key_encrypted.as_deref()),
        "rate_limit": created.rate_limit,
        "message": "API密钥创建成功",
    }))
    .into_response()
}

async fn handle_users_me_api_key_update(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let snapshot =
        match resolve_users_me_api_key_snapshot(state, &auth.user.id, &request_context.request_path)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
    if let Err(response) = ensure_users_me_api_key_mutable(&snapshot) {
        return response;
    }
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false);
    };
    let payload = match serde_json::from_slice::<UsersMeUpdateApiKeyRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };
    let name = match payload.name {
        Some(value) => match normalize_users_me_required_api_key_name(&value) {
            Ok(value) => Some(value),
            Err(detail) => {
                return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
            }
        },
        None => None,
    };
    let rate_limit = payload.rate_limit;
    if rate_limit.is_some_and(|value| value < 0) {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "rate_limit 必须大于等于 0",
            false,
        );
    }

    let Some(updated) = (match state
        .update_user_api_key_basic(aether_data::repository::auth::UpdateUserApiKeyBasicRecord {
            user_id: auth.user.id.clone(),
            api_key_id: snapshot.api_key_id.clone(),
            name,
            rate_limit,
        })
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key update failed: {err:?}"),
                false,
            )
        }
    }) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    let mut payload = build_users_me_api_key_detail_payload(state, &updated, snapshot.api_key_is_locked);
    payload["message"] = json!("API密钥已更新");
    Json(payload).into_response()
}

async fn handle_users_me_api_key_patch(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let snapshot =
        match resolve_users_me_api_key_snapshot(state, &auth.user.id, &request_context.request_path)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
    if let Err(response) = ensure_users_me_api_key_mutable(&snapshot) {
        return response;
    }
    let desired_is_active = if let Some(request_body) = request_body {
        match serde_json::from_slice::<UsersMePatchApiKeyRequest>(request_body) {
            Ok(value) => value.is_active.unwrap_or(!snapshot.api_key_is_active),
            Err(_) => {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "请求数据验证失败",
                    false,
                )
            }
        }
    } else {
        !snapshot.api_key_is_active
    };

    let Some(updated) = (match state
        .set_user_api_key_active(&auth.user.id, &snapshot.api_key_id, desired_is_active)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key toggle failed: {err:?}"),
                false,
            )
        }
    }) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    Json(json!({
        "id": updated.api_key_id,
        "is_active": updated.is_active,
        "message": format!("API密钥已{}", if updated.is_active { "启用" } else { "禁用" }),
    }))
    .into_response()
}

async fn handle_users_me_api_key_delete(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let snapshot =
        match resolve_users_me_api_key_snapshot(state, &auth.user.id, &request_context.request_path)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
    if let Err(response) = ensure_users_me_api_key_mutable(&snapshot) {
        return response;
    }

    match state.delete_user_api_key(&auth.user.id, &snapshot.api_key_id).await {
        Ok(true) => Json(json!({ "message": "API密钥已删除" })).into_response(),
        Ok(false) => build_auth_error_response(http::StatusCode::NOT_FOUND, "API密钥不存在", false),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("user api key delete failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_api_key_providers_put(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let snapshot =
        match resolve_users_me_api_key_snapshot(state, &auth.user.id, &request_context.request_path)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
    if let Err(response) = ensure_users_me_api_key_mutable(&snapshot) {
        return response;
    }
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false);
    };
    let payload = match serde_json::from_slice::<UsersMeUpdateApiKeyProvidersRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false)
        }
    };
    let allowed_providers = match normalize_users_me_api_key_providers(payload) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };

    let allowed_providers = if let Some(providers) = allowed_providers {
        if state.has_provider_catalog_data_reader() {
            let catalog_providers = match state.list_provider_catalog_providers(true).await {
                Ok(value) => value,
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("provider validation failed: {err:?}"),
                        false,
                    )
                }
            };
            let mut by_key = BTreeMap::new();
            for provider in catalog_providers {
                by_key.insert(provider.id.to_ascii_lowercase(), provider.id.clone());
                by_key.insert(provider.name.to_ascii_lowercase(), provider.id);
            }
            let mut invalid = Vec::new();
            let mut normalized = Vec::new();
            for provider_id in providers {
                let key = provider_id.trim().to_ascii_lowercase();
                if let Some(mapped) = by_key.get(&key) {
                    if !normalized.iter().any(|value| value == mapped) {
                        normalized.push(mapped.clone());
                    }
                } else {
                    invalid.push(provider_id);
                }
            }
            if !invalid.is_empty() {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    format!("无效的提供商ID: {}", invalid.join(", ")),
                    false,
                );
            }
            Some(normalized)
        } else {
            let mut invalid = Vec::new();
            for provider_id in &providers {
                match state.find_active_provider_name(provider_id).await {
                    Ok(Some(_)) => {}
                    Ok(None) => invalid.push(provider_id.clone()),
                    Err(err) => {
                        return build_auth_error_response(
                            http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("provider validation failed: {err:?}"),
                            false,
                        )
                    }
                }
            }
            if !invalid.is_empty() {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    format!("无效的提供商ID: {}", invalid.join(", ")),
                    false,
                );
            }
            Some(providers)
        }
    } else {
        None
    };

    let Some(updated) = (match state
        .set_user_api_key_allowed_providers(&auth.user.id, &snapshot.api_key_id, allowed_providers)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key providers update failed: {err:?}"),
                false,
            )
        }
    }) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    Json(json!({
        "message": "API密钥可用提供商已更新",
        "allowed_providers": updated.allowed_providers,
    }))
    .into_response()
}

async fn handle_users_me_api_key_capabilities_put(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_auth_api_key_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let snapshot =
        match resolve_users_me_api_key_snapshot(state, &auth.user.id, &request_context.request_path)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
    if let Err(response) = ensure_users_me_api_key_mutable(&snapshot) {
        return response;
    }
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "请求数据验证失败", false);
    };
    let payload =
        match serde_json::from_slice::<UsersMeUpdateApiKeyCapabilitiesRequest>(request_body) {
            Ok(value) => value,
            Err(_) => {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "请求数据验证失败",
                    false,
                )
            }
        };
    let force_capabilities = match normalize_users_me_api_key_force_capabilities(payload) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };

    let Some(updated) = (match state
        .set_user_api_key_force_capabilities(&auth.user.id, &snapshot.api_key_id, force_capabilities)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user api key capabilities update failed: {err:?}"),
                false,
            )
        }
    }) else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };

    Json(json!({
        "message": "API密钥能力配置已更新",
        "force_capabilities": updated.force_capabilities.unwrap_or(serde_json::Value::Null),
    }))
    .into_response()
}

async fn handle_users_me_usage_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_usage_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let time_range = match AdminStatsTimeRange::resolve_optional(query) {
        Ok(value) => value,
        Err(detail) => return admin_stats_bad_request_response(detail),
    };
    let search = query_param_value(query, "search");
    let limit = match parse_users_me_usage_limit(query) {
        Ok(value) => value,
        Err(detail) => return admin_stats_bad_request_response(detail),
    };
    let offset = match parse_users_me_usage_offset(query) {
        Ok(value) => value,
        Err(detail) => return admin_stats_bad_request_response(detail),
    };

    let usage = match list_usage_for_optional_range(
        state,
        time_range.as_ref(),
        &AdminStatsUsageFilter {
            user_id: Some(auth.user.id.clone()),
            provider_name: None,
            model: None,
        },
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user usage lookup failed: {err:?}"),
                false,
            )
        }
    };

    let summary_items = usage
        .iter()
        .filter(|item| {
            !matches!(item.status.as_str(), "pending" | "streaming")
                && !matches!(item.provider_name.as_str(), "unknown" | "pending")
        })
        .cloned()
        .collect::<Vec<_>>();
    let include_actual_cost = auth.user.role.eq_ignore_ascii_case("admin");
    let total_requests = summary_items.len() as u64;
    let total_input_tokens = summary_items.iter().map(|item| item.input_tokens).sum::<u64>();
    let total_output_tokens = summary_items
        .iter()
        .map(|item| item.output_tokens)
        .sum::<u64>();
    let total_tokens = summary_items.iter().map(|item| item.total_tokens).sum::<u64>();
    let total_cost = round_to(
        summary_items.iter().map(|item| item.total_cost_usd).sum::<f64>(),
        6,
    );
    let total_actual_cost = round_to(
        summary_items
            .iter()
            .map(|item| item.actual_total_cost_usd)
            .sum::<f64>(),
        6,
    );

    let successful_response_times = summary_items
        .iter()
        .filter(|item| {
            item.status != "failed"
                && item.status_code.is_none_or(|status| status < 400)
                && item.error_message.is_none()
        })
        .filter_map(|item| item.response_time_ms)
        .collect::<Vec<_>>();
    let avg_response_time = if successful_response_times.is_empty() {
        0.0
    } else {
        round_to(
            successful_response_times
                .iter()
                .map(|value| *value as f64)
                .sum::<f64>()
                / successful_response_times.len() as f64
                / 1000.0,
            2,
        )
    };

    let mut records = usage
        .into_iter()
        .filter(|item| users_me_usage_matches_search(item, search.as_deref()))
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        right
            .created_at_unix_secs
            .cmp(&left.created_at_unix_secs)
            .then_with(|| left.id.cmp(&right.id))
    });
    let total_record_count = records.len();
    let records = records
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|item| build_users_me_usage_record_payload(&item, include_actual_cost))
        .collect::<Vec<_>>();

    let wallet = state
        .read_wallet_snapshot_for_auth(&auth.user.id, "", false)
        .await
        .ok()
        .flatten();

    let mut payload = json!({
        "total_requests": total_requests,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_tokens,
        "total_cost": total_cost,
        "avg_response_time": avg_response_time,
        "billing": build_auth_wallet_summary_payload(wallet.as_ref()),
        "summary_by_model": build_users_me_usage_summary_by_model(&summary_items, include_actual_cost),
        "summary_by_provider": build_users_me_usage_summary_by_provider(&summary_items),
        "summary_by_api_format": build_users_me_usage_summary_by_api_format(&summary_items),
        "pagination": {
            "total": total_record_count,
            "limit": limit,
            "offset": offset,
            "has_more": offset.saturating_add(limit) < total_record_count,
        },
        "records": records,
    });
    if include_actual_cost {
        payload["total_actual_cost"] = json!(total_actual_cost);
    }
    Json(payload).into_response()
}

async fn handle_users_me_usage_active_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_usage_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let ids = parse_users_me_usage_ids(request_context.request_query_string.as_deref());
    let items = match state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: None,
            created_until_unix_secs: None,
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
                format!("user active usage lookup failed: {err:?}"),
                false,
            )
        }
    };

    let mut items = items
        .into_iter()
        .filter(|item| match ids.as_ref() {
            Some(ids) => ids.contains(&item.id),
            None => matches!(item.status.as_str(), "pending" | "streaming"),
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right
            .created_at_unix_secs
            .cmp(&left.created_at_unix_secs)
            .then_with(|| left.id.cmp(&right.id))
    });
    if ids.is_none() && items.len() > 50 {
        items.truncate(50);
    }

    Json(json!({
        "requests": items
            .iter()
            .map(build_users_me_usage_active_payload)
            .collect::<Vec<_>>(),
    }))
    .into_response()
}

async fn handle_users_me_usage_interval_timeline_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_usage_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let query = request_context.request_query_string.as_deref();
    let hours = match parse_users_me_usage_hours(query) {
        Ok(value) => value,
        Err(detail) => return admin_stats_bad_request_response(detail),
    };
    let limit = match parse_users_me_usage_timeline_limit(query) {
        Ok(value) => value,
        Err(detail) => return admin_stats_bad_request_response(detail),
    };
    let now_unix_secs = u64::try_from(Utc::now().timestamp()).unwrap_or_default();
    let created_from_unix_secs = now_unix_secs.saturating_sub(u64::from(hours) * 3600);

    let mut items = match state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(created_from_unix_secs),
            created_until_unix_secs: None,
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
                format!("user interval timeline lookup failed: {err:?}"),
                false,
            )
        }
    };
    items.retain(|item| item.status == "completed");
    items.sort_by(|left, right| {
        left.created_at_unix_secs
            .cmp(&right.created_at_unix_secs)
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut points = Vec::new();
    let mut previous_created_at_unix_secs = None;
    for item in items {
        if let Some(previous) = previous_created_at_unix_secs {
            let interval_minutes =
                (item.created_at_unix_secs.saturating_sub(previous) as f64) / 60.0;
            if interval_minutes <= 120.0 {
                points.push(json!({
                    "x": unix_secs_to_rfc3339(item.created_at_unix_secs),
                    "y": round_to(interval_minutes, 2),
                    "model": item.model,
                }));
                if points.len() >= limit {
                    break;
                }
            }
        }
        previous_created_at_unix_secs = Some(item.created_at_unix_secs);
    }

    Json(json!({
        "analysis_period_hours": hours,
        "total_points": points.len(),
        "points": points,
    }))
    .into_response()
}

async fn handle_users_me_usage_heatmap_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_usage_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let today = Utc::now().date_naive();
    let start_date = today
        .checked_sub_signed(chrono::Duration::days(364))
        .unwrap_or(today);
    let Some(start_of_day) = start_date.and_hms_opt(0, 0, 0) else {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "heatmap start date is invalid",
            false,
        );
    };
    let created_from_unix_secs = u64::try_from(
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(start_of_day, chrono::Utc)
            .timestamp(),
    )
    .unwrap_or_default();

    let items = match state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(created_from_unix_secs),
            created_until_unix_secs: None,
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
                format!("user heatmap lookup failed: {err:?}"),
                false,
            )
        }
    };

    let include_actual_cost = auth.user.role.eq_ignore_ascii_case("admin");
    let mut daily = BTreeMap::<chrono::NaiveDate, (u64, u64, f64, f64)>::new();
    for item in items {
        if item.billing_status != "settled" || item.total_cost_usd <= 0.0 {
            continue;
        }
        let effective = users_me_usage_effective_unix_secs(&item);
        let Some(timestamp) = chrono::DateTime::<chrono::Utc>::from_timestamp(
            i64::try_from(effective).unwrap_or_default(),
            0,
        ) else {
            continue;
        };
        let entry = daily
            .entry(timestamp.date_naive())
            .or_insert((0, 0, 0.0, 0.0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry
            .1
            .saturating_add(item.total_tokens)
            .saturating_add(item.cache_creation_input_tokens)
            .saturating_add(item.cache_read_input_tokens);
        entry.2 += item.total_cost_usd;
        entry.3 += item.actual_total_cost_usd;
    }

    let mut max_requests = 0_u64;
    let mut cursor = start_date;
    let mut days = Vec::new();
    while cursor <= today {
        let (requests, total_tokens, total_cost, actual_total_cost) =
            daily.get(&cursor).copied().unwrap_or((0, 0, 0.0, 0.0));
        max_requests = max_requests.max(requests);
        let mut day = json!({
            "date": cursor.to_string(),
            "requests": requests,
            "total_tokens": total_tokens,
            "total_cost": round_to(total_cost, 6),
        });
        if include_actual_cost {
            day["actual_total_cost"] = json!(round_to(actual_total_cost, 6));
        }
        days.push(day);
        cursor = cursor
            .checked_add_signed(chrono::Duration::days(1))
            .unwrap_or(today + chrono::Duration::days(1));
    }

    Json(json!({
        "start_date": start_date.to_string(),
        "end_date": today.to_string(),
        "total_days": days.len(),
        "max_requests": max_requests,
        "days": days,
    }))
    .into_response()
}

async fn handle_users_me_providers_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_provider_catalog_data_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let allowed_provider_names = users_me_allowed_provider_names(&auth.user);

    let mut providers = match state.list_provider_catalog_providers(true).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user provider lookup failed: {err:?}"),
                false,
            )
        }
    };
    if let Some(allowed_provider_names) = allowed_provider_names.as_ref() {
        providers.retain(|provider| {
            allowed_provider_names.contains(&provider.id.to_ascii_lowercase())
                || allowed_provider_names.contains(&provider.name.to_ascii_lowercase())
        });
    }
    providers.sort_by(|left, right| {
        left.provider_priority
            .cmp(&right.provider_priority)
            .then_with(|| left.name.cmp(&right.name))
    });

    let provider_ids = providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<Vec<_>>();
    let endpoints = match state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user provider endpoint lookup failed: {err:?}"),
                false,
            )
        }
    };
    let mut endpoints_by_provider = BTreeMap::<String, Vec<serde_json::Value>>::new();
    for endpoint in endpoints {
        endpoints_by_provider
            .entry(endpoint.provider_id)
            .or_default()
            .push(json!({
                "id": endpoint.id,
                "api_format": endpoint.api_format,
                "base_url": endpoint.base_url,
                "is_active": endpoint.is_active,
            }));
    }

    let mut models_by_provider = BTreeMap::<String, Vec<serde_json::Value>>::new();
    if state.has_global_model_data_reader() {
        for provider_id in &provider_ids {
            let models = match state
                .list_public_catalog_models(
                    &aether_data::repository::global_models::PublicCatalogModelListQuery {
                        provider_id: Some(provider_id.clone()),
                        offset: 0,
                        limit: 1000,
                    },
                )
                .await
            {
                Ok(value) => value,
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("user provider model lookup failed: {err:?}"),
                        false,
                    )
                }
            };
            models_by_provider.insert(
                provider_id.clone(),
                models
                    .into_iter()
                    .map(|model| {
                        json!({
                            "id": model.id,
                            "name": model.name,
                            "display_name": model.display_name,
                            "input_price_per_1m": model.input_price_per_1m,
                            "output_price_per_1m": model.output_price_per_1m,
                            "cache_creation_price_per_1m": model.cache_creation_price_per_1m,
                            "cache_read_price_per_1m": model.cache_read_price_per_1m,
                            "supports_vision": model.supports_vision,
                            "supports_function_calling": model.supports_function_calling,
                            "supports_streaming": model.supports_streaming,
                        })
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }

    Json(
        providers
            .into_iter()
            .map(|provider| {
                let provider_id = provider.id.clone();
                let description = provider
                    .config
                    .as_ref()
                    .and_then(|value| value.get("description"))
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned);
                json!({
                    "id": provider_id.clone(),
                    "name": provider.name,
                    "description": description,
                    "provider_priority": provider.provider_priority,
                    "endpoints": endpoints_by_provider.remove(&provider_id).unwrap_or_default(),
                    "models": models_by_provider.remove(&provider_id).unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

async fn handle_users_me_endpoint_status_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(_) => {}
        Err(response) => return response,
    };

    let Some(payload) = build_admin_endpoint_health_status_payload(state, 6).await else {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    };
    let Some(items) = payload.as_array() else {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "endpoint status payload malformed",
            false,
        );
    };

    Json(serde_json::Value::Array(
        items.iter()
            .map(|item| {
                json!({
                    "api_format": item.get("api_format").cloned().unwrap_or(serde_json::Value::Null),
                    "display_name": item.get("display_name").cloned().unwrap_or(serde_json::Value::Null),
                    "health_score": item.get("health_score").cloned().unwrap_or(serde_json::Value::Null),
                    "timeline": item.get("timeline").cloned().unwrap_or_else(|| json!([])),
                    "time_range_start": item.get("time_range_start").cloned().unwrap_or(serde_json::Value::Null),
                    "time_range_end": item.get("time_range_end").cloned().unwrap_or(serde_json::Value::Null),
                })
            })
            .collect(),
    ))
    .into_response()
}

async fn handle_users_me_model_capabilities_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let settings = match state.read_user_model_capability_settings(&auth.user.id).await {
        Ok(Some(value)) => value,
        Ok(None) => serde_json::Value::Object(serde_json::Map::new()),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user model capability lookup failed: {err:?}"),
                false,
            )
        }
    };
    Json(json!({ "model_capability_settings": settings })).into_response()
}

async fn handle_users_me_preferences_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };

    let preferences = match state.read_user_preferences(&auth.user.id).await {
        Ok(Some(value)) => value,
        Ok(None) => crate::gateway::data::StoredUserPreferenceRecord::default_for_user(&auth.user.id),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user preference lookup failed: {err:?}"),
                false,
            )
        }
    };

    Json(build_users_me_preferences_payload(&preferences)).into_response()
}

async fn handle_users_me_preferences_put(
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
    let payload = match serde_json::from_slice::<serde_json::Value>(request_body) {
        Ok(value) => value,
        Err(_) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false),
    };
    let Some(payload) = payload.as_object() else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false);
    };

    let mut preferences = match state.read_user_preferences(&auth.user.id).await {
        Ok(Some(value)) => value,
        Ok(None) => crate::gateway::data::StoredUserPreferenceRecord::default_for_user(&auth.user.id),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user preference lookup failed: {err:?}"),
                false,
            )
        }
    };

    let avatar_url = match parse_users_me_optional_string_field(payload, "avatar_url") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(avatar_url) = avatar_url {
        preferences.avatar_url = Some(avatar_url);
    }

    let bio = match parse_users_me_optional_string_field(payload, "bio") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(bio) = bio {
        preferences.bio = Some(bio);
    }

    let default_provider_id = match parse_users_me_optional_provider_id_field(payload) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(default_provider_id) = default_provider_id {
        let provider_name = match state.find_active_provider_name(&default_provider_id).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return build_auth_error_response(
                    http::StatusCode::NOT_FOUND,
                    "Provider not found or inactive",
                    false,
                )
            }
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("provider preference lookup failed: {err:?}"),
                    false,
                )
            }
        };
        preferences.default_provider_id = Some(default_provider_id);
        preferences.default_provider_name = Some(provider_name);
    }

    let theme = match parse_users_me_optional_string_field(payload, "theme") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(theme) = theme {
        if let Err(detail) = validate_users_me_preference_theme(&theme) {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
        }
        preferences.theme = theme;
    }

    let language = match parse_users_me_optional_string_field(payload, "language") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(language) = language {
        preferences.language = language;
    }

    let timezone = match parse_users_me_optional_string_field(payload, "timezone") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(timezone) = timezone {
        preferences.timezone = timezone;
    }

    let email_notifications = match parse_users_me_optional_bool_field(payload, "email_notifications")
    {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(email_notifications) = email_notifications {
        preferences.email_notifications = email_notifications;
    }

    let usage_alerts = match parse_users_me_optional_bool_field(payload, "usage_alerts") {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    if let Some(usage_alerts) = usage_alerts {
        preferences.usage_alerts = usage_alerts;
    }

    let announcement_notifications =
        match parse_users_me_optional_bool_field(payload, "announcement_notifications") {
            Ok(value) => value,
            Err(detail) => {
                return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
            }
        };
    if let Some(announcement_notifications) = announcement_notifications {
        preferences.announcement_notifications = announcement_notifications;
    }

    match state.write_user_preferences(&preferences).await {
        Ok(Some(_)) => Json(json!({ "message": "偏好设置更新成功" })).into_response(),
        Ok(None) => build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("user preference update failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_model_capabilities_put(
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
    let payload = match serde_json::from_slice::<serde_json::Value>(request_body) {
        Ok(value) => value,
        Err(_) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false),
    };
    let Some(payload) = payload.as_object() else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false);
    };
    let settings = match validate_user_model_capability_settings(
        payload.get("model_capability_settings").cloned(),
    ) {
        Ok(value) => value,
        Err(detail) => return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false),
    };
    let persisted = match state
        .update_user_model_capability_settings(&auth.user.id, settings)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("user model capability update failed: {err:?}"),
                false,
            )
        }
    };

    Json(json!({
        "message": "模型能力配置已更新",
        "model_capability_settings": persisted.unwrap_or(serde_json::Value::Null),
    }))
    .into_response()
}

fn build_users_me_management_token_user_summary(
    auth: &AuthenticatedLocalUserContext,
) -> Result<StoredManagementTokenUserSummary, Response<Body>> {
    StoredManagementTokenUserSummary::new(
        auth.user.id.clone(),
        auth.user.email.clone(),
        auth.user.username.clone(),
        auth.user.role.clone(),
    )
    .map_err(|err| {
        build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token user summary build failed: {err:?}"),
            false,
        )
    })
}

async fn list_users_me_management_tokens_for_user(
    state: &AppState,
    user_id: &str,
    is_active: Option<bool>,
    limit: usize,
) -> Result<aether_data::repository::management_tokens::StoredManagementTokenListPage, Response<Body>>
{
    if !state.has_management_token_reader() {
        return Err(build_public_support_maintenance_response(
            USERS_ME_MAINTENANCE_DETAIL,
        ));
    }
    state
        .list_management_tokens(&ManagementTokenListQuery {
            user_id: Some(user_id.to_string()),
            is_active,
            offset: 0,
            limit,
        })
        .await
        .map_err(|err| {
            build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("management token list failed: {err:?}"),
                false,
            )
        })
}

async fn resolve_users_me_management_token(
    state: &AppState,
    user_id: &str,
    token_id: &str,
) -> Result<aether_data::repository::management_tokens::StoredManagementTokenWithUser, Response<Body>>
{
    if !state.has_management_token_reader() {
        return Err(build_public_support_maintenance_response(
            USERS_ME_MAINTENANCE_DETAIL,
        ));
    }
    match state.get_management_token_with_user(token_id).await {
        Ok(Some(token)) if token.token.user_id == user_id => Ok(token),
        Ok(Some(_)) | Ok(None) => Err(build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        )),
        Err(err) => Err(build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token lookup failed: {err:?}"),
            false,
        )),
    }
}

async fn handle_users_me_management_tokens_list(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_management_token_reader() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let is_active =
        query_param_optional_bool(request_context.request_query_string.as_deref(), "is_active");
    let skip = users_me_management_token_skip(request_context.request_query_string.as_deref());
    let limit = users_me_management_token_limit(request_context.request_query_string.as_deref());
    let page = match state
        .list_management_tokens(&ManagementTokenListQuery {
            user_id: Some(auth.user.id.clone()),
            is_active,
            offset: skip,
            limit,
        })
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("management token list failed: {err:?}"),
                false,
            )
        }
    };

    Json(json!({
        "items": page
            .items
            .iter()
            .map(|item| build_management_token_payload(&item.token, None))
            .collect::<Vec<_>>(),
        "total": page.total,
        "skip": skip,
        "limit": limit,
        "quota": {
            "used": page.total,
            "max": users_me_management_token_max_per_user(),
        },
    }))
    .into_response()
}

async fn handle_users_me_management_token_create(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_management_token_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let input = match users_me_parse_management_token_create_input(request_body) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
        }
    };

    let existing = match list_users_me_management_tokens_for_user(
        state,
        &auth.user.id,
        None,
        USERS_ME_MANAGEMENT_TOKEN_FETCH_LIMIT,
    )
    .await
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    let max_tokens = users_me_management_token_max_per_user();
    if existing.total >= max_tokens {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            format!("已达到 Token 数量上限（{max_tokens}）"),
            false,
        );
    }
    if existing.items.iter().any(|item| item.token.name == input.name) {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            format!("已存在名为 '{}' 的 Token", input.name),
            false,
        );
    }

    let raw_token = generate_users_me_management_token_plaintext();
    let record = CreateManagementTokenRecord {
        id: Uuid::new_v4().to_string(),
        user_id: auth.user.id.clone(),
        user: match build_users_me_management_token_user_summary(&auth) {
            Ok(value) => value,
            Err(response) => return response,
        },
        token_hash: hash_users_me_management_token(&raw_token),
        token_prefix: users_me_management_token_prefix(&raw_token),
        name: input.name.clone(),
        description: input.description,
        allowed_ips: input.allowed_ips,
        expires_at_unix_secs: input.expires_at_unix_secs,
        is_active: true,
    };

    match state.create_management_token(&record).await {
        Ok(LocalMutationOutcome::Applied(token)) => (
            http::StatusCode::CREATED,
            Json(json!({
                "message": "Management Token 创建成功",
                "token": raw_token,
                "data": build_management_token_payload(&token, None),
            })),
        )
            .into_response(),
        Ok(LocalMutationOutcome::Invalid(detail)) => {
            build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
        }
        Ok(LocalMutationOutcome::Unavailable) => {
            build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL)
        }
        Ok(LocalMutationOutcome::NotFound) => build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        ),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token create failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_management_token_detail_get(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(token_id) = users_me_management_token_id_from_path(&request_context.request_path) else {
        return build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        );
    };
    match resolve_users_me_management_token(state, &auth.user.id, &token_id).await {
        Ok(token) => Json(build_management_token_payload(&token.token, None)).into_response(),
        Err(response) => response,
    }
}

async fn handle_users_me_management_token_update(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    if !state.has_management_token_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(token_id) = users_me_management_token_id_from_path(&request_context.request_path) else {
        return build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        );
    };
    let existing = match resolve_users_me_management_token(state, &auth.user.id, &token_id).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let input = match users_me_parse_management_token_update_input(request_body) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
        }
    };
    if input.is_noop() {
        return Json(json!({
            "message": "更新成功",
            "data": build_management_token_payload(&existing.token, None),
        }))
        .into_response();
    }
    if let Some(name) = input.name.as_deref() {
        if name != existing.token.name {
            let page = match list_users_me_management_tokens_for_user(
                state,
                &auth.user.id,
                None,
                USERS_ME_MANAGEMENT_TOKEN_FETCH_LIMIT,
            )
            .await
            {
                Ok(value) => value,
                Err(response) => return response,
            };
            if page
                .items
                .iter()
                .any(|item| item.token.id != existing.token.id && item.token.name == name)
            {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    format!("已存在名为 '{}' 的 Token", name),
                    false,
                );
            }
        }
    }

    let record = UpdateManagementTokenRecord {
        token_id: existing.token.id.clone(),
        name: input.name,
        description: input.description,
        clear_description: input.clear_description,
        allowed_ips: input.allowed_ips,
        clear_allowed_ips: input.clear_allowed_ips,
        expires_at_unix_secs: input.expires_at_unix_secs,
        clear_expires_at: input.clear_expires_at,
        is_active: None,
    };

    match state.update_management_token(&record).await {
        Ok(LocalMutationOutcome::Applied(token)) => Json(json!({
            "message": "更新成功",
            "data": build_management_token_payload(&token, None),
        }))
        .into_response(),
        Ok(LocalMutationOutcome::NotFound) => build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        ),
        Ok(LocalMutationOutcome::Invalid(detail)) => {
            build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
        }
        Ok(LocalMutationOutcome::Unavailable) => {
            build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL)
        }
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token update failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_management_token_delete(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_management_token_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(token_id) = users_me_management_token_id_from_path(&request_context.request_path) else {
        return build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        );
    };
    let existing = match resolve_users_me_management_token(state, &auth.user.id, &token_id).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state.delete_management_token(&existing.token.id).await {
        Ok(true) => Json(json!({ "message": "删除成功" })).into_response(),
        Ok(false) => build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        ),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token delete failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_management_token_toggle(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_management_token_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(token_id) =
        users_me_management_token_status_id_from_path(&request_context.request_path)
    else {
        return build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        );
    };
    let existing = match resolve_users_me_management_token(state, &auth.user.id, &token_id).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state
        .set_management_token_active(&existing.token.id, !existing.token.is_active)
        .await
    {
        Ok(Some(token)) => Json(json!({
            "message": format!("Token 已{}", if token.is_active { "启用" } else { "禁用" }),
            "data": build_management_token_payload(&token, None),
        }))
        .into_response(),
        Ok(None) => build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        ),
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token toggle failed: {err:?}"),
            false,
        ),
    }
}

async fn handle_users_me_management_token_regenerate(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if !state.has_management_token_writer() {
        return build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL);
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(token_id) =
        users_me_management_token_regenerate_id_from_path(&request_context.request_path)
    else {
        return build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        );
    };
    let existing = match resolve_users_me_management_token(state, &auth.user.id, &token_id).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let raw_token = generate_users_me_management_token_plaintext();
    let mutation = RegenerateManagementTokenSecret {
        token_id: existing.token.id.clone(),
        token_hash: hash_users_me_management_token(&raw_token),
        token_prefix: users_me_management_token_prefix(&raw_token),
    };

    match state.regenerate_management_token_secret(&mutation).await {
        Ok(LocalMutationOutcome::Applied(token)) => Json(json!({
            "message": "Token 已重新生成",
            "token": raw_token,
            "data": build_management_token_payload(&token, None),
        }))
        .into_response(),
        Ok(LocalMutationOutcome::NotFound) => build_auth_error_response(
            http::StatusCode::NOT_FOUND,
            "Management Token 不存在",
            false,
        ),
        Ok(LocalMutationOutcome::Invalid(detail)) => {
            build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false)
        }
        Ok(LocalMutationOutcome::Unavailable) => {
            build_public_support_maintenance_response(USERS_ME_MAINTENANCE_DETAIL)
        }
        Err(err) => build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("management token regenerate failed: {err:?}"),
            false,
        ),
    }
}

async fn maybe_build_local_users_me_legacy_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("users_me_legacy") {
        return None;
    }

    match decision.route_kind.as_deref() {
        Some("detail") if request_context.request_path == "/api/users/me" => {
            Some(handle_auth_me(state, request_context, headers).await)
        }
        Some("update_detail") if request_context.request_path == "/api/users/me" => {
            Some(
                handle_users_me_detail_put(state, request_context, headers, request_body).await,
            )
        }
        Some("password") if request_context.request_path == "/api/users/me/password" => {
            Some(
                handle_users_me_password_patch(state, request_context, headers, request_body).await,
            )
        }
        Some("sessions") if request_context.request_path == "/api/users/me/sessions" => {
            Some(handle_users_me_sessions_get(state, request_context, headers).await)
        }
        Some("sessions_others_delete")
            if request_context.request_path == "/api/users/me/sessions/others" =>
        {
            Some(handle_users_me_delete_other_sessions(state, request_context, headers).await)
        }
        Some("session_delete")
            if request_context.request_path.starts_with("/api/users/me/sessions/") =>
        {
            Some(handle_users_me_delete_session(state, request_context, headers).await)
        }
        Some("session_update")
            if request_context.request_path.starts_with("/api/users/me/sessions/") =>
        {
            Some(
                handle_users_me_update_session(state, request_context, headers, request_body).await,
            )
        }
        Some("api_keys_list") if request_context.request_path == "/api/users/me/api-keys" => {
            Some(handle_users_me_api_keys_get(state, request_context, headers).await)
        }
        Some("management_tokens_list") if users_me_management_tokens_root(&request_context.request_path) => {
            Some(handle_users_me_management_tokens_list(state, request_context, headers).await)
        }
        Some("api_keys_create") if request_context.request_path == "/api/users/me/api-keys" => {
            Some(
                handle_users_me_api_key_create(state, request_context, headers, request_body).await,
            )
        }
        Some("management_tokens_create")
            if users_me_management_tokens_root(&request_context.request_path) =>
        {
            Some(
                handle_users_me_management_token_create(
                    state,
                    request_context,
                    headers,
                    request_body,
                )
                .await,
            )
        }
        Some("api_key_detail")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(handle_users_me_api_key_detail_get(state, request_context, headers).await)
        }
        Some("management_token_detail")
            if request_context
                .request_path
                .starts_with("/api/me/management-tokens/") =>
        {
            Some(handle_users_me_management_token_detail_get(state, request_context, headers).await)
        }
        Some("api_key_update")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(
                handle_users_me_api_key_update(state, request_context, headers, request_body).await,
            )
        }
        Some("management_token_update")
            if request_context
                .request_path
                .starts_with("/api/me/management-tokens/") =>
        {
            Some(
                handle_users_me_management_token_update(
                    state,
                    request_context,
                    headers,
                    request_body,
                )
                .await,
            )
        }
        Some("api_key_patch")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(
                handle_users_me_api_key_patch(state, request_context, headers, request_body).await,
            )
        }
        Some("management_token_toggle")
            if request_context
                .request_path
                .starts_with("/api/me/management-tokens/") =>
        {
            Some(handle_users_me_management_token_toggle(state, request_context, headers).await)
        }
        Some("api_key_delete")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(handle_users_me_api_key_delete(state, request_context, headers).await)
        }
        Some("management_token_delete")
            if request_context
                .request_path
                .starts_with("/api/me/management-tokens/") =>
        {
            Some(handle_users_me_management_token_delete(state, request_context, headers).await)
        }
        Some("api_key_providers_update")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(
                handle_users_me_api_key_providers_put(
                    state,
                    request_context,
                    headers,
                    request_body,
                )
                .await,
            )
        }
        Some("api_key_capabilities_update")
            if request_context
                .request_path
                .starts_with("/api/users/me/api-keys/") =>
        {
            Some(
                handle_users_me_api_key_capabilities_put(
                    state,
                    request_context,
                    headers,
                    request_body,
                )
                .await,
            )
        }
        Some("management_token_regenerate")
            if request_context
                .request_path
                .starts_with("/api/me/management-tokens/") =>
        {
            Some(handle_users_me_management_token_regenerate(state, request_context, headers).await)
        }
        Some("usage") if request_context.request_path == "/api/users/me/usage" => {
            Some(handle_users_me_usage_get(state, request_context, headers).await)
        }
        Some("usage_active") if request_context.request_path == "/api/users/me/usage/active" => {
            Some(handle_users_me_usage_active_get(state, request_context, headers).await)
        }
        Some("usage_interval_timeline")
            if request_context.request_path == "/api/users/me/usage/interval-timeline" =>
        {
            Some(handle_users_me_usage_interval_timeline_get(state, request_context, headers).await)
        }
        Some("usage_heatmap") if request_context.request_path == "/api/users/me/usage/heatmap" => {
            Some(handle_users_me_usage_heatmap_get(state, request_context, headers).await)
        }
        Some("endpoint_status") if request_context.request_path == "/api/users/me/endpoint-status" => {
            Some(handle_users_me_endpoint_status_get(state, request_context, headers).await)
        }
        Some("providers") if request_context.request_path == "/api/users/me/providers" => {
            Some(handle_users_me_providers_get(state, request_context, headers).await)
        }
        Some("preferences") if request_context.request_path == "/api/users/me/preferences" => {
            Some(handle_users_me_preferences_get(state, request_context, headers).await)
        }
        Some("available_models") if request_context.request_path == "/api/users/me/available-models" => {
            Some(handle_users_me_available_models(state, request_context, headers).await)
        }
        Some("model_capabilities")
            if request_context.request_path == "/api/users/me/model-capabilities" =>
        {
            Some(handle_users_me_model_capabilities_get(state, request_context, headers).await)
        }
        Some("model_capabilities_update")
            if request_context.request_path == "/api/users/me/model-capabilities" =>
        {
            Some(
                handle_users_me_model_capabilities_put(
                    state,
                    request_context,
                    headers,
                    request_body,
                )
                .await,
            )
        }
        Some("preferences_update") if request_context.request_path == "/api/users/me/preferences" => {
            Some(
                handle_users_me_preferences_put(state, request_context, headers, request_body)
                    .await,
            )
        }
        _ => Some(build_public_support_maintenance_response(
            USERS_ME_MAINTENANCE_DETAIL,
        )),
    }
}
