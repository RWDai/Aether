const ADMIN_USERS_RUST_BACKEND_DETAIL: &str =
    "Admin user management routes require Rust maintenance backend";

#[derive(Debug, serde::Deserialize)]
struct AdminCreateUserApiKeyRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    allowed_providers: Option<Vec<String>>,
    #[serde(default)]
    allowed_api_formats: Option<Vec<String>>,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    rate_limit: Option<i32>,
    #[serde(default)]
    expire_days: Option<i32>,
    #[serde(default)]
    expires_at: Option<String>,
    #[serde(default)]
    initial_balance_usd: Option<f64>,
    #[serde(default)]
    unlimited_balance: Option<bool>,
    #[serde(default)]
    is_standalone: Option<bool>,
    #[serde(default)]
    auto_delete_on_expiry: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminUpdateUserApiKeyRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    rate_limit: Option<i32>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminToggleUserApiKeyLockRequest {
    #[serde(default)]
    locked: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminCreateUserRequest {
    username: String,
    password: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    initial_gift_usd: Option<f64>,
    #[serde(default)]
    unlimited: bool,
    #[serde(default)]
    allowed_providers: Option<Vec<String>>,
    #[serde(default)]
    allowed_api_formats: Option<Vec<String>>,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    rate_limit: Option<i32>,
}

#[derive(Debug, serde::Deserialize)]
struct AdminUpdateUserRequest {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    unlimited: Option<bool>,
    #[serde(default)]
    allowed_providers: Option<Vec<String>>,
    #[serde(default)]
    allowed_api_formats: Option<Vec<String>>,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    rate_limit: Option<i32>,
    #[serde(default)]
    is_active: Option<bool>,
}

#[derive(Debug, Default)]
struct AdminUpdateUserFieldPresence {
    allowed_providers: bool,
    allowed_api_formats: bool,
    allowed_models: bool,
}

fn build_admin_users_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_USERS_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

fn build_admin_users_read_only_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::CONFLICT,
        Json(json!({
            "detail": detail,
            "error_code": "read_only_mode",
        })),
    )
        .into_response()
}

fn build_admin_users_bad_request_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn normalize_admin_optional_user_email(value: Option<&str>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let normalized = value.to_ascii_lowercase();
    let pattern = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        .expect("email regex should compile");
    if !pattern.is_match(&normalized) {
        return Err("邮箱格式无效".to_string());
    }
    Ok(Some(normalized))
}

fn normalize_admin_username(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("用户名不能为空".to_string());
    }
    if value.len() < 3 {
        return Err("用户名长度至少为3个字符".to_string());
    }
    if value.len() > 30 {
        return Err("用户名长度不能超过30个字符".to_string());
    }
    let pattern = Regex::new(r"^[a-zA-Z0-9_.-]+$").expect("username regex should compile");
    if !pattern.is_match(value) {
        return Err("用户名只能包含字母、数字、下划线、连字符和点号".to_string());
    }
    Ok(value.to_string())
}

fn validate_admin_user_password(password: &str, policy: &str) -> Result<(), String> {
    if password.is_empty() {
        return Err("密码不能为空".to_string());
    }
    if password.as_bytes().len() > 72 {
        return Err("密码长度不能超过72字节".to_string());
    }
    let min_len = if matches!(policy, "medium" | "strong") {
        8
    } else {
        6
    };
    if password.chars().count() < min_len {
        return Err(format!("密码长度至少为{min_len}个字符"));
    }
    if policy == "medium" {
        if !password.chars().any(|ch| ch.is_ascii_alphabetic()) {
            return Err("密码必须包含至少一个字母".to_string());
        }
        if !password.chars().any(|ch| ch.is_ascii_digit()) {
            return Err("密码必须包含至少一个数字".to_string());
        }
    } else if policy == "strong" {
        if !password.chars().any(|ch| ch.is_ascii_uppercase()) {
            return Err("密码必须包含至少一个大写字母".to_string());
        }
        if !password.chars().any(|ch| ch.is_ascii_lowercase()) {
            return Err("密码必须包含至少一个小写字母".to_string());
        }
        if !password.chars().any(|ch| ch.is_ascii_digit()) {
            return Err("密码必须包含至少一个数字".to_string());
        }
        if !password.chars().any(|ch| !ch.is_ascii_alphanumeric()) {
            return Err("密码必须包含至少一个特殊字符".to_string());
        }
    }
    Ok(())
}

fn normalize_admin_user_role(value: Option<&str>) -> Result<String, String> {
    match value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("user")
        .to_ascii_lowercase()
        .as_str()
    {
        "user" => Ok("user".to_string()),
        "admin" => Ok("admin".to_string()),
        _ => Err("角色参数不合法".to_string()),
    }
}

fn normalize_admin_user_string_list(
    value: Option<Vec<String>>,
    field_name: &str,
) -> Result<Option<Vec<String>>, String> {
    let Some(values) = value else {
        return Ok(None);
    };
    let mut normalized = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for item in values {
        let item = item.trim();
        if item.is_empty() {
            return Err(format!("{field_name} 不能为空"));
        }
        if seen.insert(item.to_string()) {
            normalized.push(item.to_string());
        }
    }
    Ok(Some(normalized))
}

fn normalize_admin_user_api_formats(
    value: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, String> {
    let Some(values) = value else {
        return Ok(None);
    };
    let mut normalized = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let pattern = Regex::new(r"^[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+$")
        .expect("api format regex should compile");
    for item in values {
        let item = item.trim();
        if item.is_empty() {
            return Err("allowed_api_formats 不能为空".to_string());
        }
        if !pattern.is_match(item) {
            return Err(format!("allowed_api_formats 格式无效: {item}"));
        }
        let normalized_item = item.to_ascii_lowercase();
        if seen.insert(normalized_item.clone()) {
            normalized.push(normalized_item);
        }
    }
    Ok(Some(normalized))
}

fn admin_default_user_initial_gift(value: Option<&serde_json::Value>) -> f64 {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_f64().unwrap_or(10.0),
        Some(serde_json::Value::String(value)) => value.parse::<f64>().unwrap_or(10.0),
        _ => 10.0,
    }
}

async fn admin_user_password_policy(state: &AppState) -> Result<String, GatewayError> {
    let config = state
        .read_system_config_json_value("password_policy_level")
        .await?;
    Ok(match config
        .as_ref()
        .and_then(|value| value.as_str())
        .unwrap_or("weak")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "medium" => "medium".to_string(),
        "strong" => "strong".to_string(),
        _ => "weak".to_string(),
    })
}

async fn find_admin_export_user(
    state: &AppState,
    user_id: &str,
) -> Result<Option<aether_data::repository::users::StoredUserExportRow>, GatewayError> {
    state.find_export_user_by_id(user_id).await
}

fn build_admin_user_payload(
    user: &aether_data::repository::users::StoredUserAuthRecord,
    rate_limit: Option<i32>,
    unlimited: bool,
) -> serde_json::Value {
    json!({
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "role": user.role,
        "allowed_providers": user.allowed_providers,
        "allowed_api_formats": user.allowed_api_formats,
        "allowed_models": user.allowed_models,
        "rate_limit": rate_limit,
        "unlimited": unlimited,
        "is_active": user.is_active,
        "created_at": format_optional_datetime_iso8601(user.created_at),
        "updated_at": serde_json::Value::Null,
        "last_login_at": format_optional_datetime_iso8601(user.last_login_at),
    })
}

fn admin_user_id_from_sessions_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/admin/users/")?
        .strip_suffix("/sessions")
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty() && !value.contains('/'))
}

fn admin_user_session_parts(request_path: &str) -> Option<(String, String)> {
    let raw = request_path.strip_prefix("/api/admin/users/")?;
    let (user_id, session_id) = raw.split_once("/sessions/")?;
    let user_id = user_id.trim().trim_matches('/');
    let session_id = session_id.trim().trim_matches('/');
    if user_id.is_empty()
        || session_id.is_empty()
        || user_id.contains('/')
        || session_id.contains('/')
    {
        None
    } else {
        Some((user_id.to_string(), session_id.to_string()))
    }
}

fn admin_user_api_key_full_key_parts(request_path: &str) -> Option<(String, String)> {
    let raw = request_path.strip_prefix("/api/admin/users/")?;
    let (user_id, key_id) = raw.split_once("/api-keys/")?;
    let user_id = user_id.trim().trim_matches('/');
    let key_id = key_id
        .trim()
        .trim_matches('/')
        .strip_suffix("/full-key")?
        .trim()
        .trim_matches('/');
    if user_id.is_empty() || key_id.is_empty() || user_id.contains('/') || key_id.contains('/') {
        None
    } else {
        Some((user_id.to_string(), key_id.to_string()))
    }
}

fn admin_user_api_key_parts(request_path: &str) -> Option<(String, String)> {
    let raw = request_path.strip_prefix("/api/admin/users/")?;
    let (user_id, key_id) = raw.split_once("/api-keys/")?;
    let user_id = user_id.trim().trim_matches('/');
    let key_id = key_id.trim().trim_matches('/');
    if user_id.is_empty() || key_id.is_empty() || user_id.contains('/') || key_id.contains('/') {
        None
    } else {
        Some((user_id.to_string(), key_id.to_string()))
    }
}

fn admin_user_api_key_lock_parts(request_path: &str) -> Option<(String, String)> {
    let raw = request_path.strip_prefix("/api/admin/users/")?;
    let (user_id, key_id) = raw.split_once("/api-keys/")?;
    let user_id = user_id.trim().trim_matches('/');
    let key_id = key_id
        .trim()
        .trim_matches('/')
        .strip_suffix("/lock")?
        .trim()
        .trim_matches('/');
    if user_id.is_empty() || key_id.is_empty() || user_id.contains('/') || key_id.contains('/') {
        None
    } else {
        Some((user_id.to_string(), key_id.to_string()))
    }
}

fn format_optional_unix_secs_iso8601(value: Option<u64>) -> Option<String> {
    let secs = value?;
    let secs = i64::try_from(secs).ok()?;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).map(|value| value.to_rfc3339())
}

fn admin_user_id_from_api_keys_path(request_path: &str) -> Option<String> {
    request_path
        .strip_prefix("/api/admin/users/")?
        .strip_suffix("/api-keys")
        .map(|value| value.trim().trim_matches('/').to_string())
        .filter(|value| !value.is_empty() && !value.contains('/'))
}

fn admin_user_id_from_detail_path(request_path: &str) -> Option<String> {
    let value = request_path
        .strip_prefix("/api/admin/users/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn masked_user_api_key_display(state: &AppState, ciphertext: Option<&str>) -> String {
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

fn build_admin_user_api_key_detail_payload(
    state: &AppState,
    record: &aether_data::repository::auth::StoredAuthApiKeyExportRecord,
    is_locked: bool,
) -> serde_json::Value {
    json!({
        "id": record.api_key_id,
        "name": record.name,
        "key_display": masked_user_api_key_display(state, record.key_encrypted.as_deref()),
        "is_active": record.is_active,
        "is_locked": is_locked,
        "total_requests": record.total_requests,
        "total_cost_usd": record.total_cost_usd,
        "rate_limit": record.rate_limit,
        "expires_at": format_optional_unix_secs_iso8601(record.expires_at_unix_secs),
        "last_used_at": serde_json::Value::Null,
        "created_at": serde_json::Value::Null,
    })
}

fn normalize_admin_optional_api_key_name(value: Option<String>) -> Result<Option<String>, String> {
    match value {
        None => Ok(None),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err("API密钥名称不能为空".to_string());
            }
            Ok(Some(trimmed.chars().take(100).collect()))
        }
    }
}

fn normalize_admin_api_key_providers(
    value: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, String> {
    let Some(values) = value else {
        return Ok(None);
    };
    let mut normalized = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for provider_id in values {
        let provider_id = provider_id.trim();
        if provider_id.is_empty() {
            return Err("提供商ID不能为空".to_string());
        }
        if seen.insert(provider_id.to_string()) {
            normalized.push(provider_id.to_string());
        }
    }
    Ok(Some(normalized))
}

fn generate_admin_user_api_key_plaintext() -> String {
    let first = uuid::Uuid::new_v4().simple().to_string();
    let second = uuid::Uuid::new_v4().simple().to_string();
    format!("sk-{}{}", first, &second[..16])
}

fn hash_admin_user_api_key(value: &str) -> String {
    use sha2::Digest;

    let mut hasher = sha2::Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn default_admin_user_api_key_name() -> String {
    format!(
        "API Key {}",
        chrono::Utc::now().format("%Y%m%d%H%M%S")
    )
}

fn format_optional_datetime_iso8601(
    value: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<String> {
    value.map(|value| value.to_rfc3339())
}

fn format_required_session_datetime_iso8601(
    session: &crate::gateway::data::StoredUserSessionRecord,
) -> String {
    session
        .created_at
        .or(session.updated_at)
        .or(session.last_seen_at)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339()
}

async fn build_admin_list_users_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let skip = query_param_value(request_context.request_query_string.as_deref(), "skip")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let limit = query_param_value(request_context.request_query_string.as_deref(), "limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let role = query_param_value(request_context.request_query_string.as_deref(), "role")
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    let is_active =
        query_param_optional_bool(request_context.request_query_string.as_deref(), "is_active");

    let paged_rows = state
        .list_export_users_page(&aether_data::repository::users::UserExportListQuery {
            skip,
            limit,
            role: role.clone(),
            is_active,
        })
        .await?;
    let user_ids = paged_rows
        .iter()
        .map(|row| row.id.clone())
        .collect::<Vec<_>>();
    let auth_by_user_id = state
        .list_user_auth_by_ids(&user_ids)
        .await?
        .into_iter()
        .map(|user| (user.id.clone(), user))
        .collect::<std::collections::BTreeMap<_, _>>();
    let wallet_by_user_id = state
        .list_wallet_snapshots_by_user_ids(&user_ids)
        .await?
        .into_iter()
        .filter_map(|wallet| wallet.user_id.clone().map(|user_id| (user_id, wallet)))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut payload = Vec::with_capacity(paged_rows.len());
    for row in paged_rows {
        let auth = auth_by_user_id.get(&row.id);
        let unlimited = wallet_by_user_id
            .get(&row.id)
            .is_some_and(|wallet| wallet.limit_mode.eq_ignore_ascii_case("unlimited"));
        payload.push(json!({
            "id": row.id,
            "email": row.email,
            "username": row.username,
            "role": row.role,
            "allowed_providers": row.allowed_providers,
            "allowed_api_formats": row.allowed_api_formats,
            "allowed_models": row.allowed_models,
            "rate_limit": row.rate_limit,
            "unlimited": unlimited,
            "is_active": row.is_active,
            "created_at": format_optional_datetime_iso8601(auth.as_ref().and_then(|user| user.created_at)),
            "updated_at": serde_json::Value::Null,
            "last_login_at": format_optional_datetime_iso8601(
                auth.as_ref().and_then(|user| user.last_login_at),
            ),
        }));
    }

    Ok(Json(payload).into_response())
}

async fn build_admin_get_user_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_detail_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };
    let Some(user) = state.find_user_auth_by_id(&user_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    };

    let wallet = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(
            &user_id,
        ))
        .await?;
    let export_row = find_admin_export_user(state, &user_id).await?;
    let unlimited = wallet
        .as_ref()
        .is_some_and(|wallet| wallet.limit_mode.eq_ignore_ascii_case("unlimited"));
    Ok(Json(build_admin_user_payload(
        &user,
        export_row.as_ref().and_then(|row| row.rate_limit),
        unlimited,
    ))
    .into_response())
}

async fn build_admin_create_user_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let _ = request_context;
    if !state.has_auth_user_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户",
        ));
    }
    if !state.has_auth_wallet_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法初始化用户钱包",
        ));
    }
    let Some(request_body) = request_body else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "请求数据验证失败" })),
        )
            .into_response());
    };
    let payload = match serde_json::from_slice::<AdminCreateUserRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };

    let email = match normalize_admin_optional_user_email(payload.email.as_deref()) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let username = match normalize_admin_username(&payload.username) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let role = match normalize_admin_user_role(payload.role.as_deref()) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let password_policy = admin_user_password_policy(state).await?;
    if let Err(detail) = validate_admin_user_password(&payload.password, &password_policy) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": detail })),
        )
            .into_response());
    }
    if payload.rate_limit.is_some_and(|value| value < 0) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "rate_limit 必须大于等于 0" })),
        )
            .into_response());
    }
    if payload
        .initial_gift_usd
        .is_some_and(|value| !value.is_finite() || !(0.0..=10000.0).contains(&value))
    {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "初始赠款必须在 0-10000 范围内" })),
        )
            .into_response());
    }
    let allowed_providers =
        match normalize_admin_user_string_list(payload.allowed_providers, "allowed_providers") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        };
    let allowed_api_formats = match normalize_admin_user_api_formats(payload.allowed_api_formats) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let allowed_models =
        match normalize_admin_user_string_list(payload.allowed_models, "allowed_models") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        };

    if let Some(email) = email.as_deref() {
        if state.find_user_auth_by_identifier(email).await?.is_some() {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": format!("邮箱已存在: {email}") })),
            )
                .into_response());
        }
    }
    if state.find_user_auth_by_identifier(&username).await?.is_some() {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": format!("用户名已存在: {username}") })),
        )
            .into_response());
    }

    let password_hash = match bcrypt::hash(&payload.password, bcrypt::DEFAULT_COST) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "密码长度不能超过72字节" })),
            )
                .into_response())
        }
    };
    let initial_gift_usd = if payload.unlimited {
        0.0
    } else if let Some(value) = payload.initial_gift_usd {
        value
    } else {
        admin_default_user_initial_gift(
            state
                .read_system_config_json_value("default_user_initial_gift_usd")
                .await?
                .as_ref(),
        )
    };

    let Some(user) = state
        .create_local_auth_user_with_settings(
            email,
            false,
            username,
            password_hash,
            role,
            allowed_providers,
            allowed_api_formats,
            allowed_models,
            payload.rate_limit,
        )
        .await?
    else {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户",
        ));
    };

    if state
        .initialize_auth_user_wallet(&user.id, initial_gift_usd, payload.unlimited)
        .await?
        .is_none()
    {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法初始化用户钱包",
        ));
    }

    Ok(Json(build_admin_user_payload(
        &user,
        payload.rate_limit,
        payload.unlimited,
    ))
    .into_response())
}

async fn build_admin_update_user_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_detail_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };
    let Some(_existing_user) = state.find_user_auth_by_id(&user_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    };
    let Some(request_body) = request_body else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "请求数据验证失败" })),
        )
            .into_response());
    };
    let raw_payload = match serde_json::from_slice::<serde_json::Value>(request_body) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };
    let field_presence = AdminUpdateUserFieldPresence {
        allowed_providers: raw_payload.contains_key("allowed_providers"),
        allowed_api_formats: raw_payload.contains_key("allowed_api_formats"),
        allowed_models: raw_payload.contains_key("allowed_models"),
    };
    let payload = match serde_json::from_value::<AdminUpdateUserRequest>(serde_json::Value::Object(
        raw_payload.clone(),
    )) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };

    let email = match payload.email.as_deref() {
        Some(value) => match normalize_admin_optional_user_email(Some(value)) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => None,
    };
    if let Some(email) = email.as_deref() {
        if state.is_other_user_auth_email_taken(email, &user_id).await? {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": format!("邮箱已存在: {email}") })),
            )
                .into_response());
        }
    }

    let username = match payload.username.as_deref() {
        Some(value) => match normalize_admin_username(value) {
            Ok(value) => Some(value),
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => None,
    };
    if let Some(username) = username.as_deref() {
        if state.is_other_user_auth_username_taken(username, &user_id).await? {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": format!("用户名已存在: {username}") })),
            )
                .into_response());
        }
    }

    let role = match payload.role.as_deref() {
        Some(value) => match normalize_admin_user_role(Some(value)) {
            Ok(value) => Some(value),
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        },
        None => None,
    };
    if payload.rate_limit.is_some_and(|value| value < 0) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "rate_limit 必须大于等于 0" })),
        )
            .into_response());
    }
    let allowed_providers = if field_presence.allowed_providers {
        match normalize_admin_user_string_list(payload.allowed_providers, "allowed_providers") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        }
    } else {
        None
    };
    let allowed_api_formats = if field_presence.allowed_api_formats {
        match normalize_admin_user_api_formats(payload.allowed_api_formats) {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        }
    } else {
        None
    };
    let allowed_models = if field_presence.allowed_models {
        match normalize_admin_user_string_list(payload.allowed_models, "allowed_models") {
            Ok(value) => value,
            Err(detail) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": detail })),
                )
                    .into_response())
            }
        }
    } else {
        None
    };
    let needs_auth_user_write = email.is_some()
        || username.is_some()
        || payload.password.is_some()
        || role.is_some()
        || field_presence.allowed_providers
        || field_presence.allowed_api_formats
        || field_presence.allowed_models
        || payload.rate_limit.is_some()
        || payload.is_active.is_some();
    if needs_auth_user_write && !state.has_auth_user_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法更新用户",
        ));
    }
    if payload.unlimited.is_some() && !state.has_auth_wallet_write_capability() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法更新用户钱包",
        ));
    }

    if email.is_some() || username.is_some() {
        if state
            .update_local_auth_user_profile(&user_id, email.clone(), username.clone())
            .await?
            .is_none()
        {
            return Ok((
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": "用户不存在" })),
            )
                .into_response());
        }
    }

    if let Some(password) = payload.password.as_deref() {
        let password_policy = admin_user_password_policy(state).await?;
        if let Err(detail) = validate_admin_user_password(password, &password_policy) {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response());
        }
        let password_hash = match bcrypt::hash(password, bcrypt::DEFAULT_COST) {
            Ok(value) => value,
            Err(_) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "密码长度不能超过72字节" })),
                )
                    .into_response())
            }
        };
        if state
            .update_local_auth_user_password_hash(&user_id, password_hash, chrono::Utc::now())
            .await?
            .is_none()
        {
            return Ok((
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": "用户不存在" })),
            )
                .into_response());
        }
    }

    if role.is_some()
        || field_presence.allowed_providers
        || field_presence.allowed_api_formats
        || field_presence.allowed_models
        || payload.rate_limit.is_some()
        || payload.is_active.is_some()
    {
        if state
            .update_local_auth_user_admin_fields(
                &user_id,
                role,
                field_presence.allowed_providers,
                allowed_providers,
                field_presence.allowed_api_formats,
                allowed_api_formats,
                field_presence.allowed_models,
                allowed_models,
                payload.rate_limit,
                payload.is_active,
            )
            .await?
            .is_none()
        {
            return Ok((
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": "用户不存在" })),
            )
                .into_response());
        }
    }

    if let Some(unlimited) = payload.unlimited {
        match state
            .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&user_id))
            .await?
        {
            Some(wallet) => {
                let desired_limit_mode = if unlimited { "unlimited" } else { "finite" };
                if !wallet.limit_mode.eq_ignore_ascii_case(desired_limit_mode) {
                    if state
                        .update_auth_user_wallet_limit_mode(&user_id, desired_limit_mode)
                        .await?
                        .is_none()
                    {
                        return Ok(build_admin_users_maintenance_response());
                    }
                }
            }
            None => {
                if state
                    .initialize_auth_user_wallet(&user_id, 0.0, unlimited)
                    .await?
                    .is_none()
                {
                    return Ok(build_admin_users_maintenance_response());
                }
            }
        }
    }

    let Some(user) = state.find_user_auth_by_id(&user_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    };
    let wallet = state
        .find_wallet(aether_data::repository::wallet::WalletLookupKey::UserId(&user_id))
        .await?;
    let unlimited = wallet
        .as_ref()
        .is_some_and(|wallet| wallet.limit_mode.eq_ignore_ascii_case("unlimited"));
    let export_row = find_admin_export_user(state, &user_id).await?;
    let rate_limit = export_row
        .as_ref()
        .and_then(|row| row.rate_limit)
        .or(payload.rate_limit);

    Ok(Json(build_admin_user_payload(&user, rate_limit, unlimited)).into_response())
}

async fn build_admin_delete_user_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_detail_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };
    let Some(user) = state.find_user_auth_by_id(&user_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    };

    if user.role.eq_ignore_ascii_case("admin") && state.count_active_admin_users().await? <= 1 {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "不能删除最后一个管理员账户" })),
        )
            .into_response());
    }
    if state.count_user_pending_refunds(&user_id).await? > 0 {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "用户存在未完结退款，禁止删除" })),
        )
            .into_response());
    }
    if state.count_user_pending_payment_orders(&user_id).await? > 0 {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "用户存在未完结充值订单，禁止删除" })),
        )
            .into_response());
    }

    if !state.delete_local_auth_user(&user_id).await? {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    }

    Ok(Json(json!({ "message": "用户删除成功" })).into_response())
}

async fn build_admin_list_user_api_keys_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_api_keys_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };
    let Some(user) = state.find_user_auth_by_id(&user_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    };

    let active_filter =
        query_param_optional_bool(request_context.request_query_string.as_deref(), "is_active");
    let mut export_records = state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&user_id))
        .await?;
    if let Some(is_active) = active_filter {
        export_records.retain(|record| record.is_active == is_active);
    }

    let snapshot_ids = export_records
        .iter()
        .map(|record| record.api_key_id.clone())
        .collect::<Vec<_>>();
    let snapshot_by_id = state
        .read_auth_api_key_snapshots_by_ids(&snapshot_ids)
        .await?
        .into_iter()
        .map(|snapshot| (snapshot.api_key_id.clone(), snapshot))
        .collect::<std::collections::BTreeMap<_, _>>();

    let api_keys = export_records
        .into_iter()
        .map(|record| {
            let is_locked = snapshot_by_id
                .get(&record.api_key_id)
                .map(|snapshot| snapshot.api_key_is_locked)
                .unwrap_or(false);
            json!({
                "id": record.api_key_id,
                "name": record.name,
                "key_display": masked_user_api_key_display(state, record.key_encrypted.as_deref()),
                "is_active": record.is_active,
                "is_locked": is_locked,
                "total_requests": record.total_requests,
                "total_cost_usd": record.total_cost_usd,
                "rate_limit": record.rate_limit,
                "expires_at": format_optional_unix_secs_iso8601(record.expires_at_unix_secs),
                "last_used_at": serde_json::Value::Null,
                "created_at": serde_json::Value::Null,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "api_keys": api_keys,
        "total": api_keys.len(),
        "user_email": user.email,
        "username": user.username,
    }))
    .into_response())
}

async fn build_admin_create_user_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法创建用户 API Key",
        ));
    }

    let Some(user_id) = admin_user_id_from_api_keys_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };
    if state.find_user_auth_by_id(&user_id).await?.is_none() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    }

    let Some(request_body) = request_body else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "请求数据验证失败" })),
        )
            .into_response());
    };
    let payload = match serde_json::from_slice::<AdminCreateUserApiKeyRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };
    if payload.allowed_api_formats.is_some()
        || payload.allowed_models.is_some()
        || payload.expire_days.is_some()
        || payload.expires_at.is_some()
        || payload.initial_balance_usd.is_some()
        || payload.unlimited_balance.unwrap_or(false)
        || payload.is_standalone.unwrap_or(false)
        || payload.auto_delete_on_expiry.unwrap_or(false)
    {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "当前仅支持 name、rate_limit、allowed_providers 字段" })),
        )
            .into_response());
    }

    let name = match normalize_admin_optional_api_key_name(payload.name) {
        Ok(Some(value)) => value,
        Ok(None) => default_admin_user_api_key_name(),
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let allowed_providers = match normalize_admin_api_key_providers(payload.allowed_providers) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    let rate_limit = payload.rate_limit.unwrap_or(0);
    if rate_limit < 0 {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "rate_limit 必须大于等于 0" })),
        )
            .into_response());
    }

    let plaintext_key = generate_admin_user_api_key_plaintext();
    let Some(key_encrypted) = encrypt_catalog_secret_with_fallbacks(state, &plaintext_key) else {
        return Ok((
            http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": "API密钥加密失败" })),
        )
            .into_response());
    };

    let Some(created) = state
        .create_user_api_key(aether_data::repository::auth::CreateUserApiKeyRecord {
            user_id: user_id.clone(),
            api_key_id: uuid::Uuid::new_v4().to_string(),
            key_hash: hash_admin_user_api_key(&plaintext_key),
            key_encrypted: Some(key_encrypted),
            name: Some(name.clone()),
            rate_limit,
            concurrent_limit: 5,
        })
        .await?
    else {
        return Ok(build_admin_users_maintenance_response());
    };

    let created = if allowed_providers.is_some() {
        match state
            .set_user_api_key_allowed_providers(&user_id, &created.api_key_id, allowed_providers)
            .await?
        {
            Some(updated) => updated,
            None => created,
        }
    } else {
        created
    };

    Ok(Json(json!({
        "id": created.api_key_id,
        "key": plaintext_key,
        "name": created.name,
        "key_display": masked_user_api_key_display(state, created.key_encrypted.as_deref()),
        "rate_limit": created.rate_limit,
        "expires_at": format_optional_unix_secs_iso8601(created.expires_at_unix_secs),
        "created_at": chrono::Utc::now().to_rfc3339(),
        "message": "API Key创建成功，请妥善保存完整密钥",
    }))
    .into_response())
}

async fn build_admin_update_user_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法更新用户 API Key",
        ));
    }

    let Some((user_id, api_key_id)) = admin_user_api_key_parts(&request_context.request_path)
    else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id 或 key_id"));
    };
    let Some(request_body) = request_body else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "请求数据验证失败" })),
        )
            .into_response());
    };
    let payload = match serde_json::from_slice::<AdminUpdateUserApiKeyRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": "请求数据验证失败" })),
            )
                .into_response())
        }
    };
    let name = match normalize_admin_optional_api_key_name(payload.name) {
        Ok(value) => value,
        Err(detail) => {
            return Ok((
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response())
        }
    };
    if payload.rate_limit.is_some_and(|value| value < 0) {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "rate_limit 必须大于等于 0" })),
        )
            .into_response());
    }

    let Some(updated) = state
        .update_user_api_key_basic(aether_data::repository::auth::UpdateUserApiKeyBasicRecord {
            user_id,
            api_key_id: api_key_id.clone(),
            name,
            rate_limit: payload.rate_limit,
        })
        .await?
    else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response());
    };

    let is_locked = state
        .read_auth_api_key_snapshots_by_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .find(|snapshot| snapshot.api_key_id == api_key_id)
        .map(|snapshot| snapshot.api_key_is_locked)
        .unwrap_or(false);
    let mut payload = build_admin_user_api_key_detail_payload(state, &updated, is_locked);
    payload["message"] = json!("API Key更新成功");
    Ok(Json(payload).into_response())
}

async fn build_admin_delete_user_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法删除用户 API Key",
        ));
    }

    let Some((user_id, api_key_id)) = admin_user_api_key_parts(&request_context.request_path)
    else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id 或 key_id"));
    };

    match state.delete_user_api_key(&user_id, &api_key_id).await? {
        true => Ok(Json(json!({ "message": "API Key已删除" })).into_response()),
        false => Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response()),
    }
}

async fn build_admin_toggle_user_api_key_lock_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_auth_api_key_writer() {
        return Ok(build_admin_users_read_only_response(
            "当前为只读模式，无法锁定或解锁用户 API Key",
        ));
    }

    let Some((user_id, api_key_id)) = admin_user_api_key_lock_parts(&request_context.request_path)
    else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id 或 key_id"));
    };

    let Some(snapshot) = state
        .read_auth_api_key_snapshots_by_ids(std::slice::from_ref(&api_key_id))
        .await?
        .into_iter()
        .find(|snapshot| snapshot.user_id == user_id && snapshot.api_key_id == api_key_id)
    else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response());
    };

    if snapshot.api_key_is_standalone {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response());
    }

    let desired_is_locked = match request_body {
        None => !snapshot.api_key_is_locked,
        Some(body) if body.is_empty() => !snapshot.api_key_is_locked,
        Some(body) => match serde_json::from_slice::<AdminToggleUserApiKeyLockRequest>(body) {
            Ok(payload) => payload.locked.unwrap_or(!snapshot.api_key_is_locked),
            Err(_) => {
                return Ok((
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response())
            }
        },
    };

    if !state
        .set_user_api_key_locked(&user_id, &api_key_id, desired_is_locked)
        .await?
    {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response());
    }

    Ok(Json(json!({
        "id": api_key_id,
        "is_locked": desired_is_locked,
        "message": if desired_is_locked {
            "API密钥已锁定"
        } else {
            "API密钥已解锁"
        },
    }))
    .into_response())
}

async fn build_admin_list_user_sessions_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_sessions_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };

    if state.find_user_auth_by_id(&user_id).await?.is_none() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    }

    let sessions = state.list_user_sessions(&user_id).await?;
    let payload = sessions
        .into_iter()
        .map(|session| {
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
                "last_seen_at": format_optional_datetime_iso8601(session.last_seen_at),
                "created_at": format_required_session_datetime_iso8601(&session),
                "is_current": false,
                "revoked_at": format_optional_datetime_iso8601(session.revoked_at),
                "revoke_reason": session.revoke_reason,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(payload).into_response())
}

async fn build_admin_delete_user_session_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((user_id, session_id)) = admin_user_session_parts(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id 或 session_id"));
    };

    if state.find_user_auth_by_id(&user_id).await?.is_none() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    }

    if state.find_user_session(&user_id, &session_id).await?.is_none() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "会话不存在" })),
        )
            .into_response());
    }

    state
        .revoke_user_session(
            &user_id,
            &session_id,
            chrono::Utc::now(),
            "admin_session_revoked",
        )
        .await?;

    Ok(Json(json!({ "message": "用户设备已强制下线" })).into_response())
}

async fn build_admin_delete_user_sessions_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some(user_id) = admin_user_id_from_sessions_path(&request_context.request_path) else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id"));
    };

    if state.find_user_auth_by_id(&user_id).await?.is_none() {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "用户不存在" })),
        )
            .into_response());
    }

    let revoked_count = state
        .revoke_all_user_sessions(
            &user_id,
            chrono::Utc::now(),
            "admin_revoke_all_sessions",
        )
        .await?;

    Ok(Json(json!({
        "message": "已强制下线该用户所有设备",
        "revoked_count": revoked_count,
    }))
    .into_response())
}

async fn build_admin_reveal_user_api_key_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((user_id, key_id)) = admin_user_api_key_full_key_parts(&request_context.request_path)
    else {
        return Ok(build_admin_users_bad_request_response("缺少 user_id 或 key_id"));
    };

    let records = state
        .list_auth_api_key_export_records_by_user_ids(std::slice::from_ref(&user_id))
        .await?;
    let Some(record) = records.into_iter().find(|record| record.api_key_id == key_id) else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "API Key不存在或不属于该用户" })),
        )
            .into_response());
    };

    let Some(ciphertext) = record.key_encrypted.as_deref().map(str::trim) else {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "该密钥没有存储完整密钥信息" })),
        )
            .into_response());
    };
    if ciphertext.is_empty() {
        return Ok((
            http::StatusCode::BAD_REQUEST,
            Json(json!({ "detail": "该密钥没有存储完整密钥信息" })),
        )
            .into_response());
    }

    let Some(full_key) = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), ciphertext)
    else {
        return Ok((
            http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "detail": "解密密钥失败" })),
        )
            .into_response());
    };

    Ok(Json(json!({ "key": full_key })).into_response())
}

async fn maybe_build_local_admin_users_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("users_manage") {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("create_user") => Ok(Some(
            build_admin_create_user_response(state, request_context, request_body).await?,
        )),
        Some("list_users") => Ok(Some(
            build_admin_list_users_response(state, request_context).await?,
        )),
        Some("get_user") => Ok(Some(
            build_admin_get_user_response(state, request_context).await?,
        )),
        Some("update_user") => Ok(Some(
            build_admin_update_user_response(state, request_context, request_body).await?,
        )),
        Some("delete_user") => Ok(Some(
            build_admin_delete_user_response(state, request_context).await?,
        )),
        Some("list_user_sessions") => Ok(Some(
            build_admin_list_user_sessions_response(state, request_context).await?,
        )),
        Some("list_user_api_keys") => Ok(Some(
            build_admin_list_user_api_keys_response(state, request_context).await?,
        )),
        Some("create_user_api_key") => Ok(Some(
            build_admin_create_user_api_key_response(state, request_context, request_body).await?,
        )),
        Some("update_user_api_key") => Ok(Some(
            build_admin_update_user_api_key_response(state, request_context, request_body).await?,
        )),
        Some("delete_user_api_key") => Ok(Some(
            build_admin_delete_user_api_key_response(state, request_context).await?,
        )),
        Some("lock_user_api_key") => Ok(Some(
            build_admin_toggle_user_api_key_lock_response(state, request_context, request_body)
                .await?,
        )),
        Some("delete_user_session") => Ok(Some(
            build_admin_delete_user_session_response(state, request_context).await?,
        )),
        Some("delete_user_sessions") => Ok(Some(
            build_admin_delete_user_sessions_response(state, request_context).await?,
        )),
        Some("reveal_user_api_key") => Ok(Some(
            build_admin_reveal_user_api_key_response(state, request_context).await?,
        )),
        _ => {
            let path = request_context.request_path.as_str();
            let is_users_route = (request_context.request_method == http::Method::GET
                && matches!(path, "/api/admin/users" | "/api/admin/users/"))
                || (request_context.request_method == http::Method::POST
                    && matches!(path, "/api/admin/users" | "/api/admin/users/"))
                || ((request_context.request_method == http::Method::GET
                    || request_context.request_method == http::Method::PUT
                    || request_context.request_method == http::Method::DELETE)
                    && path.starts_with("/api/admin/users/")
                    && !path.ends_with("/sessions")
                    && !path.contains("/sessions/")
                    && !path.ends_with("/api-keys")
                    && !path.contains("/api-keys/")
                    && path.matches('/').count() == 4)
                || (request_context.request_method == http::Method::GET
                    && path.starts_with("/api/admin/users/")
                    && path.ends_with("/sessions")
                    && path.matches('/').count() == 5)
                || (request_context.request_method == http::Method::DELETE
                    && path.starts_with("/api/admin/users/")
                    && path.ends_with("/sessions")
                    && path.matches('/').count() == 5)
                || (request_context.request_method == http::Method::DELETE
                    && path.starts_with("/api/admin/users/")
                    && path.contains("/sessions/")
                    && path.matches('/').count() == 6)
                || ((request_context.request_method == http::Method::GET
                    || request_context.request_method == http::Method::POST)
                    && path.starts_with("/api/admin/users/")
                    && path.ends_with("/api-keys")
                    && path.matches('/').count() == 5)
                || ((request_context.request_method == http::Method::DELETE
                    || request_context.request_method == http::Method::PUT)
                    && path.starts_with("/api/admin/users/")
                    && path.contains("/api-keys/")
                    && !path.ends_with("/lock")
                    && !path.ends_with("/full-key")
                    && path.matches('/').count() == 6)
                || (request_context.request_method == http::Method::PATCH
                    && path.starts_with("/api/admin/users/")
                    && path.ends_with("/lock")
                    && path.matches('/').count() == 7)
                || (request_context.request_method == http::Method::GET
                    && path.starts_with("/api/admin/users/")
                    && path.ends_with("/full-key")
                    && path.matches('/').count() == 7);

            if !is_users_route {
                return Ok(None);
            }

            Ok(Some(build_admin_users_maintenance_response()))
        }
    }
}
