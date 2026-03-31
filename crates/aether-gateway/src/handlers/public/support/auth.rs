async fn build_auth_registration_settings_payload(
    state: &AppState,
) -> Result<serde_json::Value, GatewayError> {
    let enable_registration = state.read_system_config_json_value("enable_registration").await?;
    let require_email_verification = state
        .read_system_config_json_value("require_email_verification")
        .await?;
    let smtp_host = state.read_system_config_json_value("smtp_host").await?;
    let smtp_from_email = state.read_system_config_json_value("smtp_from_email").await?;
    let password_policy_level_config = state
        .read_system_config_json_value("password_policy_level")
        .await?;

    let email_configured = smtp_host
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        && smtp_from_email
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some();
    let enable_registration = system_config_bool(enable_registration.as_ref(), false);
    let require_email_verification =
        system_config_bool(require_email_verification.as_ref(), false) && email_configured;
    let password_policy_level = match system_config_string(password_policy_level_config.as_ref()) {
        Some(value) if matches!(value.as_str(), "weak" | "medium" | "strong") => value,
        _ => "weak".to_string(),
    };

    Ok(json!({
        "enable_registration": enable_registration,
        "require_email_verification": require_email_verification,
        "email_configured": email_configured,
        "password_policy_level": password_policy_level,
    }))
}

async fn build_auth_settings_payload(state: &AppState) -> Result<serde_json::Value, GatewayError> {
    let ldap_enabled_config = state
        .read_system_config_json_value("module.ldap.enabled")
        .await?;
    let ldap_config = state.get_ldap_module_config().await?;
    let ldap_enabled = module_available_from_env("LDAP_AVAILABLE", true)
        && system_config_bool(ldap_enabled_config.as_ref(), false)
        && ldap_config_is_enabled(ldap_config.as_ref());
    let ldap_exclusive = ldap_enabled
        && ldap_config
            .as_ref()
            .map(|config| config.is_exclusive)
            .unwrap_or(false);

    Ok(json!({
        "local_enabled": !ldap_exclusive,
        "ldap_enabled": ldap_enabled,
        "ldap_exclusive": ldap_exclusive,
    }))
}

fn ldap_config_is_enabled(
    config: Option<&aether_data::repository::auth_modules::StoredLdapModuleConfig>,
) -> bool {
    config.is_some_and(|config| config.is_enabled) && ldap_module_config_is_valid(config)
}

const AUTH_ACCESS_TOKEN_DEFAULT_EXPIRATION_HOURS: i64 = 24;
const AUTH_REFRESH_TOKEN_EXPIRATION_DAYS: i64 = 7;
const AUTH_EMAIL_VERIFICATION_PREFIX: &str = "email:verification:";
const AUTH_EMAIL_VERIFIED_PREFIX: &str = "email:verified:";
const AUTH_EMAIL_VERIFIED_TTL_SECS: u64 = 3600;
const AUTH_SMTP_TIMEOUT_SECS: u64 = 30;

fn build_auth_json_response(
    status: http::StatusCode,
    payload: serde_json::Value,
    set_cookie: Option<String>,
) -> Response<Body> {
    let mut response = (status, Json(payload)).into_response();
    if let Some(set_cookie) = set_cookie {
        if let Ok(value) = axum::http::HeaderValue::from_str(&set_cookie) {
            response
                .headers_mut()
                .append(axum::http::header::SET_COOKIE, value);
        }
    }
    response
}

fn build_auth_error_response(
    status: http::StatusCode,
    detail: impl Into<String>,
    clear_cookie: bool,
) -> Response<Body> {
    let cookie = clear_cookie.then(build_auth_refresh_cookie_clear_header);
    build_auth_json_response(status, json!({ "detail": detail.into() }), cookie)
}

fn auth_environment() -> String {
    std::env::var("ENVIRONMENT")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "development".to_string())
}

fn auth_jwt_secret() -> Result<String, String> {
    if let Ok(value) = std::env::var("JWT_SECRET_KEY") {
        let value = value.trim();
        if !value.is_empty() {
            return Ok(value.to_string());
        }
    }
    if auth_environment().eq_ignore_ascii_case("production") {
        return Err("JWT_SECRET_KEY 未配置".to_string());
    }
    Ok("aether-rust-dev-jwt-secret".to_string())
}

fn auth_access_token_expiry_hours() -> i64 {
    std::env::var("JWT_EXPIRATION_HOURS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(AUTH_ACCESS_TOKEN_DEFAULT_EXPIRATION_HOURS)
}

fn auth_verification_code_expire_minutes() -> i64 {
    std::env::var("VERIFICATION_CODE_EXPIRE_MINUTES")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(5)
}

fn auth_verification_send_cooldown_seconds() -> i64 {
    std::env::var("VERIFICATION_SEND_COOLDOWN")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(60)
}

fn auth_refresh_cookie_name() -> String {
    std::env::var("AUTH_REFRESH_COOKIE_NAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "aether_refresh_token".to_string())
}

fn auth_refresh_cookie_secure() -> bool {
    std::env::var("AUTH_REFRESH_COOKIE_SECURE")
        .ok()
        .map(|value| value.trim().eq_ignore_ascii_case("true"))
        .unwrap_or_else(|| auth_environment().eq_ignore_ascii_case("production"))
}

fn auth_refresh_cookie_samesite() -> &'static str {
    match std::env::var("AUTH_REFRESH_COOKIE_SAMESITE") {
        Ok(value) if value.trim().eq_ignore_ascii_case("strict") => "Strict",
        Ok(value) if value.trim().eq_ignore_ascii_case("none") => "None",
        Ok(value) if value.trim().eq_ignore_ascii_case("lax") => "Lax",
        _ if auth_environment().eq_ignore_ascii_case("production") => "None",
        _ => "Lax",
    }
}

fn build_auth_refresh_cookie_header(refresh_token: &str) -> String {
    let mut cookie = format!(
        "{}={}; Path=/api/auth; HttpOnly; SameSite={}; Max-Age={}",
        auth_refresh_cookie_name(),
        refresh_token,
        auth_refresh_cookie_samesite(),
        AUTH_REFRESH_TOKEN_EXPIRATION_DAYS * 24 * 60 * 60,
    );
    if auth_refresh_cookie_secure() {
        cookie.push_str("; Secure");
    }
    cookie
}

fn build_auth_refresh_cookie_clear_header() -> String {
    let mut cookie = format!(
        "{}=; Path=/api/auth; HttpOnly; SameSite={}; Max-Age=0",
        auth_refresh_cookie_name(),
        auth_refresh_cookie_samesite(),
    );
    if auth_refresh_cookie_secure() {
        cookie.push_str("; Secure");
    }
    cookie
}

fn auth_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

fn auth_non_empty_string(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn extract_bearer_token(headers: &http::HeaderMap) -> Option<String> {
    let value = crate::gateway::headers::header_value_str(headers, http::header::AUTHORIZATION.as_str())?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    auth_non_empty_string(Some(token.to_string()))
}

fn extract_cookie_value(headers: &http::HeaderMap, cookie_name: &str) -> Option<String> {
    let header = crate::gateway::headers::header_value_str(headers, http::header::COOKIE.as_str())?;
    for pair in header.split(';') {
        let (name, value) = pair.trim().split_once('=')?;
        if name.trim() == cookie_name {
            return auth_non_empty_string(Some(value.to_string()));
        }
    }
    None
}

fn extract_client_device_id(
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Result<String, Response<Body>> {
    let header_value = crate::gateway::headers::header_value_str(headers, "x-client-device-id");
    let query_value = request_context.request_query_string.as_deref().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "client_device_id")
            .map(|(_, value)| value.into_owned())
    });
    let candidate = header_value.or(query_value).unwrap_or_default();
    let candidate = candidate.trim();
    if candidate.is_empty()
        || candidate.len() > 128
        || !candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "缺少或无效的设备标识",
            false,
        ));
    }
    Ok(candidate.to_string())
}

fn auth_user_agent(headers: &http::HeaderMap) -> Option<String> {
    crate::gateway::headers::header_value_str(headers, http::header::USER_AGENT.as_str())
        .map(|value| value.chars().take(1000).collect())
}

fn auth_client_ip(headers: &http::HeaderMap) -> Option<String> {
    crate::gateway::headers::header_value_str(headers, "x-forwarded-for")
        .and_then(|value| value.split(',').next().map(|segment| segment.trim().to_string()))
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(45).collect())
        .or_else(|| {
            crate::gateway::headers::header_value_str(headers, "x-real-ip")
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(45).collect())
        })
}

fn normalize_auth_login_identifier(value: &str) -> String {
    let normalized = value.trim();
    if normalized.contains('@') {
        normalized.to_ascii_lowercase()
    } else {
        normalized.to_string()
    }
}

fn validate_auth_login_password(password: &str) -> Result<(), String> {
    if password.is_empty() {
        return Err("密码不能为空".to_string());
    }
    if password.len() > 72 || password.as_bytes().len() > 72 {
        return Err("密码长度不能超过72字节".to_string());
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct AuthLoginRequest {
    email: String,
    password: String,
    #[serde(default = "default_auth_login_type")]
    auth_type: String,
}

#[derive(Debug, Deserialize)]
struct AuthRegisterRequest {
    email: Option<String>,
    username: String,
    password: String,
}

fn default_auth_login_type() -> String {
    "local".to_string()
}

#[derive(Debug, Deserialize)]
struct AuthEmailRequest {
    email: String,
}

#[derive(Debug, Deserialize)]
struct AuthVerifyEmailRequest {
    email: String,
    code: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredAuthEmailVerificationCode {
    code: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct AuthSmtpConfig {
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    use_tls: bool,
    use_ssl: bool,
    from_email: String,
    from_name: String,
}

#[derive(Debug, Clone)]
struct AuthComposedEmail {
    to_email: String,
    subject: String,
    html_body: String,
    text_body: String,
}

#[derive(Debug, Clone)]
struct AuthLdapRuntimeConfig {
    server_url: String,
    bind_dn: String,
    bind_password: String,
    base_dn: String,
    user_search_filter: String,
    username_attr: String,
    email_attr: String,
    display_name_attr: String,
    use_starttls: bool,
    connect_timeout_secs: u64,
}

#[derive(Debug, Clone)]
struct AuthLdapAuthenticatedUser {
    username: String,
    ldap_username: String,
    ldap_dn: String,
    email: String,
    display_name: String,
}

fn normalize_auth_email(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty() {
        return None;
    }
    let pattern = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        .expect("email regex should compile");
    pattern.is_match(&value).then_some(value)
}

fn normalize_auth_optional_email(value: Option<&str>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    normalize_auth_email(value)
        .map(Some)
        .ok_or_else(|| "邮箱格式无效".to_string())
}

fn validate_auth_register_username(value: &str) -> Result<String, String> {
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
    let pattern =
        Regex::new(r"^[a-zA-Z0-9_.\-]+$").expect("username regex should compile");
    if !pattern.is_match(value) {
        return Err("用户名只能包含字母、数字、下划线、连字符和点号".to_string());
    }
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "admin"
            | "root"
            | "system"
            | "api"
            | "test"
            | "demo"
            | "user"
            | "guest"
            | "bot"
            | "webhook"
            | "support"
    ) {
        return Err("该用户名为系统保留用户名".to_string());
    }
    Ok(value.to_string())
}

fn validate_auth_register_password(password: &str, policy: &str) -> Result<(), String> {
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
        if !password
            .chars()
            .any(|ch| r#"!@#$%^&*()_+-=[]{};:'",.<>?/\|`~"#.contains(ch))
        {
            return Err("密码必须包含至少一个特殊字符".to_string());
        }
    }
    Ok(())
}

fn system_config_f64(value: Option<&serde_json::Value>, default: f64) -> f64 {
    match value {
        Some(serde_json::Value::Number(value)) => value.as_f64().unwrap_or(default),
        Some(serde_json::Value::String(value)) => value.trim().parse::<f64>().unwrap_or(default),
        _ => default,
    }
}

fn system_config_u16(value: Option<&serde_json::Value>, default: u16) -> u16 {
    match value {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or(default),
        Some(serde_json::Value::String(value)) => {
            value.trim().parse::<u16>().unwrap_or(default)
        }
        _ => default,
    }
}

fn system_config_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    match value {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase())
            .collect(),
        Some(serde_json::Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase())
            .collect(),
        _ => Vec::new(),
    }
}

fn auth_email_verification_key(email: &str) -> String {
    format!("{AUTH_EMAIL_VERIFICATION_PREFIX}{email}")
}

fn auth_email_verified_key(email: &str) -> String {
    format!("{AUTH_EMAIL_VERIFIED_PREFIX}{email}")
}

fn load_auth_email_verification_entry_for_tests(_state: &AppState, _key: &str) -> Option<String> {
    #[cfg(test)]
    {
        return _state.auth_email_verification_store.as_ref().and_then(|store| {
            store
                .lock()
                .expect("auth email verification store should lock")
                .get(_key)
                .cloned()
        });
    }

    #[allow(unreachable_code)]
    None
}

fn save_auth_email_verification_entry_for_tests(
    _state: &AppState,
    _key: &str,
    _value: &str,
) -> bool {
    #[cfg(test)]
    {
        if let Some(store) = _state.auth_email_verification_store.as_ref() {
            store
                .lock()
                .expect("auth email verification store should lock")
                .insert(_key.to_string(), _value.to_string());
            return true;
        }
    }

    false
}

fn delete_auth_email_verification_entries_for_tests(_state: &AppState, _keys: &[String]) -> bool {
    #[cfg(test)]
    {
        if let Some(store) = _state.auth_email_verification_store.as_ref() {
            let mut guard = store
                .lock()
                .expect("auth email verification store should lock");
            for key in _keys {
                guard.remove(key);
            }
            return true;
        }
    }

    false
}

fn record_auth_email_delivery_for_tests(_state: &AppState, _payload: serde_json::Value) -> bool {
    #[cfg(test)]
    {
        if let Some(store) = _state.auth_email_delivery_store.as_ref() {
            store
                .lock()
                .expect("auth email delivery store should lock")
                .push(_payload);
            return true;
        }
    }

    false
}

fn generate_auth_verification_code() -> String {
    format!("{:06}", uuid::Uuid::new_v4().as_u128() % 1_000_000)
}

fn render_auth_template_string(
    template: &str,
    variables: &std::collections::BTreeMap<String, String>,
    escape_html: bool,
) -> Result<String, GatewayError> {
    let mut rendered = template.to_string();
    for (key, value) in variables {
        let pattern = regex::Regex::new(&format!(r"\{{\{{\s*{}\s*\}}\}}", regex::escape(key)))
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let replacement = if escape_html {
            escape_admin_email_template_html(value)
        } else {
            value.clone()
        };
        rendered = pattern
            .replace_all(&rendered, replacement.as_str())
            .into_owned();
    }
    Ok(rendered)
}

fn auth_encode_mime_header(value: &str) -> String {
    if value.is_ascii() {
        return value.to_string();
    }
    format!(
        "=?UTF-8?B?{}?=",
        base64::engine::general_purpose::STANDARD.encode(value.as_bytes())
    )
}

fn auth_wrap_base64(value: &str) -> String {
    let mut wrapped = String::new();
    for chunk in value.as_bytes().chunks(76) {
        wrapped.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        wrapped.push_str("\r\n");
    }
    wrapped
}

fn auth_build_verification_text_body(
    app_name: &str,
    email: &str,
    code: &str,
    expire_minutes: i64,
) -> String {
    format!(
        "{app_name}\n\n您的验证码是：{code}\n目标邮箱：{email}\n有效期：{expire_minutes} 分钟\n\n如果这不是您本人的操作，请忽略此邮件。"
    )
}

fn auth_build_tls_config() -> std::sync::Arc<rustls::ClientConfig> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    std::sync::Arc::new(config)
}

fn auth_resolve_server_name(
    host: &str,
) -> Result<rustls::pki_types::ServerName<'static>, GatewayError> {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(rustls::pki_types::ServerName::from(ip));
    }
    rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|err| GatewayError::Internal(err.to_string()))
}

fn auth_connect_tcp_stream(config: &AuthSmtpConfig) -> Result<std::net::TcpStream, GatewayError> {
    let stream = std::net::TcpStream::connect((config.host.as_str(), config.port))
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(AUTH_SMTP_TIMEOUT_SECS)))
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(AUTH_SMTP_TIMEOUT_SECS)))
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    Ok(stream)
}

fn auth_wrap_tls_stream(
    stream: std::net::TcpStream,
    host: &str,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>, GatewayError> {
    let server_name = auth_resolve_server_name(host)?;
    let connection =
        rustls::ClientConnection::new(auth_build_tls_config(), server_name)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
    Ok(rustls::StreamOwned::new(connection, stream))
}

fn auth_smtp_read_response<T: std::io::BufRead>(
    reader: &mut T,
) -> Result<(u16, String), GatewayError> {
    let mut message = String::new();
    let code = loop {
        let parsed_code;
        let continuation;
        let trimmed;
        {
            let mut line = String::new();
            let bytes = reader
                .read_line(&mut line)
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            if bytes == 0 {
                return Err(GatewayError::Internal(
                    "smtp connection closed unexpectedly".to_string(),
                ));
            }
            trimmed = line.trim_end_matches(['\r', '\n']).to_string();
            if trimmed.len() < 3 {
                return Err(GatewayError::Internal("invalid smtp response".to_string()));
            }
            parsed_code = trimmed[..3]
                .parse::<u16>()
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            continuation = trimmed.as_bytes().get(3).copied() == Some(b'-');
        }
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(&trimmed);
        if !continuation {
            break parsed_code;
        }
    };
    Ok((code, message))
}

fn auth_smtp_expect<T: std::io::BufRead>(
    reader: &mut T,
    allowed_codes: &[u16],
) -> Result<String, GatewayError> {
    let (code, message) = auth_smtp_read_response(reader)?;
    if allowed_codes.contains(&code) {
        return Ok(message);
    }
    Err(GatewayError::Internal(format!(
        "unexpected smtp response {code}: {message}"
    )))
}

fn auth_smtp_write_line<T: std::io::Write>(
    writer: &mut T,
    line: &str,
) -> Result<(), GatewayError> {
    writer
        .write_all(line.as_bytes())
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    writer
        .write_all(b"\r\n")
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    writer
        .flush()
        .map_err(|err| GatewayError::Internal(err.to_string()))
}

fn auth_smtp_send_command<S: std::io::Read + std::io::Write>(
    reader: &mut std::io::BufReader<S>,
    command: &str,
    allowed_codes: &[u16],
) -> Result<String, GatewayError> {
    auth_smtp_write_line(reader.get_mut(), command)?;
    auth_smtp_expect(reader, allowed_codes)
}

fn auth_build_email_message(config: &AuthSmtpConfig, email: &AuthComposedEmail) -> String {
    let boundary = format!("aether-{}", uuid::Uuid::new_v4().simple());
    let text_body = auth_wrap_base64(
        &base64::engine::general_purpose::STANDARD.encode(email.text_body.as_bytes()),
    );
    let html_body = auth_wrap_base64(
        &base64::engine::general_purpose::STANDARD.encode(email.html_body.as_bytes()),
    );
    let from_header = if config.from_name.trim().is_empty() {
        format!("<{}>", config.from_email)
    } else {
        format!(
            "{} <{}>",
            auth_encode_mime_header(config.from_name.trim()),
            config.from_email
        )
    };
    format!(
        "From: {from_header}\r\nTo: <{to_email}>\r\nSubject: {subject}\r\nMIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=\"{boundary}\"\r\n\r\n--{boundary}\r\nContent-Type: text/plain; charset=\"utf-8\"\r\nContent-Transfer-Encoding: base64\r\n\r\n{text_body}--{boundary}\r\nContent-Type: text/html; charset=\"utf-8\"\r\nContent-Transfer-Encoding: base64\r\n\r\n{html_body}--{boundary}--\r\n",
        to_email = email.to_email,
        subject = auth_encode_mime_header(&email.subject),
    )
}

fn auth_smtp_authenticate<S: std::io::Read + std::io::Write>(
    reader: &mut std::io::BufReader<S>,
    config: &AuthSmtpConfig,
) -> Result<(), GatewayError> {
    let Some(username) = config
        .user
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let password = config.password.as_deref().unwrap_or("");
    auth_smtp_send_command(reader, "AUTH LOGIN", &[334])?;
    auth_smtp_send_command(
        reader,
        &base64::engine::general_purpose::STANDARD.encode(username.as_bytes()),
        &[334],
    )?;
    auth_smtp_send_command(
        reader,
        &base64::engine::general_purpose::STANDARD.encode(password.as_bytes()),
        &[235],
    )?;
    Ok(())
}

fn auth_smtp_deliver_message<S: std::io::Read + std::io::Write>(
    reader: &mut std::io::BufReader<S>,
    config: &AuthSmtpConfig,
    email: &AuthComposedEmail,
) -> Result<(), GatewayError> {
    auth_smtp_send_command(
        reader,
        &format!("MAIL FROM:<{}>", config.from_email),
        &[250],
    )?;
    auth_smtp_send_command(reader, &format!("RCPT TO:<{}>", email.to_email), &[250, 251])?;
    auth_smtp_send_command(reader, "DATA", &[354])?;
    let message = auth_build_email_message(config, email);
    reader
        .get_mut()
        .write_all(message.as_bytes())
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    reader
        .get_mut()
        .write_all(b"\r\n.\r\n")
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    reader
        .get_mut()
        .flush()
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let _ = auth_smtp_expect(reader, &[250])?;
    let _ = auth_smtp_send_command(reader, "QUIT", &[221]);
    Ok(())
}

fn auth_smtp_send_message<S: std::io::Read + std::io::Write>(
    reader: &mut std::io::BufReader<S>,
    config: &AuthSmtpConfig,
    email: &AuthComposedEmail,
) -> Result<(), GatewayError> {
    auth_smtp_send_command(reader, "EHLO aether.local", &[250])?;
    auth_smtp_authenticate(reader, config)?;
    auth_smtp_deliver_message(reader, config, email)
}

fn send_auth_email_blocking(
    config: AuthSmtpConfig,
    email: AuthComposedEmail,
) -> Result<(), GatewayError> {
    if config.use_ssl {
        let stream = auth_connect_tcp_stream(&config)?;
        let tls_stream = auth_wrap_tls_stream(stream, &config.host)?;
        let mut reader = std::io::BufReader::new(tls_stream);
        let _ = auth_smtp_expect(&mut reader, &[220])?;
        return auth_smtp_send_message(&mut reader, &config, &email);
    }

    let stream = auth_connect_tcp_stream(&config)?;
    let mut reader = std::io::BufReader::new(stream);
    let _ = auth_smtp_expect(&mut reader, &[220])?;
    let _ = auth_smtp_send_command(&mut reader, "EHLO aether.local", &[250])?;
    if config.use_tls {
        let _ = auth_smtp_send_command(&mut reader, "STARTTLS", &[220])?;
        let stream = reader.into_inner();
        let tls_stream = auth_wrap_tls_stream(stream, &config.host)?;
        let mut reader = std::io::BufReader::new(tls_stream);
        return auth_smtp_send_message(&mut reader, &config, &email);
    }

    auth_smtp_authenticate(&mut reader, &config)?;
    auth_smtp_deliver_message(&mut reader, &config, &email)
}

fn base64url_encode(bytes: &[u8]) -> String {
    use base64::Engine;

    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn base64url_decode(value: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;

    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| "无效的Token".to_string())
}

fn create_auth_token(
    token_type: &str,
    mut payload: serde_json::Map<String, serde_json::Value>,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> Result<String, String> {
    use hmac::Mac;

    let secret = auth_jwt_secret()?;
    let header = serde_json::json!({ "alg": "HS256", "typ": "JWT" });
    payload.insert("exp".to_string(), json!(expires_at.timestamp()));
    payload.insert("type".to_string(), json!(token_type));
    let header_segment = base64url_encode(
        serde_json::to_vec(&header)
            .map_err(|_| "无法序列化JWT header".to_string())?
            .as_slice(),
    );
    let payload_segment = base64url_encode(
        serde_json::to_vec(&payload)
            .map_err(|_| "无法序列化JWT payload".to_string())?
            .as_slice(),
    );
    let signing_input = format!("{header_segment}.{payload_segment}");
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| "JWT secret 无效".to_string())?;
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(format!(
        "{header_segment}.{payload_segment}.{}",
        base64url_encode(signature.as_slice())
    ))
}

fn decode_auth_token(
    token: &str,
    expected_type: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    use hmac::Mac;

    let secret = auth_jwt_secret()?;
    let mut parts = token.split('.');
    let Some(header_segment) = parts.next() else {
        return Err("无效的Token".to_string());
    };
    let Some(payload_segment) = parts.next() else {
        return Err("无效的Token".to_string());
    };
    let Some(signature_segment) = parts.next() else {
        return Err("无效的Token".to_string());
    };
    if parts.next().is_some() {
        return Err("无效的Token".to_string());
    }

    let signing_input = format!("{header_segment}.{payload_segment}");
    let signature = base64url_decode(signature_segment)?;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| "JWT secret 无效".to_string())?;
    mac.update(signing_input.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| "无效的Token".to_string())?;

    let payload_bytes = base64url_decode(payload_segment)?;
    let payload = serde_json::from_slice::<serde_json::Value>(&payload_bytes)
        .map_err(|_| "无效的Token".to_string())?;
    let payload = payload
        .as_object()
        .cloned()
        .ok_or_else(|| "无效的Token".to_string())?;
    let actual_type = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if actual_type != expected_type {
        return Err(format!("Token类型错误: 期望 {expected_type}, 实际 {actual_type}"));
    }
    let exp = payload
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "无效的Token".to_string())?;
    if exp <= auth_now().timestamp() {
        return Err("Token已过期".to_string());
    }
    Ok(payload)
}

fn auth_token_identity_matches_user(
    payload: &serde_json::Map<String, serde_json::Value>,
    user: &aether_data::repository::users::StoredUserAuthRecord,
) -> bool {
    if let Some(token_email) = payload.get("email").and_then(serde_json::Value::as_str) {
        if user.email.as_deref().is_some_and(|email| email != token_email) {
            return false;
        }
    }

    let Some(token_created_at) = payload.get("created_at").and_then(serde_json::Value::as_str) else {
        return true;
    };
    let Some(user_created_at) = user.created_at else {
        return true;
    };
    let Ok(token_created_at) = chrono::DateTime::parse_from_rfc3339(token_created_at) else {
        return false;
    };
    let token_created_at = token_created_at.with_timezone(&chrono::Utc);
    (user_created_at - token_created_at).num_seconds().abs() <= 1
}

fn build_auth_wallet_summary_payload(
    wallet: Option<&aether_data::repository::wallet::StoredWalletSnapshot>,
) -> serde_json::Value {
    let recharge_balance = wallet.map(|value| value.balance).unwrap_or(0.0);
    let gift_balance = wallet.map(|value| value.gift_balance).unwrap_or(0.0);
    let limit_mode = wallet
        .map(|value| value.limit_mode.clone())
        .unwrap_or_else(|| "finite".to_string());
    json!({
        "id": wallet.map(|value| value.id.clone()),
        "balance": recharge_balance + gift_balance,
        "recharge_balance": recharge_balance,
        "gift_balance": gift_balance,
        "refundable_balance": recharge_balance,
        "currency": wallet.map(|value| value.currency.clone()).unwrap_or_else(|| "USD".to_string()),
        "status": wallet.map(|value| value.status.clone()).unwrap_or_else(|| "active".to_string()),
        "limit_mode": limit_mode,
        "unlimited": wallet
            .map(|value| value.limit_mode.eq_ignore_ascii_case("unlimited"))
            .unwrap_or(false),
        "total_recharged": wallet.map(|value| value.total_recharged).unwrap_or(0.0),
        "total_consumed": wallet.map(|value| value.total_consumed).unwrap_or(0.0),
        "total_refunded": wallet.map(|value| value.total_refunded).unwrap_or(0.0),
        "total_adjusted": wallet.map(|value| value.total_adjusted).unwrap_or(0.0),
        "updated_at": wallet
            .and_then(|value| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(value.updated_at_unix_secs as i64, 0)
            })
            .map(|value| value.to_rfc3339()),
    })
}

fn build_auth_me_payload(
    user: &aether_data::repository::users::StoredUserAuthRecord,
    wallet: Option<&aether_data::repository::wallet::StoredWalletSnapshot>,
) -> serde_json::Value {
    let billing = build_auth_wallet_summary_payload(wallet);
    json!({
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "role": user.role,
        "is_active": user.is_active,
        "billing": billing,
        "allowed_providers": user.allowed_providers,
        "allowed_api_formats": user.allowed_api_formats,
        "allowed_models": user.allowed_models,
        "created_at": user.created_at.map(|value| value.to_rfc3339()),
        "last_login_at": user.last_login_at.map(|value| value.to_rfc3339()),
        "auth_source": user.auth_source,
    })
}

#[derive(Debug, Clone)]
struct AuthenticatedLocalUserContext {
    user: aether_data::repository::users::StoredUserAuthRecord,
    session_id: String,
}

async fn resolve_authenticated_local_user(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Result<AuthenticatedLocalUserContext, Response<Body>> {
    let Some(token) = extract_bearer_token(headers) else {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "缺少用户凭证",
            false,
        ));
    };
    let claims = match decode_auth_token(&token, "access") {
        Ok(value) => value,
        Err(detail) => {
            return Err(build_auth_error_response(
                http::StatusCode::UNAUTHORIZED,
                detail,
                false,
            ))
        }
    };
    let Some(user_id) = claims.get("user_id").and_then(serde_json::Value::as_str) else {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "无效的用户令牌",
            false,
        ));
    };
    let Some(session_id) = claims.get("session_id").and_then(serde_json::Value::as_str) else {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            false,
        ));
    };
    let user = match state.find_user_auth_by_id(user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return Err(build_auth_error_response(
                http::StatusCode::FORBIDDEN,
                "用户不存在或已禁用",
                false,
            ))
        }
        Err(err) => {
            return Err(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth user lookup failed: {err:?}"),
                false,
            ))
        }
    };
    if !user.is_active || user.is_deleted {
        return Err(build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "用户不存在或已禁用",
            false,
        ));
    }
    if !auth_token_identity_matches_user(&claims, &user) {
        return Err(build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "无效的用户令牌",
            false,
        ));
    }
    let client_device_id = match extract_client_device_id(request_context, headers) {
        Ok(value) => value,
        Err(response) => return Err(response),
    };
    let now = auth_now();
    let Some(session) = (match state.find_user_session(user_id, session_id).await {
        Ok(value) => value,
        Err(err) => {
            return Err(build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth session lookup failed: {err:?}"),
                false,
            ))
        }
    }) else {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            false,
        ));
    };
    if session.is_revoked() || session.is_expired(now) {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            false,
        ));
    }
    if session.client_device_id != client_device_id {
        return Err(build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "设备标识与登录会话不匹配",
            false,
        ));
    }
    if session.should_touch(now) {
        let _ = state
            .touch_user_session(
                user_id,
                session_id,
                now,
                None,
                auth_user_agent(headers).as_deref(),
            )
            .await;
    }
    Ok(AuthenticatedLocalUserContext {
        user,
        session_id: session.id,
    })
}

async fn handle_auth_me(
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
        build_auth_me_payload(&auth.user, wallet.as_ref()),
        None,
    )
}

async fn handle_auth_refresh(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if crate::gateway::headers::header_value_str(headers, http::header::CONTENT_LENGTH.as_str())
        .as_deref()
        .is_some_and(|value| value.trim() != "0")
    {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "刷新接口不接受请求体，请使用 Cookie",
            true,
        );
    }
    let cookie_name = auth_refresh_cookie_name();
    let Some(refresh_token) = extract_cookie_value(headers, &cookie_name) else {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "缺少刷新令牌",
            true,
        );
    };
    let claims = match decode_auth_token(&refresh_token, "refresh") {
        Ok(value) => value,
        Err(detail) => {
            let detail = if detail == "Token已过期" || detail == "无效的Token" {
                "刷新令牌失败".to_string()
            } else {
                detail
            };
            return build_auth_error_response(http::StatusCode::UNAUTHORIZED, detail, true);
        }
    };
    let Some(user_id) = claims.get("user_id").and_then(serde_json::Value::as_str) else {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "无效的刷新令牌",
            true,
        );
    };
    let Some(session_id) = claims.get("session_id").and_then(serde_json::Value::as_str) else {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "无效的刷新令牌",
            true,
        );
    };
    let user = match state.find_user_auth_by_id(user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return build_auth_error_response(
                http::StatusCode::UNAUTHORIZED,
                "无效的刷新令牌",
                true,
            )
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth user lookup failed: {err:?}"),
                true,
            )
        }
    };
    if !user.is_active {
        return build_auth_error_response(http::StatusCode::FORBIDDEN, "用户已禁用", true);
    }
    if user.is_deleted {
        return build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "用户不存在或已禁用",
            true,
        );
    }
    if !auth_token_identity_matches_user(&claims, &user) {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "无效的刷新令牌",
            true,
        );
    }
    let client_device_id = match extract_client_device_id(request_context, headers) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let now = auth_now();
    let Some(session) = (match state.find_user_session(user_id, session_id).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth session lookup failed: {err:?}"),
                true,
            )
        }
    }) else {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            true,
        );
    };
    if session.is_revoked() || session.is_expired(now) {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            true,
        );
    }
    if session.client_device_id != client_device_id {
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "设备标识与登录会话不匹配",
            true,
        );
    }
    let (is_valid, is_prev) = session.verify_refresh_token(&refresh_token, now);
    if !is_valid {
        let _ = state
            .revoke_user_session(user_id, session_id, now, "refresh_token_reused")
            .await;
        return build_auth_error_response(
            http::StatusCode::UNAUTHORIZED,
            "登录会话已失效，请重新登录",
            true,
        );
    }

    let access_expires_at = now + chrono::Duration::hours(auth_access_token_expiry_hours());
    let access_token = match create_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!(session.id)),
        ]),
        access_expires_at,
    ) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                detail,
                true,
            )
        }
    };

    let mut set_cookie = None;
    if !is_prev {
        let new_refresh_token = match create_auth_token(
            "refresh",
            serde_json::Map::from_iter([
                ("user_id".to_string(), json!(user.id)),
                (
                    "created_at".to_string(),
                    json!(user.created_at.map(|value| value.to_rfc3339())),
                ),
                ("session_id".to_string(), json!(session.id)),
                ("jti".to_string(), json!(uuid::Uuid::new_v4().to_string())),
            ]),
            now + chrono::Duration::days(AUTH_REFRESH_TOKEN_EXPIRATION_DAYS),
        ) {
            Ok(value) => value,
            Err(detail) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    detail,
                    true,
                )
            }
        };
        let rotated = state
            .rotate_user_session_refresh_token(
                user_id,
                session_id,
                &session.refresh_token_hash,
                &crate::gateway::data::StoredUserSessionRecord::hash_refresh_token(
                    &new_refresh_token,
                ),
                now,
                now + chrono::Duration::days(AUTH_REFRESH_TOKEN_EXPIRATION_DAYS),
                None,
                auth_user_agent(headers).as_deref(),
            )
            .await;
        if rotated.ok() != Some(true) {
            return build_auth_error_response(
                http::StatusCode::UNAUTHORIZED,
                "刷新令牌失败",
                true,
            );
        }
        set_cookie = Some(build_auth_refresh_cookie_header(&new_refresh_token));
    }

    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": auth_access_token_expiry_hours() * 60 * 60,
        }),
        set_cookie,
    )
}

async fn auth_local_login_allowed_for_user(
    state: &AppState,
    user: &aether_data::repository::users::StoredUserAuthRecord,
) -> Result<bool, GatewayError> {
    let ldap_enabled_config = state
        .read_system_config_json_value("module.ldap.enabled")
        .await?;
    let ldap_config = state.get_ldap_module_config().await?;
    let ldap_enabled = module_available_from_env("LDAP_AVAILABLE", true)
        && system_config_bool(ldap_enabled_config.as_ref(), false)
        && ldap_config_is_enabled(ldap_config.as_ref());
    let ldap_exclusive = ldap_enabled
        && ldap_config
            .as_ref()
            .map(|config| config.is_exclusive)
            .unwrap_or(false);
    if !ldap_exclusive {
        return Ok(true);
    }
    Ok(user.role.eq_ignore_ascii_case("admin") && user.auth_source.eq_ignore_ascii_case("local"))
}

fn auth_ldap_default_search_filter(username_attr: &str) -> String {
    format!("({username_attr}={{username}})")
}

fn auth_ldap_escape_filter(value: &str) -> Result<String, GatewayError> {
    use std::fmt::Write as _;

    let normalized = value.trim();
    if normalized.chars().count() > 128 {
        return Err(GatewayError::Internal(
            "ldap filter value too long".to_string(),
        ));
    }
    let mut escaped = String::with_capacity(normalized.len());
    for ch in normalized.chars() {
        match ch {
            '\\' => escaped.push_str(r"\5c"),
            '*' => escaped.push_str(r"\2a"),
            '(' => escaped.push_str(r"\28"),
            ')' => escaped.push_str(r"\29"),
            '\0' => escaped.push_str(r"\00"),
            '&' => escaped.push_str(r"\26"),
            '|' => escaped.push_str(r"\7c"),
            '=' => escaped.push_str(r"\3d"),
            '>' => escaped.push_str(r"\3e"),
            '<' => escaped.push_str(r"\3c"),
            '~' => escaped.push_str(r"\7e"),
            '!' => escaped.push_str(r"\21"),
            _ if ch.is_control() => {
                let _ = write!(&mut escaped, "\\{:02x}", ch as u32);
            }
            _ => escaped.push(ch),
        }
    }
    Ok(escaped)
}

fn auth_ldap_normalize_server_url(server_url: &str) -> Option<String> {
    let server_url = server_url.trim();
    if server_url.is_empty() {
        return None;
    }
    if server_url.contains("://") {
        return Some(server_url.to_string());
    }
    Some(format!("ldap://{server_url}"))
}

fn auth_ldap_decrypt_bind_password(
    state: &AppState,
    config: &aether_data::repository::auth_modules::StoredLdapModuleConfig,
) -> Option<String> {
    config
        .bind_password_encrypted
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            decrypt_catalog_secret_with_fallbacks(state.encryption_key(), value)
                .unwrap_or_else(|| value.to_string())
        })
        .filter(|value| !value.trim().is_empty())
}

async fn read_auth_ldap_runtime_config(
    state: &AppState,
) -> Result<Option<AuthLdapRuntimeConfig>, GatewayError> {
    let ldap_enabled_config = state
        .read_system_config_json_value("module.ldap.enabled")
        .await?;
    let ldap_config = state.get_ldap_module_config().await?;
    let Some(config) = ldap_config.filter(|config| {
        module_available_from_env("LDAP_AVAILABLE", true)
            && system_config_bool(ldap_enabled_config.as_ref(), false)
            && ldap_config_is_enabled(Some(config))
    }) else {
        return Ok(None);
    };
    let Some(server_url) = auth_ldap_normalize_server_url(&config.server_url) else {
        return Ok(None);
    };
    let Some(bind_password) = auth_ldap_decrypt_bind_password(state, &config) else {
        return Ok(None);
    };

    let username_attr = config
        .username_attr
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("uid")
        .to_string();
    let default_search_filter = auth_ldap_default_search_filter(&username_attr);
    Ok(Some(AuthLdapRuntimeConfig {
        server_url,
        bind_dn: config.bind_dn.trim().to_string(),
        bind_password,
        base_dn: config.base_dn.trim().to_string(),
        user_search_filter: config
            .user_search_filter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_search_filter.as_str())
            .to_string(),
        username_attr,
        email_attr: config
            .email_attr
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("mail")
            .to_string(),
        display_name_attr: config
            .display_name_attr
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("displayName")
            .to_string(),
        use_starttls: config.use_starttls,
        connect_timeout_secs: u64::try_from(config.connect_timeout.unwrap_or(10).max(1))
            .unwrap_or(10),
    }))
}

#[cfg(test)]
fn authenticate_auth_ldap_user_mock(
    config: &AuthLdapRuntimeConfig,
    identifier: &str,
    password: &str,
) -> Option<AuthLdapAuthenticatedUser> {
    if !config.server_url.starts_with("mockldap://") {
        return None;
    }
    if password != "secret123" {
        return None;
    }
    let normalized = normalize_auth_login_identifier(identifier);
    let (username, email, display_name) = match normalized.as_str() {
        "alice" | "alice@example.com" => (
            "alice".to_string(),
            "alice@example.com".to_string(),
            "Alice LDAP".to_string(),
        ),
        "bob" | "bob@example.com" => (
            "bob".to_string(),
            "bob@example.com".to_string(),
            "Bob LDAP".to_string(),
        ),
        _ => return None,
    };
    Some(AuthLdapAuthenticatedUser {
        ldap_dn: format!("cn={username},dc=example,dc=com"),
        ldap_username: username.clone(),
        username,
        email,
        display_name,
    })
}

fn authenticate_auth_ldap_user_blocking(
    config: AuthLdapRuntimeConfig,
    identifier: String,
    password: String,
) -> Option<AuthLdapAuthenticatedUser> {
    #[cfg(test)]
    if let Some(user) = authenticate_auth_ldap_user_mock(&config, &identifier, &password) {
        return Some(user);
    }

    let settings = ldap3::LdapConnSettings::new()
        .set_conn_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
        .set_starttls(config.use_starttls && !config.server_url.starts_with("ldaps://"));
    let mut admin = ldap3::LdapConn::with_settings(settings.clone(), &config.server_url).ok()?;
    admin
        .simple_bind(&config.bind_dn, &config.bind_password)
        .ok()?
        .success()
        .ok()?;

    let escaped_identifier = auth_ldap_escape_filter(&identifier).ok()?;
    let search_filter = config
        .user_search_filter
        .replace("{username}", &escaped_identifier);
    let attrs = vec![
        config.username_attr.as_str(),
        config.email_attr.as_str(),
        config.display_name_attr.as_str(),
    ];
    let (entries, _result) = admin
        .search(
            &config.base_dn,
            ldap3::Scope::Subtree,
            &search_filter,
            attrs,
        )
        .ok()?
        .success()
        .ok()?;
    if entries.len() != 1 {
        let _ = admin.unbind();
        return None;
    }

    let entry = ldap3::SearchEntry::construct(entries[0].clone());
    let user_dn = entry.dn;
    let mut user = ldap3::LdapConn::with_settings(settings, &config.server_url).ok()?;
    user.simple_bind(&user_dn, &password).ok()?.success().ok()?;

    let ldap_username = entry
        .attrs
        .get(&config.username_attr)
        .and_then(|values| values.first())
        .cloned()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| identifier.clone());
    let email = entry
        .attrs
        .get(&config.email_attr)
        .and_then(|values| values.first())
        .cloned()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{ldap_username}@ldap.local"));
    let display_name = entry
        .attrs
        .get(&config.display_name_attr)
        .and_then(|values| values.first())
        .cloned()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| ldap_username.clone());

    let _ = admin.unbind();
    let _ = user.unbind();

    Some(AuthLdapAuthenticatedUser {
        username: ldap_username.clone(),
        ldap_username,
        ldap_dn: user_dn,
        email,
        display_name,
    })
}

async fn authenticate_auth_ldap_user(
    state: &AppState,
    identifier: &str,
    password: &str,
) -> Result<Option<AuthLdapAuthenticatedUser>, GatewayError> {
    let Some(config) = read_auth_ldap_runtime_config(state).await? else {
        return Ok(None);
    };
    tokio::task::spawn_blocking({
        let identifier = identifier.to_string();
        let password = password.to_string();
        move || authenticate_auth_ldap_user_blocking(config, identifier, password)
    })
    .await
    .map_err(|err| GatewayError::Internal(err.to_string()))
}

async fn build_auth_login_success_response(
    state: &AppState,
    headers: &http::HeaderMap,
    client_device_id: String,
    user: aether_data::repository::users::StoredUserAuthRecord,
) -> Response<Body> {
    let now = auth_now();
    if let Err(err) = state.touch_auth_user_last_login(&user.id, now).await {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("auth last login update failed: {err:?}"),
            false,
        );
    }

    let session_id = Uuid::new_v4().to_string();
    let access_expires_at = now + chrono::Duration::hours(auth_access_token_expiry_hours());
    let refresh_expires_at = now + chrono::Duration::days(AUTH_REFRESH_TOKEN_EXPIRATION_DAYS);
    let access_token = match create_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!(session_id.clone())),
        ]),
        access_expires_at,
    ) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                detail,
                false,
            )
        }
    };
    let refresh_token = match create_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!(session_id.clone())),
            ("jti".to_string(), json!(Uuid::new_v4().to_string())),
        ]),
        refresh_expires_at,
    ) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                detail,
                false,
            )
        }
    };
    let session = match crate::gateway::data::StoredUserSessionRecord::new(
        session_id,
        user.id.clone(),
        client_device_id,
        None,
        crate::gateway::data::StoredUserSessionRecord::hash_refresh_token(&refresh_token),
        None,
        None,
        Some(now),
        Some(refresh_expires_at),
        None,
        None,
        auth_client_ip(headers),
        auth_user_agent(headers),
        Some(now),
        Some(now),
    ) {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth session build failed: {err:?}"),
                false,
            )
        }
    };
    let created = match state.create_user_session(session).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "auth session backend unavailable",
                false,
            )
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth session create failed: {err:?}"),
                false,
            )
        }
    };

    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": auth_access_token_expiry_hours() * 60 * 60,
            "user_id": user.id,
            "email": user.email,
            "username": user.username,
            "role": user.role,
            "session_id": created.id,
        }),
        Some(build_auth_refresh_cookie_header(&refresh_token)),
    )
}

async fn read_auth_email_verification_code(
    state: &AppState,
    email: &str,
) -> Result<Option<StoredAuthEmailVerificationCode>, GatewayError> {
    let key = auth_email_verification_key(email);
    let raw = if let Some(runner) = state.redis_kv_runner() {
        let mut connection = runner
            .client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let namespaced_key = runner.keyspace().key(&key);
        redis::cmd("GET")
            .arg(&namespaced_key)
            .query_async::<Option<String>>(&mut connection)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
    } else {
        load_auth_email_verification_entry_for_tests(state, &key)
    };
    raw.map(|value| {
        serde_json::from_str::<StoredAuthEmailVerificationCode>(&value)
            .map_err(|err| GatewayError::Internal(err.to_string()))
    })
    .transpose()
}

async fn auth_email_is_verified(state: &AppState, email: &str) -> Result<bool, GatewayError> {
    let key = auth_email_verified_key(email);
    if let Some(runner) = state.redis_kv_runner() {
        let mut connection = runner
            .client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let namespaced_key = runner.keyspace().key(&key);
        let exists = redis::cmd("EXISTS")
            .arg(&namespaced_key)
            .query_async::<i64>(&mut connection)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(exists > 0);
    }
    Ok(load_auth_email_verification_entry_for_tests(state, &key).is_some())
}

async fn mark_auth_email_verified(state: &AppState, email: &str) -> Result<bool, GatewayError> {
    let key = auth_email_verified_key(email);
    if let Some(runner) = state.redis_kv_runner() {
        runner
            .setex(&key, "verified", Some(AUTH_EMAIL_VERIFIED_TTL_SECS))
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(true);
    }
    Ok(save_auth_email_verification_entry_for_tests(
        state,
        &key,
        "verified",
    ))
}

async fn clear_auth_email_pending_code(state: &AppState, email: &str) -> Result<bool, GatewayError> {
    let verification_key = auth_email_verification_key(email);
    if let Some(runner) = state.redis_kv_runner() {
        let _ = runner
            .del(&verification_key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(true);
    }
    Ok(delete_auth_email_verification_entries_for_tests(
        state,
        &[verification_key],
    ))
}

async fn clear_auth_email_verification(state: &AppState, email: &str) -> Result<bool, GatewayError> {
    let verification_key = auth_email_verification_key(email);
    let verified_key = auth_email_verified_key(email);
    if let Some(runner) = state.redis_kv_runner() {
        let _ = runner
            .del(&verification_key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let _ = runner
            .del(&verified_key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(true);
    }
    Ok(delete_auth_email_verification_entries_for_tests(
        state,
        &[verification_key, verified_key],
    ))
}

async fn store_auth_email_verification_code(
    state: &AppState,
    email: &str,
    code: &str,
    created_at: chrono::DateTime<chrono::Utc>,
    ttl_seconds: u64,
) -> Result<bool, GatewayError> {
    let key = auth_email_verification_key(email);
    let value = json!({
        "code": code,
        "created_at": created_at.to_rfc3339(),
    })
    .to_string();
    if let Some(runner) = state.redis_kv_runner() {
        runner
            .setex(&key, &value, Some(ttl_seconds))
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(true);
    }
    Ok(save_auth_email_verification_entry_for_tests(
        state, &key, &value,
    ))
}

async fn read_auth_smtp_config(state: &AppState) -> Result<Option<AuthSmtpConfig>, GatewayError> {
    let smtp_host = state.read_system_config_json_value("smtp_host").await?;
    let smtp_from_email = state.read_system_config_json_value("smtp_from_email").await?;
    let Some(host) = system_config_string(smtp_host.as_ref()) else {
        return Ok(None);
    };
    let Some(from_email) = system_config_string(smtp_from_email.as_ref()) else {
        return Ok(None);
    };
    let smtp_port = state.read_system_config_json_value("smtp_port").await?;
    let smtp_user = state.read_system_config_json_value("smtp_user").await?;
    let smtp_password = state.read_system_config_json_value("smtp_password").await?;
    let smtp_use_tls = state.read_system_config_json_value("smtp_use_tls").await?;
    let smtp_use_ssl = state.read_system_config_json_value("smtp_use_ssl").await?;
    let smtp_from_name = state.read_system_config_json_value("smtp_from_name").await?;

    let password = system_config_string(smtp_password.as_ref()).map(|value| {
        decrypt_catalog_secret_with_fallbacks(state.encryption_key(), &value).unwrap_or(value)
    });

    Ok(Some(AuthSmtpConfig {
        host,
        port: system_config_u16(smtp_port.as_ref(), 587),
        user: system_config_string(smtp_user.as_ref()),
        password,
        use_tls: system_config_bool(smtp_use_tls.as_ref(), true),
        use_ssl: system_config_bool(smtp_use_ssl.as_ref(), false),
        from_email,
        from_name: system_config_string(smtp_from_name.as_ref())
            .unwrap_or_else(|| "Aether".to_string()),
    }))
}

async fn auth_email_app_name(state: &AppState) -> Result<String, GatewayError> {
    let email_app_name = state.read_system_config_json_value("email_app_name").await?;
    let site_name = state.read_system_config_json_value("site_name").await?;
    let smtp_from_name = state.read_system_config_json_value("smtp_from_name").await?;
    Ok(system_config_string(email_app_name.as_ref())
        .or_else(|| system_config_string(site_name.as_ref()))
        .or_else(|| system_config_string(smtp_from_name.as_ref()))
        .unwrap_or_else(|| "Aether".to_string()))
}

async fn build_auth_verification_email(
    state: &AppState,
    email: &str,
    code: &str,
    expire_minutes: i64,
) -> Result<AuthComposedEmail, GatewayError> {
    let template = read_admin_email_template_payload(state, "verification")
        .await?
        .ok_or_else(|| GatewayError::Internal("verification email template missing".to_string()))?;
    let subject_template = template
        .get("subject")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("邮箱验证码");
    let html_template = template
        .get("html")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let app_name = auth_email_app_name(state).await?;
    let variables = std::collections::BTreeMap::from([
        ("app_name".to_string(), app_name.clone()),
        ("code".to_string(), code.to_string()),
        ("expire_minutes".to_string(), expire_minutes.to_string()),
        ("email".to_string(), email.to_string()),
    ]);
    let subject = render_auth_template_string(subject_template, &variables, false)?;
    let html_body = render_admin_email_template_html(html_template, &variables)?;
    let text_body = auth_build_verification_text_body(&app_name, email, code, expire_minutes);
    Ok(AuthComposedEmail {
        to_email: email.to_string(),
        subject,
        html_body,
        text_body,
    })
}

async fn send_auth_email(
    state: &AppState,
    config: AuthSmtpConfig,
    email: AuthComposedEmail,
) -> Result<(), GatewayError> {
    if record_auth_email_delivery_for_tests(
        state,
        json!({
            "to_email": email.to_email,
            "subject": email.subject,
            "html_body": email.html_body,
            "text_body": email.text_body,
        }),
    ) {
        return Ok(());
    }

    tokio::task::spawn_blocking(move || send_auth_email_blocking(config, email))
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
}

async fn auth_registration_email_configured(state: &AppState) -> Result<bool, GatewayError> {
    let smtp_host = state.read_system_config_json_value("smtp_host").await?;
    let smtp_from_email = state.read_system_config_json_value("smtp_from_email").await?;
    Ok(system_config_string(smtp_host.as_ref()).is_some()
        && system_config_string(smtp_from_email.as_ref()).is_some())
}

async fn auth_password_policy_level(state: &AppState) -> Result<String, GatewayError> {
    let config = state
        .read_system_config_json_value("password_policy_level")
        .await?;
    Ok(match system_config_string(config.as_ref()) {
        Some(value) if matches!(value.as_str(), "weak" | "medium" | "strong") => value,
        _ => "weak".to_string(),
    })
}

async fn validate_auth_email_suffix(
    state: &AppState,
    email: &str,
) -> Result<Result<(), String>, GatewayError> {
    let mode_config = state.read_system_config_json_value("email_suffix_mode").await?;
    let mode = system_config_string(mode_config.as_ref()).unwrap_or_else(|| "none".to_string());
    if mode == "none" {
        return Ok(Ok(()));
    }

    let suffixes_config = state.read_system_config_json_value("email_suffix_list").await?;
    let suffixes = system_config_string_list(suffixes_config.as_ref());
    if suffixes.is_empty() {
        return Ok(Ok(()));
    }

    let Some((_, suffix)) = email.split_once('@') else {
        return Ok(Err("邮箱格式无效".to_string()));
    };
    let suffix = suffix.to_ascii_lowercase();
    if mode == "whitelist" && !suffixes.iter().any(|item| item == &suffix) {
        return Ok(Err(format!(
            "该邮箱后缀不在允许列表中，仅支持: {}",
            suffixes.join(", ")
        )));
    }
    if mode == "blacklist" && suffixes.iter().any(|item| item == &suffix) {
        return Ok(Err(format!("该邮箱后缀 ({suffix}) 不允许注册")));
    }
    Ok(Ok(()))
}

async fn handle_auth_send_verification_code(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let Some(request_body) = request_body else {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "请求数据验证失败",
            false,
        );
    };
    let payload = match serde_json::from_slice::<AuthEmailRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "请求数据验证失败",
                false,
            );
        }
    };
    let Some(email) = normalize_auth_email(&payload.email) else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "邮箱格式无效", false);
    };

    if state
        .find_user_auth_by_identifier(&email)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "该邮箱已被注册，请直接登录或使用其他邮箱",
            false,
        );
    }

    match validate_auth_email_suffix(state, &email).await {
        Ok(Ok(())) => {}
        Ok(Err(detail)) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    }

    let smtp_config = match read_auth_smtp_config(state).await {
        Ok(Some(value)) => value,
        Ok(None) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "发送验证码失败，请稍后重试",
                false,
            );
        }
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth smtp settings lookup failed: {err:?}"),
                false,
            );
        }
    };

    let now = auth_now();
    if let Ok(Some(stored)) = read_auth_email_verification_code(state, &email).await {
        let created_at = chrono::DateTime::parse_from_rfc3339(&stored.created_at)
            .ok()
            .map(|value| value.with_timezone(&chrono::Utc));
        let expires_at = created_at
            .map(|value| value + chrono::Duration::minutes(auth_verification_code_expire_minutes()));
        if expires_at.is_some_and(|value| value <= now) {
            let _ = clear_auth_email_pending_code(state, &email).await;
        } else if let Some(created_at) = created_at {
            let elapsed = now.signed_duration_since(created_at).num_seconds();
            let remaining = auth_verification_send_cooldown_seconds() - elapsed;
            if remaining > 0 {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    format!("请在 {remaining} 秒后重试"),
                    false,
                );
            }
        }
    }

    let expire_minutes = auth_verification_code_expire_minutes();
    let code = generate_auth_verification_code();
    let email_message = match build_auth_verification_email(state, &email, &code, expire_minutes).await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth verification email render failed: {err:?}"),
                false,
            );
        }
    };

    if let Err(err) = store_auth_email_verification_code(
        state,
        &email,
        &code,
        now,
        u64::try_from(expire_minutes.saturating_mul(60)).unwrap_or(300),
    )
    .await
    {
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("auth verification code save failed: {err:?}"),
            false,
        );
    }

    if let Err(_err) = send_auth_email(state, smtp_config, email_message).await {
        let _ = clear_auth_email_pending_code(state, &email).await;
        return build_auth_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "发送验证码失败，请稍后重试",
            false,
        );
    }

    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "message": "验证码已发送，请查收邮件",
            "success": true,
            "expire_minutes": expire_minutes,
        }),
        None,
    )
}

async fn handle_auth_login(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少登录请求体", false);
    };
    let payload = match serde_json::from_slice::<AuthLoginRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "无效的登录请求", false)
        }
    };
    let identifier = normalize_auth_login_identifier(&payload.email);
    if identifier.is_empty() {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "邮箱或用户名不能为空", false);
    }
    if let Err(detail) = validate_auth_login_password(&payload.password) {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
    }
    let client_device_id = match extract_client_device_id(request_context, headers) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let auth_type = payload.auth_type.trim().to_ascii_lowercase();
    let user = match auth_type.as_str() {
        "local" => {
            let user = match state.find_user_auth_by_identifier(&identifier).await {
                Ok(Some(user)) => user,
                Ok(None) => {
                    return build_auth_error_response(
                        http::StatusCode::UNAUTHORIZED,
                        "邮箱或密码错误",
                        false,
                    )
                }
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth user lookup failed: {err:?}"),
                        false,
                    )
                }
            };
            if user.is_deleted
                || !user.is_active
                || !user.auth_source.eq_ignore_ascii_case("local")
                || user.password_hash.as_deref().is_none_or(str::is_empty)
            {
                return build_auth_error_response(
                    http::StatusCode::UNAUTHORIZED,
                    "邮箱或密码错误",
                    false,
                );
            }
            match auth_local_login_allowed_for_user(state, &user).await {
                Ok(true) => {}
                Ok(false) => {
                    return build_auth_error_response(
                        http::StatusCode::UNAUTHORIZED,
                        "邮箱或密码错误",
                        false,
                    )
                }
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth settings lookup failed: {err:?}"),
                        false,
                    )
                }
            }
            let password_hash = user
                .password_hash
                .as_deref()
                .expect("validated password hash should exist");
            let password_matches = bcrypt::verify(&payload.password, password_hash).unwrap_or(false);
            if !password_matches {
                return build_auth_error_response(
                    http::StatusCode::UNAUTHORIZED,
                    "邮箱或密码错误",
                    false,
                );
            }
            user
        }
        "ldap" => {
            let ldap_user = match authenticate_auth_ldap_user(state, &identifier, &payload.password).await
            {
                Ok(Some(user)) => user,
                Ok(None) => {
                    return build_auth_error_response(
                        http::StatusCode::UNAUTHORIZED,
                        "邮箱或密码错误",
                        false,
                    )
                }
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth ldap login failed: {err:?}"),
                        false,
                    )
                }
            };
            let _ = &ldap_user.display_name;
            let initial_gift = match state
                .read_system_config_json_value("default_user_initial_gift_usd")
                .await
            {
                Ok(value) => system_config_f64(value.as_ref(), 10.0),
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth settings lookup failed: {err:?}"),
                        false,
                    )
                }
            };
            match state
                .get_or_create_ldap_auth_user(
                    ldap_user.email,
                    ldap_user.username,
                    Some(ldap_user.ldap_dn),
                    Some(ldap_user.ldap_username),
                    auth_now(),
                    initial_gift,
                    false,
                )
                .await
            {
                Ok(Some(user)) => user,
                Ok(None) => {
                    return build_auth_error_response(
                        http::StatusCode::UNAUTHORIZED,
                        "邮箱或密码错误",
                        false,
                    )
                }
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth ldap user sync failed: {err:?}"),
                        false,
                    )
                }
            }
        }
        _ => {
            return build_public_support_maintenance_response(
                "Non-local auth login requires Rust maintenance backend",
            )
        }
    };

    build_auth_login_success_response(state, headers, client_device_id, user).await
}

async fn handle_auth_register(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let payload = match serde_json::from_slice::<AuthRegisterRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false)
        }
    };
    let email = match normalize_auth_optional_email(payload.email.as_deref()) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
        }
    };
    let username = match validate_auth_register_username(&payload.username) {
        Ok(value) => value,
        Err(detail) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
        }
    };
    let password_policy = match auth_password_policy_level(state).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    };
    if let Err(detail) = validate_auth_register_password(&payload.password, &password_policy) {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
    }

    let enable_registration = match state.read_system_config_json_value("enable_registration").await
    {
        Ok(value) => system_config_bool(value.as_ref(), false),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    };
    if !enable_registration {
        return build_auth_error_response(
            http::StatusCode::FORBIDDEN,
            "系统暂不开放注册",
            false,
        );
    }

    let email_configured = match auth_registration_email_configured(state).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    };
    let require_verification = match state
        .read_system_config_json_value("require_email_verification")
        .await
    {
        Ok(value) => system_config_bool(value.as_ref(), false) && email_configured,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    };

    if require_verification && email.is_none() {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "系统要求邮箱验证，请填写邮箱",
            false,
        );
    }
    if require_verification {
        if let Some(email) = email.as_deref() {
            let is_verified = match auth_email_is_verified(state, email).await {
                Ok(value) => value,
                Err(err) => {
                    return build_auth_error_response(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("auth verification lookup failed: {err:?}"),
                        false,
                    );
                }
            };
            if !is_verified {
                return build_auth_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "请先完成邮箱验证。请发送验证码并验证后再注册。",
                    false,
                );
            }
        }
    }
    if let Some(email) = email.as_deref() {
        match validate_auth_email_suffix(state, email).await {
            Ok(Ok(())) => {}
            Ok(Err(detail)) => {
                return build_auth_error_response(http::StatusCode::BAD_REQUEST, detail, false);
            }
            Err(err) => {
                return build_auth_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("auth settings lookup failed: {err:?}"),
                    false,
                );
            }
        }
        if state
            .find_user_auth_by_identifier(email)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                format!("邮箱已存在: {email}"),
                false,
            );
        }
    }
    if state
        .find_user_auth_by_identifier(&username)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            format!("用户名已存在: {username}"),
            false,
        );
    }

    let password_hash = match bcrypt::hash(&payload.password, bcrypt::DEFAULT_COST) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(
                http::StatusCode::BAD_REQUEST,
                "密码长度不能超过72字节",
                false,
            );
        }
    };
    let initial_gift = match state
        .read_system_config_json_value("default_user_initial_gift_usd")
        .await
    {
        Ok(value) => system_config_f64(value.as_ref(), 10.0),
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth settings lookup failed: {err:?}"),
                false,
            );
        }
    };
    let Some((user, _wallet)) = (match state
        .register_local_auth_user(
            email.clone(),
            require_verification && email.is_some(),
            username.clone(),
            password_hash,
            initial_gift,
            false,
        )
        .await
    {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("auth register failed: {err:?}"),
                false,
            );
        }
    }) else {
        return build_public_support_maintenance_response(
            "Auth registration requires Rust data backend",
        );
    };

    if require_verification {
        if let Some(email) = email.as_deref() {
            let _ = clear_auth_email_verification(state, email).await;
        }
    }
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "user_id": user.id,
            "email": user.email,
            "username": user.username,
            "message": "注册成功",
        }),
        None,
    )
}

async fn handle_auth_verify_email(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let payload = match serde_json::from_slice::<AuthVerifyEmailRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false)
        }
    };
    let Some(email) = normalize_auth_email(&payload.email) else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "邮箱格式无效", false);
    };
    let code = payload.code.trim();
    if code.len() != 6 || !code.chars().all(|ch| ch.is_ascii_digit()) {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "验证码必须是6位数字",
            false,
        );
    }
    let pending = match read_auth_email_verification_code(state, &email).await {
        Ok(value) => value,
        Err(err) => {
            return build_auth_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("verification lookup failed: {err:?}"),
                false,
            )
        }
    };
    let Some(pending) = pending else {
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "验证码不存在或已过期",
            false,
        );
    };
    let created_at = chrono::DateTime::parse_from_rfc3339(&pending.created_at)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc));
    let expires_at = created_at
        .map(|value| value + chrono::Duration::minutes(auth_verification_code_expire_minutes()));
    if expires_at.is_some_and(|value| value <= auth_now()) {
        let _ = clear_auth_email_pending_code(state, &email).await;
        return build_auth_error_response(
            http::StatusCode::BAD_REQUEST,
            "验证码不存在或已过期",
            false,
        );
    }
    if pending.code != code {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "验证码错误", false);
    }
    if mark_auth_email_verified(state, &email).await.ok() != Some(true) {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "系统错误", false);
    }
    let _ = clear_auth_email_pending_code(state, &email).await;
    build_auth_json_response(
        http::StatusCode::OK,
        json!({ "message": "邮箱验证成功", "success": true }),
        None,
    )
}

async fn handle_auth_verification_status(
    state: &AppState,
    request_body: Option<&axum::body::Bytes>,
) -> Response<Body> {
    let Some(request_body) = request_body else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "缺少请求体", false);
    };
    let payload = match serde_json::from_slice::<AuthEmailRequest>(request_body) {
        Ok(value) => value,
        Err(_) => {
            return build_auth_error_response(http::StatusCode::BAD_REQUEST, "输入验证失败", false)
        }
    };
    let Some(email) = normalize_auth_email(&payload.email) else {
        return build_auth_error_response(http::StatusCode::BAD_REQUEST, "邮箱格式无效", false);
    };
    let pending = read_auth_email_verification_code(state, &email).await.ok().flatten();
    let is_verified = auth_email_is_verified(state, &email).await.unwrap_or(false);
    let now = auth_now();
    let (has_pending_code, cooldown_remaining, code_expires_in) = if let Some(pending) = pending {
        let created_at = chrono::DateTime::parse_from_rfc3339(&pending.created_at)
            .ok()
            .map(|value| value.with_timezone(&chrono::Utc));
        let expires_at = created_at
            .map(|value| value + chrono::Duration::minutes(auth_verification_code_expire_minutes()));
        if expires_at.is_some_and(|value| value <= now) {
            let _ = clear_auth_email_pending_code(state, &email).await;
            (false, None, None)
        } else {
            let cooldown_remaining = created_at.and_then(|value| {
                let elapsed = now.signed_duration_since(value).num_seconds();
                let remaining = auth_verification_send_cooldown_seconds() - elapsed;
                (remaining > 0).then_some(i32::try_from(remaining).ok()).flatten()
            });
            let code_expires_in = expires_at.and_then(|value| {
                let remaining = value.signed_duration_since(now).num_seconds();
                (remaining > 0).then_some(i32::try_from(remaining).ok()).flatten()
            });
            (true, cooldown_remaining, code_expires_in)
        }
    } else {
        (false, None, None)
    };
    build_auth_json_response(
        http::StatusCode::OK,
        json!({
            "email": email,
            "has_pending_code": has_pending_code,
            "is_verified": is_verified,
            "cooldown_remaining": cooldown_remaining,
            "code_expires_in": code_expires_in,
        }),
        None,
    )
}

async fn try_auth_logout_with_access_token(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Option<Response<Body>> {
    let token = extract_bearer_token(headers)?;
    let claims = decode_auth_token(&token, "access").ok()?;
    let user_id = claims.get("user_id").and_then(serde_json::Value::as_str)?;
    let session_id = claims.get("session_id").and_then(serde_json::Value::as_str)?;
    let user = state.find_user_auth_by_id(user_id).await.ok().flatten()?;
    if !user.is_active || user.is_deleted || !auth_token_identity_matches_user(&claims, &user) {
        return None;
    }
    let client_device_id = extract_client_device_id(request_context, headers).ok()?;
    let now = auth_now();
    let session = state.find_user_session(user_id, session_id).await.ok().flatten()?;
    if session.is_revoked() || session.is_expired(now) || session.client_device_id != client_device_id {
        return None;
    }
    let _ = state
        .revoke_user_session(user_id, session_id, now, "user_logout")
        .await;
    Some(build_auth_json_response(
        http::StatusCode::OK,
        json!({ "message": "登出成功", "success": true }),
        Some(build_auth_refresh_cookie_clear_header()),
    ))
}

async fn try_auth_logout_with_refresh_cookie(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Option<Response<Body>> {
    let refresh_token = extract_cookie_value(headers, &auth_refresh_cookie_name())?;
    let claims = decode_auth_token(&refresh_token, "refresh").ok()?;
    let user_id = claims.get("user_id").and_then(serde_json::Value::as_str)?;
    let session_id = claims.get("session_id").and_then(serde_json::Value::as_str)?;
    let client_device_id = extract_client_device_id(request_context, headers).ok()?;
    let now = auth_now();
    if let Some(session) = state.find_user_session(user_id, session_id).await.ok().flatten() {
        if !session.is_revoked() && !session.is_expired(now) && session.client_device_id == client_device_id
        {
            let _ = state
                .revoke_user_session(user_id, session_id, now, "user_logout")
                .await;
        }
    }
    Some(build_auth_json_response(
        http::StatusCode::OK,
        json!({ "message": "登出成功", "success": true }),
        Some(build_auth_refresh_cookie_clear_header()),
    ))
}

async fn handle_auth_logout(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Response<Body> {
    if let Some(response) = try_auth_logout_with_access_token(state, request_context, headers).await
    {
        return response;
    }
    if let Some(response) =
        try_auth_logout_with_refresh_cookie(state, request_context, headers).await
    {
        return response;
    }
    build_auth_error_response(http::StatusCode::UNAUTHORIZED, "缺少认证令牌", true)
}

async fn maybe_build_local_auth_legacy_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    request_body: Option<&axum::body::Bytes>,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("auth_legacy") {
        return None;
    }

    match decision.route_kind.as_deref() {
        Some("send_verification_code")
            if request_context.request_path == "/api/auth/send-verification-code" =>
        {
            Some(handle_auth_send_verification_code(state, request_body).await)
        }
        Some("login") if request_context.request_path == "/api/auth/login" => {
            Some(handle_auth_login(state, request_context, headers, request_body).await)
        }
        Some("register") if request_context.request_path == "/api/auth/register" => {
            Some(handle_auth_register(state, request_body).await)
        }
        Some("verify_email") if request_context.request_path == "/api/auth/verify-email" => {
            Some(handle_auth_verify_email(state, request_body).await)
        }
        Some("verification_status")
            if request_context.request_path == "/api/auth/verification-status" =>
        {
            Some(handle_auth_verification_status(state, request_body).await)
        }
        Some("me") if request_context.request_path == "/api/auth/me" => {
            Some(handle_auth_me(state, request_context, headers).await)
        }
        Some("refresh") if request_context.request_path == "/api/auth/refresh" => {
            Some(handle_auth_refresh(state, request_context, headers).await)
        }
        Some("logout") if request_context.request_path == "/api/auth/logout" => {
            Some(handle_auth_logout(state, request_context, headers).await)
        }
        _ => Some(build_public_support_maintenance_response(
            "Auth routes require Rust maintenance backend",
        )),
    }
}
