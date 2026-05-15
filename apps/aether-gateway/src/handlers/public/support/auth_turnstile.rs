use super::{
    decrypt_catalog_secret_with_fallbacks, http, system_config_bool, system_config_string,
    system_config_string_list, AppState,
};
use serde::Deserialize;
use std::time::Duration;

const TURNSTILE_SITEVERIFY_URL: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";

#[derive(Debug, Clone)]
pub(super) struct AuthTurnstilePublicSettings {
    pub(super) enabled: bool,
    pub(super) site_key: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthTurnstileError {
    pub(super) status: http::StatusCode,
    pub(super) detail: String,
}

#[derive(Debug)]
struct AuthTurnstileConfig {
    enabled: bool,
    site_key: Option<String>,
    secret_key: Option<String>,
    siteverify_url: String,
    allowed_hostnames: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TurnstileSiteverifyResponse {
    success: bool,
    hostname: Option<String>,
    #[serde(default, rename = "error-codes")]
    error_codes: Vec<String>,
}

fn turnstile_error(status: http::StatusCode, detail: impl Into<String>) -> AuthTurnstileError {
    AuthTurnstileError {
        status,
        detail: detail.into(),
    }
}

fn turnstile_config_error(detail: impl Into<String>) -> AuthTurnstileError {
    turnstile_error(http::StatusCode::INTERNAL_SERVER_ERROR, detail)
}

async fn read_auth_turnstile_config(
    state: &AppState,
) -> Result<AuthTurnstileConfig, AuthTurnstileError> {
    let enabled = state
        .read_system_config_json_value("turnstile_enabled")
        .await
        .map_err(|err| {
            turnstile_config_error(format!("auth turnstile settings lookup failed: {err:?}"))
        })?;
    let site_key = state
        .read_system_config_json_value("turnstile_site_key")
        .await
        .map_err(|err| {
            turnstile_config_error(format!("auth turnstile settings lookup failed: {err:?}"))
        })?;
    let secret_key = state
        .read_system_config_json_value("turnstile_secret_key")
        .await
        .map_err(|err| {
            turnstile_config_error(format!("auth turnstile settings lookup failed: {err:?}"))
        })?;
    let siteverify_url = state
        .read_system_config_json_value("turnstile_siteverify_url")
        .await
        .map_err(|err| {
            turnstile_config_error(format!("auth turnstile settings lookup failed: {err:?}"))
        })?;
    let allowed_hostnames = state
        .read_system_config_json_value("turnstile_allowed_hostnames")
        .await
        .map_err(|err| {
            turnstile_config_error(format!("auth turnstile settings lookup failed: {err:?}"))
        })?;

    let secret_key = system_config_string(secret_key.as_ref()).map(|value| {
        decrypt_catalog_secret_with_fallbacks(state.encryption_key(), &value).unwrap_or(value)
    });

    Ok(AuthTurnstileConfig {
        enabled: system_config_bool(enabled.as_ref(), false),
        site_key: system_config_string(site_key.as_ref()),
        secret_key,
        siteverify_url: system_config_string(siteverify_url.as_ref())
            .unwrap_or_else(|| TURNSTILE_SITEVERIFY_URL.to_string()),
        allowed_hostnames: system_config_string_list(allowed_hostnames.as_ref()),
    })
}

pub(super) async fn auth_turnstile_public_settings(
    state: &AppState,
) -> Result<AuthTurnstilePublicSettings, AuthTurnstileError> {
    let config = read_auth_turnstile_config(state).await?;
    let enabled = config.enabled && config.site_key.is_some();
    Ok(AuthTurnstilePublicSettings {
        enabled,
        site_key: enabled.then_some(config.site_key).flatten(),
    })
}

fn turnstile_service_error(error_codes: &[String]) -> bool {
    error_codes.iter().any(|code| {
        matches!(
            code.trim(),
            "missing-input-secret" | "invalid-input-secret" | "internal-error"
        )
    })
}

fn turnstile_hostname_allowed(hostname: Option<&str>, allowed_hostnames: &[String]) -> bool {
    if allowed_hostnames.is_empty() {
        return true;
    }
    let Some(hostname) = hostname.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let hostname = hostname.to_ascii_lowercase();
    allowed_hostnames.iter().any(|allowed| allowed == &hostname)
}

pub(super) async fn verify_auth_turnstile_token(
    state: &AppState,
    token: Option<&str>,
    remote_ip: Option<&str>,
) -> Result<(), AuthTurnstileError> {
    let config = read_auth_turnstile_config(state).await?;
    if !config.enabled || config.site_key.is_none() {
        return Ok(());
    }
    let token = token
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| turnstile_error(http::StatusCode::BAD_REQUEST, "请先完成人机验证"))?;
    let secret_key = config.secret_key.as_deref().ok_or_else(|| {
        turnstile_error(http::StatusCode::SERVICE_UNAVAILABLE, "人机验证服务未配置")
    })?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|err| {
            turnstile_error(
                http::StatusCode::SERVICE_UNAVAILABLE,
                format!("人机验证服务暂不可用: {err}"),
            )
        })?;
    let mut form = vec![
        ("secret", secret_key.to_string()),
        ("response", token.to_string()),
    ];
    if let Some(remote_ip) = remote_ip.map(str::trim).filter(|value| !value.is_empty()) {
        form.push(("remoteip", remote_ip.to_string()));
    }

    let response = client
        .post(config.siteverify_url)
        .form(&form)
        .send()
        .await
        .map_err(|_| {
            turnstile_error(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "人机验证服务暂不可用",
            )
        })?;
    if !response.status().is_success() {
        return Err(turnstile_error(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "人机验证服务暂不可用",
        ));
    }

    let payload = response
        .json::<TurnstileSiteverifyResponse>()
        .await
        .map_err(|_| {
            turnstile_error(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "人机验证服务暂不可用",
            )
        })?;
    if payload.success {
        if turnstile_hostname_allowed(payload.hostname.as_deref(), &config.allowed_hostnames) {
            return Ok(());
        }
        return Err(turnstile_error(
            http::StatusCode::BAD_REQUEST,
            "人机验证失败，请重试",
        ));
    }
    if turnstile_service_error(&payload.error_codes) {
        return Err(turnstile_error(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "人机验证服务暂不可用",
        ));
    }
    Err(turnstile_error(
        http::StatusCode::BAD_REQUEST,
        "人机验证失败，请重试",
    ))
}
