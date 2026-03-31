mod claude;
mod gemini;
mod openai;

use axum::routing::{any, post};
use axum::Router;
use serde_json::json;

use crate::gateway::{proxy_request, AppState};

pub(crate) fn mount_ai_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/v1/chat/completions", post(proxy_request))
        .route("/v1/messages", post(proxy_request))
        .route("/v1/messages/count_tokens", post(proxy_request))
        .route("/v1/responses", post(proxy_request))
        .route("/v1/responses/compact", post(proxy_request))
        .route("/v1/models/{*gemini_path}", any(proxy_request))
        .route("/v1beta/models/{*gemini_path}", any(proxy_request))
        .route("/v1beta/operations", any(proxy_request))
        .route("/v1beta/operations/{*operation_path}", any(proxy_request))
        .route("/v1/videos", any(proxy_request))
        .route("/v1/videos/{*video_path}", any(proxy_request))
        .route("/upload/v1beta/files", any(proxy_request))
        .route("/v1beta/files", any(proxy_request))
        .route("/v1beta/files/{*file_path}", any(proxy_request))
}

pub(crate) fn public_api_format_local_path(api_format: &str) -> &'static str {
    let normalized = api_format.trim().to_ascii_lowercase();
    openai::local_path(&normalized)
        .or_else(|| claude::local_path(&normalized))
        .or_else(|| gemini::local_path(&normalized))
        .unwrap_or("/")
}

pub(crate) fn normalize_admin_endpoint_signature(api_format: &str) -> Option<&'static str> {
    let normalized = api_format.trim().to_ascii_lowercase();
    openai::normalized_signature(&normalized)
        .or_else(|| claude::normalized_signature(&normalized))
        .or_else(|| gemini::normalized_signature(&normalized))
}

pub(crate) fn admin_endpoint_signature_parts(
    api_format: &str,
) -> Option<(&'static str, &'static str, &'static str)> {
    let normalized = normalize_admin_endpoint_signature(api_format)?;
    let (api_family, endpoint_kind) = normalized.split_once(':')?;
    Some((normalized, api_family, endpoint_kind))
}

pub(crate) fn provider_type_is_fixed(provider_type: &str) -> bool {
    matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "claude_code" | "kiro" | "codex" | "gemini_cli" | "antigravity" | "vertex_ai"
    )
}

pub(crate) fn provider_type_enables_format_conversion_by_default(provider_type: &str) -> bool {
    matches!(
        provider_type.trim().to_ascii_lowercase().as_str(),
        "claude_code" | "kiro" | "codex" | "antigravity" | "vertex_ai"
    )
}

pub(crate) fn fixed_provider_template(
    provider_type: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    match provider_type.trim().to_ascii_lowercase().as_str() {
        "claude_code" => Some(("https://api.anthropic.com", &["claude:cli"])),
        "codex" => Some((
            "https://chatgpt.com/backend-api/codex",
            &["openai:cli", "openai:compact"],
        )),
        "kiro" => Some(("https://q.{region}.amazonaws.com", &["claude:cli"])),
        "gemini_cli" => Some(("https://cloudcode-pa.googleapis.com", &["gemini:cli"])),
        "vertex_ai" => Some((
            "https://aiplatform.googleapis.com",
            &["gemini:chat", "claude:chat"],
        )),
        "antigravity" => Some(("https://cloudcode-pa.googleapis.com", &["gemini:chat"])),
        _ => None,
    }
}

fn codex_default_body_rules() -> Vec<serde_json::Value> {
    vec![
        json!({"action": "drop", "path": "max_output_tokens"}),
        json!({"action": "drop", "path": "temperature"}),
        json!({"action": "drop", "path": "top_p"}),
        json!({"action": "set", "path": "store", "value": false}),
        json!({
            "action": "set",
            "path": "instructions",
            "value": "You are GPT-5.",
            "condition": {"path": "instructions", "op": "not_exists"},
        }),
    ]
}

pub(crate) fn admin_default_body_rules_for_signature(
    api_format: &str,
    provider_type: Option<&str>,
) -> Option<(String, Vec<serde_json::Value>)> {
    let normalized_api_format = normalize_admin_endpoint_signature(api_format)?.to_string();
    let provider_type = provider_type.map(|value| value.trim().to_ascii_lowercase());
    let body_rules = if normalized_api_format == "openai:compact"
        || (normalized_api_format == "openai:cli" && provider_type.as_deref() == Some("codex"))
    {
        codex_default_body_rules()
    } else {
        Vec::new()
    };
    Some((normalized_api_format, body_rules))
}
