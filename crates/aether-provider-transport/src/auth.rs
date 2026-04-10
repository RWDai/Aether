use std::collections::BTreeMap;

use super::headers::{
    should_skip_upstream_complete_passthrough_header, should_skip_upstream_passthrough_header,
};
use super::snapshot::GatewayProviderTransportSnapshot;

const DEFAULT_ANTHROPIC_VERSION: &str = "2023-06-01";

fn collect_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, value) in headers.iter() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let key = name.as_str().to_ascii_lowercase();
        if should_skip_upstream_passthrough_header(&key) {
            continue;
        }
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(key, value.to_string());
    }

    for (key, value) in extra_headers {
        let normalized_key = key.to_ascii_lowercase();
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(normalized_key, value.to_string());
    }

    out
}

fn collect_complete_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, value) in headers.iter() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let key = name.as_str().to_ascii_lowercase();
        if should_skip_upstream_complete_passthrough_header(&key) {
            continue;
        }
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(key, value.to_string());
    }

    for (key, value) in extra_headers {
        let normalized_key = key.to_ascii_lowercase();
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(normalized_key, value.to_string());
    }

    out
}

pub fn build_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = collect_passthrough_headers(headers, extra_headers);
    out.entry("content-type".to_string()).or_insert_with(|| {
        content_type
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("application/json")
            .trim()
            .to_string()
    });
    out.remove("content-length");
    out
}

pub fn build_openai_passthrough_headers(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = build_passthrough_headers(headers, extra_headers, content_type);
    ensure_upstream_auth_header(&mut out, auth_header, auth_value);
    out
}

pub fn build_complete_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = collect_complete_passthrough_headers(headers, extra_headers);
    out.entry("content-type".to_string()).or_insert_with(|| {
        content_type
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("application/json")
            .trim()
            .to_string()
    });
    out.remove("content-length");
    out
}

pub fn build_complete_passthrough_headers_with_auth(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = build_complete_passthrough_headers(headers, extra_headers, content_type);
    ensure_upstream_auth_header(&mut out, auth_header, auth_value);
    out
}

pub fn build_claude_passthrough_headers(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = build_openai_passthrough_headers(
        headers,
        auth_header,
        auth_value,
        extra_headers,
        content_type,
    );

    for (name, value) in headers.iter() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let key = name.as_str().to_ascii_lowercase();
        let value = value.trim();
        if value.is_empty() || !should_restore_claude_passthrough_header(&key) {
            continue;
        }

        if key == "anthropic-beta" {
            let merged = merge_comma_header_values(out.get(&key).map(String::as_str), Some(value));
            if let Some(merged) = merged {
                out.insert(key, merged);
            }
            continue;
        }

        out.entry(key).or_insert_with(|| value.to_string());
    }

    out.entry("anthropic-version".to_string())
        .or_insert_with(|| DEFAULT_ANTHROPIC_VERSION.to_string());
    out
}

pub fn build_passthrough_headers_with_auth(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = collect_passthrough_headers(headers, extra_headers);
    ensure_upstream_auth_header(&mut out, auth_header, auth_value);
    out.remove("content-length");
    out
}

pub fn ensure_upstream_auth_header(
    headers: &mut BTreeMap<String, String>,
    auth_header: &str,
    auth_value: &str,
) {
    let header_name = auth_header.trim().to_ascii_lowercase();
    let header_value = auth_value.trim();
    if header_name.is_empty() || header_value.is_empty() {
        return;
    }

    if headers
        .get(&header_name)
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        headers.insert(header_name, header_value.to_string());
    }
}

fn should_restore_claude_passthrough_header(name: &str) -> bool {
    name.starts_with("anthropic-") || name.starts_with("x-stainless-") || name == "x-app"
}

fn merge_comma_header_values(left: Option<&str>, right: Option<&str>) -> Option<String> {
    let mut merged = Vec::new();

    for raw in [left, right].into_iter().flatten() {
        for token in raw.split(',') {
            let token = token.trim();
            if token.is_empty() || merged.iter().any(|existing: &String| existing == token) {
                continue;
            }
            merged.push(token.to_string());
        }
    }

    if merged.is_empty() {
        None
    } else {
        Some(merged.join(","))
    }
}

pub fn resolve_local_openai_chat_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    if !matches!(auth_type.as_str(), "api_key" | "bearer") {
        return None;
    }
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    Some(("authorization".to_string(), format!("Bearer {secret}")))
}

pub fn resolve_local_standard_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    match auth_type.as_str() {
        "api_key" => Some(("x-api-key".to_string(), secret.to_string())),
        "bearer" => Some(("authorization".to_string(), format!("Bearer {secret}"))),
        _ => None,
    }
}

pub fn resolve_local_gemini_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    match auth_type.as_str() {
        "api_key" => Some(("x-goog-api-key".to_string(), secret.to_string())),
        "bearer" => Some(("authorization".to_string(), format!("Bearer {secret}"))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_claude_passthrough_headers, build_complete_passthrough_headers_with_auth};
    use std::collections::BTreeMap;

    #[test]
    fn claude_passthrough_headers_restore_stripped_anthropic_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "anthropic-beta",
            http::HeaderValue::from_static("prompt-caching-2024-07-31,context-1m-2025-08-07"),
        );
        headers.insert(
            "x-stainless-runtime-version",
            http::HeaderValue::from_static("v22.14.0"),
        );
        headers.insert("x-app", http::HeaderValue::from_static("cli"));

        let built = build_claude_passthrough_headers(
            &headers,
            "x-api-key",
            "sk-upstream-claude",
            &BTreeMap::from([("anthropic-beta".to_string(), "custom-beta".to_string())]),
            Some("application/json"),
        );

        assert_eq!(
            built.get("anthropic-version").map(String::as_str),
            Some("2023-06-01")
        );
        assert_eq!(
            built.get("anthropic-beta").map(String::as_str),
            Some("custom-beta,prompt-caching-2024-07-31,context-1m-2025-08-07")
        );
        assert_eq!(
            built.get("x-stainless-runtime-version").map(String::as_str),
            Some("v22.14.0")
        );
        assert_eq!(built.get("x-app").map(String::as_str), Some("cli"));
        assert_eq!(
            built.get("x-api-key").map(String::as_str),
            Some("sk-upstream-claude")
        );
    }

    #[test]
    fn claude_passthrough_headers_preserve_explicit_anthropic_version_override() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "anthropic-version",
            http::HeaderValue::from_static("2024-01-01"),
        );

        let built = build_claude_passthrough_headers(
            &headers,
            "authorization",
            "Bearer upstream-token",
            &BTreeMap::new(),
            Some("application/json"),
        );

        assert_eq!(
            built.get("anthropic-version").map(String::as_str),
            Some("2024-01-01")
        );
    }

    #[test]
    fn complete_passthrough_headers_preserve_business_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "anthropic-beta",
            http::HeaderValue::from_static("prompt-caching-2024-07-31"),
        );
        headers.insert(
            "x-stainless-runtime-version",
            http::HeaderValue::from_static("v24.0.0"),
        );
        headers.insert("x-app", http::HeaderValue::from_static("cli"));
        headers.insert(
            "authorization",
            http::HeaderValue::from_static("Bearer client-token"),
        );

        let built = build_complete_passthrough_headers_with_auth(
            &headers,
            "x-api-key",
            "sk-upstream",
            &BTreeMap::new(),
            Some("application/json"),
        );

        assert_eq!(
            built.get("anthropic-beta").map(String::as_str),
            Some("prompt-caching-2024-07-31")
        );
        assert_eq!(
            built.get("x-stainless-runtime-version").map(String::as_str),
            Some("v24.0.0")
        );
        assert_eq!(built.get("x-app").map(String::as_str), Some("cli"));
        assert_eq!(built.get("authorization"), None);
        assert_eq!(
            built.get("x-api-key").map(String::as_str),
            Some("sk-upstream")
        );
    }
}
