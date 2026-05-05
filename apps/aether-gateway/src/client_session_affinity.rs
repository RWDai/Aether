use aether_scheduler_core::ClientSessionAffinity;
use serde_json::Value;

use crate::headers::header_value_str;

pub(crate) const AETHER_SESSION_ID_HEADER: &str = "x-aether-session-id";
pub(crate) const AETHER_AGENT_ID_HEADER: &str = "x-aether-agent-id";

struct ClientSessionAffinityRequest<'a> {
    headers: &'a http::HeaderMap,
    body_json: Option<&'a Value>,
}

trait ClientSessionAffinityAdapter {
    fn family(&self) -> &'static str;

    fn detect(&self, request: &ClientSessionAffinityRequest<'_>) -> bool;

    fn extract_session_key(&self, request: &ClientSessionAffinityRequest<'_>) -> Option<String>;
}

struct GenericSessionAffinityAdapter;
struct CodexSessionAffinityAdapter;
struct ClaudeCodeSessionAffinityAdapter;
struct OpenCodeSessionAffinityAdapter;

pub(crate) fn client_session_affinity_from_request(
    headers: &http::HeaderMap,
    body_json: Option<&Value>,
) -> Option<ClientSessionAffinity> {
    let request = ClientSessionAffinityRequest { headers, body_json };
    let client_family = detect_client_family(&request);
    let session_key = explicit_aether_session_key(&request)
        .or_else(|| extract_session_key_for_client_family(&request, client_family.as_str()))
        .or_else(|| GenericSessionAffinityAdapter.extract_session_key(&request))
        .or_else(|| {
            extract_session_key_from_other_specific_adapters(&request, client_family.as_str())
        });

    session_key
        .map(|session_key| ClientSessionAffinity::new(Some(client_family), Some(session_key)))
}

pub(crate) fn client_session_affinity_from_parts(
    parts: &http::request::Parts,
    body_json: Option<&Value>,
) -> Option<ClientSessionAffinity> {
    client_session_affinity_from_request(&parts.headers, body_json)
}

fn detect_client_family(request: &ClientSessionAffinityRequest<'_>) -> String {
    for adapter in specific_client_session_affinity_adapters() {
        if adapter.detect(request) {
            return adapter.family().to_string();
        }
    }
    GenericSessionAffinityAdapter.family().to_string()
}

fn specific_client_session_affinity_adapters() -> [&'static dyn ClientSessionAffinityAdapter; 3] {
    [
        &CodexSessionAffinityAdapter,
        &ClaudeCodeSessionAffinityAdapter,
        &OpenCodeSessionAffinityAdapter,
    ]
}

fn extract_session_key_for_client_family(
    request: &ClientSessionAffinityRequest<'_>,
    client_family: &str,
) -> Option<String> {
    specific_client_session_affinity_adapters()
        .into_iter()
        .find(|adapter| adapter.family() == client_family)
        .and_then(|adapter| adapter.extract_session_key(request))
}

fn extract_session_key_from_other_specific_adapters(
    request: &ClientSessionAffinityRequest<'_>,
    client_family: &str,
) -> Option<String> {
    specific_client_session_affinity_adapters()
        .into_iter()
        .filter(|adapter| adapter.family() != client_family)
        .find_map(|adapter| adapter.extract_session_key(request))
}

impl ClientSessionAffinityAdapter for GenericSessionAffinityAdapter {
    fn family(&self) -> &'static str {
        "generic"
    }

    fn detect(&self, _request: &ClientSessionAffinityRequest<'_>) -> bool {
        true
    }

    fn extract_session_key(&self, request: &ClientSessionAffinityRequest<'_>) -> Option<String> {
        let body = request.body_json?;
        let root_session = value_at_paths(
            body,
            &[
                &["prompt_cache_key"],
                &["conversation_id"],
                &["conversationId"],
                &["session_id"],
                &["sessionId"],
                &["metadata", "session_id"],
                &["metadata", "conversation_id"],
                &["conversationState", "conversationId"],
                &["conversationState", "sessionId"],
            ],
        )?;
        let agent_id = value_at_paths(
            body,
            &[
                &["agent_id"],
                &["agentId"],
                &["metadata", "agent_id"],
                &["metadata", "agentId"],
                &["conversationState", "agentId"],
            ],
        );

        Some(normalize_session_key(root_session, agent_id))
    }
}

impl ClientSessionAffinityAdapter for CodexSessionAffinityAdapter {
    fn family(&self) -> &'static str {
        "codex"
    }

    fn detect(&self, request: &ClientSessionAffinityRequest<'_>) -> bool {
        header_contains(request.headers, http::header::USER_AGENT.as_str(), "codex")
            || header_contains(request.headers, "originator", "codex")
            || header_value_str(request.headers, "chatgpt-account-id").is_some()
    }

    fn extract_session_key(&self, request: &ClientSessionAffinityRequest<'_>) -> Option<String> {
        header_value_str(request.headers, "session_id")
            .or_else(|| header_value_str(request.headers, "conversation_id"))
            .or_else(|| header_value_str(request.headers, "x-client-request-id"))
            .map(|root_session| normalize_session_key(root_session.as_str(), None))
    }
}

impl ClientSessionAffinityAdapter for ClaudeCodeSessionAffinityAdapter {
    fn family(&self) -> &'static str {
        "claude_code"
    }

    fn detect(&self, request: &ClientSessionAffinityRequest<'_>) -> bool {
        header_contains(
            request.headers,
            http::header::USER_AGENT.as_str(),
            "claude-code",
        ) || header_contains(
            request.headers,
            http::header::USER_AGENT.as_str(),
            "claude code",
        ) || header_value_str(request.headers, "x-claude-code-session-id").is_some()
    }

    fn extract_session_key(&self, request: &ClientSessionAffinityRequest<'_>) -> Option<String> {
        header_value_str(request.headers, "x-claude-code-session-id")
            .or_else(|| header_value_str(request.headers, "session_id"))
            .or_else(|| header_value_str(request.headers, "conversation_id"))
            .map(|root_session| normalize_session_key(root_session.as_str(), None))
    }
}

impl ClientSessionAffinityAdapter for OpenCodeSessionAffinityAdapter {
    fn family(&self) -> &'static str {
        "opencode"
    }

    fn detect(&self, request: &ClientSessionAffinityRequest<'_>) -> bool {
        header_contains(
            request.headers,
            http::header::USER_AGENT.as_str(),
            "opencode",
        ) || header_value_str(request.headers, "x-opencode-session-id").is_some()
    }

    fn extract_session_key(&self, request: &ClientSessionAffinityRequest<'_>) -> Option<String> {
        let root_session = header_value_str(request.headers, "x-opencode-session-id")
            .or_else(|| header_value_str(request.headers, "session_id"))?;
        let agent_id = header_value_str(request.headers, "x-opencode-agent-id");
        Some(normalize_session_key(
            root_session.as_str(),
            agent_id.as_deref(),
        ))
    }
}

fn explicit_aether_session_key(request: &ClientSessionAffinityRequest<'_>) -> Option<String> {
    let root_session = header_value_str(request.headers, AETHER_SESSION_ID_HEADER)?;
    let agent_id = header_value_str(request.headers, AETHER_AGENT_ID_HEADER);
    Some(normalize_session_key(
        root_session.as_str(),
        agent_id.as_deref(),
    ))
}

fn normalize_session_key(root_session: &str, agent_id: Option<&str>) -> String {
    let root_session = root_session.trim();
    match agent_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(agent_id) => format!("session={root_session};agent={agent_id}"),
        None => format!("session={root_session}"),
    }
}

fn value_at_paths<'a>(body: &'a Value, paths: &[&[&str]]) -> Option<&'a str> {
    paths.iter().find_map(|path| value_at_path(body, path))
}

fn value_at_path<'a>(body: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = body;
    for segment in path {
        current = current.get(*segment)?;
    }
    current
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn header_contains(headers: &http::HeaderMap, key: &str, needle: &str) -> bool {
    header_value_str(headers, key)
        .map(|value| value.to_ascii_lowercase().contains(needle))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        client_session_affinity_from_request, AETHER_AGENT_ID_HEADER, AETHER_SESSION_ID_HEADER,
    };
    use http::{HeaderMap, HeaderValue};
    use serde_json::json;

    #[test]
    fn generic_adapter_extracts_body_session_and_agent() {
        let body = json!({
            "metadata": {
                "session_id": "session-1",
                "agent_id": "planner"
            }
        });

        let affinity = client_session_affinity_from_request(&HeaderMap::new(), Some(&body))
            .expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("generic"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=session-1;agent=planner")
        );
    }

    #[test]
    fn explicit_aether_headers_win_over_body_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AETHER_SESSION_ID_HEADER,
            HeaderValue::from_static("root-session"),
        );
        headers.insert(AETHER_AGENT_ID_HEADER, HeaderValue::from_static("coder"));
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("OpenCode/1.0"),
        );
        let body = json!({"session_id": "body-session"});

        let affinity = client_session_affinity_from_request(&headers, Some(&body))
            .expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("opencode"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=root-session;agent=coder")
        );
    }

    #[test]
    fn codex_adapter_extracts_header_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("codex-tui/0.122.0"),
        );
        headers.insert("session_id", HeaderValue::from_static("codex-session"));

        let affinity =
            client_session_affinity_from_request(&headers, None).expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("codex"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=codex-session")
        );
    }

    #[test]
    fn claude_code_adapter_extracts_session_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("Claude-Code/1.0"),
        );
        headers.insert(
            "x-claude-code-session-id",
            HeaderValue::from_static("claude-session"),
        );

        let affinity =
            client_session_affinity_from_request(&headers, None).expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("claude_code"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=claude-session")
        );
    }

    #[test]
    fn opencode_adapter_keeps_agent_dimension() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("OpenCode/0.9"),
        );
        headers.insert(
            "x-opencode-session-id",
            HeaderValue::from_static("oc-session"),
        );
        headers.insert("x-opencode-agent-id", HeaderValue::from_static("reviewer"));

        let affinity =
            client_session_affinity_from_request(&headers, None).expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("opencode"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=oc-session;agent=reviewer")
        );
    }

    #[test]
    fn specific_adapter_wins_over_generic_body_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("OpenCode/0.9"),
        );
        headers.insert(
            "x-opencode-session-id",
            HeaderValue::from_static("oc-session"),
        );
        headers.insert("x-opencode-agent-id", HeaderValue::from_static("reviewer"));
        let body = json!({
            "session_id": "body-session",
            "agent_id": "body-agent"
        });

        let affinity = client_session_affinity_from_request(&headers, Some(&body))
            .expect("affinity should build");

        assert_eq!(affinity.client_family.as_deref(), Some("opencode"));
        assert_eq!(
            affinity.session_key.as_deref(),
            Some("session=oc-session;agent=reviewer")
        );
    }

    #[test]
    fn missing_session_signal_returns_none() {
        let headers = HeaderMap::new();
        let body = json!({"model": "gpt-5"});

        assert!(client_session_affinity_from_request(&headers, Some(&body)).is_none());
    }
}
