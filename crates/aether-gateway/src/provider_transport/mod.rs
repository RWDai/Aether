mod adapters;
mod auth;
mod auth_config;
mod network;
mod oauth_refresh;
mod policy;
mod rules;
mod snapshot;
mod url;

pub(crate) use adapters::antigravity::{
    build_antigravity_safe_v1internal_request, build_antigravity_static_identity_headers,
    build_antigravity_v1internal_url, classify_local_antigravity_request_support,
    resolve_local_antigravity_request_auth, AntigravityEnvelopeRequestType,
    AntigravityRequestAuthSupport, AntigravityRequestEnvelopeSupport,
    AntigravityRequestSideSupport, AntigravityRequestUrlAction,
};
pub(crate) use adapters::claude::{
    build_claude_messages_url, build_passthrough_headers_with_auth, build_passthrough_path_url,
    resolve_local_standard_auth, supports_local_standard_transport_with_network,
};
pub(crate) use adapters::claude_code::{
    build_claude_code_messages_url, build_claude_code_passthrough_headers,
    sanitize_claude_code_request_body, supports_local_claude_code_transport_with_network,
};
pub(crate) use adapters::gemini::{
    build_gemini_content_url, build_gemini_files_passthrough_url,
    build_gemini_video_predict_long_running_url, resolve_local_gemini_auth,
};
#[cfg(test)]
pub(crate) use adapters::generic_oauth::GenericOAuthRefreshAdapter;
pub(crate) use adapters::kiro::KiroOAuthRefreshAdapter;
pub(crate) use adapters::kiro::{
    build_kiro_generate_assistant_response_url, build_kiro_provider_headers,
    build_kiro_provider_request_body, supports_local_kiro_request_transport_with_network,
    KiroAuthConfig, KiroRequestAuth, KIRO_ENVELOPE_NAME,
};
pub(crate) use adapters::openai::{
    build_openai_chat_url, build_openai_cli_url, build_openai_passthrough_headers,
    resolve_local_openai_chat_auth, supports_local_openai_chat_transport,
};
pub(crate) use adapters::vertex::{
    build_vertex_api_key_gemini_content_url, resolve_local_vertex_api_key_query_auth,
    supports_local_vertex_api_key_gemini_transport_with_network,
};
pub(crate) use auth::{build_passthrough_headers, ensure_upstream_auth_header};
pub(crate) use network::{
    resolve_transport_execution_timeouts, resolve_transport_proxy_snapshot,
    resolve_transport_tls_profile, transport_proxy_is_locally_supported,
};
#[cfg(test)]
pub(crate) use oauth_refresh::LocalOAuthRefreshAdapter;
pub(crate) use oauth_refresh::{
    supports_local_oauth_request_auth_resolution, CachedOAuthEntry, LocalOAuthRefreshCoordinator,
    LocalOAuthRefreshError, LocalResolvedOAuthRequestAuth,
};
pub(crate) use policy::{
    supports_local_gemini_transport, supports_local_gemini_transport_with_network,
    supports_local_standard_transport,
};
pub(crate) use rules::{
    apply_local_body_rules, apply_local_header_rules, body_rules_are_locally_supported,
    header_rules_are_locally_supported,
};
pub(crate) use snapshot::{read_provider_transport_snapshot, GatewayProviderTransportSnapshot};
