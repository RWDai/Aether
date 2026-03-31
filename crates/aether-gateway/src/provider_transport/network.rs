use aether_contracts::{ExecutionTimeouts, ProxySnapshot};
use serde_json::{Map, Value};

use super::snapshot::GatewayProviderTransportSnapshot;

pub(crate) fn resolve_transport_execution_timeouts(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<ExecutionTimeouts> {
    let total_ms = transport
        .provider
        .request_timeout_secs
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| (value * 1000.0).round() as u64);
    let first_byte_ms = transport
        .provider
        .stream_first_byte_timeout_secs
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| (value * 1000.0).round() as u64);

    if total_ms.is_none() && first_byte_ms.is_none() {
        return None;
    }

    Some(ExecutionTimeouts {
        total_ms,
        first_byte_ms,
        ..ExecutionTimeouts::default()
    })
}

pub(crate) fn resolve_transport_proxy_snapshot(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<ProxySnapshot> {
    let raw = effective_proxy_config(transport)?;
    proxy_snapshot_from_value(raw)
}

pub(crate) fn transport_proxy_is_locally_supported(
    transport: &GatewayProviderTransportSnapshot,
) -> bool {
    let has_configured_proxy = transport.provider.proxy.is_some()
        || transport.endpoint.proxy.is_some()
        || transport.key.proxy.is_some();
    if !has_configured_proxy {
        return true;
    }

    let Some(snapshot) = resolve_transport_proxy_snapshot(transport) else {
        return false;
    };

    if snapshot.enabled == Some(false) {
        return true;
    }

    snapshot
        .url
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
        || snapshot
            .node_id
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
}

pub(crate) fn resolve_transport_tls_profile(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<String> {
    transport
        .key
        .fingerprint
        .as_ref()
        .and_then(|value| value.get("tls_profile"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn effective_proxy_config(transport: &GatewayProviderTransportSnapshot) -> Option<&Value> {
    for candidate in [
        transport.key.proxy.as_ref(),
        transport.endpoint.proxy.as_ref(),
        transport.provider.proxy.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if proxy_enabled(candidate) {
            return Some(candidate);
        }
    }
    None
}

fn proxy_enabled(value: &Value) -> bool {
    value
        .as_object()
        .and_then(|object| object.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn proxy_snapshot_from_value(value: &Value) -> Option<ProxySnapshot> {
    let object = value.as_object()?;
    let enabled = object.get("enabled").and_then(Value::as_bool);
    let mode = json_string_field(object, "mode");
    let node_id = json_string_field(object, "node_id");
    let label = json_string_field(object, "label");
    let url = json_string_field(object, "url").or_else(|| json_string_field(object, "proxy_url"));

    let mut extra = Map::new();
    for (key, value) in object {
        if matches!(
            key.as_str(),
            "enabled" | "mode" | "node_id" | "label" | "url" | "proxy_url"
        ) {
            continue;
        }
        extra.insert(key.clone(), value.clone());
    }

    Some(ProxySnapshot {
        enabled,
        mode,
        node_id,
        label,
        url,
        extra: if extra.is_empty() {
            None
        } else {
            Some(Value::Object(extra))
        },
    })
}

fn json_string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::gateway::provider_transport::snapshot::{
        GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
        GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
    };

    use super::{
        resolve_transport_proxy_snapshot, resolve_transport_tls_profile,
        transport_proxy_is_locally_supported,
    };

    fn sample_transport() -> GatewayProviderTransportSnapshot {
        GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: "provider-1".to_string(),
                name: "provider".to_string(),
                provider_type: "custom".to_string(),
                website: None,
                is_active: true,
                keep_priority_on_conversion: false,
                enable_format_conversion: false,
                concurrent_limit: None,
                max_retries: None,
                proxy: Some(json!({"url":"http://provider-proxy:8080"})),
                request_timeout_secs: None,
                stream_first_byte_timeout_secs: None,
                config: None,
            },
            endpoint: GatewayProviderTransportEndpoint {
                id: "endpoint-1".to_string(),
                provider_id: "provider-1".to_string(),
                api_format: "openai:chat".to_string(),
                api_family: Some("openai".to_string()),
                endpoint_kind: Some("chat".to_string()),
                is_active: true,
                base_url: "https://api.openai.example".to_string(),
                header_rules: None,
                body_rules: None,
                max_retries: None,
                custom_path: None,
                config: None,
                format_acceptance_config: None,
                proxy: Some(json!({"enabled":false,"url":"http://endpoint-proxy:8080"})),
            },
            key: GatewayProviderTransportKey {
                id: "key-1".to_string(),
                provider_id: "provider-1".to_string(),
                name: "key".to_string(),
                auth_type: "api_key".to_string(),
                is_active: true,
                api_formats: None,
                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: Some(json!({"node_id":"proxy-node-1","kind":"manual"})),
                fingerprint: Some(json!({"tls_profile":"chrome_136"})),
                decrypted_api_key: "sk-test".to_string(),
                decrypted_auth_config: None,
            },
        }
    }

    #[test]
    fn resolves_transport_proxy_with_key_precedence() {
        let snapshot = resolve_transport_proxy_snapshot(&sample_transport())
            .expect("proxy snapshot should resolve");
        assert_eq!(snapshot.node_id.as_deref(), Some("proxy-node-1"));
        assert_eq!(snapshot.url, None);
        assert_eq!(snapshot.extra, Some(json!({"kind":"manual"})));
    }

    #[test]
    fn resolves_transport_tls_profile_from_key_fingerprint() {
        assert_eq!(
            resolve_transport_tls_profile(&sample_transport()).as_deref(),
            Some("chrome_136")
        );
        assert!(transport_proxy_is_locally_supported(&sample_transport()));
    }
}
