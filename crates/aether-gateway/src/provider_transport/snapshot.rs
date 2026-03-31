use aether_crypto::{decrypt_python_fernet_ciphertext, looks_like_python_fernet_ciphertext};
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::DataLayerError;

use super::auth_config::{absorb_local_auth_config_safe_subset, LocalAuthConfigAbsorption};
use crate::gateway::data::GatewayDataState;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct GatewayProviderTransportSnapshot {
    pub(crate) provider: GatewayProviderTransportProvider,
    pub(crate) endpoint: GatewayProviderTransportEndpoint,
    pub(crate) key: GatewayProviderTransportKey,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct GatewayProviderTransportProvider {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) provider_type: String,
    pub(crate) website: Option<String>,
    pub(crate) is_active: bool,
    pub(crate) keep_priority_on_conversion: bool,
    pub(crate) enable_format_conversion: bool,
    pub(crate) concurrent_limit: Option<i32>,
    pub(crate) max_retries: Option<i32>,
    pub(crate) proxy: Option<serde_json::Value>,
    pub(crate) request_timeout_secs: Option<f64>,
    pub(crate) stream_first_byte_timeout_secs: Option<f64>,
    pub(crate) config: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct GatewayProviderTransportEndpoint {
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) api_format: String,
    pub(crate) api_family: Option<String>,
    pub(crate) endpoint_kind: Option<String>,
    pub(crate) is_active: bool,
    pub(crate) base_url: String,
    pub(crate) header_rules: Option<serde_json::Value>,
    pub(crate) body_rules: Option<serde_json::Value>,
    pub(crate) max_retries: Option<i32>,
    pub(crate) custom_path: Option<String>,
    pub(crate) config: Option<serde_json::Value>,
    pub(crate) format_acceptance_config: Option<serde_json::Value>,
    pub(crate) proxy: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct GatewayProviderTransportKey {
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) name: String,
    pub(crate) auth_type: String,
    pub(crate) is_active: bool,
    pub(crate) api_formats: Option<Vec<String>>,
    pub(crate) allowed_models: Option<Vec<String>>,
    pub(crate) capabilities: Option<serde_json::Value>,
    pub(crate) rate_multipliers: Option<serde_json::Value>,
    pub(crate) global_priority_by_format: Option<serde_json::Value>,
    pub(crate) expires_at_unix_secs: Option<u64>,
    pub(crate) proxy: Option<serde_json::Value>,
    pub(crate) fingerprint: Option<serde_json::Value>,
    pub(crate) decrypted_api_key: String,
    pub(crate) decrypted_auth_config: Option<String>,
}

pub(crate) async fn read_provider_transport_snapshot(
    state: &GatewayDataState,
    provider_id: &str,
    endpoint_id: &str,
    key_id: &str,
) -> Result<Option<GatewayProviderTransportSnapshot>, DataLayerError> {
    let Some(encryption_key) = state.encryption_key() else {
        return Ok(None);
    };
    let fallback_encryption_keys = fallback_encryption_keys(encryption_key);

    let providers = state
        .list_provider_catalog_providers_by_ids(&[provider_id.to_string()])
        .await?;
    let endpoints = state
        .list_provider_catalog_endpoints_by_ids(&[endpoint_id.to_string()])
        .await?;
    let keys = state
        .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
        .await?;

    let Some(provider) = providers.into_iter().next() else {
        return Ok(None);
    };
    let Some(endpoint) = endpoints.into_iter().next() else {
        return Ok(None);
    };
    let Some(key) = keys.into_iter().next() else {
        return Ok(None);
    };

    if endpoint.provider_id != provider.id {
        return Err(DataLayerError::UnexpectedValue(format!(
            "provider_endpoints.provider_id mismatch: expected {}, got {}",
            provider.id, endpoint.provider_id
        )));
    }
    if key.provider_id != provider.id {
        return Err(DataLayerError::UnexpectedValue(format!(
            "provider_api_keys.provider_id mismatch: expected {}, got {}",
            provider.id, key.provider_id
        )));
    }

    let provider = map_provider(provider);
    let mut endpoint = map_endpoint(endpoint);
    let mut key = map_key(key, encryption_key, &fallback_encryption_keys)?;

    if let LocalAuthConfigAbsorption::Absorbed {
        base_url,
        header_rules,
        custom_path,
    } = absorb_local_auth_config_safe_subset(
        &endpoint.base_url,
        endpoint.header_rules.clone(),
        endpoint.custom_path.clone(),
        key.decrypted_auth_config.as_deref(),
    ) {
        endpoint.base_url = base_url;
        endpoint.header_rules = header_rules;
        endpoint.custom_path = custom_path;
        key.decrypted_auth_config = None;
    }

    Ok(Some(GatewayProviderTransportSnapshot {
        provider,
        endpoint,
        key,
    }))
}

fn map_provider(provider: StoredProviderCatalogProvider) -> GatewayProviderTransportProvider {
    GatewayProviderTransportProvider {
        id: provider.id,
        name: provider.name,
        provider_type: provider.provider_type,
        website: provider.website,
        is_active: provider.is_active,
        keep_priority_on_conversion: provider.keep_priority_on_conversion,
        enable_format_conversion: provider.enable_format_conversion,
        concurrent_limit: provider.concurrent_limit,
        max_retries: provider.max_retries,
        proxy: normalize_optional_json(provider.proxy),
        request_timeout_secs: provider.request_timeout_secs,
        stream_first_byte_timeout_secs: provider.stream_first_byte_timeout_secs,
        config: normalize_optional_json(provider.config),
    }
}

fn map_endpoint(endpoint: StoredProviderCatalogEndpoint) -> GatewayProviderTransportEndpoint {
    GatewayProviderTransportEndpoint {
        id: endpoint.id,
        provider_id: endpoint.provider_id,
        api_format: endpoint.api_format,
        api_family: endpoint.api_family,
        endpoint_kind: endpoint.endpoint_kind,
        is_active: endpoint.is_active,
        base_url: endpoint.base_url,
        header_rules: normalize_optional_json(endpoint.header_rules),
        body_rules: normalize_optional_json(endpoint.body_rules),
        max_retries: endpoint.max_retries,
        custom_path: endpoint.custom_path,
        config: normalize_optional_json(endpoint.config),
        format_acceptance_config: normalize_optional_json(endpoint.format_acceptance_config),
        proxy: normalize_optional_json(endpoint.proxy),
    }
}

fn map_key(
    key: StoredProviderCatalogKey,
    encryption_key: &str,
    fallback_encryption_keys: &[String],
) -> Result<GatewayProviderTransportKey, DataLayerError> {
    let decrypted_api_key = decrypt_secret(
        encryption_key,
        fallback_encryption_keys,
        &key.encrypted_api_key,
        "provider_api_keys.api_key",
    )?;
    let decrypted_auth_config = key
        .encrypted_auth_config
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|ciphertext| {
            decrypt_secret(
                encryption_key,
                fallback_encryption_keys,
                ciphertext,
                "provider_api_keys.auth_config",
            )
        })
        .transpose()?;

    Ok(GatewayProviderTransportKey {
        id: key.id,
        provider_id: key.provider_id,
        name: key.name,
        auth_type: key.auth_type,
        is_active: key.is_active,
        api_formats: normalize_string_list(
            normalize_optional_json(key.api_formats),
            "provider_api_keys.api_formats",
        )?,
        allowed_models: normalize_string_list(
            normalize_optional_json(key.allowed_models),
            "provider_api_keys.allowed_models",
        )?,
        capabilities: normalize_optional_json(key.capabilities),
        rate_multipliers: normalize_optional_json(key.rate_multipliers),
        global_priority_by_format: normalize_optional_json(key.global_priority_by_format),
        expires_at_unix_secs: key.expires_at_unix_secs,
        proxy: normalize_optional_json(key.proxy),
        fingerprint: normalize_optional_json(key.fingerprint),
        decrypted_api_key,
        decrypted_auth_config,
    })
}

fn normalize_optional_json(value: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match value {
        Some(serde_json::Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

fn decrypt_secret(
    encryption_key: &str,
    fallback_encryption_keys: &[String],
    ciphertext: &str,
    field_name: &str,
) -> Result<String, DataLayerError> {
    match decrypt_python_fernet_ciphertext(encryption_key, ciphertext) {
        Ok(value) => Ok(value),
        Err(error) if should_use_plaintext_secret(ciphertext, field_name) => {
            Ok(ciphertext.trim().to_string())
        }
        Err(error) => {
            for fallback_encryption_key in fallback_encryption_keys {
                if let Ok(value) =
                    decrypt_python_fernet_ciphertext(fallback_encryption_key, ciphertext)
                {
                    return Ok(value);
                }
            }
            Err(DataLayerError::UnexpectedValue(format!(
                "failed to decrypt {field_name}: {error}"
            )))
        }
    }
}

fn fallback_encryption_keys(primary_encryption_key: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for env_key in ["AETHER_GATEWAY_DATA_ENCRYPTION_KEY", "ENCRYPTION_KEY"] {
        let Ok(value) = std::env::var(env_key) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty()
            || value == primary_encryption_key
            || keys.iter().any(|existing| existing == value)
        {
            continue;
        }
        keys.push(value.to_string());
    }
    keys
}

fn should_use_plaintext_secret(ciphertext: &str, field_name: &str) -> bool {
    let ciphertext = ciphertext.trim();
    if ciphertext.is_empty() {
        return false;
    }
    if looks_like_python_fernet_ciphertext(ciphertext) {
        return false;
    }

    match field_name {
        "provider_api_keys.api_key" => !ciphertext.starts_with('{') && !ciphertext.starts_with('['),
        "provider_api_keys.auth_config" => {
            ciphertext.starts_with('{') || ciphertext.starts_with('[')
        }
        _ => false,
    }
}

fn normalize_string_list(
    raw: Option<serde_json::Value>,
    field_name: &str,
) -> Result<Option<Vec<String>>, DataLayerError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    normalize_string_list_value(&raw, field_name)
}

fn normalize_string_list_value(
    raw: &serde_json::Value,
    field_name: &str,
) -> Result<Option<Vec<String>>, DataLayerError> {
    match raw {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Array(items) => normalize_string_list_array(items, field_name).map(Some),
        serde_json::Value::String(raw) => normalize_embedded_string_list(raw, field_name),
        _ => Err(DataLayerError::UnexpectedValue(format!(
            "{field_name} is not a JSON array"
        ))),
    }
}

fn normalize_embedded_string_list(
    raw: &str,
    field_name: &str,
) -> Result<Option<Vec<String>>, DataLayerError> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        return Ok(None);
    }

    if let Ok(decoded) = serde_json::from_str::<serde_json::Value>(raw) {
        return normalize_string_list_value(&decoded, field_name);
    }

    Ok(Some(vec![raw.to_string()]))
}

fn normalize_string_list_array(
    items: &[serde_json::Value],
    field_name: &str,
) -> Result<Vec<String>, DataLayerError> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = item.as_str() else {
            return Err(DataLayerError::UnexpectedValue(format!(
                "{field_name} contains a non-string item"
            )));
        };
        let value = value.trim();
        if !value.is_empty() {
            values.push(value.to_string());
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
    use aether_data::repository::provider_catalog::{
        InMemoryProviderCatalogReadRepository, StoredProviderCatalogEndpoint,
        StoredProviderCatalogKey, StoredProviderCatalogProvider,
    };
    use aether_data::DataLayerError;

    use crate::gateway::data::GatewayDataState;
    use crate::gateway::provider_transport::{
        supports_local_openai_chat_transport, supports_local_standard_transport_with_network,
    };

    use super::{map_key, read_provider_transport_snapshot, GatewayProviderTransportSnapshot};

    fn sample_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-1".to_string(),
            "OpenAI".to_string(),
            Some("https://openai.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            Some(32),
            Some(3),
            Some(serde_json::json!({"url":"http://provider-proxy"})),
            Some(20.0),
            Some(8.0),
            Some(serde_json::json!({"region":"global"})),
        )
    }

    fn sample_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-1".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com".to_string(),
            Some(serde_json::json!([{"action":"set","key":"x-test","value":"1"}])),
            Some(serde_json::json!([{"action":"drop","path":"stream"}])),
            Some(2),
            Some("/v1/chat/completions".to_string()),
            Some(serde_json::json!({"api_version":"v1"})),
            Some(serde_json::json!({"allow":["openai:chat"]})),
            Some(serde_json::json!({"url":"http://endpoint-proxy"})),
        )
        .expect("endpoint transport fields should build")
    }

    fn sample_key() -> StoredProviderCatalogKey {
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            "{\"refresh_token\":\"rt-1\",\"project\":\"demo\"}",
        )
        .expect("auth config ciphertext should build");
        StoredProviderCatalogKey::new(
            "key-1".to_string(),
            "provider-1".to_string(),
            "prod-key".to_string(),
            "api_key".to_string(),
            Some(serde_json::json!({"cache_1h": true})),
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat", "openai:cli"])),
            encrypted_api_key,
            Some(encrypted_auth_config),
            Some(serde_json::json!({"openai:chat": 0.8})),
            Some(serde_json::json!({"openai:chat": 1})),
            Some(serde_json::json!(["gpt-4.1", "gpt-4.1-mini"])),
            Some(1_800_000_000),
            Some(serde_json::json!({"node_id":"proxy-node-1"})),
            Some(serde_json::json!({"tls_profile":"chrome_136"})),
        )
        .expect("key transport fields should build")
    }

    fn read_state() -> GatewayDataState {
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![sample_endpoint()],
            vec![sample_key()],
        ));
        GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        )
    }

    #[tokio::test]
    async fn reads_decrypted_provider_transport_snapshot() {
        let state = read_state();

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-1", "key-1")
                .await
                .expect("snapshot should read")
                .expect("snapshot should exist");

        assert_eq!(
            snapshot,
            GatewayProviderTransportSnapshot {
                provider: super::GatewayProviderTransportProvider {
                    id: "provider-1".to_string(),
                    name: "OpenAI".to_string(),
                    provider_type: "custom".to_string(),
                    website: Some("https://openai.com".to_string()),
                    is_active: true,
                    keep_priority_on_conversion: false,
                    enable_format_conversion: true,
                    concurrent_limit: Some(32),
                    max_retries: Some(3),
                    proxy: Some(serde_json::json!({"url":"http://provider-proxy"})),
                    request_timeout_secs: Some(20.0),
                    stream_first_byte_timeout_secs: Some(8.0),
                    config: Some(serde_json::json!({"region":"global"})),
                },
                endpoint: super::GatewayProviderTransportEndpoint {
                    id: "endpoint-1".to_string(),
                    provider_id: "provider-1".to_string(),
                    api_format: "openai:chat".to_string(),
                    api_family: Some("openai".to_string()),
                    endpoint_kind: Some("chat".to_string()),
                    is_active: true,
                    base_url: "https://api.openai.com".to_string(),
                    header_rules: Some(
                        serde_json::json!([{"action":"set","key":"x-test","value":"1"}]),
                    ),
                    body_rules: Some(serde_json::json!([{"action":"drop","path":"stream"}])),
                    max_retries: Some(2),
                    custom_path: Some("/v1/chat/completions".to_string()),
                    config: Some(serde_json::json!({"api_version":"v1"})),
                    format_acceptance_config: Some(serde_json::json!({"allow":["openai:chat"]}),),
                    proxy: Some(serde_json::json!({"url":"http://endpoint-proxy"})),
                },
                key: super::GatewayProviderTransportKey {
                    id: "key-1".to_string(),
                    provider_id: "provider-1".to_string(),
                    name: "prod-key".to_string(),
                    auth_type: "api_key".to_string(),
                    is_active: true,
                    api_formats: Some(vec!["openai:chat".to_string(), "openai:cli".to_string(),]),
                    allowed_models: Some(vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string(),]),
                    capabilities: Some(serde_json::json!({"cache_1h": true})),
                    rate_multipliers: Some(serde_json::json!({"openai:chat": 0.8})),
                    global_priority_by_format: Some(serde_json::json!({"openai:chat": 1})),
                    expires_at_unix_secs: Some(1_800_000_000),
                    proxy: Some(serde_json::json!({"node_id":"proxy-node-1"})),
                    fingerprint: Some(serde_json::json!({"tls_profile":"chrome_136"})),
                    decrypted_api_key: "sk-live-openai".to_string(),
                    decrypted_auth_config: Some(
                        "{\"refresh_token\":\"rt-1\",\"project\":\"demo\"}".to_string(),
                    ),
                },
            }
        );
    }

    #[tokio::test]
    async fn returns_none_when_encryption_key_is_not_configured() {
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider()],
            vec![sample_endpoint()],
            vec![sample_key()],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(repository, "");

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-1", "key-1")
                .await
                .expect("snapshot read should not error");
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn absorbs_safe_auth_config_into_local_transport_fields() {
        let provider = sample_provider();
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-safe-1".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com".to_string(),
            Some(serde_json::json!([{"action":"set","key":"x-test","value":"1"}])),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport fields should build");
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"headers":{"x-account-id":"acc-1"},"query":{"tenant":"demo"}}"#,
        )
        .expect("auth config ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-safe-1".to_string(),
            "provider-1".to_string(),
            "safe-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            encrypted_api_key,
            Some(encrypted_auth_config),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-safe-1", "key-safe-1")
                .await
                .expect("snapshot read should succeed")
                .expect("snapshot should exist");

        assert_eq!(snapshot.key.decrypted_auth_config, None);
        assert_eq!(
            snapshot.endpoint.base_url,
            "https://api.openai.com?tenant=demo"
        );
        assert_eq!(snapshot.endpoint.custom_path.as_deref(), None);
        assert_eq!(
            snapshot.endpoint.header_rules,
            Some(serde_json::json!([
                {"action":"set","key":"x-test","value":"1"},
                {"action":"set","key":"x-account-id","value":"acc-1"}
            ]))
        );
        assert!(supports_local_openai_chat_transport(&snapshot));
    }

    #[tokio::test]
    async fn accepts_plaintext_legacy_key_material() {
        let provider = sample_provider();
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-legacy-1".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com".to_string(),
            Some(serde_json::json!([{"action":"set","key":"x-test","value":"1"}])),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport fields should build");
        let key = StoredProviderCatalogKey::new(
            "key-legacy-1".to_string(),
            "provider-1".to_string(),
            "legacy-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            "sk-plaintext-openai".to_string(),
            Some(r#"{"headers":{"x-account-id":"acc-legacy"}}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot = read_provider_transport_snapshot(
            &state,
            "provider-1",
            "endpoint-legacy-1",
            "key-legacy-1",
        )
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");

        assert_eq!(snapshot.key.decrypted_api_key, "sk-plaintext-openai");
        assert_eq!(snapshot.key.decrypted_auth_config, None);
        assert_eq!(
            snapshot.endpoint.header_rules,
            Some(serde_json::json!([
                {"action":"set","key":"x-test","value":"1"},
                {"action":"set","key":"x-account-id","value":"acc-legacy"}
            ]))
        );
    }

    #[tokio::test]
    async fn rejects_fernet_shaped_key_material_when_encryption_key_is_wrong() {
        let key = sample_key();
        let error = map_key(key, "wrong-encryption-key", &[])
            .expect_err("snapshot read should fail for Fernet-shaped data with wrong key");

        assert!(matches!(error, DataLayerError::UnexpectedValue(message)
            if message.contains("failed to decrypt provider_api_keys.api_key")));
    }

    #[test]
    fn decrypts_fernet_shaped_key_material_with_fallback_encryption_key() {
        let key = sample_key();

        let mapped = map_key(
            key,
            "wrong-encryption-key",
            &[DEVELOPMENT_ENCRYPTION_KEY.to_string()],
        )
        .expect("fallback key should decrypt");

        assert_eq!(mapped.decrypted_api_key, "sk-live-openai");
        assert_eq!(
            mapped.decrypted_auth_config.as_deref(),
            Some("{\"refresh_token\":\"rt-1\",\"project\":\"demo\"}")
        );
    }

    #[test]
    fn accepts_stringified_allowed_models_in_transport_key() {
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-compat-1".to_string(),
            "provider-1".to_string(),
            "compat-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            encrypted_api_key,
            None,
            None,
            None,
            Some(serde_json::json!("[\"gpt-5.2\", \"gpt-5\"]")),
            None,
            None,
            None,
        )
        .expect("key transport fields should build");

        let mapped =
            map_key(key, DEVELOPMENT_ENCRYPTION_KEY, &[]).expect("stringified list should parse");

        assert_eq!(
            mapped.allowed_models,
            Some(vec!["gpt-5.2".to_string(), "gpt-5".to_string()])
        );
    }

    #[test]
    fn accepts_single_string_api_format_in_transport_key() {
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-compat-2".to_string(),
            "provider-1".to_string(),
            "compat-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!("openai:chat")),
            encrypted_api_key,
            None,
            None,
            None,
            Some(serde_json::json!("gpt-5.2")),
            None,
            None,
            None,
        )
        .expect("key transport fields should build");

        let mapped =
            map_key(key, DEVELOPMENT_ENCRYPTION_KEY, &[]).expect("single string should parse");

        assert_eq!(mapped.api_formats, Some(vec!["openai:chat".to_string()]));
        assert_eq!(mapped.allowed_models, Some(vec!["gpt-5.2".to_string()]));
    }

    #[tokio::test]
    async fn keeps_unsupported_auth_config_blocking_local_transport() {
        let provider = sample_provider();
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-safe-2".to_string(),
            "provider-1".to_string(),
            "openai:cli".to_string(),
            Some("openai".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com".to_string(),
            None,
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport fields should build");
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"refresh_token":"rt-1","project":"demo"}"#,
        )
        .expect("auth config ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-safe-2".to_string(),
            "provider-1".to_string(),
            "unsafe-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:cli"])),
            encrypted_api_key,
            Some(encrypted_auth_config),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-safe-2", "key-safe-2")
                .await
                .expect("snapshot read should succeed")
                .expect("snapshot should exist");

        assert_eq!(
            snapshot.key.decrypted_auth_config.as_deref(),
            Some(r#"{"refresh_token":"rt-1","project":"demo"}"#)
        );
        assert!(!supports_local_standard_transport_with_network(
            &snapshot,
            "openai:cli"
        ));
    }

    #[tokio::test]
    async fn absorbs_query_only_auth_config_into_gemini_base_url() {
        let provider = sample_provider();
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-safe-3".to_string(),
            "provider-1".to_string(),
            "gemini:chat".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com/v1beta".to_string(),
            None,
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport fields should build");
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"query":{"alt":"sse"}}"#,
        )
        .expect("auth config ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-safe-3".to_string(),
            "provider-1".to_string(),
            "safe-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["gemini:chat"])),
            encrypted_api_key,
            Some(encrypted_auth_config),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-safe-3", "key-safe-3")
                .await
                .expect("snapshot read should succeed")
                .expect("snapshot should exist");

        assert_eq!(
            snapshot.endpoint.base_url,
            "https://generativelanguage.googleapis.com/v1beta?alt=sse"
        );
        assert_eq!(snapshot.endpoint.custom_path, None);
        assert_eq!(snapshot.key.decrypted_auth_config, None);
    }

    #[tokio::test]
    async fn absorbs_transport_subset_when_metadata_is_present() {
        let provider = sample_provider();
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-safe-4".to_string(),
            "provider-1".to_string(),
            "openai:cli".to_string(),
            Some("openai".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com/v1".to_string(),
            Some(serde_json::json!([{"action":"set","key":"x-base","value":"1"}])),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport fields should build");
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{
                "email":"user@example.com",
                "plan_type":"plus",
                "transport":{
                    "extraHeaders":{"x-org-id":"org-1"},
                    "queryParams":{"tenant":"demo","retry":2}
                }
            }"#,
        )
        .expect("auth config ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-safe-4".to_string(),
            "provider-1".to_string(),
            "safe-key".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:cli"])),
            encrypted_api_key,
            Some(encrypted_auth_config),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot =
            read_provider_transport_snapshot(&state, "provider-1", "endpoint-safe-4", "key-safe-4")
                .await
                .expect("snapshot read should succeed")
                .expect("snapshot should exist");

        assert_eq!(snapshot.key.decrypted_auth_config, None);
        assert_eq!(
            snapshot.endpoint.base_url,
            "https://api.openai.com/v1?retry=2&tenant=demo"
        );
        assert_eq!(
            snapshot.endpoint.header_rules,
            Some(serde_json::json!([
                {"action":"set","key":"x-base","value":"1"},
                {"action":"set","key":"x-org-id","value":"org-1"}
            ]))
        );
        assert!(supports_local_standard_transport_with_network(
            &snapshot,
            "openai:cli"
        ));
    }

    #[tokio::test]
    async fn normalizes_json_null_transport_fields_before_local_support_checks() {
        let provider = sample_provider().with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            Some(serde_json::Value::Null),
            Some(20.0),
            Some(8.0),
            Some(serde_json::Value::Null),
        );
        let endpoint = StoredProviderCatalogEndpoint::new(
            "endpoint-null-json".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com".to_string(),
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
            Some(2),
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
        )
        .expect("endpoint transport fields should build");
        let encrypted_api_key =
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-live-openai")
                .expect("api key ciphertext should build");
        let key = StoredProviderCatalogKey::new(
            "key-null-json".to_string(),
            "provider-1".to_string(),
            "safe-key".to_string(),
            "api_key".to_string(),
            Some(serde_json::Value::Null),
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::Value::Null),
            encrypted_api_key,
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::Value::Null),
        )
        .expect("key transport fields should build");
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![provider],
            vec![endpoint],
            vec![key],
        ));
        let state = GatewayDataState::with_provider_transport_reader_for_tests(
            repository,
            DEVELOPMENT_ENCRYPTION_KEY.to_string(),
        );

        let snapshot = read_provider_transport_snapshot(
            &state,
            "provider-1",
            "endpoint-null-json",
            "key-null-json",
        )
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");

        assert_eq!(snapshot.provider.proxy, None);
        assert_eq!(snapshot.provider.config, None);
        assert_eq!(snapshot.endpoint.header_rules, None);
        assert_eq!(snapshot.endpoint.body_rules, None);
        assert_eq!(snapshot.endpoint.config, None);
        assert_eq!(snapshot.endpoint.format_acceptance_config, None);
        assert_eq!(snapshot.endpoint.proxy, None);
        assert_eq!(snapshot.key.api_formats, None);
        assert_eq!(snapshot.key.allowed_models, None);
        assert_eq!(snapshot.key.capabilities, None);
        assert_eq!(snapshot.key.rate_multipliers, None);
        assert_eq!(snapshot.key.global_priority_by_format, None);
        assert_eq!(snapshot.key.proxy, None);
        assert_eq!(snapshot.key.fingerprint, None);
        assert!(supports_local_openai_chat_transport(&snapshot));
    }
}
