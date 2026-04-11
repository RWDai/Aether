use std::sync::{Arc, Mutex};

use aether_crypto::{decrypt_python_fernet_ciphertext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::auth_modules::{
    AuthModuleReadRepository, InMemoryAuthModuleReadRepository, StoredOAuthProviderModuleConfig,
};
use aether_data::repository::global_models::InMemoryGlobalModelReadRepository;
use aether_data::repository::oauth_providers::{
    InMemoryOAuthProviderRepository, OAuthProviderReadRepository, StoredOAuthProviderConfig,
};
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, GlobalModelReadRepository,
    StoredPublicGlobalModel,
};
use aether_data_contracts::repository::provider_catalog::ProviderCatalogReadRepository;
use axum::body::Body;
use axum::routing::any;
use axum::{extract::Request, Router};
use http::StatusCode;
use serde_json::{json, Value};

use super::super::{build_router_with_state, start_server, AppState};
use crate::constants::{
    GATEWAY_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER, TRUSTED_ADMIN_USER_ID_HEADER,
    TRUSTED_ADMIN_USER_ROLE_HEADER,
};
use crate::data::GatewayDataState;

fn build_empty_admin_system_data_state() -> GatewayDataState {
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ));
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(Vec::<
        StoredPublicGlobalModel,
    >::new()));
    let auth_module_repository = Arc::new(InMemoryAuthModuleReadRepository::seed(
        Vec::<StoredOAuthProviderModuleConfig>::new(),
        None,
    ));
    let oauth_provider_repository = Arc::new(InMemoryOAuthProviderRepository::seed(Vec::<
        StoredOAuthProviderConfig,
    >::new()));

    GatewayDataState::with_provider_catalog_repository_for_tests(provider_catalog_repository)
        .with_global_model_repository_for_tests(global_model_repository)
        .attach_auth_module_repository_for_tests(auth_module_repository)
        .attach_oauth_provider_repository_for_tests(oauth_provider_repository)
        .with_system_config_values_for_tests(Vec::<(String, Value)>::new())
        .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
}

fn sample_system_import_payload() -> Value {
    json!({
        "version": "2.2",
        "merge_mode": "overwrite",
        "global_models": [{
            "name": "gpt-5",
            "display_name": "GPT 5",
            "default_price_per_request": 0.03,
            "default_tiered_pricing": {
                "tiers": [{
                    "up_to": null,
                    "input_price_per_1m": 4.0,
                    "output_price_per_1m": 20.0,
                }]
            },
            "supported_capabilities": ["streaming", "vision"],
            "config": { "quality": "high" },
            "is_active": true
        }],
        "providers": [{
            "name": "import-openai",
            "provider_type": "custom",
            "website": "https://example.com",
            "billing_type": "pay_as_you_go",
            "provider_priority": 10,
            "keep_priority_on_conversion": false,
            "enable_format_conversion": true,
            "is_active": true,
            "max_retries": 2,
            "request_timeout": 30.0,
            "stream_first_byte_timeout": 15.0,
            "config": {
                "provider_ops": {
                    "connector": {
                        "credentials": {
                            "api_key": "ops-secret"
                        }
                    }
                }
            },
            "endpoints": [{
                "api_format": "openai:chat",
                "base_url": "https://api.example.com",
                "max_retries": 2,
                "is_active": true
            }],
            "api_keys": [{
                "name": "primary",
                "api_formats": ["openai:chat"],
                "auth_type": "api_key",
                "api_key": "sk-import-123",
                "internal_priority": 5,
                "is_active": true
            }],
            "models": [{
                "global_model_name": "gpt-5",
                "provider_model_name": "gpt-5",
                "price_per_request": 0.03,
                "tiered_pricing": {
                    "tiers": [{
                        "up_to": null,
                        "input_price_per_1m": 4.0,
                        "output_price_per_1m": 20.0,
                    }]
                },
                "supports_vision": true,
                "supports_function_calling": true,
                "supports_streaming": true,
                "supports_extended_thinking": false,
                "supports_image_generation": false,
                "is_active": true,
                "config": {
                    "kind": "chat"
                }
            }]
        }],
        "ldap_config": {
            "server_url": "ldaps://ldap.example.com",
            "bind_dn": "cn=admin,dc=example,dc=com",
            "bind_password": "bind-secret",
            "base_dn": "dc=example,dc=com",
            "user_search_filter": "(uid={username})",
            "username_attr": "uid",
            "email_attr": "mail",
            "display_name_attr": "displayName",
            "is_enabled": false,
            "is_exclusive": false,
            "use_starttls": true,
            "connect_timeout": 10
        },
        "oauth_providers": [{
            "provider_type": "linuxdo",
            "display_name": "Linux Do",
            "client_id": "linuxdo-client",
            "client_secret": "linuxdo-secret",
            "authorization_url_override": "https://connect.linux.do/oauth2/authorize",
            "token_url_override": "https://connect.linux.do/oauth2/token",
            "userinfo_url_override": "https://connect.linux.do/api/user",
            "scopes": ["openid", "profile"],
            "redirect_uri": "https://backend.example.com/oauth/callback",
            "frontend_callback_url": "https://frontend.example.com/auth/callback",
            "attribute_mapping": { "email": "email" },
            "extra_config": { "team": true },
            "is_enabled": true
        }],
        "system_configs": [
            {
                "key": "site_name",
                "value": "Imported Aether",
                "description": "Site name"
            },
            {
                "key": "smtp_password",
                "value": "smtp-secret",
                "description": "SMTP secret"
            }
        ]
    })
}

fn fixture_system_import_payload(name: &str) -> Value {
    let raw = match name {
        "v20" => include_str!("../../fixtures/admin_system/config_export_v20.json"),
        "v21" => include_str!("../../fixtures/admin_system/config_export_v21.json"),
        "v22" => include_str!("../../fixtures/admin_system/config_export_v22.json"),
        _ => panic!("unknown fixture: {name}"),
    };
    serde_json::from_str(raw).expect("fixture json should parse")
}

#[tokio::test]
async fn gateway_imports_admin_system_config_locally_and_persists_data() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ));
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(Vec::<
        StoredPublicGlobalModel,
    >::new()));
    let auth_module_repository = Arc::new(InMemoryAuthModuleReadRepository::seed(
        Vec::<StoredOAuthProviderModuleConfig>::new(),
        None,
    ));
    let oauth_provider_repository = Arc::new(InMemoryOAuthProviderRepository::seed(Vec::<
        StoredOAuthProviderConfig,
    >::new()));

    let data_state = GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(
        &provider_catalog_repository,
    ))
    .with_global_model_repository_for_tests(Arc::clone(&global_model_repository))
    .attach_auth_module_repository_for_tests(Arc::clone(&auth_module_repository))
    .attach_oauth_provider_repository_for_tests(Arc::clone(&oauth_provider_repository))
    .with_system_config_values_for_tests(Vec::<(String, Value)>::new())
    .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY);

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/api/admin/system/config/import"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&sample_system_import_payload())
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["message"], "配置导入成功");
    assert_eq!(payload["stats"]["global_models"]["created"], json!(1));
    assert_eq!(payload["stats"]["providers"]["created"], json!(1));
    assert_eq!(payload["stats"]["endpoints"]["created"], json!(1));
    assert_eq!(payload["stats"]["keys"]["created"], json!(1));
    assert_eq!(payload["stats"]["models"]["created"], json!(1));
    assert_eq!(payload["stats"]["ldap"]["created"], json!(1));
    assert_eq!(payload["stats"]["oauth"]["created"], json!(1));
    assert_eq!(payload["stats"]["system_configs"]["created"], json!(2));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    let global_models = global_model_repository
        .list_admin_global_models(&AdminGlobalModelListQuery {
            offset: 0,
            limit: 10_000,
            is_active: None,
            search: None,
        })
        .await
        .expect("global models should load");
    assert_eq!(global_models.items.len(), 1);
    assert_eq!(global_models.items[0].name, "gpt-5");

    let providers = provider_catalog_repository
        .list_providers(false)
        .await
        .expect("providers should load");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].name, "import-openai");
    assert!(providers[0].enable_format_conversion);

    let provider_ids = providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<Vec<_>>();
    let endpoints = provider_catalog_repository
        .list_endpoints_by_provider_ids(&provider_ids)
        .await
        .expect("endpoints should load");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].api_format, "openai:chat");

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&provider_ids)
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);
    assert_eq!(
        decrypt_python_fernet_ciphertext(DEVELOPMENT_ENCRYPTION_KEY, &keys[0].encrypted_api_key)
            .expect("api key should decrypt"),
        "sk-import-123"
    );

    let provider_models = global_model_repository
        .list_admin_provider_models(&AdminProviderModelListQuery {
            provider_id: providers[0].id.clone(),
            is_active: None,
            offset: 0,
            limit: 10_000,
        })
        .await
        .expect("provider models should load");
    assert_eq!(provider_models.len(), 1);
    assert_eq!(provider_models[0].provider_model_name, "gpt-5");
    assert_eq!(
        provider_models[0].global_model_id,
        global_models.items[0].id
    );

    let ldap_config = auth_module_repository
        .get_ldap_config()
        .await
        .expect("ldap config should load")
        .expect("ldap config should exist");
    assert_eq!(ldap_config.server_url, "ldaps://ldap.example.com");
    assert_eq!(
        decrypt_python_fernet_ciphertext(
            DEVELOPMENT_ENCRYPTION_KEY,
            ldap_config
                .bind_password_encrypted
                .as_deref()
                .expect("bind password should exist"),
        )
        .expect("ldap password should decrypt"),
        "bind-secret"
    );

    let oauth_provider = oauth_provider_repository
        .get_oauth_provider_config("linuxdo")
        .await
        .expect("oauth config should load")
        .expect("oauth config should exist");
    assert_eq!(oauth_provider.client_id, "linuxdo-client");
    assert_eq!(
        decrypt_python_fernet_ciphertext(
            DEVELOPMENT_ENCRYPTION_KEY,
            oauth_provider
                .client_secret_encrypted
                .as_deref()
                .expect("oauth secret should exist"),
        )
        .expect("oauth secret should decrypt"),
        "linuxdo-secret"
    );

    let export_response = client
        .get(format!("{gateway_url}/api/admin/system/config/export"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("export request should succeed");
    assert_eq!(export_response.status(), StatusCode::OK);
    let export_payload: Value = export_response
        .json()
        .await
        .expect("export json should parse");

    let exported_provider = export_payload["providers"]
        .as_array()
        .and_then(|items| items.first())
        .expect("provider export should exist");
    assert_eq!(
        exported_provider["config"]["provider_ops"]["connector"]["credentials"]["api_key"],
        "ops-secret"
    );

    let exported_ldap = export_payload["ldap_config"]
        .as_object()
        .expect("ldap export should exist");
    assert_eq!(exported_ldap["bind_password"], "bind-secret");

    let exported_oauth = export_payload["oauth_providers"]
        .as_array()
        .and_then(|items| items.first())
        .expect("oauth export should exist");
    assert_eq!(exported_oauth["client_secret"], "linuxdo-secret");

    let exported_system_configs = export_payload["system_configs"]
        .as_array()
        .expect("system configs export should exist");
    let exported_site_name = exported_system_configs
        .iter()
        .find(|entry| entry["key"] == "site_name")
        .expect("site_name should exist");
    let exported_smtp_password = exported_system_configs
        .iter()
        .find(|entry| entry["key"] == "smtp_password")
        .expect("smtp_password should exist");
    assert_eq!(exported_site_name["value"], "Imported Aether");
    assert_eq!(exported_smtp_password["value"], "smtp-secret");

    gateway_handle.abort();
    upstream_handle.abort();
    let _ = upstream_url;
}

#[tokio::test]
async fn gateway_returns_503_for_admin_system_config_import_when_local_data_is_unavailable() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/system/config/import"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({ "version": "2.2" }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "Admin system data unavailable");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    let _ = upstream_url;
}

#[tokio::test]
async fn gateway_accepts_legacy_admin_system_config_import_versions() {
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(build_empty_admin_system_data_state()),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    for version in ["2.0", "2.1"] {
        let response = client
            .post(format!("{gateway_url}/api/admin/system/config/import"))
            .header(GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .json(&json!({
                "version": version,
                "merge_mode": "skip",
                "global_models": [],
                "providers": []
            }))
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["message"], "配置导入成功");
        assert_eq!(payload["stats"]["errors"], json!([]));
    }

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_imports_admin_system_config_fixtures_from_legacy_exports() {
    for fixture in ["v20", "v21", "v22"] {
        let gateway = build_router_with_state(
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(build_empty_admin_system_data_state()),
        );
        let (gateway_url, gateway_handle) = start_server(gateway).await;

        let response = reqwest::Client::new()
            .post(format!("{gateway_url}/api/admin/system/config/import"))
            .header(GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .json(&fixture_system_import_payload(fixture))
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "fixture {fixture} should import"
        );
        let payload: Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["message"], "配置导入成功");

        gateway_handle.abort();
    }
}

#[tokio::test]
async fn gateway_skips_proxy_nodes_during_admin_system_config_import() {
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(build_empty_admin_system_data_state()),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/system/config/import"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "version": "2.2",
            "merge_mode": "overwrite",
            "global_models": [],
            "providers": [],
            "proxy_nodes": [{
                "id": "legacy-node-1",
                "name": "Legacy Node",
                "ip": "127.0.0.1",
                "port": 8080
            }]
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["stats"]["proxy_nodes"]["skipped"], json!(1));
    assert!(payload["stats"]["errors"]
        .as_array()
        .expect("errors should be an array")
        .iter()
        .any(|item| item
            .as_str()
            .is_some_and(|value| value.contains("暂不支持导入代理节点"))));

    gateway_handle.abort();
}
