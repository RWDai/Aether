use super::AdminAppState;
use crate::api::ai::admin_endpoint_signature_parts;
use crate::handlers::admin::provider::endpoints_admin::payloads::AdminProviderEndpointUpdatePatch;
use crate::handlers::admin::provider::shared::payloads::{
    AdminProviderCreateRequest, AdminProviderKeyCreateRequest, AdminProviderKeyUpdatePatch,
    AdminProviderUpdatePatch,
};
use crate::handlers::admin::shared::{
    normalize_json_array, normalize_json_object, normalize_string_list,
};
use crate::handlers::admin::system::shared::configs::apply_admin_system_config_update;
use crate::handlers::public::normalize_admin_base_url;
use crate::GatewayError;
use aether_admin::provider::endpoints as admin_provider_endpoints_pure;
use aether_admin::provider::models_write as admin_provider_models_write_pure;
use aether_admin::system::{
    normalize_admin_system_config_key, parse_admin_system_config_array,
    parse_admin_system_config_import_request, parse_admin_system_config_nested_array,
    parse_admin_system_config_optional_object, AdminImportMergeMode,
    AdminSystemConfigEndpoint as ImportedEndpoint, AdminSystemConfigEntry as ImportedSystemConfig,
    AdminSystemConfigGlobalModel as ImportedGlobalModel, AdminSystemConfigImportStats,
    AdminSystemConfigLdap as ImportedLdapConfig,
    AdminSystemConfigOAuthProvider as ImportedOAuthProvider,
    AdminSystemConfigProvider as ImportedProvider,
    AdminSystemConfigProviderKey as ImportedProviderKey,
    AdminSystemConfigProviderModel as ImportedProviderModel,
    AdminSystemConfigProxyNode as ImportedProxyNode,
    ADMIN_SYSTEM_PROVIDER_OPS_SENSITIVE_CREDENTIAL_FIELDS,
};
use aether_data::repository::auth_modules::StoredLdapModuleConfig;
use aether_data::repository::oauth_providers::{
    EncryptedSecretUpdate, UpsertOAuthProviderConfigRecord,
};
use aether_data_contracts::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, CreateAdminGlobalModelRecord,
    UpdateAdminGlobalModelRecord, UpsertAdminProviderModelRecord,
};
use axum::{body::Bytes, http};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const ADMIN_SYSTEM_IMPORT_MAX_SIZE_BYTES: usize = 10 * 1024 * 1024;

fn invalid_request(detail: impl Into<String>) -> (http::StatusCode, Value) {
    (
        http::StatusCode::BAD_REQUEST,
        json!({ "detail": detail.into() }),
    )
}

fn trim_required(value: &str, field_name: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{field_name} 不能为空"));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_price(value: Option<f64>, field_name: &str) -> Result<Option<f64>, String> {
    admin_provider_models_write_pure::normalize_optional_price(value, field_name)
}

fn normalize_supported_capabilities(value: Option<Vec<String>>) -> Option<Value> {
    normalize_string_list(value).map(|items| json!(items))
}

fn normalize_import_auth_config(value: Option<Value>) -> Result<Option<Value>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed = serde_json::from_str::<Value>(trimmed)
                .map_err(|_| "auth_config 必须是 JSON 对象或 JSON 字符串".to_string())?;
            normalize_json_object(Some(parsed), "auth_config")
        }
        other => normalize_json_object(Some(other), "auth_config"),
    }
}

fn encrypt_imported_provider_config(
    state: &AdminAppState<'_>,
    config: Option<Value>,
) -> Result<Option<Value>, String> {
    let Some(mut config) = normalize_json_object(config, "config")? else {
        return Ok(None);
    };
    let Some(credentials) = config
        .get_mut("provider_ops")
        .and_then(Value::as_object_mut)
        .and_then(|provider_ops| provider_ops.get_mut("connector"))
        .and_then(Value::as_object_mut)
        .and_then(|connector| connector.get_mut("credentials"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(Some(config));
    };

    for field in ADMIN_SYSTEM_PROVIDER_OPS_SENSITIVE_CREDENTIAL_FIELDS {
        let Some(Value::String(raw)) = credentials.get_mut(*field) else {
            continue;
        };
        if raw.is_empty() {
            continue;
        }
        let encrypted = state
            .encrypt_catalog_secret_with_fallbacks(raw)
            .ok_or_else(|| "gateway 未配置 Provider Ops 加密密钥".to_string())?;
        *raw = encrypted;
    }

    Ok(Some(config))
}

fn remap_import_proxy(
    proxy: Option<Value>,
    node_id_map: &BTreeMap<String, String>,
) -> Option<Value> {
    let proxy = match proxy {
        Some(Value::Object(map)) if map.is_empty() => return None,
        Some(Value::Object(map)) => map,
        _ => return None,
    };
    let Some(Value::String(old_node_id)) = proxy.get("node_id") else {
        return Some(Value::Object(proxy));
    };
    let old_node_id = old_node_id.trim();
    if old_node_id.is_empty() {
        return Some(Value::Object(proxy));
    }
    let new_node_id = node_id_map.get(old_node_id)?;
    let mut remapped = proxy;
    remapped.insert("node_id".to_string(), json!(new_node_id));
    Some(Value::Object(remapped))
}

fn normalize_import_endpoint_format(value: &str) -> Result<String, String> {
    admin_endpoint_signature_parts(value)
        .map(|(signature, _, _)| signature.to_string())
        .ok_or_else(|| format!("无效的 api_format: {value}"))
}

fn normalize_import_key_formats(
    item: &ImportedProviderKey,
    provider_endpoint_formats: &BTreeSet<String>,
) -> (Vec<String>, Vec<String>) {
    let source = item
        .api_formats
        .clone()
        .filter(|items| !items.is_empty())
        .or_else(|| {
            item.supported_endpoints
                .clone()
                .filter(|items| !items.is_empty())
        })
        .unwrap_or_else(|| provider_endpoint_formats.iter().cloned().collect());

    let mut normalized = Vec::new();
    let mut missing = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in source {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(api_format) = normalize_import_endpoint_format(trimmed) else {
            missing.push(trimmed.to_string());
            continue;
        };
        if !seen.insert(api_format.clone()) {
            continue;
        }
        if !provider_endpoint_formats.is_empty() && !provider_endpoint_formats.contains(&api_format)
        {
            missing.push(api_format);
            continue;
        }
        normalized.push(api_format);
    }

    (normalized, missing)
}

fn imported_key_auth_type(item: &ImportedProviderKey) -> String {
    item.auth_type
        .as_deref()
        .unwrap_or("api_key")
        .trim()
        .to_ascii_lowercase()
}

fn imported_service_account_email(config: Option<&Value>) -> Option<String> {
    match config {
        Some(Value::Object(map)) => map
            .get("client_email")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        Some(Value::String(raw)) => serde_json::from_str::<Value>(raw)
            .ok()
            .and_then(|value| imported_service_account_email(Some(&value))),
        _ => None,
    }
}

fn build_import_key_match_name(item: &ImportedProviderKey) -> Option<String> {
    item.name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_import_key_raw_payload(
    raw_key: &Map<String, Value>,
    auth_type: &str,
    normalized_api_formats: &[String],
    normalized_auth_config: Option<Value>,
) -> Map<String, Value> {
    let mut payload = raw_key.clone();
    if auth_type == "oauth" {
        payload.remove("api_key");
    }
    payload.insert("api_formats".to_string(), json!(normalized_api_formats));
    if let Some(auth_config) = normalized_auth_config {
        payload.insert("auth_config".to_string(), auth_config);
    } else if raw_key.contains_key("auth_config") {
        payload.insert("auth_config".to_string(), Value::Null);
    }
    payload
}

fn apply_imported_oauth_key_credentials(
    state: &AdminAppState<'_>,
    raw_key: &Map<String, Value>,
    normalized_auth_config: Option<&Value>,
    record: &mut aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey,
) -> Result<(), String> {
    if let Some(api_key_value) = raw_key.get("api_key") {
        let plaintext = match api_key_value {
            Value::String(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    "__placeholder__"
                } else {
                    trimmed
                }
            }
            _ => "__placeholder__",
        };
        record.encrypted_api_key = state
            .encrypt_catalog_secret_with_fallbacks(plaintext)
            .ok_or_else(|| "gateway 未配置 provider key 加密密钥".to_string())?;
    }

    if raw_key.contains_key("auth_config") {
        record.encrypted_auth_config = match normalized_auth_config {
            Some(auth_config) => {
                let plaintext =
                    serde_json::to_string(auth_config).map_err(|err| err.to_string())?;
                Some(
                    state
                        .encrypt_catalog_secret_with_fallbacks(&plaintext)
                        .ok_or_else(|| "gateway 未配置 provider key 加密密钥".to_string())?,
                )
            }
            None => None,
        };
    }

    Ok(())
}

fn build_import_provider_model_record(
    provider_id: &str,
    existing_id: Option<&str>,
    global_model_id: &str,
    item: &ImportedProviderModel,
) -> Result<UpsertAdminProviderModelRecord, String> {
    let provider_model_name = trim_required(&item.provider_model_name, "provider_model_name")?;
    let provider_model_mappings = normalize_json_array(
        item.provider_model_mappings.clone(),
        "provider_model_mappings",
    )?;
    let price_per_request = normalize_optional_price(item.price_per_request, "price_per_request")?;
    let tiered_pricing = normalize_json_object(item.tiered_pricing.clone(), "tiered_pricing")?;
    let config = normalize_json_object(item.config.clone(), "config")?;

    UpsertAdminProviderModelRecord::new(
        existing_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        provider_id.to_string(),
        global_model_id.to_string(),
        provider_model_name,
        provider_model_mappings,
        price_per_request,
        tiered_pricing,
        item.supports_vision,
        item.supports_function_calling,
        item.supports_streaming,
        item.supports_extended_thinking,
        item.supports_image_generation,
        item.is_active,
        true,
        config,
    )
    .map_err(|err| err.to_string())
}

impl<'a> AdminAppState<'a> {
    pub(crate) async fn import_admin_system_config(
        &self,
        request_body: &Bytes,
    ) -> Result<Result<Value, (http::StatusCode, Value)>, GatewayError> {
        macro_rules! invalid {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(detail) => return Ok(Err(invalid_request(detail))),
                }
            };
        }
        macro_rules! routed {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(err) => return Ok(Err(err)),
                }
            };
        }

        if !self.has_global_model_data_reader()
            || !self.has_global_model_data_writer()
            || !self.has_provider_catalog_data_reader()
            || !self.has_provider_catalog_data_writer()
        {
            return Ok(Err((
                http::StatusCode::SERVICE_UNAVAILABLE,
                json!({ "detail": "Admin system data unavailable" }),
            )));
        }
        if request_body.len() > ADMIN_SYSTEM_IMPORT_MAX_SIZE_BYTES {
            return Ok(Err(invalid_request("请求体大小不能超过 10MB")));
        }

        let parsed = routed!(parse_admin_system_config_import_request(request_body));
        let root = parsed.root;
        let merge_mode = parsed.request.merge_mode;

        let imported_global_models = routed!(
            parse_admin_system_config_array::<ImportedGlobalModel>(&root, "global_models")
        );
        let imported_providers = routed!(parse_admin_system_config_array::<ImportedProvider>(
            &root,
            "providers"
        ));
        let imported_proxy_nodes = routed!(parse_admin_system_config_array::<ImportedProxyNode>(
            &root,
            "proxy_nodes"
        ));
        let imported_ldap = routed!(parse_admin_system_config_optional_object::<
            ImportedLdapConfig,
        >(&root, "ldap_config"));
        let imported_oauth_providers = routed!(parse_admin_system_config_array::<
            ImportedOAuthProvider,
        >(&root, "oauth_providers",));
        let imported_system_configs = routed!(parse_admin_system_config_array::<
            ImportedSystemConfig,
        >(&root, "system_configs",));

        let mut stats = AdminSystemConfigImportStats::default();

        let mut global_models_by_name = self
            .list_admin_global_models(&AdminGlobalModelListQuery {
                offset: 0,
                limit: 10_000,
                is_active: None,
                search: None,
            })
            .await?
            .items
            .into_iter()
            .map(|model| (model.name.clone(), model))
            .collect::<BTreeMap<_, _>>();

        if !imported_proxy_nodes.is_empty() {
            let empty_proxy_node_ids = imported_proxy_nodes
                .iter()
                .filter(|node| {
                    node.value
                        .id
                        .as_deref()
                        .map(str::trim)
                        .is_none_or(|value| value.is_empty())
                })
                .count();
            stats.proxy_nodes.skipped = imported_proxy_nodes.len() as u64;
            if empty_proxy_node_ids > 0 {
                stats.errors.push(format!(
                    "检测到 {empty_proxy_node_ids} 个无效 proxy_nodes 项；当前 Rust 管理后端暂不支持导入代理节点"
                ));
            } else {
                stats.errors.push(
                    "当前 Rust 管理后端暂不支持导入代理节点；仅引用这些节点(node_id)的自动连接代理配置会被清除，手动 URL 代理配置会保留"
                        .to_string(),
                );
            }
        }
        let node_id_map = BTreeMap::<String, String>::new();

        for imported_model in imported_global_models {
            let (_, model) = imported_model.into_parts();
            let name = invalid!(trim_required(&model.name, "name"));
            let display_name = invalid!(trim_required(&model.display_name, "display_name"));
            let default_price_per_request = invalid!(normalize_optional_price(
                model.default_price_per_request,
                "default_price_per_request",
            ));
            let default_tiered_pricing = invalid!(normalize_json_object(
                model.default_tiered_pricing,
                "default_tiered_pricing",
            ));
            let supported_capabilities =
                normalize_supported_capabilities(model.supported_capabilities);
            let config = invalid!(normalize_json_object(model.config, "config"));

            if let Some(existing) = global_models_by_name.get(&name).cloned() {
                match merge_mode {
                    AdminImportMergeMode::Skip => {
                        stats.global_models.skipped += 1;
                    }
                    AdminImportMergeMode::Error => {
                        return Ok(Err(invalid_request(format!("GlobalModel '{name}' 已存在"))));
                    }
                    AdminImportMergeMode::Overwrite => {
                        let record = invalid!(UpdateAdminGlobalModelRecord::new(
                            existing.id.clone(),
                            display_name,
                            model.is_active,
                            default_price_per_request,
                            default_tiered_pricing,
                            supported_capabilities,
                            config,
                        )
                        .map_err(|err| err.to_string()));
                        let Some(updated) = self.update_admin_global_model(&record).await? else {
                            return Ok(Err(invalid_request(format!(
                                "更新 GlobalModel '{name}' 失败"
                            ))));
                        };
                        global_models_by_name.insert(name, updated);
                        stats.global_models.updated += 1;
                    }
                }
                continue;
            }

            let record = invalid!(CreateAdminGlobalModelRecord::new(
                Uuid::new_v4().to_string(),
                name.clone(),
                display_name,
                model.is_active,
                default_price_per_request,
                default_tiered_pricing,
                supported_capabilities,
                config,
            )
            .map_err(|err| err.to_string()));
            let Some(created) = self.create_admin_global_model(&record).await? else {
                return Ok(Err(invalid_request(format!(
                    "创建 GlobalModel '{name}' 失败"
                ))));
            };
            global_models_by_name.insert(name, created);
            stats.global_models.created += 1;
        }

        let mut providers_by_name = self
            .list_provider_catalog_providers(false)
            .await?
            .into_iter()
            .map(|provider| (provider.name.clone(), provider))
            .collect::<BTreeMap<_, _>>();

        for imported_provider_item in imported_providers {
            let (raw_provider, imported_provider) = imported_provider_item.into_parts();
            let provider_name = invalid!(trim_required(&imported_provider.name, "name"));
            let existing_provider = providers_by_name.get(&provider_name).cloned();

            let provider = if let Some(existing) = existing_provider {
                match merge_mode {
                    AdminImportMergeMode::Skip => {
                        stats.providers.skipped += 1;
                        existing
                    }
                    AdminImportMergeMode::Error => {
                        return Ok(Err(invalid_request(format!(
                            "Provider '{provider_name}' 已存在"
                        ))));
                    }
                    AdminImportMergeMode::Overwrite => {
                        let patch =
                            match AdminProviderUpdatePatch::from_object(raw_provider.clone()) {
                                Ok(patch) => patch,
                                Err(_) => {
                                    return Ok(Err(invalid_request(format!(
                                        "Provider '{provider_name}' 配置格式无效"
                                    ))))
                                }
                            };
                        let mut updated = invalid!(
                            self.build_admin_update_provider_record(&existing, patch)
                                .await
                        );
                        updated.proxy =
                            remap_import_proxy(imported_provider.proxy.clone(), &node_id_map);
                        updated.config = invalid!(encrypt_imported_provider_config(
                            self,
                            imported_provider.config.clone(),
                        ));
                        let Some(persisted) =
                            self.update_provider_catalog_provider(&updated).await?
                        else {
                            return Ok(Err(invalid_request(format!(
                                "更新 Provider '{provider_name}' 失败"
                            ))));
                        };
                        providers_by_name.insert(provider_name.clone(), persisted.clone());
                        stats.providers.updated += 1;
                        persisted
                    }
                }
            } else {
                let payload = match serde_json::from_value::<AdminProviderCreateRequest>(
                    Value::Object(raw_provider.clone()),
                ) {
                    Ok(payload) => payload,
                    Err(_) => {
                        return Ok(Err(invalid_request(format!(
                            "Provider '{provider_name}' 配置格式无效"
                        ))))
                    }
                };
                let (mut record, shift_existing_priorities_from) =
                    invalid!(self.build_admin_create_provider_record(payload).await);
                if let Some(enable_format_conversion) = imported_provider.enable_format_conversion {
                    record.enable_format_conversion = enable_format_conversion;
                }
                record.proxy = remap_import_proxy(imported_provider.proxy.clone(), &node_id_map);
                record.config = invalid!(encrypt_imported_provider_config(
                    self,
                    imported_provider.config.clone(),
                ));
                let Some(created) = self
                    .create_provider_catalog_provider(&record, shift_existing_priorities_from)
                    .await?
                else {
                    return Ok(Err(invalid_request(format!(
                        "创建 Provider '{provider_name}' 失败"
                    ))));
                };
                providers_by_name.insert(provider_name.clone(), created.clone());
                stats.providers.created += 1;
                created
            };

            let imported_endpoints = routed!(parse_admin_system_config_nested_array::<
                ImportedEndpoint,
            >(&raw_provider, "endpoints"));
            let mut existing_endpoints_by_format = self
                .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider.id))
                .await?
                .into_iter()
                .map(|endpoint| (endpoint.api_format.clone(), endpoint))
                .collect::<BTreeMap<_, _>>();

            for imported_endpoint_item in imported_endpoints {
                let (raw_endpoint, imported_endpoint) = imported_endpoint_item.into_parts();
                let normalized_api_format = invalid!(normalize_import_endpoint_format(
                    &imported_endpoint.api_format
                ));
                let existing_endpoint = existing_endpoints_by_format
                    .get(&normalized_api_format)
                    .cloned();

                if let Some(existing_endpoint) = existing_endpoint {
                    match merge_mode {
                        AdminImportMergeMode::Skip => {
                            stats.endpoints.skipped += 1;
                        }
                        AdminImportMergeMode::Error => {
                            return Ok(Err(invalid_request(format!(
                                "Endpoint '{normalized_api_format}' 已存在于 Provider '{provider_name}'"
                            ))));
                        }
                        AdminImportMergeMode::Overwrite => {
                            let Some((normalized_signature, api_family, endpoint_kind)) =
                                admin_endpoint_signature_parts(&imported_endpoint.api_format)
                            else {
                                return Ok(Err(invalid_request(format!(
                                    "无效的 api_format: {}",
                                    imported_endpoint.api_format
                                ))));
                            };
                            let patch = match AdminProviderEndpointUpdatePatch::from_object(
                                raw_endpoint.clone(),
                            ) {
                                Ok(patch) => patch,
                                Err(_) => {
                                    return Ok(Err(invalid_request(
                                        "Provider Endpoint 配置格式无效",
                                    )))
                                }
                            };
                            let (fields, payload) = patch.into_parts();
                            let normalized_base_url = match payload.base_url.as_deref() {
                                Some(base_url) => {
                                    Some(invalid!(normalize_admin_base_url(base_url)))
                                }
                                None => None,
                            };
                            let update_fields =
                                admin_provider_endpoints_pure::AdminProviderEndpointUpdateFields {
                                    base_url: normalized_base_url,
                                    custom_path: payload.custom_path,
                                    header_rules: payload.header_rules,
                                    body_rules: payload.body_rules,
                                    max_retries: payload.max_retries,
                                    is_active: payload.is_active,
                                    config: payload.config,
                                    proxy: payload.proxy,
                                    format_acceptance_config: payload.format_acceptance_config,
                                };
                            let mut updated = invalid!(
                                admin_provider_endpoints_pure::apply_admin_provider_endpoint_update_fields(
                                    &existing_endpoint,
                                    |field| fields.contains(field),
                                    |field| fields.is_null(field),
                                    &update_fields,
                                )
                            );
                            if fields.contains("proxy") {
                                updated.proxy = remap_import_proxy(
                                    imported_endpoint.proxy.clone(),
                                    &node_id_map,
                                );
                            }
                            updated.api_format = normalized_signature.to_string();
                            updated.api_family = Some(api_family.to_string());
                            updated.endpoint_kind = Some(endpoint_kind.to_string());
                            updated.updated_at_unix_secs = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .ok()
                                .map(|duration| duration.as_secs());
                            let Some(persisted) =
                                self.update_provider_catalog_endpoint(&updated).await?
                            else {
                                return Ok(Err(invalid_request(format!(
                                    "更新 Endpoint '{normalized_api_format}' 失败"
                                ))));
                            };
                            existing_endpoints_by_format
                                .insert(normalized_api_format.clone(), persisted);
                            stats.endpoints.updated += 1;
                        }
                    }
                    continue;
                }

                let Some((normalized_signature, api_family, endpoint_kind)) =
                    admin_endpoint_signature_parts(&imported_endpoint.api_format)
                else {
                    return Ok(Err(invalid_request(format!(
                        "无效的 api_format: {}",
                        imported_endpoint.api_format
                    ))));
                };
                let now_unix_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_secs())
                    .unwrap_or(0);
                let mut record = invalid!(
                    admin_provider_endpoints_pure::build_admin_provider_endpoint_record(
                        Uuid::new_v4().to_string(),
                        provider.id.clone(),
                        normalized_signature.to_string(),
                        api_family.to_string(),
                        endpoint_kind.to_string(),
                        invalid!(normalize_admin_base_url(&imported_endpoint.base_url)),
                        imported_endpoint.custom_path.clone(),
                        imported_endpoint.header_rules.clone(),
                        imported_endpoint.body_rules.clone(),
                        imported_endpoint.max_retries.unwrap_or(2),
                        imported_endpoint.config.clone(),
                        remap_import_proxy(imported_endpoint.proxy.clone(), &node_id_map),
                        imported_endpoint.format_acceptance_config.clone(),
                        now_unix_secs,
                    )
                );
                record = record.with_health_score(1.0);
                record.is_active = imported_endpoint.is_active;
                let Some(created) = self.create_provider_catalog_endpoint(&record).await? else {
                    return Ok(Err(invalid_request(format!(
                        "创建 Endpoint '{normalized_api_format}' 失败"
                    ))));
                };
                existing_endpoints_by_format.insert(normalized_api_format, created);
                stats.endpoints.created += 1;
            }

            let provider_endpoint_formats = existing_endpoints_by_format
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();

            let imported_keys = routed!(parse_admin_system_config_nested_array::<
                ImportedProviderKey,
            >(&raw_provider, "api_keys"));
            let mut existing_keys = self
                .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
                .await?;

            for imported_key_item in imported_keys {
                let (raw_key, imported_key) = imported_key_item.into_parts();
                let (normalized_api_formats, missing_formats) =
                    normalize_import_key_formats(&imported_key, &provider_endpoint_formats);
                if !missing_formats.is_empty() {
                    stats.errors.push(format!(
                        "Key (Provider: {provider_name}) 的 api_formats 未配置对应 Endpoint，已跳过: {:?}",
                        missing_formats
                    ));
                }
                if normalized_api_formats.is_empty() {
                    stats.keys.skipped += 1;
                    continue;
                }

                let normalized_auth_config = invalid!(normalize_import_auth_config(
                    imported_key.auth_config.clone()
                ));
                let auth_type = imported_key_auth_type(&imported_key);
                let normalized_raw_key = normalize_import_key_raw_payload(
                    &raw_key,
                    &auth_type,
                    &normalized_api_formats,
                    normalized_auth_config.clone(),
                );
                let existing_key_index = if auth_type == "api_key" {
                    let target_key = imported_key
                        .api_key
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned);
                    existing_keys.iter().position(|existing_key| {
                        target_key
                            .as_deref()
                            .zip(
                                self.decrypt_catalog_secret_with_fallbacks(
                                    &existing_key.encrypted_api_key,
                                )
                                .as_deref(),
                            )
                            .is_some_and(|(target, decrypted)| decrypted == target)
                    })
                } else if matches!(auth_type.as_str(), "service_account" | "vertex_ai") {
                    let target_email =
                        imported_service_account_email(normalized_auth_config.as_ref());
                    existing_keys.iter().position(|existing_key| {
                        target_email.as_deref().is_some_and(|target_email| {
                            self.parse_catalog_auth_config_json(existing_key)
                                .and_then(|config| {
                                    config
                                        .get("client_email")
                                        .and_then(Value::as_str)
                                        .map(str::trim)
                                        .filter(|value| !value.is_empty())
                                        .map(ToOwned::to_owned)
                                })
                                .as_deref()
                                == Some(target_email)
                        })
                    })
                } else {
                    build_import_key_match_name(&imported_key).and_then(|target_name| {
                        existing_keys.iter().position(|existing_key| {
                            existing_key
                                .auth_type
                                .trim()
                                .eq_ignore_ascii_case(&auth_type)
                                && existing_key.name == target_name
                        })
                    })
                };

                if let Some(existing_index) = existing_key_index {
                    let existing_key = existing_keys[existing_index].clone();
                    match merge_mode {
                        AdminImportMergeMode::Skip => {
                            stats.keys.skipped += 1;
                        }
                        AdminImportMergeMode::Error => {
                            return Ok(Err(invalid_request(format!(
                                "Provider '{provider_name}' 中存在重复 Key"
                            ))));
                        }
                        AdminImportMergeMode::Overwrite => {
                            let patch = match AdminProviderKeyUpdatePatch::from_object(
                                normalized_raw_key.clone(),
                            ) {
                                Ok(patch) => patch,
                                Err(_) => {
                                    return Ok(Err(invalid_request("Provider Key 配置格式无效")))
                                }
                            };
                            let mut updated = invalid!(
                                self.build_admin_update_provider_key_record(
                                    &provider,
                                    &existing_key,
                                    patch,
                                )
                                .await
                            );
                            if auth_type == "oauth" {
                                invalid!(apply_imported_oauth_key_credentials(
                                    self,
                                    &raw_key,
                                    normalized_auth_config.as_ref(),
                                    &mut updated,
                                ));
                            }
                            updated.proxy =
                                remap_import_proxy(imported_key.proxy.clone(), &node_id_map);
                            updated.fingerprint = invalid!(normalize_json_object(
                                imported_key.fingerprint.clone(),
                                "fingerprint",
                            ));
                            let Some(persisted) =
                                self.update_provider_catalog_key(&updated).await?
                            else {
                                return Ok(Err(invalid_request(format!(
                                    "更新 Provider '{provider_name}' 的 Key 失败"
                                ))));
                            };
                            existing_keys[existing_index] = persisted;
                            stats.keys.updated += 1;
                        }
                    }
                    continue;
                }

                let payload = match serde_json::from_value::<AdminProviderKeyCreateRequest>(
                    Value::Object(normalized_raw_key.clone()),
                ) {
                    Ok(payload) => payload,
                    Err(_) => return Ok(Err(invalid_request("Provider Key 配置格式无效"))),
                };
                let mut record = invalid!(
                    self.build_admin_create_provider_key_record(&provider, payload)
                        .await
                );
                if auth_type == "oauth" {
                    invalid!(apply_imported_oauth_key_credentials(
                        self,
                        &raw_key,
                        normalized_auth_config.as_ref(),
                        &mut record,
                    ));
                }
                record.is_active = imported_key.is_active;
                record.global_priority_by_format = invalid!(normalize_json_object(
                    imported_key.global_priority_by_format.clone(),
                    "global_priority_by_format",
                ));
                record.proxy = remap_import_proxy(imported_key.proxy.clone(), &node_id_map);
                record.fingerprint = invalid!(normalize_json_object(
                    imported_key.fingerprint.clone(),
                    "fingerprint",
                ));
                let Some(created) = self.create_provider_catalog_key(&record).await? else {
                    return Ok(Err(invalid_request(format!(
                        "创建 Provider '{provider_name}' 的 Key 失败"
                    ))));
                };
                existing_keys.push(created);
                stats.keys.created += 1;
            }

            let imported_models = routed!(parse_admin_system_config_nested_array::<
                ImportedProviderModel,
            >(&raw_provider, "models"));
            let mut existing_models_by_name = self
                .list_admin_provider_models(&AdminProviderModelListQuery {
                    provider_id: provider.id.clone(),
                    is_active: None,
                    offset: 0,
                    limit: 10_000,
                })
                .await?
                .into_iter()
                .map(|model| (model.provider_model_name.clone(), model))
                .collect::<BTreeMap<_, _>>();

            for imported_model_item in imported_models {
                let (_, imported_model) = imported_model_item.into_parts();
                let Some(global_model_name) = imported_model
                    .global_model_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    stats.errors.push(format!(
                        "跳过无 global_model_name 的模型 (Provider: {provider_name})"
                    ));
                    continue;
                };
                let Some(global_model_id) = global_models_by_name
                    .get(global_model_name)
                    .map(|model| model.id.clone())
                else {
                    stats.errors.push(format!(
                        "GlobalModel '{global_model_name}' 不存在，跳过模型"
                    ));
                    continue;
                };

                let provider_model_name = invalid!(trim_required(
                    &imported_model.provider_model_name,
                    "provider_model_name"
                ));
                let existing_model = existing_models_by_name.get(&provider_model_name).cloned();

                if let Some(existing_model) = existing_model {
                    match merge_mode {
                        AdminImportMergeMode::Skip => {
                            stats.models.skipped += 1;
                        }
                        AdminImportMergeMode::Error => {
                            return Ok(Err(invalid_request(format!(
                                "Model '{provider_model_name}' 已存在于 Provider '{provider_name}'"
                            ))));
                        }
                        AdminImportMergeMode::Overwrite => {
                            let record = invalid!(build_import_provider_model_record(
                                &provider.id,
                                Some(&existing_model.id),
                                &global_model_id,
                                &imported_model,
                            ));
                            let Some(updated) = self.update_admin_provider_model(&record).await?
                            else {
                                return Ok(Err(invalid_request(format!(
                                    "更新 Provider '{provider_name}' 的模型 '{provider_model_name}' 失败"
                                ))));
                            };
                            existing_models_by_name.insert(provider_model_name, updated);
                            stats.models.updated += 1;
                        }
                    }
                    continue;
                }

                let record = invalid!(build_import_provider_model_record(
                    &provider.id,
                    None,
                    &global_model_id,
                    &imported_model,
                ));
                let Some(created) = self.create_admin_provider_model(&record).await? else {
                    return Ok(Err(invalid_request(format!(
                        "创建 Provider '{provider_name}' 的模型 '{provider_model_name}' 失败"
                    ))));
                };
                existing_models_by_name.insert(provider_model_name, created);
                stats.models.created += 1;
            }
        }

        if let Some(imported_ldap_item) = imported_ldap {
            let (_, ldap_config) = imported_ldap_item.into_parts();
            if !self.has_auth_module_writer() {
                stats.ldap.skipped += 1;
                stats
                    .errors
                    .push("当前运行环境不支持写入 LDAP 配置，已跳过 ldap_config".to_string());
            } else {
                let existing = self.get_ldap_module_config().await?;
                let server_url =
                    invalid!(trim_required(&ldap_config.server_url, "LDAP 服务器地址"));
                let bind_dn = invalid!(trim_required(&ldap_config.bind_dn, "绑定 DN"));
                let base_dn = invalid!(trim_required(&ldap_config.base_dn, "Base DN"));
                let user_search_filter = invalid!(trim_required(
                    ldap_config
                        .user_search_filter
                        .as_deref()
                        .unwrap_or("(uid={username})"),
                    "搜索过滤器",
                ));
                let username_attr = invalid!(trim_required(
                    ldap_config.username_attr.as_deref().unwrap_or("uid"),
                    "用户名属性",
                ));
                let email_attr = invalid!(trim_required(
                    ldap_config.email_attr.as_deref().unwrap_or("mail"),
                    "邮箱属性",
                ));
                let display_name_attr = invalid!(trim_required(
                    ldap_config.display_name_attr.as_deref().unwrap_or("cn"),
                    "显示名称属性",
                ));
                let connect_timeout = ldap_config.connect_timeout.unwrap_or(10);
                if !(1..=60).contains(&connect_timeout) {
                    return Ok(Err(invalid_request(
                        "LDAP connect_timeout 必须在 1 到 60 秒之间",
                    )));
                }
                let bind_password = ldap_config
                    .bind_password
                    .as_deref()
                    .map(str::trim)
                    .map(ToOwned::to_owned);
                let will_have_password = bind_password
                    .as_deref()
                    .map(|value| !value.is_empty())
                    .unwrap_or_else(|| {
                        existing
                            .as_ref()
                            .and_then(|config| config.bind_password_encrypted.as_deref())
                            .map(str::trim)
                            .is_some_and(|value| !value.is_empty())
                    });
                if existing.is_none() && !will_have_password {
                    return Ok(Err(invalid_request("首次配置 LDAP 时必须设置绑定密码")));
                }
                if ldap_config.is_exclusive && !ldap_config.is_enabled {
                    return Ok(Err(invalid_request(
                        "仅允许 LDAP 登录 需要先启用 LDAP 认证",
                    )));
                }
                if ldap_config.is_enabled && !will_have_password {
                    return Ok(Err(invalid_request("启用 LDAP 认证 需要先设置绑定密码")));
                }
                if ldap_config.is_enabled && ldap_config.is_exclusive {
                    let admin_count = self
                        .count_active_local_admin_users_with_valid_password()
                        .await?;
                    if admin_count < 1 {
                        return Ok(Err(invalid_request(
                            "启用 LDAP 独占模式前，必须至少保留 1 个有效的本地管理员账户（含有效密码）作为紧急恢复通道",
                        )));
                    }
                }
                let bind_password_encrypted = match bind_password {
                    Some(password) if password.is_empty() => None,
                    Some(password) => Some(routed!(self
                        .encrypt_catalog_secret_with_fallbacks(&password)
                        .ok_or_else(|| {
                            invalid_request("LDAP 绑定密码加密失败，请检查 Rust 数据加密配置")
                        }))),
                    None => existing
                        .as_ref()
                        .and_then(|config| config.bind_password_encrypted.clone()),
                };
                let config = StoredLdapModuleConfig {
                    server_url,
                    bind_dn,
                    bind_password_encrypted,
                    base_dn,
                    user_search_filter: Some(user_search_filter),
                    username_attr: Some(username_attr),
                    email_attr: Some(email_attr),
                    display_name_attr: Some(display_name_attr),
                    is_enabled: ldap_config.is_enabled,
                    is_exclusive: ldap_config.is_exclusive,
                    use_starttls: ldap_config.use_starttls,
                    connect_timeout: Some(connect_timeout),
                };

                match (existing.is_some(), merge_mode) {
                    (true, AdminImportMergeMode::Skip) => stats.ldap.skipped += 1,
                    (true, AdminImportMergeMode::Error) => {
                        return Ok(Err(invalid_request("LDAP 配置已存在")));
                    }
                    (true, AdminImportMergeMode::Overwrite) => {
                        let Some(_) = self.upsert_ldap_module_config(&config).await? else {
                            return Ok(Err(invalid_request("更新 LDAP 配置失败")));
                        };
                        stats.ldap.updated += 1;
                    }
                    (false, _) => {
                        let Some(_) = self.upsert_ldap_module_config(&config).await? else {
                            return Ok(Err(invalid_request("创建 LDAP 配置失败")));
                        };
                        stats.ldap.created += 1;
                    }
                }
            }
        }

        if !imported_oauth_providers.is_empty() {
            let imported_oauth_provider_count = imported_oauth_providers.len();
            let mut oauth_by_type = self
                .list_oauth_provider_configs()
                .await?
                .into_iter()
                .map(|provider| (provider.provider_type.clone(), provider))
                .collect::<BTreeMap<_, _>>();

            for (index, imported_oauth_item) in imported_oauth_providers.into_iter().enumerate() {
                let (_, oauth_provider) = imported_oauth_item.into_parts();
                let provider_type = invalid!(trim_required(
                    &oauth_provider.provider_type,
                    "provider_type",
                ));
                let existed = oauth_by_type.contains_key(&provider_type);
                if existed {
                    match merge_mode {
                        AdminImportMergeMode::Skip => {
                            stats.oauth.skipped += 1;
                            continue;
                        }
                        AdminImportMergeMode::Error => {
                            return Ok(Err(invalid_request(format!(
                                "OAuth Provider '{provider_type}' 已存在"
                            ))));
                        }
                        AdminImportMergeMode::Overwrite => {}
                    }
                }

                let display_name =
                    invalid!(trim_required(&oauth_provider.display_name, "display_name"));
                let client_id = invalid!(trim_required(&oauth_provider.client_id, "client_id"));
                let redirect_uri =
                    invalid!(trim_required(&oauth_provider.redirect_uri, "redirect_uri"));
                let frontend_callback_url = invalid!(trim_required(
                    &oauth_provider.frontend_callback_url,
                    "frontend_callback_url",
                ));
                let client_secret_encrypted =
                    match oauth_provider.client_secret.as_deref().map(str::trim) {
                        Some(secret) if !secret.is_empty() => {
                            EncryptedSecretUpdate::Set(routed!(self
                                .encrypt_catalog_secret_with_fallbacks(secret)
                                .ok_or_else(|| {
                                    invalid_request("gateway 未配置 OAuth provider 加密密钥")
                                })))
                        }
                        _ => EncryptedSecretUpdate::Preserve,
                    };
                let record = UpsertOAuthProviderConfigRecord {
                    provider_type: provider_type.clone(),
                    display_name,
                    client_id,
                    client_secret_encrypted,
                    authorization_url_override: oauth_provider
                        .authorization_url_override
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                    token_url_override: oauth_provider
                        .token_url_override
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                    userinfo_url_override: oauth_provider
                        .userinfo_url_override
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty()),
                    scopes: normalize_string_list(oauth_provider.scopes),
                    redirect_uri,
                    frontend_callback_url,
                    attribute_mapping: invalid!(normalize_json_object(
                        oauth_provider.attribute_mapping,
                        "attribute_mapping",
                    )),
                    extra_config: invalid!(normalize_json_object(
                        oauth_provider.extra_config,
                        "extra_config",
                    )),
                    is_enabled: oauth_provider.is_enabled,
                };
                invalid!(record.validate().map_err(|err| err.to_string()));

                let Some(persisted) = self.upsert_oauth_provider_config(&record).await? else {
                    stats.oauth.skipped += (imported_oauth_provider_count - index) as u64;
                    stats.errors.push(
                        "当前运行环境不支持 OAuth Provider 配置读写，已跳过 oauth_providers"
                            .to_string(),
                    );
                    break;
                };
                oauth_by_type.insert(provider_type, persisted);
                if existed {
                    stats.oauth.updated += 1;
                } else {
                    stats.oauth.created += 1;
                }
            }
        }

        let mut existing_system_config_keys = self
            .list_system_config_entries()
            .await?
            .into_iter()
            .map(|entry| normalize_admin_system_config_key(&entry.key))
            .collect::<BTreeSet<_>>();
        for imported_config_item in imported_system_configs {
            let (_, system_config) = imported_config_item.into_parts();
            let normalized_key = normalize_admin_system_config_key(&system_config.key);
            let exists = existing_system_config_keys.contains(&normalized_key);
            match (exists, merge_mode) {
                (true, AdminImportMergeMode::Skip) => {
                    stats.system_configs.skipped += 1;
                    continue;
                }
                (true, AdminImportMergeMode::Error) => {
                    return Ok(Err(invalid_request(format!(
                        "SystemConfig '{normalized_key}' 已存在"
                    ))));
                }
                _ => {}
            }

            let request_bytes = Bytes::from(
                serde_json::to_vec(&json!({
                    "value": system_config.value,
                    "description": system_config.description,
                }))
                .map_err(|err| GatewayError::Internal(err.to_string()))?,
            );
            match apply_admin_system_config_update(self, &system_config.key, &request_bytes).await?
            {
                Ok(_) => {
                    if exists {
                        stats.system_configs.updated += 1;
                    } else {
                        stats.system_configs.created += 1;
                        existing_system_config_keys.insert(normalized_key);
                    }
                }
                Err((status, payload)) => return Ok(Err((status, payload))),
            }
        }

        Ok(Ok(json!({
            "message": "配置导入成功",
            "stats": stats,
        })))
    }
}
