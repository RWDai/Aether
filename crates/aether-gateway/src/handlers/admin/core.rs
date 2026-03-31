const ADMIN_AWS_REGIONS: &[&str] = &[
    "af-south-1",
    "ap-east-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ca-central-1",
    "ca-west-1",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "me-south-1",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
];
const ADMIN_MODEL_CATALOG_RUST_BACKEND_DETAIL: &str =
    "Admin model catalog routes require Rust maintenance backend";

fn build_admin_model_catalog_maintenance_response() -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": ADMIN_MODEL_CATALOG_RUST_BACKEND_DETAIL })),
    )
        .into_response()
}

async fn maybe_build_local_admin_core_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() == Some("management_tokens_manage")
        && decision
            .admin_principal
            .as_ref()
            .and_then(|principal| principal.management_token_id.as_deref())
            .is_some()
    {
        return Ok(Some(
            (
                http::StatusCode::FORBIDDEN,
                Json(json!({
                    "detail": "不允许使用 Management Token 管理其他 Token，请使用 Web 界面或 JWT 认证"
                })),
            )
                .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("supported_types")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/oauth/supported-types"
    {
        return Ok(Some(
            Json(build_admin_oauth_supported_types_payload()).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("list_providers")
        && request_context.request_method == http::Method::GET
        && matches!(
            request_context.request_path.as_str(),
            "/api/admin/oauth/providers" | "/api/admin/oauth/providers/"
        )
    {
        let providers = state.list_oauth_provider_configs().await?;
        return Ok(Some(
            Json(
                providers
                    .iter()
                    .map(build_admin_oauth_provider_payload)
                    .collect::<Vec<_>>(),
            )
            .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("get_provider")
        && request_context.request_method == http::Method::GET
    {
        let Some(provider_type) =
            admin_oauth_provider_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider 配置不存在" })),
                )
                    .into_response(),
            ));
        };
        return Ok(Some(
            match state.get_oauth_provider_config(&provider_type).await? {
                Some(provider) => {
                    Json(build_admin_oauth_provider_payload(&provider)).into_response()
                }
                None => (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider 配置不存在" })),
                )
                    .into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("upsert_provider")
        && request_context.request_method == http::Method::PUT
    {
        let Some(provider_type) =
            admin_oauth_provider_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "Provider 配置不存在",
                None,
            )));
        };
        let Some(request_body) = request_body else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "请求数据验证失败",
                None,
            )));
        };
        let payload = match serde_json::from_slice::<AdminOAuthProviderUpsertRequest>(request_body)
        {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "请求数据验证失败",
                    None,
                )));
            }
        };
        let existing = state.get_oauth_provider_config(&provider_type).await?;
        let ldap_exclusive = state.get_ldap_module_config().await?.is_some_and(|config| {
            config.is_enabled
                && config.is_exclusive
                && config
                    .bind_password_encrypted
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|value| !value.is_empty())
        });
        if existing
            .as_ref()
            .is_some_and(|provider| provider.is_enabled && !payload.is_enabled)
        {
            let affected_count = state
                .count_locked_users_if_oauth_provider_disabled(&provider_type, ldap_exclusive)
                .await?;
            if affected_count > 0 && !payload.force {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::CONFLICT,
                    "confirmation_required",
                    format!("禁用该 Provider 会导致 {affected_count} 个用户无法登录"),
                    Some(json!({
                        "affected_count": affected_count,
                        "action": "disable_oauth_provider",
                    })),
                )));
            }
        }
        let record = match build_admin_oauth_upsert_record(state, &provider_type, payload) {
            Ok(record) => record,
            Err(message) => {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid_request",
                    message,
                    None,
                )));
            }
        };
        let Some(provider) = state.upsert_oauth_provider_config(&record).await? else {
            return Ok(None);
        };
        return Ok(Some(
            Json(build_admin_oauth_provider_payload(&provider)).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("delete_provider")
        && request_context.request_method == http::Method::DELETE
    {
        let Some(provider_type) =
            admin_oauth_provider_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "Provider 配置不存在",
                None,
            )));
        };
        let Some(existing) = state.get_oauth_provider_config(&provider_type).await? else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "Provider 配置不存在",
                None,
            )));
        };
        if existing.is_enabled {
            let ldap_exclusive = state.get_ldap_module_config().await?.is_some_and(|config| {
                config.is_enabled
                    && config.is_exclusive
                    && config
                        .bind_password_encrypted
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|value| !value.is_empty())
            });
            let affected_count = state
                .count_locked_users_if_oauth_provider_disabled(&provider_type, ldap_exclusive)
                .await?;
            if affected_count > 0 {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid_request",
                    format!(
                        "删除该 Provider 会导致部分用户无法登录（数量: {affected_count}），已阻止操作"
                    ),
                    None,
                )));
            }
        }
        let deleted = state.delete_oauth_provider_config(&provider_type).await?;
        if !deleted {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "Provider 配置不存在",
                None,
            )));
        }
        return Ok(Some(Json(json!({ "message": "删除成功" })).into_response()));
    }

    if decision.route_family.as_deref() == Some("oauth_manage")
        && decision.route_kind.as_deref() == Some("test_provider")
        && request_context.request_method == http::Method::POST
    {
        let Some(provider_type) =
            admin_oauth_test_provider_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Provider 配置不存在" })),
                )
                    .into_response(),
            ));
        };
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response(),
            ));
        };
        let payload = match serde_json::from_slice::<serde_json::Value>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求数据验证失败" })),
                    )
                        .into_response(),
                ));
            }
        };
        let client_id = payload
            .get("client_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let redirect_uri = payload
            .get("redirect_uri")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if client_id.is_none() || redirect_uri.is_none() {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response(),
            ));
        }
        let provided_secret = payload
            .get("client_secret")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let persisted_secret = state
            .get_oauth_provider_config(&provider_type)
            .await?
            .and_then(|provider| provider.client_secret_encrypted);
        let supported_provider = provider_type.eq_ignore_ascii_case("linuxdo");
        let secret_status = if supported_provider {
            if provided_secret.is_some() || persisted_secret.is_some() {
                "unsupported"
            } else {
                "not_provided"
            }
        } else {
            "unknown"
        };
        let details = if supported_provider {
            "OAuth 配置测试仅支持 Rust executor"
        } else {
            "provider 未安装/不可用"
        };
        return Ok(Some(
            Json(json!({
                "authorization_url_reachable": false,
                "token_url_reachable": false,
                "secret_status": secret_status,
                "details": details,
            }))
            .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("management_tokens_manage")
        && decision.route_kind.as_deref() == Some("list_tokens")
        && request_context.request_method == http::Method::GET
        && is_admin_management_tokens_root(&request_context.request_path)
    {
        if !state.has_management_token_reader() {
            return Ok(None);
        }
        let user_id = query_param_value(request_context.request_query_string.as_deref(), "user_id");
        let is_active =
            query_param_optional_bool(request_context.request_query_string.as_deref(), "is_active");
        let skip = query_param_value(request_context.request_query_string.as_deref(), "skip")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let limit = query_param_value(request_context.request_query_string.as_deref(), "limit")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0 && *value <= 100)
            .unwrap_or(50);
        let page = state
            .list_management_tokens(&ManagementTokenListQuery {
                user_id,
                is_active,
                offset: skip,
                limit,
            })
            .await?;
        let items = page
            .items
            .iter()
            .map(|item| build_management_token_payload(&item.token, Some(&item.user)))
            .collect::<Vec<_>>();
        return Ok(Some(
            Json(json!({
                "items": items,
                "total": page.total,
                "skip": skip,
                "limit": limit,
            }))
            .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("management_tokens_manage")
        && decision.route_kind.as_deref() == Some("get_token")
        && request_context.request_method == http::Method::GET
    {
        if !state.has_management_token_reader() {
            return Ok(None);
        }
        let Some(token_id) = admin_management_token_id_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Management Token 不存在" })),
                )
                    .into_response(),
            ));
        };
        return Ok(Some(
            match state.get_management_token_with_user(&token_id).await? {
                Some(token) => Json(build_management_token_payload(
                    &token.token,
                    Some(&token.user),
                ))
                .into_response(),
                None => (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Management Token 不存在" })),
                )
                    .into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("management_tokens_manage")
        && decision.route_kind.as_deref() == Some("delete_token")
        && request_context.request_method == http::Method::DELETE
    {
        if !state.has_management_token_writer() {
            return Ok(None);
        }
        let Some(token_id) = admin_management_token_id_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Management Token 不存在" })),
                )
                    .into_response(),
            ));
        };
        let existing = match state.get_management_token_with_user(&token_id).await? {
            Some(token) => token,
            None => {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Management Token 不存在" })),
                    )
                        .into_response(),
                ));
            }
        };
        let deleted = state.delete_management_token(&existing.token.id).await?;
        return Ok(Some(if deleted {
            Json(json!({ "message": "删除成功" })).into_response()
        } else {
            (
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": "Management Token 不存在" })),
            )
                .into_response()
        }));
    }

    if decision.route_family.as_deref() == Some("management_tokens_manage")
        && decision.route_kind.as_deref() == Some("toggle_status")
        && request_context.request_method == http::Method::PATCH
    {
        if !state.has_management_token_writer() {
            return Ok(None);
        }
        let Some(token_id) =
            admin_management_token_status_id_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Management Token 不存在" })),
                )
                    .into_response(),
            ));
        };
        let existing = match state.get_management_token_with_user(&token_id).await? {
            Some(token) => token,
            None => {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Management Token 不存在" })),
                    )
                        .into_response(),
                ));
            }
        };
        let Some(updated) = state
            .set_management_token_active(&existing.token.id, !existing.token.is_active)
            .await?
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Management Token 不存在" })),
                )
                    .into_response(),
            ));
        };
        return Ok(Some(
            Json(json!({
                "message": format!("Token 已{}", if updated.is_active { "启用" } else { "禁用" }),
                "data": build_management_token_payload(&updated, Some(&existing.user)),
            }))
            .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("modules_manage")
        && decision.route_kind.as_deref() == Some("status_list")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/modules/status"
    {
        let payload = build_admin_modules_status_payload(state).await?;
        return Ok(Some(Json(payload).into_response()));
    }

    if decision.route_family.as_deref() == Some("modules_manage")
        && decision.route_kind.as_deref() == Some("status_detail")
        && request_context.request_method == http::Method::GET
        && request_context
            .request_path
            .starts_with("/api/admin/modules/status/")
    {
        let Some(module_name) = admin_module_name_from_status_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "模块不存在" })),
                )
                    .into_response(),
            ));
        };
        let Some(module) = admin_module_by_name(&module_name) else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": format!("模块 '{module_name}' 不存在") })),
                )
                    .into_response(),
            ));
        };
        let runtime = build_admin_module_runtime_state(state).await?;
        let payload = build_admin_module_status_payload(state, module, &runtime).await?;
        return Ok(Some(Json(payload).into_response()));
    }

    if decision.route_family.as_deref() == Some("modules_manage")
        && decision.route_kind.as_deref() == Some("set_enabled")
        && request_context.request_method == http::Method::PUT
        && request_context
            .request_path
            .starts_with("/api/admin/modules/status/")
        && request_context.request_path.ends_with("/enabled")
    {
        let Some(module_name) = admin_module_name_from_enabled_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "模块不存在" })),
                )
                    .into_response(),
            ));
        };
        let Some(module) = admin_module_by_name(&module_name) else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": format!("模块 '{module_name}' 不存在") })),
                )
                    .into_response(),
            ));
        };
        let available = module_available_from_env(module.env_key, module.default_available);
        if !available {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({
                        "detail": format!(
                            "模块 '{}' 不可用，无法启用。请检查环境变量 {} 和依赖库。",
                            module.name, module.env_key
                        )
                    })),
                )
                    .into_response(),
            ));
        }
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求体不能为空" })),
                )
                    .into_response(),
            ));
        };
        let payload = match serde_json::from_slice::<AdminSetModuleEnabledRequest>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体格式错误，需要 enabled 字段" })),
                    )
                        .into_response(),
                ));
            }
        };
        let runtime = build_admin_module_runtime_state(state).await?;
        if payload.enabled {
            let (config_validated, config_error) =
                build_admin_module_validation_result(module, &runtime);
            if !config_validated {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({
                            "detail": format!(
                                "模块配置未验证通过: {}",
                                config_error.unwrap_or_else(|| "未知错误".to_string())
                            )
                        })),
                    )
                        .into_response(),
                ));
            }
        }
        let _ = state
            .upsert_system_config_json_value(
                &format!("module.{}.enabled", module.name),
                &json!(payload.enabled),
                Some(&format!("模块 [{}] 启用状态", module.display_name)),
            )
            .await?;
        let updated_runtime = build_admin_module_runtime_state(state).await?;
        let payload = build_admin_module_status_payload(state, module, &updated_runtime).await?;
        return Ok(Some(Json(payload).into_response()));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("version")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/version"
    {
        return Ok(Some(
            Json(json!({ "version": current_aether_version() })).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("check_update")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/check-update"
    {
        return Ok(Some(
            Json(build_admin_system_check_update_payload()).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("aws_regions")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/aws-regions"
    {
        return Ok(Some(
            Json(json!({ "regions": ADMIN_AWS_REGIONS })).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("stats")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/stats"
    {
        return Ok(Some(
            Json(build_admin_system_stats_payload(state).await?).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("settings_get")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/settings"
    {
        return Ok(Some(
            Json(build_admin_system_settings_payload(state).await?).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("config_export")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/config/export"
    {
        return Ok(Some(
            Json(build_admin_system_config_export_payload(state).await?).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("users_export")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/users/export"
    {
        return Ok(Some(
            Json(build_admin_system_users_export_payload(state).await?).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && matches!(
            decision.route_kind.as_deref(),
            Some(
                "config_import"
                    | "users_import"
                    | "smtp_test"
                    | "cleanup"
                    | "purge_config"
                    | "purge_users"
                    | "purge_usage"
                    | "purge_audit_logs"
                    | "purge_request_bodies"
                    | "purge_stats"
            )
        )
        && request_context.request_method == http::Method::POST
    {
        return Ok(Some(
            (
                http::StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "detail": "Admin system maintenance requires Rust backend" })),
            )
                .into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("settings_set")
        && request_context.request_method == http::Method::PUT
        && request_context.request_path == "/api/admin/system/settings"
    {
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response(),
            ));
        };
        return Ok(Some(
            match apply_admin_system_settings_update(state, request_body).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("configs_list")
        && request_context.request_method == http::Method::GET
        && is_admin_system_configs_root(&request_context.request_path)
    {
        let entries = state.list_system_config_entries().await?;
        return Ok(Some(
            Json(build_admin_system_configs_payload(&entries)).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("config_get")
        && request_context.request_method == http::Method::GET
    {
        let Some(config_key) = admin_system_config_key_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "配置项不存在",
                None,
            )));
        };
        return Ok(Some(
            match build_admin_system_config_detail_payload(state, &config_key).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("config_set")
        && request_context.request_method == http::Method::PUT
    {
        let Some(config_key) = admin_system_config_key_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "配置项不存在",
                None,
            )));
        };
        let Some(request_body) = request_body else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "请求数据验证失败",
                None,
            )));
        };
        return Ok(Some(
            match apply_admin_system_config_update(state, &config_key, request_body).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("config_delete")
        && request_context.request_method == http::Method::DELETE
    {
        let Some(config_key) = admin_system_config_key_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "配置项不存在",
                None,
            )));
        };
        return Ok(Some(
            match delete_admin_system_config(state, &config_key).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("api_formats")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/system/api-formats"
    {
        return Ok(Some(
            Json(build_admin_api_formats_payload()).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("email_templates_list")
        && request_context.request_method == http::Method::GET
        && is_admin_system_email_templates_root(&request_context.request_path)
    {
        return Ok(Some(
            Json(build_admin_email_templates_payload(state).await?).into_response(),
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("email_template_get")
        && request_context.request_method == http::Method::GET
    {
        let Some(template_type) =
            admin_system_email_template_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "模板类型不存在",
                None,
            )));
        };
        return Ok(Some(
            match build_admin_email_template_payload(state, &template_type).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("email_template_set")
        && request_context.request_method == http::Method::PUT
    {
        let Some(template_type) =
            admin_system_email_template_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "模板类型不存在",
                None,
            )));
        };
        let Some(request_body) = request_body else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid_request",
                "请求数据验证失败",
                None,
            )));
        };
        return Ok(Some(
            match apply_admin_email_template_update(state, &template_type, request_body).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("email_template_preview")
        && request_context.request_method == http::Method::POST
    {
        let Some(template_type) =
            admin_system_email_template_preview_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "模板类型不存在",
                None,
            )));
        };
        return Ok(Some(
            match preview_admin_email_template(state, &template_type, request_body).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("system_manage")
        && decision.route_kind.as_deref() == Some("email_template_reset")
        && request_context.request_method == http::Method::POST
    {
        let Some(template_type) =
            admin_system_email_template_reset_type_from_path(&request_context.request_path)
        else {
            return Ok(Some(build_proxy_error_response(
                http::StatusCode::NOT_FOUND,
                "not_found",
                "模板类型不存在",
                None,
            )));
        };
        return Ok(Some(
            match reset_admin_email_template(state, &template_type).await? {
                Ok(payload) => Json(payload).into_response(),
                Err((status, payload)) => (status, Json(payload)).into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("model_catalog_manage")
        && decision.route_kind.as_deref() == Some("catalog")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/models/catalog"
    {
        if !state.has_global_model_data_reader() || !state.has_provider_catalog_data_reader() {
            return Ok(Some(build_admin_model_catalog_maintenance_response()));
        }
        let Some(payload) = build_admin_model_catalog_payload(state).await else {
            return Ok(Some(build_admin_model_catalog_maintenance_response()));
        };
        return Ok(Some(Json(payload).into_response()));
    }

    if decision.route_family.as_deref() == Some("model_external_manage")
        && decision.route_kind.as_deref() == Some("external")
        && request_context.request_method == http::Method::GET
        && request_context.request_path == "/api/admin/models/external"
    {
        return Ok(Some(
            match read_admin_external_models_cache(state).await? {
                Some(payload) => Json(payload).into_response(),
                None => (
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "detail": "External models catalog requires Rust admin backend"
                    })),
                )
                    .into_response(),
            },
        ));
    }

    if decision.route_family.as_deref() == Some("model_external_manage")
        && decision.route_kind.as_deref() == Some("clear_external_cache")
        && request_context.request_method == http::Method::DELETE
        && request_context.request_path == "/api/admin/models/external/cache"
    {
        return Ok(Some(
            Json(clear_admin_external_models_cache(state).await?).into_response(),
        ));
    }

    Ok(None)
}
