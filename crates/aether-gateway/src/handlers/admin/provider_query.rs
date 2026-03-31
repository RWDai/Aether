const ADMIN_PROVIDER_QUERY_INVALID_JSON_DETAIL: &str = "Invalid JSON request body";
const ADMIN_PROVIDER_QUERY_LOCAL_TEST_MODEL_MESSAGE: &str =
    "Rust local provider-query model test is not configured";
const ADMIN_PROVIDER_QUERY_LOCAL_TEST_MODEL_FAILOVER_MESSAGE: &str =
    "Rust local provider-query failover simulation is not configured";
const ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL: &str = "provider_id is required";
const ADMIN_PROVIDER_QUERY_MODEL_REQUIRED_DETAIL: &str = "model is required";
const ADMIN_PROVIDER_QUERY_FAILOVER_MODELS_REQUIRED_DETAIL: &str =
    "failover_models should not be empty";
const ADMIN_PROVIDER_QUERY_PROVIDER_NOT_FOUND_DETAIL: &str = "Provider not found";
const ADMIN_PROVIDER_QUERY_API_KEY_NOT_FOUND_DETAIL: &str = "API Key not found";
const ADMIN_PROVIDER_QUERY_NO_ACTIVE_API_KEY_DETAIL: &str =
    "No active API Key found for this provider";
const ADMIN_PROVIDER_QUERY_NO_LOCAL_MODELS_DETAIL: &str =
    "No models available from local provider catalog";

fn build_admin_provider_query_bad_request_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn build_admin_provider_query_not_found_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::NOT_FOUND,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn parse_admin_provider_query_body(
    request_body: Option<&axum::body::Bytes>,
) -> Result<serde_json::Value, Response<Body>> {
    let Some(raw_body) = request_body else {
        return Ok(json!({}));
    };
    if raw_body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice::<serde_json::Value>(raw_body).map_err(|_| {
        build_admin_provider_query_bad_request_response(ADMIN_PROVIDER_QUERY_INVALID_JSON_DETAIL)
    })
}

fn provider_query_extract_provider_id(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("provider_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn provider_query_extract_api_key_id(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("api_key_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn provider_query_extract_model(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("model")
        .or_else(|| payload.get("model_name"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn provider_query_extract_failover_models(payload: &serde_json::Value) -> Vec<String> {
    payload
        .get("failover_models")
        .or_else(|| payload.get("models"))
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn provider_query_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn provider_query_resolved_api_formats(
    endpoints: &[StoredProviderCatalogEndpoint],
    selected_key: Option<&StoredProviderCatalogKey>,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let key_formats = selected_key
        .map(|key| provider_query_string_list(key.api_formats.as_ref()))
        .unwrap_or_default();
    let mut formats = Vec::new();

    for endpoint in endpoints.iter().filter(|endpoint| endpoint.is_active) {
        let api_format = endpoint.api_format.trim();
        if api_format.is_empty() {
            continue;
        }
        if !key_formats.is_empty() && !key_formats.iter().any(|value| value == api_format) {
            continue;
        }
        if seen.insert(api_format.to_string()) {
            formats.push(api_format.to_string());
        }
    }

    if formats.is_empty() {
        for api_format in key_formats {
            if seen.insert(api_format.clone()) {
                formats.push(api_format);
            }
        }
    }

    formats
}

async fn build_admin_provider_query_models_response(
    state: &AppState,
    payload: &serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let Some(provider_id) = provider_query_extract_provider_id(payload) else {
        return Ok(build_admin_provider_query_bad_request_response(
            ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL,
        ));
    };

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .find(|item| item.id == provider_id)
    else {
        return Ok(build_admin_provider_query_not_found_response(
            ADMIN_PROVIDER_QUERY_PROVIDER_NOT_FOUND_DETAIL,
        ));
    };

    let provider_ids = vec![provider.id.clone()];
    let endpoints = state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?;
    let keys = state
        .list_provider_catalog_keys_by_provider_ids(&provider_ids)
        .await?;
    let selected_key = if let Some(api_key_id) = provider_query_extract_api_key_id(payload) {
        let Some(key) = keys.iter().find(|key| key.id == api_key_id) else {
            return Ok(build_admin_provider_query_not_found_response(
                ADMIN_PROVIDER_QUERY_API_KEY_NOT_FOUND_DETAIL,
            ));
        };
        Some(key)
    } else {
        None
    };
    let active_keys = keys.iter().filter(|key| key.is_active).count();
    if selected_key.is_none() && active_keys == 0 {
        return Ok(build_admin_provider_query_bad_request_response(
            ADMIN_PROVIDER_QUERY_NO_ACTIVE_API_KEY_DETAIL,
        ));
    }

    let resolved_api_formats = provider_query_resolved_api_formats(&endpoints, selected_key);
    let provider_models = state
        .list_admin_provider_available_source_models(&provider.id)
        .await?;

    let mut grouped: BTreeMap<
        String,
        (
            aether_data::repository::global_models::StoredAdminProviderModel,
            BTreeSet<String>,
        ),
    > = BTreeMap::new();
    for model in provider_models {
        let entry = grouped
            .entry(model.provider_model_name.clone())
            .or_insert_with(|| (model.clone(), BTreeSet::new()));
        for api_format in &resolved_api_formats {
            entry.1.insert(api_format.clone());
        }
    }

    let models: Vec<_> = grouped
        .into_iter()
        .map(|(model_id, (model, api_formats))| {
            let display_name = model
                .global_model_display_name
                .clone()
                .or(model.global_model_name.clone())
                .unwrap_or_else(|| model_id.clone());
            let api_formats: Vec<_> = api_formats.into_iter().collect();
            json!({
                "id": model_id,
                "object": "model",
                "created": model.created_at_unix_secs,
                "owned_by": provider.name,
                "display_name": display_name,
                "api_format": api_formats.first().cloned(),
                "api_formats": api_formats,
                "provider_model_name": model.provider_model_name,
                "global_model_id": model.global_model_id,
                "global_model_name": model.global_model_name,
                "supports_streaming": model.supports_streaming,
                "supports_function_calling": model.supports_function_calling,
                "supports_vision": model.supports_vision,
                "supports_extended_thinking": model.supports_extended_thinking,
                "supports_image_generation": model.supports_image_generation,
                "is_available": model.is_available,
            })
        })
        .collect();
    let success = !models.is_empty();
    let error = if success {
        None
    } else {
        Some(ADMIN_PROVIDER_QUERY_NO_LOCAL_MODELS_DETAIL)
    };

    Ok(Json(json!({
        "success": success,
        "data": {
            "models": models,
            "error": error,
            "from_cache": true,
            "keys_total": active_keys,
            "keys_cached": 0,
            "keys_fetched": 0,
        },
        "provider": {
            "id": provider.id,
            "name": provider.name,
            "display_name": provider.name,
        },
    }))
    .into_response())
}

fn build_admin_provider_query_test_model_response(
    provider_id: String,
    model: String,
) -> Response<Body> {
    Json(json!({
        "success": false,
        "tested": false,
        "provider_id": provider_id,
        "model": model,
        "source": "local",
        "message": ADMIN_PROVIDER_QUERY_LOCAL_TEST_MODEL_MESSAGE,
    }))
    .into_response()
}

fn build_admin_provider_query_test_model_failover_response(
    provider_id: String,
    failover_models: Vec<String>,
) -> Response<Body> {
    Json(json!({
        "success": false,
        "tested": false,
        "provider_id": provider_id,
        "failover_models": failover_models,
        "source": "local",
        "message": ADMIN_PROVIDER_QUERY_LOCAL_TEST_MODEL_FAILOVER_MESSAGE,
    }))
    .into_response()
}

async fn maybe_build_local_admin_provider_query_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("provider_query_manage") {
        return Ok(None);
    }

    if request_context.request_method != http::Method::POST {
        return Ok(None);
    }

    let payload = match parse_admin_provider_query_body(request_body) {
        Ok(value) => value,
        Err(response) => return Ok(Some(response)),
    };

    let route_kind = decision.route_kind.as_deref().unwrap_or("query_models");
    match route_kind {
        "query_models" => Ok(Some(
            build_admin_provider_query_models_response(state, &payload).await?,
        )),
        "test_model" => {
            let Some(provider_id) = provider_query_extract_provider_id(&payload) else {
                return Ok(Some(build_admin_provider_query_bad_request_response(
                    ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL,
                )));
            };
            let Some(model) = provider_query_extract_model(&payload) else {
                return Ok(Some(build_admin_provider_query_bad_request_response(
                    ADMIN_PROVIDER_QUERY_MODEL_REQUIRED_DETAIL,
                )));
            };
            Ok(Some(build_admin_provider_query_test_model_response(
                provider_id,
                model,
            )))
        }
        "test_model_failover" => {
            let Some(provider_id) = provider_query_extract_provider_id(&payload) else {
                return Ok(Some(build_admin_provider_query_bad_request_response(
                    ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL,
                )));
            };
            let failover_models = provider_query_extract_failover_models(&payload);
            if failover_models.is_empty() {
                return Ok(Some(build_admin_provider_query_bad_request_response(
                    ADMIN_PROVIDER_QUERY_FAILOVER_MODELS_REQUIRED_DETAIL,
                )));
            }
            Ok(Some(
                build_admin_provider_query_test_model_failover_response(
                    provider_id,
                    failover_models,
                ),
            ))
        }
        _ => Ok(Some(
            build_admin_provider_query_models_response(state, &payload).await?,
        )),
    }
}
