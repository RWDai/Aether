const ADMIN_USAGE_RUST_BACKEND_DETAIL: &str = "Admin usage routes require Rust maintenance backend";
fn admin_usage_id_from_path_suffix(request_path: &str, suffix: Option<&str>) -> Option<String> {
    let mut value = request_path
        .strip_prefix("/api/admin/usage/")?
        .trim()
        .trim_matches('/')
        .to_string();
    if let Some(suffix) = suffix {
        value = value.strip_suffix(suffix)?.trim_matches('/').to_string();
    }
    if value.is_empty() || value.contains('/') {
        None
    } else {
        Some(value)
    }
}

fn admin_usage_id_from_detail_path(request_path: &str) -> Option<String> {
    admin_usage_id_from_path_suffix(request_path, None)
}

fn admin_usage_id_from_action_path(request_path: &str, action: &str) -> Option<String> {
    admin_usage_id_from_path_suffix(request_path, Some(action))
}

#[derive(Debug, Default, serde::Deserialize)]
struct AdminUsageReplayRequest {
    #[serde(default, alias = "target_provider_id")]
    provider_id: Option<String>,
    #[serde(default, alias = "target_endpoint_id")]
    endpoint_id: Option<String>,
    #[serde(default, alias = "target_api_key_id")]
    api_key_id: Option<String>,
    #[serde(default)]
    body_override: Option<serde_json::Value>,
}

fn admin_usage_resolve_replay_mode(same_provider: bool, same_endpoint: bool) -> &'static str {
    if same_provider && same_endpoint {
        "same_endpoint_reuse"
    } else if same_provider {
        "same_provider_remap"
    } else {
        "cross_provider_remap"
    }
}

fn admin_usage_resolve_request_preview_body(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    body_override: Option<serde_json::Value>,
) -> serde_json::Value {
    let resolved_model = item.model.clone();
    let mut request_body = body_override.unwrap_or_else(|| {
        json!({
            "model": resolved_model,
            "stream": item.is_stream,
        })
    });
    if let Some(body) = request_body.as_object_mut() {
        body.entry("model".to_string())
            .or_insert_with(|| json!(resolved_model));
        if !body.contains_key("stream") {
            body.insert("stream".to_string(), json!(item.is_stream));
        }
        if let Some(target_model) = item
            .target_model
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            body.entry("target_model".to_string())
                .or_insert_with(|| json!(target_model));
        }
        if let Some(request_type) = item
            .request_type
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            body.entry("request_type".to_string())
                .or_insert_with(|| json!(request_type));
        }
        if let Some(api_format) = item
            .api_format
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            body.entry("api_format".to_string())
                .or_insert_with(|| json!(api_format));
        }
    }
    request_body
}

async fn build_admin_usage_replay_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_usage_data_reader() || !state.has_provider_catalog_data_reader() {
        return Ok(admin_usage_maintenance_response(
            ADMIN_USAGE_RUST_BACKEND_DETAIL,
        ));
    }

    let Some(usage_id) = admin_usage_id_from_action_path(&request_context.request_path, "/replay")
    else {
        return Ok(admin_usage_bad_request_response("usage_id 无效"));
    };

    let payload = match request_body {
        Some(body) if !body.is_empty() => {
            serde_json::from_slice::<AdminUsageReplayRequest>(body).unwrap_or_default()
        }
        _ => AdminUsageReplayRequest::default(),
    };

    let Some(item) = state.find_request_usage_by_id(&usage_id).await? else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": "Usage record not found" })),
        )
            .into_response());
    };

    let target_provider_id = payload
        .provider_id
        .clone()
        .or_else(|| item.provider_id.clone())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let Some(target_provider_id) = target_provider_id else {
        return Ok(admin_usage_bad_request_response(
            "Replay target provider is unavailable",
        ));
    };
    let Some(target_provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&target_provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok((
            http::StatusCode::NOT_FOUND,
            Json(json!({ "detail": format!("Provider {target_provider_id} 不存在") })),
        )
            .into_response());
    };

    let requested_endpoint_id = payload
        .endpoint_id
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let target_endpoint = if let Some(endpoint_id) = requested_endpoint_id.clone() {
        let Some(endpoint) = state
            .read_provider_catalog_endpoints_by_ids(std::slice::from_ref(&endpoint_id))
            .await?
            .into_iter()
            .next()
        else {
            return Ok((
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": format!("Endpoint {endpoint_id} 不存在") })),
            )
                .into_response());
        };
        if endpoint.provider_id != target_provider.id {
            return Ok(admin_usage_bad_request_response(
                "Target endpoint does not belong to the target provider",
            ));
        }
        endpoint
    } else {
        let preferred_endpoint_id = item
            .provider_endpoint_id
            .clone()
            .filter(|_| item.provider_id.as_deref() == Some(target_provider.id.as_str()));
        if let Some(endpoint_id) = preferred_endpoint_id {
            if let Some(endpoint) = state
                .read_provider_catalog_endpoints_by_ids(std::slice::from_ref(&endpoint_id))
                .await?
                .into_iter()
                .find(|endpoint| endpoint.provider_id == target_provider.id)
            {
                endpoint
            } else {
                let mut endpoints = state
                    .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(
                        &target_provider.id,
                    ))
                    .await?;
                let preferred_api_format = item
                    .endpoint_api_format
                    .as_deref()
                    .or(item.api_format.as_deref())
                    .unwrap_or_default();
                endpoints
                    .iter()
                    .find(|endpoint| {
                        endpoint.is_active && endpoint.api_format == preferred_api_format
                    })
                    .cloned()
                    .or_else(|| {
                        endpoints
                            .iter()
                            .find(|endpoint| endpoint.is_active)
                            .cloned()
                    })
                    .or_else(|| endpoints.into_iter().next())
                    .ok_or_else(|| {
                        GatewayError::Internal("target provider has no endpoints".to_string())
                    })?
            }
        } else {
            let mut endpoints = state
                .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(
                    &target_provider.id,
                ))
                .await?;
            let preferred_api_format = item
                .endpoint_api_format
                .as_deref()
                .or(item.api_format.as_deref())
                .unwrap_or_default();
            endpoints
                .iter()
                .find(|endpoint| endpoint.is_active && endpoint.api_format == preferred_api_format)
                .cloned()
                .or_else(|| {
                    endpoints
                        .iter()
                        .find(|endpoint| endpoint.is_active)
                        .cloned()
                })
                .or_else(|| endpoints.into_iter().next())
                .ok_or_else(|| {
                    GatewayError::Internal("target provider has no endpoints".to_string())
                })?
        }
    };

    let same_provider = item.provider_id.as_deref() == Some(target_provider.id.as_str());
    let same_endpoint = item.provider_endpoint_id.as_deref() == Some(target_endpoint.id.as_str());
    let resolved_model = item.model.clone();
    let mapping_source = "none";
    let request_body = admin_usage_resolve_request_preview_body(&item, payload.body_override);

    let url = admin_usage_curl_url(&target_endpoint, &item);
    let headers = admin_usage_curl_headers();
    let curl = admin_usage_build_curl_command(Some(&url), &headers, Some(&request_body));
    Ok(Json(json!({
        "dry_run": true,
        "usage_id": item.id,
        "request_id": item.request_id,
        "mode": admin_usage_resolve_replay_mode(same_provider, same_endpoint),
        "target_provider_id": target_provider.id,
        "target_provider_name": target_provider.name,
        "target_endpoint_id": target_endpoint.id,
        "target_api_key_id": payload.api_key_id.or(item.provider_api_key_id.clone()),
        "target_api_format": target_endpoint.api_format,
        "resolved_model": resolved_model,
        "mapping_source": mapping_source,
        "method": "POST",
        "url": url,
        "request_headers": headers,
        "request_body": request_body,
        "original_request_body_available": false,
        "note": "Rust local replay currently exposes a dry-run plan and does not dispatch upstream",
        "curl": curl,
    }))
    .into_response())
}

async fn maybe_build_local_admin_usage_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("usage_manage") {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("stats")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/stats" | "/api/admin/usage/stats/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let time_range = match AdminStatsTimeRange::resolve_optional(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let usage = list_usage_for_optional_range(
                state,
                time_range.as_ref(),
                &AdminStatsUsageFilter::default(),
            )
            .await?;
            let aggregate = aggregate_usage_stats(&usage);
            let cache_creation_tokens: u64 = usage
                .iter()
                .map(|item| item.cache_creation_input_tokens)
                .sum();
            let cache_read_tokens: u64 =
                usage.iter().map(|item| item.cache_read_input_tokens).sum();
            let cache_creation_cost: f64 =
                usage.iter().map(|item| item.cache_creation_cost_usd).sum();
            let cache_read_cost: f64 = usage.iter().map(|item| item.cache_read_cost_usd).sum();
            let total_tokens: u64 = usage.iter().map(admin_usage_total_tokens).sum();
            let avg_response_time = round_to(aggregate.avg_response_time_ms() / 1000.0, 2);
            let error_rate = if aggregate.total_requests == 0 {
                0.0
            } else {
                round_to(
                    (aggregate.error_requests as f64 / aggregate.total_requests as f64) * 100.0,
                    2,
                )
            };

            return Ok(Some(
                Json(json!({
                    "total_requests": aggregate.total_requests,
                    "total_tokens": total_tokens,
                    "total_cost": round_to(aggregate.total_cost, 6),
                    "total_actual_cost": round_to(aggregate.actual_total_cost, 6),
                    "avg_response_time": avg_response_time,
                    "error_count": aggregate.error_requests,
                    "error_rate": error_rate,
                    "cache_stats": {
                        "cache_creation_tokens": cache_creation_tokens,
                        "cache_read_tokens": cache_read_tokens,
                        "cache_creation_cost": round_to(cache_creation_cost, 6),
                        "cache_read_cost": round_to(cache_read_cost, 6),
                    }
                }))
                .into_response(),
            ));
        }
        Some("active")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/active" | "/api/admin/usage/active/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let requested_ids = admin_usage_parse_ids(query);
            let usage = state
                .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery::default())
                .await?;
            let mut items: Vec<_> = usage
                .into_iter()
                .filter(|item| match requested_ids.as_ref() {
                    Some(ids) => ids.contains(&item.id),
                    None => matches!(item.status.as_str(), "pending" | "streaming"),
                })
                .collect();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| left.id.cmp(&right.id))
            });
            if requested_ids.is_none() && items.len() > 50 {
                items.truncate(50);
            }

            let payload: Vec<_> = items
                .into_iter()
                .map(|item| {
                    let mut value = json!({
                        "id": item.id,
                        "status": item.status,
                        "input_tokens": item.input_tokens,
                        "output_tokens": item.output_tokens,
                        "cache_creation_input_tokens": item.cache_creation_input_tokens,
                        "cache_read_input_tokens": item.cache_read_input_tokens,
                        "cost": round_to(item.total_cost_usd, 6),
                        "actual_cost": round_to(item.actual_total_cost_usd, 6),
                        "response_time_ms": item.response_time_ms,
                        "first_byte_time_ms": item.first_byte_time_ms,
                        "provider": item.provider_name,
                        "api_key_name": item.api_key_name,
                    });
                    if let Some(api_format) = item.api_format {
                        value["api_format"] = json!(api_format);
                    }
                    if let Some(endpoint_api_format) = item.endpoint_api_format {
                        value["endpoint_api_format"] = json!(endpoint_api_format);
                    }
                    value["has_format_conversion"] = json!(item.has_format_conversion);
                    if let Some(target_model) = item.target_model {
                        value["target_model"] = json!(target_model);
                    }
                    value
                })
                .collect();

            return Ok(Some(Json(json!({ "requests": payload })).into_response()));
        }
        Some("records")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/records" | "/api/admin/usage/records/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let time_range = match AdminStatsTimeRange::resolve_optional(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let filters = AdminStatsUsageFilter {
                user_id: query_param_value(query, "user_id"),
                provider_name: None,
                model: None,
            };
            let mut usage =
                list_usage_for_optional_range(state, time_range.as_ref(), &filters).await?;

            let search = query_param_value(query, "search");
            let username_filter = query_param_value(query, "username");
            let model_filter = query_param_value(query, "model");
            let provider_filter = query_param_value(query, "provider");
            let api_format_filter = query_param_value(query, "api_format");
            let status_filter = query_param_value(query, "status");
            let limit = match admin_usage_parse_limit(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let offset = match admin_usage_parse_offset(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };

            usage.retain(|item| {
                admin_usage_matches_search(item, search.as_deref())
                    && admin_usage_matches_username(item, username_filter.as_deref())
                    && admin_usage_matches_eq(item.model.as_str(), model_filter.as_deref())
                    && admin_usage_matches_eq(
                        item.provider_name.as_str(),
                        provider_filter.as_deref(),
                    )
                    && admin_usage_matches_api_format(item, api_format_filter.as_deref())
                    && admin_usage_matches_status(item, status_filter.as_deref())
            });
            usage.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| left.id.cmp(&right.id))
            });
            let total = usage.len();

            let user_ids: Vec<String> = usage
                .iter()
                .filter_map(|item| item.user_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let users_by_id: BTreeMap<String, aether_data::repository::users::StoredUserSummary> =
                if state.has_user_data_reader() && !user_ids.is_empty() {
                    state
                        .list_users_by_ids(&user_ids)
                        .await?
                        .into_iter()
                        .map(|user| (user.id.clone(), user))
                        .collect()
                } else {
                    BTreeMap::new()
                };

            let records: Vec<_> = usage
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|item| admin_usage_record_json(&item, &users_by_id))
                .collect();

            return Ok(Some(
                Json(json!({
                    "records": records,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                }))
                .into_response(),
            ));
        }
        Some("aggregation_stats")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/aggregation/stats" | "/api/admin/usage/aggregation/stats/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let group_by = query_param_value(query, "group_by")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !matches!(
                group_by.as_str(),
                "model" | "user" | "provider" | "api_format"
            ) {
                return Ok(Some(admin_usage_bad_request_response(
                    "Invalid group_by value: must be one of model, user, provider, api_format",
                )));
            }
            let limit = match admin_usage_parse_aggregation_limit(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let time_range = match AdminStatsTimeRange::resolve_optional(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };

            let mut usage = list_usage_for_optional_range(
                state,
                time_range.as_ref(),
                &AdminStatsUsageFilter::default(),
            )
            .await?;
            usage.retain(|item| item.status != "pending" && item.status != "streaming");

            let response = match group_by.as_str() {
                "model" => admin_usage_aggregation_by_model_json(&usage, limit),
                "user" => admin_usage_aggregation_by_user_json(state, &usage, limit).await?,
                "provider" => admin_usage_aggregation_by_provider_json(&usage, limit),
                "api_format" => admin_usage_aggregation_by_api_format_json(&usage, limit),
                _ => unreachable!(),
            };
            return Ok(Some(Json(response).into_response()));
        }
        Some("heatmap")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/heatmap" | "/api/admin/usage/heatmap/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }
            let now_unix_secs = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
            let created_from_unix_secs = now_unix_secs.saturating_sub(365 * 24 * 3600);
            let mut usage = state
                .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
                    created_from_unix_secs: Some(created_from_unix_secs),
                    ..Default::default()
                })
                .await?;
            usage.retain(|item| item.status != "pending" && item.status != "streaming");
            return Ok(Some(Json(admin_usage_heatmap_json(&usage)).into_response()));
        }
        Some("curl")
            if request_context.request_method == http::Method::GET
                && request_context
                    .request_path
                    .starts_with("/api/admin/usage/")
                && request_context.request_path.ends_with("/curl") =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let Some(usage_id) =
                admin_usage_id_from_action_path(&request_context.request_path, "/curl")
            else {
                return Ok(Some(admin_usage_bad_request_response("usage_id 无效")));
            };

            let Some(item) = state.find_request_usage_by_id(&usage_id).await? else {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Usage record not found" })),
                    )
                        .into_response(),
                ));
            };

            let endpoint = if let Some(endpoint_id) = item.provider_endpoint_id.as_ref() {
                state
                    .read_provider_catalog_endpoints_by_ids(std::slice::from_ref(endpoint_id))
                    .await?
                    .into_iter()
                    .next()
            } else {
                None
            };
            let url = endpoint
                .as_ref()
                .map(|endpoint| admin_usage_curl_url(endpoint, &item));
            let headers = admin_usage_curl_headers();
            let body = admin_usage_resolve_request_preview_body(&item, None);
            let curl = admin_usage_build_curl_command(url.as_deref(), &headers, Some(&body));

            return Ok(Some(
                Json(json!({
                    "url": url,
                    "method": "POST",
                    "headers": headers,
                    "body": body,
                    "curl": curl,
                    "original_request_body_available": false,
                }))
                .into_response(),
            ));
        }
        Some("cache_affinity_hit_analysis")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/cache-affinity/hit-analysis"
                        | "/api/admin/usage/cache-affinity/hit-analysis/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let hours = match admin_usage_parse_recent_hours(query, 168) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let user_id = query_param_value(query, "user_id");
            let api_key_id = query_param_value(query, "api_key_id");
            let usage =
                list_recent_completed_usage_for_cache_affinity(state, hours, user_id.as_deref())
                    .await?;
            let filtered: Vec<_> = usage
                .into_iter()
                .filter(|item| {
                    admin_usage_matches_optional_id(item.user_id.as_deref(), user_id.as_deref())
                        && admin_usage_matches_optional_id(
                            item.api_key_id.as_deref(),
                            api_key_id.as_deref(),
                        )
                })
                .collect();
            let total_requests = filtered.len();
            let total_input_tokens: u64 = filtered.iter().map(|item| item.input_tokens).sum();
            let total_cache_read_tokens: u64 = filtered
                .iter()
                .map(|item| item.cache_read_input_tokens)
                .sum();
            let total_cache_creation_tokens: u64 = filtered
                .iter()
                .map(|item| item.cache_creation_input_tokens)
                .sum();
            let total_cache_read_cost: f64 =
                filtered.iter().map(|item| item.cache_read_cost_usd).sum();
            let total_cache_creation_cost: f64 = filtered
                .iter()
                .map(|item| item.cache_creation_cost_usd)
                .sum();
            let requests_with_cache_hit = filtered
                .iter()
                .filter(|item| item.cache_read_input_tokens > 0)
                .count();
            let total_context_tokens = total_input_tokens.saturating_add(total_cache_read_tokens);
            let token_cache_hit_rate = if total_context_tokens == 0 {
                0.0
            } else {
                round_to(
                    total_cache_read_tokens as f64 / total_context_tokens as f64 * 100.0,
                    2,
                )
            };
            let request_cache_hit_rate = if total_requests == 0 {
                0.0
            } else {
                round_to(
                    requests_with_cache_hit as f64 / total_requests as f64 * 100.0,
                    2,
                )
            };

            return Ok(Some(
                Json(json!({
                    "analysis_period_hours": hours,
                    "total_requests": total_requests,
                    "requests_with_cache_hit": requests_with_cache_hit,
                    "request_cache_hit_rate": request_cache_hit_rate,
                    "total_input_tokens": total_input_tokens,
                    "total_cache_read_tokens": total_cache_read_tokens,
                    "total_cache_creation_tokens": total_cache_creation_tokens,
                    "token_cache_hit_rate": token_cache_hit_rate,
                    "total_cache_read_cost_usd": round_to(total_cache_read_cost, 4),
                    "total_cache_creation_cost_usd": round_to(total_cache_creation_cost, 4),
                    "estimated_savings_usd": round_to(total_cache_read_cost * 9.0, 4),
                }))
                .into_response(),
            ));
        }
        Some("cache_affinity_interval_timeline")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/cache-affinity/interval-timeline"
                        | "/api/admin/usage/cache-affinity/interval-timeline/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let hours = match admin_usage_parse_recent_hours(query, 24) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let limit = match admin_usage_parse_timeline_limit(query) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let user_id = query_param_value(query, "user_id");
            let include_user_info = query_param_bool(query, "include_user_info", false);
            let usage =
                list_recent_completed_usage_for_cache_affinity(state, hours, user_id.as_deref())
                    .await?;
            let mut grouped: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
            let mut models = BTreeSet::new();
            let mut usernames_by_user_id = BTreeMap::new();

            for (group_user_id, items) in admin_usage_group_completed_by_user(&usage) {
                if let Some(ref requested_user_id) = user_id {
                    if &group_user_id != requested_user_id {
                        continue;
                    }
                }

                let mut previous_created_at_unix_secs = None;
                for item in items {
                    if let Some(previous) = previous_created_at_unix_secs {
                        let interval_minutes =
                            item.created_at_unix_secs.saturating_sub(previous) as f64 / 60.0;
                        if interval_minutes <= 120.0 {
                            let mut point = json!({
                                "x": unix_secs_to_rfc3339(item.created_at_unix_secs),
                                "y": round_to(interval_minutes, 2),
                            });
                            if !item.model.trim().is_empty() {
                                point["model"] = json!(item.model.clone());
                                models.insert(item.model.clone());
                            }
                            if include_user_info && user_id.is_none() {
                                point["user_id"] = json!(group_user_id.clone());
                                if let Some(username) = item.username.clone() {
                                    usernames_by_user_id
                                        .entry(group_user_id.clone())
                                        .or_insert(username);
                                }
                            }
                            grouped
                                .entry(group_user_id.clone())
                                .or_default()
                                .push(point);
                        }
                    }
                    previous_created_at_unix_secs = Some(item.created_at_unix_secs);
                }
            }

            if include_user_info && user_id.is_none() && state.has_user_data_reader() {
                let user_ids: Vec<_> = grouped.keys().cloned().collect();
                let user_map: BTreeMap<_, _> = state
                    .list_users_by_ids(&user_ids)
                    .await?
                    .into_iter()
                    .map(|user| (user.id, user.username))
                    .collect();
                for (user_id, username) in user_map {
                    usernames_by_user_id.insert(user_id, username);
                }
            }

            let total_points_before_limit: usize = grouped.values().map(Vec::len).sum();
            let points: Vec<serde_json::Value> = if include_user_info && user_id.is_none() {
                let user_limits =
                    admin_usage_proportional_limits(&grouped, limit, total_points_before_limit);
                let mut selected = Vec::new();
                for (group_user_id, mut items) in grouped {
                    let take = user_limits
                        .get(&group_user_id)
                        .copied()
                        .unwrap_or(items.len());
                    selected.extend(items.drain(..std::cmp::min(take, items.len())));
                }
                selected.sort_by(admin_usage_point_sort_key);
                selected
            } else {
                let mut selected = grouped
                    .into_values()
                    .flatten()
                    .collect::<Vec<serde_json::Value>>();
                selected.sort_by(admin_usage_point_sort_key);
                if selected.len() > limit {
                    selected.truncate(limit);
                }
                selected
            };

            let mut response = json!({
                "analysis_period_hours": hours,
                "total_points": points.len(),
                "points": points,
            });
            if include_user_info && user_id.is_none() {
                response["users"] = json!(usernames_by_user_id);
            }
            if !models.is_empty() {
                response["models"] = json!(models.into_iter().collect::<Vec<_>>());
            }

            return Ok(Some(Json(response).into_response()));
        }
        Some("cache_affinity_ttl_analysis")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/admin/usage/cache-affinity/ttl-analysis"
                        | "/api/admin/usage/cache-affinity/ttl-analysis/"
                ) =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let query = request_context.request_query_string.as_deref();
            let hours = match admin_usage_parse_recent_hours(query, 168) {
                Ok(value) => value,
                Err(detail) => return Ok(Some(admin_usage_bad_request_response(detail))),
            };
            let user_id = query_param_value(query, "user_id");
            let api_key_id = query_param_value(query, "api_key_id");
            let group_by_api_key = api_key_id.is_some();
            let usage =
                list_recent_completed_usage_for_cache_affinity(state, hours, user_id.as_deref())
                    .await?;

            let grouped = if group_by_api_key {
                admin_usage_group_completed_by_api_key(&usage, api_key_id.as_deref())
            } else {
                admin_usage_group_completed_by_user(&usage)
                    .into_iter()
                    .filter(|(group_user_id, _)| {
                        admin_usage_matches_optional_id(
                            Some(group_user_id.as_str()),
                            user_id.as_deref(),
                        )
                    })
                    .collect()
            };

            let user_map: BTreeMap<String, aether_data::repository::users::StoredUserSummary> =
                if !group_by_api_key && state.has_user_data_reader() {
                    let user_ids = grouped.keys().cloned().collect::<Vec<_>>();
                    state
                        .list_users_by_ids(&user_ids)
                        .await?
                        .into_iter()
                        .map(|user| (user.id.clone(), user))
                        .collect()
                } else {
                    BTreeMap::new()
                };

            let mut ttl_distribution = json!({
                "5min": 0_u64,
                "15min": 0_u64,
                "30min": 0_u64,
                "60min": 0_u64,
            });
            let mut users = Vec::new();

            for (group_id, items) in grouped {
                let intervals = admin_usage_collect_request_intervals_minutes(&items);
                if intervals.len() < 2 {
                    continue;
                }

                let within_5min = intervals.iter().filter(|value| **value <= 5.0).count() as u64;
                let within_15min = intervals
                    .iter()
                    .filter(|value| **value > 5.0 && **value <= 15.0)
                    .count() as u64;
                let within_30min = intervals
                    .iter()
                    .filter(|value| **value > 15.0 && **value <= 30.0)
                    .count() as u64;
                let within_60min = intervals
                    .iter()
                    .filter(|value| **value > 30.0 && **value <= 60.0)
                    .count() as u64;
                let over_60min = intervals.iter().filter(|value| **value > 60.0).count() as u64;
                let request_count = intervals.len() as u64;
                let p50 = admin_usage_percentile_cont(&intervals, 0.5);
                let p75 = admin_usage_percentile_cont(&intervals, 0.75);
                let p90 = admin_usage_percentile_cont(&intervals, 0.90);
                let avg_interval = intervals.iter().copied().sum::<f64>() / intervals.len() as f64;
                let min_interval = intervals.iter().copied().reduce(f64::min);
                let max_interval = intervals.iter().copied().reduce(f64::max);
                let recommended_ttl = admin_usage_calculate_recommended_ttl(p75, p90);
                match recommended_ttl {
                    0..=5 => {
                        ttl_distribution["5min"] = json!(ttl_distribution["5min"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(1))
                    }
                    6..=15 => {
                        ttl_distribution["15min"] = json!(ttl_distribution["15min"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(1))
                    }
                    16..=30 => {
                        ttl_distribution["30min"] = json!(ttl_distribution["30min"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(1))
                    }
                    _ => {
                        ttl_distribution["60min"] = json!(ttl_distribution["60min"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_add(1))
                    }
                }

                let (username, email) = if group_by_api_key {
                    (Value::Null, Value::Null)
                } else if let Some(user) = user_map.get(&group_id) {
                    (
                        json!(user.username.clone()),
                        json!(user.email.clone().unwrap_or_default()),
                    )
                } else {
                    (Value::Null, Value::Null)
                };

                users.push(json!({
                    "group_id": group_id,
                    "username": username,
                    "email": email,
                    "request_count": request_count,
                    "interval_distribution": {
                        "within_5min": within_5min,
                        "within_15min": within_15min,
                        "within_30min": within_30min,
                        "within_60min": within_60min,
                        "over_60min": over_60min,
                    },
                    "interval_percentages": {
                        "within_5min": round_to(within_5min as f64 / request_count as f64 * 100.0, 1),
                        "within_15min": round_to(within_15min as f64 / request_count as f64 * 100.0, 1),
                        "within_30min": round_to(within_30min as f64 / request_count as f64 * 100.0, 1),
                        "within_60min": round_to(within_60min as f64 / request_count as f64 * 100.0, 1),
                        "over_60min": round_to(over_60min as f64 / request_count as f64 * 100.0, 1),
                    },
                    "percentiles": {
                        "p50": p50.map(|value| round_to(value, 2)),
                        "p75": p75.map(|value| round_to(value, 2)),
                        "p90": p90.map(|value| round_to(value, 2)),
                    },
                    "avg_interval_minutes": round_to(avg_interval, 2),
                    "min_interval_minutes": min_interval.map(|value| round_to(value, 2)),
                    "max_interval_minutes": max_interval.map(|value| round_to(value, 2)),
                    "recommended_ttl_minutes": recommended_ttl,
                    "recommendation_reason": admin_usage_ttl_recommendation_reason(recommended_ttl, p75, p90),
                }));
            }

            users.sort_by(|left, right| {
                right["request_count"]
                    .as_u64()
                    .unwrap_or(0)
                    .cmp(&left["request_count"].as_u64().unwrap_or(0))
                    .then_with(|| {
                        left["group_id"]
                            .as_str()
                            .unwrap_or_default()
                            .cmp(right["group_id"].as_str().unwrap_or_default())
                    })
            });

            return Ok(Some(
                Json(json!({
                    "analysis_period_hours": hours,
                    "total_users_analyzed": users.len(),
                    "ttl_distribution": ttl_distribution,
                    "users": users,
                }))
                .into_response(),
            ));
        }
        Some("replay") => {
            return Ok(Some(
                build_admin_usage_replay_response(state, request_context, request_body).await?,
            ));
        }
        Some("detail")
            if request_context.request_method == http::Method::GET
                && request_context
                    .request_path
                    .starts_with("/api/admin/usage/") =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_maintenance_response(
                    ADMIN_USAGE_RUST_BACKEND_DETAIL,
                )));
            }

            let Some(usage_id) = admin_usage_id_from_detail_path(&request_context.request_path)
            else {
                return Ok(Some(admin_usage_bad_request_response("usage_id 无效")));
            };
            let include_bodies = query_param_bool(
                request_context.request_query_string.as_deref(),
                "include_bodies",
                true,
            );

            let Some(item) = state.find_request_usage_by_id(&usage_id).await? else {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Usage record not found" })),
                    )
                        .into_response(),
                ));
            };

            let users_by_id: BTreeMap<String, aether_data::repository::users::StoredUserSummary> =
                if state.has_user_data_reader() {
                    if let Some(user_id) = item.user_id.as_ref() {
                        state
                            .list_users_by_ids(std::slice::from_ref(user_id))
                            .await?
                            .into_iter()
                            .map(|user| (user.id.clone(), user))
                            .collect()
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    BTreeMap::new()
                };

            let mut payload = admin_usage_record_json(&item, &users_by_id);
            payload["user"] = match item.user_id.as_ref() {
                Some(user_id) => json!({
                    "id": user_id,
                    "email": payload["user_email"].clone(),
                    "username": payload["username"].clone(),
                }),
                None => Value::Null,
            };
            payload["request_id"] = json!(item.request_id);
            payload["billing_status"] = json!(item.billing_status);
            payload["request_type"] = json!(item.request_type);
            payload["provider_id"] = json!(item.provider_id);
            payload["provider_endpoint_id"] = json!(item.provider_endpoint_id);
            payload["provider_api_key_id"] = json!(item.provider_api_key_id);
            payload["error_category"] = json!(item.error_category);
            payload["cache_creation_cost"] = json!(round_to(item.cache_creation_cost_usd, 6));
            payload["cache_read_cost"] = json!(round_to(item.cache_read_cost_usd, 6));
            payload["request_cost"] = json!(round_to(item.total_cost_usd, 6));
            payload["request_headers"] = json!(admin_usage_curl_headers());
            payload["provider_request_headers"] = json!(admin_usage_curl_headers());
            payload["response_headers"] = serde_json::Value::Null;
            payload["client_response_headers"] = serde_json::Value::Null;
            payload["metadata"] = json!({
                "request_preview_source": "local_reconstruction",
                "original_request_body_available": false,
                "original_response_body_available": false,
            });
            payload["has_request_body"] = json!(true);
            payload["has_provider_request_body"] = json!(false);
            payload["has_response_body"] = json!(false);
            payload["has_client_response_body"] = json!(false);
            payload["tiered_pricing"] = serde_json::Value::Null;
            if include_bodies {
                payload["request_body"] = admin_usage_resolve_request_preview_body(&item, None);
                payload["provider_request_body"] = serde_json::Value::Null;
                payload["response_body"] = serde_json::Value::Null;
                payload["client_response_body"] = serde_json::Value::Null;
            } else {
                payload["request_body"] = serde_json::Value::Null;
                payload["provider_request_body"] = serde_json::Value::Null;
                payload["response_body"] = serde_json::Value::Null;
                payload["client_response_body"] = serde_json::Value::Null;
            }

            return Ok(Some(Json(payload).into_response()));
        }
        _ => {}
    }

    Ok(None)
}

fn admin_usage_maintenance_response(detail: &'static str) -> Response<Body> {
    (
        http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

fn admin_usage_bad_request_response(detail: impl Into<String>) -> Response<Body> {
    (
        http::StatusCode::BAD_REQUEST,
        Json(json!({ "detail": detail.into() })),
    )
        .into_response()
}

fn admin_usage_curl_headers() -> BTreeMap<String, String> {
    BTreeMap::from([("Content-Type".to_string(), "application/json".to_string())])
}

fn admin_usage_curl_url(
    endpoint: &aether_data::repository::provider_catalog::StoredProviderCatalogEndpoint,
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
) -> String {
    let api_format = item
        .endpoint_api_format
        .as_deref()
        .or(item.api_format.as_deref())
        .unwrap_or(endpoint.api_format.as_str());

    if let Some(custom_path) = endpoint
        .custom_path
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return crate::gateway::provider_transport::build_passthrough_path_url(
            &endpoint.base_url,
            custom_path,
            None,
            &[],
        )
        .unwrap_or_else(|| endpoint.base_url.clone());
    }

    match api_format {
        value if value.starts_with("claude:") => {
            crate::gateway::provider_transport::build_claude_messages_url(&endpoint.base_url, None)
        }
        value if value.starts_with("gemini:") => {
            crate::gateway::provider_transport::build_gemini_content_url(
                &endpoint.base_url,
                item.target_model.as_deref().unwrap_or(item.model.as_str()),
                item.is_stream,
                None,
            )
            .unwrap_or_else(|| endpoint.base_url.clone())
        }
        value if value.starts_with("openai:") => {
            crate::gateway::provider_transport::build_openai_chat_url(&endpoint.base_url, None)
        }
        _ => endpoint.base_url.clone(),
    }
}

fn admin_usage_curl_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn admin_usage_build_curl_command(
    url: Option<&str>,
    headers: &BTreeMap<String, String>,
    body: Option<&serde_json::Value>,
) -> String {
    let mut parts = vec!["curl".to_string()];
    if let Some(url) = url {
        parts.push(admin_usage_curl_shell_quote(url));
    }
    parts.push("-X POST".to_string());
    for (key, value) in headers {
        parts.push(format!(
            "-H {}",
            admin_usage_curl_shell_quote(&format!("{key}: {value}"))
        ));
    }
    if let Some(body) = body {
        parts.push(format!(
            "-d {}",
            admin_usage_curl_shell_quote(&body.to_string())
        ));
    }
    parts.join(" \\\n  ")
}

fn admin_usage_total_tokens(item: &aether_data::repository::usage::StoredRequestUsageAudit) -> u64 {
    item.input_tokens
        .saturating_add(item.output_tokens)
        .saturating_add(item.cache_creation_input_tokens)
        .saturating_add(item.cache_read_input_tokens)
}

fn admin_usage_parse_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        None => Ok(100),
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be a positive integer".to_string())?;
            if parsed == 0 || parsed > 500 {
                return Err("limit must be between 1 and 500".to_string());
            }
            Ok(parsed)
        }
    }
}

fn admin_usage_parse_offset(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "offset") {
        None => Ok(0),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a non-negative integer".to_string()),
    }
}

fn admin_usage_parse_ids(query: Option<&str>) -> Option<BTreeSet<String>> {
    let ids = query_param_value(query, "ids")?;
    let parsed: BTreeSet<String> = ids
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    Some(parsed)
}

fn admin_usage_parse_recent_hours(query: Option<&str>, default: u32) -> Result<u32, String> {
    match query_param_value(query, "hours") {
        Some(value) => parse_bounded_u32("hours", &value, 1, 720),
        None => Ok(default),
    }
}

fn admin_usage_parse_timeline_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be an integer between 100 and 50000".to_string())?;
            if (100..=50_000).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("limit must be an integer between 100 and 50000".to_string())
            }
        }
        None => Ok(3_000),
    }
}

fn admin_usage_parse_aggregation_limit(query: Option<&str>) -> Result<usize, String> {
    match query_param_value(query, "limit") {
        Some(value) => {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| "limit must be an integer between 1 and 100".to_string())?;
            if (1..=100).contains(&parsed) {
                Ok(parsed)
            } else {
                Err("limit must be an integer between 1 and 100".to_string())
            }
        }
        None => Ok(20),
    }
}

fn admin_usage_token_cache_hit_rate(input_tokens: u64, cache_read_tokens: u64) -> f64 {
    let total_input_context = input_tokens.saturating_add(cache_read_tokens);
    if total_input_context == 0 {
        0.0
    } else {
        round_to(
            cache_read_tokens as f64 / total_input_context as f64 * 100.0,
            2,
        )
    }
}

fn admin_usage_aggregation_by_model_json(
    usage: &[aether_data::repository::usage::StoredRequestUsageAudit],
    limit: usize,
) -> serde_json::Value {
    let mut grouped: BTreeMap<String, (u64, u64, u64, u64, f64, f64)> = BTreeMap::new();
    for item in usage {
        let key = item.model.clone();
        let entry = grouped.entry(key).or_insert((0, 0, 0, 0, 0.0, 0.0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(item.total_tokens);
        entry.2 = entry.2.saturating_add(item.input_tokens);
        entry.3 = entry.3.saturating_add(item.cache_read_input_tokens);
        entry.4 += item.total_cost_usd;
        entry.5 += item.actual_total_cost_usd;
    }

    let mut items: Vec<serde_json::Value> = grouped
        .into_iter()
        .map(
            |(model, (request_count, total_tokens, input_tokens, cache_read_tokens, total_cost, actual_cost))| {
                json!({
                    "model": model,
                    "request_count": request_count,
                    "total_tokens": total_tokens,
                    "total_input_context": input_tokens.saturating_add(cache_read_tokens),
                    "output_tokens": total_tokens.saturating_sub(input_tokens),
                    "total_cost": round_to(total_cost, 6),
                    "actual_cost": round_to(actual_cost, 6),
                    "cache_read_tokens": cache_read_tokens,
                    "cache_creation_tokens": 0,
                    "cache_hit_rate": admin_usage_token_cache_hit_rate(input_tokens, cache_read_tokens),
                })
            },
        )
        .collect();
    items.sort_by(|left, right| {
        right["request_count"]
            .as_u64()
            .unwrap_or_default()
            .cmp(&left["request_count"].as_u64().unwrap_or_default())
            .then_with(|| {
                left["model"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["model"].as_str().unwrap_or_default())
            })
    });
    items.truncate(limit);
    json!(items)
}

async fn admin_usage_aggregation_by_user_json(
    state: &AppState,
    usage: &[aether_data::repository::usage::StoredRequestUsageAudit],
    limit: usize,
) -> Result<serde_json::Value, GatewayError> {
    let mut grouped: BTreeMap<String, (u64, u64, f64)> = BTreeMap::new();
    for item in usage {
        let Some(user_id) = item.user_id.as_ref() else {
            continue;
        };
        let entry = grouped.entry(user_id.clone()).or_insert((0, 0, 0.0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(item.total_tokens);
        entry.2 += item.total_cost_usd;
    }

    let usernames = if state.has_user_data_reader() && !grouped.is_empty() {
        state
            .list_users_by_ids(&grouped.keys().cloned().collect::<Vec<_>>())
            .await?
            .into_iter()
            .map(|user| (user.id, (user.email, user.username)))
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };

    let mut items: Vec<serde_json::Value> = grouped
        .into_iter()
        .map(|(user_id, (request_count, total_tokens, total_cost))| {
            let (email, username) = usernames
                .get(&user_id)
                .cloned()
                .unwrap_or((None, String::new()));
            json!({
                "user_id": user_id,
                "email": email,
                "username": if username.is_empty() { serde_json::Value::Null } else { json!(username) },
                "request_count": request_count,
                "total_tokens": total_tokens,
                "total_cost": round_to(total_cost, 6),
            })
        })
        .collect();
    items.sort_by(|left, right| {
        right["request_count"]
            .as_u64()
            .unwrap_or_default()
            .cmp(&left["request_count"].as_u64().unwrap_or_default())
            .then_with(|| {
                left["user_id"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["user_id"].as_str().unwrap_or_default())
            })
    });
    items.truncate(limit);
    Ok(json!(items))
}

fn admin_usage_aggregation_by_provider_json(
    usage: &[aether_data::repository::usage::StoredRequestUsageAudit],
    limit: usize,
) -> serde_json::Value {
    let mut grouped: BTreeMap<String, (u64, u64, u64, u64, f64, f64, u64, u64)> = BTreeMap::new();
    for item in usage {
        let key = item
            .provider_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let entry = grouped.entry(key).or_insert((0, 0, 0, 0, 0.0, 0.0, 0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(item.total_tokens);
        entry.2 = entry.2.saturating_add(item.input_tokens);
        entry.3 = entry.3.saturating_add(item.cache_read_input_tokens);
        entry.4 += item.total_cost_usd;
        entry.5 += item.actual_total_cost_usd;
        entry.6 = entry
            .6
            .saturating_add(item.response_time_ms.unwrap_or_default());
        entry.7 = entry
            .7
            .saturating_add(if admin_usage_is_success(item) { 1 } else { 0 });
    }

    let mut items: Vec<serde_json::Value> = grouped
        .into_iter()
        .map(
            |(provider_id, (request_count, total_tokens, input_tokens, cache_read_tokens, total_cost, actual_cost, response_time_ms_sum, success_count))| {
                let avg_response_time_ms = if request_count == 0 {
                    0.0
                } else {
                    round_to(response_time_ms_sum as f64 / request_count as f64, 2)
                };
                let error_count = request_count.saturating_sub(success_count);
                let success_rate = if request_count == 0 {
                    0.0
                } else {
                    round_to(success_count as f64 / request_count as f64 * 100.0, 2)
                };
                json!({
                    "provider_id": provider_id,
                    "provider": serde_json::Value::Null,
                    "request_count": request_count,
                    "total_tokens": total_tokens,
                    "total_input_context": input_tokens.saturating_add(cache_read_tokens),
                    "output_tokens": total_tokens.saturating_sub(input_tokens),
                    "total_cost": round_to(total_cost, 6),
                    "actual_cost": round_to(actual_cost, 6),
                    "avg_response_time_ms": avg_response_time_ms,
                    "success_rate": success_rate,
                    "error_count": error_count,
                    "cache_read_tokens": cache_read_tokens,
                    "cache_creation_tokens": 0,
                    "cache_hit_rate": admin_usage_token_cache_hit_rate(input_tokens, cache_read_tokens),
                })
            },
        )
        .collect();
    items.sort_by(|left, right| {
        right["request_count"]
            .as_u64()
            .unwrap_or_default()
            .cmp(&left["request_count"].as_u64().unwrap_or_default())
            .then_with(|| {
                left["provider_id"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["provider_id"].as_str().unwrap_or_default())
            })
    });
    items.truncate(limit);
    json!(items)
}

fn admin_usage_aggregation_by_api_format_json(
    usage: &[aether_data::repository::usage::StoredRequestUsageAudit],
    limit: usize,
) -> serde_json::Value {
    let mut grouped: BTreeMap<String, (u64, u64, u64, u64, f64, f64, u64)> = BTreeMap::new();
    for item in usage {
        let key = item
            .api_format
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let entry = grouped.entry(key).or_insert((0, 0, 0, 0, 0.0, 0.0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(item.total_tokens);
        entry.2 = entry.2.saturating_add(item.input_tokens);
        entry.3 = entry.3.saturating_add(item.cache_read_input_tokens);
        entry.4 += item.total_cost_usd;
        entry.5 += item.actual_total_cost_usd;
        entry.6 = entry
            .6
            .saturating_add(item.response_time_ms.unwrap_or_default());
    }

    let mut items: Vec<serde_json::Value> = grouped
        .into_iter()
        .map(
            |(api_format, (request_count, total_tokens, input_tokens, cache_read_tokens, total_cost, actual_cost, response_time_ms_sum))| {
                let avg_response_time_ms = if request_count == 0 {
                    0.0
                } else {
                    round_to(response_time_ms_sum as f64 / request_count as f64, 2)
                };
                json!({
                    "api_format": api_format,
                    "request_count": request_count,
                    "total_tokens": total_tokens,
                    "total_input_context": input_tokens.saturating_add(cache_read_tokens),
                    "output_tokens": total_tokens.saturating_sub(input_tokens),
                    "total_cost": round_to(total_cost, 6),
                    "actual_cost": round_to(actual_cost, 6),
                    "avg_response_time_ms": avg_response_time_ms,
                    "cache_read_tokens": cache_read_tokens,
                    "cache_creation_tokens": 0,
                    "cache_hit_rate": admin_usage_token_cache_hit_rate(input_tokens, cache_read_tokens),
                })
            },
        )
        .collect();
    items.sort_by(|left, right| {
        right["request_count"]
            .as_u64()
            .unwrap_or_default()
            .cmp(&left["request_count"].as_u64().unwrap_or_default())
            .then_with(|| {
                left["api_format"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["api_format"].as_str().unwrap_or_default())
            })
    });
    items.truncate(limit);
    json!(items)
}

fn admin_usage_heatmap_json(
    usage: &[aether_data::repository::usage::StoredRequestUsageAudit],
) -> serde_json::Value {
    let mut grouped: BTreeMap<String, (u64, u64, f64, f64, u64, u64)> = BTreeMap::new();
    for item in usage {
        let Ok(created_at_unix_secs) = i64::try_from(item.created_at_unix_secs) else {
            continue;
        };
        let Some(created_at) =
            chrono::DateTime::<chrono::Utc>::from_timestamp(created_at_unix_secs, 0)
        else {
            continue;
        };
        let date_key = created_at.format("%Y-%m-%d").to_string();
        let entry = grouped.entry(date_key).or_insert((0, 0, 0.0, 0.0, 0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(item.total_tokens);
        entry.2 += item.total_cost_usd;
        entry.3 += item.actual_total_cost_usd;
        entry.4 = entry.4.saturating_add(item.cache_read_input_tokens);
        entry.5 = entry.5.saturating_add(item.cache_creation_input_tokens);
    }
    let items = grouped
        .into_iter()
        .map(
            |(
                date,
                (
                    request_count,
                    total_tokens,
                    total_cost,
                    actual_total_cost,
                    cache_read_tokens,
                    cache_creation_tokens,
                ),
            )| {
                json!({
                    "date": date,
                    "request_count": request_count,
                    "total_tokens": total_tokens,
                    "total_cost": round_to(total_cost, 6),
                    "actual_total_cost": round_to(actual_total_cost, 6),
                    "cache_read_tokens": cache_read_tokens,
                    "cache_creation_tokens": cache_creation_tokens,
                })
            },
        )
        .collect::<Vec<_>>();
    json!(items)
}

fn admin_usage_is_success(item: &aether_data::repository::usage::StoredRequestUsageAudit) -> bool {
    matches!(
        item.status.as_str(),
        "completed" | "success" | "ok" | "billed" | "settled"
    ) && item.status_code.is_none_or(|code| code < 400)
}

fn admin_usage_matches_optional_id(value: Option<&str>, expected: Option<&str>) -> bool {
    let Some(expected) = expected.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    value.is_some_and(|candidate| candidate == expected)
}

async fn list_recent_completed_usage_for_cache_affinity(
    state: &AppState,
    hours: u32,
    user_id: Option<&str>,
) -> Result<Vec<aether_data::repository::usage::StoredRequestUsageAudit>, GatewayError> {
    let now_unix_secs = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
    let created_from_unix_secs = now_unix_secs.saturating_sub(u64::from(hours) * 3600);
    let mut items = state
        .list_usage_audits(&aether_data::repository::usage::UsageAuditListQuery {
            created_from_unix_secs: Some(created_from_unix_secs),
            created_until_unix_secs: None,
            user_id: user_id.map(ToOwned::to_owned),
            provider_name: None,
            model: None,
        })
        .await?;
    items.retain(|item| item.status == "completed");
    items.sort_by(|left, right| {
        left.created_at_unix_secs
            .cmp(&right.created_at_unix_secs)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(items)
}

fn admin_usage_group_completed_by_user(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
) -> BTreeMap<String, Vec<aether_data::repository::usage::StoredRequestUsageAudit>> {
    let mut grouped = BTreeMap::new();
    for item in items.iter().filter(|item| item.user_id.is_some()) {
        grouped
            .entry(item.user_id.clone().unwrap_or_default())
            .or_insert_with(Vec::new)
            .push(item.clone());
    }
    grouped
}

fn admin_usage_group_completed_by_api_key(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
    api_key_id: Option<&str>,
) -> BTreeMap<String, Vec<aether_data::repository::usage::StoredRequestUsageAudit>> {
    let mut grouped = BTreeMap::new();
    for item in items.iter().filter(|item| item.api_key_id.is_some()) {
        if !admin_usage_matches_optional_id(item.api_key_id.as_deref(), api_key_id) {
            continue;
        }
        grouped
            .entry(item.api_key_id.clone().unwrap_or_default())
            .or_insert_with(Vec::new)
            .push(item.clone());
    }
    grouped
}

fn admin_usage_collect_request_intervals_minutes(
    items: &[aether_data::repository::usage::StoredRequestUsageAudit],
) -> Vec<f64> {
    let mut previous_created_at_unix_secs = None;
    let mut intervals = Vec::new();
    for item in items {
        if let Some(previous) = previous_created_at_unix_secs {
            intervals.push(item.created_at_unix_secs.saturating_sub(previous) as f64 / 60.0);
        }
        previous_created_at_unix_secs = Some(item.created_at_unix_secs);
    }
    intervals
}

fn admin_usage_percentile_cont(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(values[0]);
    }
    let position = percentile.clamp(0.0, 1.0) * (values.len() - 1) as f64;
    let lower_index = position.floor() as usize;
    let upper_index = position.ceil() as usize;
    let lower = values[lower_index];
    let upper = values[upper_index];
    Some(lower + (upper - lower) * (position - lower_index as f64))
}

fn admin_usage_calculate_recommended_ttl(
    p75_interval: Option<f64>,
    p90_interval: Option<f64>,
) -> u64 {
    let Some(p75_interval) = p75_interval else {
        return 5;
    };
    let Some(p90_interval) = p90_interval else {
        return 5;
    };

    if p90_interval <= 5.0 {
        5
    } else if p75_interval <= 15.0 {
        15
    } else if p75_interval <= 30.0 {
        30
    } else {
        60
    }
}

fn admin_usage_ttl_recommendation_reason(
    ttl: u64,
    p75_interval: Option<f64>,
    p90_interval: Option<f64>,
) -> String {
    let Some(p75_interval) = p75_interval else {
        return "数据不足，使用默认值".to_string();
    };
    let Some(p90_interval) = p90_interval else {
        return "数据不足，使用默认值".to_string();
    };

    match ttl {
        5 => format!("高频用户：90% 的请求间隔在 {:.1} 分钟内", p90_interval),
        15 => format!("中高频用户：75% 的请求间隔在 {:.1} 分钟内", p75_interval),
        30 => format!("中频用户：75% 的请求间隔在 {:.1} 分钟内", p75_interval),
        _ => format!(
            "低频用户：75% 的请求间隔为 {:.1} 分钟，建议使用长 TTL",
            p75_interval
        ),
    }
}

fn admin_usage_proportional_limits(
    grouped: &BTreeMap<String, Vec<serde_json::Value>>,
    limit: usize,
    total_points: usize,
) -> BTreeMap<String, usize> {
    let mut limits = BTreeMap::new();
    for (group_id, items) in grouped {
        let computed = if total_points <= limit || total_points == 0 {
            items.len()
        } else {
            let scaled =
                ((items.len() as f64 * limit as f64) / total_points as f64).ceil() as usize;
            std::cmp::max(scaled, 1)
        };
        limits.insert(group_id.clone(), computed);
    }
    limits
}

fn admin_usage_point_sort_key(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> std::cmp::Ordering {
    left["x"]
        .as_str()
        .unwrap_or_default()
        .cmp(right["x"].as_str().unwrap_or_default())
        .then_with(|| {
            left["user_id"]
                .as_str()
                .unwrap_or_default()
                .cmp(right["user_id"].as_str().unwrap_or_default())
        })
}

fn admin_usage_matches_search(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    search: Option<&str>,
) -> bool {
    let Some(search) = search.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let haystack = [
        item.username.as_deref(),
        item.api_key_name.as_deref(),
        Some(item.model.as_str()),
        Some(item.provider_name.as_str()),
    ];
    search.split_whitespace().all(|keyword| {
        let keyword = keyword.to_ascii_lowercase();
        haystack
            .iter()
            .flatten()
            .any(|value| value.to_ascii_lowercase().contains(keyword.as_str()))
    })
}

fn admin_usage_matches_username(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    username: Option<&str>,
) -> bool {
    let Some(username) = username.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    item.username
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains(username.to_ascii_lowercase().as_str())
}

fn admin_usage_matches_eq(value: &str, query: Option<&str>) -> bool {
    let Some(query) = query
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
    else {
        return true;
    };
    value.eq_ignore_ascii_case(query)
}

fn admin_usage_matches_api_format(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    api_format: Option<&str>,
) -> bool {
    let Some(api_format) = api_format.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    item.api_format
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case(api_format))
}

fn admin_usage_matches_status(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    status: Option<&str>,
) -> bool {
    let Some(status) = status.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    match status {
        "stream" => item.is_stream,
        "standard" => !item.is_stream,
        "error" => {
            item.status_code.is_some_and(|value| value >= 400) || item.error_message.is_some()
        }
        "pending" | "streaming" | "completed" | "cancelled" => item.status == status,
        "failed" => {
            item.status == "failed"
                || item.status_code.is_some_and(|value| value >= 400)
                || item.error_message.is_some()
        }
        "active" => matches!(item.status.as_str(), "pending" | "streaming"),
        _ => true,
    }
}

fn admin_usage_record_json(
    item: &aether_data::repository::usage::StoredRequestUsageAudit,
    users_by_id: &BTreeMap<String, aether_data::repository::users::StoredUserSummary>,
) -> Value {
    let user = item
        .user_id
        .as_ref()
        .and_then(|user_id| users_by_id.get(user_id));
    let username = user
        .map(|value| value.username.clone())
        .or_else(|| item.username.clone())
        .unwrap_or_else(|| "已删除用户".to_string());
    let user_email = user
        .and_then(|value| value.email.clone())
        .unwrap_or_else(|| "已删除用户".to_string());

    json!({
        "id": item.id,
        "user_id": item.user_id,
        "user_email": user_email,
        "username": username,
        "api_key": item.api_key_id.as_ref().map(|api_key_id| json!({
            "id": api_key_id,
            "name": item.api_key_name.clone(),
            "display": item.api_key_name.clone().unwrap_or_else(|| api_key_id.clone()),
        })),
        "provider": item.provider_name,
        "model": item.model,
        "target_model": item.target_model,
        "input_tokens": item.input_tokens,
        "output_tokens": item.output_tokens,
        "cache_creation_input_tokens": item.cache_creation_input_tokens,
        "cache_read_input_tokens": item.cache_read_input_tokens,
        "total_tokens": admin_usage_total_tokens(item),
        "cost": round_to(item.total_cost_usd, 6),
        "actual_cost": round_to(item.actual_total_cost_usd, 6),
        "rate_multiplier": Value::Null,
        "response_time_ms": item.response_time_ms,
        "first_byte_time_ms": item.first_byte_time_ms,
        "created_at": unix_secs_to_rfc3339(item.created_at_unix_secs),
        "is_stream": item.is_stream,
        "input_price_per_1m": Value::Null,
        "output_price_per_1m": item.output_price_per_1m,
        "cache_creation_price_per_1m": Value::Null,
        "cache_read_price_per_1m": Value::Null,
        "status_code": item.status_code,
        "error_message": item.error_message,
        "status": item.status,
        "has_fallback": false,
        "has_retry": false,
        "has_rectified": false,
        "api_format": item.api_format,
        "endpoint_api_format": item.endpoint_api_format,
        "has_format_conversion": item.has_format_conversion,
        "api_key_name": item.api_key_name,
        "model_version": Value::Null,
    })
}
