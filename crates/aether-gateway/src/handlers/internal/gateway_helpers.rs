fn build_retired_internal_gateway_response() -> Response<Body> {
    (
        http::StatusCode::GONE,
        Json(json!({
            "detail": "legacy internal gateway route removed; use public proxy",
        })),
    )
        .into_response()
}

fn build_internal_gateway_passthrough_payload(uri: &http::Uri) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert("action".to_string(), json!("proxy_public"));
    payload.insert("route_class".to_string(), json!("passthrough"));
    payload.insert("public_path".to_string(), json!(uri.path()));
    payload.insert("executor_candidate".to_string(), json!(false));
    if let Some(query) = uri.query().filter(|value| !value.is_empty()) {
        payload.insert("public_query_string".to_string(), json!(query));
    }
    serde_json::Value::Object(payload)
}

fn build_internal_gateway_resolve_payload(decision: GatewayControlDecision) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert("action".to_string(), json!("proxy_public"));
    payload.insert("route_class".to_string(), json!(decision.route_class));
    payload.insert("public_path".to_string(), json!(decision.public_path));
    payload.insert(
        "executor_candidate".to_string(),
        json!(decision.executor_candidate),
    );
    if let Some(query) = decision.public_query_string {
        payload.insert("public_query_string".to_string(), json!(query));
    }
    if let Some(route_family) = decision.route_family {
        payload.insert("route_family".to_string(), json!(route_family));
    }
    if let Some(route_kind) = decision.route_kind {
        payload.insert("route_kind".to_string(), json!(route_kind));
    }
    if let Some(signature) = decision.auth_endpoint_signature {
        payload.insert("auth_endpoint_signature".to_string(), json!(signature));
    }
    if let Some(auth_context) = decision.auth_context {
        payload.insert(
            "auth_context".to_string(),
            serde_json::to_value(auth_context).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Object(payload)
}

fn build_internal_gateway_fallback_plan_payload(
    auth_context: Option<&crate::gateway::GatewayControlAuthContext>,
) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert("action".to_string(), json!("fallback_plan"));
    if let Some(auth_context) = auth_context {
        payload.insert(
            "auth_context".to_string(),
            serde_json::to_value(auth_context).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Object(payload)
}

fn build_internal_gateway_proxy_public_response() -> Response<Body> {
    (
        http::StatusCode::CONFLICT,
        [(CONTROL_ACTION_HEADER, CONTROL_ACTION_PROXY_PUBLIC)],
        Json(json!({ "action": CONTROL_ACTION_PROXY_PUBLIC })),
    )
        .into_response()
}

fn attach_execution_path_header(
    mut response: Response<Body>,
    execution_path: &'static str,
) -> Response<Body> {
    response.headers_mut().insert(
        HeaderName::from_static(EXECUTION_PATH_HEADER),
        HeaderValue::from_static(execution_path),
    );
    response
}

fn resolve_local_proxy_execution_path(
    response: &Response<Body>,
    default_execution_path: &'static str,
) -> &'static str {
    match response
        .headers()
        .get(EXECUTION_PATH_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(EXECUTION_PATH_EXECUTOR_SYNC) => EXECUTION_PATH_EXECUTOR_SYNC,
        Some(EXECUTION_PATH_EXECUTOR_STREAM) => EXECUTION_PATH_EXECUTOR_STREAM,
        Some(EXECUTION_PATH_CONTROL_EXECUTE_SYNC) => EXECUTION_PATH_CONTROL_EXECUTE_SYNC,
        Some(EXECUTION_PATH_CONTROL_EXECUTE_STREAM) => EXECUTION_PATH_CONTROL_EXECUTE_STREAM,
        Some(EXECUTION_PATH_LOCAL_AUTH_DENIED) => EXECUTION_PATH_LOCAL_AUTH_DENIED,
        Some(EXECUTION_PATH_LOCAL_RATE_LIMITED) => EXECUTION_PATH_LOCAL_RATE_LIMITED,
        Some(EXECUTION_PATH_LOCAL_OVERLOADED) => EXECUTION_PATH_LOCAL_OVERLOADED,
        Some(EXECUTION_PATH_DISTRIBUTED_OVERLOADED) => EXECUTION_PATH_DISTRIBUTED_OVERLOADED,
        Some(EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS) => {
            EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS
        }
        Some(EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH) => EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH,
        _ => default_execution_path,
    }
}

fn decode_legacy_gateway_request_body_bytes(
    payload: &LegacyGatewayExecuteRequest,
) -> Result<Bytes, Response<Body>> {
    if let Some(body_base64) = payload.body_base64.as_deref() {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body_base64)
            .map_err(|_| {
                build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid internal gateway body payload",
                )
            })?;
        return Ok(Bytes::from(decoded));
    }

    let body_json = payload.body_json.clone();
    if body_json.is_null() {
        return Ok(Bytes::new());
    }
    if body_json
        .as_object()
        .map(|value| value.is_empty())
        .unwrap_or(false)
    {
        return Ok(Bytes::new());
    }

    let encoded = serde_json::to_vec(&body_json).map_err(|_| {
        build_internal_hub_error_response(
            http::StatusCode::BAD_REQUEST,
            "invalid internal gateway body payload",
        )
    })?;
    Ok(Bytes::from(encoded))
}

fn build_internal_gateway_header_map(
    headers: &BTreeMap<String, String>,
) -> Result<http::HeaderMap, Response<Body>> {
    let mut mapped = http::HeaderMap::new();
    for (name, value) in headers {
        let header_name = match HeaderName::from_bytes(name.as_bytes()) {
            Ok(name) => name,
            Err(_) => {
                return Err(build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid internal gateway header",
                ));
            }
        };
        let header_value = match HeaderValue::from_str(value) {
            Ok(value) => value,
            Err(_) => {
                return Err(build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid internal gateway header",
                ));
            }
        };
        mapped.append(header_name, header_value);
    }
    Ok(mapped)
}

fn build_internal_gateway_request_parts(
    method: &str,
    path: &str,
    query_string: Option<&str>,
    headers: &BTreeMap<String, String>,
) -> Result<http::request::Parts, Response<Body>> {
    let mapped_headers = build_internal_gateway_header_map(headers)?;
    let method = match http::Method::from_bytes(method.as_bytes()) {
        Ok(method) => method,
        Err(_) => {
            return Err(build_internal_hub_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid internal gateway method",
            ));
        }
    };
    let uri = build_internal_gateway_uri(path, query_string)?;
    let request = match http::Request::builder().method(method).uri(uri).body(()) {
        Ok(request) => request,
        Err(_) => {
            return Err(build_internal_hub_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid internal gateway request",
            ));
        }
    };
    let (mut parts, _) = request.into_parts();
    parts.headers = mapped_headers;
    Ok(parts)
}

fn build_internal_gateway_uri(
    path: &str,
    query_string: Option<&str>,
) -> Result<http::Uri, Response<Body>> {
    let normalized_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let uri_text = if let Some(query) = query_string.filter(|value| !value.is_empty()) {
        format!("{normalized_path}?{query}")
    } else {
        normalized_path
    };
    uri_text.parse::<http::Uri>().map_err(|_| {
        build_internal_hub_error_response(
            http::StatusCode::BAD_REQUEST,
            "invalid internal gateway uri",
        )
    })
}

fn infer_legacy_finalize_signature(
    payload: &crate::gateway::executor::GatewaySyncReportRequest,
) -> Option<String> {
    let report_context = payload.report_context.as_ref()?;
    let from_context = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .or_else(|| {
            report_context
                .get("provider_api_format")
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if from_context.is_some() {
        return from_context;
    }

    let report_kind = payload.report_kind.trim().to_ascii_lowercase();
    if report_kind.starts_with("openai_chat_") {
        return Some("openai:chat".to_string());
    }
    if report_kind.starts_with("openai_compact_") {
        return Some("openai:compact".to_string());
    }
    if report_kind.starts_with("openai_cli_") {
        return Some("openai:cli".to_string());
    }
    if report_kind.starts_with("openai_video_") {
        return Some("openai:video".to_string());
    }
    if report_kind.starts_with("claude_chat_") {
        return Some("claude:chat".to_string());
    }
    if report_kind.starts_with("claude_cli_") {
        return Some("claude:cli".to_string());
    }
    if report_kind.starts_with("gemini_chat_") {
        return Some("gemini:chat".to_string());
    }
    if report_kind.starts_with("gemini_cli_") {
        return Some("gemini:cli".to_string());
    }
    if report_kind.starts_with("gemini_video_") {
        return Some("gemini:video".to_string());
    }
    None
}

fn build_legacy_finalize_decision(
    payload: &crate::gateway::executor::GatewaySyncReportRequest,
) -> Option<GatewayControlDecision> {
    let signature = infer_legacy_finalize_signature(payload)?;
    let (public_path, route_family, route_kind) = match signature.as_str() {
        "openai:chat" => ("/v1/chat/completions", "openai", "chat"),
        "openai:cli" => ("/v1/responses", "openai", "cli"),
        "openai:compact" => ("/v1/responses/compact", "openai", "compact"),
        "openai:video" => ("/v1/videos", "openai", "video"),
        "claude:chat" => ("/v1/messages", "claude", "chat"),
        "claude:cli" => ("/v1/messages", "claude", "cli"),
        "gemini:chat" => ("/v1beta/models", "gemini", "chat"),
        "gemini:cli" => ("/v1beta/models", "gemini", "cli"),
        "gemini:video" => ("/v1beta/models", "gemini", "video"),
        _ => return None,
    };
    Some(GatewayControlDecision {
        public_path: public_path.to_string(),
        public_query_string: None,
        route_class: Some("ai_public".to_string()),
        route_family: Some(route_family.to_string()),
        route_kind: Some(route_kind.to_string()),
        auth_endpoint_signature: Some(signature),
        executor_candidate: true,
        auth_context: None,
        admin_principal: None,
        local_auth_rejection: None,
    })
}

fn build_legacy_finalize_video_plan(
    payload: &crate::gateway::executor::GatewaySyncReportRequest,
) -> Option<aether_contracts::ExecutionPlan> {
    let signature = infer_legacy_finalize_signature(payload)?;
    if !matches!(signature.as_str(), "openai:video" | "gemini:video") {
        return None;
    }

    let report_context = payload.report_context.as_ref().and_then(Value::as_object);
    let context_text = |key: &str| {
        report_context
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    };

    let provider_name = signature
        .split(':')
        .next()
        .expect("video finalize signature should include provider name")
        .to_string();
    let model_name = context_text("model")
        .or_else(|| context_text("model_name"))
        .or_else(|| match signature.as_str() {
            "openai:video" => Some("sora-2".to_string()),
            "gemini:video" => Some("veo-3".to_string()),
            _ => None,
        });
    let original_request_body = report_context
        .and_then(|value| value.get("original_request_body"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let url = match signature.as_str() {
        "openai:video" => "https://legacy.internal.invalid/v1/videos".to_string(),
        "gemini:video" => format!(
            "https://legacy.internal.invalid/v1beta/models/{}:predictLongRunning",
            model_name
                .clone()
                .unwrap_or_else(|| "veo-3".to_string())
        ),
        _ => return None,
    };

    Some(aether_contracts::ExecutionPlan {
        request_id: context_text("request_id").unwrap_or_else(|| payload.trace_id.clone()),
        candidate_id: None,
        provider_name: Some(provider_name.clone()),
        provider_id: context_text("provider_id")
            .unwrap_or_else(|| format!("legacy-{provider_name}-video-provider")),
        endpoint_id: context_text("endpoint_id")
            .unwrap_or_else(|| format!("legacy-{provider_name}-video-endpoint")),
        key_id: context_text("key_id")
            .or_else(|| context_text("api_key_id"))
            .unwrap_or_else(|| format!("legacy-{provider_name}-video-key")),
        method: "POST".to_string(),
        url,
        headers: std::collections::BTreeMap::from([(
            "authorization".to_string(),
            "Bearer legacy-internal-gateway".to_string(),
        )]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: aether_contracts::RequestBody::from_json(original_request_body),
        stream: false,
        client_api_format: signature.clone(),
        provider_api_format: signature,
        model_name,
        proxy: Some(aether_contracts::ProxySnapshot {
            enabled: Some(false),
            mode: Some("direct".to_string()),
            node_id: None,
            label: None,
            url: None,
            extra: None,
        }),
        tls_profile: None,
        timeouts: None,
    })
}

fn build_legacy_finalize_video_request_path(
    payload: &crate::gateway::executor::GatewaySyncReportRequest,
) -> Option<String> {
    let signature = infer_legacy_finalize_signature(payload)?;
    let report_context = payload.report_context.as_ref().and_then(Value::as_object);
    let context_text = |key: &str| {
        report_context
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    };

    match payload.report_kind.as_str() {
        "openai_video_delete_sync_finalize" => {
            let task_id = context_text("task_id")?;
            Some(format!("/v1/videos/{task_id}"))
        }
        "openai_video_cancel_sync_finalize" => {
            let task_id = context_text("task_id")?;
            Some(format!("/v1/videos/{task_id}/cancel"))
        }
        "gemini_video_cancel_sync_finalize" => {
            let short_id = context_text("task_id")
                .or_else(|| context_text("local_short_id"))
                .or_else(|| {
                    context_text("operation_name").and_then(|value| {
                        value
                            .rsplit('/')
                            .next()
                            .map(str::trim)
                            .filter(|inner| !inner.is_empty())
                            .map(ToOwned::to_owned)
                    })
                })?;
            let model = context_text("model")
                .or_else(|| context_text("model_name"))
                .unwrap_or_else(|| match signature.as_str() {
                    "gemini:video" => "veo-3".to_string(),
                    _ => "unknown".to_string(),
                });
            Some(format!("/v1beta/models/{model}/operations/{short_id}:cancel"))
        }
        _ => None,
    }
}

async fn maybe_build_legacy_finalize_video_response(
    state: &AppState,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &crate::gateway::executor::GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(plan) = build_legacy_finalize_video_plan(payload) else {
        return Ok(None);
    };

    if let Some(outcome) = crate::gateway::executor::maybe_build_local_video_success_outcome(
        trace_id,
        decision,
        payload,
        &state.video_tasks,
        &plan,
    )? {
        if let Some(snapshot) = outcome.local_task_snapshot.clone() {
            state.video_tasks.record_snapshot(snapshot.clone());
            let _ = state.upsert_video_task_snapshot(&snapshot).await?;
        }
        match outcome.report_mode {
            crate::gateway::video_tasks::VideoTaskSyncReportMode::InlineSync => {
                crate::gateway::usage::submit_sync_report(state, "", trace_id, outcome.report_payload)
                    .await?;
            }
            crate::gateway::video_tasks::VideoTaskSyncReportMode::Background => {
                crate::gateway::usage::spawn_sync_report(
                    state.clone(),
                    String::new(),
                    trace_id.to_string(),
                    outcome.report_payload,
                );
            }
        }
        let mut response = outcome.response;
        response.headers_mut().insert(
            HeaderName::from_static(CONTROL_EXECUTED_HEADER),
            HeaderValue::from_static("true"),
        );
        return Ok(Some(response));
    }

    if let Some(mut response) = crate::gateway::executor::maybe_build_local_sync_finalize_response(
        trace_id,
        decision,
        payload,
    )? {
        if let Some(request_path) = build_legacy_finalize_video_request_path(payload) {
            state
                .video_tasks
                .apply_finalize_mutation(request_path.as_str(), payload.report_kind.as_str());
            if let Some(snapshot) = state
                .video_tasks
                .snapshot_for_route(decision.route_family.as_deref(), request_path.as_str())
            {
                let _ = state.upsert_video_task_snapshot(&snapshot).await?;
            }
        }
        if let Some(success_report_kind) =
            crate::gateway::executor::resolve_local_sync_success_background_report_kind(
                payload.report_kind.as_str(),
            )
        {
            let mut report_payload = payload.clone();
            report_payload.report_kind = success_report_kind;
            crate::gateway::usage::spawn_sync_report(
                state.clone(),
                String::new(),
                trace_id.to_string(),
                report_payload,
            );
        }
        response.headers_mut().insert(
            HeaderName::from_static(CONTROL_EXECUTED_HEADER),
            HeaderValue::from_static("true"),
        );
        return Ok(Some(response));
    }

    if let Some(mut response) =
        crate::gateway::executor::maybe_build_local_video_error_response(trace_id, decision, payload)?
    {
        if let Some(error_report_kind) =
            crate::gateway::executor::resolve_local_sync_error_background_report_kind(
                payload.report_kind.as_str(),
            )
        {
            let mut report_payload = payload.clone();
            report_payload.report_kind = error_report_kind;
            crate::gateway::usage::spawn_sync_report(
                state.clone(),
                String::new(),
                trace_id.to_string(),
                report_payload,
            );
        }
        response.headers_mut().insert(
            HeaderName::from_static(CONTROL_EXECUTED_HEADER),
            HeaderValue::from_static("true"),
        );
        return Ok(Some(response));
    }

    Ok(None)
}

fn gateway_error_message(error: GatewayError) -> String {
    match error {
        GatewayError::UpstreamUnavailable { message, .. }
        | GatewayError::ControlUnavailable { message, .. }
        | GatewayError::Internal(message) => message,
    }
}

fn build_internal_hub_heartbeat_ack(node: &StoredProxyNode) -> serde_json::Value {
    let Some(remote_config) = node.remote_config.as_ref() else {
        return json!({});
    };

    let mut payload = serde_json::Map::new();
    payload.insert("remote_config".to_string(), remote_config.clone());
    payload.insert("config_version".to_string(), json!(node.config_version));
    if let Some(upgrade_to) = remote_config
        .as_object()
        .and_then(|value| value.get("upgrade_to"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        payload.insert("upgrade_to".to_string(), json!(upgrade_to));
    }
    serde_json::Value::Object(payload)
}

fn parse_internal_hub_heartbeat_request(
    request_body: &[u8],
) -> Result<InternalHubHeartbeatRequest, Response<Body>> {
    let payload =
        serde_json::from_slice::<InternalHubHeartbeatRequest>(request_body).map_err(|_| {
            build_internal_hub_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid heartbeat payload",
            )
        })?;

    let node_id = payload.node_id.trim();
    if node_id.is_empty() || node_id.len() > 36 {
        return Err(build_internal_hub_error_response(
            http::StatusCode::BAD_REQUEST,
            "invalid heartbeat payload",
        ));
    }
    if payload
        .heartbeat_interval
        .is_some_and(|value| !(5..=600).contains(&value))
        || payload.active_connections.is_some_and(|value| value < 0)
        || payload.total_requests.is_some_and(|value| value < 0)
        || payload.avg_latency_ms.is_some_and(|value| value < 0.0)
        || payload.failed_requests.is_some_and(|value| value < 0)
        || payload.dns_failures.is_some_and(|value| value < 0)
        || payload.stream_errors.is_some_and(|value| value < 0)
        || payload
            .proxy_version
            .as_deref()
            .is_some_and(|value| value.chars().count() > 20)
        || payload
            .proxy_metadata
            .as_ref()
            .is_some_and(|value| !value.is_object())
    {
        return Err(build_internal_hub_error_response(
            http::StatusCode::BAD_REQUEST,
            "invalid heartbeat payload",
        ));
    }

    Ok(payload)
}

fn parse_internal_hub_node_status_request(
    request_body: &[u8],
) -> Result<InternalHubNodeStatusRequest, Response<Body>> {
    let payload =
        serde_json::from_slice::<InternalHubNodeStatusRequest>(request_body).map_err(|_| {
            build_internal_hub_error_response(
                http::StatusCode::BAD_REQUEST,
                "invalid node-status payload",
            )
        })?;

    let node_id = payload.node_id.trim();
    if node_id.is_empty() || node_id.len() > 36 || payload.conn_count < 0 {
        return Err(build_internal_hub_error_response(
            http::StatusCode::BAD_REQUEST,
            "invalid node-status payload",
        ));
    }

    Ok(payload)
}

fn build_management_token_user_payload(
    user: &StoredManagementTokenUserSummary,
) -> serde_json::Value {
    json!({
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "role": user.role,
    })
}

fn build_management_token_payload(
    token: &StoredManagementToken,
    user: Option<&StoredManagementTokenUserSummary>,
) -> serde_json::Value {
    let mut payload = json!({
        "id": token.id,
        "user_id": token.user_id,
        "name": token.name,
        "description": token.description,
        "token_display": token.token_display(),
        "allowed_ips": token.allowed_ips,
        "expires_at": token.expires_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "last_used_at": token.last_used_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "last_used_ip": token.last_used_ip,
        "usage_count": token.usage_count,
        "is_active": token.is_active,
        "created_at": token.created_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "updated_at": token.updated_at_unix_secs.and_then(unix_secs_to_rfc3339),
    });
    if let Some(user) = user {
        payload["user"] = build_management_token_user_payload(user);
    }
    payload
}
