async fn maybe_build_local_internal_proxy_response_impl(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    remote_addr: &std::net::SocketAddr,
    request_body: Option<&axum::body::Bytes>,
    legacy_internal_gateway_allowed: bool,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };
    if decision.route_class.as_deref() != Some("internal_proxy") {
        return Ok(None);
    }
    if decision.route_family.as_deref() == Some("gateway_legacy") {
        if !legacy_internal_gateway_allowed {
            return Ok(Some(build_retired_internal_gateway_response()));
        }
        state.record_fallback_metric(
            GatewayFallbackMetricKind::LegacyInternalBridge,
            Some(decision),
            None,
            Some("legacy_internal_bridge"),
            GatewayFallbackReason::LegacyInternalGateway,
        );
        if !remote_addr.ip().is_loopback() {
            return Ok(Some(build_internal_hub_error_response(
                http::StatusCode::FORBIDDEN,
                "loopback access only",
            )));
        }
        match decision.route_kind.as_deref() {
            Some("resolve") if request_context.request_path == "/api/internal/gateway/resolve" => {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway resolve payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayResolveRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway resolve payload",
                            )));
                        }
                    };
                let headers = match build_internal_gateway_header_map(&payload.headers) {
                    Ok(headers) => headers,
                    Err(response) => return Ok(Some(response)),
                };
                let method = match http::Method::from_bytes(payload.method.as_bytes()) {
                    Ok(method) => method,
                    Err(_) => {
                        return Ok(Some(build_internal_hub_error_response(
                            http::StatusCode::BAD_REQUEST,
                            "invalid internal gateway method",
                        )));
                    }
                };
                let uri = match build_internal_gateway_uri(
                    &payload.path,
                    payload.query_string.as_deref(),
                ) {
                    Ok(uri) => uri,
                    Err(response) => return Ok(Some(response)),
                };
                let resolved = crate::gateway::resolve_control_route(
                    state,
                    &method,
                    &uri,
                    &headers,
                    payload
                        .trace_id
                        .as_deref()
                        .unwrap_or(request_context.trace_id.as_str()),
                )
                .await?;
                let response_payload = resolved
                    .map(build_internal_gateway_resolve_payload)
                    .unwrap_or_else(|| build_internal_gateway_passthrough_payload(&uri));
                return Ok(Some(Json(response_payload).into_response()));
            }
            Some("auth_context")
                if request_context.request_path == "/api/internal/gateway/auth-context" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway auth-context payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayAuthContextRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway auth-context payload",
                            )));
                        }
                    };
                let headers = match build_internal_gateway_header_map(&payload.headers) {
                    Ok(headers) => headers,
                    Err(response) => return Ok(Some(response)),
                };
                let uri = match build_internal_gateway_uri("/", payload.query_string.as_deref()) {
                    Ok(uri) => uri,
                    Err(response) => return Ok(Some(response)),
                };
                let synthetic_decision = GatewayControlDecision {
                    public_path: "/".to_string(),
                    public_query_string: uri.query().map(ToOwned::to_owned),
                    route_class: Some("internal_proxy".to_string()),
                    route_family: Some("gateway_legacy".to_string()),
                    route_kind: Some("auth_context".to_string()),
                    auth_endpoint_signature: Some(payload.auth_endpoint_signature),
                    executor_candidate: false,
                    auth_context: None,
                    admin_principal: None,
                    local_auth_rejection: None,
                };
                let auth_context = crate::gateway::resolve_executor_auth_context(
                    state,
                    &synthetic_decision,
                    &headers,
                    &uri,
                    payload
                        .trace_id
                        .as_deref()
                        .unwrap_or(request_context.trace_id.as_str()),
                )
                .await?;
                return Ok(Some(
                    Json(json!({ "auth_context": auth_context })).into_response(),
                ));
            }
            Some("decision_sync")
                if request_context.request_path == "/api/internal/gateway/decision-sync" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway decision-sync payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway decision-sync payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let body_is_empty = payload.body_base64.is_none()
                    && payload
                        .body_json
                        .as_object()
                        .map(|value| value.is_empty())
                        .unwrap_or(false);
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(None)).into_response(),
                    ));
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                let auth_context = resolved.auth_context.clone();
                if auth_context
                    .as_ref()
                    .map(|value| !value.access_allowed)
                    .unwrap_or(true)
                {
                    let fallback_auth_context = if payload.auth_context.is_none() {
                        auth_context.as_ref()
                    } else {
                        None
                    };
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(
                            fallback_auth_context,
                        ))
                        .into_response(),
                    ));
                }
                let Some(mut local_payload) = maybe_build_sync_decision_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                    payload.body_base64.as_deref(),
                    body_is_empty,
                )
                .await?
                else {
                    let fallback_auth_context = if payload.auth_context.is_none() {
                        auth_context.as_ref()
                    } else {
                        None
                    };
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(
                            fallback_auth_context,
                        ))
                        .into_response(),
                    ));
                };
                if payload.auth_context.is_some() {
                    local_payload.auth_context = None;
                }
                return Ok(Some(Json(local_payload).into_response()));
            }
            Some("decision_stream")
                if request_context.request_path == "/api/internal/gateway/decision-stream" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway decision-stream payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway decision-stream payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(None)).into_response(),
                    ));
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                let auth_context = resolved.auth_context.clone();
                if auth_context
                    .as_ref()
                    .map(|value| !value.access_allowed)
                    .unwrap_or(true)
                {
                    let fallback_auth_context = if payload.auth_context.is_none() {
                        auth_context.as_ref()
                    } else {
                        None
                    };
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(
                            fallback_auth_context,
                        ))
                        .into_response(),
                    ));
                }
                let Some(mut local_payload) = maybe_build_stream_decision_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                )
                .await?
                else {
                    let fallback_auth_context = if payload.auth_context.is_none() {
                        auth_context.as_ref()
                    } else {
                        None
                    };
                    return Ok(Some(
                        Json(build_internal_gateway_fallback_plan_payload(
                            fallback_auth_context,
                        ))
                        .into_response(),
                    ));
                };
                if payload.auth_context.is_some() {
                    local_payload.auth_context = None;
                }
                return Ok(Some(Json(local_payload).into_response()));
            }
            Some("plan_sync")
                if request_context.request_path == "/api/internal/gateway/plan-sync" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway plan-sync payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway plan-sync payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let body_is_empty = payload.body_base64.is_none()
                    && payload
                        .body_json
                        .as_object()
                        .map(|value| value.is_empty())
                        .unwrap_or(false);
                let body_bytes = match decode_legacy_gateway_request_body_bytes(&payload) {
                    Ok(bytes) => bytes,
                    Err(response) => return Ok(Some(response)),
                };
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(Some(build_internal_gateway_proxy_public_response()));
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                if let Some(mut planned) = maybe_build_sync_plan_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                    payload.body_base64.as_deref(),
                    body_is_empty,
                )
                .await?
                {
                    if payload.auth_context.is_some() {
                        planned.auth_context = None;
                    }
                    return Ok(Some(Json(planned).into_response()));
                }
                if let Some(executed_response) = maybe_execute_via_executor_sync(
                    state,
                    &parts,
                    &body_bytes,
                    trace_id.as_str(),
                    Some(&resolved),
                )
                .await?
                {
                    return Ok(Some(attach_execution_path_header(
                        executed_response,
                        EXECUTION_PATH_EXECUTOR_SYNC,
                    )));
                }
                return Ok(Some(build_internal_gateway_proxy_public_response()));
            }
            Some("plan_stream")
                if request_context.request_path == "/api/internal/gateway/plan-stream" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway plan-stream payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway plan-stream payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let body_bytes = match decode_legacy_gateway_request_body_bytes(&payload) {
                    Ok(bytes) => bytes,
                    Err(response) => return Ok(Some(response)),
                };
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(Some(build_internal_gateway_proxy_public_response()));
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                if let Some(mut planned) = maybe_build_stream_plan_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                )
                .await?
                {
                    if payload.auth_context.is_some() {
                        planned.auth_context = None;
                    }
                    return Ok(Some(Json(planned).into_response()));
                }
                if let Some(executed_response) = maybe_execute_via_executor_stream(
                    state,
                    &parts,
                    &body_bytes,
                    trace_id.as_str(),
                    Some(&resolved),
                )
                .await?
                {
                    return Ok(Some(attach_execution_path_header(
                        executed_response,
                        EXECUTION_PATH_EXECUTOR_STREAM,
                    )));
                }
                return Ok(Some(build_internal_gateway_proxy_public_response()));
            }
            Some("execute_sync")
                if request_context.request_path == "/api/internal/gateway/execute-sync" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway execute-sync payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway execute-sync payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let body_bytes = match decode_legacy_gateway_request_body_bytes(&payload) {
                    Ok(bytes) => bytes,
                    Err(response) => return Ok(Some(response)),
                };
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(None);
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                if let Some(plan_payload) = maybe_build_sync_plan_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                    payload.body_base64.as_deref(),
                    body_bytes.is_empty(),
                )
                .await?
                {
                    let plan_kind = plan_payload.plan_kind.unwrap_or_default();
                    if let (Some(executor_base_url), Some(plan)) =
                        (state.executor_base_url.as_deref(), plan_payload.plan)
                    {
                        if !plan_kind.trim().is_empty() {
                            let executed_response = execute_executor_sync(
                                state,
                                state.control_base_url.as_deref().unwrap_or(""),
                                executor_base_url,
                                parts.uri.path(),
                                plan,
                                trace_id.as_str(),
                                &resolved,
                                plan_kind.as_str(),
                                plan_payload.report_kind,
                                plan_payload.report_context,
                            )
                            .await?;
                            if let Some(executed_response) = executed_response {
                                return Ok(Some(attach_execution_path_header(
                                    executed_response,
                                    EXECUTION_PATH_EXECUTOR_SYNC,
                                )));
                            }
                        }
                    }
                }
                if let Some(executed_response) = maybe_execute_via_executor_sync(
                    state,
                    &parts,
                    &body_bytes,
                    trace_id.as_str(),
                    Some(&resolved),
                )
                .await?
                {
                    return Ok(Some(attach_execution_path_header(
                        executed_response,
                        EXECUTION_PATH_EXECUTOR_SYNC,
                    )));
                }
                return Ok(None);
            }
            Some("execute_stream")
                if request_context.request_path == "/api/internal/gateway/execute-stream" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway execute-stream payload",
                    )));
                };
                let payload =
                    match serde_json::from_slice::<LegacyGatewayExecuteRequest>(request_body) {
                        Ok(payload) => payload,
                        Err(_) => {
                            return Ok(Some(build_internal_hub_error_response(
                                http::StatusCode::BAD_REQUEST,
                                "invalid internal gateway execute-stream payload",
                            )));
                        }
                    };
                let parts = match build_internal_gateway_request_parts(
                    &payload.method,
                    &payload.path,
                    payload.query_string.as_deref(),
                    &payload.headers,
                ) {
                    Ok(parts) => parts,
                    Err(response) => return Ok(Some(response)),
                };
                let trace_id = payload
                    .trace_id
                    .as_deref()
                    .unwrap_or(request_context.trace_id.as_str())
                    .to_string();
                let body_bytes = match decode_legacy_gateway_request_body_bytes(&payload) {
                    Ok(bytes) => bytes,
                    Err(response) => return Ok(Some(response)),
                };
                let Some(mut resolved) = crate::gateway::resolve_control_route(
                    state,
                    &parts.method,
                    &parts.uri,
                    &parts.headers,
                    trace_id.as_str(),
                )
                .await?
                else {
                    return Ok(None);
                };
                if let Some(auth_context) = payload.auth_context.clone() {
                    resolved.auth_context = Some(auth_context);
                    resolved.local_auth_rejection = None;
                }
                if let Some(plan_payload) = maybe_build_stream_plan_payload_via_local_path(
                    state,
                    &parts,
                    trace_id.as_str(),
                    &resolved,
                    &payload.body_json,
                )
                .await?
                {
                    let plan_kind = plan_payload.plan_kind.unwrap_or_default();
                    if let (Some(executor_base_url), Some(plan)) =
                        (state.executor_base_url.as_deref(), plan_payload.plan)
                    {
                        if !plan_kind.trim().is_empty() {
                            let executed_response = execute_executor_stream(
                                state,
                                state.control_base_url.as_deref().unwrap_or(""),
                                executor_base_url,
                                plan,
                                trace_id.as_str(),
                                &resolved,
                                plan_kind.as_str(),
                                plan_payload.report_kind,
                                plan_payload.report_context,
                            )
                            .await?;
                            if let Some(executed_response) = executed_response {
                                return Ok(Some(attach_execution_path_header(
                                    executed_response,
                                    EXECUTION_PATH_EXECUTOR_STREAM,
                                )));
                            }
                        }
                    }
                }
                if let Some(executed_response) = maybe_execute_via_executor_stream(
                    state,
                    &parts,
                    &body_bytes,
                    trace_id.as_str(),
                    Some(&resolved),
                )
                .await?
                {
                    return Ok(Some(attach_execution_path_header(
                        executed_response,
                        EXECUTION_PATH_EXECUTOR_STREAM,
                    )));
                }
                return Ok(None);
            }
            Some("report_sync")
                if request_context.request_path == "/api/internal/gateway/report-sync" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway report-sync payload",
                    )));
                };
                let payload = match serde_json::from_slice::<
                    crate::gateway::executor::GatewaySyncReportRequest,
                >(request_body)
                {
                    Ok(payload) => payload,
                    Err(_) => {
                        return Ok(Some(build_internal_hub_error_response(
                            http::StatusCode::BAD_REQUEST,
                            "invalid internal gateway report-sync payload",
                        )));
                    }
                };
                let trace_id = payload.trace_id.clone();
                crate::gateway::usage::submit_sync_report(state, "", &trace_id, payload).await?;
                return Ok(Some(Json(json!({ "ok": true })).into_response()));
            }
            Some("report_stream")
                if request_context.request_path == "/api/internal/gateway/report-stream" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway report-stream payload",
                    )));
                };
                let payload = match serde_json::from_slice::<
                    crate::gateway::executor::GatewayStreamReportRequest,
                >(request_body)
                {
                    Ok(payload) => payload,
                    Err(_) => {
                        return Ok(Some(build_internal_hub_error_response(
                            http::StatusCode::BAD_REQUEST,
                            "invalid internal gateway report-stream payload",
                        )));
                    }
                };
                let trace_id = payload.trace_id.clone();
                crate::gateway::usage::submit_stream_report(state, "", &trace_id, payload).await?;
                return Ok(Some(Json(json!({ "ok": true })).into_response()));
            }
            Some("finalize_sync")
                if request_context.request_path == "/api/internal/gateway/finalize-sync" =>
            {
                let Some(request_body) = request_body else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "invalid internal gateway finalize-sync payload",
                    )));
                };
                let payload = match serde_json::from_slice::<
                    crate::gateway::executor::GatewaySyncReportRequest,
                >(request_body)
                {
                    Ok(payload) => payload,
                    Err(_) => {
                        return Ok(Some(build_internal_hub_error_response(
                            http::StatusCode::BAD_REQUEST,
                            "invalid internal gateway finalize-sync payload",
                        )));
                    }
                };
                let Some(synthetic_decision) = build_legacy_finalize_decision(&payload) else {
                    return Ok(Some(build_internal_hub_error_response(
                        http::StatusCode::BAD_REQUEST,
                        "Unsupported gateway sync finalize kind",
                    )));
                };
                let trace_id = payload.trace_id.clone();
                if let Some(outcome) =
                    crate::gateway::local_finalize::maybe_build_local_core_sync_finalize_response(
                        &trace_id,
                        &synthetic_decision,
                        &payload,
                    )?
                {
                    if let Some(background_report) = outcome.background_report {
                        crate::gateway::usage::spawn_sync_report(
                            state.clone(),
                            String::new(),
                            trace_id.clone(),
                            background_report,
                        );
                    }
                    let mut response = outcome.response;
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    return Ok(Some(response));
                }
                if let Some(response) = maybe_build_legacy_finalize_video_response(
                    state,
                    trace_id.as_str(),
                    &synthetic_decision,
                    &payload,
                )
                .await?
                {
                    return Ok(Some(response));
                }
                return Ok(Some(build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "Unsupported gateway sync finalize kind",
                )));
            }
            _ => return Ok(None),
        }
    }
    if !remote_addr.ip().is_loopback() {
        return Ok(Some(build_internal_hub_error_response(
            http::StatusCode::FORBIDDEN,
            "loopback access only",
        )));
    }

    if decision.route_family.as_deref() != Some("hub_manage")
        || request_context.request_method != http::Method::POST
    {
        return Ok(None);
    }

    match decision.route_kind.as_deref() {
        Some("heartbeat") if request_context.request_path == "/api/internal/hub/heartbeat" => {
            let Some(request_body) = request_body else {
                return Ok(Some(build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid heartbeat payload",
                )));
            };
            let payload = match parse_internal_hub_heartbeat_request(request_body) {
                Ok(payload) => payload,
                Err(response) => return Ok(Some(response)),
            };
            let node_id = payload.node_id.trim().to_string();
            let mutation = ProxyNodeHeartbeatMutation {
                node_id: node_id.clone(),
                heartbeat_interval: payload.heartbeat_interval,
                active_connections: payload.active_connections,
                total_requests_delta: payload.total_requests,
                avg_latency_ms: payload.avg_latency_ms,
                failed_requests_delta: payload.failed_requests,
                dns_failures_delta: payload.dns_failures,
                stream_errors_delta: payload.stream_errors,
                proxy_metadata: payload.proxy_metadata,
                proxy_version: payload.proxy_version,
            };

            let response = match state.apply_proxy_node_heartbeat(&mutation).await {
                Ok(Some(node)) => Json(build_internal_hub_heartbeat_ack(&node)).into_response(),
                Ok(None) => build_internal_hub_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("heartbeat sync failed: ProxyNode {node_id} 不存在"),
                ),
                Err(err) => build_internal_hub_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("heartbeat sync failed: {}", gateway_error_message(err)),
                ),
            };
            return Ok(Some(response));
        }
        Some("node_status") if request_context.request_path == "/api/internal/hub/node-status" => {
            let Some(request_body) = request_body else {
                return Ok(Some(build_internal_hub_error_response(
                    http::StatusCode::BAD_REQUEST,
                    "invalid node-status payload",
                )));
            };
            let payload = match parse_internal_hub_node_status_request(request_body) {
                Ok(payload) => payload,
                Err(response) => return Ok(Some(response)),
            };
            let mutation = ProxyNodeTunnelStatusMutation {
                node_id: payload.node_id.trim().to_string(),
                connected: payload.connected,
                conn_count: payload.conn_count,
                detail: None,
                observed_at_unix_secs: None,
            };

            let response = match state.update_proxy_node_tunnel_status(&mutation).await {
                Ok(node) => Json(json!({ "updated": node.is_some() })).into_response(),
                Err(err) => build_internal_hub_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("node status sync failed: {}", gateway_error_message(err)),
                ),
            };
            return Ok(Some(response));
        }
        _ => {}
    }

    Ok(None)
}
