use super::*;

include!("proxy/local.rs");

include!("admin/provider_oauth/dispatch.rs");
include!("admin/core.rs");
include!("admin/global_models.rs");
include!("admin/provider_models.rs");
include!("admin/providers.rs");
include!("admin/endpoints.rs");
include!("public/support.rs");

const OPENAI_CHAT_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "OpenAI chat executor miss did not match a Rust execution path, and Python fallback has been removed";
const OPENAI_RESPONSES_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "OpenAI responses executor miss did not match a Rust execution path, and Python fallback has been removed";
const OPENAI_COMPACT_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "OpenAI compact executor miss did not match a Rust execution path, and Python fallback has been removed";
const OPENAI_VIDEO_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "OpenAI video executor miss did not match a Rust execution path, and Python fallback has been removed";
const CLAUDE_MESSAGES_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "Claude messages executor miss did not match a Rust execution path, and Python fallback has been removed";
const GEMINI_PUBLIC_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "Gemini public executor miss did not match a Rust execution path, and Python fallback has been removed";
const GEMINI_FILES_PYTHON_FALLBACK_REMOVED_DETAIL: &str =
    "Gemini files executor miss did not match a Rust execution path, and Python fallback has been removed";

pub(crate) async fn proxy_request(
    State(state): State<AppState>,
    ConnectInfo(remote_addr): ConnectInfo<std::net::SocketAddr>,
    request: Request,
) -> Result<Response<Body>, GatewayError> {
    let started_at = Instant::now();
    let mut request_permit = match state.try_acquire_request_permit().await {
        Ok(permit) => permit,
        Err(crate::gateway::RequestAdmissionError::Local(
            aether_runtime::ConcurrencyError::Saturated { gate, limit },
        )) => {
            let trace_id = extract_or_generate_trace_id(request.headers());
            let response = build_local_overloaded_response(&trace_id, None, gate, limit)?;
            return Ok(finalize_gateway_response(
                &state,
                response,
                &trace_id,
                &remote_addr,
                request.method(),
                request
                    .uri()
                    .path_and_query()
                    .map(|value| value.as_str())
                    .unwrap_or("/"),
                None,
                EXECUTION_PATH_LOCAL_OVERLOADED,
                &started_at,
                None,
            ));
        }
        Err(crate::gateway::RequestAdmissionError::Local(
            aether_runtime::ConcurrencyError::Closed { gate },
        )) => {
            return Err(GatewayError::Internal(format!(
                "gateway request concurrency gate {gate} is closed"
            )));
        }
        Err(crate::gateway::RequestAdmissionError::Distributed(
            aether_runtime::DistributedConcurrencyError::Saturated { gate, limit },
        ))
        | Err(crate::gateway::RequestAdmissionError::Distributed(
            aether_runtime::DistributedConcurrencyError::Unavailable { gate, limit, .. },
        )) => {
            let trace_id = extract_or_generate_trace_id(request.headers());
            let response = build_local_overloaded_response(&trace_id, None, gate, limit)?;
            return Ok(finalize_gateway_response(
                &state,
                response,
                &trace_id,
                &remote_addr,
                request.method(),
                request
                    .uri()
                    .path_and_query()
                    .map(|value| value.as_str())
                    .unwrap_or("/"),
                None,
                EXECUTION_PATH_DISTRIBUTED_OVERLOADED,
                &started_at,
                None,
            ));
        }
        Err(crate::gateway::RequestAdmissionError::Distributed(
            aether_runtime::DistributedConcurrencyError::InvalidConfiguration(message),
        )) => return Err(GatewayError::Internal(message)),
    };
    let (parts, body) = request.into_parts();
    let trace_id = extract_or_generate_trace_id(&parts.headers);
    let request_context = resolve_public_request_context(
        &state,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &trace_id,
    )
    .await?;
    let mut request_body = Some(body);
    let local_proxy_body = if local_proxy_route_requires_buffered_body(&request_context) {
        Some(
            to_bytes(
                request_body
                    .take()
                    .expect("local proxy body buffering should own request body"),
                usize::MAX,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        )
    } else {
        None
    };
    let method = request_context.request_method.clone();
    let request_path_and_query = request_context.request_path_and_query();
    let path_and_query = request_path_and_query.as_str();
    let control_decision = request_context.control_decision.as_ref();
    let legacy_internal_gateway_allowed = request_enables_control_execute(&parts.headers);
    if let Some(response) = maybe_build_local_internal_proxy_response(
        &state,
        &request_context,
        &remote_addr,
        local_proxy_body.as_ref(),
        legacy_internal_gateway_allowed,
    )
    .await?
    {
        let execution_path =
            resolve_local_proxy_execution_path(&response, EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH);
        return Ok(finalize_gateway_response_with_context(
            &state,
            response,
            &remote_addr,
            &request_context,
            execution_path,
            &started_at,
            request_permit.take(),
        ));
    }
    if let Some(response) =
        maybe_build_local_admin_proxy_response(&state, &request_context, local_proxy_body.as_ref())
            .await?
    {
        let execution_path =
            resolve_local_proxy_execution_path(&response, EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH);
        return Ok(finalize_gateway_response_with_context(
            &state,
            response,
            &remote_addr,
            &request_context,
            execution_path,
            &started_at,
            request_permit.take(),
        ));
    }
    if let Some(response) = maybe_build_local_public_support_response(
        &state,
        &request_context,
        &parts.headers,
        local_proxy_body.as_ref(),
    )
    .await
    {
        let execution_path =
            resolve_local_proxy_execution_path(&response, EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH);
        return Ok(finalize_gateway_response_with_context(
            &state,
            response,
            &remote_addr,
            &request_context,
            execution_path,
            &started_at,
            request_permit.take(),
        ));
    }
    if let Some(buffered_body) = local_proxy_body {
        request_body = Some(Body::from(buffered_body));
    }
    if let Some(rejection) = trusted_auth_local_rejection(control_decision, &parts.headers) {
        let response =
            build_local_auth_rejection_response(&trace_id, control_decision, &rejection)?;
        return Ok(finalize_gateway_response_with_context(
            &state,
            response,
            &remote_addr,
            &request_context,
            EXECUTION_PATH_LOCAL_AUTH_DENIED,
            &started_at,
            request_permit.take(),
        ));
    }
    let rate_limit_outcome = state
        .frontdoor_user_rpm()
        .check_and_consume(&state, control_decision)
        .await?;
    if let FrontdoorUserRpmOutcome::Rejected(rejection) = &rate_limit_outcome {
        let response =
            build_local_user_rpm_limited_response(&trace_id, control_decision, rejection)?;
        return Ok(finalize_gateway_response_with_context(
            &state,
            response,
            &remote_addr,
            &request_context,
            EXECUTION_PATH_LOCAL_RATE_LIMITED,
            &started_at,
            request_permit.take(),
        ));
    }
    let upstream_path_and_query =
        sanitize_upstream_path_and_query(control_decision, path_and_query);
    let target_url = format!("{}{}", state.upstream_base_url, upstream_path_and_query);
    let should_try_control_execute = control_decision
        .map(|decision| {
            decision.executor_candidate && decision.route_class.as_deref() == Some("ai_public")
        })
        .unwrap_or(false);
    let should_buffer_for_local_auth =
        should_buffer_request_for_local_auth(control_decision, &parts.headers);
    let should_buffer_body = should_try_control_execute || should_buffer_for_local_auth;

    let mut upstream_request = state.client.request(method.clone(), &target_url);
    for (name, value) in &parts.headers {
        if should_skip_request_header(name.as_str()) {
            continue;
        }
        // Once Rust has produced trusted auth headers, Python should not need the raw
        // provider credential anymore. Keep the bridge boundary explicit.
        if should_strip_forwarded_provider_credential_header(control_decision, name) {
            continue;
        }
        if should_strip_forwarded_trusted_admin_header(control_decision, name) {
            continue;
        }
        upstream_request = upstream_request.header(name, value);
    }

    if let Some(host) = request_context.host_header.as_deref() {
        if !parts.headers.contains_key(FORWARDED_HOST_HEADER) {
            upstream_request = upstream_request.header(FORWARDED_HOST_HEADER, host);
        }
    }

    if !parts.headers.contains_key(FORWARDED_FOR_HEADER) {
        upstream_request =
            upstream_request.header(FORWARDED_FOR_HEADER, remote_addr.ip().to_string());
    }

    if !parts.headers.contains_key(FORWARDED_PROTO_HEADER) {
        upstream_request = upstream_request.header(FORWARDED_PROTO_HEADER, "http");
    }

    if !parts.headers.contains_key(TRACE_ID_HEADER) {
        upstream_request = upstream_request.header(TRACE_ID_HEADER, &trace_id);
    }

    if let Some(decision) = control_decision {
        upstream_request = upstream_request
            .header(
                CONTROL_ROUTE_CLASS_HEADER,
                decision.route_class.as_deref().unwrap_or("passthrough"),
            )
            .header(
                CONTROL_EXECUTOR_HEADER,
                if decision.executor_candidate {
                    "true"
                } else {
                    "false"
                },
            );
        if let Some(route_family) = decision.route_family.as_deref() {
            upstream_request = upstream_request.header(CONTROL_ROUTE_FAMILY_HEADER, route_family);
        }
        if let Some(route_kind) = decision.route_kind.as_deref() {
            upstream_request = upstream_request.header(CONTROL_ROUTE_KIND_HEADER, route_kind);
        }
        if let Some(endpoint_signature) = decision.auth_endpoint_signature.as_deref() {
            upstream_request =
                upstream_request.header(CONTROL_ENDPOINT_SIGNATURE_HEADER, endpoint_signature);
        }
        if let Some(auth_context) = decision.auth_context.as_ref() {
            upstream_request = upstream_request
                .header(TRUSTED_AUTH_USER_ID_HEADER, &auth_context.user_id)
                .header(TRUSTED_AUTH_API_KEY_ID_HEADER, &auth_context.api_key_id)
                .header(
                    TRUSTED_AUTH_ACCESS_ALLOWED_HEADER,
                    if auth_context.access_allowed {
                        "true"
                    } else {
                        "false"
                    },
                );
            if let Some(balance_remaining) = auth_context.balance_remaining {
                upstream_request = upstream_request
                    .header(TRUSTED_AUTH_BALANCE_HEADER, balance_remaining.to_string());
            }
        }
        if let Some(admin_principal) = decision.admin_principal.as_ref() {
            upstream_request = upstream_request
                .header(TRUSTED_ADMIN_USER_ID_HEADER, &admin_principal.user_id)
                .header(TRUSTED_ADMIN_USER_ROLE_HEADER, &admin_principal.user_role);
            if let Some(session_id) = admin_principal.session_id.as_deref() {
                upstream_request =
                    upstream_request.header(TRUSTED_ADMIN_SESSION_ID_HEADER, session_id);
            }
            if let Some(token_id) = admin_principal.management_token_id.as_deref() {
                upstream_request =
                    upstream_request.header(TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER, token_id);
            }
        }
    }
    if matches!(rate_limit_outcome, FrontdoorUserRpmOutcome::Allowed) {
        upstream_request = upstream_request.header(TRUSTED_RATE_LIMIT_PREFLIGHT_HEADER, "true");
    }

    upstream_request = upstream_request.header(GATEWAY_HEADER, "rust-phase3b");

    let allow_control_execute_fallback = should_try_control_execute
        && control_decision.is_some_and(allows_control_execute_emergency)
        && state.control_base_url.is_some()
        && request_enables_control_execute(&parts.headers);

    let buffered_body = if should_buffer_body {
        Some(
            to_bytes(
                request_body
                    .take()
                    .expect("buffered auth/executor path should own request body"),
                usize::MAX,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        )
    } else {
        None
    };

    if let Some(buffered_body) = buffered_body.as_ref() {
        if let Some(rejection) = request_model_local_rejection(
            control_decision,
            &parts.uri,
            &parts.headers,
            buffered_body,
        ) {
            let response =
                build_local_auth_rejection_response(&trace_id, control_decision, &rejection)?;
            return Ok(finalize_gateway_response_with_context(
                &state,
                response,
                &remote_addr,
                &request_context,
                EXECUTION_PATH_LOCAL_AUTH_DENIED,
                &started_at,
                request_permit.take(),
            ));
        }
    }

    let upstream_response = if should_try_control_execute {
        let buffered_body = buffered_body
            .as_ref()
            .expect("executor/control auth gate should have buffered request body");
        let stream_request = request_wants_stream(&request_context, buffered_body);
        if stream_request {
            if let Some(executor_response) = maybe_execute_via_executor_stream(
                &state,
                &parts,
                buffered_body,
                &trace_id,
                control_decision,
            )
            .await?
            {
                return Ok(finalize_gateway_response_with_context(
                    &state,
                    executor_response,
                    &remote_addr,
                    &request_context,
                    EXECUTION_PATH_EXECUTOR_STREAM,
                    &started_at,
                    request_permit.take(),
                ));
            }
        }
        if let Some(executor_response) = maybe_execute_via_executor_sync(
            &state,
            &parts,
            buffered_body,
            &trace_id,
            control_decision,
        )
        .await?
        {
            return Ok(finalize_gateway_response_with_context(
                &state,
                executor_response,
                &remote_addr,
                &request_context,
                EXECUTION_PATH_EXECUTOR_SYNC,
                &started_at,
                request_permit.take(),
            ));
        }
        if parts.method != http::Method::POST {
            if let Some(executor_response) = maybe_execute_via_executor_stream(
                &state,
                &parts,
                buffered_body,
                &trace_id,
                control_decision,
            )
            .await?
            {
                return Ok(finalize_gateway_response_with_context(
                    &state,
                    executor_response,
                    &remote_addr,
                    &request_context,
                    EXECUTION_PATH_EXECUTOR_STREAM,
                    &started_at,
                    request_permit.take(),
                ));
            }
        }
        if allow_control_execute_fallback {
            if let Some(control_response) = maybe_execute_via_control(
                &state,
                &parts,
                buffered_body.clone(),
                &trace_id,
                control_decision,
                stream_request,
            )
            .await?
            {
                let reason = if state.executor_base_url.is_none() {
                    GatewayFallbackReason::ExecutorMissing
                } else {
                    GatewayFallbackReason::ControlExecuteEmergency
                };
                let control_execution_path = if stream_request {
                    EXECUTION_PATH_CONTROL_EXECUTE_STREAM
                } else {
                    EXECUTION_PATH_CONTROL_EXECUTE_SYNC
                };
                state.record_fallback_metric(
                    GatewayFallbackMetricKind::ControlExecuteFallback,
                    control_decision,
                    None,
                    Some(control_execution_path),
                    reason,
                );
                state.record_fallback_metric(
                    GatewayFallbackMetricKind::PythonExecuteEmergency,
                    control_decision,
                    None,
                    Some(control_execution_path),
                    reason,
                );
                let mut control_response = control_response;
                control_response.headers_mut().insert(
                    HeaderName::from_static(PYTHON_DEPENDENCY_REASON_HEADER),
                    HeaderValue::from_static(reason.as_label_value()),
                );
                return Ok(finalize_gateway_response_with_context(
                    &state,
                    control_response,
                    &remote_addr,
                    &request_context,
                    control_execution_path,
                    &started_at,
                    request_permit.take(),
                ));
            }
        }
        let local_executor_miss_detail =
            local_executor_miss_detail_after_python_fallback_removal(control_decision);
        state.record_fallback_metric(
            if local_executor_miss_detail.is_some() {
                GatewayFallbackMetricKind::LocalExecutorMiss
            } else {
                GatewayFallbackMetricKind::PublicProxyAfterExecutorMiss
            },
            control_decision,
            None,
            Some(if local_executor_miss_detail.is_some() {
                EXECUTION_PATH_LOCAL_EXECUTOR_MISS
            } else {
                EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS
            }),
            if local_executor_miss_detail.is_some() {
                GatewayFallbackReason::PythonFallbackRemoved
            } else {
                GatewayFallbackReason::ExecutorMiss
            },
        );
        if let Some(local_executor_miss_detail) = local_executor_miss_detail {
            let response = build_local_http_error_response(
                &trace_id,
                control_decision,
                http::StatusCode::SERVICE_UNAVAILABLE,
                local_executor_miss_detail,
            )?;
            return Ok(finalize_gateway_response_with_context(
                &state,
                response,
                &remote_addr,
                &request_context,
                EXECUTION_PATH_LOCAL_EXECUTOR_MISS,
                &started_at,
                request_permit.take(),
            ));
        }
        upstream_request = upstream_request.header(
            EXECUTION_PATH_HEADER,
            EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS,
        );
        upstream_request
            .body(buffered_body.clone())
            .send()
            .await
            .map_err(|err| GatewayError::UpstreamUnavailable {
                trace_id: trace_id.clone(),
                message: err.to_string(),
            })?
    } else {
        state.record_fallback_metric(
            GatewayFallbackMetricKind::PublicProxyPassthrough,
            control_decision,
            None,
            Some(EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH),
            GatewayFallbackReason::ProxyPassthrough,
        );
        upstream_request = upstream_request.header(
            EXECUTION_PATH_HEADER,
            EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH,
        );
        if let Some(buffered_body) = buffered_body {
            upstream_request
                .body(buffered_body)
                .send()
                .await
                .map_err(|err| GatewayError::UpstreamUnavailable {
                    trace_id: trace_id.clone(),
                    message: err.to_string(),
                })?
        } else {
            let request_body_stream = request_body
                .take()
                .expect("streaming passthrough path should retain request body")
                .into_data_stream()
                .map_err(|err| std::io::Error::other(err.to_string()));
            upstream_request
                .body(reqwest::Body::wrap_stream(request_body_stream))
                .send()
                .await
                .map_err(|err| GatewayError::UpstreamUnavailable {
                    trace_id: trace_id.clone(),
                    message: err.to_string(),
                })?
        }
    };

    let mut response = build_client_response(upstream_response, &trace_id, control_decision)?;
    let python_dependency_reason = if should_try_control_execute {
        GatewayFallbackReason::ExecutorMiss
    } else {
        GatewayFallbackReason::ProxyPassthrough
    };
    response.headers_mut().insert(
        HeaderName::from_static(PYTHON_DEPENDENCY_REASON_HEADER),
        HeaderValue::from_static(python_dependency_reason.as_label_value()),
    );
    Ok(finalize_gateway_response_with_context(
        &state,
        response,
        &remote_addr,
        &request_context,
        if should_try_control_execute {
            EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS
        } else {
            EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH
        },
        &started_at,
        request_permit.take(),
    ))
}

fn local_executor_miss_detail_after_python_fallback_removal(
    decision: Option<&GatewayControlDecision>,
) -> Option<&'static str> {
    let decision = decision?;
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }
    let public_path = decision.public_path.as_str();
    match public_path {
        "/v1/chat/completions" => Some(OPENAI_CHAT_PYTHON_FALLBACK_REMOVED_DETAIL),
        "/v1/responses" => Some(OPENAI_RESPONSES_PYTHON_FALLBACK_REMOVED_DETAIL),
        "/v1/responses/compact" => Some(OPENAI_COMPACT_PYTHON_FALLBACK_REMOVED_DETAIL),
        "/v1/messages" => Some(CLAUDE_MESSAGES_PYTHON_FALLBACK_REMOVED_DETAIL),
        path if path.starts_with("/v1/videos") => Some(OPENAI_VIDEO_PYTHON_FALLBACK_REMOVED_DETAIL),
        path if path.starts_with("/upload/v1beta/files") || path.starts_with("/v1beta/files") => {
            Some(GEMINI_FILES_PYTHON_FALLBACK_REMOVED_DETAIL)
        }
        path if decision.route_family.as_deref() == Some("gemini")
            && (path.starts_with("/v1beta/models/") || path.starts_with("/v1/models/")) =>
        {
            Some(GEMINI_PUBLIC_PYTHON_FALLBACK_REMOVED_DETAIL)
        }
        _ => None,
    }
}

include!("proxy/finalize.rs");
