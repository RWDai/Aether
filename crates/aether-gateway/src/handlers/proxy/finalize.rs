fn request_wants_stream(
    request_context: &GatewayPublicRequestContext,
    body: &axum::body::Bytes,
) -> bool {
    if request_context
        .request_path
        .contains(":streamGenerateContent")
    {
        return true;
    }
    if !request_context
        .request_content_type
        .as_deref()
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(false)
        || body.is_empty()
    {
        return false;
    }
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("stream").and_then(|stream| stream.as_bool()))
        .unwrap_or(false)
}

fn finalize_gateway_response(
    state: &AppState,
    mut response: Response<Body>,
    trace_id: &str,
    remote_addr: &std::net::SocketAddr,
    method: &http::Method,
    path_and_query: &str,
    control_decision: Option<&GatewayControlDecision>,
    execution_path: &'static str,
    started_at: &Instant,
    request_permit: Option<AdmissionPermit>,
) -> Response<Body> {
    response.headers_mut().insert(
        HeaderName::from_static(EXECUTION_PATH_HEADER),
        HeaderValue::from_static(execution_path),
    );

    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    let python_dependency_reason = response
        .headers()
        .get(PYTHON_DEPENDENCY_REASON_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("none");
    info!(
        trace_id = %trace_id,
        remote_addr = %remote_addr,
        method = %method,
        path = %path_and_query,
        route_class = control_decision
            .and_then(|decision| decision.route_class.as_deref())
            .unwrap_or("passthrough"),
        execution_path,
        python_dependency_reason,
        status = response.status().as_u16(),
        elapsed_ms,
        "gateway completed request"
    );

    record_shadow_result_non_blocking(
        state.clone(),
        trace_id,
        method,
        path_and_query,
        control_decision,
        execution_path,
        &response,
    );

    maybe_hold_axum_response_permit(response, request_permit)
}

fn finalize_gateway_response_with_context(
    state: &AppState,
    response: Response<Body>,
    remote_addr: &std::net::SocketAddr,
    request_context: &GatewayPublicRequestContext,
    execution_path: &'static str,
    started_at: &Instant,
    request_permit: Option<AdmissionPermit>,
) -> Response<Body> {
    finalize_gateway_response(
        state,
        response,
        &request_context.trace_id,
        remote_addr,
        &request_context.request_method,
        &request_context.request_path_and_query(),
        request_context.control_decision.as_ref(),
        execution_path,
        started_at,
        request_permit,
    )
}
