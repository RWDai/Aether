use super::*;

#[tokio::test]
async fn gateway_strips_spoofed_admin_principal_headers_without_gateway_marker() {
    #[derive(Debug, Clone)]
    struct SeenAdminRequest {
        trusted_admin_user_id: String,
        trusted_admin_user_role: String,
        trusted_admin_session_id: String,
    }

    let seen_admin = Arc::new(Mutex::new(None::<SeenAdminRequest>));
    let seen_admin_clone = Arc::clone(&seen_admin);
    let upstream = Router::new().route(
        "/api/admin/endpoints/health/api-formats",
        any(move |request: Request| {
            let seen_admin_inner = Arc::clone(&seen_admin_clone);
            async move {
                *seen_admin_inner.lock().expect("mutex should lock") = Some(SeenAdminRequest {
                    trusted_admin_user_id: request
                        .headers()
                        .get(TRUSTED_ADMIN_USER_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    trusted_admin_user_role: request
                        .headers()
                        .get(TRUSTED_ADMIN_USER_ROLE_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    trusted_admin_session_id: request
                        .headers()
                        .get(TRUSTED_ADMIN_SESSION_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                });
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router(upstream_url.clone()).expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/admin/endpoints/health/api-formats"
        ))
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let seen_request = seen_admin
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("admin request should be captured");
    assert_eq!(seen_request.trusted_admin_user_id, "");
    assert_eq!(seen_request.trusted_admin_user_role, "");
    assert_eq!(seen_request.trusted_admin_session_id, "");

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_proxies_ai_routes_without_python_auth_context_fallback() {
    #[derive(Debug, Clone)]
    struct SeenPublicRequest {
        control_route_class: String,
        control_route_family: String,
        control_route_kind: String,
        control_executor_candidate: String,
        control_endpoint_signature: String,
        trusted_user_id: String,
        trusted_api_key_id: String,
        trusted_balance_remaining: String,
        trusted_access_allowed: String,
        trace_id: String,
    }

    let auth_context_hits = Arc::new(Mutex::new(0usize));
    let auth_context_hits_clone = Arc::clone(&auth_context_hits);
    let seen_public = Arc::new(Mutex::new(None::<SeenPublicRequest>));
    let seen_public_clone = Arc::clone(&seen_public);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(move |_request: Request| {
                let auth_context_hits_inner = Arc::clone(&auth_context_hits_clone);
                async move {
                    *auth_context_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "auth_context": {
                            "user_id": "user-123",
                            "api_key_id": "key-123",
                            "balance_remaining": 42.5,
                            "access_allowed": true
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let seen_public_inner = Arc::clone(&seen_public_clone);
                async move {
                    *seen_public_inner.lock().expect("mutex should lock") =
                        Some(SeenPublicRequest {
                            control_route_class: request
                                .headers()
                                .get(CONTROL_ROUTE_CLASS_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_route_family: request
                                .headers()
                                .get(CONTROL_ROUTE_FAMILY_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_route_kind: request
                                .headers()
                                .get(CONTROL_ROUTE_KIND_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_executor_candidate: request
                                .headers()
                                .get(CONTROL_EXECUTOR_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_endpoint_signature: request
                                .headers()
                                .get(CONTROL_ENDPOINT_SIGNATURE_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_user_id: request
                                .headers()
                                .get(TRUSTED_AUTH_USER_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_api_key_id: request
                                .headers()
                                .get(TRUSTED_AUTH_API_KEY_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_balance_remaining: request
                                .headers()
                                .get(TRUSTED_AUTH_BALANCE_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_access_allowed: request
                                .headers()
                                .get(TRUSTED_AUTH_ACCESS_ALLOWED_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trace_id: request
                                .headers()
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    (
                        StatusCode::OK,
                        [(GATEWAY_HEADER, "python-upstream")],
                        Body::from("proxied"),
                    )
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions?stream=true"))
        .header(TRACE_ID_HEADER, "trace-control-123")
        .body("{\"hello\":\"world\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*auth_context_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("ai_public")
    );
    assert_eq!(
        response
            .headers()
            .get(CONTROL_EXECUTOR_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );

    let seen_public_request = seen_public
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("public request should be captured");
    assert_eq!(seen_public_request.control_route_class, "ai_public");
    assert_eq!(seen_public_request.control_route_family, "openai");
    assert_eq!(seen_public_request.control_route_kind, "chat");
    assert_eq!(seen_public_request.control_executor_candidate, "true");
    assert_eq!(
        seen_public_request.control_endpoint_signature,
        "openai:chat"
    );
    assert_eq!(seen_public_request.trusted_user_id, "");
    assert_eq!(seen_public_request.trusted_api_key_id, "");
    assert_eq!(seen_public_request.trusted_balance_remaining, "");
    assert_eq!(seen_public_request.trusted_access_allowed, "");
    assert_eq!(seen_public_request.trace_id, "trace-control-123");

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_proxies_models_support_routes_with_public_support_control_headers() {
    #[derive(Debug, Clone)]
    struct SeenSupportRequest {
        control_route_class: String,
        control_route_family: String,
        control_route_kind: String,
        control_executor_candidate: String,
        control_endpoint_signature: String,
        trace_id: String,
    }

    let seen_support = Arc::new(Mutex::new(None::<SeenSupportRequest>));
    let seen_support_clone = Arc::clone(&seen_support);

    let upstream = Router::new().route(
        "/v1/models",
        any(move |request: Request| {
            let seen_support_inner = Arc::clone(&seen_support_clone);
            async move {
                *seen_support_inner.lock().expect("mutex should lock") = Some(SeenSupportRequest {
                    control_route_class: request
                        .headers()
                        .get(CONTROL_ROUTE_CLASS_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    control_route_family: request
                        .headers()
                        .get(CONTROL_ROUTE_FAMILY_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    control_route_kind: request
                        .headers()
                        .get(CONTROL_ROUTE_KIND_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    control_executor_candidate: request
                        .headers()
                        .get(CONTROL_EXECUTOR_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    control_endpoint_signature: request
                        .headers()
                        .get(CONTROL_ENDPOINT_SIGNATURE_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    trace_id: request
                        .headers()
                        .get(TRACE_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                });
                (
                    StatusCode::OK,
                    [(GATEWAY_HEADER, "python-support-upstream")],
                    Body::from("{\"object\":\"list\",\"data\":[]}"),
                )
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-test-models")),
        sample_currently_usable_auth_snapshot("key-models-1", "user-models-1"),
    )]));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url, None)
            .expect("gateway should build")
            .with_auth_api_key_data_reader_for_tests(auth_repository),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/models?limit=20"))
        .header(http::header::AUTHORIZATION, "Bearer sk-test-models")
        .header(TRACE_ID_HEADER, "trace-models-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("public_support")
    );
    assert_eq!(
        response
            .headers()
            .get(CONTROL_EXECUTOR_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("false")
    );

    let seen_support_request = seen_support
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("support request should be captured");
    assert_eq!(seen_support_request.control_route_class, "public_support");
    assert_eq!(seen_support_request.control_route_family, "models");
    assert_eq!(seen_support_request.control_route_kind, "list");
    assert_eq!(seen_support_request.control_executor_candidate, "false");
    assert_eq!(
        seen_support_request.control_endpoint_signature,
        "openai:chat"
    );
    assert_eq!(seen_support_request.trace_id, "trace-models-123");

    gateway_handle.abort();
    upstream_handle.abort();
}
