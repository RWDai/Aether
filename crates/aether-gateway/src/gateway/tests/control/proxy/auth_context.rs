use super::*;

#[tokio::test]
async fn gateway_uses_data_backed_trusted_auth_context_without_calling_control_auth_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenPublicRequest {
        trusted_user_id: String,
        trusted_api_key_id: String,
        trusted_balance_remaining: String,
        trusted_access_allowed: String,
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
                            "user_id": "user-from-control",
                            "api_key_id": "key-from-control",
                            "balance_remaining": 99.0,
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
                        });
                    (
                        StatusCode::OK,
                        [(GATEWAY_HEADER, "python-upstream")],
                        Body::from("proxied"),
                    )
                }
            }),
        );

    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_currently_usable_auth_snapshot("key-123", "user-123"),
    )]));
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url.clone(), Some(upstream_url))
            .expect("gateway state should build")
            .with_auth_api_key_data_reader_for_tests(repository),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(TRACE_ID_HEADER, "trace-control-data-auth-1")
        .header(crate::gateway::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_AUTH_USER_ID_HEADER, "user-123")
        .header(TRUSTED_AUTH_API_KEY_ID_HEADER, "key-123")
        .header(TRUSTED_AUTH_BALANCE_HEADER, "42.5")
        .body("{\"hello\":\"world\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*auth_context_hits.lock().expect("mutex should lock"), 0);

    let seen_public_request = seen_public
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("public request should be captured");
    assert_eq!(seen_public_request.trusted_user_id, "user-123");
    assert_eq!(seen_public_request.trusted_api_key_id, "key-123");
    assert_eq!(seen_public_request.trusted_balance_remaining, "42.5");
    assert_eq!(seen_public_request.trusted_access_allowed, "true");

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_uses_data_backed_bearer_auth_context_without_calling_control_auth_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenPublicRequest {
        trusted_user_id: String,
        trusted_api_key_id: String,
        trusted_access_allowed: String,
        authorization: String,
        x_api_key: String,
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
                            "user_id": "user-from-control",
                            "api_key_id": "key-from-control",
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
                            trusted_access_allowed: request
                                .headers()
                                .get(TRUSTED_AUTH_ACCESS_ALLOWED_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            authorization: request
                                .headers()
                                .get(http::header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            x_api_key: request
                                .headers()
                                .get("x-api-key")
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    (StatusCode::OK, Body::from("proxied"))
                }
            }),
        );

    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-live-openai-auth-123")),
        sample_currently_usable_auth_snapshot("key-123", "user-123"),
    )]));
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url.clone(), Some(upstream_url))
            .expect("gateway state should build")
            .with_auth_api_key_data_reader_for_tests(repository),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-live-openai-auth-123",
        )
        .header("x-api-key", "should-not-forward")
        .header(TRACE_ID_HEADER, "trace-control-bearer-auth-1")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let seen_public_request = seen_public
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("public request should be captured");
    assert_eq!(seen_public_request.trusted_user_id, "user-123");
    assert_eq!(seen_public_request.trusted_api_key_id, "key-123");
    assert_eq!(seen_public_request.trusted_access_allowed, "true");
    assert_eq!(seen_public_request.authorization, "");
    assert_eq!(seen_public_request.x_api_key, "");
    assert_eq!(*auth_context_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_strips_gemini_query_api_key_after_data_backed_auth_context() {
    #[derive(Debug, Clone)]
    struct SeenPublicRequest {
        query: String,
        trusted_user_id: String,
        trusted_api_key_id: String,
        x_goog_api_key: String,
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
                    Json(json!({"auth_context": null}))
                }
            }),
        )
        .route(
            "/v1beta/models/{*path}",
            any(move |request: Request| {
                let seen_public_inner = Arc::clone(&seen_public_clone);
                async move {
                    *seen_public_inner.lock().expect("mutex should lock") =
                        Some(SeenPublicRequest {
                            query: request.uri().query().unwrap_or_default().to_string(),
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
                            x_goog_api_key: request
                                .headers()
                                .get("x-goog-api-key")
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    (StatusCode::OK, Body::from("proxied"))
                }
            }),
        );

    let mut snapshot = sample_currently_usable_auth_snapshot("key-gemini-123", "user-gemini-123");
    snapshot.api_key_allowed_providers = Some(vec!["gemini".to_string()]);
    snapshot.user_allowed_providers = Some(vec!["gemini".to_string()]);
    snapshot.api_key_allowed_api_formats = Some(vec!["gemini:chat".to_string()]);
    snapshot.user_allowed_api_formats = Some(vec!["gemini:chat".to_string()]);
    snapshot.api_key_allowed_models = Some(vec!["gemini-2.5-pro".to_string()]);
    snapshot.user_allowed_models = Some(vec!["gemini-2.5-pro".to_string()]);
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("gemini-client-key-123")),
        snapshot,
    )]));
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url.clone(), Some(upstream_url))
            .expect("gateway state should build")
            .with_auth_api_key_data_reader_for_tests(repository),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent?key=gemini-client-key-123&alt=sse"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "secondary-header-key")
        .header(TRACE_ID_HEADER, "trace-control-gemini-strip-1")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*auth_context_hits.lock().expect("mutex should lock"), 0);

    let seen_public_request = seen_public
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("public request should be captured");
    assert_eq!(seen_public_request.query, "alt=sse");
    assert_eq!(seen_public_request.trusted_user_id, "user-gemini-123");
    assert_eq!(seen_public_request.trusted_api_key_id, "key-gemini-123");
    assert_eq!(seen_public_request.x_goog_api_key, "");

    gateway_handle.abort();
    upstream_handle.abort();
}
