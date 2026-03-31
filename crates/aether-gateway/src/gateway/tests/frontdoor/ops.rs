use super::*;

#[tokio::test]
async fn gateway_exposes_frontdoor_manifest_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some("http://127.0.0.1:19091".to_string()),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}{FRONTDOOR_MANIFEST_PATH}"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["component"], "aether-gateway");
    assert_eq!(payload["mode"], "compatibility_frontdoor");
    assert_eq!(
        payload["entrypoints"]["public_manifest"],
        FRONTDOOR_MANIFEST_PATH
    );
    assert_eq!(payload["entrypoints"]["readiness"], READYZ_PATH);
    assert_eq!(payload["entrypoints"]["health"], "/_gateway/health");
    assert_eq!(
        payload["rust_frontdoor"]["capabilities"]["public_proxy_catch_all"],
        true
    );
    assert_eq!(
        payload["python_host_boundary"]["replaceable_shell"]["status"],
        "should_move_to_rust_frontdoor"
    );
    let owned_routes = payload["rust_frontdoor"]["owned_route_patterns"]
        .as_array()
        .expect("owned route patterns should be an array");
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/chat/completions"));
    assert!(owned_routes.iter().any(|value| value == "/v1/messages"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/messages/count_tokens"));
    assert!(owned_routes.iter().any(|value| value == "/v1/responses"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/responses/compact"));
    assert!(owned_routes.iter().any(|value| value == "/health"));
    assert!(owned_routes.iter().any(|value| value == "/v1/health"));
    assert!(owned_routes.iter().any(|value| value == "/v1/providers"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/providers/{path...}"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/test-connection"));
    assert!(owned_routes.iter().any(|value| value == "/test-connection"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/public/providers"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/oauth/providers"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/oauth/{provider_type}/authorize"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/oauth/{provider_type}/callback"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/user/oauth/bindable-providers"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/user/oauth/links"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/user/oauth/{provider_type}/bind-token"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/user/oauth/{provider_type}/bind"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/user/oauth/{provider_type}"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/capabilities"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/public/health/api-formats"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/modules/auth-status"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/internal/gateway/{path...}"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/internal/hub/heartbeat"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/internal/hub/node-status"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/capabilities/user-configurable"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/api/capabilities/model/{path...}"));
    assert!(owned_routes.iter().any(|value| value == "/v1/models"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/models/{path...}"));
    assert!(owned_routes.iter().any(|value| value == "/v1beta/models"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/models/{path...}"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/models/{model}:generateContent"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/models/{model}:streamGenerateContent"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/models/{model}:predictLongRunning"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/operations/{id}"));
    assert!(owned_routes.iter().any(|value| value == "/v1/videos"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1/videos/{path...}"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/upload/v1beta/files"));
    assert!(owned_routes.iter().any(|value| value == "/v1beta/files"));
    assert!(owned_routes
        .iter()
        .any(|value| value == "/v1beta/files/{path...}"));
    assert_eq!(
        payload["python_host_boundary"]["legacy_bridge"]["status"],
        "remove_after_rust_control_plane_cutover"
    );
    assert_eq!(payload["features"]["control_api_configured"], true);
    assert_eq!(payload["features"]["executor_api_configured"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_cors_preflight_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let state = AppState::new(upstream_url, None)
        .expect("state should build")
        .with_frontdoor_cors_config(
            FrontdoorCorsConfig::new(vec!["http://localhost:3000".to_string()], true)
                .expect("cors config should build"),
        );
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!("{gateway_url}/v1/chat/completions"),
        )
        .header("origin", "http://localhost:3000")
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization,content-type",
        )
        .send()
        .await
        .expect("preflight should succeed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .expect("allow origin header"),
        "http://localhost:3000"
    );
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-credentials")
            .expect("allow credentials header"),
        "true"
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_adds_cors_headers_to_proxied_responses() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        any(|_request: Request| async move { (StatusCode::OK, Body::from("proxied")) }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let state = AppState::new(upstream_url, None)
        .expect("state should build")
        .with_frontdoor_cors_config(
            FrontdoorCorsConfig::new(vec!["http://localhost:3000".to_string()], true)
                .expect("cors config should build"),
        );
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header("origin", "http://localhost:3000")
        .send()
        .await
        .expect("proxy request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .expect("allow origin header"),
        "http://localhost:3000"
    );
    assert_eq!(
        response
            .headers()
            .get("access-control-expose-headers")
            .expect("expose headers header"),
        "*"
    );

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_applies_explicit_user_rpm_preflight_before_proxy() {
    #[derive(Clone, Debug)]
    struct SeenRequest {
        preflight: String,
    }

    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let seen_request = Arc::new(Mutex::new(None::<SeenRequest>));
    let seen_request_clone = Arc::clone(&seen_request);
    let upstream = Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            let seen_request_inner = Arc::clone(&seen_request_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                *seen_request_inner.lock().expect("mutex should lock") = Some(SeenRequest {
                    preflight: request
                        .headers()
                        .get(TRUSTED_RATE_LIMIT_PREFLIGHT_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                });
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-frontdoor-explicit")),
        explicit_user_limit_snapshot("key-frontdoor-1", "user-frontdoor-1"),
    )]));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let state = AppState::new(upstream_url, None)
        .expect("state should build")
        .with_auth_api_key_data_reader_for_tests(repository)
        .with_frontdoor_user_rpm_config(FrontdoorUserRpmConfig::new(60, 120, false));
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let first = client
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header("x-api-key", "sk-frontdoor-explicit")
        .json(&json!({"model":"gpt-5","messages":[]}))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 1);
    let seen_request = seen_request
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("upstream request should be captured");
    assert_eq!(seen_request.preflight, "true");

    let second = client
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header("x-api-key", "sk-frontdoor-explicit")
        .json(&json!({"model":"gpt-5","messages":[]}))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        second
            .headers()
            .get("x-ratelimit-scope")
            .and_then(|value| value.to_str().ok()),
        Some("user")
    );
    let retry_after = second
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("retry-after header should be present");
    assert!((1..=60).contains(&retry_after));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 1);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_applies_system_default_user_rpm_preflight_before_proxy() {
    #[derive(Clone, Debug)]
    struct SeenRequest {
        preflight: String,
    }

    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let seen_request = Arc::new(Mutex::new(None::<SeenRequest>));
    let seen_request_clone = Arc::clone(&seen_request);
    let upstream = Router::new().route(
        "/v1/chat/completions",
        any(move |request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            let seen_request_inner = Arc::clone(&seen_request_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                *seen_request_inner.lock().expect("mutex should lock") = Some(SeenRequest {
                    preflight: request
                        .headers()
                        .get("x-aether-rate-limit-preflight")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                });
                (StatusCode::OK, Json(serde_json::json!({"ok": true})))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let api_key = "ak-frontdoor-default-rpm";
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        system_default_user_limit_snapshot(
            "key-frontdoor-default-rpm",
            "user-frontdoor-default-rpm",
        ),
    )]));
    let state = AppState::new(upstream_url, None)
        .expect("state should build")
        .with_auth_api_key_data_reader_for_tests(repository)
        .with_frontdoor_user_rpm_config(FrontdoorUserRpmConfig::new(60, 120, false))
        .with_frontdoor_system_default_rpm_for_tests(1);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let first = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {api_key}"))
        .json(&serde_json::json!({"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}))
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 1);
    let seen = seen_request
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("upstream request should be captured");
    assert_eq!(seen.preflight, "true");

    let second = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {api_key}"))
        .json(&serde_json::json!({"model":"gpt-5","messages":[{"role":"user","content":"again"}]}))
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        second
            .headers()
            .get("x-ratelimit-scope")
            .and_then(|value| value.to_str().ok()),
        Some("user")
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 1);

    gateway_handle.abort();
    upstream_handle.abort();
}
