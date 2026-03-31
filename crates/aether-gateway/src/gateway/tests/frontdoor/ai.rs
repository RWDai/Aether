use super::*;

#[tokio::test]
async fn gateway_handles_public_openai_models_without_proxying_python_upstream() {
    let python_upstream_hits = Arc::new(Mutex::new(0usize));
    let python_upstream_hits_clone = Arc::clone(&python_upstream_hits);
    let python_upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let python_upstream_hits_inner = Arc::clone(&python_upstream_hits_clone);
            async move {
                *python_upstream_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-openai-models")),
        unrestricted_models_snapshot("key-1", "user-1"),
    )]));
    let candidate_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_models_candidate_row("provider-openai", "openai", "openai:chat", "gpt-5", 10),
            sample_models_candidate_row("provider-openai", "openai", "openai:chat", "gpt-4.1", 10),
        ]));

    let (python_upstream_url, python_upstream_handle) = start_server(python_upstream).await;
    let gateway = build_router_with_state(
        AppState::new(python_upstream_url, None)
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_minimal_candidate_selection_and_auth_for_tests(
                    candidate_repository,
                    auth_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/models"))
        .header("authorization", "Bearer sk-openai-models")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["object"], "list");
    assert_eq!(payload["data"][0]["id"], "gpt-4.1");
    assert_eq!(payload["data"][1]["id"], "gpt-5");
    assert_eq!(payload["data"][0]["owned_by"], "openai");
    assert_eq!(*python_upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    python_upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_claude_models_without_proxying_python_upstream() {
    let python_upstream_hits = Arc::new(Mutex::new(0usize));
    let python_upstream_hits_clone = Arc::clone(&python_upstream_hits);
    let python_upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let python_upstream_hits_inner = Arc::clone(&python_upstream_hits_clone);
            async move {
                *python_upstream_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                (StatusCode::OK, Json(json!({"proxied": true}))).into_response()
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-claude-models")),
        unrestricted_models_snapshot("key-claude", "user-claude"),
    )]));
    let candidate_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_models_candidate_row(
                "provider-claude",
                "claude",
                "claude:chat",
                "claude-3-7-sonnet",
                10,
            ),
            sample_models_candidate_row(
                "provider-claude",
                "claude",
                "claude:chat",
                "claude-3-5-haiku",
                10,
            ),
        ]));

    let (python_upstream_url, python_upstream_handle) = start_server(python_upstream).await;
    let gateway = build_router_with_state(
        AppState::new(python_upstream_url, None)
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_minimal_candidate_selection_and_auth_for_tests(
                    candidate_repository,
                    auth_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/models?limit=1"))
        .header("x-api-key", "sk-claude-models")
        .header("anthropic-version", "2023-06-01")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["data"][0]["id"], "claude-3-5-haiku");
    assert_eq!(payload["first_id"], "claude-3-5-haiku");
    assert_eq!(payload["last_id"], "claude-3-5-haiku");
    assert_eq!(payload["has_more"], true);
    assert_eq!(*python_upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    python_upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_gemini_models_without_proxying_python_upstream() {
    let python_upstream_hits = Arc::new(Mutex::new(0usize));
    let python_upstream_hits_clone = Arc::clone(&python_upstream_hits);
    let python_upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let python_upstream_hits_inner = Arc::clone(&python_upstream_hits_clone);
            async move {
                *python_upstream_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                (StatusCode::OK, Json(json!({"proxied": true}))).into_response()
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-gemini-models")),
        unrestricted_models_snapshot("key-gemini", "user-gemini"),
    )]));
    let candidate_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_models_candidate_row(
                "provider-gemini",
                "gemini",
                "gemini:chat",
                "gemini-2.5-flash",
                10,
            ),
            sample_models_candidate_row(
                "provider-gemini",
                "gemini",
                "gemini:chat",
                "gemini-2.5-pro",
                10,
            ),
        ]));

    let (python_upstream_url, python_upstream_handle) = start_server(python_upstream).await;
    let gateway = build_router_with_state(
        AppState::new(python_upstream_url, None)
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::gateway::data::GatewayDataState::with_minimal_candidate_selection_and_auth_for_tests(
                    candidate_repository,
                    auth_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1beta/models?pageSize=1&key=sk-gemini-models"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["models"][0]["name"], "models/gemini-2.5-flash");
    assert_eq!(payload["nextPageToken"], "1");
    assert_eq!(*python_upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    python_upstream_handle.abort();
}
