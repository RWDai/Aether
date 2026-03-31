use super::*;

#[tokio::test]
async fn gateway_skips_unsupported_local_openai_chat_sync_candidate_before_trying_next_one() {
    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        url: String,
        model: String,
        authorization: String,
    }

    fn hash_api_key(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
        )
        .expect("auth snapshot should build")
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-openai-skip-local-1".to_string(),
            provider_name: "openai".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-openai-skip-local-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: "key-openai-skip-local-1".to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 5,
            key_global_priority_by_format: Some(serde_json::json!({"openai:chat": 1})),
            model_id: "model-openai-skip-local-1".to_string(),
            global_model_id: "global-model-openai-skip-local-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "gpt-5-upstream".to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: "gpt-5-upstream".to_string(),
                priority: 1,
                api_formats: Some(vec!["openai:chat".to_string()]),
            }]),
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-skip-local-1".to_string(),
            "openai".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-skip-local-1".to_string(),
            "provider-openai-skip-local-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.skip.example".to_string(),
            None,
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-skip-local-1".to_string(),
            "provider-openai-skip-local-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-upstream-openai")
                .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"openai:chat": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
                        trace_id: parts
                            .headers
                            .get(TRACE_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        model: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("model"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        authorization: payload
                            .get("headers")
                            .and_then(|value| value.get("authorization"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "trace-openai-chat-skip-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-local-skip-123",
                            "object": "chat.completion",
                            "model": "gpt-5-upstream-backup",
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 2,
                                "completion_tokens": 3,
                                "total_tokens": 5
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 25
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-skip-local")),
        sample_auth_snapshot("api-key-openai-skip-local-1", "user-openai-skip-local-1"),
    )]));
    let mut backup_candidate_row = sample_candidate_row();
    backup_candidate_row.provider_id = "provider-openai-skip-local-2".to_string();
    backup_candidate_row.endpoint_id = "endpoint-openai-skip-local-2".to_string();
    backup_candidate_row.key_id = "key-openai-skip-local-2".to_string();
    backup_candidate_row.key_name = "backup".to_string();
    backup_candidate_row.key_internal_priority = 6;
    backup_candidate_row.key_global_priority_by_format =
        Some(serde_json::json!({"openai:chat": 2}));
    backup_candidate_row.model_id = "model-openai-skip-local-2".to_string();
    backup_candidate_row.global_model_id = "global-model-openai-skip-local-2".to_string();
    backup_candidate_row.model_provider_model_name = "gpt-5-upstream-backup".to_string();
    backup_candidate_row.model_provider_model_mappings = Some(vec![StoredProviderModelMapping {
        name: "gpt-5-upstream-backup".to_string(),
        priority: 1,
        api_formats: Some(vec!["openai:chat".to_string()]),
    }]);
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
            backup_candidate_row,
        ]));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let mut unsupported_provider = sample_provider_catalog_provider();
    unsupported_provider.provider_type = "codex".to_string();
    let mut supported_provider = sample_provider_catalog_provider();
    supported_provider.id = "provider-openai-skip-local-2".to_string();
    let mut unsupported_endpoint = sample_provider_catalog_endpoint();
    unsupported_endpoint.base_url = "https://chatgpt.com/backend-api/codex".to_string();
    let mut supported_endpoint = sample_provider_catalog_endpoint();
    supported_endpoint.id = "endpoint-openai-skip-local-2".to_string();
    supported_endpoint.provider_id = "provider-openai-skip-local-2".to_string();
    supported_endpoint.base_url = "https://api.openai.backup.example".to_string();
    let unsupported_key = sample_provider_catalog_key();
    let mut supported_key = sample_provider_catalog_key();
    supported_key.id = "key-openai-skip-local-2".to_string();
    supported_key.provider_id = "provider-openai-skip-local-2".to_string();
    supported_key.name = "backup".to_string();
    supported_key.encrypted_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-upstream-openai-backup")
            .expect("api key should encrypt");
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![unsupported_provider, supported_provider],
        vec![unsupported_endpoint, supported_endpoint],
        vec![unsupported_key, supported_key],
    ));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        crate::gateway::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEVELOPMENT_ENCRYPTION_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-skip-local",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-skip-local-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTOR_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json["model"], "gpt-5-upstream-backup");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-chat-skip-local-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.backup.example/v1/chat/completions"
    );
    assert_eq!(seen_executor_request.model, "gpt-5-upstream-backup");
    assert_eq!(
        seen_executor_request.authorization,
        "Bearer sk-upstream-openai-backup"
    );

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-skip-local-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 2);
    let skipped_candidate = stored_candidates
        .iter()
        .find(|candidate| candidate.candidate_index == 0)
        .expect("skipped candidate should exist");
    assert_eq!(skipped_candidate.status, RequestCandidateStatus::Skipped);
    assert_eq!(
        skipped_candidate.skip_reason.as_deref(),
        Some("transport_unsupported")
    );
    assert!(skipped_candidate.started_at_unix_secs.is_none());
    assert!(skipped_candidate.finished_at_unix_secs.is_some());
    let successful_candidate = stored_candidates
        .iter()
        .find(|candidate| candidate.candidate_index == 1)
        .expect("successful candidate should exist");
    assert_eq!(successful_candidate.status, RequestCandidateStatus::Success);

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_retries_next_local_openai_chat_sync_candidate_with_local_failover_only() {
    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        url: String,
        model: String,
        authorization: String,
    }

    fn hash_api_key(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
        )
        .expect("auth snapshot should build")
    }

    fn sample_candidate_row(
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
        provider_priority: i32,
        global_priority: i32,
        mapped_model: &str,
    ) -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: provider_id.to_string(),
            provider_name: "openai".to_string(),
            provider_type: "custom".to_string(),
            provider_priority,
            provider_is_active: true,
            endpoint_id: endpoint_id.to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: key_id.to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 5,
            key_global_priority_by_format: Some(
                serde_json::json!({"openai:chat": global_priority}),
            ),
            model_id: format!("model-{provider_id}"),
            global_model_id: "global-model-openai-sync-failover".to_string(),
            global_model_name: "gpt-5".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: mapped_model.to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: mapped_model.to_string(),
                priority: 1,
                api_formats: Some(vec!["openai:chat".to_string()]),
            }]),
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }

    fn sample_provider_catalog_provider(
        provider_id: &str,
        provider_name: &str,
    ) -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            provider_id.to_string(),
            provider_name.to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint(
        endpoint_id: &str,
        provider_id: &str,
        base_url: &str,
    ) -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            endpoint_id.to_string(),
            provider_id.to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            base_url.to_string(),
            None,
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key(
        key_id: &str,
        provider_id: &str,
        secret: &str,
        global_priority: i32,
    ) -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            key_id.to_string(),
            provider_id.to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, secret)
                .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"openai:chat": global_priority})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_executor = Arc::new(Mutex::new(Vec::<SeenExecutorSyncRequest>::new()));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let executor_hits = Arc::new(Mutex::new(0usize));
    let executor_hits_clone = Arc::clone(&executor_hits);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            let executor_hits_inner = Arc::clone(&executor_hits_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                let mut hits = executor_hits_inner.lock().expect("mutex should lock");
                *hits += 1;
                let attempt = *hits;
                drop(hits);

                seen_executor_inner.lock().expect("mutex should lock").push(
                    SeenExecutorSyncRequest {
                        trace_id: parts
                            .headers
                            .get(TRACE_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        model: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("model"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        authorization: payload
                            .get("headers")
                            .and_then(|value| value.get("authorization"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    },
                );

                if attempt == 1 {
                    return Json(json!({
                        "request_id": "trace-openai-chat-local-failover-123",
                        "status_code": 502,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "error": {
                                    "message": "primary unavailable"
                                }
                            }
                        },
                        "telemetry": {
                            "elapsed_ms": 9
                        }
                    }));
                }

                Json(json!({
                    "request_id": "trace-openai-chat-local-failover-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-local-failover-123",
                            "object": "chat.completion",
                            "model": "gpt-5-upstream-backup",
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 2,
                                "completion_tokens": 4,
                                "total_tokens": 6
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 19
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-local-failover")),
        sample_auth_snapshot(
            "api-key-openai-local-failover-1",
            "user-openai-local-failover-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(
                "provider-openai-local-primary",
                "endpoint-openai-local-primary",
                "key-openai-local-primary",
                10,
                1,
                "gpt-5-upstream-primary",
            ),
            sample_candidate_row(
                "provider-openai-local-backup",
                "endpoint-openai-local-backup",
                "key-openai-local-backup",
                10,
                2,
                "gpt-5-upstream-backup",
            ),
        ]));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider_catalog_provider("provider-openai-local-primary", "openai"),
            sample_provider_catalog_provider("provider-openai-local-backup", "openai"),
        ],
        vec![
            sample_provider_catalog_endpoint(
                "endpoint-openai-local-primary",
                "provider-openai-local-primary",
                "https://api.openai.primary.example",
            ),
            sample_provider_catalog_endpoint(
                "endpoint-openai-local-backup",
                "provider-openai-local-backup",
                "https://api.openai.backup.example",
            ),
        ],
        vec![
            sample_provider_catalog_key(
                "key-openai-local-primary",
                "provider-openai-local-primary",
                "sk-upstream-openai-primary",
                1,
            ),
            sample_provider_catalog_key(
                "key-openai-local-backup",
                "provider-openai-local-backup",
                "sk-upstream-openai-backup",
                2,
            ),
        ],
    ));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        crate::gateway::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEVELOPMENT_ENCRYPTION_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-local-failover",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-local-failover-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTOR_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json["model"], "gpt-5-upstream-backup");

    let seen_executor_requests = seen_executor.lock().expect("mutex should lock").clone();
    assert_eq!(seen_executor_requests.len(), 2);
    assert_eq!(
        seen_executor_requests[0].trace_id,
        "trace-openai-chat-local-failover-123"
    );
    assert_eq!(
        seen_executor_requests[0].url,
        "https://api.openai.primary.example/v1/chat/completions"
    );
    assert_eq!(
        seen_executor_requests[0].authorization,
        "Bearer sk-upstream-openai-primary"
    );
    assert_eq!(
        seen_executor_requests[1].url,
        "https://api.openai.backup.example/v1/chat/completions"
    );
    assert_eq!(seen_executor_requests[1].model, "gpt-5-upstream-backup");
    assert_eq!(
        seen_executor_requests[1].authorization,
        "Bearer sk-upstream-openai-backup"
    );
    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-local-failover-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 2);
    assert_eq!(stored_candidates[0].candidate_index, 0);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);
    assert_eq!(stored_candidates[0].status_code, Some(502));
    assert_eq!(
        stored_candidates[0].error_message.as_deref(),
        Some("primary unavailable")
    );
    assert_eq!(stored_candidates[1].candidate_index, 1);
    assert_eq!(stored_candidates[1].status, RequestCandidateStatus::Success);
    assert_eq!(stored_candidates[1].status_code, Some(200));

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*executor_hits.lock().expect("mutex should lock"), 2);
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
