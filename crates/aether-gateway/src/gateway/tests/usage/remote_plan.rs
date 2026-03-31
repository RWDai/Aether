use super::*;

#[tokio::test]
#[ignore = "ai remote decision/plan fallback removed from hot path"]
async fn gateway_handles_remote_plan_sync_report_without_python_gateway_when_usage_runtime_enabled()
{
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-usage-remote-plan-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-123",
                        "endpoint_id": "endpoint-usage-remote-plan-123",
                        "key_id": "key-usage-remote-plan-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_success",
                    "report_context": {
                        "user_id": "user-usage-remote-plan-123",
                        "api_key_id": "api-key-usage-remote-plan-123",
                        "request_id": "req-usage-remote-plan-123",
                        "model": "gpt-5",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-123",
                        "endpoint_id": "endpoint-usage-remote-plan-123",
                        "key_id": "key-usage-remote-plan-123",
                        "provider_api_format": "openai:chat",
                        "client_api_format": "openai:chat",
                        "mapped_model": "gpt-5"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-usage-remote-plan-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-usage-remote-plan-123",
                        "usage": {
                            "input_tokens": 5,
                            "output_tokens": 8,
                            "total_tokens": 13
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 27
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    )
    .with_usage_runtime_for_tests(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    });
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let mut stored_usage = None;
    for _ in 0..50 {
        stored_usage = usage_repository
            .find_by_request_id("req-usage-remote-plan-123")
            .await
            .expect("usage lookup should succeed");
        if stored_usage.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored_usage = stored_usage.expect("usage should be recorded");
    assert_eq!(stored_usage.status, "completed");
    assert_eq!(stored_usage.total_tokens, 13);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("req-usage-remote-plan-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);
    assert_eq!(
        stored_candidates[0].provider_id.as_deref(),
        Some("provider-usage-remote-plan-123")
    );
    assert_eq!(stored_candidates[0].candidate_index, 0);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
#[ignore = "ai remote decision/plan fallback removed from hot path"]
async fn gateway_handles_remote_plan_stream_report_without_python_gateway_when_usage_runtime_enabled(
) {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream",
                    "plan_kind": "openai_chat_stream",
                    "plan": {
                        "request_id": "req-usage-remote-plan-stream-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-stream-123",
                        "endpoint_id": "endpoint-usage-remote-plan-stream-123",
                        "key_id": "key-usage-remote-plan-stream-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "messages": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_stream_success",
                    "report_context": {
                        "user_id": "user-usage-remote-plan-stream-123",
                        "api_key_id": "api-key-usage-remote-plan-stream-123",
                        "request_id": "req-usage-remote-plan-stream-123",
                        "candidate_id": "cand-usage-remote-plan-stream-123",
                        "model": "gpt-5",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-stream-123",
                        "endpoint_id": "endpoint-usage-remote-plan-stream-123",
                        "key_id": "key-usage-remote-plan-stream-123",
                        "provider_api_format": "openai:chat",
                        "client_api_format": "openai:chat",
                        "mapped_model": "gpt-5"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"chatcmpl-usage-remote-plan-stream-123\\\",\\\"usage\\\":{\\\"input_tokens\\\":3,\\\"output_tokens\\\":6,\\\"total_tokens\\\":9}}\\n\\n\"}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DONE]\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":28,\"ttfb_ms\":9}}}\n",
                "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
            );
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(frames))
                .expect("response should build");
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson"),
            );
            response
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    )
    .with_usage_runtime_for_tests(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    });
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body_text = response.text().await.expect("stream body should read");
    assert_eq!(
        body_text,
        "data: {\"id\":\"chatcmpl-usage-remote-plan-stream-123\",\"usage\":{\"input_tokens\":3,\"output_tokens\":6,\"total_tokens\":9}}\n\ndata: [DONE]\n\n"
    );

    let mut stored_usage = None;
    for _ in 0..50 {
        stored_usage = usage_repository
            .find_by_request_id("req-usage-remote-plan-stream-123")
            .await
            .expect("usage lookup should succeed");
        if stored_usage.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored_usage = stored_usage.expect("usage should be recorded");
    assert_eq!(stored_usage.status, "completed");
    assert_eq!(stored_usage.total_tokens, 9);
    assert_eq!(stored_usage.first_byte_time_ms, Some(9));
    assert!(stored_usage.is_stream);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("req-usage-remote-plan-stream-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].id, "cand-usage-remote-plan-stream-123");
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
#[ignore = "ai remote decision/plan fallback removed from hot path"]
async fn gateway_handles_remote_plan_sync_error_report_without_python_gateway_when_usage_runtime_enabled(
) {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-usage-remote-plan-error-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-error-123",
                        "endpoint_id": "endpoint-usage-remote-plan-error-123",
                        "key_id": "key-usage-remote-plan-error-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_success",
                    "report_context": {
                        "user_id": "user-usage-remote-plan-error-123",
                        "api_key_id": "api-key-usage-remote-plan-error-123",
                        "request_id": "req-usage-remote-plan-error-123",
                        "model": "gpt-5",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-remote-plan-error-123",
                        "endpoint_id": "endpoint-usage-remote-plan-error-123",
                        "key_id": "key-usage-remote-plan-error-123",
                        "provider_api_format": "openai:chat",
                        "client_api_format": "openai:chat",
                        "mapped_model": "gpt-5"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-usage-remote-plan-error-123",
                "status_code": 429,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "error": {
                            "message": "rate limited"
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 23
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    )
    .with_usage_runtime_for_tests(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    });
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "error": {
                "message": "rate limited"
            }
        })
    );

    let mut stored_usage = None;
    for _ in 0..50 {
        stored_usage = usage_repository
            .find_by_request_id("req-usage-remote-plan-error-123")
            .await
            .expect("usage lookup should succeed");
        if stored_usage.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored_usage = stored_usage.expect("usage should be recorded");
    assert_eq!(stored_usage.status, "failed");
    assert_eq!(stored_usage.status_code, Some(429));
    assert_eq!(stored_usage.error_message.as_deref(), Some("rate limited"));

    let stored_candidates = request_candidate_repository
        .list_by_request_id("req-usage-remote-plan-error-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);
    assert_eq!(stored_candidates[0].candidate_index, 0);
    assert_eq!(
        stored_candidates[0].provider_id.as_deref(),
        Some("provider-usage-remote-plan-error-123")
    );
    assert_eq!(
        stored_candidates[0].error_message.as_deref(),
        Some("rate limited")
    );

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
#[ignore = "ai remote decision/plan fallback removed from hot path"]
async fn gateway_handles_remote_plan_sync_report_locally_when_execution_seeds_candidate_slot() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-usage-weak-plan-sync-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-weak-plan-sync-123",
                        "endpoint_id": "endpoint-usage-weak-plan-sync-123",
                        "key_id": "key-usage-weak-plan-sync-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_success",
                    "report_context": {
                        "request_id": "req-usage-weak-plan-sync-123",
                        "client_api_format": "openai:chat"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-usage-weak-plan-sync-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-usage-weak-plan-sync-123",
                        "usage": {
                            "input_tokens": 3,
                            "output_tokens": 5,
                            "total_tokens": 8
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 21
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    )
    .with_usage_runtime_for_tests(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    });
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("req-usage-weak-plan-sync-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(
        stored_candidates[0].provider_id.as_deref(),
        Some("provider-usage-weak-plan-sync-123")
    );
    assert_eq!(
        stored_candidates[0].endpoint_id.as_deref(),
        Some("endpoint-usage-weak-plan-sync-123")
    );
    assert_eq!(
        stored_candidates[0].key_id.as_deref(),
        Some("key-usage-weak-plan-sync-123")
    );
    assert_eq!(
        stored_candidates[0].status,
        aether_data::repository::candidates::RequestCandidateStatus::Success
    );

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
