use super::*;

#[tokio::test]
async fn gateway_records_usage_for_direct_executor_sync_when_runtime_enabled() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-usage-sync-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-sync-123",
                        "endpoint_id": "endpoint-usage-sync-123",
                        "key_id": "key-usage-sync-123",
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
                        "user_id": "user-usage-sync-123",
                        "api_key_id": "api-key-usage-sync-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-sync-123",
                        "endpoint_id": "endpoint-usage-sync-123",
                        "key_id": "key-usage-sync-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model": "gpt-5",
                        "mapped_model": "gpt-5"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-usage-sync-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-usage-sync-123",
                        "usage": {
                            "input_tokens": 3,
                            "output_tokens": 5,
                            "total_tokens": 8
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 45
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state =
        AppState::new_with_executor(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway state should build")
            .with_usage_data_repository_for_tests(usage_repository.clone())
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

    let mut stored = None;
    for _ in 0..50 {
        stored = usage_repository
            .find_by_request_id("req-usage-sync-123")
            .await
            .expect("usage lookup should succeed");
        if stored.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored = stored.expect("usage should be recorded");
    assert_eq!(stored.status, "completed");
    assert_eq!(stored.billing_status, "pending");
    assert_eq!(stored.total_tokens, 8);
    assert_eq!(stored.response_time_ms, Some(45));
    assert_eq!(stored.user_id.as_deref(), Some("user-usage-sync-123"));

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_records_usage_for_direct_executor_stream_when_runtime_enabled() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream",
                    "plan_kind": "openai_chat_stream",
                    "plan": {
                        "request_id": "req-usage-stream-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-stream-123",
                        "endpoint_id": "endpoint-usage-stream-123",
                        "key_id": "key-usage-stream-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
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
                        "user_id": "user-usage-stream-123",
                        "api_key_id": "api-key-usage-stream-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-stream-123",
                        "endpoint_id": "endpoint-usage-stream-123",
                        "key_id": "key-usage-stream-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model": "gpt-5",
                        "mapped_model": "gpt-5"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        );

    let executor = Router::new().route(
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"chatcmpl-usage-stream-123\\\",\\\"usage\\\":{\\\"input_tokens\\\":2,\\\"output_tokens\\\":4,\\\"total_tokens\\\":6}}\\n\\n\"}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DONE]\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":51,\"ttfb_ms\":19}}}\n",
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
    let gateway_state =
        AppState::new_with_executor(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway state should build")
            .with_usage_data_repository_for_tests(usage_repository.clone())
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
    let _ = response.text().await.expect("stream body should read");

    let mut stored = None;
    for _ in 0..50 {
        stored = usage_repository
            .find_by_request_id("req-usage-stream-123")
            .await
            .expect("usage lookup should succeed");
        if stored.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored = stored.expect("usage should be recorded");
    assert_eq!(stored.status, "completed");
    assert_eq!(stored.billing_status, "pending");
    assert_eq!(stored.total_tokens, 6);
    assert_eq!(stored.first_byte_time_ms, Some(19));
    assert_eq!(stored.is_stream, true);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
