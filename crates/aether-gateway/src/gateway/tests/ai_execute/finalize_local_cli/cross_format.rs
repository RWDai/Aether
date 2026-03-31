use super::*;
use crate::gateway::data::GatewayDataState;
use aether_data::repository::candidates::{
    InMemoryRequestCandidateRepository, RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data::repository::usage::InMemoryUsageReadRepository;

#[tokio::test]
async fn gateway_executes_openai_cli_cross_format_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        client_id: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-cli-xfmt-stream-123",
                        "api_key_id": "key-openai-cli-xfmt-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_cli_sync",
                    "plan": {
                        "request_id": "req-openai-cli-xfmt-stream-123",
                        "candidate_id": "cand-openai-cli-xfmt-stream-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-openai-cli-xfmt-stream-123",
                        "endpoint_id": "endpoint-openai-cli-xfmt-stream-123",
                        "key_id": "key-openai-cli-xfmt-stream-123",
                        "method": "POST",
                        "url": "https://api.gemini.example/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "contents": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-cli-xfmt-stream-123",
                        "api_key_id": "key-openai-cli-xfmt-stream-123",
                        "provider_id": "provider-openai-cli-xfmt-stream-123",
                        "endpoint_id": "endpoint-openai-cli-xfmt-stream-123",
                        "key_id": "key-openai-cli-xfmt-stream-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-openai-cli-xfmt-stream-123",
                        "model": "gpt-5",
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(|_request: Request| async move {
                (
                    StatusCode::IM_A_TEAPOT,
                    Body::from("finalize-sync-should-not-be-hit"),
                )
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            status_code: payload
                                .get("status_code")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                            upstream_id: payload
                                .get("body_json")
                                .and_then(|value| {
                                    value
                                        .get("responseId")
                                        .or_else(|| value.get("id"))
                                })
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            client_id: payload
                                .get("client_body_json")
                                .and_then(|value| value.get("id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"fallback\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-cli-xfmt-stream-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"responseId\":\"upstream-cli-stream-123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
                            "data: {\"responseId\":\"upstream-cli-stream-123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 31
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-xfmt-stream-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "upstream-cli-stream-123",
            "object": "response",
            "status": "completed",
            "model": "gemini-2.5-pro-upstream",
            "output": [{
                "type": "message",
                "id": "upstream-cli-stream-123_msg",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello Gemini CLI",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 2,
                "output_tokens": 3,
                "total_tokens": 5
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("req-openai-cli-xfmt-stream-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_cli_cross_format_function_call_upstream_stream_via_local_finalize_response(
) {
    use base64::Engine as _;

    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-cli-xfmt-tool-stream-123",
                        "api_key_id": "key-openai-cli-xfmt-tool-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_cli_sync",
                    "plan": {
                        "request_id": "req-openai-cli-xfmt-tool-stream-123",
                        "candidate_id": "cand-openai-cli-xfmt-tool-stream-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-openai-cli-xfmt-tool-stream-123",
                        "endpoint_id": "endpoint-openai-cli-xfmt-tool-stream-123",
                        "key_id": "key-openai-cli-xfmt-tool-stream-123",
                        "method": "POST",
                        "url": "https://api.gemini.example/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "contents": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-cli-xfmt-tool-stream-123",
                        "api_key_id": "key-openai-cli-xfmt-tool-stream-123",
                        "provider_id": "provider-openai-cli-xfmt-tool-stream-123",
                        "endpoint_id": "endpoint-openai-cli-xfmt-tool-stream-123",
                        "key_id": "key-openai-cli-xfmt-tool-stream-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-openai-cli-xfmt-tool-stream-123",
                        "model": "gpt-5",
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(|_request: Request| async move {
                (
                    StatusCode::IM_A_TEAPOT,
                    Body::from("finalize-sync-should-not-be-hit"),
                )
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
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"fallback\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-cli-xfmt-tool-stream-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"responseId\":\"upstream-cli-tool-123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"location\":\"Tokyo\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 31
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-xfmt-tool-stream-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"weather\"}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "upstream-cli-tool-123",
            "object": "response",
            "status": "completed",
            "model": "gemini-2.5-pro-upstream",
            "output": [
                {
                    "type": "message",
                    "id": "upstream-cli-tool-123_msg",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "Need a tool.",
                        "annotations": []
                    }]
                },
                {
                    "type": "function_call",
                    "call_id": "call_auto_1",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Tokyo\"}"
                }
            ],
            "usage": {
                "input_tokens": 2,
                "output_tokens": 3,
                "total_tokens": 5
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("req-openai-cli-xfmt-tool-stream-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_cli_antigravity_cross_format_upstream_stream_via_local_finalize_response(
) {
    use base64::Engine as _;

    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-cli-antigravity-xfmt-stream-123",
                        "api_key_id": "key-openai-cli-antigravity-xfmt-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_cli_sync",
                    "plan": {
                        "request_id": "req-openai-cli-antigravity-xfmt-stream-123",
                        "candidate_id": "cand-openai-cli-antigravity-xfmt-stream-123",
                        "provider_name": "antigravity",
                        "provider_id": "provider-openai-cli-antigravity-xfmt-stream-123",
                        "endpoint_id": "endpoint-openai-cli-antigravity-xfmt-stream-123",
                        "key_id": "key-openai-cli-antigravity-xfmt-stream-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent?alt=sse",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "contents": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-cli-antigravity-xfmt-stream-123",
                        "api_key_id": "key-openai-cli-antigravity-xfmt-stream-123",
                        "provider_id": "provider-openai-cli-antigravity-xfmt-stream-123",
                        "endpoint_id": "endpoint-openai-cli-antigravity-xfmt-stream-123",
                        "key_id": "key-openai-cli-antigravity-xfmt-stream-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-openai-cli-antigravity-xfmt-stream-123",
                        "model": "gpt-5",
                        "mapped_model": "claude-sonnet-4-5",
                        "has_envelope": true,
                        "envelope_name": "antigravity:v1internal",
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(|_request: Request| async move {
                (
                    StatusCode::IM_A_TEAPOT,
                    Body::from("finalize-sync-should-not-be-hit"),
                )
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
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"fallback\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-cli-antigravity-xfmt-stream-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_antigravity_cli_xfmt_123\"}\n\n",
                            "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Antigravity\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}},\"responseId\":\"resp_antigravity_cli_xfmt_123\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 31
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway state should build")
    .with_data_state_for_tests(
        GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-cli-antigravity-xfmt-stream-123",
        )
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_antigravity_cli_xfmt_123",
            "object": "response",
            "status": "completed",
            "model": "claude-sonnet-4-5",
            "output": [{
                "type": "message",
                "id": "resp_antigravity_cli_xfmt_123_msg",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello Antigravity",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 2,
                "output_tokens": 3,
                "total_tokens": 5
            }
        })
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("req-openai-cli-antigravity-xfmt-stream-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
