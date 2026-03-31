use super::*;
use crate::gateway::data::GatewayDataState;
use aether_data::repository::candidates::{
    InMemoryRequestCandidateRepository, RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data::repository::usage::InMemoryUsageReadRepository;

#[tokio::test]
async fn gateway_executes_openai_cli_sync_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest;

    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                        "user_id": "user-cli-stream-sync-direct-123",
                        "api_key_id": "key-cli-stream-sync-direct-123",
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
                        "request_id": "req-openai-cli-stream-sync-direct-123",
                        "candidate_id": "cand-openai-cli-stream-sync-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-openai-cli-stream-sync-direct-123",
                        "key_id": "key-openai-cli-stream-sync-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello",
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-cli-stream-sync-direct-123",
                        "api_key_id": "key-cli-stream-sync-direct-123",
                        "provider_id": "provider-openai-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-openai-cli-stream-sync-direct-123",
                        "key_id": "key-openai-cli-stream-sync-direct-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "request_id": "req-openai-cli-stream-sync-direct-123",
                        "model": "gpt-5",
                        "has_envelope": false,
                        "needs_conversion": false
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    let _ = parts;
                    let _ = payload;
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"ignored-cli-finalize-response\",\"object\":\"response\",\"output\":[]}",
                        ))
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
                "request_id": "req-openai-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: response.created\n",
                            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
                            "event: response.output_text.delta\n",
                            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
                            "event: response.completed\n",
                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 29
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
        .header(TRACE_ID_HEADER, "trace-openai-cli-stream-sync-direct-123")
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
            "id": "resp_stream_001",
            "object": "response",
            "model": "gpt-5",
            "status": "completed",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
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
            .list_by_request_id("req-openai-cli-stream-sync-direct-123")
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
    assert!(
        seen_finalize.lock().expect("mutex should lock").is_none(),
        "finalize-sync should not be called when local finalize can downgrade to success report"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_kiro_claude_cli_sync_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    fn crc32(data: &[u8]) -> u32 {
        let mut crc = 0xffff_ffffu32;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = if crc & 1 == 1 { 0xedb8_8320 } else { 0 };
                crc = (crc >> 1) ^ mask;
            }
        }
        !crc
    }

    fn encode_string_header(name: &str, value: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        out.push(7);
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn encode_event_frame(
        message_type: &str,
        event_type: Option<&str>,
        payload: serde_json::Value,
    ) -> Vec<u8> {
        let mut headers = encode_string_header(":message-type", message_type);
        if let Some(event_type) = event_type {
            headers.extend_from_slice(&encode_string_header(":event-type", event_type));
        }
        let payload = serde_json::to_vec(&payload).expect("payload should encode");
        let total_len = 12 + headers.len() + payload.len() + 4;
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(total_len as u32).to_be_bytes());
        out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        let prelude_crc = crc32(&out[..8]);
        out.extend_from_slice(&prelude_crc.to_be_bytes());
        out.extend_from_slice(&headers);
        out.extend_from_slice(&payload);
        let message_crc = crc32(&out);
        out.extend_from_slice(&message_crc.to_be_bytes());
        out
    }

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest;

    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
    let seen_report = Arc::new(Mutex::new(None::<serde_json::Value>));
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
                    "route_family": "claude",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "claude:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-kiro-cli-sync-local-123",
                        "api_key_id": "key-kiro-cli-sync-local-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "claude_cli_sync",
                    "plan": {
                        "request_id": "req-kiro-cli-sync-local-123",
                        "candidate_id": "cand-kiro-cli-sync-local-123",
                        "provider_name": "kiro",
                        "provider_id": "provider-kiro-cli-sync-local-123",
                        "endpoint_id": "endpoint-kiro-cli-sync-local-123",
                        "key_id": "key-kiro-cli-sync-local-123",
                        "method": "POST",
                        "url": "https://kiro.example/generateAssistantResponse",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "application/vnd.amazon.eventstream"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-sonnet-4-upstream",
                                "messages": []
                            }
                        },
                        "stream": true,
                        "client_api_format": "claude:cli",
                        "provider_api_format": "claude:cli",
                        "model_name": "claude-sonnet-4"
                    },
                    "report_kind": "claude_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-kiro-cli-sync-local-123",
                        "api_key_id": "key-kiro-cli-sync-local-123",
                        "provider_id": "provider-kiro-cli-sync-local-123",
                        "endpoint_id": "endpoint-kiro-cli-sync-local-123",
                        "key_id": "key-kiro-cli-sync-local-123",
                        "client_api_format": "claude:cli",
                        "provider_api_format": "claude:cli",
                        "request_id": "req-kiro-cli-sync-local-123",
                        "model": "claude-sonnet-4",
                        "mapped_model": "claude-sonnet-4-upstream",
                        "has_envelope": true,
                        "envelope_name": "kiro:generateAssistantResponse",
                        "needs_conversion": false,
                        "original_request_body": {
                            "model": "claude-sonnet-4",
                            "messages": []
                        }
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |_request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"ignored-kiro-cli-finalize-response\"}",
                        ))
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
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") = Some(payload);
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
            let kiro_frames = [
                encode_event_frame(
                    "event",
                    Some("assistantResponseEvent"),
                    json!({"content": "Hello from Kiro"}),
                ),
                encode_event_frame(
                    "event",
                    Some("contextUsageEvent"),
                    json!({"contextUsagePercentage": 1.0}),
                ),
            ]
            .concat();
            Json(json!({
                "request_id": "req-kiro-cli-sync-local-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/vnd.amazon.eventstream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(kiro_frames)
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
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer client-key")
        .header("anthropic-beta", "output-128k-2025-02-19")
        .header(TRACE_ID_HEADER, "trace-kiro-cli-sync-local-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    let status = response.status();
    let response_text = response.text().await.expect("body should read");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected gateway response: {response_text}"
    );
    let response_json: serde_json::Value =
        serde_json::from_str(&response_text).expect("body should parse");
    let response_id = response_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist");
    assert_eq!(
        response_json,
        json!({
            "id": response_id,
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [{
                "type": "text",
                "text": "Hello from Kiro"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 2000,
                "output_tokens": 4
            }
        })
    );
    assert!(
        response_json
            .get("id")
            .and_then(|value| value.as_str())
            .is_some_and(|value| value.starts_with("msg_")),
        "response id should be generated by the local Kiro rewriter"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("req-kiro-cli-sync-local-123")
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
    assert!(
        seen_finalize.lock().expect("mutex should lock").is_none(),
        "finalize-sync should not be called when local finalize can aggregate Kiro eventstream"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
