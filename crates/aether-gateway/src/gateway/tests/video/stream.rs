use aether_contracts::{StreamFrame, StreamFramePayload, StreamFrameType};
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::provider_catalog::{
    InMemoryProviderCatalogReadRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use aether_data::repository::video_tasks::{InMemoryVideoTaskRepository, UpsertVideoTask};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::*;

#[tokio::test]
#[ignore = "legacy python plan fallback removed"]
async fn gateway_executes_openai_video_content_via_executor_stream_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanStreamRequest {
        trace_id: String,
        path: String,
        auth_context_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanStreamRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-direct-123",
                        "api_key_id": "key-video-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123/content"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).unwrap_or_else(|_| json!({}));
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanStreamRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                        });
                    Json(json!({
                        "action": "executor_stream",
                        "plan_kind": "openai_video_content",
                        "plan": {
                            "request_id": "req-video-direct-123",
                            "provider_id": "provider-video-direct-123",
                            "endpoint_id": "endpoint-video-direct-123",
                            "key_id": "key-video-direct-123",
                            "provider_name": "openai",
                            "method": "GET",
                            "url": "https://cdn.example.com/video.mp4",
                            "headers": {},
                            "body": {},
                            "stream": true,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1/videos/task-123/content",
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
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorStreamRequest {
                        method: payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });

                let frames = [
                    StreamFrame {
                        frame_type: StreamFrameType::Headers,
                        payload: StreamFramePayload::Headers {
                            status_code: 200,
                            headers: std::collections::BTreeMap::from([(
                                "content-type".to_string(),
                                "video/mp4".to_string(),
                            )]),
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"openai-")),
                            text: None,
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"video")),
                            text: None,
                        },
                    },
                    StreamFrame::eof(),
                ];
                let body = frames
                    .into_iter()
                    .map(|frame| serde_json::to_string(&frame).expect("frame should serialize"))
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n";
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(body))
                    .expect("response should build");
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/videos/task-123/content?variant=video"
        ))
        .header(TRACE_ID_HEADER, "trace-video-direct-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4")
    );
    assert_eq!(
        response.bytes().await.expect("body should read"),
        Bytes::from_static(b"openai-video")
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-stream should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-video-direct-123");
    assert_eq!(seen_plan_request.path, "/v1/videos/task-123/content");
    assert!(!seen_plan_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://cdn.example.com/video.mp4"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
#[ignore = "legacy python decision fallback removed"]
async fn gateway_executes_openai_video_content_via_executor_stream_decision() {
    #[derive(Debug, Clone)]
    struct SeenDecisionStreamRequest {
        trace_id: String,
        path: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    let seen_decision = Arc::new(Mutex::new(None::<SeenDecisionStreamRequest>));
    let seen_decision_clone = Arc::clone(&seen_decision);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(|_request: Request| async move {
                Json(json!({
                    "auth_context": {
                        "user_id": "user-video-direct-123",
                        "api_key_id": "key-video-direct-123",
                        "access_allowed": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |request: Request| {
                let seen_decision_inner = Arc::clone(&seen_decision_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    *seen_decision_inner.lock().expect("mutex should lock") =
                        Some(SeenDecisionStreamRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "openai_video_content",
                        "request_id": "req-video-direct-decision-123",
                        "provider_name": "openai",
                        "provider_id": "provider-video-direct-123",
                        "endpoint_id": "endpoint-video-direct-123",
                        "key_id": "key-video-direct-123",
                        "upstream_url": "https://cdn.example.com/video.mp4",
                        "provider_api_format": "openai:video",
                        "client_api_format": "openai:video",
                        "model_name": "sora-2",
                        "provider_request_headers": {}
                    }))
                }
            }),
        )
        .route(
            "/v1/videos/task-123/content",
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
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorStreamRequest {
                        method: payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });

                let frames = [
                    StreamFrame {
                        frame_type: StreamFrameType::Headers,
                        payload: StreamFramePayload::Headers {
                            status_code: 200,
                            headers: std::collections::BTreeMap::from([(
                                "content-type".to_string(),
                                "video/mp4".to_string(),
                            )]),
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            text: Some("video-bytes".to_string()),
                            chunk_b64: None,
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Eof,
                        payload: StreamFramePayload::Eof { summary: None },
                    },
                ];
                let body = frames.into_iter().map(|frame| {
                    let line = serde_json::to_string(&frame).expect("frame should serialize");
                    Ok::<_, Infallible>(Bytes::from(format!("{line}\n")))
                });
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from_stream(futures_util::stream::iter(body)))
                    .expect("response should build");
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/videos/task-123/content"))
        .header(TRACE_ID_HEADER, "trace-video-decision-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "video-bytes"
    );

    let seen_decision_request = seen_decision
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-stream should be captured");
    assert_eq!(seen_decision_request.trace_id, "trace-video-decision-123");
    assert_eq!(seen_decision_request.path, "/v1/videos/task-123/content");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor request should be captured");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://cdn.example.com/video.mp4"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
#[ignore = "legacy create path still depended on python decision finalize flow"]
async fn gateway_executes_openai_video_content_from_local_video_task_without_decision_stream() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorStreamRequest {
        method: String,
        url: String,
    }

    let seen_decision_sync_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_sync_paths_clone = Arc::clone(&seen_decision_sync_paths);
    let decision_stream_hits = Arc::new(Mutex::new(0usize));
    let decision_stream_hits_clone = Arc::clone(&decision_stream_hits);
    let seen_executor_sync = Arc::new(Mutex::new(Vec::<SeenExecutorSyncRequest>::new()));
    let seen_executor_sync_clone = Arc::clone(&seen_executor_sync);
    let seen_executor_stream = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_stream_clone = Arc::clone(&seen_executor_stream);
    let fallback_execute_hits = Arc::new(Mutex::new(0usize));
    let fallback_execute_hits_clone = Arc::clone(&fallback_execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_create_clone = Arc::clone(&public_hits);
    let public_hits_content_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                let path = request.uri().path().to_string();
                let public_path = if path == "/v1/videos" {
                    "/v1/videos".to_string()
                } else {
                    path
                };
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-content-local-123",
                        "api_key_id": "key-video-content-local-123",
                        "access_allowed": true
                    },
                    "public_path": public_path
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_sync_paths_inner = Arc::clone(&seen_decision_sync_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    seen_decision_sync_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(
                            payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        );
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_create_sync",
                        "request_id": "req-openai-video-content-local-create-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-content-local-123",
                        "endpoint_id": "endpoint-openai-video-content-local-123",
                        "key_id": "key-openai-video-content-local-123",
                        "upstream_base_url": "https://api.openai.example",
                        "upstream_url": "https://api.openai.example/v1/videos",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "model": "sora-2",
                            "prompt": "hello"
                        },
                        "content_type": "application/json",
                        "client_api_format": "openai:video",
                        "provider_api_format": "openai:video",
                        "model_name": "sora-2",
                        "report_kind": "openai_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-content-local-123",
                            "api_key_id": "key-video-content-local-123",
                            "model": "sora-2",
                            "original_request_body": {
                                "model": "sora-2",
                                "prompt": "hello"
                            }
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_stream_hits_inner = Arc::clone(&decision_stream_hits_clone);
                async move {
                    *decision_stream_hits_inner
                        .lock()
                        .expect("mutex should lock") += 1;
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "openai_video_content",
                        "request_id": "unexpected-decision-stream-hit"
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let fallback_execute_hits_inner = Arc::clone(&fallback_execute_hits_clone);
                async move {
                    *fallback_execute_hits_inner
                        .lock()
                        .expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
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
        )
        .route(
            "/v1/videos",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_create_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        )
        .route(
            "/v1/videos/{task_id}/content",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_content_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new()
        .route(
            "/v1/execute/sync",
            any(move |request: Request| {
                let seen_executor_sync_inner = Arc::clone(&seen_executor_sync_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("executor payload should parse");
                    let method = payload
                        .get("method")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let url = payload
                        .get("url")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();
                    seen_executor_sync_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(SeenExecutorSyncRequest {
                            method: method.clone(),
                            url: url.clone(),
                        });

                    let body_json = if method == "POST" {
                        json!({
                            "id": "ext-video-task-123",
                            "status": "submitted"
                        })
                    } else {
                        json!({
                            "id": "ext-video-task-123",
                            "status": "completed",
                            "progress": 100,
                            "video_url": "https://cdn.example.com/ext-video-task-123.mp4"
                        })
                    };

                    Json(json!({
                        "request_id": "req-openai-video-content-local-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": body_json
                        },
                        "telemetry": {
                            "elapsed_ms": 31
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1/execute/stream",
            any(move |request: Request| {
                let seen_executor_stream_inner = Arc::clone(&seen_executor_stream_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("executor payload should parse");
                    *seen_executor_stream_inner
                        .lock()
                        .expect("mutex should lock") = Some(SeenExecutorStreamRequest {
                        method: payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });

                    let frames = [
                        StreamFrame {
                            frame_type: StreamFrameType::Headers,
                            payload: StreamFramePayload::Headers {
                                status_code: 200,
                                headers: std::collections::BTreeMap::from([(
                                    "content-type".to_string(),
                                    "video/mp4".to_string(),
                                )]),
                            },
                        },
                        StreamFrame {
                            frame_type: StreamFrameType::Data,
                            payload: StreamFramePayload::Data {
                                chunk_b64: Some(BASE64_STANDARD.encode(b"content-")),
                                text: None,
                            },
                        },
                        StreamFrame {
                            frame_type: StreamFrameType::Data,
                            payload: StreamFramePayload::Data {
                                chunk_b64: Some(BASE64_STANDARD.encode(b"bytes")),
                                text: None,
                            },
                        },
                        StreamFrame::eof(),
                    ];
                    let body = frames
                        .into_iter()
                        .map(|frame| serde_json::to_string(&frame).expect("frame should serialize"))
                        .collect::<Vec<_>>()
                        .join("\n")
                        + "\n";
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(body))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/x-ndjson"),
                    );
                    response
                }
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
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-content-local-create-123",
        )
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("local task id should exist")
        .to_string();

    let content_response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/videos/{local_task_id}/content?variant=video"
        ))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-content-local-stream-123",
        )
        .send()
        .await
        .expect("content request should succeed");

    assert_eq!(content_response.status(), StatusCode::OK);
    assert_eq!(
        content_response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4")
    );
    assert_eq!(
        content_response.bytes().await.expect("body should read"),
        Bytes::from_static(b"content-bytes")
    );
    assert_eq!(
        seen_decision_sync_paths
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        ["/v1/videos"]
    );
    assert_eq!(*decision_stream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        seen_executor_sync
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        [
            SeenExecutorSyncRequest {
                method: "POST".to_string(),
                url: "https://api.openai.example/v1/videos".to_string(),
            },
            SeenExecutorSyncRequest {
                method: "GET".to_string(),
                url: "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
            }
        ]
    );
    let seen_stream_request = seen_executor_stream
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(seen_stream_request.method, "GET");
    assert_eq!(
        seen_stream_request.url,
        "https://cdn.example.com/ext-video-task-123.mp4"
    );
    assert_eq!(*fallback_execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_video_content_from_reconstructed_legacy_data_task_without_decision_stream(
) {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorStreamRequest {
        method: String,
        url: String,
    }

    fn sample_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-video-content-legacy-1".to_string(),
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

    fn sample_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-video-content-legacy-1".to_string(),
            "provider-openai-video-content-legacy-1".to_string(),
            "openai:video".to_string(),
            Some("openai".to_string()),
            Some("video".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example".to_string(),
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

    fn sample_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-video-content-legacy-1".to_string(),
            "provider-openai-video-content-legacy-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(json!(["openai:video"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-upstream-openai-video")
                .expect("api key should encrypt"),
            None,
            None,
            Some(json!({"openai:video": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let decision_stream_hits = Arc::new(Mutex::new(0usize));
    let decision_stream_hits_clone = Arc::clone(&decision_stream_hits);
    let execute_stream_hits = Arc::new(Mutex::new(0usize));
    let execute_stream_hits_clone = Arc::clone(&execute_stream_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let seen_executor_stream = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_stream_clone = Arc::clone(&seen_executor_stream);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-content-legacy-123",
                        "api_key_id": "key-video-content-legacy-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_stream_hits_inner = Arc::clone(&decision_stream_hits_clone);
                async move {
                    *decision_stream_hits_inner
                        .lock()
                        .expect("mutex should lock") += 1;
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "openai_video_content",
                        "request_id": "unexpected-decision-stream-hit"
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-stream",
            any(move |_request: Request| {
                let execute_stream_hits_inner = Arc::clone(&execute_stream_hits_clone);
                async move {
                    *execute_stream_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("fallback"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        )
        .route(
            "/v1/videos/task-legacy-content-123/content",
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
        any(move |request: Request| {
            let seen_executor_stream_inner = Arc::clone(&seen_executor_stream_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_stream_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutorStreamRequest {
                    method: payload
                        .get("method")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    url: payload
                        .get("url")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });

                let frames = [
                    StreamFrame {
                        frame_type: StreamFrameType::Headers,
                        payload: StreamFramePayload::Headers {
                            status_code: 200,
                            headers: std::collections::BTreeMap::from([(
                                "content-type".to_string(),
                                "video/mp4".to_string(),
                            )]),
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"legacy-")),
                            text: None,
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"content")),
                            text: None,
                        },
                    },
                    StreamFrame::eof(),
                ];
                let body = frames
                    .into_iter()
                    .map(|frame| serde_json::to_string(&frame).expect("frame should serialize"))
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n";
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(body))
                    .expect("response should build");
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;

    let repository = Arc::new(InMemoryVideoTaskRepository::default());
    repository
        .upsert(UpsertVideoTask {
            id: "task-legacy-content-123".to_string(),
            short_id: None,
            request_id: "request-openai-video-content-legacy-123".to_string(),
            user_id: Some("user-video-content-legacy-123".to_string()),
            api_key_id: Some("key-video-content-legacy-123".to_string()),
            username: Some("video-user".to_string()),
            api_key_name: Some("video-key".to_string()),
            external_task_id: Some("ext-video-content-legacy-123".to_string()),
            provider_id: Some("provider-openai-video-content-legacy-1".to_string()),
            endpoint_id: Some("endpoint-openai-video-content-legacy-1".to_string()),
            key_id: Some("key-openai-video-content-legacy-1".to_string()),
            client_api_format: Some("openai:video".to_string()),
            provider_api_format: Some("openai:video".to_string()),
            format_converted: false,
            model: Some("sora-2".to_string()),
            prompt: Some("legacy content".to_string()),
            original_request_body: Some(json!({
                "model": "sora-2",
                "prompt": "legacy content"
            })),
            duration_seconds: Some(4),
            resolution: Some("720p".to_string()),
            aspect_ratio: Some("16:9".to_string()),
            size: Some("1280x720".to_string()),
            status: aether_data::repository::video_tasks::VideoTaskStatus::Completed,
            progress_percent: 100,
            progress_message: None,
            retry_count: 0,
            poll_interval_seconds: 10,
            next_poll_at_unix_secs: None,
            poll_count: 0,
            max_poll_count: 360,
            created_at_unix_secs: 123,
            submitted_at_unix_secs: Some(123),
            completed_at_unix_secs: Some(456),
            updated_at_unix_secs: 456,
            error_code: None,
            error_message: None,
            video_url: Some("https://cdn.example.com/legacy-video.mp4".to_string()),
            request_metadata: None,
        })
        .await
        .expect("upsert should succeed");
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![sample_key()],
    ));

    let gateway = build_router_with_state(
        AppState::new_with_executor(
            upstream_url.clone(),
            Some(upstream_url.clone()),
            Some(executor_url),
        )
        .expect("gateway state should build")
        .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative)
        .with_video_task_repository_and_provider_transport_for_tests(
            repository,
            provider_catalog_repository,
            DEVELOPMENT_ENCRYPTION_KEY,
        ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/videos/task-legacy-content-123/content?variant=video"
        ))
        .header(CONTROL_EXECUTE_FALLBACK_HEADER, "true")
        .header(TRACE_ID_HEADER, "trace-openai-video-content-legacy-123")
        .send()
        .await
        .expect("content request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4")
    );
    assert_eq!(
        response.bytes().await.expect("body should read"),
        Bytes::from_static(b"legacy-content")
    );

    let seen_stream_request = seen_executor_stream
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(seen_stream_request.method, "GET");
    assert_eq!(
        seen_stream_request.url,
        "https://cdn.example.com/legacy-video.mp4"
    );
    assert_eq!(*decision_stream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_stream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
