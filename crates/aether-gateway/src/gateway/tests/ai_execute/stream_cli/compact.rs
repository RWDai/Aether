use super::*;
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::{
    InMemoryMinimalCandidateSelectionReadRepository, StoredMinimalCandidateSelectionRow,
    StoredProviderModelMapping,
};
use aether_data::repository::candidates::{
    InMemoryRequestCandidateRepository, RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data::repository::provider_catalog::{
    InMemoryProviderCatalogReadRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn gateway_executes_openai_compact_gemini_cross_format_stream_via_local_stream_rewrite() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportStreamRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        body: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportStreamRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:compact",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-compact-stream-gemini-xfmt-direct-123",
                        "api_key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream_decision",
                    "decision_kind": "openai_compact_stream",
                    "request_id": "req-openai-compact-stream-gemini-xfmt-direct-123",
                    "candidate_id": "cand-openai-compact-stream-gemini-xfmt-direct-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-openai-compact-stream-gemini-xfmt-direct-123",
                    "endpoint_id": "endpoint-openai-compact-stream-gemini-xfmt-direct-123",
                    "key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse",
                    "auth_header": "x-goog-api-key",
                    "auth_value": "upstream-key",
                    "provider_api_format": "gemini:cli",
                    "client_api_format": "openai:compact",
                    "model_name": "gpt-5",
                    "mapped_model": "gemini-2.5-pro-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "x-goog-api-key": "upstream-key",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "contents": [{"role": "user", "parts": [{"text": "hello"}]}]
                    },
                    "content_type": "application/json",
                    "report_kind": "openai_cli_stream_success",
                    "report_context": {
                        "user_id": "user-openai-compact-stream-gemini-xfmt-direct-123",
                        "api_key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "request_id": "req-openai-compact-stream-gemini-xfmt-direct-123",
                        "model": "gpt-5",
                        "provider_name": "gemini",
                        "provider_id": "provider-openai-compact-stream-gemini-xfmt-direct-123",
                        "endpoint_id": "endpoint-openai-compact-stream-gemini-xfmt-direct-123",
                        "key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "provider_api_format": "gemini:cli",
                        "client_api_format": "openai:compact",
                        "mapped_model": "gemini-2.5-pro-upstream",
                        "original_headers": {
                            "content-type": "application/json"
                        },
                        "original_request_body": {
                            "model": "gpt-5",
                            "input": "hello",
                            "stream": true
                        },
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    let encoded_body = payload
                        .get("body_base64")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let decoded_body = base64::engine::general_purpose::STANDARD
                        .decode(encoded_body)
                        .expect("stream body should decode");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportStreamRequest {
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
                            body: String::from_utf8(decoded_body).expect("body should be utf8"),
                        });
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-stream",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("fallback-stream"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/plain"),
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
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let gemini_stream = concat!(
                "data: {\"responseId\":\"resp_openai_compact_gemini_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini Compact\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n"
            );
            let frames = format!(
                concat!(
                    "{{\"type\":\"headers\",\"payload\":{{\"kind\":\"headers\",\"status_code\":200,\"headers\":{{\"content-type\":\"text/event-stream\"}}}}}}\n",
                    "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"text\":{}}}}}\n",
                    "{{\"type\":\"telemetry\",\"payload\":{{\"kind\":\"telemetry\",\"telemetry\":{{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}}}}\n",
                    "{{\"type\":\"eof\",\"payload\":{{\"kind\":\"eof\"}}}}\n"
                ),
                serde_json::to_string(gemini_stream).expect("stream should encode"),
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
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-compact-stream-gemini-xfmt-direct-123",
        )
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_text = response.text().await.expect("body should read");
    assert!(response_text.contains("event: response.completed"));
    assert!(response_text.contains("\"type\":\"response.completed\""));
    assert!(response_text.contains("\"object\":\"response\""));
    assert!(response_text.contains("\"text\":\"Hello Gemini Compact\""));
    assert!(response_text.contains("\"total_tokens\":5"));

    wait_until(300, || {
        seen_report
            .lock()
            .expect("mutex should lock")
            .as_ref()
            .is_some()
    })
    .await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-compact-stream-gemini-xfmt-direct-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_cli_stream_success");
    assert_eq!(seen_report_request.status_code, 200);
    assert!(seen_report_request
        .body
        .contains("event: response.completed"));
    assert!(seen_report_request
        .body
        .contains("\"text\":\"Hello Gemini Compact\""));

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_compact_stream_via_local_decision_gate_without_python_decision_stream(
) {
    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        trace_id: String,
        url: String,
        model: String,
        stream: bool,
        accept: String,
        authorization: String,
        endpoint_tag: String,
        conditional_header: String,
        renamed_header: String,
        dropped_header_present: bool,
        metadata_mode: String,
        metadata_source: String,
        metadata_origin: String,
        instructions: String,
        store_present: bool,
        proxy_node_id: String,
        tls_profile: String,
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
            Some(serde_json::json!(["openai:compact"])),
            Some(serde_json::json!(["gpt-5"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800_i64),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:compact"])),
            Some(serde_json::json!(["gpt-5"])),
        )
        .expect("auth snapshot should build")
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-openai-compact-local-1".to_string(),
            provider_name: "openai".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-openai-compact-local-1".to_string(),
            endpoint_api_format: "openai:compact".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("compact".to_string()),
            endpoint_is_active: true,
            key_id: "key-openai-compact-local-1".to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "bearer".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:compact".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 5,
            key_global_priority_by_format: Some(serde_json::json!({"openai:compact": 1})),
            model_id: "model-openai-compact-local-1".to_string(),
            global_model_id: "global-model-openai-compact-local-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "gpt-5-upstream".to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: "gpt-5-upstream".to_string(),
                priority: 1,
                api_formats: Some(vec!["openai:compact".to_string()]),
            }]),
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-compact-local-1".to_string(),
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
            Some(serde_json::json!({"url":"http://provider-proxy.internal:8080"})),
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-compact-local-1".to_string(),
            "provider-openai-compact-local-1".to_string(),
            "openai:compact".to_string(),
            Some("openai".to_string()),
            Some("compact".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-compact-local"},
                {"action":"set","key":"x-conditional-tag","value":"header-condition-hit","condition":{"path":"instructions","op":"exists","source":"current"}},
                {"action":"rename","from":"x-client-rename","to":"x-upstream-rename"},
                {"action":"drop","key":"x-drop-me"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"instructions","value":"You are GPT-5.","condition":{"path":"instructions","op":"not_exists","source":"current"}},
                {"action":"set","path":"metadata.mode","value":"safe","condition":{"path":"metadata.mode","op":"not_exists","source":"current"}},
                {"action":"rename","from":"metadata.client","to":"metadata.source"},
                {"action":"set","path":"metadata.origin","value":"from-original","condition":{"path":"metadata.client","op":"exists","source":"original"}},
                {"action":"drop","path":"store"}
            ])),
            Some(2),
            Some("/custom/v1/responses/compact".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-compact-local-1".to_string(),
            "provider-openai-compact-local-1".to_string(),
            "prod".to_string(),
            "bearer".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:compact"])),
            encrypt_python_fernet_plaintext(
                DEVELOPMENT_ENCRYPTION_KEY,
                "sk-upstream-openai-compact",
            )
            .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"openai:compact": 1})),
            None,
            None,
            Some(serde_json::json!({"enabled": true, "node_id":"proxy-node-openai-compact-local"})),
            Some(serde_json::json!({"tls_profile":"chrome_136"})),
        )
        .expect("key transport should build")
    }

    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:compact",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-compact-local-123",
                        "api_key_id": "key-openai-compact-local-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/responses/compact",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    let stream = futures_util::stream::iter([
                        Ok::<_, Infallible>(Bytes::from_static(b"event: response.completed\n")),
                        Ok::<_, Infallible>(Bytes::from_static(
                            b"data: {\"type\":\"response.completed\"}\n\n",
                        )),
                    ]);
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from_stream(stream))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/stream",
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorStreamRequest {
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
                        stream: payload
                            .get("stream")
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false),
                        accept: payload
                            .get("headers")
                            .and_then(|value| value.get("accept"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        authorization: payload
                            .get("headers")
                            .and_then(|value| value.get("authorization"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        endpoint_tag: payload
                            .get("headers")
                            .and_then(|value| value.get("x-endpoint-tag"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        conditional_header: payload
                            .get("headers")
                            .and_then(|value| value.get("x-conditional-tag"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        renamed_header: payload
                            .get("headers")
                            .and_then(|value| value.get("x-upstream-rename"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        dropped_header_present: payload
                            .get("headers")
                            .and_then(|value| value.get("x-drop-me"))
                            .is_some(),
                        metadata_mode: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("metadata"))
                            .and_then(|value| value.get("mode"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        metadata_source: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("metadata"))
                            .and_then(|value| value.get("source"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        metadata_origin: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("metadata"))
                            .and_then(|value| value.get("origin"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        instructions: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("instructions"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        store_present: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("store"))
                            .is_some(),
                        proxy_node_id: payload
                            .get("proxy")
                            .and_then(|value| value.get("node_id"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        tls_profile: payload
                            .get("tls_profile")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                let stream = concat!(
                    "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                    "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: response.completed\\ndata: {\\\"type\\\":\\\"response.completed\\\",\\\"response\\\":{\\\"id\\\":\\\"resp-compact-local-123\\\",\\\"object\\\":\\\"response\\\",\\\"model\\\":\\\"gpt-5-upstream\\\",\\\"output\\\":[]}}\\n\\n\"}}\n",
                    "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41,\"ttfb_ms\":11}}}\n",
                    "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                );
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(stream))
                    .expect("response should build");
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }
        }),
    );

    let client_api_key = "sk-client-openai-compact-local";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot(
            "key-openai-compact-local-123",
            "user-openai-compact-local-123",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
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
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {client_api_key}"),
        )
        .header("x-client-rename", "rename-openai-compact")
        .header("x-drop-me", "drop-openai-compact")
        .header(TRACE_ID_HEADER, "trace-openai-compact-local-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true,\"metadata\":{\"client\":\"desktop-openai-compact\"},\"store\":false}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTOR_STREAM)
    );
    let body = response.text().await.expect("body should read");
    assert!(body.contains("event: response.completed"));
    assert!(body.contains("\"model\":\"gpt-5-upstream\""));

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-compact-local-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/custom/v1/responses/compact"
    );
    assert_eq!(seen_executor_request.model, "gpt-5-upstream");
    assert!(seen_executor_request.stream);
    assert_eq!(seen_executor_request.accept, "text/event-stream");
    assert_eq!(
        seen_executor_request.authorization,
        "Bearer sk-upstream-openai-compact"
    );
    assert_eq!(seen_executor_request.endpoint_tag, "openai-compact-local");
    assert_eq!(
        seen_executor_request.conditional_header,
        "header-condition-hit"
    );
    assert_eq!(
        seen_executor_request.renamed_header,
        "rename-openai-compact"
    );
    assert!(!seen_executor_request.dropped_header_present);
    assert_eq!(seen_executor_request.instructions, "You are GPT-5.");
    assert_eq!(seen_executor_request.metadata_mode, "safe");
    assert_eq!(
        seen_executor_request.metadata_source,
        "desktop-openai-compact"
    );
    assert_eq!(seen_executor_request.metadata_origin, "from-original");
    assert!(!seen_executor_request.store_present);
    assert_eq!(
        seen_executor_request.proxy_node_id,
        "proxy-node-openai-compact-local"
    );
    assert_eq!(seen_executor_request.tls_profile, "chrome_136");

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-compact-local-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-stream should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
