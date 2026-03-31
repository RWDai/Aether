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
#[ignore = "legacy python decision finalize flow removed"]
async fn gateway_executes_gemini_video_create_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
        prompt: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_name: String,
        local_short_id: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-create-direct-123",
                        "api_key_id": "key-gemini-video-create-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/veo-3:predictLongRunning"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).unwrap_or_else(|_| json!({}));
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanSyncRequest {
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
                            prompt: payload
                                .get("body_json")
                                .and_then(|value| value.get("prompt"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "gemini_video_create_sync",
                        "request_id": "req-gemini-video-create-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-video-create-direct-123",
                        "endpoint_id": "endpoint-gemini-video-create-direct-123",
                        "key_id": "key-gemini-video-create-direct-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "prompt": "make a video"
                        },
                        "content_type": "application/json",
                        "client_api_format": "gemini:video",
                        "provider_api_format": "gemini:video",
                        "model_name": "veo-3",
                        "report_kind": "gemini_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-gemini-video-create-direct-123",
                            "api_key_id": "key-gemini-video-create-direct-123",
                            "model": "veo-3"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
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
                            upstream_name: payload
                                .get("body_json")
                                .and_then(|value| value.get("name"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            local_short_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("local_short_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
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
        )
        .route(
            "/v1beta/models/veo-3:predictLongRunning",
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
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
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
                Json(json!({
                    "request_id": "req-gemini-video-create-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "name": "operations/ext-video-123"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 58
                    }
                }))
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
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3:predictLongRunning"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-gemini-video-create-direct-123")
        .body("{\"prompt\":\"make a video\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    let operation_name = response_json
        .get("name")
        .and_then(|value| value.as_str())
        .expect("operation name should exist");
    assert!(operation_name.starts_with("models/veo-3/operations/"));
    let local_short_id = operation_name
        .rsplit('/')
        .next()
        .expect("operation name should end with short id");
    assert!(!local_short_id.is_empty());
    assert_eq!(response_json.get("done"), Some(&json!(false)));
    assert_eq!(response_json.get("metadata"), Some(&json!({})));

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-gemini-video-create-direct-123"
    );
    assert_eq!(
        seen_plan_request.path,
        "/v1beta/models/veo-3:predictLongRunning"
    );
    assert_eq!(seen_plan_request.prompt, "make a video");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning"
    );
    assert_eq!(seen_executor_request.provider_api_format, "gemini:video");

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-gemini-video-create-direct-123"
    );
    assert_eq!(
        seen_report_request.report_kind,
        "gemini_video_create_sync_success"
    );
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(
        seen_report_request.upstream_name,
        "operations/ext-video-123"
    );
    assert_eq!(seen_report_request.local_short_id, local_short_id);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_video_create_via_local_decision_gate_without_python_plan_or_decision(
) {
    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        auth_header_value: String,
        prompt: String,
        endpoint_tag: String,
        conditional_header: String,
        renamed_header: String,
        dropped_header_present: bool,
        metadata_mode: String,
        metadata_source: String,
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
            "video-user".to_string(),
            Some("video@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(json!(["gemini"])),
            Some(json!(["gemini:video"])),
            Some(json!(["veo-3"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800),
            Some(json!(["gemini"])),
            Some(json!(["gemini:video"])),
            Some(json!(["veo-3"])),
        )
        .expect("auth snapshot should build")
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-gemini-video-local-1".to_string(),
            provider_name: "gemini".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-gemini-video-local-1".to_string(),
            endpoint_api_format: "gemini:video".to_string(),
            endpoint_api_family: Some("gemini".to_string()),
            endpoint_kind: Some("video".to_string()),
            endpoint_is_active: true,
            key_id: "key-gemini-video-local-1".to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["gemini:video".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 5,
            key_global_priority_by_format: Some(json!({"gemini:video": 1})),
            model_id: "model-gemini-video-local-1".to_string(),
            global_model_id: "global-model-gemini-video-local-1".to_string(),
            global_model_name: "veo-3".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(false),
            model_provider_model_name: "veo-3-upstream".to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: "veo-3-upstream".to_string(),
                priority: 1,
                api_formats: Some(vec!["gemini:video".to_string()]),
            }]),
            model_supports_streaming: Some(false),
            model_is_active: true,
            model_is_available: true,
        }
    }

    fn sample_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-gemini-video-local-1".to_string(),
            "gemini".to_string(),
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
            Some(serde_json::json!({"enabled": true, "node_id":"proxy-node-gemini-video-local"})),
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-gemini-video-local-1".to_string(),
            "provider-gemini-video-local-1".to_string(),
            "gemini:video".to_string(),
            Some("gemini".to_string()),
            Some("video".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(json!([
                {"action":"set","key":"x-endpoint-tag","value":"gemini-video-local"},
                {"action":"set","key":"x-conditional-tag","value":"video-body-rule-applied","condition":{"path":"metadata.mode","op":"eq","value":"safe","source":"current"}},
                {"action":"rename","from":"x-client-rename","to":"x-upstream-rename"},
                {"action":"drop","key":"x-drop-me"}
            ])),
            Some(json!([
                {"action":"set","path":"metadata.mode","value":"safe","condition":{"path":"metadata.mode","op":"not_exists","source":"current"}},
                {"action":"rename","from":"metadata.client","to":"metadata.source"},
                {"action":"drop","path":"store"}
            ])),
            Some(2),
            Some("/custom/v1beta/models/veo-3-upstream:predictLongRunning".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-gemini-video-local-1".to_string(),
            "provider-gemini-video-local-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(json!(["gemini:video"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-upstream-gemini-video")
                .expect("api key should encrypt"),
            None,
            None,
            Some(json!({"gemini:video": 1})),
            None,
            None,
            None,
            Some(json!({"tls_profile":"chrome_136"})),
        )
        .expect("key transport should build")
    }

    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-local-123",
                        "api_key_id": "key-gemini-video-local-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/veo-3:predictLongRunning"
                }))
            }),
        )
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
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1beta/models/veo-3:predictLongRunning",
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
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
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
                        auth_header_value: payload
                            .get("headers")
                            .and_then(|value| value.get("x-goog-api-key"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        prompt: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("prompt"))
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
                Json(json!({
                    "request_id": "trace-gemini-video-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "name": "operations/ext-video-123"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 20
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("client-gemini-video-local-key")),
        sample_auth_snapshot("key-gemini-video-local-123", "user-gemini-video-local-123"),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![sample_key()],
    ));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
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
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3:predictLongRunning?key=client-gemini-video-local-key&view=full"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-client-rename", "rename-gemini-video")
        .header("x-drop-me", "drop-gemini-video")
        .header(TRACE_ID_HEADER, "trace-gemini-video-local-123")
        .body("{\"prompt\":\"make a local video\",\"metadata\":{\"client\":\"desktop-gemini-video\"},\"store\":false}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(body.get("done"), Some(&json!(false)));
    assert!(body
        .get("name")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value.starts_with("models/veo-3/operations/")));

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/veo-3-upstream:predictLongRunning?view=full"
    );
    assert_eq!(
        seen_executor_request.auth_header_value,
        "sk-upstream-gemini-video"
    );
    assert_eq!(seen_executor_request.prompt, "make a local video");
    assert_eq!(seen_executor_request.endpoint_tag, "gemini-video-local");
    assert_eq!(
        seen_executor_request.conditional_header,
        "video-body-rule-applied"
    );
    assert_eq!(seen_executor_request.renamed_header, "rename-gemini-video");
    assert!(!seen_executor_request.dropped_header_present);
    assert_eq!(seen_executor_request.metadata_mode, "safe");
    assert_eq!(
        seen_executor_request.metadata_source,
        "desktop-gemini-video"
    );
    assert!(!seen_executor_request.store_present);
    assert_eq!(
        seen_executor_request.proxy_node_id,
        "proxy-node-gemini-video-local"
    );
    assert_eq!(seen_executor_request.tls_profile, "chrome_136");

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-gemini-video-local-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
