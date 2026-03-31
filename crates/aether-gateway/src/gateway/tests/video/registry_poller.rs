use super::*;

#[tokio::test]
async fn gateway_background_video_task_poller_refreshes_due_openai_task_from_repository() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorRequest {
        method: String,
        url: String,
    }

    fn sample_due_openai_task() -> UpsertVideoTask {
        UpsertVideoTask {
            id: "task-local-123".to_string(),
            short_id: Some("task-local-123".to_string()),
            request_id: "request-video-poller-local-123".to_string(),
            user_id: Some("user-video-poller-123".to_string()),
            api_key_id: Some("key-video-poller-123".to_string()),
            username: Some("video-user".to_string()),
            api_key_name: Some("video-key".to_string()),
            external_task_id: Some("ext-video-task-123".to_string()),
            provider_id: Some("provider-openai-video-local-1".to_string()),
            endpoint_id: Some("endpoint-openai-video-local-1".to_string()),
            key_id: Some("key-openai-video-local-1".to_string()),
            client_api_format: Some("openai:video".to_string()),
            provider_api_format: Some("openai:video".to_string()),
            format_converted: false,
            model: Some("sora-2".to_string()),
            prompt: Some("hello".to_string()),
            original_request_body: Some(json!({"prompt": "hello"})),
            duration_seconds: Some(4),
            resolution: Some("720p".to_string()),
            aspect_ratio: Some("16:9".to_string()),
            size: Some("1280x720".to_string()),
            status: aether_data::repository::video_tasks::VideoTaskStatus::Submitted,
            progress_percent: 0,
            progress_message: None,
            retry_count: 0,
            poll_interval_seconds: 10,
            next_poll_at_unix_secs: Some(0),
            poll_count: 0,
            max_poll_count: 360,
            created_at_unix_secs: 123,
            submitted_at_unix_secs: Some(123),
            completed_at_unix_secs: None,
            updated_at_unix_secs: 123,
            error_code: None,
            error_message: None,
            video_url: None,
            request_metadata: Some(json!({
                "rust_local_snapshot": {
                    "OpenAi": {
                        "local_task_id": "task-local-123",
                        "upstream_task_id": "ext-video-task-123",
                        "created_at_unix_secs": 123,
                        "user_id": "user-video-poller-123",
                        "api_key_id": "key-video-poller-123",
                        "model": "sora-2",
                        "prompt": "hello",
                        "size": "1280x720",
                        "seconds": "4",
                        "remixed_from_video_id": null,
                        "status": "Submitted",
                        "progress_percent": 0,
                        "completed_at_unix_secs": null,
                        "expires_at_unix_secs": null,
                        "error_code": null,
                        "error_message": null,
                        "video_url": null,
                        "persistence": {
                            "request_id": "request-video-poller-local-123",
                            "username": "video-user",
                            "api_key_name": "video-key",
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "original_request_body": {
                                "prompt": "hello"
                            },
                            "format_converted": false
                        },
                        "transport": {
                            "upstream_base_url": "https://api.openai.example",
                            "provider_name": "openai-video",
                            "provider_id": "provider-openai-video-local-1",
                            "endpoint_id": "endpoint-openai-video-local-1",
                            "key_id": "key-openai-video-local-1",
                            "headers": {
                                "authorization": "Bearer sk-upstream-openai-video",
                                "content-type": "application/json"
                            },
                            "content_type": "application/json",
                            "model_name": "sora-2-upstream",
                            "proxy": null,
                            "tls_profile": null,
                            "timeouts": null
                        }
                    }
                }
            })),
        }
    }

    let seen_executor_requests = Arc::new(Mutex::new(Vec::<SeenExecutorRequest>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
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
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(SeenExecutorRequest {
                        method: method.clone(),
                        url: url.clone(),
                    });
                Json(json!({
                    "request_id": "req-openai-video-poller-refresh-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "ext-video-task-123",
                            "status": "processing",
                            "progress": 37
                        }
                    }
                }))
            }
        }),
    );

    let (executor_url, executor_handle) = start_server(executor).await;
    let repository = Arc::new(InMemoryVideoTaskRepository::default());
    repository
        .upsert(sample_due_openai_task())
        .await
        .expect("task upsert should succeed");

    let gateway_state = AppState::new_with_executor(
        "http://127.0.0.1:18084",
        Some("http://127.0.0.1:18084".to_string()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_video_task_data_repository_for_tests(Arc::clone(&repository))
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative)
    .with_video_task_poller_config(std::time::Duration::from_millis(25), 8);
    let background_tasks = gateway_state.spawn_background_tasks();
    assert!(!background_tasks.is_empty(), "poller task should spawn");

    let stored = {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            let stored = repository
                .find(VideoTaskLookupKey::Id("task-local-123"))
                .await
                .expect("video task lookup should succeed")
                .expect("video task should exist");
            if stored.progress_percent == 37 {
                break stored;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "poller did not refresh task within 500ms"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };

    assert_eq!(
        stored.status,
        aether_data::repository::video_tasks::VideoTaskStatus::Processing
    );
    assert_eq!(stored.progress_percent, 37);
    assert_eq!(stored.poll_count, 1);
    assert!(
        stored.next_poll_at_unix_secs.is_some_and(|value| value > 0),
        "poller should push next poll into the future"
    );
    assert_eq!(
        stored
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("rust_owner"))
            .and_then(serde_json::Value::as_str),
        Some("async_task")
    );
    assert_eq!(
        stored
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("poll_raw_response"))
            .and_then(|value| value.get("status"))
            .and_then(serde_json::Value::as_str),
        Some("processing")
    );

    assert_eq!(
        seen_executor_requests
            .lock()
            .expect("mutex should lock")
            .clone(),
        vec![SeenExecutorRequest {
            method: "GET".to_string(),
            url: "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
        }]
    );

    for handle in background_tasks {
        handle.abort();
    }
    executor_handle.abort();
}

#[tokio::test]
#[ignore = "remote video create decision fallback removed; rewrite with local scheduler data state"]
async fn gateway_background_video_task_poller_refreshes_openai_task_before_local_delete() {
    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

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
                        "user_id": "user-video-poller-123",
                        "api_key_id": "key-video-poller-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    seen_decision_paths_inner
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
                        "request_id": "req-openai-video-poller-create-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-poller-123",
                        "endpoint_id": "endpoint-openai-video-poller-123",
                        "key_id": "key-openai-video-poller-123",
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
                            "user_id": "user-video-poller-123",
                            "api_key_id": "key-video-poller-123",
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
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
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
            "/{*path}",
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
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
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
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push((method.clone(), url.clone()));
                if method == "POST" && url.ends_with("/v1/videos") {
                    return Json(json!({
                        "request_id": "req-openai-video-poller-create-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "submitted"
                            }
                        }
                    }));
                }
                if method == "GET" && url.ends_with("/v1/videos/ext-video-task-123") {
                    return Json(json!({
                        "request_id": "req-openai-video-poller-refresh-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "completed",
                                "progress": 100,
                                "completed_at": 1712345688u64
                            }
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-openai-video-poller-delete-123",
                    "status_code": 404,
                    "headers": {
                        "content-type": "application/json"
                    }
                }))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let repository = Arc::new(InMemoryVideoTaskRepository::default());
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_video_task_data_repository_for_tests(Arc::clone(&repository))
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative)
    .with_video_task_poller_config(std::time::Duration::from_millis(25), 8);
    let background_tasks = gateway_state.spawn_background_tasks();
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-poller-create-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();

    let mut stored = repository
        .find(VideoTaskLookupKey::Id(&local_task_id))
        .await
        .expect("video task lookup should succeed")
        .expect("video task should be persisted");
    stored.next_poll_at_unix_secs = Some(0);
    stored.updated_at_unix_secs = stored.updated_at_unix_secs.saturating_add(1);
    repository
        .upsert(stored.into())
        .await
        .expect("video task due update should succeed");

    wait_until(500, || {
        seen_executor_requests
            .lock()
            .expect("mutex should lock")
            .iter()
            .any(|(method, url)| {
                method == "GET" && url == "https://api.openai.example/v1/videos/ext-video-task-123"
            })
    })
    .await;

    let delete_response = reqwest::Client::new()
        .delete(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(TRACE_ID_HEADER, "trace-openai-video-poller-delete-123")
        .send()
        .await
        .expect("delete request should succeed");
    assert_eq!(delete_response.status(), StatusCode::OK);
    assert_eq!(
        delete_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({
            "id": local_task_id,
            "object": "video",
            "deleted": true
        })
    );

    assert_eq!(
        *seen_decision_paths.lock().expect("mutex should lock"),
        vec!["/v1/videos".to_string()]
    );
    let executor_requests = seen_executor_requests
        .lock()
        .expect("mutex should lock")
        .clone();
    assert!(executor_requests.contains(&(
        "POST".to_string(),
        "https://api.openai.example/v1/videos".to_string()
    )));
    assert!(executor_requests.contains(&(
        "GET".to_string(),
        "https://api.openai.example/v1/videos/ext-video-task-123".to_string()
    )));
    assert!(executor_requests.contains(&(
        "DELETE".to_string(),
        "https://api.openai.example/v1/videos/ext-video-task-123".to_string()
    )));
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    for handle in background_tasks {
        handle.abort();
    }
    executor_handle.abort();
    upstream_handle.abort();
}
