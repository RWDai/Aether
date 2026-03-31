use super::*;

#[tokio::test]
async fn gateway_locally_denies_openai_chat_after_repeated_executor_misses_without_control_execute_opt_in(
) {
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let public_execution_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let public_execution_paths_clone = Arc::clone(&public_execution_paths);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "public_path": "/v1/chat/completions"
                }))
            }),
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
                    response
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                let public_execution_paths_inner = Arc::clone(&public_execution_paths_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    public_execution_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(
                            request
                                .headers()
                                .get(EXECUTION_PATH_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        );
                    let mut response = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::from("{\"proxied\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new();

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    for trace_id in ["trace-openai-chat-bypass-1", "trace-openai-chat-bypass-2"] {
        let response = client
            .post(format!("{gateway_url}/v1/chat/completions"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(TRACE_ID_HEADER, trace_id)
            .body("{\"model\":\"gpt-5\",\"messages\":[]}")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(EXECUTION_PATH_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some(EXECUTION_PATH_LOCAL_EXECUTOR_MISS)
        );
        assert_eq!(
            response
                .headers()
                .get(PYTHON_DEPENDENCY_REASON_HEADER)
                .and_then(|value| value.to_str().ok()),
            None
        );
        let payload: serde_json::Value = response.json().await.expect("body should parse");
        assert_eq!(payload["error"]["type"], "http_error");
        assert_eq!(
            payload["error"]["message"],
            "OpenAI chat executor miss did not match a Rust execution path, and Python fallback has been removed"
        );
    }

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        *public_execution_paths.lock().expect("mutex should lock"),
        Vec::<String>::new()
    );

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_locally_denies_openai_chat_when_control_api_is_configured_without_opt_in() {
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let public_execution_path = Arc::new(Mutex::new(None::<String>));
    let public_execution_path_clone = Arc::clone(&public_execution_path);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "public_path": "/v1/chat/completions"
                }))
            }),
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
                    response
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                let public_execution_path_inner = Arc::clone(&public_execution_path_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    *public_execution_path_inner
                        .lock()
                        .expect("mutex should lock") = Some(
                        request
                            .headers()
                            .get(EXECUTION_PATH_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                    );
                    let mut response = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::from("{\"proxied\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_LOCAL_EXECUTOR_MISS)
    );
    assert_eq!(
        response
            .headers()
            .get(PYTHON_DEPENDENCY_REASON_HEADER)
            .and_then(|value| value.to_str().ok()),
        None
    );
    let payload: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(payload["error"]["type"], "http_error");
    assert_eq!(
        payload["error"]["message"],
        "OpenAI chat executor miss did not match a Rust execution path, and Python fallback has been removed"
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        public_execution_path
            .lock()
            .expect("mutex should lock")
            .as_deref(),
        None
    );

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_locally_denies_openai_chat_stream_after_executor_miss_without_control_execute_opt_in(
) {
    let execute_hits = Arc::new(Mutex::new(0usize));
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let public_execution_path = Arc::new(Mutex::new(None::<String>));
    let public_execution_path_clone = Arc::clone(&public_execution_path);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                let public_execution_path_inner = Arc::clone(&public_execution_path_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    *public_execution_path_inner
                        .lock()
                        .expect("mutex should lock") = Some(
                        request
                            .headers()
                            .get(EXECUTION_PATH_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                    );
                    let mut response = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::from("{\"proxied\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new();

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_LOCAL_EXECUTOR_MISS)
    );
    assert_eq!(
        response
            .headers()
            .get(PYTHON_DEPENDENCY_REASON_HEADER)
            .and_then(|value| value.to_str().ok()),
        None
    );
    let payload: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(payload["error"]["type"], "http_error");
    assert_eq!(
        payload["error"]["message"],
        "OpenAI chat executor miss did not match a Rust execution path, and Python fallback has been removed"
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        public_execution_path
            .lock()
            .expect("mutex should lock")
            .as_deref(),
        None
    );

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

async fn assert_ai_route_locally_denied_after_executor_miss(
    public_path: &'static str,
    route_family: &'static str,
    route_kind: &'static str,
    endpoint_signature: &'static str,
    request_body: &'static str,
    expected_message: &'static str,
) {
    assert_ai_route_locally_denied_after_executor_miss_with_request(
        reqwest::Method::POST,
        public_path,
        public_path,
        route_family,
        route_kind,
        endpoint_signature,
        Some(request_body),
        expected_message,
    )
    .await;
}

async fn assert_ai_route_locally_denied_after_executor_miss_with_request(
    method: reqwest::Method,
    route_path: &'static str,
    request_path: &'static str,
    route_family: &'static str,
    route_kind: &'static str,
    endpoint_signature: &'static str,
    request_body: Option<&'static str>,
    expected_message: &'static str,
) {
    let control_execute_hits = Arc::new(Mutex::new(0usize));
    let control_execute_hits_clone = Arc::clone(&control_execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let public_execution_path = Arc::new(Mutex::new(None::<String>));
    let public_execution_path_clone = Arc::clone(&public_execution_path);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(move |_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": route_family,
                    "route_kind": route_kind,
                    "auth_endpoint_signature": endpoint_signature,
                    "executor_candidate": true,
                    "public_path": route_path
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let control_execute_hits_inner = Arc::clone(&control_execute_hits_clone);
                async move {
                    *control_execute_hits_inner
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
                    response
                }
            }),
        )
        .route(
            route_path,
            any(move |request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                let public_execution_path_inner = Arc::clone(&public_execution_path_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    *public_execution_path_inner
                        .lock()
                        .expect("mutex should lock") = Some(
                        request
                            .headers()
                            .get(EXECUTION_PATH_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                    );
                    let mut response = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::from("{\"proxied\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new();

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let mut request =
        reqwest::Client::new().request(method, format!("{gateway_url}{request_path}"));
    if let Some(request_body) = request_body {
        request = request
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(request_body);
    }
    let response = request.send().await.expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_LOCAL_EXECUTOR_MISS)
    );
    assert_eq!(
        response
            .headers()
            .get(PYTHON_DEPENDENCY_REASON_HEADER)
            .and_then(|value| value.to_str().ok()),
        None
    );
    let payload: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(payload["error"]["type"], "http_error");
    assert_eq!(payload["error"]["message"], expected_message);
    assert_eq!(*control_execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(
        public_execution_path
            .lock()
            .expect("mutex should lock")
            .as_deref(),
        None
    );

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_locally_denies_openai_responses_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/responses",
        "openai",
        "cli",
        "openai:cli",
        "{\"model\":\"gpt-5\",\"input\":\"hello\"}",
        "OpenAI responses executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_claude_messages_after_executor_miss_without_control_execute_opt_in()
{
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/messages",
        "claude",
        "chat",
        "claude:chat",
        "{\"model\":\"claude-sonnet-4-5\",\"messages\":[]}",
        "Claude messages executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_openai_responses_stream_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/responses",
        "openai",
        "cli",
        "openai:cli",
        "{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}",
        "OpenAI responses executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_claude_messages_stream_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/messages",
        "claude",
        "chat",
        "claude:chat",
        "{\"model\":\"claude-sonnet-4-5\",\"messages\":[],\"stream\":true}",
        "Claude messages executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_openai_compact_after_executor_miss_without_control_execute_opt_in()
{
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/responses/compact",
        "openai",
        "compact",
        "openai:compact",
        "{\"model\":\"gpt-5\",\"input\":\"hello\"}",
        "OpenAI compact executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_openai_compact_stream_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/responses/compact",
        "openai",
        "compact",
        "openai:compact",
        "{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}",
        "OpenAI compact executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_gemini_generate_after_executor_miss_without_control_execute_opt_in()
{
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1beta/models/gemini-2.5-pro:generateContent",
        "gemini",
        "chat",
        "gemini:chat",
        "{\"contents\":[]}",
        "Gemini public executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_gemini_stream_after_executor_miss_without_control_execute_opt_in() {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
        "gemini",
        "chat",
        "gemini:chat",
        "{\"contents\":[]}",
        "Gemini public executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_openai_video_after_executor_miss_without_control_execute_opt_in() {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1/videos",
        "openai",
        "video",
        "openai:video",
        "{\"model\":\"sora-2\"}",
        "OpenAI video executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_gemini_video_after_executor_miss_without_control_execute_opt_in() {
    assert_ai_route_locally_denied_after_executor_miss(
        "/v1beta/models/veo-3:predictLongRunning",
        "gemini",
        "video",
        "gemini:video",
        "{\"instances\":[]}",
        "Gemini public executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_gemini_files_download_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss_with_request(
        reqwest::Method::GET,
        "/v1beta/files/file-123:download",
        "/v1beta/files/file-123:download?alt=media",
        "gemini",
        "files",
        "gemini:chat",
        None,
        "Gemini files executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}

#[tokio::test]
async fn gateway_locally_denies_gemini_files_upload_after_executor_miss_without_control_execute_opt_in(
) {
    assert_ai_route_locally_denied_after_executor_miss_with_request(
        reqwest::Method::POST,
        "/upload/v1beta/files",
        "/upload/v1beta/files?uploadType=resumable",
        "gemini",
        "files",
        "gemini:chat",
        Some("{\"file\":{}}"),
        "Gemini files executor miss did not match a Rust execution path, and Python fallback has been removed",
    )
    .await;
}
