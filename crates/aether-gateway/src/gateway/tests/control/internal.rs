use super::*;

#[tokio::test]
async fn gateway_handles_internal_hub_heartbeat_locally_with_loopback() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/internal/hub/heartbeat",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![sample_proxy_node(
        "node-123",
    )]));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url.clone(), None)
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_proxy_node_repository_for_tests(
                Arc::clone(&repository),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/internal/hub/heartbeat"))
        .json(&json!({
            "node_id": "node-123",
            "heartbeat_interval": 45,
            "active_connections": 5,
            "total_requests": 9,
            "avg_latency_ms": 12.5,
            "failed_requests": 1,
            "dns_failures": 2,
            "stream_errors": 3,
            "proxy_metadata": {"arch": "arm64"},
            "proxy_version": "2.0.0",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["config_version"], 7);
    assert_eq!(payload["upgrade_to"], "1.2.3");
    assert_eq!(payload["remote_config"]["allowed_ports"][0], 443);

    let node = repository
        .find_proxy_node("node-123")
        .await
        .expect("lookup should succeed")
        .expect("node should exist");
    assert_eq!(node.status, "online");
    assert_eq!(node.heartbeat_interval, 45);
    assert_eq!(node.active_connections, 5);
    assert_eq!(node.total_requests, 9);
    assert_eq!(node.failed_requests, 1);
    assert_eq!(node.dns_failures, 2);
    assert_eq!(node.stream_errors, 3);
    assert_eq!(
        node.proxy_metadata
            .as_ref()
            .and_then(|value| value.get("version"))
            .and_then(serde_json::Value::as_str),
        Some("2.0.0")
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_internal_hub_node_status_locally_with_loopback() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/internal/hub/node-status",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![sample_proxy_node(
        "node-123",
    )]));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new(upstream_url.clone(), None)
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_proxy_node_repository_for_tests(
                Arc::clone(&repository),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/internal/hub/node-status"))
        .json(&json!({
            "node_id": "node-123",
            "connected": true,
            "conn_count": 4,
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["updated"], json!(true));

    let node = repository
        .find_proxy_node("node-123")
        .await
        .expect("lookup should succeed")
        .expect("node should exist");
    assert_eq!(node.status, "online");
    assert_eq!(node.tunnel_connected, true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
