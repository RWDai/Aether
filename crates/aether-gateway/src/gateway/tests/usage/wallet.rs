use super::*;

#[tokio::test]
async fn gateway_settles_wallet_for_completed_direct_executor_sync_usage() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let billing_repository = Arc::new(InMemoryBillingReadRepository::seed(vec![
        StoredBillingModelContext::new(
            "provider-usage-wallet-123".to_string(),
            Some("pay_as_you_go".to_string()),
            Some("key-usage-wallet-123".to_string()),
            Some(json!({"openai:chat": 1.0})),
            Some(60),
            "global-model-wallet-123".to_string(),
            "gpt-5".to_string(),
            None,
            Some(0.02),
            Some(
                json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0}]}),
            ),
            Some("model-wallet-123".to_string()),
            Some("gpt-5".to_string()),
            None,
            None,
            None,
        )
        .expect("billing context should build"),
    ]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![
        StoredWalletSnapshot::new(
            "wallet-usage-sync-123".to_string(),
            Some("user-usage-sync-123".to_string()),
            None,
            10.0,
            2.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            100,
        )
        .expect("wallet should build"),
    ]));

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-usage-wallet-sync-123",
                        "provider_name": "openai",
                        "provider_id": "provider-usage-wallet-123",
                        "endpoint_id": "endpoint-usage-wallet-123",
                        "key_id": "key-usage-wallet-123",
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
                        "provider_id": "provider-usage-wallet-123",
                        "endpoint_id": "endpoint-usage-wallet-123",
                        "key_id": "key-usage-wallet-123",
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
                "request_id": "req-usage-wallet-sync-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-usage-wallet-sync-123",
                        "usage": {
                            "input_tokens": 1000,
                            "output_tokens": 500,
                            "total_tokens": 1500
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
    let data_state = GatewayDataState::with_usage_billing_and_wallet_for_tests(
        usage_repository.clone(),
        billing_repository,
        wallet_repository.clone(),
    );
    let gateway_state =
        AppState::new_with_executor(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway state should build")
            .with_data_state_for_tests(data_state)
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
            .find_by_request_id("req-usage-wallet-sync-123")
            .await
            .expect("usage lookup should succeed");
        if stored.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let stored = stored.expect("usage should be recorded");
    assert_eq!(stored.status, "completed");
    assert_eq!(stored.total_tokens, 1500);

    let wallet = wallet_repository
        .find(WalletLookupKey::UserId("user-usage-sync-123"))
        .await
        .expect("wallet lookup should succeed")
        .expect("wallet should exist");
    assert!(wallet.balance < 10.0 || wallet.gift_balance < 2.0);
    assert!(wallet.total_consumed > 0.0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
