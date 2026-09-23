use aether_data::repository::users::StoredUserAuthRecord;
use aether_data::repository::wallet::StoredWalletSnapshot;
use axum::http::StatusCode;
use chrono::Utc;
use reqwest::{Client, RequestBuilder, Response};
use serde_json::{json, Value};

use super::super::{build_router_with_state, start_server, AppState};

fn admin_headers(request: RequestBuilder) -> RequestBuilder {
    request
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(crate::constants::TRUSTED_ADMIN_USER_ID_HEADER, "admin-user")
        .header(crate::constants::TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(
            crate::constants::TRUSTED_ADMIN_SESSION_ID_HEADER,
            "session-admin",
        )
}

fn sample_user(user_id: &str) -> StoredUserAuthRecord {
    StoredUserAuthRecord::new(
        user_id.to_string(),
        Some(format!("{user_id}@example.com")),
        true,
        user_id.to_string(),
        Some("hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-4.1"])),
        true,
        false,
        Some(Utc::now()),
        Some(Utc::now()),
    )
    .expect("test user should build")
}

fn sample_wallet(user_id: &str, balance: f64, gift_balance: f64) -> StoredWalletSnapshot {
    StoredWalletSnapshot::new(
        format!("wallet-{user_id}"),
        Some(user_id.to_string()),
        None,
        balance,
        gift_balance,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        balance.max(0.0),
        0.0,
        0.0,
        0.0,
        1_710_000_000,
    )
    .expect("test wallet should build")
}

async fn post_batch_action(client: &Client, gateway_url: &str, payload: Value) -> Response {
    admin_headers(client.post(format!("{gateway_url}/api/admin/users/batch-action")))
        .json(&payload)
        .send()
        .await
        .expect("batch request should complete")
}

async fn wallet_detail(client: &Client, gateway_url: &str, user_id: &str) -> Value {
    admin_headers(client.get(format!("{gateway_url}/api/admin/wallets/wallet-{user_id}")))
        .send()
        .await
        .expect("wallet lookup should complete")
        .json()
        .await
        .expect("wallet response should parse")
}

#[tokio::test]
async fn gateway_batches_wallet_addition_deduction_and_clamped_deduction_per_user() {
    let state = AppState::new()
        .expect("gateway should build")
        .with_auth_users_for_tests([sample_user("user-1"), sample_user("user-2")])
        .with_auth_wallets_for_tests([
            sample_wallet("user-1", 10.0, 3.0),
            sample_wallet("user-2", 2.0, 1.0),
        ]);
    let (gateway_url, gateway_handle) = start_server(build_router_with_state(state)).await;
    let client = Client::new();
    let selection = json!({ "user_ids": ["user-1", "user-2"] });

    let add_response = post_batch_action(
        &client,
        &gateway_url,
        json!({
            "selection": selection.clone(),
            "action": "adjust_wallet_balance",
            "payload": { "operation": "add", "amount": 5.0 }
        }),
    )
    .await;
    assert_eq!(add_response.status(), StatusCode::OK);
    let add_result: Value = add_response.json().await.expect("response should parse");
    assert_eq!(add_result["success"], 2);
    assert_eq!(add_result["failed"], 0);
    assert_eq!(add_result["modified_fields"], json!(["wallet_balance"]));
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-1").await["balance"],
        18.0
    );
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-2").await["balance"],
        8.0
    );

    let deduct_response = post_batch_action(
        &client,
        &gateway_url,
        json!({
            "selection": selection.clone(),
            "action": "adjust_wallet_balance",
            "payload": { "operation": "deduct", "amount": 4.0 }
        }),
    )
    .await;
    assert_eq!(deduct_response.status(), StatusCode::OK);
    let deduct_result: Value = deduct_response.json().await.expect("response should parse");
    assert_eq!(deduct_result["success"], 2);
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-1").await["balance"],
        14.0
    );
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-2").await["balance"],
        4.0
    );

    let over_deduct_response = post_batch_action(
        &client,
        &gateway_url,
        json!({
            "selection": selection,
            "action": "adjust_wallet_balance",
            "payload": { "operation": "deduct", "amount": 100.0 }
        }),
    )
    .await;
    assert_eq!(over_deduct_response.status(), StatusCode::OK);
    let over_deduct_result: Value = over_deduct_response
        .json()
        .await
        .expect("response should parse");
    assert_eq!(over_deduct_result["success"], 2);
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-1").await["balance"],
        0.0
    );
    assert_eq!(
        wallet_detail(&client, &gateway_url, "user-2").await["balance"],
        0.0
    );

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_reports_missing_wallet_and_skips_zero_delta_for_non_positive_balance() {
    let state = AppState::new()
        .expect("gateway should build")
        .with_auth_users_for_tests([sample_user("user-negative"), sample_user("user-no-wallet")])
        .with_auth_wallets_for_tests([sample_wallet("user-negative", -2.0, 1.0)]);
    let (gateway_url, gateway_handle) = start_server(build_router_with_state(state)).await;
    let client = Client::new();

    let response = post_batch_action(
        &client,
        &gateway_url,
        json!({
            "selection": { "user_ids": ["user-negative", "user-no-wallet"] },
            "action": "adjust_wallet_balance",
            "payload": { "operation": "deduct", "amount": 10.0 }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result: Value = response.json().await.expect("response should parse");
    assert_eq!(result["success"], 1);
    assert_eq!(result["failed"], 1);
    assert_eq!(result["failures"][0]["user_id"], "user-no-wallet");
    assert_eq!(result["failures"][0]["reason"], "用户钱包不可用");

    let wallet = wallet_detail(&client, &gateway_url, "user-negative").await;
    assert_eq!(wallet["balance"], -1.0);
    assert_eq!(wallet["total_adjusted"], 0.0);

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_zero_and_non_finite_batch_wallet_adjustments() {
    let state = AppState::new()
        .expect("gateway should build")
        .with_auth_users_for_tests([sample_user("user-1")])
        .with_auth_wallets_for_tests([sample_wallet("user-1", 10.0, 0.0)]);
    let (gateway_url, gateway_handle) = start_server(build_router_with_state(state)).await;
    let client = Client::new();

    let zero_response = post_batch_action(
        &client,
        &gateway_url,
        json!({
            "selection": { "user_ids": ["user-1"] },
            "action": "adjust_wallet_balance",
            "payload": { "operation": "add", "amount": 0.0 }
        }),
    )
    .await;
    assert_eq!(zero_response.status(), StatusCode::BAD_REQUEST);

    let non_finite_response = admin_headers(
        client.post(format!("{gateway_url}/api/admin/users/batch-action")),
    )
    .header(reqwest::header::CONTENT_TYPE, "application/json")
    .body(
        r#"{"selection":{"user_ids":["user-1"]},"action":"adjust_wallet_balance","payload":{"operation":"add","amount":1e999}}"#,
    )
    .send()
    .await
    .expect("non-finite amount request should complete");
    assert_eq!(non_finite_response.status(), StatusCode::BAD_REQUEST);

    gateway_handle.abort();
}
