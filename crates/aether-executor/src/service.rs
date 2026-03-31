use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant};

use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionTelemetry, ProxySnapshot, ResponseBody,
};
use aether_http::{apply_http_client_config, HttpClientConfig};
use base64::Engine as _;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::tls::Version;
use serde::Serialize;
use serde_json::Value;

use crate::{error::format_upstream_request_error, ExecutorServiceError};

const HUB_RELAY_CONTENT_TYPE: &str = "application/vnd.aether.tunnel-envelope";
const HUB_RELAY_ERROR_HEADER: &str = "x-aether-tunnel-error";
const DEFAULT_HUB_BASE_URL: &str = "http://127.0.0.1:8085";
const CLAUDE_CODE_TLS_PROFILE: &str = "claude_code_nodejs";

#[derive(Debug, Serialize)]
struct RelayRequestMeta {
    method: String,
    url: String,
    headers: BTreeMap<String, String>,
    timeout: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SyncExecutor;

#[derive(Debug)]
pub struct UpstreamStreamExecution {
    pub request_id: String,
    pub candidate_id: Option<String>,
    pub status_code: u16,
    pub headers: BTreeMap<String, String>,
    pub response: reqwest::Response,
    pub started_at: Instant,
}

impl SyncExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute_sync(
        &self,
        plan: ExecutionPlan,
    ) -> Result<ExecutionResult, ExecutorServiceError> {
        let body_bytes = build_request_body(&plan)?;

        let started_at = Instant::now();
        let response = send_request(&plan, body_bytes).await?;
        let status_code = response.status().as_u16();
        let headers = collect_response_headers(response.headers());
        let body_bytes = response.bytes().await.map_err(|err| {
            ExecutorServiceError::UpstreamRequest(format_upstream_request_error(&err))
        })?;
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        let upstream_bytes = body_bytes.len() as u64;

        let body = if body_bytes.is_empty() {
            None
        } else if plan.stream {
            Some(ResponseBody {
                json_body: None,
                body_bytes_b64: Some(base64::engine::general_purpose::STANDARD.encode(&body_bytes)),
            })
        } else if response_body_is_json(&headers, &body_bytes) {
            let body_json: Value =
                serde_json::from_slice(&body_bytes).map_err(ExecutorServiceError::InvalidJson)?;
            Some(ResponseBody {
                json_body: Some(body_json),
                body_bytes_b64: None,
            })
        } else {
            Some(ResponseBody {
                json_body: None,
                body_bytes_b64: Some(base64::engine::general_purpose::STANDARD.encode(&body_bytes)),
            })
        };

        Ok(ExecutionResult {
            request_id: plan.request_id,
            candidate_id: plan.candidate_id,
            status_code,
            headers,
            body,
            telemetry: Some(ExecutionTelemetry {
                ttfb_ms: None,
                elapsed_ms: Some(elapsed_ms),
                upstream_bytes: Some(upstream_bytes),
            }),
            error: None,
        })
    }

    pub async fn execute_stream(
        &self,
        plan: ExecutionPlan,
    ) -> Result<UpstreamStreamExecution, ExecutorServiceError> {
        if !plan.stream {
            return Err(ExecutorServiceError::StreamUnsupported);
        }

        let body_bytes = build_request_body(&plan)?;

        let started_at = Instant::now();
        let response = send_request(&plan, body_bytes).await?;
        let status_code = response.status().as_u16();
        let headers = collect_response_headers(response.headers());

        Ok(UpstreamStreamExecution {
            request_id: plan.request_id,
            candidate_id: plan.candidate_id,
            status_code,
            headers,
            response,
            started_at,
        })
    }
}

async fn send_request(
    plan: &ExecutionPlan,
    body_bytes: Vec<u8>,
) -> Result<reqwest::Response, ExecutorServiceError> {
    let method = plan.method.parse::<reqwest::Method>()?;
    let headers = build_request_headers(
        &plan.headers,
        plan.content_encoding.as_deref(),
        plan.body.body_bytes_b64.is_some(),
    )?;
    let total_timeout = plan
        .timeouts
        .as_ref()
        .and_then(|timeouts| timeouts.total_ms)
        .map(Duration::from_millis);

    if let Some(node_id) = resolve_tunnel_node_id(plan.proxy.as_ref()) {
        return send_via_tunnel_relay(plan, method, headers, body_bytes, &node_id, total_timeout)
            .await;
    }

    let client = build_client(
        plan.timeouts.as_ref(),
        plan.proxy.as_ref(),
        plan.tls_profile.as_deref(),
    )?;
    let mut request = client.request(method, &plan.url);
    request = request.headers(headers).body(body_bytes);
    if let Some(timeout) = total_timeout {
        request = request.timeout(timeout);
    }
    request
        .send()
        .await
        .map_err(|err| ExecutorServiceError::UpstreamRequest(format_upstream_request_error(&err)))
}

async fn send_via_tunnel_relay(
    plan: &ExecutionPlan,
    method: reqwest::Method,
    headers: HeaderMap,
    body_bytes: Vec<u8>,
    node_id: &str,
    total_timeout: Option<Duration>,
) -> Result<reqwest::Response, ExecutorServiceError> {
    let client = build_relay_client(plan.timeouts.as_ref())?;
    let relay_url = build_relay_url(plan.proxy.as_ref(), node_id);
    let envelope = build_relay_envelope(
        RelayRequestMeta {
            method: method.as_str().to_string(),
            url: plan.url.clone(),
            headers: header_map_to_string_map(&headers),
            timeout: resolve_relay_timeout_seconds(plan),
        },
        &body_bytes,
    )?;

    let mut request = client
        .request(reqwest::Method::POST, relay_url)
        .header(reqwest::header::CONTENT_TYPE, HUB_RELAY_CONTENT_TYPE)
        .body(envelope);
    if let Some(timeout) = total_timeout {
        request = request.timeout(timeout);
    }

    let response = request
        .send()
        .await
        .map_err(|err| ExecutorServiceError::RelayError(err.to_string()))?;

    if let Some(kind) = response
        .headers()
        .get(HUB_RELAY_ERROR_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    {
        let message = response
            .text()
            .await
            .unwrap_or_else(|_| format!("hub relay error: {kind}"));
        return Err(ExecutorServiceError::RelayError(message));
    }

    Ok(response)
}

fn build_request_body(plan: &ExecutionPlan) -> Result<Vec<u8>, ExecutorServiceError> {
    let mut body_bytes = if let Some(json_body) = plan.body.json_body.clone() {
        serde_json::to_vec(&json_body).map_err(ExecutorServiceError::BodyEncode)?
    } else if let Some(body_b64) = plan.body.body_bytes_b64.as_deref() {
        base64::engine::general_purpose::STANDARD
            .decode(body_b64)
            .map_err(ExecutorServiceError::BodyDecode)?
    } else {
        Vec::new()
    };

    if should_gzip_request_body(plan) && plan.body.json_body.is_some() {
        body_bytes = gzip_bytes(&body_bytes)?;
    }

    Ok(body_bytes)
}

fn should_gzip_request_body(plan: &ExecutionPlan) -> bool {
    matches!(
        normalize_content_encoding(plan.content_encoding.as_deref()).as_deref(),
        Some("gzip")
    )
}

fn normalize_content_encoding(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

fn gzip_bytes(body_bytes: &[u8]) -> Result<Vec<u8>, ExecutorServiceError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body_bytes)
        .map_err(|err| ExecutorServiceError::RelayError(err.to_string()))?;
    encoder
        .finish()
        .map_err(|err| ExecutorServiceError::RelayError(err.to_string()))
}

fn build_relay_client(
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
) -> Result<reqwest::Client, ExecutorServiceError> {
    let builder = apply_http_client_config(
        reqwest::Client::builder(),
        &HttpClientConfig {
            connect_timeout_ms: timeouts.and_then(|timeouts| timeouts.connect_ms),
            use_rustls_tls: false,
            ..HttpClientConfig::default()
        },
    );
    builder.build().map_err(ExecutorServiceError::ClientBuild)
}

fn build_relay_envelope(
    meta: RelayRequestMeta,
    body_bytes: &[u8],
) -> Result<Vec<u8>, ExecutorServiceError> {
    let meta_bytes = serde_json::to_vec(&meta).map_err(ExecutorServiceError::BodyEncode)?;
    let mut envelope = Vec::with_capacity(4 + meta_bytes.len() + body_bytes.len());
    envelope.extend_from_slice(&(meta_bytes.len() as u32).to_be_bytes());
    envelope.extend_from_slice(&meta_bytes);
    envelope.extend_from_slice(body_bytes);
    Ok(envelope)
}

fn build_relay_url(proxy: Option<&ProxySnapshot>, node_id: &str) -> String {
    let base_url = proxy
        .and_then(resolve_hub_base_url_from_proxy)
        .or_else(|| std::env::var("AETHER_HUB_BASE_URL").ok())
        .unwrap_or_else(|| DEFAULT_HUB_BASE_URL.to_string());
    format!("{}/local/relay/{}", base_url.trim_end_matches('/'), node_id)
}

fn resolve_hub_base_url_from_proxy(proxy: &ProxySnapshot) -> Option<String> {
    let extra = proxy.extra.as_ref()?;
    let hub_base_url = extra.get("hub_base_url")?.as_str()?.trim();
    if hub_base_url.is_empty() {
        return None;
    }
    Some(hub_base_url.to_string())
}

fn resolve_relay_timeout_seconds(plan: &ExecutionPlan) -> u64 {
    let ms = plan
        .timeouts
        .as_ref()
        .and_then(|timeouts| {
            timeouts
                .read_ms
                .or(timeouts.total_ms)
                .or(timeouts.connect_ms)
        })
        .unwrap_or(60_000);
    let secs = ms.div_ceil(1_000);
    secs.clamp(1, 300)
}

fn resolve_tunnel_node_id(proxy: Option<&ProxySnapshot>) -> Option<String> {
    let proxy = proxy?;
    if proxy.enabled == Some(false) {
        return None;
    }

    let proxy_mode = proxy
        .mode
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let node_id = proxy.node_id.as_deref().map(str::trim).unwrap_or_default();
    let has_node_id = !node_id.is_empty();
    let has_proxy_url = proxy
        .url
        .as_deref()
        .map(str::trim)
        .is_some_and(|url| !url.is_empty());

    if has_node_id && (proxy_mode == "tunnel" || !has_proxy_url) {
        return Some(node_id.to_string());
    }

    None
}

fn build_client(
    timeouts: Option<&aether_contracts::ExecutionTimeouts>,
    proxy: Option<&ProxySnapshot>,
    tls_profile: Option<&str>,
) -> Result<reqwest::Client, ExecutorServiceError> {
    let mut builder = apply_http_client_config(
        reqwest::Client::builder(),
        &HttpClientConfig {
            connect_timeout_ms: timeouts.and_then(|timeouts| timeouts.connect_ms),
            ..HttpClientConfig::default()
        },
    );
    builder = apply_tls_profile(builder, tls_profile);
    if let Some(proxy_url) = resolve_proxy_url(proxy)? {
        let proxy = reqwest::Proxy::all(&proxy_url).map_err(ExecutorServiceError::InvalidProxy)?;
        builder = builder.proxy(proxy);
    }
    builder.build().map_err(ExecutorServiceError::ClientBuild)
}

fn apply_tls_profile(
    builder: reqwest::ClientBuilder,
    tls_profile: Option<&str>,
) -> reqwest::ClientBuilder {
    let profile = normalize_tls_profile(tls_profile);
    if profile.is_none() {
        return builder;
    }

    let _ = rustls::crypto::ring::default_provider().install_default();

    let tls_config = build_best_effort_tls_config();
    let builder = builder
        .use_preconfigured_tls(tls_config)
        .min_tls_version(Version::TLS_1_2)
        .max_tls_version(Version::TLS_1_3);

    if profile.as_deref() == Some(CLAUDE_CODE_TLS_PROFILE) {
        return builder;
    }

    builder
}

fn normalize_tls_profile(tls_profile: Option<&str>) -> Option<String> {
    let profile = tls_profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())?
        .to_ascii_lowercase();
    Some(profile)
}

fn build_best_effort_tls_config() -> rustls::ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(root_store)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    config
}

fn resolve_proxy_url(
    proxy: Option<&ProxySnapshot>,
) -> Result<Option<String>, ExecutorServiceError> {
    let Some(proxy) = proxy else {
        return Ok(None);
    };

    if proxy.enabled == Some(false) {
        return Ok(None);
    }

    if let Some(proxy_url) = proxy
        .url
        .as_ref()
        .map(|url| url.trim())
        .filter(|url| !url.is_empty())
    {
        return Ok(Some(proxy_url.to_string()));
    }

    if proxy.node_id.is_some() || proxy.mode.as_deref() == Some("tunnel") {
        return Err(ExecutorServiceError::ProxyUnsupported);
    }

    Ok(None)
}

fn build_request_headers(
    headers: &BTreeMap<String, String>,
    content_encoding: Option<&str>,
    allow_passthrough_content_encoding: bool,
) -> Result<HeaderMap, ExecutorServiceError> {
    let mut out = HeaderMap::new();
    let normalized_content_encoding = normalize_content_encoding(content_encoding);
    if let Some(encoding) = normalized_content_encoding.as_deref() {
        if encoding != "gzip" && !allow_passthrough_content_encoding {
            return Err(ExecutorServiceError::UnsupportedContentEncoding(
                encoding.to_string(),
            ));
        }
    }
    for (key, value) in headers {
        let normalized_key = key.trim().to_ascii_lowercase();
        if is_hop_by_hop_header(&normalized_key) || normalized_key == "content-encoding" {
            continue;
        }

        let header_name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| ExecutorServiceError::InvalidHeaderName(key.clone()))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|_| ExecutorServiceError::InvalidHeaderValue(key.clone()))?;
        out.insert(header_name, header_value);
    }
    if let Some(encoding) = normalized_content_encoding {
        out.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_str(&encoding)
                .map_err(|_| ExecutorServiceError::InvalidHeaderValue("content-encoding".into()))?,
        );
    }
    Ok(out)
}

fn header_map_to_string_map(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "content-length"
            | "connection"
            | "upgrade"
            | "keep-alive"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
    )
}

fn collect_response_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    header_map_to_string_map(headers)
}

fn response_body_is_json(headers: &BTreeMap<String, String>, body_bytes: &[u8]) -> bool {
    if headers
        .get("content-type")
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| value.contains("json"))
    {
        return true;
    }

    serde_json::from_slice::<Value>(body_bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Read;

    use aether_contracts::{ExecutionPlan, ExecutionTimeouts, RequestBody};
    use axum::body::Bytes;
    use axum::extract::{Path, Request};
    use axum::routing::{any, post};
    use axum::{Json, Router};
    use base64::Engine as _;
    use serde_json::json;
    use tokio::sync::oneshot;

    use super::SyncExecutor;

    fn tunnel_proxy_snapshot(base_url: String) -> aether_contracts::ProxySnapshot {
        aether_contracts::ProxySnapshot {
            enabled: Some(true),
            mode: Some("tunnel".into()),
            node_id: Some("node-1".into()),
            label: Some("relay-node".into()),
            url: None,
            extra: Some(json!({"hub_base_url": base_url})),
        }
    }

    fn decode_relay_envelope(body: &[u8]) -> (serde_json::Value, Vec<u8>) {
        assert!(
            body.len() >= 4,
            "relay body must contain meta length prefix"
        );
        let meta_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
        let meta_end = 4 + meta_len;
        let meta = serde_json::from_slice::<serde_json::Value>(&body[4..meta_end])
            .expect("relay meta should decode");
        (meta, body[meta_end..].to_vec())
    }

    #[tokio::test]
    async fn sync_executor_preserves_upstream_status_and_json_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/chat",
            post(|| async {
                (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({"error": {"message": "slow down"}})),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/chat"),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution should succeed");

        server.abort();

        assert_eq!(result.status_code, 429);
        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"error": {"message": "slow down"}}))
        );
    }

    #[tokio::test]
    async fn sync_executor_supports_tunnel_relay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/local/relay/{node_id}",
            post(|Path(node_id): Path<String>, body: Bytes| async move {
                let (meta, request_body) = decode_relay_envelope(&body);
                assert_eq!(node_id, "node-1");
                assert_eq!(meta["method"], "POST");
                assert_eq!(meta["url"], "https://example.com/chat");
                let request_json: serde_json::Value =
                    serde_json::from_slice(&request_body).expect("request body should be json");
                assert_eq!(request_json["model"], "gpt-4.1");
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"tunnel": true, "node_id": node_id})),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("relay test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-1".into(),
                candidate_id: None,
                provider_name: None,
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: "https://example.com/chat".into(),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: Some(tunnel_proxy_snapshot(format!("http://{addr}"))),
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("tunnel relay execution should succeed");

        server.abort();

        assert_eq!(result.status_code, 200);
        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"tunnel": true, "node_id": "node-1"}))
        );
    }

    #[tokio::test]
    async fn sync_executor_allows_tls_profile_best_effort() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/chat",
            post(|| async {
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"tls_profile": true})),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-tls-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("claude".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/chat"),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "claude-3.7-sonnet"})),
                stream: false,
                client_api_format: "claude:chat".into(),
                provider_api_format: "claude:chat".into(),
                model_name: Some("claude-3.7-sonnet".into()),
                proxy: None,
                tls_profile: Some("claude_code_nodejs".into()),
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution with tls profile should succeed");

        server.abort();

        assert_eq!(result.status_code, 200);
        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"tls_profile": true}))
        );
    }

    #[tokio::test]
    async fn sync_executor_compresses_json_body_when_requested() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/chat",
            post(|headers: axum::http::HeaderMap, body: Bytes| async move {
                let header_encoding = headers
                    .get(axum::http::header::CONTENT_ENCODING)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
                let mut decoded = String::new();
                decoder
                    .read_to_string(&mut decoded)
                    .expect("gzip body should decode");
                let request_json: serde_json::Value =
                    serde_json::from_str(&decoded).expect("decoded body should be json");
                assert_eq!(request_json["model"], "gpt-4.1");
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"compressed": true, "content_encoding": header_encoding})),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-gzip-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/chat"),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: Some("gzip".into()),
                body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution with gzip body should succeed");

        server.abort();

        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"compressed": true, "content_encoding": "gzip"}))
        );
    }

    #[tokio::test]
    async fn sync_executor_supports_raw_body_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/raw",
            post(|headers: axum::http::HeaderMap, body: Bytes| async move {
                assert_eq!(
                    headers
                        .get(axum::http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok()),
                    Some("text/plain"),
                );
                assert_eq!(body.as_ref(), b"raw-payload");
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"raw": true, "bytes": body.len()})),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-raw-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/raw"),
                headers: BTreeMap::from([("content-type".into(), "text/plain".into())]),
                content_type: Some("text/plain".into()),
                content_encoding: None,
                body: RequestBody {
                    json_body: None,
                    body_bytes_b64: Some(
                        base64::engine::general_purpose::STANDARD.encode(b"raw-payload"),
                    ),
                    body_ref: None,
                },
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution with raw body should succeed");

        server.abort();

        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"raw": true, "bytes": 11}))
        );
    }

    #[tokio::test]
    async fn sync_executor_supports_empty_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/delete",
            any(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-empty-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "DELETE".into(),
                url: format!("http://{addr}/delete"),
                headers: BTreeMap::new(),
                content_type: None,
                content_encoding: None,
                body: RequestBody {
                    json_body: None,
                    body_bytes_b64: None,
                    body_ref: None,
                },
                stream: false,
                client_api_format: "openai:video".into(),
                provider_api_format: "openai:video".into(),
                model_name: None,
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution with empty body should succeed");

        server.abort();

        assert_eq!(result.status_code, 204);
        assert!(result.body.is_none());
    }

    #[tokio::test]
    async fn stream_executor_supports_get_without_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/download",
            any(|request: Request| async move {
                let (parts, body) = request.into_parts();
                assert_eq!(parts.method, axum::http::Method::GET);
                let body = axum::body::to_bytes(body, usize::MAX)
                    .await
                    .expect("request body should read");
                assert!(body.is_empty(), "GET request body should be empty");
                (
                    [("content-type", "video/mp4"), ("x-download-test", "true")],
                    Bytes::from_static(b"video-bytes"),
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let execution = executor
            .execute_stream(ExecutionPlan {
                request_id: "req-download-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "GET".into(),
                url: format!("http://{addr}/download"),
                headers: BTreeMap::new(),
                content_type: None,
                content_encoding: None,
                body: RequestBody {
                    json_body: None,
                    body_bytes_b64: None,
                    body_ref: None,
                },
                stream: true,
                client_api_format: "openai:video".into(),
                provider_api_format: "openai:video".into(),
                model_name: None,
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    read_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("stream execution without body should succeed");

        let body = execution
            .response
            .bytes()
            .await
            .expect("response body should read");

        server.abort();

        assert_eq!(execution.status_code, 200);
        assert_eq!(
            execution.headers.get("x-download-test").map(String::as_str),
            Some("true")
        );
        assert_eq!(body.as_ref(), b"video-bytes");
    }

    #[tokio::test]
    async fn sync_executor_supports_http_proxy_urls() {
        let (uri_tx, uri_rx) = oneshot::channel();
        let shared_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(uri_tx)));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().fallback(any(move |request: Request| {
            let shared_tx = shared_tx.clone();
            async move {
                if let Some(tx) = shared_tx.lock().expect("lock should succeed").take() {
                    let _ = tx.send(request.uri().to_string());
                }
                (axum::http::StatusCode::OK, Json(json!({"proxied": true})))
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test proxy server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: "http://example.com/chat".into(),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: Some(aether_contracts::ProxySnapshot {
                    enabled: Some(true),
                    mode: Some("http".into()),
                    node_id: None,
                    label: Some("local-proxy".into()),
                    url: Some(format!("http://{addr}")),
                    extra: None,
                }),
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("sync execution through proxy should succeed");

        server.abort();

        assert_eq!(result.status_code, 200);
        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({"proxied": true}))
        );
        assert_eq!(
            uri_rx.await.expect("proxy should observe request uri"),
            "http://example.com/chat"
        );
    }

    #[tokio::test]
    async fn sync_executor_returns_raw_stream_bytes_for_upstream_stream_mode() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/chat",
            post(|| async {
                (
                    [
                        ("content-type", "text/event-stream"),
                        ("cache-control", "no-cache"),
                    ],
                    "data: {\"type\":\"message_start\"}\n\ndata: [DONE]\n\n",
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let executor = SyncExecutor::new();
        let result = executor
            .execute_sync(ExecutionPlan {
                request_id: "req-stream-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/chat"),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "gpt-4.1", "stream": true})),
                stream: true,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: None,
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("upstream stream execution should succeed");

        server.abort();

        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(
                result
                    .body
                    .and_then(|body| body.body_bytes_b64)
                    .expect("stream body bytes should be present"),
            )
            .expect("body bytes should decode");
        let raw_text = String::from_utf8(raw_bytes).expect("body bytes should be utf8");

        assert_eq!(result.status_code, 200);
        assert!(raw_text.contains("message_start"));
        assert!(raw_text.contains("[DONE]"));
    }

    #[tokio::test]
    async fn stream_executor_supports_tunnel_relay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let app = Router::new().route(
            "/local/relay/{node_id}",
            post(|Path(node_id): Path<String>, body: Bytes| async move {
                let (meta, request_body) = decode_relay_envelope(&body);
                assert_eq!(node_id, "node-1");
                assert_eq!(meta["url"], "https://example.com/chat");
                let request_json: serde_json::Value =
                    serde_json::from_slice(&request_body).expect("request body should be json");
                assert_eq!(request_json["stream"], true);
                (
                    [("content-type", "text/event-stream")],
                    "data: {\"id\":\"chunk-1\"}\n\ndata: [DONE]\n\n",
                )
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("relay test server should run");
        });

        let executor = SyncExecutor::new();
        let execution = executor
            .execute_stream(ExecutionPlan {
                request_id: "req-stream-1".into(),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: "https://example.com/chat".into(),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: None,
                body: RequestBody::from_json(json!({"model": "gpt-4.1", "stream": true})),
                stream: true,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: Some(tunnel_proxy_snapshot(format!("http://{addr}"))),
                tls_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(5_000),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("tunnel relay stream execution should succeed");

        assert_eq!(execution.status_code, 200);
        let stream_text = execution
            .response
            .text()
            .await
            .expect("stream response should decode");
        server.abort();
        assert!(stream_text.contains("chunk-1"));
        assert!(stream_text.contains("[DONE]"));
    }
}
