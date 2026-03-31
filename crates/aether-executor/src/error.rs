use std::error::Error as _;

use http::method::InvalidMethod;
use thiserror::Error;

pub(crate) fn format_upstream_request_error(err: &reqwest::Error) -> String {
    let mut kinds = Vec::new();
    if err.is_connect() {
        kinds.push("connect");
    }
    if err.is_timeout() {
        kinds.push("timeout");
    }
    if err.is_redirect() {
        kinds.push("redirect");
    }
    if err.is_body() {
        kinds.push("body");
    }
    if err.is_decode() {
        kinds.push("decode");
    }
    if err.is_request() {
        kinds.push("request");
    }

    let mut detail = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !detail.contains(&cause_text) {
            detail.push_str(": ");
            detail.push_str(&cause_text);
        }
        source = cause.source();
    }

    if let Some(url) = err.url() {
        detail.push_str(" [url=");
        detail.push_str(url.as_str());
        detail.push(']');
    }
    if !kinds.is_empty() {
        detail.push_str(" [kind=");
        detail.push_str(&kinds.join(","));
        detail.push(']');
    }

    detail
}

#[derive(Debug, Error)]
pub enum ExecutorClientError {
    #[error("executor endpoint is not configured")]
    MissingEndpoint,
    #[error("executor request is not implemented yet")]
    Unimplemented,
    #[error("failed to encode NDJSON frame: {0}")]
    Encode(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum ExecutorServiceError {
    #[error("stream execution is not implemented yet")]
    StreamUnsupported,
    #[error("request body must contain json_body or body_bytes_b64")]
    RequestBodyRequired,
    #[error("request body base64 is invalid: {0}")]
    BodyDecode(base64::DecodeError),
    #[error("request content-encoding is not supported: {0}")]
    UnsupportedContentEncoding(String),
    #[error("proxy execution is not implemented yet")]
    ProxyUnsupported,
    #[error("tls profile overrides are not implemented yet")]
    TlsProfileUnsupported,
    #[error("tunnel delegate execution is not implemented yet")]
    DelegateUnsupported,
    #[error("invalid method: {0}")]
    InvalidMethod(#[from] InvalidMethod),
    #[error("invalid upstream header name: {0}")]
    InvalidHeaderName(String),
    #[error("invalid upstream header value for {0}")]
    InvalidHeaderValue(String),
    #[error("invalid proxy configuration: {0}")]
    InvalidProxy(reqwest::Error),
    #[error("failed to encode request body: {0}")]
    BodyEncode(serde_json::Error),
    #[error("failed to build HTTP client: {0}")]
    ClientBuild(reqwest::Error),
    #[error("failed to read executor request body: {0}")]
    RequestRead(String),
    #[error("executor request body is not valid JSON: {0}")]
    InvalidRequestJson(serde_json::Error),
    #[error("executor overloaded: gate {gate} saturated at {limit}")]
    Overloaded { gate: &'static str, limit: usize },
    #[error("failed to execute upstream request: {0}")]
    UpstreamRequest(String),
    #[error("hub relay request failed: {0}")]
    RelayError(String),
    #[error("upstream response is not valid JSON: {0}")]
    InvalidJson(serde_json::Error),
}
