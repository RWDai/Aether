use std::collections::BTreeMap;

use crate::constants::*;
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RequestOrigin {
    pub(crate) client_ip: Option<String>,
    pub(crate) user_agent: Option<String>,
}

pub(crate) fn extract_or_generate_trace_id(headers: &http::HeaderMap) -> String {
    header_value_str(headers, TRACE_ID_HEADER).unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub(crate) fn header_value_str(headers: &http::HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn header_value_u64(headers: &http::HeaderMap, key: &str) -> Option<u64> {
    header_value_str(headers, key).and_then(|value| value.parse::<u64>().ok())
}

pub(crate) fn request_origin_from_headers(headers: &http::HeaderMap) -> RequestOrigin {
    RequestOrigin {
        client_ip: client_ip_from_headers(headers),
        user_agent: user_agent_from_headers(headers),
    }
}

pub(crate) fn request_origin_from_headers_and_remote(
    headers: &http::HeaderMap,
    remote_ip: std::net::IpAddr,
) -> RequestOrigin {
    let mut origin = request_origin_from_headers(headers);
    if origin.client_ip.is_none() {
        origin.client_ip = Some(remote_ip.to_string());
    }
    origin
}

fn client_ip_from_headers(headers: &http::HeaderMap) -> Option<String> {
    header_value_str(headers, FORWARDED_FOR_HEADER)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(|segment| segment.trim().to_string())
        })
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(45).collect())
        .or_else(|| {
            header_value_str(headers, "x-real-ip")
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(45).collect())
        })
}

fn user_agent_from_headers(headers: &http::HeaderMap) -> Option<String> {
    header_value_str(headers, http::header::USER_AGENT.as_str())
        .map(|value| value.chars().take(1000).collect())
}

pub(crate) fn should_skip_request_header(name: &str) -> bool {
    crate::provider_transport::should_skip_request_header(name)
}

pub(crate) fn should_skip_upstream_passthrough_header(name: &str) -> bool {
    crate::provider_transport::should_skip_upstream_passthrough_header(name)
}

pub(crate) fn should_skip_response_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-aether-control-executed"
            | "x-aether-control-action"
    )
}

pub(crate) fn collect_control_headers(headers: &http::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

pub(crate) fn is_json_request(headers: &http::HeaderMap) -> bool {
    header_value_str(headers, http::header::CONTENT_TYPE.as_str())
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(false)
}

pub(crate) fn header_equals(
    headers: &reqwest::header::HeaderMap,
    key: &'static str,
    expected: &str,
) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use http::HeaderValue;

    use super::*;

    #[test]
    fn request_origin_prefers_forwarded_for_and_preserves_user_agent() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            FORWARDED_FOR_HEADER,
            HeaderValue::from_static("203.0.113.10, 198.51.100.7"),
        );
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.8"));
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("AetherCLI/2.0"),
        );

        let origin = request_origin_from_headers_and_remote(
            &headers,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        );

        assert_eq!(origin.client_ip.as_deref(), Some("203.0.113.10"));
        assert_eq!(origin.user_agent.as_deref(), Some("AetherCLI/2.0"));
    }

    #[test]
    fn request_origin_falls_back_to_real_ip_then_remote_ip() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.8"));

        let real_ip_origin = request_origin_from_headers_and_remote(
            &headers,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        );
        assert_eq!(real_ip_origin.client_ip.as_deref(), Some("198.51.100.8"));

        headers.clear();
        let remote_origin = request_origin_from_headers_and_remote(
            &headers,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        );
        assert_eq!(remote_origin.client_ip.as_deref(), Some("192.0.2.1"));
        assert_eq!(remote_origin.user_agent, None);
    }
}
