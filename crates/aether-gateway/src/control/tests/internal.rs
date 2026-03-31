use super::*;

#[test]
fn classifies_internal_hub_heartbeat_as_internal_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/internal/hub/heartbeat"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::POST, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("internal_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("hub_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("heartbeat"));
    assert_eq!(decision.auth_endpoint_signature.as_deref(), Some(""));
    assert!(!decision.executor_candidate);
}

#[test]
fn classifies_internal_gateway_resolve_as_internal_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/internal/gateway/resolve"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::POST, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("internal_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("gateway_legacy"));
    assert_eq!(decision.route_kind.as_deref(), Some("resolve"));
    assert_eq!(decision.auth_endpoint_signature.as_deref(), Some(""));
    assert!(!decision.executor_candidate);
}

#[test]
fn classifies_internal_hub_node_status_as_internal_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/internal/hub/node-status"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::POST, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("internal_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("hub_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("node_status"));
    assert_eq!(decision.auth_endpoint_signature.as_deref(), Some(""));
    assert!(!decision.executor_candidate);
}
