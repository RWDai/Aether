use super::{classified, ClassifiedRoute};

pub(super) fn classify_internal_route(
    method: &http::Method,
    normalized_path: &str,
) -> Option<ClassifiedRoute> {
    if method == http::Method::POST && normalized_path.starts_with("/api/internal/gateway/") {
        let route_kind = match normalized_path {
            "/api/internal/gateway/resolve" => "resolve",
            "/api/internal/gateway/auth-context" => "auth_context",
            "/api/internal/gateway/decision-sync" => "decision_sync",
            "/api/internal/gateway/decision-stream" => "decision_stream",
            "/api/internal/gateway/plan-sync" => "plan_sync",
            "/api/internal/gateway/plan-stream" => "plan_stream",
            "/api/internal/gateway/report-sync" => "report_sync",
            "/api/internal/gateway/report-stream" => "report_stream",
            "/api/internal/gateway/finalize-sync" => "finalize_sync",
            "/api/internal/gateway/execute-sync" => "execute_sync",
            "/api/internal/gateway/execute-stream" => "execute_stream",
            _ => "legacy_gateway",
        };
        Some(classified(
            "internal_proxy",
            "gateway_legacy",
            route_kind,
            "",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/internal/hub/heartbeat" {
        Some(classified(
            "internal_proxy",
            "hub_manage",
            "heartbeat",
            "",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/internal/hub/node-status" {
        Some(classified(
            "internal_proxy",
            "hub_manage",
            "node_status",
            "",
            false,
        ))
    } else {
        None
    }
}
