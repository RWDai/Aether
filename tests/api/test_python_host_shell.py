from __future__ import annotations

from starlette.routing import Match

from src.api.announcements import router as python_announcement_router
from src.api.admin import python_admin_router
from src.api.auth import router as python_auth_router
from src.api.dashboard import router as python_dashboard_router
import src.api.internal as internal_module
from src.api.internal.gateway import router as legacy_gateway_bridge_router
from src.api.monitoring import router as python_monitoring_router
from src.api.payment import router as python_payment_router
from src.api.public import frontdoor_compat_router, router as python_public_router
from src.api.user_me import router as python_user_me_router
from src.api.wallet import router as python_wallet_router
import src.main as main_module

LEGACY_GATEWAY_BRIDGE_PATH_PREFIX = "/api/internal/gateway"
RUST_OWNED_ADMIN_PATHS = {
    "/api/admin/modules/status",
    "/api/admin/modules/status/{module_name}",
    "/api/admin/modules/status/{module_name}/enabled",
    "/api/admin/system/version",
    "/api/admin/system/check-update",
    "/api/admin/system/aws-regions",
    "/api/admin/system/stats",
    "/api/admin/system/settings",
    "/api/admin/system/config/export",
    "/api/admin/system/users/export",
    "/api/admin/system/config/import",
    "/api/admin/system/users/import",
    "/api/admin/system/smtp/test",
    "/api/admin/system/cleanup",
    "/api/admin/system/purge/config",
    "/api/admin/system/purge/users",
    "/api/admin/system/purge/usage",
    "/api/admin/system/purge/audit-logs",
    "/api/admin/system/purge/request-bodies",
    "/api/admin/system/purge/stats",
    "/api/admin/system/configs",
    "/api/admin/system/configs/{key}",
    "/api/admin/system/api-formats",
    "/api/admin/system/email/templates",
    "/api/admin/system/email/templates/{template_type}",
    "/api/admin/providers/",
    "/api/admin/providers/summary",
    "/api/admin/providers/{provider_id}",
    "/api/admin/providers/{provider_id}/summary",
    "/api/admin/providers/{provider_id}/health-monitor",
    "/api/admin/providers/{provider_id}/mapping-preview",
    "/api/admin/providers/{provider_id}/delete-task/{task_id}",
    "/api/admin/providers/{provider_id}/pool-status",
    "/api/admin/providers/{provider_id}/pool/clear-cooldown/{key_id}",
    "/api/admin/providers/{provider_id}/pool/reset-cost/{key_id}",
    "/api/admin/providers/{provider_id}/models",
    "/api/admin/providers/{provider_id}/models/{model_id}",
    "/api/admin/providers/{provider_id}/models/batch",
    "/api/admin/providers/{provider_id}/available-source-models",
    "/api/admin/providers/{provider_id}/assign-global-models",
    "/api/admin/providers/{provider_id}/import-from-upstream",
    "/api/admin/endpoints/providers/{provider_id}/endpoints",
    "/api/admin/endpoints/defaults/{api_format}/body-rules",
    "/api/admin/endpoints/{endpoint_id}",
    "/api/admin/endpoints/keys/{key_id}",
    "/api/admin/endpoints/keys/grouped-by-format",
    "/api/admin/endpoints/keys/{key_id}/reveal",
    "/api/admin/endpoints/keys/{key_id}/export",
    "/api/admin/endpoints/keys/batch-delete",
    "/api/admin/endpoints/keys/{key_id}/clear-oauth-invalid",
    "/api/admin/endpoints/providers/{provider_id}/keys",
    "/api/admin/endpoints/providers/{provider_id}/refresh-quota",
    "/api/admin/endpoints/rpm/key/{key_id}",
    "/api/admin/endpoints/health/summary",
    "/api/admin/endpoints/health/status",
    "/api/admin/endpoints/health/api-formats",
    "/api/admin/endpoints/health/key/{key_id}",
    "/api/admin/endpoints/health/keys/{key_id}",
    "/api/admin/endpoints/health/keys",
    "/api/admin/provider-oauth/supported-types",
    "/api/admin/provider-oauth/keys/{key_id}/start",
    "/api/admin/provider-oauth/keys/{key_id}/complete",
    "/api/admin/provider-oauth/keys/{key_id}/refresh",
    "/api/admin/provider-oauth/providers/{provider_id}/start",
    "/api/admin/provider-oauth/providers/{provider_id}/complete",
    "/api/admin/provider-oauth/providers/{provider_id}/import-refresh-token",
    "/api/admin/provider-oauth/providers/{provider_id}/device-authorize",
    "/api/admin/provider-oauth/providers/{provider_id}/device-poll",
    "/api/admin/provider-oauth/providers/{provider_id}/batch-import",
    "/api/admin/provider-oauth/providers/{provider_id}/batch-import/tasks",
    "/api/admin/provider-oauth/providers/{provider_id}/batch-import/tasks/{task_id}",
    "/api/admin/adaptive/keys",
    "/api/admin/adaptive/keys/{key_id}/mode",
    "/api/admin/adaptive/keys/{key_id}/stats",
    "/api/admin/adaptive/keys/{key_id}/learning",
    "/api/admin/adaptive/keys/{key_id}/limit",
    "/api/admin/adaptive/summary",
    "/api/admin/provider-ops/architectures",
    "/api/admin/provider-ops/architectures/{architecture_id}",
    "/api/admin/provider-ops/providers/{provider_id}/status",
    "/api/admin/provider-ops/providers/{provider_id}/config",
    "/api/admin/provider-ops/providers/{provider_id}/connect",
    "/api/admin/provider-ops/providers/{provider_id}/disconnect",
    "/api/admin/provider-ops/providers/{provider_id}/verify",
    "/api/admin/provider-ops/providers/{provider_id}/actions/{action_type}",
    "/api/admin/provider-ops/providers/{provider_id}/balance",
    "/api/admin/provider-ops/providers/{provider_id}/checkin",
    "/api/admin/provider-ops/batch/balance",
    "/api/admin/billing/presets",
    "/api/admin/billing/presets/apply",
    "/api/admin/billing/rules",
    "/api/admin/billing/rules/{rule_id}",
    "/api/admin/billing/collectors",
    "/api/admin/billing/collectors/{collector_id}",
    "/api/admin/provider-strategy/providers/{provider_id}/billing",
    "/api/admin/provider-strategy/providers/{provider_id}/stats",
    "/api/admin/provider-strategy/strategies",
    "/api/admin/provider-strategy/providers/{provider_id}/quota",
    "/api/admin/provider-query/models",
    "/api/admin/provider-query/test-model",
    "/api/admin/provider-query/test-model-failover",
    "/api/admin/payments/orders",
    "/api/admin/payments/orders/{order_id}",
    "/api/admin/payments/orders/{order_id}/expire",
    "/api/admin/payments/orders/{order_id}/credit",
    "/api/admin/payments/orders/{order_id}/fail",
    "/api/admin/payments/callbacks",
    "/api/admin/security/ip/blacklist",
    "/api/admin/security/ip/blacklist/{ip_address}",
    "/api/admin/security/ip/blacklist/stats",
    "/api/admin/security/ip/whitelist",
    "/api/admin/security/ip/whitelist/{ip_address}",
    "/api/admin/security/ip/whitelist",
    "/api/admin/stats/providers/quota-usage",
    "/api/admin/stats/comparison",
    "/api/admin/stats/errors/distribution",
    "/api/admin/stats/performance/percentiles",
    "/api/admin/stats/cost/forecast",
    "/api/admin/stats/cost/savings",
    "/api/admin/stats/leaderboard/api-keys",
    "/api/admin/stats/leaderboard/models",
    "/api/admin/stats/leaderboard/users",
    "/api/admin/stats/time-series",
    "/api/admin/monitoring/audit-logs",
    "/api/admin/monitoring/system-status",
    "/api/admin/monitoring/suspicious-activities",
    "/api/admin/monitoring/user-behavior/{user_id}",
    "/api/admin/monitoring/resilience-status",
    "/api/admin/monitoring/resilience/circuit-history",
    "/api/admin/monitoring/resilience/error-stats",
    "/api/admin/monitoring/trace/{request_id}",
    "/api/admin/monitoring/trace/stats/provider/{provider_id}",
    "/api/admin/monitoring/cache/stats",
    "/api/admin/monitoring/cache/affinity/{user_identifier}",
    "/api/admin/monitoring/cache/affinities",
    "/api/admin/monitoring/cache/users/{user_identifier}",
    "/api/admin/monitoring/cache/affinity/{affinity_key}/{endpoint_id}/{model_id}/{api_format}",
    "/api/admin/monitoring/cache",
    "/api/admin/monitoring/cache/providers/{provider_id}",
    "/api/admin/monitoring/cache/config",
    "/api/admin/monitoring/cache/metrics",
    "/api/admin/monitoring/cache/model-mapping/stats",
    "/api/admin/monitoring/cache/model-mapping",
    "/api/admin/monitoring/cache/model-mapping/{model_name}",
    "/api/admin/monitoring/cache/model-mapping/provider/{provider_id}/{global_model_id}",
    "/api/admin/monitoring/cache/redis-keys",
    "/api/admin/monitoring/cache/redis-keys/{category}",
    "/api/admin/usage/aggregation/stats",
    "/api/admin/usage/stats",
    "/api/admin/usage/heatmap",
    "/api/admin/usage/records",
    "/api/admin/usage/active",
    "/api/admin/usage/cache-affinity/hit-analysis",
    "/api/admin/usage/cache-affinity/interval-timeline",
    "/api/admin/usage/cache-affinity/ttl-analysis",
    "/api/admin/usage/{usage_id}/curl",
    "/api/admin/usage/{usage_id}",
    "/api/admin/usage/{usage_id}/replay",
    "/api/admin/video-tasks",
    "/api/admin/video-tasks/stats",
    "/api/admin/video-tasks/{task_id}",
    "/api/admin/video-tasks/{task_id}/cancel",
    "/api/admin/video-tasks/{task_id}/video",
    "/api/admin/wallets",
    "/api/admin/wallets/ledger",
    "/api/admin/wallets/refund-requests",
    "/api/admin/wallets/{wallet_id}",
    "/api/admin/wallets/{wallet_id}/transactions",
    "/api/admin/wallets/{wallet_id}/refunds",
    "/api/admin/wallets/{wallet_id}/adjust",
    "/api/admin/wallets/{wallet_id}/recharge",
    "/api/admin/wallets/{wallet_id}/refunds/{refund_id}/process",
    "/api/admin/wallets/{wallet_id}/refunds/{refund_id}/complete",
    "/api/admin/wallets/{wallet_id}/refunds/{refund_id}/fail",
    "/api/admin/api-keys",
    "/api/admin/api-keys/{key_id}",
    "/api/admin/users",
    "/api/admin/users/{user_id}",
    "/api/admin/users/{user_id}/sessions",
    "/api/admin/users/{user_id}/sessions/{session_id}",
    "/api/admin/users/{user_id}/api-keys",
    "/api/admin/users/{user_id}/api-keys/{key_id}",
    "/api/admin/users/{user_id}/api-keys/{key_id}/lock",
    "/api/admin/users/{user_id}/api-keys/{key_id}/full-key",
    "/api/admin/pool/overview",
    "/api/admin/pool/scheduling-presets",
    "/api/admin/pool/{provider_id}/keys",
    "/api/admin/pool/{provider_id}/keys/batch-delete-task/{task_id}",
    "/api/admin/pool/{provider_id}/keys/batch-action",
    "/api/admin/pool/{provider_id}/keys/batch-import",
    "/api/admin/pool/{provider_id}/keys/cleanup-banned",
    "/api/admin/pool/{provider_id}/keys/resolve-selection",
    "/api/admin/proxy-nodes",
    "/api/admin/proxy-nodes/register",
    "/api/admin/proxy-nodes/heartbeat",
    "/api/admin/proxy-nodes/unregister",
    "/api/admin/proxy-nodes/manual",
    "/api/admin/proxy-nodes/upgrade",
    "/api/admin/proxy-nodes/test-url",
    "/api/admin/proxy-nodes/{node_id}",
    "/api/admin/proxy-nodes/{node_id}/test",
    "/api/admin/proxy-nodes/{node_id}/config",
    "/api/admin/proxy-nodes/{node_id}/events",
    "/api/admin/models/catalog",
    "/api/admin/models/external",
    "/api/admin/models/external/cache",
    "/api/admin/models/global",
    "/api/admin/models/global/{global_model_id}",
    "/api/admin/models/global/batch-delete",
    "/api/admin/models/global/{global_model_id}/assign-to-providers",
    "/api/admin/models/global/{global_model_id}/providers",
    "/api/admin/models/global/{global_model_id}/routing",
}


def _route_paths(router: object) -> set[str]:
    return {route.path for route in getattr(router, "routes", [])}


def _app_matches_http_route(path: str, method: str) -> bool:
    scope = {
        "type": "http",
        "path": path,
        "method": method,
        "root_path": "",
    }
    return any(route.matches(scope)[0] is Match.FULL for route in main_module.app.routes)


def _router_matches_http_route(router: object, path: str, method: str) -> bool:
    scope = {
        "type": "http",
        "path": path,
        "method": method,
        "root_path": "",
    }
    return any(route.matches(scope)[0] is Match.FULL for route in getattr(router, "routes", []))


def test_python_host_app_exposes_loopback_internal_gateway_bridge_routes() -> None:
    host_route_paths = _route_paths(main_module.app)
    legacy_bridge_paths = _route_paths(legacy_gateway_bridge_router)

    assert "/api/internal/gateway/resolve" in legacy_bridge_paths
    assert "/api/internal/gateway/auth-context" in legacy_bridge_paths
    assert "/api/internal/gateway/decision-sync" in legacy_bridge_paths
    assert "/api/internal/gateway/decision-stream" in legacy_bridge_paths

    assert hasattr(internal_module, "legacy_gateway_bridge_router") is False
    assert hasattr(internal_module, "LEGACY_GATEWAY_BRIDGE_PATH_PREFIXES") is False
    assert not legacy_bridge_paths.issubset(host_route_paths)
    assert not any(path.startswith(LEGACY_GATEWAY_BRIDGE_PATH_PREFIX) for path in host_route_paths)


def test_python_host_app_exposes_no_api_routes() -> None:
    host_route_paths = _route_paths(main_module.app)
    api_route_paths = sorted(path for path in host_route_paths if path.startswith("/api/"))

    assert api_route_paths == []


def test_python_internal_router_excludes_gateway_bridge() -> None:
    internal_route_paths = _route_paths(internal_module.python_internal_router)

    assert not any(
        path.startswith(LEGACY_GATEWAY_BRIDGE_PATH_PREFIX)
        for path in internal_route_paths
    )
    assert not any(path.startswith("/api/internal/hub") for path in internal_route_paths)


def test_python_host_app_surface_keeps_shell_routes_and_rejects_removed_edges() -> None:
    host_route_paths = _route_paths(main_module.app)
    compat_route_paths = _route_paths(frontdoor_compat_router)
    python_public_route_paths = _route_paths(python_public_router)
    python_auth_route_paths = _route_paths(python_auth_router)
    python_dashboard_route_paths = _route_paths(python_dashboard_router)
    python_monitoring_route_paths = _route_paths(python_monitoring_router)
    python_payment_route_paths = _route_paths(python_payment_router)
    python_user_me_route_paths = _route_paths(python_user_me_router)
    python_wallet_route_paths = _route_paths(python_wallet_router)
    python_admin_route_paths = _route_paths(python_admin_router)
    python_announcement_route_paths = _route_paths(python_announcement_router)

    assert "/v1/chat/completions" in compat_route_paths
    assert "/v1/messages" in compat_route_paths
    assert "/v1beta/models/{model}:generateContent" in compat_route_paths
    assert "/v1/videos" in compat_route_paths
    assert "/v1beta/files" in compat_route_paths

    assert "/v1/chat/completions" not in python_public_route_paths
    assert "/v1/messages" not in python_public_route_paths
    assert "/v1beta/models/{model}:generateContent" not in python_public_route_paths
    assert "/v1/videos" not in python_public_route_paths
    assert "/v1beta/files" not in python_public_route_paths
    assert "/v1/models" not in python_public_route_paths
    assert "/api/public/site-info" not in python_public_route_paths
    assert "/api/public/providers" not in python_public_route_paths
    assert "/api/public/models" not in python_public_route_paths
    assert "/api/public/search/models" not in python_public_route_paths
    assert "/api/public/stats" not in python_public_route_paths
    assert "/api/public/global-models" not in python_public_route_paths
    assert "/api/public/health/api-formats" not in python_public_route_paths
    assert "/api/modules/auth-status" not in python_public_route_paths
    assert "/api/capabilities" not in python_public_route_paths
    assert "/api/capabilities/user-configurable" not in python_public_route_paths
    assert "/api/capabilities/model/{model_name}" not in python_public_route_paths
    assert "/api/auth/registration-settings" not in python_auth_route_paths
    assert "/api/auth/settings" not in python_auth_route_paths
    assert "/api/auth/login" not in python_auth_route_paths
    assert "/api/auth/refresh" not in python_auth_route_paths
    assert "/api/auth/register" not in python_auth_route_paths
    assert "/api/auth/me" not in python_auth_route_paths
    assert "/api/auth/logout" not in python_auth_route_paths
    assert "/api/auth/send-verification-code" not in python_auth_route_paths
    assert "/api/auth/verify-email" not in python_auth_route_paths
    assert "/api/auth/verification-status" not in python_auth_route_paths
    assert "/api/dashboard/stats" not in python_dashboard_route_paths
    assert "/api/dashboard/recent-requests" not in python_dashboard_route_paths
    assert "/api/dashboard/provider-status" not in python_dashboard_route_paths
    assert "/api/dashboard/daily-stats" not in python_dashboard_route_paths
    assert "/api/monitoring/my-audit-logs" not in python_monitoring_route_paths
    assert "/api/monitoring/rate-limit-status" not in python_monitoring_route_paths
    assert "/api/payment/callback/{payment_method}" not in python_payment_route_paths
    assert "/api/wallet/balance" not in python_wallet_route_paths
    assert "/api/wallet/transactions" not in python_wallet_route_paths
    assert "/api/wallet/flow" not in python_wallet_route_paths
    assert "/api/wallet/today-cost" not in python_wallet_route_paths
    assert "/api/wallet/recharge" not in python_wallet_route_paths
    assert "/api/wallet/recharge/{order_id}" not in python_wallet_route_paths
    assert "/api/wallet/refunds" not in python_wallet_route_paths
    assert "/api/wallet/refunds/{refund_id}" not in python_wallet_route_paths
    assert "/api/users/me" not in python_user_me_route_paths
    assert "/api/users/me/password" not in python_user_me_route_paths
    assert "/api/users/me/sessions" not in python_user_me_route_paths
    assert "/api/users/me/sessions/others" not in python_user_me_route_paths
    assert "/api/users/me/sessions/{session_id}" not in python_user_me_route_paths
    assert "/api/users/me/api-keys" not in python_user_me_route_paths
    assert "/api/users/me/api-keys/{key_id}" not in python_user_me_route_paths
    assert "/api/users/me/usage" not in python_user_me_route_paths
    assert "/api/users/me/usage/active" not in python_user_me_route_paths
    assert "/api/users/me/usage/interval-timeline" not in python_user_me_route_paths
    assert "/api/users/me/usage/heatmap" not in python_user_me_route_paths
    assert "/api/users/me/providers" not in python_user_me_route_paths
    assert "/api/users/me/available-models" not in python_user_me_route_paths
    assert "/api/users/me/endpoint-status" not in python_user_me_route_paths
    assert "/api/users/me/api-keys/{api_key_id}/providers" not in python_user_me_route_paths
    assert "/api/users/me/api-keys/{api_key_id}/capabilities" not in python_user_me_route_paths
    assert "/api/users/me/preferences" not in python_user_me_route_paths
    assert "/api/users/me/model-capabilities" not in python_user_me_route_paths
    assert not (RUST_OWNED_ADMIN_PATHS & python_admin_route_paths)
    assert not _router_matches_http_route(
        python_announcement_router, "/api/announcements", "GET"
    )
    assert not _router_matches_http_route(
        python_announcement_router, "/api/announcements/active", "GET"
    )
    assert not _router_matches_http_route(
        python_announcement_router, "/api/announcements", "POST"
    )
    assert not _router_matches_http_route(
        python_announcement_router, "/api/announcements/announcement-1", "PUT"
    )
    assert not _router_matches_http_route(
        python_announcement_router, "/api/announcements/announcement-1", "DELETE"
    )
    assert not _router_matches_http_route(
        python_announcement_router,
        "/api/announcements/users/me/unread-count",
        "GET",
    )
    assert not _router_matches_http_route(
        python_announcement_router,
        "/api/announcements/announcement-1/read-status",
        "PATCH",
    )

    assert "/v1/chat/completions" not in host_route_paths
    assert "/v1/messages" not in host_route_paths
    assert "/v1beta/models/{model}:generateContent" not in host_route_paths
    assert "/v1/videos" not in host_route_paths
    assert "/v1beta/files" not in host_route_paths
    assert "/v1/models" not in host_route_paths
    assert "/v1/providers" not in host_route_paths
    assert "/v1/test-connection" not in host_route_paths
    assert "/api/public/site-info" not in host_route_paths
    assert "/api/public/providers" not in host_route_paths
    assert "/api/public/models" not in host_route_paths
    assert "/api/public/search/models" not in host_route_paths
    assert "/api/public/stats" not in host_route_paths
    assert not _app_matches_http_route("/api/announcements", "GET")
    assert not _app_matches_http_route("/api/announcements/active", "GET")
    assert not _app_matches_http_route("/api/announcements", "POST")
    assert not _app_matches_http_route("/api/announcements/announcement-1", "PUT")
    assert not _app_matches_http_route("/api/announcements/announcement-1", "DELETE")
    assert "/api/public/global-models" not in host_route_paths
    assert "/api/public/health/api-formats" not in host_route_paths
    assert "/api/modules/auth-status" not in host_route_paths
    assert "/api/capabilities" not in host_route_paths
    assert "/api/capabilities/user-configurable" not in host_route_paths
    assert "/api/capabilities/model/{model_name}" not in host_route_paths
    assert "/api/auth/registration-settings" not in host_route_paths
    assert "/api/auth/settings" not in host_route_paths
    assert "/api/auth/login" not in host_route_paths
    assert "/api/auth/refresh" not in host_route_paths
    assert "/api/auth/register" not in host_route_paths
    assert "/api/auth/me" not in host_route_paths
    assert "/api/auth/logout" not in host_route_paths
    assert "/api/auth/send-verification-code" not in host_route_paths
    assert "/api/auth/verify-email" not in host_route_paths
    assert "/api/auth/verification-status" not in host_route_paths
    assert "/api/dashboard/stats" not in host_route_paths
    assert "/api/dashboard/recent-requests" not in host_route_paths
    assert "/api/dashboard/provider-status" not in host_route_paths
    assert "/api/dashboard/daily-stats" not in host_route_paths
    assert "/api/monitoring/my-audit-logs" not in host_route_paths
    assert "/api/monitoring/rate-limit-status" not in host_route_paths
    assert "/api/payment/callback/{payment_method}" not in host_route_paths
    assert "/api/wallet/balance" not in host_route_paths
    assert "/api/wallet/transactions" not in host_route_paths
    assert "/api/wallet/flow" not in host_route_paths
    assert "/api/wallet/today-cost" not in host_route_paths
    assert "/api/wallet/recharge" not in host_route_paths
    assert "/api/wallet/recharge/{order_id}" not in host_route_paths
    assert "/api/wallet/refunds" not in host_route_paths
    assert "/api/wallet/refunds/{refund_id}" not in host_route_paths
    assert not (RUST_OWNED_ADMIN_PATHS & host_route_paths)
    assert "/health" not in host_route_paths
    assert "/v1/health" not in host_route_paths
    assert "/" not in host_route_paths
    assert "/test-connection" not in host_route_paths
    assert "/readyz" not in host_route_paths

    assert _app_matches_http_route("/v1/chat/completions", "POST") is False
    assert _app_matches_http_route("/v1/messages", "POST") is False
    assert _app_matches_http_route("/v1beta/models/gemini-2.5-pro:generateContent", "POST") is False
    assert _app_matches_http_route("/v1/videos", "POST") is False
    assert _app_matches_http_route("/v1beta/files", "GET") is False
    assert _app_matches_http_route("/v1/models", "GET") is False
    assert _app_matches_http_route("/v1/providers", "GET") is False
    assert _app_matches_http_route("/v1/test-connection", "GET") is False
    assert _app_matches_http_route("/api/public/site-info", "GET") is False
    assert _app_matches_http_route("/api/public/providers", "GET") is False
    assert _app_matches_http_route("/api/public/models", "GET") is False
    assert _app_matches_http_route("/api/public/search/models", "GET") is False
    assert _app_matches_http_route("/api/public/stats", "GET") is False
    assert _app_matches_http_route("/api/public/global-models", "GET") is False
    assert _app_matches_http_route("/api/public/health/api-formats", "GET") is False
    assert _app_matches_http_route("/api/modules/auth-status", "GET") is False
    assert _app_matches_http_route("/api/capabilities", "GET") is False
    assert _app_matches_http_route("/api/capabilities/user-configurable", "GET") is False
    assert _app_matches_http_route("/api/capabilities/model/gpt-5", "GET") is False
    assert _app_matches_http_route("/api/auth/registration-settings", "GET") is False
    assert _app_matches_http_route("/api/auth/login", "POST") is False
    assert _app_matches_http_route("/api/auth/refresh", "POST") is False
    assert _app_matches_http_route("/api/auth/register", "POST") is False
    assert _app_matches_http_route("/api/auth/me", "GET") is False
    assert _app_matches_http_route("/api/auth/logout", "POST") is False
    assert _app_matches_http_route("/api/auth/send-verification-code", "POST") is False
    assert _app_matches_http_route("/api/auth/verify-email", "POST") is False
    assert _app_matches_http_route("/api/auth/verification-status", "POST") is False
    assert _app_matches_http_route("/api/admin/stats/comparison", "GET") is False
    assert _app_matches_http_route("/api/admin/stats/errors/distribution", "GET") is False
    assert _app_matches_http_route("/api/admin/stats/performance/percentiles", "GET") is False
    assert _app_matches_http_route("/api/admin/stats/cost/forecast", "GET") is False
    assert _app_matches_http_route("/api/admin/stats/time-series", "GET") is False
    assert _app_matches_http_route("/api/admin/stats/leaderboard/users", "GET") is False
    assert _app_matches_http_route("/api/auth/settings", "GET") is False
    assert _app_matches_http_route("/api/dashboard/stats", "GET") is False
    assert _app_matches_http_route("/api/dashboard/recent-requests", "GET") is False
    assert _app_matches_http_route("/api/dashboard/provider-status", "GET") is False
    assert _app_matches_http_route("/api/dashboard/daily-stats", "GET") is False
    assert _app_matches_http_route("/api/monitoring/my-audit-logs", "GET") is False
    assert _app_matches_http_route("/api/monitoring/rate-limit-status", "GET") is False
    assert _app_matches_http_route("/api/payment/callback/alipay", "POST") is False
    assert _app_matches_http_route("/api/wallet/balance", "GET") is False
    assert _app_matches_http_route("/api/wallet/transactions", "GET") is False
    assert _app_matches_http_route("/api/wallet/flow", "GET") is False
    assert _app_matches_http_route("/api/wallet/today-cost", "GET") is False
    assert _app_matches_http_route("/api/wallet/recharge", "GET") is False
    assert _app_matches_http_route("/api/wallet/recharge", "POST") is False
    assert _app_matches_http_route("/api/wallet/recharge/order-1", "GET") is False
    assert _app_matches_http_route("/api/wallet/refunds", "GET") is False
    assert _app_matches_http_route("/api/wallet/refunds", "POST") is False
    assert _app_matches_http_route("/api/wallet/refunds/refund-1", "GET") is False
    assert _app_matches_http_route("/api/users/me", "GET") is False
    assert _app_matches_http_route("/api/users/me", "PUT") is False
    assert _app_matches_http_route("/api/users/me/password", "PATCH") is False
    assert _app_matches_http_route("/api/users/me/sessions", "GET") is False
    assert _app_matches_http_route("/api/users/me/sessions/others", "DELETE") is False
    assert _app_matches_http_route("/api/users/me/sessions/session-1", "PATCH") is False
    assert _app_matches_http_route("/api/users/me/sessions/session-1", "DELETE") is False
    assert _app_matches_http_route("/api/users/me/api-keys", "GET") is False
    assert _app_matches_http_route("/api/users/me/api-keys", "POST") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1", "GET") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1", "DELETE") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1", "PUT") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1", "PATCH") is False
    assert _app_matches_http_route("/api/users/me/usage", "GET") is False
    assert _app_matches_http_route("/api/users/me/usage/active", "GET") is False
    assert _app_matches_http_route("/api/users/me/usage/interval-timeline", "GET") is False
    assert _app_matches_http_route("/api/users/me/usage/heatmap", "GET") is False
    assert _app_matches_http_route("/api/users/me/providers", "GET") is False
    assert _app_matches_http_route("/api/users/me/available-models", "GET") is False
    assert _app_matches_http_route("/api/users/me/endpoint-status", "GET") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1/providers", "PUT") is False
    assert _app_matches_http_route("/api/users/me/api-keys/key-1/capabilities", "PUT") is False
    assert _app_matches_http_route("/api/users/me/preferences", "GET") is False
    assert _app_matches_http_route("/api/users/me/preferences", "PUT") is False
    assert _app_matches_http_route("/api/users/me/model-capabilities", "GET") is False
    assert _app_matches_http_route("/api/users/me/model-capabilities", "PUT") is False
    assert _app_matches_http_route("/api/announcements/announcement-1", "GET") is False
    assert _app_matches_http_route("/api/announcements/users/me/unread-count", "GET") is False
    assert (
        _app_matches_http_route("/api/announcements/announcement-1/read-status", "PATCH")
        is False
    )
    assert _app_matches_http_route("/api/admin/system/version", "GET") is False
    assert _app_matches_http_route("/api/admin/system/settings", "GET") is False
    assert _app_matches_http_route("/api/admin/system/config/export", "GET") is False
    assert _app_matches_http_route("/api/admin/system/configs", "GET") is False
    assert _app_matches_http_route("/api/admin/system/configs/smtp_password", "GET") is False
    assert _app_matches_http_route("/api/admin/modules/status", "GET") is False
    assert _app_matches_http_route("/api/admin/modules/status/auth", "GET") is False
    assert _app_matches_http_route("/api/admin/modules/status/auth/enabled", "PUT") is False
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/connect",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/disconnect",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/actions/query_balance",
            "POST",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/provider-strategy/strategies", "GET") is False
    assert (
        _app_matches_http_route(
            "/api/admin/provider-strategy/providers/provider-openai/billing",
            "PUT",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-strategy/providers/provider-openai/stats",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-strategy/providers/provider-openai/quota",
            "DELETE",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/stats/providers/quota-usage", "GET") is False
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/verify",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/balance",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/balance",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/checkin",
            "POST",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/provider-ops/batch/balance", "POST") is False
    assert _app_matches_http_route("/api/admin/billing/presets", "GET") is False
    assert _app_matches_http_route("/api/admin/billing/presets/apply", "POST") is False
    assert _app_matches_http_route("/api/admin/billing/rules", "GET") is False
    assert _app_matches_http_route("/api/admin/billing/rules/rule-1", "GET") is False
    assert _app_matches_http_route("/api/admin/billing/rules", "POST") is False
    assert _app_matches_http_route("/api/admin/billing/rules/rule-1", "PUT") is False
    assert _app_matches_http_route("/api/admin/billing/collectors", "GET") is False
    assert _app_matches_http_route("/api/admin/billing/collectors/collector-1", "GET") is False
    assert _app_matches_http_route("/api/admin/billing/collectors", "POST") is False
    assert _app_matches_http_route("/api/admin/billing/collectors/collector-1", "PUT") is False
    assert _app_matches_http_route("/api/admin/provider-query/models", "POST") is False
    assert _app_matches_http_route("/api/admin/provider-query/test-model", "POST") is False
    assert (
        _app_matches_http_route("/api/admin/provider-query/test-model-failover", "POST")
        is False
    )
    assert _app_matches_http_route("/api/admin/payments/orders", "GET") is False
    assert _app_matches_http_route("/api/admin/payments/orders/order-1", "GET") is False
    assert _app_matches_http_route("/api/admin/payments/orders/order-1/expire", "POST") is False
    assert _app_matches_http_route("/api/admin/payments/orders/order-1/credit", "POST") is False
    assert _app_matches_http_route("/api/admin/payments/orders/order-1/fail", "POST") is False
    assert _app_matches_http_route("/api/admin/payments/callbacks", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/aggregation/stats", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/stats", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/heatmap", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/records", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/active", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/usage-1/curl", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/usage-1", "GET") is False
    assert _app_matches_http_route("/api/admin/usage/usage-1/replay", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes", "GET") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/register", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/heartbeat", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/unregister", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/manual", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/upgrade", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/test-url", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/node-1", "PATCH") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/node-1", "DELETE") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/node-1/test", "POST") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/node-1/config", "PUT") is False
    assert _app_matches_http_route("/api/admin/proxy-nodes/node-1/events", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets/ledger", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets/refund-requests", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets/wallet-1", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets/wallet-1/transactions", "GET") is False
    assert _app_matches_http_route("/api/admin/wallets/wallet-1/refunds", "GET") is False
    assert _app_matches_http_route("/api/admin/api-keys", "GET") is False
    assert _app_matches_http_route("/api/admin/api-keys", "POST") is False
    assert _app_matches_http_route("/api/admin/api-keys/key-1", "GET") is False
    assert _app_matches_http_route("/api/admin/api-keys/key-1", "PUT") is False
    assert _app_matches_http_route("/api/admin/api-keys/key-1", "PATCH") is False
    assert _app_matches_http_route("/api/admin/api-keys/key-1", "DELETE") is False
    assert _app_matches_http_route("/api/admin/users", "GET") is False
    assert _app_matches_http_route("/api/admin/users", "POST") is False
    assert _app_matches_http_route("/api/admin/users/user-1", "GET") is False
    assert _app_matches_http_route("/api/admin/users/user-1", "PUT") is False
    assert _app_matches_http_route("/api/admin/users/user-1", "DELETE") is False
    assert _app_matches_http_route("/api/admin/users/user-1/sessions", "GET") is False
    assert _app_matches_http_route("/api/admin/users/user-1/sessions", "DELETE") is False
    assert _app_matches_http_route("/api/admin/users/user-1/sessions/session-1", "DELETE") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys", "GET") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys", "POST") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys/key-1", "DELETE") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys/key-1", "PUT") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys/key-1/lock", "PATCH") is False
    assert _app_matches_http_route("/api/admin/users/user-1/api-keys/key-1/full-key", "GET") is False
    assert _app_matches_http_route("/api/admin/system/email/templates", "GET") is False
    assert _app_matches_http_route("/api/admin/system/email/templates/verification", "GET") is False
    assert _app_matches_http_route("/api/admin/providers/", "GET") is False
    assert _app_matches_http_route("/api/admin/providers/", "POST") is False
    assert _app_matches_http_route("/api/admin/providers/provider-openai", "PATCH") is False
    assert _app_matches_http_route("/api/admin/providers/provider-openai/summary", "GET") is False
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/health-monitor",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/mapping-preview",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/delete-task/task-1",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/pool-status",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/models/model-1",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/models/batch",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/assign-global-models",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/providers/provider-openai/import-from-upstream",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/endpoints/providers/provider-openai/endpoints",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/endpoints/defaults/openai:responses/body-rules",
            "GET",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/endpoints/endpoint-1", "GET") is False
    assert _app_matches_http_route("/api/admin/endpoints/keys/key-1/export", "GET") is False
    assert _app_matches_http_route("/api/admin/endpoints/keys/key-1/reveal", "GET") is False
    assert (
        _app_matches_http_route(
            "/api/admin/endpoints/providers/provider-openai/keys",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/endpoints/providers/provider-openai/refresh-quota",
            "POST",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/endpoints/keys/batch-delete", "POST") is False
    assert _app_matches_http_route("/api/admin/endpoints/rpm/key/key-1", "GET") is False
    assert _app_matches_http_route("/api/admin/endpoints/health/status", "GET") is False
    assert _app_matches_http_route("/api/admin/endpoints/health/key/key-1", "GET") is False
    assert _app_matches_http_route("/api/admin/endpoints/health/keys/key-1", "PATCH") is False
    assert _app_matches_http_route("/api/admin/provider-oauth/supported-types", "GET") is False
    assert _app_matches_http_route("/api/admin/provider-oauth/keys/key-1/start", "POST") is False
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-kiro/device-authorize",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-kiro/device-poll",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-codex/import-refresh-token",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-codex/batch-import",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks",
            "POST",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/task-1",
            "GET",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/provider-ops/architectures", "GET") is False
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/architectures/generic_api",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/status",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/config",
            "GET",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/config",
            "PUT",
        )
        is False
    )
    assert (
        _app_matches_http_route(
            "/api/admin/provider-ops/providers/provider-openai/config",
            "DELETE",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/video-tasks", "GET") is False
    assert _app_matches_http_route("/api/admin/video-tasks/stats", "GET") is False
    assert _app_matches_http_route("/api/admin/video-tasks/task-1", "GET") is False
    assert _app_matches_http_route("/api/admin/video-tasks/task-1/cancel", "POST") is False
    assert _app_matches_http_route("/api/admin/video-tasks/task-1/video", "GET") is False
    assert _app_matches_http_route("/api/admin/adaptive/keys", "GET") is False
    assert _app_matches_http_route("/api/admin/adaptive/keys/key-1/mode", "PATCH") is False
    assert _app_matches_http_route("/api/admin/adaptive/keys/key-1/stats", "GET") is False
    assert _app_matches_http_route("/api/admin/adaptive/keys/key-1/learning", "DELETE") is False
    assert _app_matches_http_route("/api/admin/adaptive/keys/key-1/limit", "PATCH") is False
    assert _app_matches_http_route("/api/admin/adaptive/summary", "GET") is False
    assert _app_matches_http_route("/api/admin/models/catalog", "GET") is False
    assert _app_matches_http_route("/api/admin/models/external", "GET") is False
    assert _app_matches_http_route("/api/admin/models/external/cache", "DELETE") is False
    assert _app_matches_http_route("/api/admin/models/global", "GET") is False
    assert _app_matches_http_route("/api/admin/models/global", "POST") is False
    assert _app_matches_http_route("/api/admin/models/global/test-id", "GET") is False
    assert _app_matches_http_route("/api/admin/models/global/test-id", "PATCH") is False
    assert _app_matches_http_route("/api/admin/models/global/test-id", "DELETE") is False
    assert _app_matches_http_route("/api/admin/models/global/batch-delete", "POST") is False
    assert (
        _app_matches_http_route(
            "/api/admin/models/global/test-id/assign-to-providers",
            "POST",
        )
        is False
    )
    assert _app_matches_http_route("/api/admin/models/global/test-id/providers", "GET") is False
    assert _app_matches_http_route("/api/admin/models/global/test-id/routing", "GET") is False
    assert _app_matches_http_route("/health", "GET") is False
    assert _app_matches_http_route("/v1/health", "GET") is False
    assert _app_matches_http_route("/", "GET") is False
    assert _app_matches_http_route("/test-connection", "GET") is False
    assert _app_matches_http_route("/api/internal/gateway/auth-context", "POST") is False
    assert _app_matches_http_route("/api/internal/gateway/resolve", "POST") is False
    assert _app_matches_http_route("/api/internal/gateway/decision-sync", "POST") is False
    assert _app_matches_http_route("/api/internal/hub/heartbeat", "POST") is False
    assert _app_matches_http_route("/api/internal/hub/node-status", "POST") is False
    assert _app_matches_http_route("/readyz", "GET") is False

    tags = {tag.get("name") for tag in main_module.app.openapi().get("tags", [])}
    assert "OpenAI API" not in tags
    assert "Claude API" not in tags
    assert "Gemini API" not in tags
    assert "Gemini Files API" not in tags
    assert "System Catalog" not in tags


def test_python_payment_host_surface_is_single_dynamic_callback_route() -> None:
    host_route_paths = _route_paths(main_module.app)

    assert "/api/payment/callback/{payment_method}" not in host_route_paths
    assert "/api/payment/callback/alipay" not in host_route_paths
    assert "/api/payment/callback/wechat" not in host_route_paths

    assert _app_matches_http_route("/api/payment/callback/alipay", "POST") is False
    assert _app_matches_http_route("/api/payment/callback/wechat", "POST") is False
