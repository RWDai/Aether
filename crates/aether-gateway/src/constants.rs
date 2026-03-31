pub(crate) const TRACE_ID_HEADER: &str = "x-trace-id";
pub(crate) const FRONTDOOR_MANIFEST_VERSION: &str = "aether.frontdoor/v1alpha1";
pub(crate) const FRONTDOOR_MANIFEST_PATH: &str = "/.well-known/aether/frontdoor.json";
pub(crate) const INTERNAL_FRONTDOOR_MANIFEST_PATH: &str = "/_gateway/frontdoor/manifest";
pub(crate) const READYZ_PATH: &str = "/readyz";
pub(crate) const FORWARDED_HOST_HEADER: &str = "x-forwarded-host";
pub(crate) const FORWARDED_FOR_HEADER: &str = "x-forwarded-for";
pub(crate) const FORWARDED_PROTO_HEADER: &str = "x-forwarded-proto";
pub(crate) const GATEWAY_HEADER: &str = "x-aether-gateway";
pub(crate) const EXECUTION_PATH_HEADER: &str = "x-aether-execution-path";
pub(crate) const PYTHON_DEPENDENCY_REASON_HEADER: &str = "x-aether-python-dependency-reason";
pub(crate) const EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH: &str = "public_proxy_passthrough";
pub(crate) const EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS: &str =
    "public_proxy_after_executor_miss";
pub(crate) const EXECUTION_PATH_EXECUTOR_SYNC: &str = "executor_sync";
pub(crate) const EXECUTION_PATH_EXECUTOR_STREAM: &str = "executor_stream";
pub(crate) const EXECUTION_PATH_CONTROL_EXECUTE_SYNC: &str = "control_execute_sync";
pub(crate) const EXECUTION_PATH_CONTROL_EXECUTE_STREAM: &str = "control_execute_stream";
pub(crate) const EXECUTION_PATH_LOCAL_EXECUTOR_MISS: &str = "local_executor_miss";
pub(crate) const EXECUTION_PATH_LOCAL_AUTH_DENIED: &str = "local_auth_denied";
pub(crate) const EXECUTION_PATH_LOCAL_RATE_LIMITED: &str = "local_rate_limited";
pub(crate) const EXECUTION_PATH_LOCAL_OVERLOADED: &str = "local_overloaded";
pub(crate) const EXECUTION_PATH_DISTRIBUTED_OVERLOADED: &str = "distributed_overloaded";
pub(crate) const CONTROL_ROUTE_CLASS_HEADER: &str = "x-aether-control-route-class";
pub(crate) const CONTROL_ROUTE_FAMILY_HEADER: &str = "x-aether-control-route-family";
pub(crate) const CONTROL_ROUTE_KIND_HEADER: &str = "x-aether-control-route-kind";
pub(crate) const CONTROL_EXECUTOR_HEADER: &str = "x-aether-control-executor-candidate";
pub(crate) const CONTROL_REQUEST_ID_HEADER: &str = "x-aether-control-request-id";
pub(crate) const CONTROL_CANDIDATE_ID_HEADER: &str = "x-aether-control-candidate-id";
pub(crate) const CONTROL_ENDPOINT_SIGNATURE_HEADER: &str = "x-aether-control-endpoint-signature";
pub(crate) const CONTROL_EXECUTED_HEADER: &str = "x-aether-control-executed";
pub(crate) const CONTROL_ACTION_HEADER: &str = "x-aether-control-action";
pub(crate) const CONTROL_ACTION_PROXY_PUBLIC: &str = "proxy_public";
pub(crate) const CONTROL_EXECUTE_FALLBACK_HEADER: &str = "x-aether-control-execute-fallback";
pub(crate) const LEGACY_INTERNAL_GATEWAY_HEADER: &str = "x-aether-legacy-internal-gateway";
pub(crate) const TRUSTED_AUTH_USER_ID_HEADER: &str = "x-aether-auth-user-id";
pub(crate) const TRUSTED_AUTH_API_KEY_ID_HEADER: &str = "x-aether-auth-api-key-id";
pub(crate) const TRUSTED_AUTH_BALANCE_HEADER: &str = "x-aether-auth-balance-remaining";
pub(crate) const TRUSTED_AUTH_ACCESS_ALLOWED_HEADER: &str = "x-aether-auth-access-allowed";
pub(crate) const TRUSTED_ADMIN_USER_ID_HEADER: &str = "x-aether-admin-user-id";
pub(crate) const TRUSTED_ADMIN_USER_ROLE_HEADER: &str = "x-aether-admin-user-role";
pub(crate) const TRUSTED_ADMIN_SESSION_ID_HEADER: &str = "x-aether-admin-session-id";
pub(crate) const TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER: &str =
    "x-aether-admin-management-token-id";
pub(crate) const TRUSTED_RATE_LIMIT_PREFLIGHT_HEADER: &str = "x-aether-rate-limit-preflight";

pub(crate) const FRONTDOOR_REPLACEABLE_ROUTE_GROUPS: &[&str] = &["frontdoor_compat_router"];
pub(crate) const FRONTDOOR_REPLACEABLE_MIDDLEWARE_GROUPS: &[&str] = &["cors"];
pub(crate) const FRONTDOOR_COMPAT_ROUTE_PATTERNS: &[&str] = &[
    "/v1/chat/completions",
    "/v1/messages",
    "/v1/messages/count_tokens",
    "/v1/responses",
    "/v1/responses/compact",
    "/v1/videos*",
    "/v1/models/{model}:generateContent",
    "/v1/models/{model}:streamGenerateContent",
    "/v1/models/{model}:predictLongRunning",
    "/v1beta/models/{model}:generateContent",
    "/v1beta/models/{model}:streamGenerateContent",
    "/v1beta/models/{model}:predictLongRunning",
    "/v1beta/models/{model}/operations/{id}",
    "/v1beta/operations*",
    "/upload/v1beta/files",
    "/v1beta/files*",
];
pub(crate) const PYTHON_ONLY_ROUTE_GROUPS: &[&str] = &[
    "auth_router",
    "python_admin_router",
    "me_router",
    "wallet_router",
    "payment_router",
    "announcement_router",
    "dashboard_router",
    "python_public_support_router",
    "monitoring_router",
    "python_internal_router",
];
pub(crate) const PYTHON_ONLY_MIDDLEWARE_GROUPS: &[&str] = &["plugin_middleware"];
pub(crate) const PYTHON_ONLY_RUNTIME_COMPONENTS: &[&str] = &[
    "python_host_lifespan",
    "plugin_and_module_bootstrap",
    "background_workers",
];
pub(crate) const LEGACY_GATEWAY_BRIDGE_ROUTE_GROUPS: &[&str] = &["legacy_gateway_bridge_router"];
pub(crate) const LEGACY_GATEWAY_BRIDGE_PATH_PREFIXES: &[&str] = &["/api/internal/gateway"];
pub(crate) const RUST_FRONTDOOR_OWNED_ROUTE_PATTERNS: &[&str] = &[
    FRONTDOOR_MANIFEST_PATH,
    INTERNAL_FRONTDOOR_MANIFEST_PATH,
    READYZ_PATH,
    "/_gateway/health",
    "/_gateway/metrics",
    "/_gateway/async-tasks/*",
    "/_gateway/audit/*",
    "/health",
    "/v1/health",
    "/v1/providers",
    "/v1/providers/{path...}",
    "/v1/test-connection",
    "/test-connection",
    "/api/public/site-info",
    "/api/public/providers",
    "/api/public/models",
    "/api/public/search/models",
    "/api/public/stats",
    "/api/public/global-models",
    "/api/public/health/api-formats",
    "/api/oauth/providers",
    "/api/oauth/{provider_type}/authorize",
    "/api/oauth/{provider_type}/callback",
    "/api/user/oauth/bindable-providers",
    "/api/user/oauth/links",
    "/api/user/oauth/{provider_type}/bind-token",
    "/api/user/oauth/{provider_type}/bind",
    "/api/user/oauth/{provider_type}",
    "/api/modules/auth-status",
    "/api/capabilities",
    "/api/capabilities/user-configurable",
    "/api/capabilities/model/{path...}",
    "/api/internal/gateway/{path...}",
    "/api/internal/hub/heartbeat",
    "/api/internal/hub/node-status",
    "/v1/models",
    "/v1/models/{path...}",
    "/v1beta/models",
    "/v1beta/models/{path...}",
    "/v1/chat/completions",
    "/v1/messages",
    "/v1/messages/count_tokens",
    "/v1/responses",
    "/v1/responses/compact",
    "/v1/models/{model}:generateContent",
    "/v1/models/{model}:streamGenerateContent",
    "/v1/models/{model}:predictLongRunning",
    "/v1beta/models/{model}:generateContent",
    "/v1beta/models/{model}:streamGenerateContent",
    "/v1beta/models/{model}:predictLongRunning",
    "/v1beta/models/{model}/operations/{id}",
    "/v1beta/operations",
    "/v1beta/operations/{id}",
    "/v1/videos",
    "/v1/videos/{path...}",
    "/upload/v1beta/files",
    "/v1beta/files",
    "/v1beta/files/{path...}",
    "/",
    "/{*path}",
];
