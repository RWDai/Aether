#[derive(Debug, Deserialize)]
struct AdminProviderKeyCreateRequest {
    #[serde(default)]
    api_formats: Option<Vec<String>>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    auth_type: Option<String>,
    #[serde(default)]
    auth_config: Option<serde_json::Value>,
    name: String,
    #[serde(default)]
    rate_multipliers: Option<serde_json::Value>,
    #[serde(default)]
    internal_priority: Option<i32>,
    #[serde(default)]
    rpm_limit: Option<u32>,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    capabilities: Option<serde_json::Value>,
    #[serde(default)]
    cache_ttl_minutes: Option<i32>,
    #[serde(default)]
    max_probe_interval_minutes: Option<i32>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    auto_fetch_models: Option<bool>,
    #[serde(default)]
    locked_models: Option<Vec<String>>,
    #[serde(default)]
    model_include_patterns: Option<Vec<String>>,
    #[serde(default)]
    model_exclude_patterns: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderKeyUpdateRequest {
    #[serde(default)]
    api_formats: Option<Vec<String>>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    auth_type: Option<String>,
    #[serde(default)]
    auth_config: Option<serde_json::Value>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    rate_multipliers: Option<serde_json::Value>,
    #[serde(default)]
    internal_priority: Option<i32>,
    #[serde(default)]
    global_priority_by_format: Option<serde_json::Value>,
    #[serde(default)]
    rpm_limit: Option<u32>,
    #[serde(default)]
    allowed_models: Option<Vec<String>>,
    #[serde(default)]
    capabilities: Option<serde_json::Value>,
    #[serde(default)]
    cache_ttl_minutes: Option<i32>,
    #[serde(default)]
    max_probe_interval_minutes: Option<i32>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    auto_fetch_models: Option<bool>,
    #[serde(default)]
    locked_models: Option<Vec<String>>,
    #[serde(default)]
    model_include_patterns: Option<Vec<String>>,
    #[serde(default)]
    model_exclude_patterns: Option<Vec<String>>,
    #[serde(default)]
    proxy: Option<serde_json::Value>,
    #[serde(default)]
    fingerprint: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderKeyBatchDeleteRequest {
    ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderQuotaRefreshRequest {
    #[serde(default)]
    key_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct AdminOAuthProviderUpsertRequest {
    display_name: String,
    client_id: String,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    authorization_url_override: Option<String>,
    #[serde(default)]
    token_url_override: Option<String>,
    #[serde(default)]
    userinfo_url_override: Option<String>,
    #[serde(default)]
    scopes: Option<Vec<String>>,
    redirect_uri: String,
    frontend_callback_url: String,
    #[serde(default)]
    attribute_mapping: Option<serde_json::Value>,
    #[serde(default)]
    extra_config: Option<serde_json::Value>,
    #[serde(default)]
    is_enabled: bool,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
struct InternalHubHeartbeatRequest {
    node_id: String,
    #[serde(default)]
    heartbeat_interval: Option<i32>,
    #[serde(default)]
    active_connections: Option<i32>,
    #[serde(default)]
    total_requests: Option<i64>,
    #[serde(default)]
    avg_latency_ms: Option<f64>,
    #[serde(default)]
    failed_requests: Option<i64>,
    #[serde(default)]
    dns_failures: Option<i64>,
    #[serde(default)]
    stream_errors: Option<i64>,
    #[serde(default)]
    proxy_metadata: Option<serde_json::Value>,
    #[serde(default)]
    proxy_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InternalHubNodeStatusRequest {
    node_id: String,
    connected: bool,
    #[serde(default)]
    conn_count: i32,
}

#[derive(Debug, Deserialize)]
struct LegacyGatewayResolveRequest {
    #[serde(default)]
    trace_id: Option<String>,
    method: String,
    path: String,
    #[serde(default)]
    query_string: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct LegacyGatewayAuthContextRequest {
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    query_string: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    auth_endpoint_signature: String,
}

#[derive(Debug, Deserialize)]
struct LegacyGatewayExecuteRequest {
    #[serde(default)]
    trace_id: Option<String>,
    method: String,
    path: String,
    #[serde(default)]
    query_string: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body_json: serde_json::Value,
    #[serde(default)]
    body_base64: Option<String>,
    #[serde(default)]
    auth_context: Option<crate::gateway::GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderCreateRequest {
    name: String,
    #[serde(default)]
    provider_type: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    billing_type: Option<String>,
    #[serde(default)]
    monthly_quota_usd: Option<f64>,
    #[serde(default)]
    quota_reset_day: Option<u64>,
    #[serde(default)]
    quota_last_reset_at: Option<String>,
    #[serde(default)]
    quota_expires_at: Option<String>,
    #[serde(default)]
    provider_priority: Option<i32>,
    #[serde(default)]
    keep_priority_on_conversion: Option<bool>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    concurrent_limit: Option<i32>,
    #[serde(default)]
    max_retries: Option<i32>,
    #[serde(default)]
    proxy: Option<serde_json::Value>,
    #[serde(default)]
    stream_first_byte_timeout: Option<f64>,
    #[serde(default)]
    request_timeout: Option<f64>,
    #[serde(default)]
    pool_advanced: Option<serde_json::Value>,
    #[serde(default)]
    claude_code_advanced: Option<serde_json::Value>,
    #[serde(default)]
    failover_rules: Option<serde_json::Value>,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderUpdateRequest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    provider_type: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    website: Option<String>,
    #[serde(default)]
    billing_type: Option<String>,
    #[serde(default)]
    monthly_quota_usd: Option<f64>,
    #[serde(default)]
    quota_reset_day: Option<u64>,
    #[serde(default)]
    quota_last_reset_at: Option<String>,
    #[serde(default)]
    quota_expires_at: Option<String>,
    #[serde(default)]
    provider_priority: Option<i32>,
    #[serde(default)]
    keep_priority_on_conversion: Option<bool>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    concurrent_limit: Option<i32>,
    #[serde(default)]
    max_retries: Option<i32>,
    #[serde(default)]
    proxy: Option<serde_json::Value>,
    #[serde(default)]
    stream_first_byte_timeout: Option<f64>,
    #[serde(default)]
    request_timeout: Option<f64>,
    #[serde(default)]
    pool_advanced: Option<serde_json::Value>,
    #[serde(default)]
    claude_code_advanced: Option<serde_json::Value>,
    #[serde(default)]
    failover_rules: Option<serde_json::Value>,
    #[serde(default)]
    enable_format_conversion: Option<bool>,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

const CODEX_WHAM_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const KIRO_USAGE_LIMITS_PATH: &str = "/getUsageLimits";
const KIRO_USAGE_SDK_VERSION: &str = "1.0.0";
const ANTIGRAVITY_FETCH_AVAILABLE_MODELS_PATH: &str = "/v1internal:fetchAvailableModels";
const OAUTH_ACCOUNT_BLOCK_PREFIX: &str = "[ACCOUNT_BLOCK] ";
const OAUTH_REFRESH_FAILED_PREFIX: &str = "[REFRESH_FAILED] ";
const OAUTH_EXPIRED_PREFIX: &str = "[OAUTH_EXPIRED] ";
const OAUTH_REQUEST_FAILED_PREFIX: &str = "[REQUEST_FAILED] ";

fn default_admin_endpoint_max_retries() -> i32 {
    2
}

#[derive(Debug, Deserialize)]
struct AdminProviderEndpointCreateRequest {
    provider_id: String,
    api_format: String,
    base_url: String,
    #[serde(default)]
    custom_path: Option<String>,
    #[serde(default)]
    header_rules: Option<serde_json::Value>,
    #[serde(default)]
    body_rules: Option<serde_json::Value>,
    #[serde(default = "default_admin_endpoint_max_retries")]
    max_retries: i32,
    #[serde(default)]
    config: Option<serde_json::Value>,
    #[serde(default)]
    proxy: Option<serde_json::Value>,
    #[serde(default)]
    format_acceptance_config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderEndpointUpdateRequest {
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    custom_path: Option<String>,
    #[serde(default)]
    header_rules: Option<serde_json::Value>,
    #[serde(default)]
    body_rules: Option<serde_json::Value>,
    #[serde(default)]
    max_retries: Option<i32>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    config: Option<serde_json::Value>,
    #[serde(default)]
    proxy: Option<serde_json::Value>,
    #[serde(default)]
    format_acceptance_config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderModelCreateRequest {
    provider_model_name: String,
    #[serde(default)]
    provider_model_mappings: Option<serde_json::Value>,
    global_model_id: String,
    #[serde(default)]
    price_per_request: Option<f64>,
    #[serde(default)]
    tiered_pricing: Option<serde_json::Value>,
    #[serde(default)]
    supports_vision: Option<bool>,
    #[serde(default)]
    supports_function_calling: Option<bool>,
    #[serde(default)]
    supports_streaming: Option<bool>,
    #[serde(default)]
    supports_extended_thinking: Option<bool>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminProviderModelUpdateRequest {
    #[serde(default)]
    provider_model_name: Option<String>,
    #[serde(default)]
    provider_model_mappings: Option<serde_json::Value>,
    #[serde(default)]
    global_model_id: Option<String>,
    #[serde(default)]
    price_per_request: Option<f64>,
    #[serde(default)]
    tiered_pricing: Option<serde_json::Value>,
    #[serde(default)]
    supports_vision: Option<bool>,
    #[serde(default)]
    supports_function_calling: Option<bool>,
    #[serde(default)]
    supports_streaming: Option<bool>,
    #[serde(default)]
    supports_extended_thinking: Option<bool>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    is_available: Option<bool>,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminGlobalModelCreateRequest {
    name: String,
    display_name: String,
    #[serde(default)]
    default_price_per_request: Option<f64>,
    #[serde(default)]
    default_tiered_pricing: Option<serde_json::Value>,
    #[serde(default)]
    supported_capabilities: Option<Vec<String>>,
    #[serde(default)]
    config: Option<serde_json::Value>,
    #[serde(default)]
    is_active: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AdminGlobalModelUpdateRequest {
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    default_price_per_request: Option<f64>,
    #[serde(default)]
    default_tiered_pricing: Option<serde_json::Value>,
    #[serde(default)]
    supported_capabilities: Option<Vec<String>>,
    #[serde(default)]
    config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AdminBatchDeleteIdsRequest {
    ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AdminBatchAssignToProvidersRequest {
    provider_ids: Vec<String>,
    #[serde(default)]
    create_models: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AdminBatchAssignGlobalModelsRequest {
    global_model_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AdminImportProviderModelsRequest {
    model_ids: Vec<String>,
    #[serde(default)]
    tiered_pricing: Option<serde_json::Value>,
    #[serde(default)]
    price_per_request: Option<f64>,
}
