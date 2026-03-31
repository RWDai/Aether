use super::*;

pub(super) fn classify_admin_route(
    method: &http::Method,
    normalized_path: &str,
) -> Option<ClassifiedRoute> {
    let normalized_path_no_trailing = normalized_path.trim_end_matches('/');
    let normalized_path_no_trailing = if normalized_path_no_trailing.is_empty() {
        "/"
    } else {
        normalized_path_no_trailing
    };

    if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/providers" | "/api/admin/providers/"
        )
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "list_providers",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/management-tokens" | "/api/admin/management-tokens/"
        )
    {
        Some(classified(
            "admin_proxy",
            "management_tokens_manage",
            "list_tokens",
            "admin:management_tokens",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/management-tokens/")
    {
        Some(classified(
            "admin_proxy",
            "management_tokens_manage",
            "get_token",
            "admin:management_tokens",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/management-tokens/")
    {
        Some(classified(
            "admin_proxy",
            "management_tokens_manage",
            "delete_token",
            "admin:management_tokens",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/management-tokens/")
        && normalized_path.ends_with("/status")
    {
        Some(classified(
            "admin_proxy",
            "management_tokens_manage",
            "toggle_status",
            "admin:management_tokens",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/ldap/config" | "/api/admin/ldap/config/"
        )
    {
        Some(classified(
            "admin_proxy",
            "ldap_manage",
            "get_config",
            "admin:ldap",
            false,
        ))
    } else if method == http::Method::PUT
        && matches!(
            normalized_path,
            "/api/admin/ldap/config" | "/api/admin/ldap/config/"
        )
    {
        Some(classified(
            "admin_proxy",
            "ldap_manage",
            "set_config",
            "admin:ldap",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/ldap/test" | "/api/admin/ldap/test/"
        )
    {
        Some(classified(
            "admin_proxy",
            "ldap_manage",
            "test_connection",
            "admin:ldap",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/gemini-files/mappings" | "/api/admin/gemini-files/mappings/"
        )
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "list_mappings",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/gemini-files/stats" | "/api/admin/gemini-files/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "stats",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::DELETE
        && matches!(
            normalized_path,
            "/api/admin/gemini-files/mappings" | "/api/admin/gemini-files/mappings/"
        )
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "cleanup_mappings",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/gemini-files/mappings/")
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "delete_mapping",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/gemini-files/capable-keys" | "/api/admin/gemini-files/capable-keys/"
        )
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "capable_keys",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/gemini-files/upload" | "/api/admin/gemini-files/upload/"
        )
    {
        Some(classified(
            "admin_proxy",
            "gemini_files_manage",
            "upload",
            "admin:gemini_files",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/modules/status" {
        Some(classified(
            "admin_proxy",
            "modules_manage",
            "status_list",
            "admin:modules",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/modules/status/")
    {
        Some(classified(
            "admin_proxy",
            "modules_manage",
            "status_detail",
            "admin:modules",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/modules/status/")
        && normalized_path.ends_with("/enabled")
    {
        Some(classified(
            "admin_proxy",
            "modules_manage",
            "set_enabled",
            "admin:modules",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/adaptive/keys" | "/api/admin/adaptive/keys/"
        )
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "list_keys",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/adaptive/summary" | "/api/admin/adaptive/summary/"
        )
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "summary",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/adaptive/keys/")
        && normalized_path.ends_with("/stats")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "get_stats",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/adaptive/keys/")
        && normalized_path.ends_with("/mode")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "toggle_mode",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/adaptive/keys/")
        && normalized_path.ends_with("/limit")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "set_limit",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/adaptive/keys/")
        && normalized_path.ends_with("/learning")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "adaptive_manage",
            "reset_learning",
            "admin:adaptive",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/provider-strategy/strategies" | "/api/admin/provider-strategy/strategies/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_strategy_manage",
            "list_strategies",
            "admin:provider_strategy",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/provider-strategy/providers/")
        && normalized_path.ends_with("/billing")
    {
        Some(classified(
            "admin_proxy",
            "provider_strategy_manage",
            "update_provider_billing",
            "admin:provider_strategy",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/provider-strategy/providers/")
        && normalized_path.ends_with("/stats")
    {
        Some(classified(
            "admin_proxy",
            "provider_strategy_manage",
            "get_provider_stats",
            "admin:provider_strategy",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/provider-strategy/providers/")
        && normalized_path.ends_with("/quota")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_strategy_manage",
            "reset_provider_quota",
            "admin:provider_strategy",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/billing/presets" | "/api/admin/billing/presets/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "list_presets",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/billing/presets/apply" | "/api/admin/billing/presets/apply/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "apply_preset",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/billing/rules" | "/api/admin/billing/rules/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "list_rules",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/billing/rules/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "get_rule",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/billing/rules" | "/api/admin/billing/rules/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "create_rule",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/billing/rules/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "update_rule",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "list_collectors",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/billing/collectors/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "get_collector",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/billing/collectors" | "/api/admin/billing/collectors/"
        )
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "create_collector",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/billing/collectors/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "billing_manage",
            "update_collector",
            "admin:billing",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/payments/orders" | "/api/admin/payments/orders/"
        )
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "list_orders",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path_no_trailing.starts_with("/api/admin/payments/orders/")
        && normalized_path_no_trailing.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "get_order",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/payments/orders/")
        && normalized_path_no_trailing.ends_with("/expire")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "expire_order",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/payments/orders/")
        && normalized_path_no_trailing.ends_with("/credit")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "credit_order",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/payments/orders/")
        && normalized_path_no_trailing.ends_with("/fail")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "fail_order",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/payments/callbacks" | "/api/admin/payments/callbacks/"
        )
    {
        Some(classified(
            "admin_proxy",
            "payments_manage",
            "list_callbacks",
            "admin:payments",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/provider-query/models" | "/api/admin/provider-query/models/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_query_manage",
            "query_models",
            "admin:provider_query",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/provider-query/test-model" | "/api/admin/provider-query/test-model/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_query_manage",
            "test_model",
            "admin:provider_query",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/provider-query/test-model-failover"
                | "/api/admin/provider-query/test-model-failover/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_query_manage",
            "test_model_failover",
            "admin:provider_query",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/security/ip/blacklist" | "/api/admin/security/ip/blacklist/"
        )
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "blacklist_add",
            "admin:security",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/security/ip/blacklist/")
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "blacklist_remove",
            "admin:security",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/security/ip/blacklist/stats" | "/api/admin/security/ip/blacklist/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "blacklist_stats",
            "admin:security",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/security/ip/blacklist" | "/api/admin/security/ip/blacklist/"
        )
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "blacklist_list",
            "admin:security",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/security/ip/whitelist" | "/api/admin/security/ip/whitelist/"
        )
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "whitelist_add",
            "admin:security",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/security/ip/whitelist/")
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "whitelist_remove",
            "admin:security",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/security/ip/whitelist" | "/api/admin/security/ip/whitelist/"
        )
    {
        Some(classified(
            "admin_proxy",
            "security_manage",
            "whitelist_list",
            "admin:security",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/api-keys" | "/api/admin/api-keys/"
        )
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "list_api_keys",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/api-keys" | "/api/admin/api-keys/"
        )
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "create_api_key",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "api_key_detail",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "update_api_key",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "toggle_api_key",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "api_keys_manage",
            "delete_api_key",
            "admin:api_keys",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/pool/overview" | "/api/admin/pool/overview/"
        )
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "overview",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/pool/scheduling-presets" | "/api/admin/pool/scheduling-presets/"
        )
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "scheduling_presets",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.ends_with("/keys")
        && normalized_path_no_trailing.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "list_keys",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.ends_with("/keys/batch-import")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "batch_import_keys",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.ends_with("/keys/batch-action")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "batch_action_keys",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.ends_with("/keys/resolve-selection")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "resolve_selection",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.contains("/keys/batch-delete-task/")
        && normalized_path_no_trailing.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "batch_delete_task_status",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path_no_trailing.starts_with("/api/admin/pool/")
        && normalized_path_no_trailing.ends_with("/keys/cleanup-banned")
        && normalized_path_no_trailing.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "pool_manage",
            "cleanup_banned_keys",
            "admin:pool",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/aggregation/stats" | "/api/admin/usage/aggregation/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "aggregation_stats",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/stats" | "/api/admin/usage/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "stats",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/heatmap" | "/api/admin/usage/heatmap/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "heatmap",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/records" | "/api/admin/usage/records/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "records",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/active" | "/api/admin/usage/active/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "active",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/cache-affinity/hit-analysis"
                | "/api/admin/usage/cache-affinity/hit-analysis/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "cache_affinity_hit_analysis",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/cache-affinity/interval-timeline"
                | "/api/admin/usage/cache-affinity/interval-timeline/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "cache_affinity_interval_timeline",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/usage/cache-affinity/ttl-analysis"
                | "/api/admin/usage/cache-affinity/ttl-analysis/"
        )
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "cache_affinity_ttl_analysis",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/usage/")
        && normalized_path.ends_with("/curl")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "curl",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/usage/")
        && normalized_path.ends_with("/replay")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "replay",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/usage/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "usage_manage",
            "detail",
            "admin:usage",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/providers/quota-usage" | "/api/admin/stats/providers/quota-usage/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "provider_quota_usage",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/comparison" | "/api/admin/stats/comparison/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "comparison",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/errors/distribution" | "/api/admin/stats/errors/distribution/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "error_distribution",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/performance/percentiles"
                | "/api/admin/stats/performance/percentiles/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "performance_percentiles",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/cost/forecast" | "/api/admin/stats/cost/forecast/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "cost_forecast",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/cost/savings" | "/api/admin/stats/cost/savings/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "cost_savings",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/leaderboard/api-keys" | "/api/admin/stats/leaderboard/api-keys/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "leaderboard_api_keys",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/leaderboard/models" | "/api/admin/stats/leaderboard/models/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "leaderboard_models",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/leaderboard/users" | "/api/admin/stats/leaderboard/users/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "leaderboard_users",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/stats/time-series" | "/api/admin/stats/time-series/"
        )
    {
        Some(classified(
            "admin_proxy",
            "stats_manage",
            "time_series",
            "admin:stats",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/monitoring/audit-logs" | "/api/admin/monitoring/audit-logs/"
        )
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "audit_logs",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::GET
        && (matches!(
            normalized_path,
            "/api/admin/monitoring/system-status"
                | "/api/admin/monitoring/system-status/"
                | "/api/admin/monitoring/suspicious-activities"
                | "/api/admin/monitoring/suspicious-activities/"
                | "/api/admin/monitoring/user-behavior"
        ) || normalized_path.starts_with("/api/admin/monitoring/user-behavior/"))
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "user_behavior",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::GET
        && (matches!(
            normalized_path,
            "/api/admin/monitoring/resilience-status"
                | "/api/admin/monitoring/resilience-status/"
                | "/api/admin/monitoring/resilience/circuit-history"
                | "/api/admin/monitoring/resilience/circuit-history/"
        ) || (normalized_path == "/api/admin/monitoring/resilience/error-stats"))
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "monitoring_resilience",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path == "/api/admin/monitoring/resilience/error-stats"
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "monitoring_resilience",
            "admin:monitoring",
            false,
        ))
    } else if (method == http::Method::GET || method == http::Method::DELETE)
        && normalized_path.starts_with("/api/admin/monitoring/cache")
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "monitoring_cache",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/monitoring/trace/stats/provider/")
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "trace_provider_stats",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/monitoring/trace/")
        && !normalized_path.starts_with("/api/admin/monitoring/trace/stats/")
    {
        Some(classified(
            "admin_proxy",
            "monitoring",
            "trace_request",
            "admin:monitoring",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/provider-ops/architectures" | "/api/admin/provider-ops/architectures/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "list_architectures",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/video-tasks" | "/api/admin/video-tasks/"
        )
    {
        Some(classified(
            "admin_proxy",
            "video_tasks_manage",
            "list_tasks",
            "admin:video_tasks",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/video-tasks/stats" | "/api/admin/video-tasks/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "video_tasks_manage",
            "stats",
            "admin:video_tasks",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/video-tasks/")
        && normalized_path.ends_with("/video")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "video_tasks_manage",
            "video",
            "admin:video_tasks",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/video-tasks/")
        && normalized_path.ends_with("/cancel")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "video_tasks_manage",
            "cancel",
            "admin:video_tasks",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/video-tasks/")
        && normalized_path["/api/admin/video-tasks/".len()..]
            .split('/')
            .count()
            == 1
        && !matches!(
            normalized_path,
            "/api/admin/video-tasks/stats" | "/api/admin/video-tasks/stats/"
        )
    {
        Some(classified(
            "admin_proxy",
            "video_tasks_manage",
            "detail",
            "admin:video_tasks",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/provider-ops/architectures/")
        && !normalized_path.ends_with('/')
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "get_architecture",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/status")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "get_provider_status",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/config")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "get_provider_config",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/config")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "save_provider_config",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/config")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "delete_provider_config",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/connect")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "connect_provider",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/verify")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "verify_provider",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/disconnect")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "disconnect_provider",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/balance")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "get_provider_balance",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/balance")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "refresh_provider_balance",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.ends_with("/checkin")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "provider_checkin",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/provider-ops/providers/")
        && normalized_path.contains("/actions/")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "execute_provider_action",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/provider-ops/batch/balance" | "/api/admin/provider-ops/batch/balance/"
        )
    {
        Some(classified(
            "admin_proxy",
            "provider_ops_manage",
            "batch_balance",
            "admin:provider_ops",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes" | "/api/admin/proxy-nodes/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "list_nodes",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/register" | "/api/admin/proxy-nodes/register/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "register_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/heartbeat" | "/api/admin/proxy-nodes/heartbeat/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "heartbeat_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/unregister" | "/api/admin/proxy-nodes/unregister/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "unregister_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/manual" | "/api/admin/proxy-nodes/manual/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "create_manual_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/upgrade" | "/api/admin/proxy-nodes/upgrade/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "batch_upgrade_nodes",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/proxy-nodes/test-url" | "/api/admin/proxy-nodes/test-url/"
        )
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "test_proxy_url",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/proxy-nodes/")
        && !normalized_path.ends_with("/test")
        && !normalized_path.ends_with("/config")
        && !normalized_path.ends_with("/events")
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "update_manual_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/proxy-nodes/")
        && !normalized_path.ends_with("/test")
        && !normalized_path.ends_with("/config")
        && !normalized_path.ends_with("/events")
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "delete_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/proxy-nodes/")
        && normalized_path.ends_with("/test")
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "test_node",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/proxy-nodes/")
        && normalized_path.ends_with("/config")
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "update_node_config",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/proxy-nodes/")
        && normalized_path.ends_with("/events")
    {
        Some(classified(
            "admin_proxy",
            "proxy_nodes_manage",
            "list_node_events",
            "admin:proxy_nodes",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/wallets" | "/api/admin/wallets/"
        )
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "list_wallets",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/wallets/ledger" | "/api/admin/wallets/ledger/"
        )
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "ledger",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/wallets/refund-requests" | "/api/admin/wallets/refund-requests/"
        )
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "list_refund_requests",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.ends_with("/transactions")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "list_wallet_transactions",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.ends_with("/refunds")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "list_wallet_refunds",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/wallets/")
        && !normalized_path.ends_with("/transactions")
        && !normalized_path.ends_with("/refunds")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "wallet_detail",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.ends_with("/adjust")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "adjust_balance",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.ends_with("/recharge")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "recharge_balance",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.contains("/refunds/")
        && normalized_path.ends_with("/process")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "process_refund",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.contains("/refunds/")
        && normalized_path.ends_with("/complete")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "complete_refund",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/wallets/")
        && normalized_path.contains("/refunds/")
        && normalized_path.ends_with("/fail")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "wallets_manage",
            "fail_refund",
            "admin:wallets",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(normalized_path, "/api/admin/users" | "/api/admin/users/")
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "list_users",
            "admin:users",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(normalized_path, "/api/admin/users" | "/api/admin/users/")
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "create_user",
            "admin:users",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/sessions")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "list_user_sessions",
            "admin:users",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/sessions")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "delete_user_sessions",
            "admin:users",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.contains("/sessions/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "delete_user_session",
            "admin:users",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/api-keys")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "list_user_api_keys",
            "admin:users",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/api-keys")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "create_user_api_key",
            "admin:users",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.contains("/api-keys/")
        && !normalized_path.ends_with("/lock")
        && !normalized_path.ends_with("/full-key")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "delete_user_api_key",
            "admin:users",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.contains("/api-keys/")
        && !normalized_path.ends_with("/lock")
        && !normalized_path.ends_with("/full-key")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "update_user_api_key",
            "admin:users",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/lock")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "lock_user_api_key",
            "admin:users",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/users/")
        && normalized_path.ends_with("/full-key")
        && normalized_path.matches('/').count() == 7
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "reveal_user_api_key",
            "admin:users",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/users/")
        && !normalized_path.ends_with("/sessions")
        && !normalized_path.contains("/sessions/")
        && !normalized_path.ends_with("/api-keys")
        && !normalized_path.contains("/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "get_user",
            "admin:users",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/users/")
        && !normalized_path.ends_with("/sessions")
        && !normalized_path.contains("/sessions/")
        && !normalized_path.ends_with("/api-keys")
        && !normalized_path.contains("/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "update_user",
            "admin:users",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/users/")
        && !normalized_path.ends_with("/sessions")
        && !normalized_path.contains("/sessions/")
        && !normalized_path.ends_with("/api-keys")
        && !normalized_path.contains("/api-keys/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "users_manage",
            "delete_user",
            "admin:users",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/version" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "version",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/check-update" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "check_update",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/aws-regions" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "aws_regions",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/stats" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "stats",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/settings" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "settings_get",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/config/export" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "config_export",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/users/export" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "users_export",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/config/import" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "config_import",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/users/import" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "users_import",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/smtp/test" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "smtp_test",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/cleanup" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "cleanup",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/purge/config" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_config",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/purge/users" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_users",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/purge/usage" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_usage",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path == "/api/admin/system/purge/audit-logs"
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_audit_logs",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path == "/api/admin/system/purge/request-bodies"
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_request_bodies",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/system/purge/stats" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "purge_stats",
            "admin:system",
            false,
        ))
    } else if method == http::Method::PUT && normalized_path == "/api/admin/system/settings" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "settings_set",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/system/configs" | "/api/admin/system/configs/"
        )
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "configs_list",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/system/configs/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "config_get",
            "admin:system",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/system/configs/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "config_set",
            "admin:system",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/system/configs/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "config_delete",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/system/api-formats" {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "api_formats",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET
        && matches!(
            normalized_path,
            "/api/admin/system/email/templates" | "/api/admin/system/email/templates/"
        )
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "email_templates_list",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/system/email/templates/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "email_template_get",
            "admin:system",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/system/email/templates/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "email_template_set",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/system/email/templates/")
        && normalized_path.ends_with("/preview")
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "email_template_preview",
            "admin:system",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/system/email/templates/")
        && normalized_path.ends_with("/reset")
    {
        Some(classified(
            "admin_proxy",
            "system_manage",
            "email_template_reset",
            "admin:system",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/models/catalog" {
        Some(classified(
            "admin_proxy",
            "model_catalog_manage",
            "catalog",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/models/external" {
        Some(classified(
            "admin_proxy",
            "model_external_manage",
            "external",
            "admin:models",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path == "/api/admin/models/external/cache"
    {
        Some(classified(
            "admin_proxy",
            "model_external_manage",
            "clear_external_cache",
            "admin:models",
            false,
        ))
    } else if method == http::Method::POST
        && matches!(
            normalized_path,
            "/api/admin/providers" | "/api/admin/providers/"
        )
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "create_provider",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "update_provider",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.matches('/').count() == 4
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "delete_provider",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/providers/summary" {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "summary_list",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/summary")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "provider_summary",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/health-monitor")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "health_monitor",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/mapping-preview")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "mapping_preview",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/delete-task/")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "delete_provider_task",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/pool-status")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "pool_status",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/pool/clear-cooldown/")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "clear_pool_cooldown",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/pool/reset-cost/")
    {
        Some(classified(
            "admin_proxy",
            "providers_manage",
            "reset_pool_cost",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/models")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "list_provider_models",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/models")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "create_provider_model",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/models/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "get_provider_model",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/models/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "update_provider_model",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.contains("/models/")
        && normalized_path.matches('/').count() == 6
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "delete_provider_model",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/models/batch")
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "batch_create_provider_models",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/available-source-models")
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "available_source_models",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/assign-global-models")
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "assign_global_models",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/providers/")
        && normalized_path.ends_with("/import-from-upstream")
    {
        Some(classified(
            "admin_proxy",
            "provider_models_manage",
            "import_from_upstream",
            "admin:providers",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path == "/api/admin/models/global/batch-delete"
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "batch_delete_global_models",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/models/global" {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "list_global_models",
            "admin:models",
            false,
        ))
    } else if method == http::Method::POST && normalized_path == "/api/admin/models/global" {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "create_global_model",
            "admin:models",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.ends_with("/assign-to-providers")
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "assign_to_providers",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.ends_with("/providers")
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "global_model_providers",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.ends_with("/routing")
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "routing_preview",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "get_global_model",
            "admin:models",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "update_global_model",
            "admin:models",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/models/global/")
        && normalized_path.matches('/').count() == 5
    {
        Some(classified(
            "admin_proxy",
            "global_models_manage",
            "delete_global_model",
            "admin:models",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path == "/api/admin/endpoints/health/summary"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "health_summary",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/health/key/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "key_health",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::PATCH
        && normalized_path.starts_with("/api/admin/endpoints/health/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "recover_key_health",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::PATCH && normalized_path == "/api/admin/endpoints/health/keys"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "recover_all_keys_health",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::GET && normalized_path == "/api/admin/endpoints/health/status"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "health_status",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path == "/api/admin/endpoints/health/api-formats"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_health",
            "health_api_formats",
            "admin:endpoints_health",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/rpm/key/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_rpm",
            "key_rpm",
            "admin:endpoints_rpm",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/endpoints/rpm/key/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_rpm",
            "reset_key_rpm",
            "admin:endpoints_rpm",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path == "/api/admin/endpoints/keys/grouped-by-format"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "keys_grouped_by_format",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/keys/")
        && normalized_path.ends_with("/reveal")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "reveal_key",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/keys/")
        && normalized_path.ends_with("/export")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "export_key",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/endpoints/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "update_key",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/endpoints/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "delete_key",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path == "/api/admin/endpoints/keys/batch-delete"
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "batch_delete_keys",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/endpoints/keys/")
        && normalized_path.ends_with("/clear-oauth-invalid")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "clear_oauth_invalid",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/endpoints/providers/")
        && normalized_path.ends_with("/refresh-quota")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "refresh_quota",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/endpoints/providers/")
        && normalized_path.ends_with("/keys")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "create_provider_key",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/providers/")
        && normalized_path.ends_with("/keys")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "list_provider_keys",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/providers/")
        && normalized_path.ends_with("/endpoints")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "list_provider_endpoints",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::POST
        && normalized_path.starts_with("/api/admin/endpoints/providers/")
        && normalized_path.ends_with("/endpoints")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "create_endpoint",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::PUT
        && normalized_path.starts_with("/api/admin/endpoints/")
        && !normalized_path.starts_with("/api/admin/endpoints/health/")
        && !normalized_path.starts_with("/api/admin/endpoints/rpm/")
        && !normalized_path.starts_with("/api/admin/endpoints/providers/")
        && !normalized_path.starts_with("/api/admin/endpoints/defaults/")
        && !normalized_path.starts_with("/api/admin/endpoints/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "update_endpoint",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::DELETE
        && normalized_path.starts_with("/api/admin/endpoints/")
        && !normalized_path.starts_with("/api/admin/endpoints/health/")
        && !normalized_path.starts_with("/api/admin/endpoints/rpm/")
        && !normalized_path.starts_with("/api/admin/endpoints/providers/")
        && !normalized_path.starts_with("/api/admin/endpoints/defaults/")
        && !normalized_path.starts_with("/api/admin/endpoints/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "delete_endpoint",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/defaults/")
        && normalized_path.ends_with("/body-rules")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "default_body_rules",
            "admin:endpoints_manage",
            false,
        ))
    } else if method == http::Method::GET
        && normalized_path.starts_with("/api/admin/endpoints/")
        && !normalized_path.starts_with("/api/admin/endpoints/health/")
        && !normalized_path.starts_with("/api/admin/endpoints/rpm/")
        && !normalized_path.starts_with("/api/admin/endpoints/providers/")
        && !normalized_path.starts_with("/api/admin/endpoints/defaults/")
        && !normalized_path.starts_with("/api/admin/endpoints/keys/")
    {
        Some(classified(
            "admin_proxy",
            "endpoints_manage",
            "get_endpoint",
            "admin:endpoints_manage",
            false,
        ))
    } else {
        None
    }
}
