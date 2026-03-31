use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;

use aether_data::redis::{RedisKvRunner, RedisKvRunnerConfig, RedisLockRunner, RedisStreamRunner};
use aether_data::repository::announcements::{
    AnnouncementListQuery, AnnouncementReadRepository, AnnouncementWriteRepository,
    CreateAnnouncementRecord, StoredAnnouncement, StoredAnnouncementPage, UpdateAnnouncementRecord,
};
use aether_data::repository::auth::{
    AuthApiKeyLookupKey, AuthApiKeyReadRepository, AuthApiKeyWriteRepository,
    StoredAuthApiKeyExportRecord, StoredAuthApiKeySnapshot,
};
use aether_data::repository::auth_modules::{
    AuthModuleReadRepository, AuthModuleWriteRepository, StoredLdapModuleConfig,
    StoredOAuthProviderModuleConfig,
};
use aether_data::repository::billing::{BillingReadRepository, StoredBillingModelContext};
use aether_data::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, StoredMinimalCandidateSelectionRow,
};
use aether_data::repository::candidates::{
    PublicHealthStatusCount, PublicHealthTimelineBucket, RequestCandidateReadRepository,
    RequestCandidateWriteRepository, StoredRequestCandidate, UpsertRequestCandidateRecord,
};
use aether_data::repository::gemini_file_mappings::{
    GeminiFileMappingListQuery, GeminiFileMappingReadRepository, GeminiFileMappingStats,
    GeminiFileMappingWriteRepository, StoredGeminiFileMapping, StoredGeminiFileMappingListPage,
    UpsertGeminiFileMappingRecord,
};
use aether_data::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, CreateAdminGlobalModelRecord,
    GlobalModelReadRepository, GlobalModelWriteRepository, PublicCatalogModelListQuery,
    PublicCatalogModelSearchQuery, PublicGlobalModelQuery, StoredAdminGlobalModel,
    StoredAdminGlobalModelPage, StoredAdminProviderModel, StoredProviderActiveGlobalModel,
    StoredProviderModelStats, StoredPublicCatalogModel, StoredPublicGlobalModel,
    StoredPublicGlobalModelPage, UpdateAdminGlobalModelRecord, UpsertAdminProviderModelRecord,
};
use aether_data::repository::management_tokens::{
    CreateManagementTokenRecord, ManagementTokenListQuery, ManagementTokenReadRepository,
    ManagementTokenWriteRepository, RegenerateManagementTokenSecret, StoredManagementToken,
    StoredManagementTokenListPage, StoredManagementTokenWithUser, UpdateManagementTokenRecord,
};
use aether_data::repository::oauth_providers::{
    OAuthProviderReadRepository, OAuthProviderWriteRepository, StoredOAuthProviderConfig,
    UpsertOAuthProviderConfigRecord,
};
use aether_data::repository::provider_catalog::{
    ProviderCatalogKeyListQuery, ProviderCatalogReadRepository, ProviderCatalogWriteRepository,
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogKeyPage,
    StoredProviderCatalogKeyStats, StoredProviderCatalogProvider,
};
use aether_data::repository::proxy_nodes::{
    ProxyNodeHeartbeatMutation, ProxyNodeReadRepository, ProxyNodeTunnelStatusMutation,
    ProxyNodeWriteRepository, StoredProxyNode, StoredProxyNodeEvent,
};
use aether_data::repository::quota::{
    ProviderQuotaReadRepository, ProviderQuotaWriteRepository, StoredProviderQuotaSnapshot,
};
use aether_data::repository::shadow_results::{
    merge_shadow_result_sample, RecordShadowResultSample, ShadowResultLookupKey,
    ShadowResultReadRepository, ShadowResultWriteRepository, StoredShadowResult,
};
use aether_data::repository::usage::{
    StoredProviderUsageSummary, StoredRequestUsageAudit, UpsertUsageRecord, UsageReadRepository,
    UsageWriteRepository,
};
use aether_data::repository::users::{
    StoredUserAuthRecord, StoredUserExportRow, StoredUserSummary, UserReadRepository,
};
use aether_data::repository::video_tasks::{
    StoredVideoTask, UpsertVideoTask, VideoTaskLookupKey, VideoTaskModelCount,
    VideoTaskQueryFilter, VideoTaskReadRepository, VideoTaskStatusCount, VideoTaskWriteRepository,
};
use aether_data::repository::wallet::{
    StoredUsageSettlement, StoredWalletSnapshot, UsageSettlementInput, WalletLookupKey,
    WalletReadRepository, WalletWriteRepository,
};
use aether_data::{DataBackends, DataLayerError};
use chrono::{DateTime, Utc};

use super::auth::{
    read_auth_api_key_snapshot, read_auth_api_key_snapshot_by_key_hash,
    StoredGatewayAuthApiKeySnapshot,
};
use super::candidates::{read_request_candidate_trace, RequestCandidateTrace};
use super::config::GatewayDataConfig;
use super::decision_trace::{read_decision_trace, DecisionTrace};
use super::video_tasks::read_video_task_response;
use crate::gateway::provider_transport::{
    read_provider_transport_snapshot, GatewayProviderTransportSnapshot,
};
use crate::gateway::scheduler::{
    read_minimal_candidate_selection, GatewayMinimalCandidateSelectionCandidate,
};
use crate::gateway::usage::{
    read_request_audit_bundle, read_request_usage_audit, RequestAuditBundle, RequestUsageAudit,
};
use crate::gateway::video_tasks::LocalVideoTaskReadResponse;

#[derive(Debug, Clone)]
pub(crate) struct StoredSystemConfigEntry {
    pub(crate) key: String,
    pub(crate) value: serde_json::Value,
    pub(crate) description: Option<String>,
    pub(crate) updated_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredUserSessionRecord {
    pub(crate) id: String,
    pub(crate) user_id: String,
    pub(crate) client_device_id: String,
    pub(crate) device_label: Option<String>,
    pub(crate) refresh_token_hash: String,
    pub(crate) prev_refresh_token_hash: Option<String>,
    pub(crate) rotated_at: Option<DateTime<Utc>>,
    pub(crate) last_seen_at: Option<DateTime<Utc>>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) revoked_at: Option<DateTime<Utc>>,
    pub(crate) revoke_reason: Option<String>,
    pub(crate) ip_address: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) created_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
}

impl StoredUserSessionRecord {
    pub(crate) const REFRESH_GRACE_SECONDS: i64 = 10;
    pub(crate) const TOUCH_INTERVAL_SECONDS: i64 = 300;

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: String,
        user_id: String,
        client_device_id: String,
        device_label: Option<String>,
        refresh_token_hash: String,
        prev_refresh_token_hash: Option<String>,
        rotated_at: Option<DateTime<Utc>>,
        last_seen_at: Option<DateTime<Utc>>,
        expires_at: Option<DateTime<Utc>>,
        revoked_at: Option<DateTime<Utc>>,
        revoke_reason: Option<String>,
        ip_address: Option<String>,
        user_agent: Option<String>,
        created_at: Option<DateTime<Utc>>,
        updated_at: Option<DateTime<Utc>>,
    ) -> Result<Self, DataLayerError> {
        if id.trim().is_empty() {
            return Err(DataLayerError::UnexpectedValue(
                "user_sessions.id is empty".to_string(),
            ));
        }
        if user_id.trim().is_empty() {
            return Err(DataLayerError::UnexpectedValue(
                "user_sessions.user_id is empty".to_string(),
            ));
        }
        if client_device_id.trim().is_empty() {
            return Err(DataLayerError::UnexpectedValue(
                "user_sessions.client_device_id is empty".to_string(),
            ));
        }
        if refresh_token_hash.trim().is_empty() {
            return Err(DataLayerError::UnexpectedValue(
                "user_sessions.refresh_token_hash is empty".to_string(),
            ));
        }

        Ok(Self {
            id,
            user_id,
            client_device_id,
            device_label,
            refresh_token_hash,
            prev_refresh_token_hash,
            rotated_at,
            last_seen_at,
            expires_at,
            revoked_at,
            revoke_reason,
            ip_address,
            user_agent,
            created_at,
            updated_at,
        })
    }

    pub(crate) fn hash_refresh_token(token: &str) -> String {
        use sha2::Digest;

        let mut hasher = sha2::Sha256::new();
        hasher.update(token.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub(crate) fn verify_refresh_token(&self, token: &str, now: DateTime<Utc>) -> (bool, bool) {
        let token_hash = Self::hash_refresh_token(token);
        if self.refresh_token_hash == token_hash {
            return (true, false);
        }
        let Some(prev_hash) = self.prev_refresh_token_hash.as_ref() else {
            return (false, false);
        };
        let Some(rotated_at) = self.rotated_at else {
            return (false, false);
        };
        if prev_hash == &token_hash
            && now.signed_duration_since(rotated_at).num_seconds() <= Self::REFRESH_GRACE_SECONDS
        {
            return (true, true);
        }
        (false, false)
    }

    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    pub(crate) fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|expires_at| expires_at <= now)
    }

    pub(crate) fn should_touch(&self, now: DateTime<Utc>) -> bool {
        self.last_seen_at
            .map(|last_seen_at| {
                now.signed_duration_since(last_seen_at).num_seconds()
                    >= Self::TOUCH_INTERVAL_SECONDS
            })
            .unwrap_or(true)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredUserPreferenceRecord {
    pub(crate) user_id: String,
    pub(crate) avatar_url: Option<String>,
    pub(crate) bio: Option<String>,
    pub(crate) default_provider_id: Option<String>,
    pub(crate) default_provider_name: Option<String>,
    pub(crate) theme: String,
    pub(crate) language: String,
    pub(crate) timezone: String,
    pub(crate) email_notifications: bool,
    pub(crate) usage_alerts: bool,
    pub(crate) announcement_notifications: bool,
}

impl StoredUserPreferenceRecord {
    pub(crate) fn default_for_user(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            avatar_url: None,
            bio: None,
            default_provider_id: None,
            default_provider_name: None,
            theme: "light".to_string(),
            language: "zh-CN".to_string(),
            timezone: "Asia/Shanghai".to_string(),
            email_notifications: true,
            usage_alerts: true,
            announcement_notifications: true,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct GatewayDataState {
    config: GatewayDataConfig,
    backends: Option<DataBackends>,
    auth_api_key_reader: Option<Arc<dyn AuthApiKeyReadRepository>>,
    auth_api_key_writer: Option<Arc<dyn AuthApiKeyWriteRepository>>,
    auth_module_reader: Option<Arc<dyn AuthModuleReadRepository>>,
    auth_module_writer: Option<Arc<dyn AuthModuleWriteRepository>>,
    announcement_reader: Option<Arc<dyn AnnouncementReadRepository>>,
    announcement_writer: Option<Arc<dyn AnnouncementWriteRepository>>,
    management_token_reader: Option<Arc<dyn ManagementTokenReadRepository>>,
    management_token_writer: Option<Arc<dyn ManagementTokenWriteRepository>>,
    oauth_provider_reader: Option<Arc<dyn OAuthProviderReadRepository>>,
    oauth_provider_writer: Option<Arc<dyn OAuthProviderWriteRepository>>,
    proxy_node_reader: Option<Arc<dyn ProxyNodeReadRepository>>,
    proxy_node_writer: Option<Arc<dyn ProxyNodeWriteRepository>>,
    billing_reader: Option<Arc<dyn BillingReadRepository>>,
    gemini_file_mapping_reader: Option<Arc<dyn GeminiFileMappingReadRepository>>,
    gemini_file_mapping_writer: Option<Arc<dyn GeminiFileMappingWriteRepository>>,
    global_model_reader: Option<Arc<dyn GlobalModelReadRepository>>,
    global_model_writer: Option<Arc<dyn GlobalModelWriteRepository>>,
    minimal_candidate_selection_reader: Option<Arc<dyn MinimalCandidateSelectionReadRepository>>,
    request_candidate_reader: Option<Arc<dyn RequestCandidateReadRepository>>,
    request_candidate_writer: Option<Arc<dyn RequestCandidateWriteRepository>>,
    provider_catalog_reader: Option<Arc<dyn ProviderCatalogReadRepository>>,
    provider_catalog_writer: Option<Arc<dyn ProviderCatalogWriteRepository>>,
    provider_quota_reader: Option<Arc<dyn ProviderQuotaReadRepository>>,
    provider_quota_writer: Option<Arc<dyn ProviderQuotaWriteRepository>>,
    usage_reader: Option<Arc<dyn UsageReadRepository>>,
    usage_writer: Option<Arc<dyn UsageWriteRepository>>,
    user_reader: Option<Arc<dyn UserReadRepository>>,
    user_preferences: Option<Arc<RwLock<BTreeMap<String, StoredUserPreferenceRecord>>>>,
    usage_worker_runner: Option<RedisStreamRunner>,
    video_task_reader: Option<Arc<dyn VideoTaskReadRepository>>,
    video_task_writer: Option<Arc<dyn VideoTaskWriteRepository>>,
    wallet_reader: Option<Arc<dyn WalletReadRepository>>,
    wallet_writer: Option<Arc<dyn WalletWriteRepository>>,
    shadow_result_reader: Option<Arc<dyn ShadowResultReadRepository>>,
    shadow_result_writer: Option<Arc<dyn ShadowResultWriteRepository>>,
    system_config_values: Option<Arc<RwLock<BTreeMap<String, StoredSystemConfigEntry>>>>,
}

impl fmt::Debug for GatewayDataState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayDataState")
            .field("config", &self.config)
            .field("has_backends", &self.backends.is_some())
            .field(
                "has_auth_api_key_reader",
                &self.auth_api_key_reader.is_some(),
            )
            .field(
                "has_auth_api_key_writer",
                &self.auth_api_key_writer.is_some(),
            )
            .field("has_auth_module_reader", &self.auth_module_reader.is_some())
            .field("has_auth_module_writer", &self.auth_module_writer.is_some())
            .field(
                "has_announcement_reader",
                &self.announcement_reader.is_some(),
            )
            .field(
                "has_announcement_writer",
                &self.announcement_writer.is_some(),
            )
            .field(
                "has_management_token_reader",
                &self.management_token_reader.is_some(),
            )
            .field(
                "has_management_token_writer",
                &self.management_token_writer.is_some(),
            )
            .field(
                "has_oauth_provider_reader",
                &self.oauth_provider_reader.is_some(),
            )
            .field(
                "has_oauth_provider_writer",
                &self.oauth_provider_writer.is_some(),
            )
            .field("has_proxy_node_reader", &self.proxy_node_reader.is_some())
            .field("has_proxy_node_writer", &self.proxy_node_writer.is_some())
            .field("has_billing_reader", &self.billing_reader.is_some())
            .field(
                "has_gemini_file_mapping_reader",
                &self.gemini_file_mapping_reader.is_some(),
            )
            .field(
                "has_gemini_file_mapping_writer",
                &self.gemini_file_mapping_writer.is_some(),
            )
            .field(
                "has_global_model_reader",
                &self.global_model_reader.is_some(),
            )
            .field(
                "has_global_model_writer",
                &self.global_model_writer.is_some(),
            )
            .field(
                "has_minimal_candidate_selection_reader",
                &self.minimal_candidate_selection_reader.is_some(),
            )
            .field(
                "has_request_candidate_reader",
                &self.request_candidate_reader.is_some(),
            )
            .field(
                "has_request_candidate_writer",
                &self.request_candidate_writer.is_some(),
            )
            .field(
                "has_provider_catalog_reader",
                &self.provider_catalog_reader.is_some(),
            )
            .field(
                "has_provider_catalog_writer",
                &self.provider_catalog_writer.is_some(),
            )
            .field(
                "has_provider_quota_reader",
                &self.provider_quota_reader.is_some(),
            )
            .field(
                "has_provider_quota_writer",
                &self.provider_quota_writer.is_some(),
            )
            .field("has_usage_reader", &self.usage_reader.is_some())
            .field("has_usage_writer", &self.usage_writer.is_some())
            .field("has_user_preferences", &self.user_preferences.is_some())
            .field(
                "has_usage_worker_runner",
                &self.usage_worker_runner.is_some(),
            )
            .field("has_video_task_reader", &self.video_task_reader.is_some())
            .field("has_video_task_writer", &self.video_task_writer.is_some())
            .field("has_wallet_reader", &self.wallet_reader.is_some())
            .field("has_wallet_writer", &self.wallet_writer.is_some())
            .field(
                "has_shadow_result_reader",
                &self.shadow_result_reader.is_some(),
            )
            .field(
                "has_shadow_result_writer",
                &self.shadow_result_writer.is_some(),
            )
            .field(
                "has_system_config_values",
                &self.system_config_values.is_some(),
            )
            .finish()
    }
}

mod auth;
mod catalog;
mod core;
mod models;
mod runtime;
#[cfg(test)]
mod testing;
