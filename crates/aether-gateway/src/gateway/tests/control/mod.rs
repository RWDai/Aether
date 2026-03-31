use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use aether_crypto::{
    decrypt_python_fernet_ciphertext, encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY,
};
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::auth_modules::{
    InMemoryAuthModuleReadRepository, StoredLdapModuleConfig, StoredOAuthProviderModuleConfig,
};
use aether_data::repository::candidates::{
    InMemoryRequestCandidateRepository, RequestCandidateStatus, StoredRequestCandidate,
};
use aether_data::repository::global_models::{
    GlobalModelReadRepository, InMemoryGlobalModelReadRepository, StoredAdminGlobalModel,
    StoredAdminProviderModel, StoredProviderActiveGlobalModel, StoredProviderModelStats,
    StoredPublicGlobalModel,
};
use aether_data::repository::management_tokens::{
    InMemoryManagementTokenRepository, ManagementTokenReadRepository, StoredManagementToken,
    StoredManagementTokenUserSummary, StoredManagementTokenWithUser,
};
use aether_data::repository::oauth_providers::{
    InMemoryOAuthProviderRepository, OAuthProviderReadRepository, StoredOAuthProviderConfig,
};
use aether_data::repository::provider_catalog::{
    InMemoryProviderCatalogReadRepository, ProviderCatalogReadRepository,
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::repository::proxy_nodes::{
    InMemoryProxyNodeRepository, ProxyNodeReadRepository, StoredProxyNode, StoredProxyNodeEvent,
};
use aether_data::repository::quota::{
    InMemoryProviderQuotaRepository, StoredProviderQuotaSnapshot,
};
use aether_data::repository::wallet::InMemoryWalletRepository;
use axum::routing::post;
use http::HeaderMap;

mod admin;
mod helpers;
mod internal;
mod proxy;

use crate::gateway::data::GatewayDataState;
use helpers::*;
