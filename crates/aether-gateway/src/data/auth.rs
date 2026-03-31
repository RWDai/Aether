use aether_data::repository::auth::{AuthApiKeyLookupKey, StoredAuthApiKeySnapshot};
use aether_data::DataLayerError;

use super::state::GatewayDataState;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct StoredGatewayAuthApiKeySnapshot {
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) email: Option<String>,
    pub(crate) user_role: String,
    pub(crate) user_auth_source: String,
    pub(crate) user_is_active: bool,
    pub(crate) user_is_deleted: bool,
    pub(crate) user_rate_limit: Option<i32>,
    pub(crate) user_allowed_providers: Option<Vec<String>>,
    pub(crate) user_allowed_api_formats: Option<Vec<String>>,
    pub(crate) user_allowed_models: Option<Vec<String>>,
    pub(crate) api_key_id: String,
    pub(crate) api_key_name: Option<String>,
    pub(crate) api_key_is_active: bool,
    pub(crate) api_key_is_locked: bool,
    pub(crate) api_key_is_standalone: bool,
    pub(crate) api_key_rate_limit: Option<i32>,
    pub(crate) api_key_concurrent_limit: Option<i32>,
    pub(crate) api_key_expires_at_unix_secs: Option<u64>,
    pub(crate) api_key_allowed_providers: Option<Vec<String>>,
    pub(crate) api_key_allowed_api_formats: Option<Vec<String>>,
    pub(crate) api_key_allowed_models: Option<Vec<String>>,
    pub(crate) currently_usable: bool,
}

impl StoredGatewayAuthApiKeySnapshot {
    fn from_stored(snapshot: StoredAuthApiKeySnapshot, now_unix_secs: u64) -> Self {
        let currently_usable = snapshot.is_currently_usable(now_unix_secs);
        Self {
            user_id: snapshot.user_id,
            username: snapshot.username,
            email: snapshot.email,
            user_role: snapshot.user_role,
            user_auth_source: snapshot.user_auth_source,
            user_is_active: snapshot.user_is_active,
            user_is_deleted: snapshot.user_is_deleted,
            user_rate_limit: snapshot.user_rate_limit,
            user_allowed_providers: snapshot.user_allowed_providers,
            user_allowed_api_formats: snapshot.user_allowed_api_formats,
            user_allowed_models: snapshot.user_allowed_models,
            api_key_id: snapshot.api_key_id,
            api_key_name: snapshot.api_key_name,
            api_key_is_active: snapshot.api_key_is_active,
            api_key_is_locked: snapshot.api_key_is_locked,
            api_key_is_standalone: snapshot.api_key_is_standalone,
            api_key_rate_limit: snapshot.api_key_rate_limit,
            api_key_concurrent_limit: snapshot.api_key_concurrent_limit,
            api_key_expires_at_unix_secs: snapshot.api_key_expires_at_unix_secs,
            api_key_allowed_providers: snapshot.api_key_allowed_providers,
            api_key_allowed_api_formats: snapshot.api_key_allowed_api_formats,
            api_key_allowed_models: snapshot.api_key_allowed_models,
            currently_usable,
        }
    }

    pub(crate) fn effective_allowed_providers(&self) -> Option<&[String]> {
        self.api_key_allowed_providers
            .as_deref()
            .or(self.user_allowed_providers.as_deref())
    }

    pub(crate) fn effective_allowed_api_formats(&self) -> Option<&[String]> {
        self.api_key_allowed_api_formats
            .as_deref()
            .or(self.user_allowed_api_formats.as_deref())
    }

    pub(crate) fn effective_allowed_models(&self) -> Option<&[String]> {
        self.api_key_allowed_models
            .as_deref()
            .or(self.user_allowed_models.as_deref())
    }
}

pub(crate) async fn read_auth_api_key_snapshot(
    state: &GatewayDataState,
    user_id: &str,
    api_key_id: &str,
    now_unix_secs: u64,
) -> Result<Option<StoredGatewayAuthApiKeySnapshot>, DataLayerError> {
    let snapshot = state
        .find_auth_api_key_snapshot(AuthApiKeyLookupKey::UserApiKeyIds {
            user_id,
            api_key_id,
        })
        .await?;
    Ok(snapshot
        .map(|snapshot| StoredGatewayAuthApiKeySnapshot::from_stored(snapshot, now_unix_secs)))
}

pub(crate) async fn read_auth_api_key_snapshot_by_key_hash(
    state: &GatewayDataState,
    key_hash: &str,
    now_unix_secs: u64,
) -> Result<Option<StoredGatewayAuthApiKeySnapshot>, DataLayerError> {
    let snapshot = state
        .find_auth_api_key_snapshot(AuthApiKeyLookupKey::KeyHash(key_hash))
        .await?;
    Ok(snapshot
        .map(|snapshot| StoredGatewayAuthApiKeySnapshot::from_stored(snapshot, now_unix_secs)))
}

#[cfg(test)]
mod tests {
    use super::super::GatewayDataState;
    use super::{
        read_auth_api_key_snapshot, read_auth_api_key_snapshot_by_key_hash,
        StoredGatewayAuthApiKeySnapshot,
    };
    use aether_data::repository::auth::{
        InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
    };
    use std::sync::Arc;

    fn sample_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(200),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
        )
        .expect("snapshot should build")
    }

    #[tokio::test]
    async fn reads_trusted_auth_snapshot_and_derives_usability() {
        let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some("hash-1".to_string()),
            sample_snapshot("key-1", "user-1"),
        )]));
        let state = GatewayDataState::with_auth_api_key_reader_for_tests(repository);

        let snapshot = read_auth_api_key_snapshot(&state, "user-1", "key-1", 150)
            .await
            .expect("read should succeed")
            .expect("snapshot should exist");

        assert_eq!(
            snapshot,
            StoredGatewayAuthApiKeySnapshot {
                user_id: "user-1".to_string(),
                username: "alice".to_string(),
                email: Some("alice@example.com".to_string()),
                user_role: "user".to_string(),
                user_auth_source: "local".to_string(),
                user_is_active: true,
                user_is_deleted: false,
                user_rate_limit: None,
                user_allowed_providers: Some(vec!["openai".to_string()]),
                user_allowed_api_formats: Some(vec!["openai:chat".to_string()]),
                user_allowed_models: Some(vec!["gpt-4.1".to_string()]),
                api_key_id: "key-1".to_string(),
                api_key_name: Some("default".to_string()),
                api_key_is_active: true,
                api_key_is_locked: false,
                api_key_is_standalone: false,
                api_key_rate_limit: Some(60),
                api_key_concurrent_limit: Some(5),
                api_key_expires_at_unix_secs: Some(200),
                api_key_allowed_providers: Some(vec!["openai".to_string()]),
                api_key_allowed_api_formats: Some(vec!["openai:chat".to_string()]),
                api_key_allowed_models: Some(vec!["gpt-4.1".to_string()]),
                currently_usable: true,
            }
        );
    }

    #[tokio::test]
    async fn reads_auth_snapshot_by_key_hash() {
        let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some("hash-lookup".to_string()),
            sample_snapshot("key-1", "user-1"),
        )]));
        let state = GatewayDataState::with_auth_api_key_reader_for_tests(repository);

        let snapshot = read_auth_api_key_snapshot_by_key_hash(&state, "hash-lookup", 150)
            .await
            .expect("read should succeed")
            .expect("snapshot should exist");

        assert_eq!(snapshot.user_id, "user-1");
        assert_eq!(snapshot.api_key_id, "key-1");
        assert_eq!(
            snapshot.effective_allowed_providers(),
            Some(&["openai".to_string()][..])
        );
        assert_eq!(
            snapshot.effective_allowed_api_formats(),
            Some(&["openai:chat".to_string()][..])
        );
        assert_eq!(
            snapshot.effective_allowed_models(),
            Some(&["gpt-4.1".to_string()][..])
        );
    }
}
