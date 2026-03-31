use async_trait::async_trait;
use sqlx::{postgres::PgRow, PgPool, Row};

use super::types::{
    OAuthProviderReadRepository, OAuthProviderWriteRepository, StoredOAuthProviderConfig,
    UpsertOAuthProviderConfigRecord,
};
use crate::DataLayerError;

const LIST_OAUTH_PROVIDER_CONFIGS_SQL: &str = r#"
SELECT
  provider_type,
  display_name,
  client_id,
  client_secret_encrypted,
  authorization_url_override,
  token_url_override,
  userinfo_url_override,
  scopes,
  redirect_uri,
  frontend_callback_url,
  attribute_mapping,
  extra_config,
  is_enabled,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM oauth_providers
ORDER BY provider_type ASC
"#;

const GET_OAUTH_PROVIDER_CONFIG_SQL: &str = r#"
SELECT
  provider_type,
  display_name,
  client_id,
  client_secret_encrypted,
  authorization_url_override,
  token_url_override,
  userinfo_url_override,
  scopes,
  redirect_uri,
  frontend_callback_url,
  attribute_mapping,
  extra_config,
  is_enabled,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM oauth_providers
WHERE provider_type = $1
LIMIT 1
"#;

const COUNT_LOCKED_USERS_IF_PROVIDER_DISABLED_SQL: &str = r#"
WITH affected_users AS (
  SELECT DISTINCT
    users.id,
    users.auth_source,
    users.role,
    (
      SELECT COUNT(*)
      FROM user_oauth_links other_links
      JOIN oauth_providers other_provider
        ON other_links.provider_type = other_provider.provider_type
      WHERE other_links.user_id = users.id
        AND other_links.provider_type <> $1
        AND other_provider.is_enabled IS TRUE
    ) AS other_enabled_count
  FROM users
  JOIN user_oauth_links
    ON users.id = user_oauth_links.user_id
  WHERE users.is_active IS TRUE
    AND users.is_deleted IS FALSE
    AND user_oauth_links.provider_type = $1
)
SELECT COUNT(*)::bigint AS locked_count
FROM affected_users
WHERE (
    auth_source = 'oauth'
    AND other_enabled_count = 0
  ) OR (
    $2::boolean IS TRUE
    AND auth_source = 'local'
    AND role <> 'admin'
    AND other_enabled_count = 0
  )
"#;

const UPSERT_OAUTH_PROVIDER_CONFIG_SQL: &str = r#"
INSERT INTO oauth_providers (
  provider_type,
  display_name,
  client_id,
  client_secret_encrypted,
  authorization_url_override,
  token_url_override,
  userinfo_url_override,
  scopes,
  redirect_uri,
  frontend_callback_url,
  attribute_mapping,
  extra_config,
  is_enabled,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  $3,
  CASE $4
    WHEN 'set' THEN $5
    WHEN 'clear' THEN NULL
    ELSE NULL
  END,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12,
  $13,
  $14,
  NOW(),
  NOW()
)
ON CONFLICT (provider_type) DO UPDATE
SET display_name = EXCLUDED.display_name,
    client_id = EXCLUDED.client_id,
    client_secret_encrypted = CASE $4
      WHEN 'set' THEN $5
      WHEN 'clear' THEN NULL
      ELSE oauth_providers.client_secret_encrypted
    END,
    authorization_url_override = EXCLUDED.authorization_url_override,
    token_url_override = EXCLUDED.token_url_override,
    userinfo_url_override = EXCLUDED.userinfo_url_override,
    scopes = EXCLUDED.scopes,
    redirect_uri = EXCLUDED.redirect_uri,
    frontend_callback_url = EXCLUDED.frontend_callback_url,
    attribute_mapping = EXCLUDED.attribute_mapping,
    extra_config = EXCLUDED.extra_config,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = NOW()
RETURNING
  provider_type,
  display_name,
  client_id,
  client_secret_encrypted,
  authorization_url_override,
  token_url_override,
  userinfo_url_override,
  scopes,
  redirect_uri,
  frontend_callback_url,
  attribute_mapping,
  extra_config,
  is_enabled,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_secs,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
"#;

const DELETE_OAUTH_PROVIDER_CONFIG_SQL: &str = r#"
DELETE FROM oauth_providers
WHERE provider_type = $1
"#;

#[derive(Debug, Clone)]
pub struct SqlxOAuthProviderRepository {
    pool: PgPool,
}

impl SqlxOAuthProviderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OAuthProviderReadRepository for SqlxOAuthProviderRepository {
    async fn list_oauth_provider_configs(
        &self,
    ) -> Result<Vec<StoredOAuthProviderConfig>, DataLayerError> {
        let rows = sqlx::query(LIST_OAUTH_PROVIDER_CONFIGS_SQL)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(map_oauth_provider_row).collect()
    }

    async fn get_oauth_provider_config(
        &self,
        provider_type: &str,
    ) -> Result<Option<StoredOAuthProviderConfig>, DataLayerError> {
        let row = sqlx::query(GET_OAUTH_PROVIDER_CONFIG_SQL)
            .bind(provider_type)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(map_oauth_provider_row).transpose()
    }

    async fn count_locked_users_if_provider_disabled(
        &self,
        provider_type: &str,
        ldap_exclusive: bool,
    ) -> Result<usize, DataLayerError> {
        let locked_count: i64 = sqlx::query_scalar(COUNT_LOCKED_USERS_IF_PROVIDER_DISABLED_SQL)
            .bind(provider_type)
            .bind(ldap_exclusive)
            .fetch_one(&self.pool)
            .await?;
        usize::try_from(locked_count).map_err(|_| {
            DataLayerError::UnexpectedValue(
                "oauth_providers.locked_user_count is negative".to_string(),
            )
        })
    }
}

#[async_trait]
impl OAuthProviderWriteRepository for SqlxOAuthProviderRepository {
    async fn upsert_oauth_provider_config(
        &self,
        record: &UpsertOAuthProviderConfigRecord,
    ) -> Result<StoredOAuthProviderConfig, DataLayerError> {
        record.validate()?;
        let row = sqlx::query(UPSERT_OAUTH_PROVIDER_CONFIG_SQL)
            .bind(&record.provider_type)
            .bind(&record.display_name)
            .bind(&record.client_id)
            .bind(record.client_secret_encrypted.mode_name())
            .bind(record.client_secret_encrypted.value())
            .bind(record.authorization_url_override.as_deref())
            .bind(record.token_url_override.as_deref())
            .bind(record.userinfo_url_override.as_deref())
            .bind(scopes_to_json(record.scopes.as_ref()))
            .bind(&record.redirect_uri)
            .bind(&record.frontend_callback_url)
            .bind(record.attribute_mapping.as_ref())
            .bind(record.extra_config.as_ref())
            .bind(record.is_enabled)
            .fetch_one(&self.pool)
            .await?;
        map_oauth_provider_row(&row)
    }

    async fn delete_oauth_provider_config(
        &self,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(DELETE_OAUTH_PROVIDER_CONFIG_SQL)
            .bind(provider_type)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

fn optional_unix_secs(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
}

fn scopes_to_json(scopes: Option<&Vec<String>>) -> Option<serde_json::Value> {
    scopes.map(|items| {
        serde_json::Value::Array(
            items
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        )
    })
}

fn parse_scopes(value: Option<serde_json::Value>) -> Result<Option<Vec<String>>, DataLayerError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let serde_json::Value::Array(items) = value else {
        return Err(DataLayerError::UnexpectedValue(
            "oauth_providers.scopes is not a JSON array".to_string(),
        ));
    };
    let mut scopes = Vec::with_capacity(items.len());
    for item in items {
        let serde_json::Value::String(scope) = item else {
            return Err(DataLayerError::UnexpectedValue(
                "oauth_providers.scopes contains non-string value".to_string(),
            ));
        };
        scopes.push(scope);
    }
    Ok(Some(scopes))
}

fn map_oauth_provider_row(row: &PgRow) -> Result<StoredOAuthProviderConfig, DataLayerError> {
    Ok(StoredOAuthProviderConfig::new(
        row.try_get("provider_type")?,
        row.try_get("display_name")?,
        row.try_get("client_id")?,
        row.try_get("redirect_uri")?,
        row.try_get("frontend_callback_url")?,
    )?
    .with_config_fields(
        row.try_get("client_secret_encrypted")?,
        row.try_get("authorization_url_override")?,
        row.try_get("token_url_override")?,
        row.try_get("userinfo_url_override")?,
        parse_scopes(row.try_get("scopes")?)?,
        row.try_get("attribute_mapping")?,
        row.try_get("extra_config")?,
        row.try_get("is_enabled")?,
    )
    .with_timestamps(
        optional_unix_secs(row.try_get("created_at_unix_secs")?),
        optional_unix_secs(row.try_get("updated_at_unix_secs")?),
    ))
}

#[cfg(test)]
mod tests {
    use super::SqlxOAuthProviderRepository;
    use crate::postgres::{PostgresPoolConfig, PostgresPoolFactory};

    #[tokio::test]
    async fn repository_constructs_from_lazy_pool() {
        let factory = PostgresPoolFactory::new(PostgresPoolConfig {
            database_url: "postgres://localhost/aether".to_string(),
            min_connections: 1,
            max_connections: 4,
            acquire_timeout_ms: 1_000,
            idle_timeout_ms: 5_000,
            max_lifetime_ms: 30_000,
            statement_cache_capacity: 64,
            require_ssl: false,
        })
        .expect("factory should build");

        let pool = factory.connect_lazy().expect("pool should build");
        let _repository = SqlxOAuthProviderRepository::new(pool);
    }
}
