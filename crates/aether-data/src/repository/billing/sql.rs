use async_trait::async_trait;
use sqlx::{PgPool, Row};

use super::types::{BillingReadRepository, StoredBillingModelContext};
use crate::DataLayerError;

const FIND_MODEL_CONTEXT_SQL: &str = r#"
SELECT
  p.id AS provider_id,
  CAST(p.billing_type AS TEXT) AS provider_billing_type,
  pak.id AS provider_api_key_id,
  pak.rate_multipliers AS provider_api_key_rate_multipliers,
  pak.cache_ttl_minutes AS provider_api_key_cache_ttl_minutes,
  gm.id AS global_model_id,
  gm.name AS global_model_name,
  gm.config AS global_model_config,
  CAST(gm.default_price_per_request AS DOUBLE PRECISION) AS default_price_per_request,
  gm.default_tiered_pricing AS default_tiered_pricing,
  m.id AS model_id,
  m.provider_model_name AS model_provider_model_name,
  m.config AS model_config,
  CAST(m.price_per_request AS DOUBLE PRECISION) AS model_price_per_request,
  m.tiered_pricing AS model_tiered_pricing
FROM providers p
INNER JOIN global_models gm
  ON gm.name = $2
 AND gm.is_active = TRUE
LEFT JOIN models m
  ON m.global_model_id = gm.id
 AND m.provider_id = p.id
 AND m.is_active = TRUE
LEFT JOIN provider_api_keys pak
  ON pak.id = $3
 AND pak.provider_id = p.id
WHERE p.id = $1
ORDER BY COALESCE(m.is_available, FALSE) DESC, m.created_at ASC
LIMIT 1
"#;

#[derive(Debug, Clone)]
pub struct SqlxBillingReadRepository {
    pool: PgPool,
}

impl SqlxBillingReadRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        let row = sqlx::query(FIND_MODEL_CONTEXT_SQL)
            .bind(provider_id)
            .bind(global_model_name)
            .bind(provider_api_key_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(map_row).transpose()
    }
}

#[async_trait]
impl BillingReadRepository for SqlxBillingReadRepository {
    async fn find_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        Self::find_model_context(self, provider_id, provider_api_key_id, global_model_name).await
    }
}

fn map_row(row: &sqlx::postgres::PgRow) -> Result<StoredBillingModelContext, DataLayerError> {
    StoredBillingModelContext::new(
        row.try_get("provider_id")?,
        row.try_get("provider_billing_type")?,
        row.try_get("provider_api_key_id")?,
        row.try_get("provider_api_key_rate_multipliers")?,
        row.try_get::<Option<i32>, _>("provider_api_key_cache_ttl_minutes")?
            .map(i64::from),
        row.try_get("global_model_id")?,
        row.try_get("global_model_name")?,
        row.try_get("global_model_config")?,
        row.try_get("default_price_per_request")?,
        row.try_get("default_tiered_pricing")?,
        row.try_get("model_id")?,
        row.try_get("model_provider_model_name")?,
        row.try_get("model_config")?,
        row.try_get("model_price_per_request")?,
        row.try_get("model_tiered_pricing")?,
    )
}

#[cfg(test)]
mod tests {
    use super::SqlxBillingReadRepository;
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
        let _repository = SqlxBillingReadRepository::new(pool);
    }
}
