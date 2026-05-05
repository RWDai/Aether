use async_trait::async_trait;
use sqlx::{PgPool, Row};

use super::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingMutationOutcome,
    AdminBillingPresetApplyResult, AdminBillingRuleRecord, AdminBillingRuleWriteInput,
    BillingReadRepository, StoredBillingModelContext,
};
use crate::{error::SqlxResultExt, DataLayerError};

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
  ON gm.is_active = TRUE
LEFT JOIN models m
  ON m.global_model_id = gm.id
 AND m.provider_id = p.id
 AND m.is_active = TRUE
LEFT JOIN provider_api_keys pak
  ON pak.id = $3
 AND pak.provider_id = p.id
WHERE p.id = $1
  AND (
    gm.name = $2
    OR m.provider_model_name = $2
    OR (
      m.provider_model_mappings IS NOT NULL
      AND (
        m.provider_model_mappings @> jsonb_build_array(jsonb_build_object('name', $2::TEXT))
        OR m.provider_model_mappings @> jsonb_build_array(to_jsonb($2::TEXT))
        OR m.provider_model_mappings @> jsonb_build_object('name', $2::TEXT)
        OR m.provider_model_mappings = to_jsonb($2::TEXT)
      )
    )
  )
ORDER BY
  CASE
    WHEN m.provider_model_name = $2 THEN 0
    WHEN m.provider_model_mappings IS NOT NULL
      AND (
        m.provider_model_mappings @> jsonb_build_array(jsonb_build_object('name', $2::TEXT))
        OR m.provider_model_mappings @> jsonb_build_array(to_jsonb($2::TEXT))
        OR m.provider_model_mappings @> jsonb_build_object('name', $2::TEXT)
        OR m.provider_model_mappings = to_jsonb($2::TEXT)
      ) THEN 1
    WHEN gm.name = $2 THEN 2
    ELSE 3
  END ASC,
  COALESCE(m.is_available, FALSE) DESC,
  CASE
    WHEN m.tiered_pricing IS NOT NULL OR m.price_per_request IS NOT NULL THEN 0
    WHEN gm.default_tiered_pricing IS NOT NULL OR gm.default_price_per_request IS NOT NULL THEN 1
    ELSE 2
  END ASC,
  m.created_at ASC
LIMIT 1
"#;

const FIND_MODEL_CONTEXT_BY_MODEL_ID_SQL: &str = r#"
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
INNER JOIN models m
  ON m.id = $2
 AND m.provider_id = p.id
 AND m.is_active = TRUE
INNER JOIN global_models gm
  ON gm.id = m.global_model_id
 AND gm.is_active = TRUE
LEFT JOIN provider_api_keys pak
  ON pak.id = $3
 AND pak.provider_id = p.id
WHERE p.id = $1
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
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_row).transpose()
    }

    pub async fn find_model_context_by_model_id(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        model_id: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        let row = sqlx::query(FIND_MODEL_CONTEXT_BY_MODEL_ID_SQL)
            .bind(provider_id)
            .bind(model_id)
            .bind(provider_api_key_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
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

    async fn find_model_context_by_model_id(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        model_id: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        Self::find_model_context_by_model_id(self, provider_id, provider_api_key_id, model_id).await
    }

    async fn admin_billing_enabled_default_value_exists(
        &self,
        api_format: &str,
        task_type: &str,
        dimension_name: &str,
        existing_id: Option<&str>,
    ) -> Result<Option<bool>, DataLayerError> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
SELECT EXISTS(
  SELECT 1
  FROM dimension_collectors
  WHERE api_format = $1
    AND task_type = $2
    AND dimension_name = $3
    AND is_enabled = TRUE
    AND default_value IS NOT NULL
    AND ($4::TEXT IS NULL OR id <> $4)
)
            "#,
        )
        .bind(api_format)
        .bind(task_type)
        .bind(dimension_name)
        .bind(existing_id)
        .fetch_one(&self.pool)
        .await
        .map_postgres_err()?;
        Ok(Some(exists))
    }

    async fn create_admin_billing_rule(
        &self,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingRuleRecord>, DataLayerError> {
        let rule_id = uuid::Uuid::new_v4().to_string();
        let row = match sqlx::query(
            r#"
INSERT INTO billing_rules (
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
RETURNING
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(&rule_id)
        .bind(&input.name)
        .bind(&input.task_type)
        .bind(input.global_model_id.as_deref())
        .bind(input.model_id.as_deref())
        .bind(&input.expression)
        .bind(&input.variables)
        .bind(&input.dimension_mappings)
        .bind(input.is_enabled)
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(DataLayerError::postgres(err)),
        };
        Ok(AdminBillingMutationOutcome::Applied(
            map_admin_billing_rule_row(&row)?,
        ))
    }

    async fn list_admin_billing_rules(
        &self,
        task_type: Option<&str>,
        is_enabled: Option<bool>,
        page: u32,
        page_size: u32,
    ) -> Result<Option<(Vec<AdminBillingRuleRecord>, u64)>, DataLayerError> {
        let total = read_count(
            sqlx::query(
                r#"
SELECT COUNT(*) AS total
FROM billing_rules
WHERE ($1::TEXT IS NULL OR task_type = $1)
  AND ($2::BOOL IS NULL OR is_enabled = $2)
                "#,
            )
            .bind(task_type)
            .bind(is_enabled)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?,
        )?;
        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM billing_rules
WHERE ($1::TEXT IS NULL OR task_type = $1)
  AND ($2::BOOL IS NULL OR is_enabled = $2)
ORDER BY updated_at DESC
OFFSET $3
LIMIT $4
            "#,
        )
        .bind(task_type)
        .bind(is_enabled)
        .bind(
            i64::try_from(offset)
                .map_err(|err| DataLayerError::UnexpectedValue(err.to_string()))?,
        )
        .bind(i64::from(page_size))
        .fetch_all(&self.pool)
        .await
        .map_postgres_err()?;
        let items = rows
            .iter()
            .map(map_admin_billing_rule_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_rule(
        &self,
        rule_id: &str,
    ) -> Result<Option<AdminBillingRuleRecord>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM billing_rules
WHERE id = $1
            "#,
        )
        .bind(rule_id)
        .fetch_optional(&self.pool)
        .await
        .map_postgres_err()?;
        row.as_ref().map(map_admin_billing_rule_row).transpose()
    }

    async fn update_admin_billing_rule(
        &self,
        rule_id: &str,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingRuleRecord>, DataLayerError> {
        let row = match sqlx::query(
            r#"
UPDATE billing_rules
SET
  name = $2,
  task_type = $3,
  global_model_id = $4,
  model_id = $5,
  expression = $6,
  variables = $7,
  dimension_mappings = $8,
  is_enabled = $9,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(rule_id)
        .bind(&input.name)
        .bind(&input.task_type)
        .bind(input.global_model_id.as_deref())
        .bind(input.model_id.as_deref())
        .bind(&input.expression)
        .bind(&input.variables)
        .bind(&input.dimension_mappings)
        .bind(input.is_enabled)
        .fetch_optional(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(DataLayerError::postgres(err)),
        };
        match row {
            Some(row) => Ok(AdminBillingMutationOutcome::Applied(
                map_admin_billing_rule_row(&row)?,
            )),
            None => Ok(AdminBillingMutationOutcome::NotFound),
        }
    }

    async fn create_admin_billing_collector(
        &self,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingCollectorRecord>, DataLayerError> {
        let collector_id = uuid::Uuid::new_v4().to_string();
        let row = match sqlx::query(
            r#"
INSERT INTO dimension_collectors (
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())
RETURNING
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(&collector_id)
        .bind(&input.api_format)
        .bind(&input.task_type)
        .bind(&input.dimension_name)
        .bind(&input.source_type)
        .bind(input.source_path.as_deref())
        .bind(&input.value_type)
        .bind(input.transform_expression.as_deref())
        .bind(input.default_value.as_deref())
        .bind(input.priority)
        .bind(input.is_enabled)
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(DataLayerError::postgres(err)),
        };
        Ok(AdminBillingMutationOutcome::Applied(
            map_admin_billing_collector_row(&row)?,
        ))
    }

    async fn list_admin_billing_collectors(
        &self,
        api_format: Option<&str>,
        task_type: Option<&str>,
        dimension_name: Option<&str>,
        is_enabled: Option<bool>,
        page: u32,
        page_size: u32,
    ) -> Result<Option<(Vec<AdminBillingCollectorRecord>, u64)>, DataLayerError> {
        let total = read_count(
            sqlx::query(
                r#"
SELECT COUNT(*) AS total
FROM dimension_collectors
WHERE ($1::TEXT IS NULL OR api_format = $1)
  AND ($2::TEXT IS NULL OR task_type = $2)
  AND ($3::TEXT IS NULL OR dimension_name = $3)
  AND ($4::BOOL IS NULL OR is_enabled = $4)
                "#,
            )
            .bind(api_format)
            .bind(task_type)
            .bind(dimension_name)
            .bind(is_enabled)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?,
        )?;
        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM dimension_collectors
WHERE ($1::TEXT IS NULL OR api_format = $1)
  AND ($2::TEXT IS NULL OR task_type = $2)
  AND ($3::TEXT IS NULL OR dimension_name = $3)
  AND ($4::BOOL IS NULL OR is_enabled = $4)
ORDER BY updated_at DESC, priority DESC, id ASC
OFFSET $5
LIMIT $6
            "#,
        )
        .bind(api_format)
        .bind(task_type)
        .bind(dimension_name)
        .bind(is_enabled)
        .bind(
            i64::try_from(offset)
                .map_err(|err| DataLayerError::UnexpectedValue(err.to_string()))?,
        )
        .bind(i64::from(page_size))
        .fetch_all(&self.pool)
        .await
        .map_postgres_err()?;
        let items = rows
            .iter()
            .map(map_admin_billing_collector_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_collector(
        &self,
        collector_id: &str,
    ) -> Result<Option<AdminBillingCollectorRecord>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM dimension_collectors
WHERE id = $1
            "#,
        )
        .bind(collector_id)
        .fetch_optional(&self.pool)
        .await
        .map_postgres_err()?;
        row.as_ref()
            .map(map_admin_billing_collector_row)
            .transpose()
    }

    async fn update_admin_billing_collector(
        &self,
        collector_id: &str,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingCollectorRecord>, DataLayerError> {
        let row = match sqlx::query(
            r#"
UPDATE dimension_collectors
SET
  api_format = $2,
  task_type = $3,
  dimension_name = $4,
  source_type = $5,
  source_path = $6,
  value_type = $7,
  transform_expression = $8,
  default_value = $9,
  priority = $10,
  is_enabled = $11,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_ms,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(collector_id)
        .bind(&input.api_format)
        .bind(&input.task_type)
        .bind(&input.dimension_name)
        .bind(&input.source_type)
        .bind(input.source_path.as_deref())
        .bind(&input.value_type)
        .bind(input.transform_expression.as_deref())
        .bind(input.default_value.as_deref())
        .bind(input.priority)
        .bind(input.is_enabled)
        .fetch_optional(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(DataLayerError::postgres(err)),
        };
        match row {
            Some(row) => Ok(AdminBillingMutationOutcome::Applied(
                map_admin_billing_collector_row(&row)?,
            )),
            None => Ok(AdminBillingMutationOutcome::NotFound),
        }
    }

    async fn apply_admin_billing_preset(
        &self,
        preset: &str,
        mode: &str,
        collectors: &[AdminBillingCollectorWriteInput],
    ) -> Result<AdminBillingMutationOutcome<AdminBillingPresetApplyResult>, DataLayerError> {
        let mut created = 0_u64;
        let mut updated = 0_u64;
        let mut skipped = 0_u64;
        let mut errors = Vec::new();

        for collector in collectors {
            let existing_id = match sqlx::query_scalar::<_, String>(
                r#"
SELECT id
FROM dimension_collectors
WHERE api_format = $1
  AND task_type = $2
  AND dimension_name = $3
  AND priority = $4
  AND is_enabled = TRUE
LIMIT 1
                "#,
            )
            .bind(&collector.api_format)
            .bind(&collector.task_type)
            .bind(&collector.dimension_name)
            .bind(collector.priority)
            .fetch_optional(&self.pool)
            .await
            {
                Ok(value) => value,
                Err(err) => {
                    errors.push(format!(
                        "Failed to query collector: api_format={} task_type={} dim={}: {}",
                        collector.api_format, collector.task_type, collector.dimension_name, err
                    ));
                    continue;
                }
            };

            if let Some(existing_id) = existing_id {
                if mode == "overwrite" {
                    match sqlx::query(
                        r#"
UPDATE dimension_collectors
SET
  source_type = $2,
  source_path = $3,
  value_type = $4,
  transform_expression = $5,
  default_value = $6,
  is_enabled = $7,
  updated_at = NOW()
WHERE id = $1
                        "#,
                    )
                    .bind(&existing_id)
                    .bind(&collector.source_type)
                    .bind(collector.source_path.as_deref())
                    .bind(&collector.value_type)
                    .bind(collector.transform_expression.as_deref())
                    .bind(collector.default_value.as_deref())
                    .bind(collector.is_enabled)
                    .execute(&self.pool)
                    .await
                    {
                        Ok(_) => updated += 1,
                        Err(err) => errors.push(format!(
                            "Failed to update collector {}: {}",
                            existing_id, err
                        )),
                    }
                } else {
                    skipped += 1;
                }
                continue;
            }

            match sqlx::query(
                r#"
INSERT INTO dimension_collectors (
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())
                "#,
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&collector.api_format)
            .bind(&collector.task_type)
            .bind(&collector.dimension_name)
            .bind(&collector.source_type)
            .bind(collector.source_path.as_deref())
            .bind(&collector.value_type)
            .bind(collector.transform_expression.as_deref())
            .bind(collector.default_value.as_deref())
            .bind(collector.priority)
            .bind(collector.is_enabled)
            .execute(&self.pool)
            .await
            {
                Ok(_) => created += 1,
                Err(err) => errors.push(format!(
                    "Failed to create collector: api_format={} task_type={} dim={}: {}",
                    collector.api_format, collector.task_type, collector.dimension_name, err
                )),
            }
        }

        Ok(AdminBillingMutationOutcome::Applied(
            AdminBillingPresetApplyResult {
                preset: preset.to_string(),
                mode: mode.to_string(),
                created,
                updated,
                skipped,
                errors,
            },
        ))
    }
}

fn map_row(row: &sqlx::postgres::PgRow) -> Result<StoredBillingModelContext, DataLayerError> {
    StoredBillingModelContext::new(
        row.try_get("provider_id").map_postgres_err()?,
        row.try_get("provider_billing_type").map_postgres_err()?,
        row.try_get("provider_api_key_id").map_postgres_err()?,
        row.try_get("provider_api_key_rate_multipliers")
            .map_postgres_err()?,
        row.try_get::<Option<i32>, _>("provider_api_key_cache_ttl_minutes")
            .map_postgres_err()?
            .map(i64::from),
        row.try_get("global_model_id").map_postgres_err()?,
        row.try_get("global_model_name").map_postgres_err()?,
        row.try_get("global_model_config").map_postgres_err()?,
        row.try_get("default_price_per_request")
            .map_postgres_err()?,
        row.try_get("default_tiered_pricing").map_postgres_err()?,
        row.try_get("model_id").map_postgres_err()?,
        row.try_get("model_provider_model_name")
            .map_postgres_err()?,
        row.try_get("model_config").map_postgres_err()?,
        row.try_get("model_price_per_request").map_postgres_err()?,
        row.try_get("model_tiered_pricing").map_postgres_err()?,
    )
}

fn read_count(row: sqlx::postgres::PgRow) -> Result<u64, DataLayerError> {
    Ok(row.try_get::<i64, _>("total").map_postgres_err()?.max(0) as u64)
}

fn map_admin_billing_rule_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminBillingRuleRecord, DataLayerError> {
    Ok(AdminBillingRuleRecord {
        id: row.try_get("id").map_postgres_err()?,
        name: row.try_get("name").map_postgres_err()?,
        task_type: row.try_get("task_type").map_postgres_err()?,
        global_model_id: row.try_get("global_model_id").map_postgres_err()?,
        model_id: row.try_get("model_id").map_postgres_err()?,
        expression: row.try_get("expression").map_postgres_err()?,
        variables: row
            .try_get::<Option<serde_json::Value>, _>("variables")
            .map_postgres_err()?
            .unwrap_or_else(|| serde_json::json!({})),
        dimension_mappings: row
            .try_get::<Option<serde_json::Value>, _>("dimension_mappings")
            .map_postgres_err()?
            .unwrap_or_else(|| serde_json::json!({})),
        is_enabled: row.try_get("is_enabled").map_postgres_err()?,
        created_at_unix_ms: row
            .try_get::<i64, _>("created_at_unix_ms")
            .map_postgres_err()?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_postgres_err()?
            .max(0) as u64,
    })
}

fn map_admin_billing_collector_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminBillingCollectorRecord, DataLayerError> {
    Ok(AdminBillingCollectorRecord {
        id: row.try_get("id").map_postgres_err()?,
        api_format: row.try_get("api_format").map_postgres_err()?,
        task_type: row.try_get("task_type").map_postgres_err()?,
        dimension_name: row.try_get("dimension_name").map_postgres_err()?,
        source_type: row.try_get("source_type").map_postgres_err()?,
        source_path: row.try_get("source_path").map_postgres_err()?,
        value_type: row.try_get("value_type").map_postgres_err()?,
        transform_expression: row.try_get("transform_expression").map_postgres_err()?,
        default_value: row.try_get("default_value").map_postgres_err()?,
        priority: row.try_get("priority").map_postgres_err()?,
        is_enabled: row.try_get("is_enabled").map_postgres_err()?,
        created_at_unix_ms: row
            .try_get::<i64, _>("created_at_unix_ms")
            .map_postgres_err()?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_postgres_err()?
            .max(0) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::SqlxBillingReadRepository;
    use crate::driver::postgres::{PostgresPoolConfig, PostgresPoolFactory};

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
