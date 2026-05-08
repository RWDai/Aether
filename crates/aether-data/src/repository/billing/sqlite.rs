use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

use super::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingMutationOutcome,
    AdminBillingPresetApplyResult, AdminBillingRuleRecord, AdminBillingRuleWriteInput,
    BillingReadRepository, StoredBillingModelContext,
};
use crate::driver::sqlite::{sqlite_optional_real, SqlitePool};
use crate::error::SqlResultExt;
use crate::DataLayerError;

const MODEL_CONTEXT_COLUMNS: &str = r#"
SELECT
  p.id AS provider_id,
  p.billing_type AS provider_billing_type,
  pak.id AS provider_api_key_id,
  pak.rate_multipliers AS provider_api_key_rate_multipliers,
  pak.cache_ttl_minutes AS provider_api_key_cache_ttl_minutes,
  gm.id AS global_model_id,
  gm.name AS global_model_name,
  gm.config AS global_model_config,
  CAST(gm.default_price_per_request AS REAL) AS default_price_per_request,
  gm.default_tiered_pricing AS default_tiered_pricing,
  m.id AS model_id,
  m.provider_model_name AS model_provider_model_name,
  m.config AS model_config,
  CAST(m.price_per_request AS REAL) AS model_price_per_request,
  m.tiered_pricing AS model_tiered_pricing,
  m.provider_model_mappings AS provider_model_mappings,
  m.is_available AS model_is_available,
  m.created_at AS model_created_at
FROM providers p
"#;

#[derive(Debug, Clone)]
pub struct SqliteBillingReadRepository {
    pool: SqlitePool,
}

impl SqliteBillingReadRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BillingReadRepository for SqliteBillingReadRepository {
    async fn find_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        let rows = sqlx::query(&format!(
            r#"
{MODEL_CONTEXT_COLUMNS}
INNER JOIN global_models gm
  ON gm.is_active = 1
LEFT JOIN models m
  ON m.global_model_id = gm.id
 AND m.provider_id = p.id
 AND m.is_active = 1
LEFT JOIN provider_api_keys pak
  ON pak.id = ?
 AND pak.provider_id = p.id
WHERE p.id = ?
  AND (
    gm.name = ?
    OR m.provider_model_name = ?
    OR m.provider_model_mappings IS NOT NULL
  )
"#
        ))
        .bind(provider_api_key_id)
        .bind(provider_id)
        .bind(global_model_name)
        .bind(global_model_name)
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        rows.iter()
            .filter_map(|row| match_rank(row, global_model_name).transpose())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min_by_key(|candidate| {
                (
                    candidate.rank,
                    !candidate.is_available,
                    candidate.pricing_rank,
                    candidate.created_at,
                )
            })
            .map(|candidate| candidate.context)
            .transpose()
    }

    async fn find_model_context_by_model_id(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        model_id: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        let row = sqlx::query(&format!(
            r#"
{MODEL_CONTEXT_COLUMNS}
INNER JOIN models m
  ON m.id = ?
 AND m.provider_id = p.id
 AND m.is_active = 1
INNER JOIN global_models gm
  ON gm.id = m.global_model_id
 AND gm.is_active = 1
LEFT JOIN provider_api_keys pak
  ON pak.id = ?
 AND pak.provider_id = p.id
WHERE p.id = ?
LIMIT 1
"#
        ))
        .bind(model_id)
        .bind(provider_api_key_id)
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_sql_err()?;
        row.as_ref().map(map_row).transpose()
    }

    async fn admin_billing_enabled_default_value_exists(
        &self,
        api_format: &str,
        task_type: &str,
        dimension_name: &str,
        existing_id: Option<&str>,
    ) -> Result<Option<bool>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM dimension_collectors
WHERE api_format = ?
  AND task_type = ?
  AND dimension_name = ?
  AND is_enabled = 1
  AND default_value IS NOT NULL
  AND (? IS NULL OR id <> ?)
            "#,
        )
        .bind(api_format)
        .bind(task_type)
        .bind(dimension_name)
        .bind(existing_id)
        .bind(existing_id)
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;
        Ok(Some(read_count_sqlite(&row)? > 0))
    }

    async fn create_admin_billing_rule(
        &self,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingRuleRecord>, DataLayerError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = current_unix_secs_i64();
        let result = sqlx::query(
            r#"
INSERT INTO billing_rules (
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled, created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(&input.name)
        .bind(&input.task_type)
        .bind(input.global_model_id.as_deref())
        .bind(input.model_id.as_deref())
        .bind(&input.expression)
        .bind(json_to_string(&input.variables)?)
        .bind(json_to_string(&input.dimension_mappings)?)
        .bind(input.is_enabled)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await;
        if let Err(err) = result {
            return Ok(AdminBillingMutationOutcome::Invalid(format!(
                "Integrity error: {err}"
            )));
        }
        match find_admin_billing_rule_sqlite(&self.pool, &id).await? {
            Some(record) => Ok(AdminBillingMutationOutcome::Applied(record)),
            None => Err(DataLayerError::UnexpectedValue(
                "created billing rule missing".to_string(),
            )),
        }
    }

    async fn list_admin_billing_rules(
        &self,
        task_type: Option<&str>,
        is_enabled: Option<bool>,
        page: u32,
        page_size: u32,
    ) -> Result<Option<(Vec<AdminBillingRuleRecord>, u64)>, DataLayerError> {
        let total_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM billing_rules
WHERE (? IS NULL OR task_type = ?)
  AND (? IS NULL OR is_enabled = ?)
            "#,
        )
        .bind(task_type)
        .bind(task_type)
        .bind(is_enabled)
        .bind(is_enabled)
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;
        let total = read_count_sqlite(&total_row)?;
        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled, created_at AS created_at_unix_ms,
  updated_at AS updated_at_unix_secs
FROM billing_rules
WHERE (? IS NULL OR task_type = ?)
  AND (? IS NULL OR is_enabled = ?)
ORDER BY updated_at DESC, id DESC
LIMIT ? OFFSET ?
            "#,
        )
        .bind(task_type)
        .bind(task_type)
        .bind(is_enabled)
        .bind(is_enabled)
        .bind(i64::from(page_size))
        .bind(
            i64::try_from(offset)
                .map_err(|err| DataLayerError::UnexpectedValue(err.to_string()))?,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        let items = rows
            .iter()
            .map(map_admin_billing_rule_sqlite)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_rule(
        &self,
        rule_id: &str,
    ) -> Result<Option<AdminBillingRuleRecord>, DataLayerError> {
        find_admin_billing_rule_sqlite(&self.pool, rule_id).await
    }

    async fn update_admin_billing_rule(
        &self,
        rule_id: &str,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingRuleRecord>, DataLayerError> {
        let result = sqlx::query(
            r#"
UPDATE billing_rules
SET name = ?,
    task_type = ?,
    global_model_id = ?,
    model_id = ?,
    expression = ?,
    variables = ?,
    dimension_mappings = ?,
    is_enabled = ?,
    updated_at = ?
WHERE id = ?
            "#,
        )
        .bind(&input.name)
        .bind(&input.task_type)
        .bind(input.global_model_id.as_deref())
        .bind(input.model_id.as_deref())
        .bind(&input.expression)
        .bind(json_to_string(&input.variables)?)
        .bind(json_to_string(&input.dimension_mappings)?)
        .bind(input.is_enabled)
        .bind(current_unix_secs_i64())
        .bind(rule_id)
        .execute(&self.pool)
        .await;
        let affected = match result {
            Ok(result) => result.rows_affected(),
            Err(err) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
        };
        if affected == 0 {
            return Ok(AdminBillingMutationOutcome::NotFound);
        }
        match find_admin_billing_rule_sqlite(&self.pool, rule_id).await? {
            Some(record) => Ok(AdminBillingMutationOutcome::Applied(record)),
            None => Ok(AdminBillingMutationOutcome::NotFound),
        }
    }

    async fn create_admin_billing_collector(
        &self,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingCollectorRecord>, DataLayerError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = current_unix_secs_i64();
        let result = sqlx::query(
            r#"
INSERT INTO dimension_collectors (
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled, created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
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
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await;
        if let Err(err) = result {
            return Ok(AdminBillingMutationOutcome::Invalid(format!(
                "Integrity error: {err}"
            )));
        }
        match find_admin_billing_collector_sqlite(&self.pool, &id).await? {
            Some(record) => Ok(AdminBillingMutationOutcome::Applied(record)),
            None => Err(DataLayerError::UnexpectedValue(
                "created billing collector missing".to_string(),
            )),
        }
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
        let total_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM dimension_collectors
WHERE (? IS NULL OR api_format = ?)
  AND (? IS NULL OR task_type = ?)
  AND (? IS NULL OR dimension_name = ?)
  AND (? IS NULL OR is_enabled = ?)
            "#,
        )
        .bind(api_format)
        .bind(api_format)
        .bind(task_type)
        .bind(task_type)
        .bind(dimension_name)
        .bind(dimension_name)
        .bind(is_enabled)
        .bind(is_enabled)
        .fetch_one(&self.pool)
        .await
        .map_sql_err()?;
        let total = read_count_sqlite(&total_row)?;
        let offset = u64::from(page.saturating_sub(1) * page_size);
        let rows = sqlx::query(
            r#"
SELECT
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  created_at AS created_at_unix_ms, updated_at AS updated_at_unix_secs
FROM dimension_collectors
WHERE (? IS NULL OR api_format = ?)
  AND (? IS NULL OR task_type = ?)
  AND (? IS NULL OR dimension_name = ?)
  AND (? IS NULL OR is_enabled = ?)
ORDER BY updated_at DESC, priority DESC, id ASC
LIMIT ? OFFSET ?
            "#,
        )
        .bind(api_format)
        .bind(api_format)
        .bind(task_type)
        .bind(task_type)
        .bind(dimension_name)
        .bind(dimension_name)
        .bind(is_enabled)
        .bind(is_enabled)
        .bind(i64::from(page_size))
        .bind(
            i64::try_from(offset)
                .map_err(|err| DataLayerError::UnexpectedValue(err.to_string()))?,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        let items = rows
            .iter()
            .map(map_admin_billing_collector_sqlite)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_collector(
        &self,
        collector_id: &str,
    ) -> Result<Option<AdminBillingCollectorRecord>, DataLayerError> {
        find_admin_billing_collector_sqlite(&self.pool, collector_id).await
    }

    async fn update_admin_billing_collector(
        &self,
        collector_id: &str,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<AdminBillingMutationOutcome<AdminBillingCollectorRecord>, DataLayerError> {
        let result = sqlx::query(
            r#"
UPDATE dimension_collectors
SET api_format = ?,
    task_type = ?,
    dimension_name = ?,
    source_type = ?,
    source_path = ?,
    value_type = ?,
    transform_expression = ?,
    default_value = ?,
    priority = ?,
    is_enabled = ?,
    updated_at = ?
WHERE id = ?
            "#,
        )
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
        .bind(current_unix_secs_i64())
        .bind(collector_id)
        .execute(&self.pool)
        .await;
        let affected = match result {
            Ok(result) => result.rows_affected(),
            Err(err) => {
                return Ok(AdminBillingMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
        };
        if affected == 0 {
            return Ok(AdminBillingMutationOutcome::NotFound);
        }
        match find_admin_billing_collector_sqlite(&self.pool, collector_id).await? {
            Some(record) => Ok(AdminBillingMutationOutcome::Applied(record)),
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
WHERE api_format = ?
  AND task_type = ?
  AND dimension_name = ?
  AND priority = ?
  AND is_enabled = 1
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
SET source_type = ?,
    source_path = ?,
    value_type = ?,
    transform_expression = ?,
    default_value = ?,
    is_enabled = ?,
    updated_at = ?
WHERE id = ?
                        "#,
                    )
                    .bind(&collector.source_type)
                    .bind(collector.source_path.as_deref())
                    .bind(&collector.value_type)
                    .bind(collector.transform_expression.as_deref())
                    .bind(collector.default_value.as_deref())
                    .bind(collector.is_enabled)
                    .bind(current_unix_secs_i64())
                    .bind(&existing_id)
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

            let id = uuid::Uuid::new_v4().to_string();
            let now = current_unix_secs_i64();
            match sqlx::query(
                r#"
INSERT INTO dimension_collectors (
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled, created_at, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(id)
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
            .bind(now)
            .bind(now)
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

struct RankedContext {
    rank: u8,
    is_available: bool,
    pricing_rank: u8,
    created_at: i64,
    context: Result<StoredBillingModelContext, DataLayerError>,
}

fn match_rank(
    row: &SqliteRow,
    requested_model: &str,
) -> Result<Option<RankedContext>, DataLayerError> {
    let provider_model_name: Option<String> =
        row.try_get("model_provider_model_name").map_sql_err()?;
    let global_model_name: String = row.try_get("global_model_name").map_sql_err()?;
    let mappings: Option<String> = row.try_get("provider_model_mappings").ok().flatten();

    let rank = if provider_model_name.as_deref() == Some(requested_model) {
        0
    } else if mappings
        .as_deref()
        .is_some_and(|mappings| provider_model_mappings_match(mappings, requested_model))
    {
        1
    } else if global_model_name == requested_model {
        2
    } else {
        return Ok(None);
    };

    let has_model_price = sqlite_optional_real(row, "model_price_per_request")?.is_some()
        || row
            .try_get::<Option<String>, _>("model_tiered_pricing")
            .ok()
            .flatten()
            .is_some();
    let has_default_price = sqlite_optional_real(row, "default_price_per_request")?.is_some()
        || row
            .try_get::<Option<String>, _>("default_tiered_pricing")
            .ok()
            .flatten()
            .is_some();
    let pricing_rank = if has_model_price {
        0
    } else if has_default_price {
        1
    } else {
        2
    };

    Ok(Some(RankedContext {
        rank,
        is_available: row
            .try_get::<Option<bool>, _>("model_is_available")
            .map_sql_err()?
            .unwrap_or(false),
        pricing_rank,
        created_at: row
            .try_get::<Option<i64>, _>("model_created_at")
            .map_sql_err()?
            .unwrap_or(i64::MAX),
        context: map_row(row),
    }))
}

fn provider_model_mappings_match(raw: &str, requested_model: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return raw == requested_model;
    };
    json_mapping_matches(&value, requested_model)
}

fn json_mapping_matches(value: &serde_json::Value, requested_model: &str) -> bool {
    match value {
        serde_json::Value::String(value) => value == requested_model,
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_mapping_matches(value, requested_model)),
        serde_json::Value::Object(map) => map
            .get("name")
            .is_some_and(|value| json_mapping_matches(value, requested_model)),
        _ => false,
    }
}

fn map_row(row: &SqliteRow) -> Result<StoredBillingModelContext, DataLayerError> {
    StoredBillingModelContext::new(
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("provider_billing_type").map_sql_err()?,
        row.try_get("provider_api_key_id").map_sql_err()?,
        parse_json(
            row.try_get("provider_api_key_rate_multipliers")
                .ok()
                .flatten(),
        )?,
        row.try_get::<Option<i64>, _>("provider_api_key_cache_ttl_minutes")
            .map_sql_err()?,
        row.try_get("global_model_id").map_sql_err()?,
        row.try_get("global_model_name").map_sql_err()?,
        parse_json(row.try_get("global_model_config").ok().flatten())?,
        sqlite_optional_real(row, "default_price_per_request")?,
        parse_json(row.try_get("default_tiered_pricing").ok().flatten())?,
        row.try_get("model_id").map_sql_err()?,
        row.try_get("model_provider_model_name").map_sql_err()?,
        parse_json(row.try_get("model_config").ok().flatten())?,
        sqlite_optional_real(row, "model_price_per_request")?,
        parse_json(row.try_get("model_tiered_pricing").ok().flatten())?,
    )
}

fn parse_json(value: Option<String>) -> Result<Option<serde_json::Value>, DataLayerError> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            serde_json::from_str(&value).map_err(|err| {
                DataLayerError::UnexpectedValue(format!("billing JSON field is invalid: {err}"))
            })
        })
        .transpose()
}

fn current_unix_secs_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn json_to_string(value: &serde_json::Value) -> Result<String, DataLayerError> {
    serde_json::to_string(value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("billing JSON encode failed: {err}"))
    })
}

fn read_count_sqlite(row: &SqliteRow) -> Result<u64, DataLayerError> {
    Ok(row.try_get::<i64, _>("total").map_sql_err()?.max(0) as u64)
}

async fn find_admin_billing_rule_sqlite(
    pool: &SqlitePool,
    rule_id: &str,
) -> Result<Option<AdminBillingRuleRecord>, DataLayerError> {
    let row = sqlx::query(
        r#"
SELECT
  id, name, task_type, global_model_id, model_id, expression, variables,
  dimension_mappings, is_enabled, created_at AS created_at_unix_ms,
  updated_at AS updated_at_unix_secs
FROM billing_rules
WHERE id = ?
        "#,
    )
    .bind(rule_id)
    .fetch_optional(pool)
    .await
    .map_sql_err()?;
    row.as_ref().map(map_admin_billing_rule_sqlite).transpose()
}

fn map_admin_billing_rule_sqlite(
    row: &SqliteRow,
) -> Result<AdminBillingRuleRecord, DataLayerError> {
    Ok(AdminBillingRuleRecord {
        id: row.try_get("id").map_sql_err()?,
        name: row.try_get("name").map_sql_err()?,
        task_type: row.try_get("task_type").map_sql_err()?,
        global_model_id: row.try_get("global_model_id").map_sql_err()?,
        model_id: row.try_get("model_id").map_sql_err()?,
        expression: row.try_get("expression").map_sql_err()?,
        variables: parse_required_json(row.try_get("variables").map_sql_err()?)?,
        dimension_mappings: parse_required_json(row.try_get("dimension_mappings").map_sql_err()?)?,
        is_enabled: row.try_get("is_enabled").map_sql_err()?,
        created_at_unix_ms: row
            .try_get::<i64, _>("created_at_unix_ms")
            .map_sql_err()?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_sql_err()?
            .max(0) as u64,
    })
}

async fn find_admin_billing_collector_sqlite(
    pool: &SqlitePool,
    collector_id: &str,
) -> Result<Option<AdminBillingCollectorRecord>, DataLayerError> {
    let row = sqlx::query(
        r#"
SELECT
  id, api_format, task_type, dimension_name, source_type, source_path, value_type,
  transform_expression, default_value, priority, is_enabled,
  created_at AS created_at_unix_ms, updated_at AS updated_at_unix_secs
FROM dimension_collectors
WHERE id = ?
        "#,
    )
    .bind(collector_id)
    .fetch_optional(pool)
    .await
    .map_sql_err()?;
    row.as_ref()
        .map(map_admin_billing_collector_sqlite)
        .transpose()
}

fn map_admin_billing_collector_sqlite(
    row: &SqliteRow,
) -> Result<AdminBillingCollectorRecord, DataLayerError> {
    Ok(AdminBillingCollectorRecord {
        id: row.try_get("id").map_sql_err()?,
        api_format: row.try_get("api_format").map_sql_err()?,
        task_type: row.try_get("task_type").map_sql_err()?,
        dimension_name: row.try_get("dimension_name").map_sql_err()?,
        source_type: row.try_get("source_type").map_sql_err()?,
        source_path: row.try_get("source_path").map_sql_err()?,
        value_type: row.try_get("value_type").map_sql_err()?,
        transform_expression: row.try_get("transform_expression").map_sql_err()?,
        default_value: row.try_get("default_value").map_sql_err()?,
        priority: row.try_get("priority").map_sql_err()?,
        is_enabled: row.try_get("is_enabled").map_sql_err()?,
        created_at_unix_ms: row
            .try_get::<i64, _>("created_at_unix_ms")
            .map_sql_err()?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_sql_err()?
            .max(0) as u64,
    })
}

fn parse_required_json(raw: String) -> Result<serde_json::Value, DataLayerError> {
    serde_json::from_str(&raw).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("billing JSON field is invalid: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::SqliteBillingReadRepository;
    use crate::lifecycle::migrate::run_sqlite_migrations;
    use crate::repository::billing::{
        AdminBillingCollectorWriteInput, AdminBillingMutationOutcome, AdminBillingRuleWriteInput,
        BillingReadRepository,
    };

    #[tokio::test]
    async fn sqlite_repository_reads_billing_model_context() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_billing_context(&pool).await;

        let repository = SqliteBillingReadRepository::new(pool);
        let by_alias = repository
            .find_model_context("provider-1", Some("key-1"), "gpt-upstream-alias")
            .await
            .expect("context lookup should run")
            .expect("context should exist");
        assert_eq!(by_alias.model_id.as_deref(), Some("model-1"));
        assert_eq!(by_alias.provider_api_key_cache_ttl_minutes, Some(60));
        assert_eq!(by_alias.model_price_per_request, Some(0.01));

        let by_model_id = repository
            .find_model_context_by_model_id("provider-1", Some("key-1"), "model-1")
            .await
            .expect("model lookup should run")
            .expect("context should exist");
        assert_eq!(by_model_id.global_model_name, "gpt-5");
    }

    #[tokio::test]
    async fn sqlite_repository_manages_admin_billing_rules_and_collectors() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        let repository = SqliteBillingReadRepository::new(pool);

        let rule = match repository
            .create_admin_billing_rule(&AdminBillingRuleWriteInput {
                name: "Chat rule".to_string(),
                task_type: "chat".to_string(),
                global_model_id: Some("global-1".to_string()),
                model_id: None,
                expression: "total_tokens * 0.01".to_string(),
                variables: json!({"rate": 0.01}),
                dimension_mappings: json!({"tokens": "total_tokens"}),
                is_enabled: true,
            })
            .await
            .expect("rule create should run")
        {
            AdminBillingMutationOutcome::Applied(rule) => rule,
            other => panic!("unexpected rule create outcome: {other:?}"),
        };
        assert_eq!(rule.variables["rate"], json!(0.01));
        let (rules, total) = repository
            .list_admin_billing_rules(Some("chat"), Some(true), 1, 20)
            .await
            .expect("rule list should run")
            .expect("rule list should be available");
        assert_eq!(total, 1);
        assert_eq!(rules[0].id, rule.id);

        let updated_rule = match repository
            .update_admin_billing_rule(
                &rule.id,
                &AdminBillingRuleWriteInput {
                    name: "Updated chat rule".to_string(),
                    task_type: "chat".to_string(),
                    global_model_id: Some("global-1".to_string()),
                    model_id: None,
                    expression: "total_tokens * 0.02".to_string(),
                    variables: json!({"rate": 0.02}),
                    dimension_mappings: json!({"tokens": "total_tokens"}),
                    is_enabled: false,
                },
            )
            .await
            .expect("rule update should run")
        {
            AdminBillingMutationOutcome::Applied(rule) => rule,
            other => panic!("unexpected rule update outcome: {other:?}"),
        };
        assert_eq!(updated_rule.name, "Updated chat rule");
        assert!(!updated_rule.is_enabled);

        let collector = match repository
            .create_admin_billing_collector(&AdminBillingCollectorWriteInput {
                api_format: "openai".to_string(),
                task_type: "chat".to_string(),
                dimension_name: "total_tokens".to_string(),
                source_type: "usage".to_string(),
                source_path: Some("$.usage.total_tokens".to_string()),
                value_type: "float".to_string(),
                transform_expression: None,
                default_value: Some("1".to_string()),
                priority: 10,
                is_enabled: true,
            })
            .await
            .expect("collector create should run")
        {
            AdminBillingMutationOutcome::Applied(collector) => collector,
            other => panic!("unexpected collector create outcome: {other:?}"),
        };
        assert!(repository
            .admin_billing_enabled_default_value_exists("openai", "chat", "total_tokens", None,)
            .await
            .expect("default value check should run")
            .expect("default value check should be available"));
        let (collectors, total) = repository
            .list_admin_billing_collectors(Some("openai"), Some("chat"), None, Some(true), 1, 20)
            .await
            .expect("collector list should run")
            .expect("collector list should be available");
        assert_eq!(total, 1);
        assert_eq!(collectors[0].id, collector.id);

        let preset = match repository
            .apply_admin_billing_preset(
                "openai-chat",
                "overwrite",
                &[AdminBillingCollectorWriteInput {
                    api_format: "openai".to_string(),
                    task_type: "chat".to_string(),
                    dimension_name: "total_tokens".to_string(),
                    source_type: "usage".to_string(),
                    source_path: Some("$.usage.total_tokens".to_string()),
                    value_type: "float".to_string(),
                    transform_expression: Some("max(total_tokens, 1)".to_string()),
                    default_value: Some("1".to_string()),
                    priority: 10,
                    is_enabled: true,
                }],
            )
            .await
            .expect("preset apply should run")
        {
            AdminBillingMutationOutcome::Applied(result) => result,
            other => panic!("unexpected preset outcome: {other:?}"),
        };
        assert_eq!(preset.updated, 1);
        assert_eq!(preset.errors, Vec::<String>::new());
    }

    async fn seed_billing_context(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO providers (id, name, provider_type, billing_type, is_active, created_at, updated_at)
VALUES ('provider-1', 'Provider One', 'openai', 'pay_as_you_go', 1, 1, 1)
"#,
        )
        .execute(pool)
        .await
        .expect("provider should seed");
        sqlx::query(
            r#"
INSERT INTO provider_api_keys (
  id, provider_id, name, rate_multipliers, cache_ttl_minutes, created_at, updated_at
)
VALUES ('key-1', 'provider-1', 'Primary', '{"openai:chat":0.8}', 60, 1, 1)
"#,
        )
        .execute(pool)
        .await
        .expect("provider key should seed");
        sqlx::query(
            r#"
INSERT INTO global_models (
  id, name, display_name, is_active, default_price_per_request, default_tiered_pricing,
  config, created_at, updated_at
)
VALUES (
  'global-1', 'gpt-5', 'GPT-5', 1, 0.02,
  '{"tiers":[{"up_to":null,"input_price_per_1m":3.0}]}',
  '{"streaming":true}', 1, 1
)
"#,
        )
        .execute(pool)
        .await
        .expect("global model should seed");
        sqlx::query(
            r#"
INSERT INTO models (
  id, provider_id, global_model_id, provider_model_name, is_active, is_available,
  price_per_request, provider_model_mappings, created_at, updated_at
)
VALUES (
  'model-1', 'provider-1', 'global-1', 'gpt-upstream', 1, 1, 0.01,
  '[{"name":"gpt-upstream-alias"}]', 1, 1
)
"#,
        )
        .execute(pool)
        .await
        .expect("model should seed");
    }
}
