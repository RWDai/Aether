use async_trait::async_trait;
use sqlx::{mysql::MySqlRow, Row};

use super::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingMutationOutcome,
    AdminBillingPresetApplyResult, AdminBillingRuleRecord, AdminBillingRuleWriteInput,
    BillingReadRepository, StoredBillingModelContext,
};
use crate::driver::mysql::MysqlPool;
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
  gm.default_price_per_request AS default_price_per_request,
  gm.default_tiered_pricing AS default_tiered_pricing,
  m.id AS model_id,
  m.provider_model_name AS model_provider_model_name,
  m.config AS model_config,
  m.price_per_request AS model_price_per_request,
  m.tiered_pricing AS model_tiered_pricing,
  m.provider_model_mappings AS provider_model_mappings,
  m.is_available AS model_is_available,
  m.created_at AS model_created_at
FROM providers p
"#;

#[derive(Debug, Clone)]
pub struct MysqlBillingReadRepository {
    pool: MysqlPool,
}

impl MysqlBillingReadRepository {
    pub fn new(pool: MysqlPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BillingReadRepository for MysqlBillingReadRepository {
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
        Ok(Some(read_count_mysql(&row)? > 0))
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
        match find_admin_billing_rule_mysql(&self.pool, &id).await? {
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
        let total = read_count_mysql(&total_row)?;
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
            .map(map_admin_billing_rule_mysql)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_rule(
        &self,
        rule_id: &str,
    ) -> Result<Option<AdminBillingRuleRecord>, DataLayerError> {
        find_admin_billing_rule_mysql(&self.pool, rule_id).await
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
        match find_admin_billing_rule_mysql(&self.pool, rule_id).await? {
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
        match find_admin_billing_collector_mysql(&self.pool, &id).await? {
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
        let total = read_count_mysql(&total_row)?;
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
            .map(map_admin_billing_collector_mysql)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((items, total)))
    }

    async fn find_admin_billing_collector(
        &self,
        collector_id: &str,
    ) -> Result<Option<AdminBillingCollectorRecord>, DataLayerError> {
        find_admin_billing_collector_mysql(&self.pool, collector_id).await
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
        match find_admin_billing_collector_mysql(&self.pool, collector_id).await? {
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
    row: &MySqlRow,
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

    let has_model_price = row
        .try_get::<Option<f64>, _>("model_price_per_request")
        .map_sql_err()?
        .is_some()
        || row
            .try_get::<Option<String>, _>("model_tiered_pricing")
            .ok()
            .flatten()
            .is_some();
    let has_default_price = row
        .try_get::<Option<f64>, _>("default_price_per_request")
        .map_sql_err()?
        .is_some()
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

fn map_row(row: &MySqlRow) -> Result<StoredBillingModelContext, DataLayerError> {
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
        row.try_get("default_price_per_request").map_sql_err()?,
        parse_json(row.try_get("default_tiered_pricing").ok().flatten())?,
        row.try_get("model_id").map_sql_err()?,
        row.try_get("model_provider_model_name").map_sql_err()?,
        parse_json(row.try_get("model_config").ok().flatten())?,
        row.try_get("model_price_per_request").map_sql_err()?,
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

fn read_count_mysql(row: &MySqlRow) -> Result<u64, DataLayerError> {
    Ok(row.try_get::<i64, _>("total").map_sql_err()?.max(0) as u64)
}

async fn find_admin_billing_rule_mysql(
    pool: &MysqlPool,
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
    row.as_ref().map(map_admin_billing_rule_mysql).transpose()
}

fn map_admin_billing_rule_mysql(row: &MySqlRow) -> Result<AdminBillingRuleRecord, DataLayerError> {
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

async fn find_admin_billing_collector_mysql(
    pool: &MysqlPool,
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
        .map(map_admin_billing_collector_mysql)
        .transpose()
}

fn map_admin_billing_collector_mysql(
    row: &MySqlRow,
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
    use super::MysqlBillingReadRepository;

    #[tokio::test]
    async fn repository_builds_from_lazy_pool() {
        let pool = sqlx::mysql::MySqlPoolOptions::new().connect_lazy_with(
            "mysql://user:pass@localhost:3306/aether"
                .parse()
                .expect("mysql options should parse"),
        );

        let _repository = MysqlBillingReadRepository::new(pool);
    }
}
