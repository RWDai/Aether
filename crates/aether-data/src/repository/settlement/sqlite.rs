use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

use super::{SettlementWriteRepository, StoredUsageSettlement, UsageSettlementInput};
use crate::driver::sqlite::SqlitePool;
use crate::error::SqlResultExt;
use crate::DataLayerError;

const FIND_USAGE_FOR_SETTLEMENT_SQL: &str = r#"
SELECT
  usage_record.request_id,
  COALESCE(usage_settlement_snapshots.wallet_id, usage_record.wallet_id) AS wallet_id,
  COALESCE(usage_settlement_snapshots.billing_status, usage_record.billing_status) AS billing_status,
  COALESCE(
    usage_settlement_snapshots.wallet_balance_before,
    usage_record.wallet_balance_before
  ) AS wallet_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_balance_after,
    usage_record.wallet_balance_after
  ) AS wallet_balance_after,
  COALESCE(
    usage_settlement_snapshots.wallet_recharge_balance_before,
    usage_record.wallet_recharge_balance_before
  ) AS wallet_recharge_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_recharge_balance_after,
    usage_record.wallet_recharge_balance_after
  ) AS wallet_recharge_balance_after,
  COALESCE(
    usage_settlement_snapshots.wallet_gift_balance_before,
    usage_record.wallet_gift_balance_before
  ) AS wallet_gift_balance_before,
  COALESCE(
    usage_settlement_snapshots.wallet_gift_balance_after,
    usage_record.wallet_gift_balance_after
  ) AS wallet_gift_balance_after,
  usage_settlement_snapshots.provider_monthly_used_usd AS provider_monthly_used_usd,
  usage_record.provider_id,
  COALESCE(usage_settlement_snapshots.finalized_at, usage_record.finalized_at) AS finalized_at_unix_secs
FROM "usage" AS usage_record
LEFT JOIN usage_settlement_snapshots
  ON usage_settlement_snapshots.request_id = usage_record.request_id
WHERE usage_record.request_id = ?
"#;

const FINALIZE_USAGE_BILLING_SQL: &str = r#"
UPDATE "usage"
SET
  billing_status = ?,
  finalized_at = COALESCE(finalized_at, ?)
WHERE request_id = ?
"#;

const UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL: &str = r#"
INSERT INTO usage_settlement_snapshots (
  request_id,
  billing_status,
  wallet_id,
  wallet_balance_before,
  wallet_balance_after,
  wallet_recharge_balance_before,
  wallet_recharge_balance_after,
  wallet_gift_balance_before,
  wallet_gift_balance_after,
  provider_monthly_used_usd,
  finalized_at,
  created_at,
  updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (request_id)
DO UPDATE SET
  billing_status = excluded.billing_status,
  wallet_id = COALESCE(excluded.wallet_id, usage_settlement_snapshots.wallet_id),
  wallet_balance_before = COALESCE(
    excluded.wallet_balance_before,
    usage_settlement_snapshots.wallet_balance_before
  ),
  wallet_balance_after = COALESCE(
    excluded.wallet_balance_after,
    usage_settlement_snapshots.wallet_balance_after
  ),
  wallet_recharge_balance_before = COALESCE(
    excluded.wallet_recharge_balance_before,
    usage_settlement_snapshots.wallet_recharge_balance_before
  ),
  wallet_recharge_balance_after = COALESCE(
    excluded.wallet_recharge_balance_after,
    usage_settlement_snapshots.wallet_recharge_balance_after
  ),
  wallet_gift_balance_before = COALESCE(
    excluded.wallet_gift_balance_before,
    usage_settlement_snapshots.wallet_gift_balance_before
  ),
  wallet_gift_balance_after = COALESCE(
    excluded.wallet_gift_balance_after,
    usage_settlement_snapshots.wallet_gift_balance_after
  ),
  provider_monthly_used_usd = COALESCE(
    excluded.provider_monthly_used_usd,
    usage_settlement_snapshots.provider_monthly_used_usd
  ),
  finalized_at = COALESCE(excluded.finalized_at, usage_settlement_snapshots.finalized_at),
  updated_at = excluded.updated_at
"#;

#[derive(Debug, Clone)]
pub struct SqliteSettlementRepository {
    pool: SqlitePool,
}

impl SqliteSettlementRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn settlement_from_row(row: &SqliteRow) -> Result<StoredUsageSettlement, DataLayerError> {
    Ok(StoredUsageSettlement {
        request_id: row.try_get("request_id").map_sql_err()?,
        wallet_id: row.try_get("wallet_id").map_sql_err()?,
        billing_status: row.try_get("billing_status").map_sql_err()?,
        wallet_balance_before: row.try_get("wallet_balance_before").map_sql_err()?,
        wallet_balance_after: row.try_get("wallet_balance_after").map_sql_err()?,
        wallet_recharge_balance_before: row
            .try_get("wallet_recharge_balance_before")
            .map_sql_err()?,
        wallet_recharge_balance_after: row
            .try_get("wallet_recharge_balance_after")
            .map_sql_err()?,
        wallet_gift_balance_before: row.try_get("wallet_gift_balance_before").map_sql_err()?,
        wallet_gift_balance_after: row.try_get("wallet_gift_balance_after").map_sql_err()?,
        provider_monthly_used_usd: row.try_get("provider_monthly_used_usd").map_sql_err()?,
        finalized_at_unix_secs: row
            .try_get::<Option<i64>, _>("finalized_at_unix_secs")
            .map_sql_err()?
            .map(|value| value as u64),
    })
}

fn now_unix_secs() -> Result<i64, DataLayerError> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .map_err(|_| DataLayerError::InvalidInput("timestamp overflow".to_string()))
}

#[async_trait]
impl SettlementWriteRepository for SqliteSettlementRepository {
    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        input.validate()?;
        let finalized_at = i64::try_from(
            input
                .finalized_at_unix_secs
                .unwrap_or(now_unix_secs()? as u64),
        )
        .map_err(|_| DataLayerError::InvalidInput("finalized_at overflow".to_string()))?;
        let updated_at = now_unix_secs()?;

        let mut tx = self.pool.begin().await.map_sql_err()?;
        let row = sqlx::query(FIND_USAGE_FOR_SETTLEMENT_SQL)
            .bind(&input.request_id)
            .fetch_optional(&mut *tx)
            .await
            .map_sql_err()?;

        let Some(usage_row) = row else {
            tx.commit().await.map_sql_err()?;
            return Ok(None);
        };

        let current_billing_status: String = usage_row.try_get("billing_status").map_sql_err()?;
        if current_billing_status == "settled" || current_billing_status == "void" {
            let settlement = settlement_from_row(&usage_row)?;
            tx.commit().await.map_sql_err()?;
            return Ok(Some(settlement));
        }

        let final_billing_status = if input.status == "completed" {
            "settled"
        } else {
            "void"
        };
        let mut settlement = StoredUsageSettlement {
            request_id: input.request_id.clone(),
            wallet_id: None,
            billing_status: final_billing_status.to_string(),
            wallet_balance_before: None,
            wallet_balance_after: None,
            wallet_recharge_balance_before: None,
            wallet_recharge_balance_after: None,
            wallet_gift_balance_before: None,
            wallet_gift_balance_after: None,
            provider_monthly_used_usd: None,
            finalized_at_unix_secs: Some(finalized_at as u64),
        };

        if final_billing_status == "settled" {
            let api_key_id = input
                .api_key_id
                .as_deref()
                .filter(|value| !value.is_empty());
            let api_key_is_standalone = if input.api_key_is_standalone {
                true
            } else if let Some(api_key_id) = api_key_id {
                sqlx::query_scalar::<_, bool>(
                    r#"
SELECT is_standalone
FROM api_keys
WHERE id = ?
LIMIT 1
"#,
                )
                .bind(api_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_sql_err()?
                .unwrap_or(false)
            } else {
                false
            };

            let wallet_row = if let Some(api_key_id) = api_key_id {
                sqlx::query(
                    r#"
SELECT id, balance, gift_balance, limit_mode
FROM wallets
WHERE api_key_id = ?
LIMIT 1
"#,
                )
                .bind(api_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_sql_err()?
            } else {
                None
            };

            let wallet_row = if wallet_row.is_some() {
                wallet_row
            } else if !api_key_is_standalone {
                if let Some(user_id) = input.user_id.as_deref().filter(|value| !value.is_empty()) {
                    sqlx::query(
                        r#"
SELECT id, balance, gift_balance, limit_mode
FROM wallets
WHERE user_id = ?
LIMIT 1
"#,
                    )
                    .bind(user_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_sql_err()?
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(wallet_row) = wallet_row {
                let wallet_id: String = wallet_row.try_get("id").map_sql_err()?;
                let before_recharge: f64 = wallet_row.try_get("balance").map_sql_err()?;
                let before_gift: f64 = wallet_row.try_get("gift_balance").map_sql_err()?;
                let limit_mode: String = wallet_row.try_get("limit_mode").map_sql_err()?;
                let before_total = before_recharge + before_gift;
                let mut after_recharge = before_recharge;
                let mut after_gift = before_gift;
                if !limit_mode.eq_ignore_ascii_case("unlimited") {
                    let gift_deduction = before_gift.max(0.0).min(input.total_cost_usd);
                    let recharge_deduction = input.total_cost_usd - gift_deduction;
                    after_gift = before_gift - gift_deduction;
                    after_recharge = before_recharge - recharge_deduction;
                }
                sqlx::query(
                    r#"
UPDATE wallets
SET
  balance = ?,
  gift_balance = ?,
  total_consumed = COALESCE(total_consumed, 0) + ?,
  updated_at = ?
WHERE id = ?
"#,
                )
                .bind(after_recharge)
                .bind(after_gift)
                .bind(input.total_cost_usd)
                .bind(updated_at)
                .bind(&wallet_id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;

                settlement.wallet_id = Some(wallet_id);
                settlement.wallet_balance_before = Some(before_total);
                settlement.wallet_balance_after = Some(after_recharge + after_gift);
                settlement.wallet_recharge_balance_before = Some(before_recharge);
                settlement.wallet_recharge_balance_after = Some(after_recharge);
                settlement.wallet_gift_balance_before = Some(before_gift);
                settlement.wallet_gift_balance_after = Some(after_gift);
            }

            if let Some(provider_id) = input
                .provider_id
                .as_deref()
                .filter(|value| !value.is_empty())
            {
                sqlx::query(
                    r#"
UPDATE providers
SET
  monthly_used_usd = COALESCE(monthly_used_usd, 0) + ?,
  updated_at = ?
WHERE id = ?
"#,
                )
                .bind(input.actual_total_cost_usd)
                .bind(updated_at)
                .bind(provider_id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;

                settlement.provider_monthly_used_usd = sqlx::query_scalar::<_, Option<f64>>(
                    "SELECT monthly_used_usd FROM providers WHERE id = ? LIMIT 1",
                )
                .bind(provider_id)
                .fetch_optional(&mut *tx)
                .await
                .map_sql_err()?
                .flatten();
            }
        }

        sqlx::query(UPSERT_USAGE_SETTLEMENT_SNAPSHOT_SQL)
            .bind(&settlement.request_id)
            .bind(&settlement.billing_status)
            .bind(settlement.wallet_id.as_deref())
            .bind(settlement.wallet_balance_before)
            .bind(settlement.wallet_balance_after)
            .bind(settlement.wallet_recharge_balance_before)
            .bind(settlement.wallet_recharge_balance_after)
            .bind(settlement.wallet_gift_balance_before)
            .bind(settlement.wallet_gift_balance_after)
            .bind(settlement.provider_monthly_used_usd)
            .bind(settlement.finalized_at_unix_secs.map(|value| value as i64))
            .bind(updated_at)
            .bind(updated_at)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;

        sqlx::query(FINALIZE_USAGE_BILLING_SQL)
            .bind(final_billing_status)
            .bind(finalized_at)
            .bind(&input.request_id)
            .execute(&mut *tx)
            .await
            .map_sql_err()?;

        tx.commit().await.map_sql_err()?;
        Ok(Some(settlement))
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteSettlementRepository;
    use crate::lifecycle::migrate::run_sqlite_migrations;
    use crate::repository::settlement::{SettlementWriteRepository, UsageSettlementInput};
    use sqlx::Row;

    #[tokio::test]
    async fn sqlite_repository_settles_usage_once() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-1".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 2.0,
                finalized_at_unix_secs: Some(1_234),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "settled");
        assert_eq!(settlement.wallet_id.as_deref(), Some("wallet-1"));
        assert_eq!(settlement.wallet_balance_before, Some(12.0));
        assert_eq!(settlement.wallet_balance_after, Some(9.0));
        assert_eq!(settlement.wallet_recharge_balance_after, Some(9.0));
        assert_eq!(settlement.wallet_gift_balance_after, Some(0.0));
        assert_eq!(settlement.provider_monthly_used_usd, Some(7.0));

        let wallet = sqlx::query(
            "SELECT balance, gift_balance, total_consumed FROM wallets WHERE id = 'wallet-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("wallet should load");
        assert_eq!(wallet.try_get::<f64, _>("balance").unwrap(), 9.0);
        assert_eq!(wallet.try_get::<f64, _>("gift_balance").unwrap(), 0.0);
        assert_eq!(wallet.try_get::<f64, _>("total_consumed").unwrap(), 3.0);

        let second = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-1".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 2.0,
                finalized_at_unix_secs: Some(9_999),
            })
            .await
            .expect("second settlement should run")
            .expect("usage should exist");
        assert_eq!(second.finalized_at_unix_secs, Some(1_234));

        let provider_used: f64 =
            sqlx::query_scalar("SELECT monthly_used_usd FROM providers WHERE id = 'provider-1'")
                .fetch_one(&pool)
                .await
                .expect("provider should load");
        assert_eq!(provider_used, 7.0);
    }

    #[tokio::test]
    async fn sqlite_repository_voids_failed_usage_without_wallet_mutation() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_settlement_rows(&pool).await;

        let repository = SqliteSettlementRepository::new(pool.clone());
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "request-2".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: None,
                api_key_is_standalone: false,
                provider_id: Some("provider-1".to_string()),
                status: "failed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 2.0,
                finalized_at_unix_secs: Some(1_235),
            })
            .await
            .expect("settlement should run")
            .expect("usage should exist");

        assert_eq!(settlement.billing_status, "void");
        assert_eq!(settlement.wallet_id, None);
        let wallet_total: f64 =
            sqlx::query_scalar("SELECT balance + gift_balance FROM wallets WHERE id = 'wallet-1'")
                .fetch_one(&pool)
                .await
                .expect("wallet should load");
        assert_eq!(wallet_total, 12.0);
    }

    async fn seed_settlement_rows(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO providers (
  id, name, provider_type, monthly_used_usd, created_at, updated_at
)
VALUES ('provider-1', 'Provider One', 'openai', 5.0, 1, 1);

INSERT INTO wallets (
  id, user_id, balance, gift_balance, limit_mode, created_at, updated_at
)
VALUES ('wallet-1', 'user-1', 10.0, 2.0, 'finite', 1, 1);

INSERT INTO "usage" (
  request_id, user_id, provider_id, status, billing_status, total_cost_usd, actual_total_cost_usd
)
VALUES
  ('request-1', 'user-1', 'provider-1', 'completed', 'pending', 3.0, 2.0),
  ('request-2', 'user-1', 'provider-1', 'failed', 'pending', 3.0, 2.0);
"#,
        )
        .execute(pool)
        .await
        .expect("settlement rows should seed");
    }
}
