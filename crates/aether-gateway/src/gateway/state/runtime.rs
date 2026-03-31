use super::*;
use sqlx::Row;

#[derive(serde::Serialize)]
pub(crate) struct AdminSecurityBlacklistEntryPayload {
    pub(crate) ip_address: String,
    pub(crate) reason: String,
    pub(crate) ttl_seconds: Option<i64>,
}

fn admin_wallet_build_order_no(now: chrono::DateTime<chrono::Utc>) -> String {
    format!(
        "po_{}_{}",
        now.format("%Y%m%d%H%M%S%6f"),
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

fn admin_payment_gateway_response_map(
    value: Option<serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    match value {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    }
}

fn admin_wallet_snapshot_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<aether_data::repository::wallet::StoredWalletSnapshot, GatewayError> {
    aether_data::repository::wallet::StoredWalletSnapshot::new(
        row.try_get("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("user_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("api_key_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("limit_mode")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("currency")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("total_recharged")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("total_consumed")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("total_refunded")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("total_adjusted")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        row.try_get("updated_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    )
    .map_err(|err| GatewayError::Internal(err.to_string()))
}

fn admin_wallet_payment_order_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminWalletPaymentOrderRecord, GatewayError> {
    Ok(AdminWalletPaymentOrderRecord {
        id: row
            .try_get("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        order_no: row
            .try_get("order_no")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        wallet_id: row
            .try_get("wallet_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        user_id: row
            .try_get("user_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        amount_usd: row
            .try_get("amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        pay_amount: row
            .try_get("pay_amount")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        pay_currency: row
            .try_get("pay_currency")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        exchange_rate: row
            .try_get("exchange_rate")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        refunded_amount_usd: row
            .try_get("refunded_amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        refundable_amount_usd: row
            .try_get("refundable_amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        payment_method: row
            .try_get("payment_method")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        gateway_order_id: row
            .try_get("gateway_order_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        status: row
            .try_get("status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        gateway_response: row
            .try_get("gateway_response")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        created_at_unix_secs: row
            .try_get::<i64, _>("created_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
        paid_at_unix_secs: row
            .try_get::<Option<i64>, _>("paid_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| value.max(0) as u64),
        credited_at_unix_secs: row
            .try_get::<Option<i64>, _>("credited_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| value.max(0) as u64),
        expires_at_unix_secs: row
            .try_get::<Option<i64>, _>("expires_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| value.max(0) as u64),
    })
}

fn admin_wallet_refund_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminWalletRefundRecord, GatewayError> {
    Ok(AdminWalletRefundRecord {
        id: row
            .try_get("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        refund_no: row
            .try_get("refund_no")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        wallet_id: row
            .try_get("wallet_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        user_id: row
            .try_get("user_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        payment_order_id: row
            .try_get("payment_order_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        source_type: row
            .try_get("source_type")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        source_id: row
            .try_get("source_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        refund_mode: row
            .try_get("refund_mode")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        amount_usd: row
            .try_get("amount_usd")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        status: row
            .try_get("status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        reason: row
            .try_get("reason")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        failure_reason: row
            .try_get("failure_reason")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        gateway_refund_id: row
            .try_get("gateway_refund_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        payout_method: row
            .try_get("payout_method")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        payout_reference: row
            .try_get("payout_reference")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        payout_proof: row
            .try_get("payout_proof")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        requested_by: row
            .try_get("requested_by")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        approved_by: row
            .try_get("approved_by")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        processed_by: row
            .try_get("processed_by")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        created_at_unix_secs: row
            .try_get::<i64, _>("created_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
        processed_at_unix_secs: row
            .try_get::<Option<i64>, _>("processed_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| value.max(0) as u64),
        completed_at_unix_secs: row
            .try_get::<Option<i64>, _>("completed_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| value.max(0) as u64),
    })
}

fn admin_billing_rule_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminBillingRuleRecord, GatewayError> {
    Ok(AdminBillingRuleRecord {
        id: row
            .try_get("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        name: row
            .try_get("name")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        task_type: row
            .try_get("task_type")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        global_model_id: row
            .try_get("global_model_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        model_id: row
            .try_get("model_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        expression: row
            .try_get("expression")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        variables: row
            .try_get::<Option<serde_json::Value>, _>("variables")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .unwrap_or_else(|| serde_json::json!({})),
        dimension_mappings: row
            .try_get::<Option<serde_json::Value>, _>("dimension_mappings")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .unwrap_or_else(|| serde_json::json!({})),
        is_enabled: row
            .try_get("is_enabled")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        created_at_unix_secs: row
            .try_get::<i64, _>("created_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
    })
}

fn admin_billing_collector_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AdminBillingCollectorRecord, GatewayError> {
    Ok(AdminBillingCollectorRecord {
        id: row
            .try_get("id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        api_format: row
            .try_get("api_format")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        task_type: row
            .try_get("task_type")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        dimension_name: row
            .try_get("dimension_name")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        source_type: row
            .try_get("source_type")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        source_path: row
            .try_get("source_path")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        value_type: row
            .try_get("value_type")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        transform_expression: row
            .try_get("transform_expression")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        default_value: row
            .try_get("default_value")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        priority: row
            .try_get("priority")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        is_enabled: row
            .try_get("is_enabled")
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
        created_at_unix_secs: row
            .try_get::<i64, _>("created_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
        updated_at_unix_secs: row
            .try_get::<i64, _>("updated_at_unix_secs")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64,
    })
}

impl AppState {
    pub fn has_announcement_data_reader(&self) -> bool {
        self.data.has_announcement_reader()
    }

    pub fn has_announcement_data_writer(&self) -> bool {
        self.data.has_announcement_writer()
    }

    pub fn has_video_task_data_reader(&self) -> bool {
        self.data.has_video_task_reader()
    }

    pub fn has_video_task_data_writer(&self) -> bool {
        self.data.has_video_task_writer()
    }

    pub fn has_request_candidate_data_reader(&self) -> bool {
        self.data.has_request_candidate_reader()
    }

    pub fn has_request_candidate_data_writer(&self) -> bool {
        self.data.has_request_candidate_writer()
    }

    pub fn has_usage_data_reader(&self) -> bool {
        self.data.has_usage_reader()
    }

    pub fn has_user_data_reader(&self) -> bool {
        self.data.has_user_reader()
    }

    pub fn has_usage_data_writer(&self) -> bool {
        self.data.has_usage_writer()
    }

    pub fn has_usage_worker_backend(&self) -> bool {
        self.data.has_usage_worker_runner()
    }

    pub fn has_wallet_data_reader(&self) -> bool {
        self.data.has_wallet_reader()
    }

    pub fn has_wallet_data_writer(&self) -> bool {
        self.data.has_wallet_writer()
    }

    pub fn has_auth_user_write_capability(&self) -> bool {
        #[cfg(test)]
        if self.auth_user_store.is_some() {
            return true;
        }

        self.postgres_pool().is_some()
    }

    pub fn has_auth_wallet_write_capability(&self) -> bool {
        #[cfg(test)]
        if self.auth_wallet_store.is_some() {
            return true;
        }

        self.postgres_pool().is_some()
    }

    pub fn has_provider_quota_data_writer(&self) -> bool {
        self.data.has_provider_quota_writer()
    }

    pub fn has_shadow_result_data_writer(&self) -> bool {
        self.data.has_shadow_result_writer()
    }

    pub fn has_shadow_result_data_reader(&self) -> bool {
        self.data.has_shadow_result_reader()
    }

    pub(crate) async fn add_admin_security_blacklist(
        &self,
        ip_address: &str,
        reason: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            let key = runner
                .keyspace()
                .key(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}{ip_address}"));
            let result = if let Some(ttl_seconds) = ttl_seconds {
                redis::cmd("SETEX")
                    .arg(&key)
                    .arg(ttl_seconds)
                    .arg(reason)
                    .query_async::<String>(&mut connection)
                    .await
            } else {
                redis::cmd("SET")
                    .arg(&key)
                    .arg(reason)
                    .query_async::<String>(&mut connection)
                    .await
            };
            return Ok(result.is_ok());
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_blacklist_store.as_ref() {
            store
                .lock()
                .expect("admin security blacklist store should lock")
                .insert(ip_address.to_string(), reason.to_string());
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) async fn remove_admin_security_blacklist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            let key = runner
                .keyspace()
                .key(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}{ip_address}"));
            let deleted = match redis::cmd("DEL")
                .arg(&key)
                .query_async::<i64>(&mut connection)
                .await
            {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            return Ok(deleted > 0);
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_blacklist_store.as_ref() {
            let removed = store
                .lock()
                .expect("admin security blacklist store should lock")
                .remove(ip_address)
                .is_some();
            return Ok(removed);
        }

        Ok(false)
    }

    pub(crate) async fn admin_security_blacklist_stats(
        &self,
    ) -> Result<(bool, usize, Option<String>), GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok((false, 0, Some("Redis 不可用".to_string()))),
            };
            let pattern = runner
                .keyspace()
                .key(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}*"));
            let mut cursor = 0u64;
            let mut total = 0usize;
            loop {
                let (next_cursor, keys) = match redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async::<(u64, Vec<String>)>(&mut connection)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => return Ok((false, 0, Some(err.to_string()))),
                };
                total += keys.len();
                if next_cursor == 0 {
                    break;
                }
                cursor = next_cursor;
            }
            return Ok((true, total, None));
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_blacklist_store.as_ref() {
            let total = store
                .lock()
                .expect("admin security blacklist store should lock")
                .len();
            return Ok((true, total, None));
        }

        Ok((false, 0, Some("Redis 不可用".to_string())))
    }

    pub(crate) async fn list_admin_security_blacklist(
        &self,
    ) -> Result<Vec<AdminSecurityBlacklistEntry>, GatewayError> {
        const ADMIN_SECURITY_BLACKLIST_PREFIX: &str = "ip:blacklist:";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(Vec::new()),
            };
            let pattern = runner
                .keyspace()
                .key(&format!("{ADMIN_SECURITY_BLACKLIST_PREFIX}*"));
            let prefix = runner.keyspace().key(ADMIN_SECURITY_BLACKLIST_PREFIX);
            let mut cursor = 0u64;
            let mut entries = Vec::new();
            loop {
                let (next_cursor, keys) = match redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async::<(u64, Vec<String>)>(&mut connection)
                    .await
                {
                    Ok(value) => value,
                    Err(_) => break,
                };
                for full_key in keys {
                    let ip_address = full_key
                        .strip_prefix(prefix.as_str())
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| full_key.clone());
                    let reason: Result<String, _> = redis::cmd("GET")
                        .arg(&full_key)
                        .query_async(&mut connection)
                        .await;
                    let reason = match reason {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    let ttl = match redis::cmd("TTL")
                        .arg(&full_key)
                        .query_async::<i64>(&mut connection)
                        .await
                    {
                        Ok(value) if value >= 0 => Some(value),
                        _ => None,
                    };
                    entries.push(AdminSecurityBlacklistEntry {
                        ip_address,
                        reason,
                        ttl_seconds: ttl,
                    });
                }
                if next_cursor == 0 {
                    break;
                }
                cursor = next_cursor;
            }
            entries.sort_by(|a, b| a.ip_address.cmp(&b.ip_address));
            return Ok(entries);
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_blacklist_store.as_ref() {
            let mut entries = store
                .lock()
                .expect("admin security blacklist store should lock")
                .iter()
                .map(|(ip, reason)| AdminSecurityBlacklistEntry {
                    ip_address: ip.clone(),
                    reason: reason.clone(),
                    ttl_seconds: None,
                })
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| a.ip_address.cmp(&b.ip_address));
            return Ok(entries);
        }

        Ok(Vec::new())
    }

    pub(crate) async fn add_admin_security_whitelist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            let key = runner.keyspace().key(ADMIN_SECURITY_WHITELIST_KEY);
            let added = match redis::cmd("SADD")
                .arg(&key)
                .arg(ip_address)
                .query_async::<i64>(&mut connection)
                .await
            {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            return Ok(added >= 0);
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_whitelist_store.as_ref() {
            store
                .lock()
                .expect("admin security whitelist store should lock")
                .insert(ip_address.to_string());
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) async fn remove_admin_security_whitelist(
        &self,
        ip_address: &str,
    ) -> Result<bool, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            let key = runner.keyspace().key(ADMIN_SECURITY_WHITELIST_KEY);
            let removed = match redis::cmd("SREM")
                .arg(&key)
                .arg(ip_address)
                .query_async::<i64>(&mut connection)
                .await
            {
                Ok(value) => value,
                Err(_) => return Ok(false),
            };
            return Ok(removed > 0);
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_whitelist_store.as_ref() {
            let removed = store
                .lock()
                .expect("admin security whitelist store should lock")
                .remove(ip_address);
            return Ok(removed);
        }

        Ok(false)
    }

    pub(crate) async fn list_admin_security_whitelist(&self) -> Result<Vec<String>, GatewayError> {
        const ADMIN_SECURITY_WHITELIST_KEY: &str = "ip:whitelist";

        if let Some(runner) = self.redis_kv_runner() {
            let mut connection = match runner.client().get_multiplexed_async_connection().await {
                Ok(value) => value,
                Err(_) => return Ok(Vec::new()),
            };
            let key = runner.keyspace().key(ADMIN_SECURITY_WHITELIST_KEY);
            let mut whitelist = match redis::cmd("SMEMBERS")
                .arg(&key)
                .query_async::<Vec<String>>(&mut connection)
                .await
            {
                Ok(value) => value,
                Err(_) => return Ok(Vec::new()),
            };
            whitelist.sort();
            return Ok(whitelist);
        }

        #[cfg(test)]
        if let Some(store) = self.admin_security_whitelist_store.as_ref() {
            return Ok(store
                .lock()
                .expect("admin security whitelist store should lock")
                .iter()
                .cloned()
                .collect());
        }

        Ok(Vec::new())
    }

    pub(crate) async fn admin_billing_enabled_default_value_exists(
        &self,
        api_format: &str,
        task_type: &str,
        dimension_name: &str,
        existing_id: Option<&str>,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            let exists = store
                .lock()
                .expect("admin billing collector store should lock")
                .values()
                .any(|collector| {
                    collector.api_format == api_format
                        && collector.task_type == task_type
                        && collector.dimension_name == dimension_name
                        && collector.is_enabled
                        && collector.default_value.is_some()
                        && existing_id.is_none_or(|value| collector.id != value)
                });
            return Ok(exists);
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(false);
        };
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
        .fetch_one(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok(exists)
    }

    pub(crate) async fn create_admin_billing_rule(
        &self,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<LocalMutationOutcome<AdminBillingRuleRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_rule_store.as_ref() {
            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let record = AdminBillingRuleRecord {
                id: uuid::Uuid::new_v4().to_string(),
                name: input.name.clone(),
                task_type: input.task_type.clone(),
                global_model_id: input.global_model_id.clone(),
                model_id: input.model_id.clone(),
                expression: input.expression.clone(),
                variables: input.variables.clone(),
                dimension_mappings: input.dimension_mappings.clone(),
                is_enabled: input.is_enabled,
                created_at_unix_secs: now_unix_secs,
                updated_at_unix_secs: now_unix_secs,
            };
            store
                .lock()
                .expect("admin billing rule store should lock")
                .insert(record.id.clone(), record.clone());
            return Ok(LocalMutationOutcome::Applied(record));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(LocalMutationOutcome::Unavailable);
        };
        let rule_id = uuid::Uuid::new_v4().to_string();
        let row = match sqlx::query(
            r#"
INSERT INTO billing_rules (
  id,
  name,
  task_type,
  global_model_id,
  model_id,
  expression,
  variables,
  dimension_mappings,
  is_enabled,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  NOW(),
  NOW()
)
RETURNING
  id,
  name,
  task_type,
  global_model_id,
  model_id,
  expression,
  variables,
  dimension_mappings,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
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
        .fetch_one(&pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(LocalMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(GatewayError::Internal(err.to_string())),
        };
        Ok(LocalMutationOutcome::Applied(admin_billing_rule_from_row(
            &row,
        )?))
    }

    pub(crate) async fn list_admin_billing_rules(
        &self,
        task_type: Option<&str>,
        is_enabled: Option<bool>,
        page: u32,
        page_size: u32,
    ) -> Result<Option<(Vec<AdminBillingRuleRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (task_type, is_enabled, page, page_size);

        #[cfg(test)]
        if let Some(store) = self.admin_billing_rule_store.as_ref() {
            let mut items = store
                .lock()
                .expect("admin billing rule store should lock")
                .values()
                .filter(|record| {
                    task_type.is_none_or(|expected| record.task_type == expected)
                        && is_enabled.is_none_or(|expected| record.is_enabled == expected)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .updated_at_unix_secs
                    .cmp(&left.updated_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let offset = (page.saturating_sub(1) as usize) * (page_size as usize);
            let items = items
                .into_iter()
                .skip(offset)
                .take(page_size as usize)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn read_admin_billing_rule(
        &self,
        rule_id: &str,
    ) -> Result<Option<AdminBillingRuleRecord>, GatewayError> {
        #[cfg(not(test))]
        let _ = rule_id;

        #[cfg(test)]
        if let Some(store) = self.admin_billing_rule_store.as_ref() {
            return Ok(store
                .lock()
                .expect("admin billing rule store should lock")
                .get(rule_id)
                .cloned());
        }

        Ok(None)
    }

    pub(crate) async fn update_admin_billing_rule(
        &self,
        rule_id: &str,
        input: &AdminBillingRuleWriteInput,
    ) -> Result<LocalMutationOutcome<AdminBillingRuleRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_rule_store.as_ref() {
            let mut guard = store.lock().expect("admin billing rule store should lock");
            let Some(record) = guard.get_mut(rule_id) else {
                return Ok(LocalMutationOutcome::NotFound);
            };
            record.name = input.name.clone();
            record.task_type = input.task_type.clone();
            record.global_model_id = input.global_model_id.clone();
            record.model_id = input.model_id.clone();
            record.expression = input.expression.clone();
            record.variables = input.variables.clone();
            record.dimension_mappings = input.dimension_mappings.clone();
            record.is_enabled = input.is_enabled;
            record.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            return Ok(LocalMutationOutcome::Applied(record.clone()));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(LocalMutationOutcome::Unavailable);
        };
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
  id,
  name,
  task_type,
  global_model_id,
  model_id,
  expression,
  variables,
  dimension_mappings,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
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
        .fetch_optional(&pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(LocalMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(GatewayError::Internal(err.to_string())),
        };
        match row {
            Some(row) => Ok(LocalMutationOutcome::Applied(admin_billing_rule_from_row(
                &row,
            )?)),
            None => Ok(LocalMutationOutcome::NotFound),
        }
    }

    pub(crate) async fn create_admin_billing_collector(
        &self,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<LocalMutationOutcome<AdminBillingCollectorRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let record = AdminBillingCollectorRecord {
                id: uuid::Uuid::new_v4().to_string(),
                api_format: input.api_format.clone(),
                task_type: input.task_type.clone(),
                dimension_name: input.dimension_name.clone(),
                source_type: input.source_type.clone(),
                source_path: input.source_path.clone(),
                value_type: input.value_type.clone(),
                transform_expression: input.transform_expression.clone(),
                default_value: input.default_value.clone(),
                priority: input.priority,
                is_enabled: input.is_enabled,
                created_at_unix_secs: now_unix_secs,
                updated_at_unix_secs: now_unix_secs,
            };
            store
                .lock()
                .expect("admin billing collector store should lock")
                .insert(record.id.clone(), record.clone());
            return Ok(LocalMutationOutcome::Applied(record));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(LocalMutationOutcome::Unavailable);
        };
        let collector_id = uuid::Uuid::new_v4().to_string();
        let row = match sqlx::query(
            r#"
INSERT INTO dimension_collectors (
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  NOW(),
  NOW()
)
RETURNING
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
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
        .fetch_one(&pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(LocalMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(GatewayError::Internal(err.to_string())),
        };
        Ok(LocalMutationOutcome::Applied(
            admin_billing_collector_from_row(&row)?,
        ))
    }

    pub(crate) async fn list_admin_billing_collectors(
        &self,
        api_format: Option<&str>,
        task_type: Option<&str>,
        dimension_name: Option<&str>,
        is_enabled: Option<bool>,
        page: u32,
        page_size: u32,
    ) -> Result<Option<(Vec<AdminBillingCollectorRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (
            api_format,
            task_type,
            dimension_name,
            is_enabled,
            page,
            page_size,
        );

        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            let mut items = store
                .lock()
                .expect("admin billing collector store should lock")
                .values()
                .filter(|record| {
                    api_format.is_none_or(|expected| record.api_format == expected)
                        && task_type.is_none_or(|expected| record.task_type == expected)
                        && dimension_name.is_none_or(|expected| record.dimension_name == expected)
                        && is_enabled.is_none_or(|expected| record.is_enabled == expected)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .updated_at_unix_secs
                    .cmp(&left.updated_at_unix_secs)
                    .then_with(|| right.priority.cmp(&left.priority))
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let offset = (page.saturating_sub(1) as usize) * (page_size as usize);
            let items = items
                .into_iter()
                .skip(offset)
                .take(page_size as usize)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn read_admin_billing_collector(
        &self,
        collector_id: &str,
    ) -> Result<Option<AdminBillingCollectorRecord>, GatewayError> {
        #[cfg(not(test))]
        let _ = collector_id;

        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            return Ok(store
                .lock()
                .expect("admin billing collector store should lock")
                .get(collector_id)
                .cloned());
        }

        Ok(None)
    }

    pub(crate) async fn update_admin_billing_collector(
        &self,
        collector_id: &str,
        input: &AdminBillingCollectorWriteInput,
    ) -> Result<LocalMutationOutcome<AdminBillingCollectorRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            let mut guard = store
                .lock()
                .expect("admin billing collector store should lock");
            let Some(record) = guard.get_mut(collector_id) else {
                return Ok(LocalMutationOutcome::NotFound);
            };
            record.api_format = input.api_format.clone();
            record.task_type = input.task_type.clone();
            record.dimension_name = input.dimension_name.clone();
            record.source_type = input.source_type.clone();
            record.source_path = input.source_path.clone();
            record.value_type = input.value_type.clone();
            record.transform_expression = input.transform_expression.clone();
            record.default_value = input.default_value.clone();
            record.priority = input.priority;
            record.is_enabled = input.is_enabled;
            record.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            return Ok(LocalMutationOutcome::Applied(record.clone()));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(LocalMutationOutcome::Unavailable);
        };
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
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
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
        .fetch_optional(&pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(err)) => {
                return Ok(LocalMutationOutcome::Invalid(format!(
                    "Integrity error: {err}"
                )))
            }
            Err(err) => return Err(GatewayError::Internal(err.to_string())),
        };
        match row {
            Some(row) => Ok(LocalMutationOutcome::Applied(
                admin_billing_collector_from_row(&row)?,
            )),
            None => Ok(LocalMutationOutcome::NotFound),
        }
    }

    pub(crate) async fn apply_admin_billing_preset(
        &self,
        preset: &str,
        mode: &str,
        collectors: &[AdminBillingCollectorWriteInput],
    ) -> Result<LocalMutationOutcome<AdminBillingPresetApplyResult>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_billing_collector_store.as_ref() {
            let mut created = 0_u64;
            let mut updated = 0_u64;
            let mut skipped = 0_u64;
            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let mut guard = store
                .lock()
                .expect("admin billing collector store should lock");
            for collector in collectors {
                let existing_id = guard
                    .values()
                    .find(|record| {
                        record.api_format == collector.api_format
                            && record.task_type == collector.task_type
                            && record.dimension_name == collector.dimension_name
                            && record.priority == collector.priority
                            && record.is_enabled
                    })
                    .map(|record| record.id.clone());

                match existing_id {
                    Some(existing_id) if mode == "overwrite" => {
                        if let Some(record) = guard.get_mut(&existing_id) {
                            record.source_type = collector.source_type.clone();
                            record.source_path = collector.source_path.clone();
                            record.value_type = collector.value_type.clone();
                            record.transform_expression = collector.transform_expression.clone();
                            record.default_value = collector.default_value.clone();
                            record.is_enabled = collector.is_enabled;
                            record.updated_at_unix_secs = now_unix_secs;
                            updated += 1;
                        } else {
                            skipped += 1;
                        }
                    }
                    Some(_) => {
                        skipped += 1;
                    }
                    None => {
                        let record = AdminBillingCollectorRecord {
                            id: uuid::Uuid::new_v4().to_string(),
                            api_format: collector.api_format.clone(),
                            task_type: collector.task_type.clone(),
                            dimension_name: collector.dimension_name.clone(),
                            source_type: collector.source_type.clone(),
                            source_path: collector.source_path.clone(),
                            value_type: collector.value_type.clone(),
                            transform_expression: collector.transform_expression.clone(),
                            default_value: collector.default_value.clone(),
                            priority: collector.priority,
                            is_enabled: collector.is_enabled,
                            created_at_unix_secs: now_unix_secs,
                            updated_at_unix_secs: now_unix_secs,
                        };
                        guard.insert(record.id.clone(), record);
                        created += 1;
                    }
                }
            }
            return Ok(LocalMutationOutcome::Applied(
                AdminBillingPresetApplyResult {
                    preset: preset.to_string(),
                    mode: mode.to_string(),
                    created,
                    updated,
                    skipped,
                    errors: Vec::new(),
                },
            ));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(LocalMutationOutcome::Unavailable);
        };

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
            .fetch_optional(&pool)
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
                    .execute(&pool)
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
  id,
  api_format,
  task_type,
  dimension_name,
  source_type,
  source_path,
  value_type,
  transform_expression,
  default_value,
  priority,
  is_enabled,
  created_at,
  updated_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  NOW(),
  NOW()
)
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
            .execute(&pool)
            .await
            {
                Ok(_) => created += 1,
                Err(err) => errors.push(format!(
                    "Failed to create collector: api_format={} task_type={} dim={}: {}",
                    collector.api_format, collector.task_type, collector.dimension_name, err
                )),
            }
        }

        Ok(LocalMutationOutcome::Applied(
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

    pub(crate) async fn read_request_candidate_trace(
        &self,
        request_id: &str,
        attempted_only: bool,
    ) -> Result<Option<data::RequestCandidateTrace>, GatewayError> {
        self.data
            .read_request_candidate_trace(request_id, attempted_only)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_announcements(
        &self,
        query: &aether_data::repository::announcements::AnnouncementListQuery,
    ) -> Result<aether_data::repository::announcements::StoredAnnouncementPage, GatewayError> {
        self.data
            .list_announcements(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_announcement_by_id(
        &self,
        announcement_id: &str,
    ) -> Result<Option<aether_data::repository::announcements::StoredAnnouncement>, GatewayError>
    {
        self.data
            .find_announcement_by_id(announcement_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn count_unread_active_announcements(
        &self,
        user_id: &str,
        now_unix_secs: u64,
    ) -> Result<u64, GatewayError> {
        self.data
            .count_unread_active_announcements(user_id, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn create_announcement(
        &self,
        record: aether_data::repository::announcements::CreateAnnouncementRecord,
    ) -> Result<Option<aether_data::repository::announcements::StoredAnnouncement>, GatewayError>
    {
        self.data
            .create_announcement(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_announcement(
        &self,
        record: aether_data::repository::announcements::UpdateAnnouncementRecord,
    ) -> Result<Option<aether_data::repository::announcements::StoredAnnouncement>, GatewayError>
    {
        self.data
            .update_announcement(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_announcement(
        &self,
        announcement_id: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_announcement(announcement_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn mark_announcement_as_read(
        &self,
        user_id: &str,
        announcement_id: &str,
        read_at_unix_secs: u64,
    ) -> Result<bool, GatewayError> {
        self.data
            .mark_announcement_as_read(user_id, announcement_id, read_at_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_request_candidates_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Vec<aether_data::repository::candidates::StoredRequestCandidate>, GatewayError>
    {
        self.data
            .list_request_candidates_by_request_id(request_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_request_candidates_by_provider_id(
        &self,
        provider_id: &str,
        limit: usize,
    ) -> Result<Vec<aether_data::repository::candidates::StoredRequestCandidate>, GatewayError>
    {
        self.data
            .list_request_candidates_by_provider_id(provider_id, limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_decision_trace(
        &self,
        request_id: &str,
        attempted_only: bool,
    ) -> Result<Option<data::DecisionTrace>, GatewayError> {
        self.data
            .read_decision_trace(request_id, attempted_only)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_request_usage_audit(
        &self,
        request_id: &str,
    ) -> Result<Option<usage::RequestUsageAudit>, GatewayError> {
        self.data
            .read_request_usage_audit(request_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_request_usage_by_id(
        &self,
        usage_id: &str,
    ) -> Result<Option<aether_data::repository::usage::StoredRequestUsageAudit>, GatewayError> {
        self.data
            .find_request_usage_by_id(usage_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_provider_usage_since(
        &self,
        provider_id: &str,
        since_unix_secs: u64,
    ) -> Result<aether_data::repository::usage::StoredProviderUsageSummary, GatewayError> {
        self.data
            .summarize_provider_usage_since(provider_id, since_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_usage_audits(
        &self,
        query: &aether_data::repository::usage::UsageAuditListQuery,
    ) -> Result<Vec<aether_data::repository::usage::StoredRequestUsageAudit>, GatewayError> {
        self.data
            .list_usage_audits(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_recent_usage_audits(
        &self,
        user_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<aether_data::repository::usage::StoredRequestUsageAudit>, GatewayError> {
        self.data
            .list_recent_usage_audits(user_id, limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_usage_total_tokens_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<std::collections::BTreeMap<String, u64>, GatewayError> {
        self.data
            .summarize_usage_total_tokens_by_api_key_ids(api_key_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_users_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<aether_data::repository::users::StoredUserSummary>, GatewayError> {
        self.data
            .list_users_by_ids(user_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_user_auth_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            if let Some(user) = store
                .lock()
                .expect("auth user store should lock")
                .get(user_id)
                .cloned()
            {
                return Ok(Some(user));
            }
        }
        self.data
            .find_user_auth_by_id(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_user_auth_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let identifier = identifier.trim();
            if !identifier.is_empty() {
                if let Some(user) = store
                    .lock()
                    .expect("auth user store should lock")
                    .values()
                    .find(|user| {
                        user.username == identifier || user.email.as_deref() == Some(identifier)
                    })
                    .cloned()
                {
                    return Ok(Some(user));
                }
            }
        }
        self.data
            .find_user_auth_by_identifier(identifier)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn is_other_user_auth_email_taken(
        &self,
        email: &str,
        user_id: &str,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            if store
                .lock()
                .expect("auth user store should lock")
                .values()
                .any(|user| user.id != user_id && user.email.as_deref() == Some(email))
            {
                return Ok(true);
            }
        }

        self.data
            .is_other_user_auth_email_taken(email, user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn is_other_user_auth_username_taken(
        &self,
        username: &str,
        user_id: &str,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            if store
                .lock()
                .expect("auth user store should lock")
                .values()
                .any(|user| user.id != user_id && user.username == username)
            {
                return Ok(true);
            }
        }

        self.data
            .is_other_user_auth_username_taken(username, user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_local_auth_user_profile(
        &self,
        user_id: &str,
        email: Option<String>,
        username: Option<String>,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let existing = {
                store
                    .lock()
                    .expect("auth user store should lock")
                    .get(user_id)
                    .cloned()
            };
            let existing = match existing {
                Some(user) => Some(user),
                None => self
                    .data
                    .find_user_auth_by_id(user_id)
                    .await
                    .map_err(|err| GatewayError::Internal(err.to_string()))?,
            };
            let Some(mut user) = existing else {
                return Ok(None);
            };
            if let Some(email) = email {
                user.email = Some(email);
            }
            if let Some(username) = username {
                user.username = username;
            }
            store
                .lock()
                .expect("auth user store should lock")
                .insert(user.id.clone(), user.clone());
            return Ok(Some(user));
        }

        self.data
            .update_local_auth_user_profile(user_id, email, username)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_local_auth_user_password_hash(
        &self,
        user_id: &str,
        password_hash: String,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let existing = {
                store
                    .lock()
                    .expect("auth user store should lock")
                    .get(user_id)
                    .cloned()
            };
            let existing = match existing {
                Some(user) => Some(user),
                None => self
                    .data
                    .find_user_auth_by_id(user_id)
                    .await
                    .map_err(|err| GatewayError::Internal(err.to_string()))?,
            };
            let Some(mut user) = existing else {
                return Ok(None);
            };
            user.password_hash = Some(password_hash);
            store
                .lock()
                .expect("auth user store should lock")
                .insert(user.id.clone(), user.clone());
            return Ok(Some(user));
        }

        self.data
            .update_local_auth_user_password_hash(user_id, password_hash, updated_at)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn create_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let now = chrono::Utc::now();
            let user = aether_data::repository::users::StoredUserAuthRecord::new(
                uuid::Uuid::new_v4().to_string(),
                email,
                email_verified,
                username,
                Some(password_hash),
                "user".to_string(),
                "local".to_string(),
                None,
                None,
                None,
                true,
                false,
                Some(now),
                None,
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            store
                .lock()
                .expect("auth user store should lock")
                .insert(user.id.clone(), user.clone());
            return Ok(Some(user));
        }

        self.data
            .create_local_auth_user(email, email_verified, username, password_hash)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_local_auth_user_with_settings(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        role: String,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models: Option<Vec<String>>,
        rate_limit: Option<i32>,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let now = chrono::Utc::now();
            let user = aether_data::repository::users::StoredUserAuthRecord::new(
                uuid::Uuid::new_v4().to_string(),
                email,
                email_verified,
                username,
                Some(password_hash),
                role,
                "local".to_string(),
                allowed_providers.map(serde_json::Value::from),
                allowed_api_formats.map(serde_json::Value::from),
                allowed_models.map(serde_json::Value::from),
                true,
                false,
                Some(now),
                None,
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            store
                .lock()
                .expect("auth user store should lock")
                .insert(user.id.clone(), user.clone());
            let _ = rate_limit;
            return Ok(Some(user));
        }

        self.data
            .create_local_auth_user_with_settings(
                email,
                email_verified,
                username,
                password_hash,
                role,
                allowed_providers,
                allowed_api_formats,
                allowed_models,
                rate_limit,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_local_auth_user_admin_fields(
        &self,
        user_id: &str,
        role: Option<String>,
        allowed_providers_present: bool,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats_present: bool,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models_present: bool,
        allowed_models: Option<Vec<String>>,
        rate_limit: Option<i32>,
        is_active: Option<bool>,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let mut guard = store.lock().expect("auth user store should lock");
            let Some(user) = guard.get_mut(user_id) else {
                return Ok(None);
            };
            if let Some(role) = role {
                user.role = role;
            }
            if allowed_providers_present {
                user.allowed_providers = allowed_providers;
            }
            if allowed_api_formats_present {
                user.allowed_api_formats = allowed_api_formats;
            }
            if allowed_models_present {
                user.allowed_models = allowed_models;
            }
            if let Some(is_active) = is_active {
                user.is_active = is_active;
            }
            let _ = rate_limit;
            return Ok(Some(user.clone()));
        }

        self.data
            .update_local_auth_user_admin_fields(
                user_id,
                role,
                allowed_providers_present,
                allowed_providers,
                allowed_api_formats_present,
                allowed_api_formats,
                allowed_models_present,
                allowed_models,
                rate_limit,
                is_active,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn touch_auth_user_last_login(
        &self,
        user_id: &str,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let mut guard = store.lock().expect("auth user store should lock");
            if let Some(user) = guard.get_mut(user_id) {
                user.last_login_at = Some(logged_in_at);
                return Ok(true);
            }
            return Ok(false);
        }

        self.data
            .touch_auth_user_last_login(user_id, logged_in_at)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_user_model_capability_settings(
        &self,
        user_id: &str,
    ) -> Result<Option<serde_json::Value>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_model_capability_store.as_ref() {
            if let Some(settings) = store
                .lock()
                .expect("auth user model capability store should lock")
                .get(user_id)
                .cloned()
            {
                return Ok(Some(settings));
            }
        }

        let users = self.list_non_admin_export_users().await?;
        Ok(users
            .into_iter()
            .find(|user| user.id == user_id)
            .and_then(|user| user.model_capability_settings))
    }

    pub(crate) async fn update_user_model_capability_settings(
        &self,
        user_id: &str,
        settings: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_model_capability_store.as_ref() {
            let mut guard = store
                .lock()
                .expect("auth user model capability store should lock");
            match settings {
                Some(value) => {
                    guard.insert(user_id.to_string(), value.clone());
                    return Ok(Some(value));
                }
                None => {
                    guard.remove(user_id);
                    return Ok(None);
                }
            }
        }

        self.data
            .update_user_model_capability_settings(user_id, settings)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_user_preferences(
        &self,
        user_id: &str,
    ) -> Result<Option<data::StoredUserPreferenceRecord>, GatewayError> {
        self.data
            .read_user_preferences(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn write_user_preferences(
        &self,
        preferences: &data::StoredUserPreferenceRecord,
    ) -> Result<Option<data::StoredUserPreferenceRecord>, GatewayError> {
        self.data
            .write_user_preferences(preferences)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_active_provider_name(
        &self,
        provider_id: &str,
    ) -> Result<Option<String>, GatewayError> {
        self.data
            .find_active_provider_name(provider_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn get_or_create_ldap_auth_user(
        &self,
        email: String,
        username: String,
        ldap_dn: Option<String>,
        ldap_username: Option<String>,
        logged_in_at: chrono::DateTime<chrono::Utc>,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        #[cfg(test)]
        if let (Some(user_store), Some(wallet_store)) = (
            self.auth_user_store.as_ref(),
            self.auth_wallet_store.as_ref(),
        ) {
            let mut users = user_store.lock().expect("auth user store should lock");
            let existing_id = users
                .values()
                .find(|user| {
                    user.email.as_deref() == Some(email.as_str())
                        || user.username == username
                        || ldap_username
                            .as_deref()
                            .is_some_and(|value| user.username == value)
                })
                .map(|user| user.id.clone());

            if let Some(existing_id) = existing_id {
                let Some(user) = users.get_mut(&existing_id) else {
                    return Ok(None);
                };
                if user.is_deleted || !user.is_active {
                    return Ok(None);
                }
                if !user.auth_source.eq_ignore_ascii_case("ldap") {
                    return Ok(None);
                }
                user.email = Some(email);
                user.email_verified = true;
                user.last_login_at = Some(logged_in_at);
                return Ok(Some(user.clone()));
            }

            let base_username = ldap_username
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(username.as_str())
                .trim()
                .to_string();
            let mut candidate_username = base_username.clone();
            while users
                .values()
                .any(|user| user.username == candidate_username)
            {
                let suffix = uuid::Uuid::new_v4().simple().to_string();
                candidate_username = format!(
                    "{}_ldap_{}{}",
                    base_username,
                    logged_in_at.timestamp(),
                    &suffix[..4]
                );
            }

            let user = aether_data::repository::users::StoredUserAuthRecord::new(
                uuid::Uuid::new_v4().to_string(),
                Some(email),
                true,
                candidate_username,
                None,
                "user".to_string(),
                "ldap".to_string(),
                None,
                None,
                None,
                true,
                false,
                Some(logged_in_at),
                Some(logged_in_at),
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            users.insert(user.id.clone(), user.clone());
            drop(users);

            let gift_balance = if unlimited {
                0.0
            } else {
                initial_gift_usd.max(0.0)
            };
            let wallet = aether_data::repository::wallet::StoredWalletSnapshot::new(
                uuid::Uuid::new_v4().to_string(),
                Some(user.id.clone()),
                None,
                0.0,
                gift_balance,
                if unlimited {
                    "unlimited".to_string()
                } else {
                    "finite".to_string()
                },
                "USD".to_string(),
                "active".to_string(),
                0.0,
                0.0,
                0.0,
                gift_balance,
                logged_in_at.timestamp(),
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .insert(wallet.id.clone(), wallet);
            let _ = ldap_dn;
            return Ok(Some(user));
        }

        self.data
            .get_or_create_ldap_auth_user(
                email,
                username,
                ldap_dn,
                ldap_username,
                logged_in_at,
                initial_gift_usd,
                unlimited,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn initialize_auth_user_wallet(
        &self,
        user_id: &str,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<Option<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_wallet_store.as_ref() {
            let gift_balance = if unlimited {
                0.0
            } else {
                initial_gift_usd.max(0.0)
            };
            let now_unix_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let wallet = aether_data::repository::wallet::StoredWalletSnapshot::new(
                uuid::Uuid::new_v4().to_string(),
                Some(user_id.to_string()),
                None,
                0.0,
                gift_balance,
                if unlimited {
                    "unlimited".to_string()
                } else {
                    "finite".to_string()
                },
                "USD".to_string(),
                "active".to_string(),
                0.0,
                0.0,
                0.0,
                gift_balance,
                now_unix_secs,
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            store
                .lock()
                .expect("auth wallet store should lock")
                .insert(wallet.id.clone(), wallet.clone());
            return Ok(Some(wallet));
        }

        self.data
            .initialize_auth_user_wallet(user_id, initial_gift_usd, unlimited)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_auth_user_wallet_limit_mode(
        &self,
        user_id: &str,
        limit_mode: &str,
    ) -> Result<Option<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_wallet_store.as_ref() {
            let mut guard = store.lock().expect("auth wallet store should lock");
            let Some((wallet_id, wallet)) = guard
                .iter_mut()
                .find(|(_, wallet)| wallet.user_id.as_deref() == Some(user_id))
            else {
                return Ok(None);
            };
            let _ = wallet_id;
            wallet.limit_mode = limit_mode.to_string();
            wallet.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            return Ok(Some(wallet.clone()));
        }

        self.data
            .update_auth_user_wallet_limit_mode(user_id, limit_mode)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn admin_adjust_wallet_balance(
        &self,
        wallet_id: &str,
        amount_usd: f64,
        balance_type: &str,
        operator_id: Option<&str>,
        description: Option<&str>,
    ) -> Result<
        Option<(
            aether_data::repository::wallet::StoredWalletSnapshot,
            AdminWalletTransactionRecord,
        )>,
        GatewayError,
    > {
        #[cfg(test)]
        if let Some(store) = self.auth_wallet_store.as_ref() {
            let mut guard = store.lock().expect("auth wallet store should lock");
            let Some(wallet) = guard.get_mut(wallet_id) else {
                return Ok(None);
            };

            let before_recharge = wallet.balance;
            let before_gift = wallet.gift_balance;
            let before_total = before_recharge + before_gift;
            let mut after_recharge = before_recharge;
            let mut after_gift = before_gift;

            if amount_usd > 0.0 {
                if balance_type.eq_ignore_ascii_case("gift") {
                    after_gift += amount_usd;
                } else {
                    after_recharge += amount_usd;
                }
            } else {
                let mut remaining = -amount_usd;
                let consume_positive_bucket = |balance: &mut f64, to_consume: &mut f64| {
                    if *to_consume <= 0.0 {
                        return;
                    }
                    let available = (*balance).max(0.0);
                    let consumed = available.min(*to_consume);
                    *balance -= consumed;
                    *to_consume -= consumed;
                };
                if balance_type.eq_ignore_ascii_case("gift") {
                    consume_positive_bucket(&mut after_gift, &mut remaining);
                    consume_positive_bucket(&mut after_recharge, &mut remaining);
                } else {
                    consume_positive_bucket(&mut after_recharge, &mut remaining);
                    consume_positive_bucket(&mut after_gift, &mut remaining);
                }
                if remaining > 0.0 {
                    after_recharge -= remaining;
                }
            }

            wallet.balance = after_recharge;
            wallet.gift_balance = after_gift;
            wallet.total_adjusted += amount_usd;
            wallet.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;

            let transaction = AdminWalletTransactionRecord {
                id: uuid::Uuid::new_v4().to_string(),
                wallet_id: wallet.id.clone(),
                category: "adjust".to_string(),
                reason_code: "adjust_admin".to_string(),
                amount: amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + after_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: after_gift,
                link_type: Some("admin_action".to_string()),
                link_id: Some(wallet.id.clone()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some(
                    description
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or("管理员调账")
                        .to_string(),
                ),
                created_at_unix_secs: chrono::Utc::now().timestamp().max(0) as u64,
            };
            return Ok(Some((wallet.clone(), transaction)));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(None);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(row) = sqlx::query(
            r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted
FROM wallets
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(None);
        };

        let before_recharge = row
            .try_get::<f64, _>("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_gift = row
            .try_get::<f64, _>("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_total = before_recharge + before_gift;
        let mut after_recharge = before_recharge;
        let mut after_gift = before_gift;

        if amount_usd > 0.0 {
            if balance_type.eq_ignore_ascii_case("gift") {
                after_gift += amount_usd;
            } else {
                after_recharge += amount_usd;
            }
        } else {
            let mut remaining = -amount_usd;
            let consume_positive_bucket = |balance: &mut f64, to_consume: &mut f64| {
                if *to_consume <= 0.0 {
                    return;
                }
                let available = (*balance).max(0.0);
                let consumed = available.min(*to_consume);
                *balance -= consumed;
                *to_consume -= consumed;
            };
            if balance_type.eq_ignore_ascii_case("gift") {
                consume_positive_bucket(&mut after_gift, &mut remaining);
                consume_positive_bucket(&mut after_recharge, &mut remaining);
            } else {
                consume_positive_bucket(&mut after_recharge, &mut remaining);
                consume_positive_bucket(&mut after_gift, &mut remaining);
            }
            if remaining > 0.0 {
                after_recharge -= remaining;
            }
        }

        let wallet_row = sqlx::query(
            r#"
UPDATE wallets
SET
  balance = $2,
  gift_balance = $3,
  total_adjusted = total_adjusted + $4,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(wallet_id)
        .bind(after_recharge)
        .bind(after_gift)
        .bind(amount_usd)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wallet = admin_wallet_snapshot_from_row(&wallet_row)?;

        let transaction_id = uuid::Uuid::new_v4().to_string();
        let created_at = chrono::Utc::now().timestamp().max(0) as u64;
        sqlx::query(
            r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'adjust',
  'adjust_admin',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'admin_action',
  $10,
  $11,
  $12,
  NOW()
)
            "#,
        )
        .bind(&transaction_id)
        .bind(wallet_id)
        .bind(amount_usd)
        .bind(before_total)
        .bind(after_recharge + after_gift)
        .bind(before_recharge)
        .bind(after_recharge)
        .bind(before_gift)
        .bind(after_gift)
        .bind(wallet_id)
        .bind(operator_id)
        .bind(
            description
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("管理员调账"),
        )
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;

        Ok(Some((
            wallet,
            AdminWalletTransactionRecord {
                id: transaction_id,
                wallet_id: wallet_id.to_string(),
                category: "adjust".to_string(),
                reason_code: "adjust_admin".to_string(),
                amount: amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + after_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: after_gift,
                link_type: Some("admin_action".to_string()),
                link_id: Some(wallet_id.to_string()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some(
                    description
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or("管理员调账")
                        .to_string(),
                ),
                created_at_unix_secs: created_at,
            },
        )))
    }

    pub(crate) async fn admin_create_manual_wallet_recharge(
        &self,
        wallet_id: &str,
        amount_usd: f64,
        payment_method: &str,
        operator_id: Option<&str>,
        description: Option<&str>,
    ) -> Result<
        Option<(
            aether_data::repository::wallet::StoredWalletSnapshot,
            AdminWalletPaymentOrderRecord,
        )>,
        GatewayError,
    > {
        #[cfg(test)]
        if let Some(store) = self.auth_wallet_store.as_ref() {
            let mut guard = store.lock().expect("auth wallet store should lock");
            let Some(wallet) = guard.get_mut(wallet_id) else {
                return Ok(None);
            };
            wallet.balance += amount_usd;
            wallet.total_recharged += amount_usd;
            wallet.updated_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let now = chrono::Utc::now();
            let created_at = now.timestamp().max(0) as u64;
            let order = AdminWalletPaymentOrderRecord {
                id: uuid::Uuid::new_v4().to_string(),
                order_no: admin_wallet_build_order_no(now),
                wallet_id: wallet.id.clone(),
                user_id: wallet.user_id.clone(),
                amount_usd,
                pay_amount: None,
                pay_currency: None,
                exchange_rate: None,
                refunded_amount_usd: 0.0,
                refundable_amount_usd: amount_usd,
                payment_method: payment_method.to_string(),
                gateway_order_id: None,
                status: "credited".to_string(),
                gateway_response: Some(serde_json::json!({
                    "source": "manual",
                    "operator_id": operator_id,
                    "description": description,
                })),
                created_at_unix_secs: created_at,
                paid_at_unix_secs: Some(created_at),
                credited_at_unix_secs: Some(created_at),
                expires_at_unix_secs: None,
            };
            return Ok(Some((wallet.clone(), order)));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(None);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(wallet_row) = sqlx::query(
            r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted
FROM wallets
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(None);
        };

        let before_recharge = wallet_row
            .try_get::<f64, _>("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_gift = wallet_row
            .try_get::<f64, _>("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let user_id = wallet_row
            .try_get::<Option<String>, _>("user_id")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let now = chrono::Utc::now();
        let created_at = now.timestamp().max(0) as u64;
        let order_id = uuid::Uuid::new_v4().to_string();
        let order_no = admin_wallet_build_order_no(now);
        let gateway_response = serde_json::json!({
            "source": "manual",
            "operator_id": operator_id,
            "description": description,
        });

        sqlx::query(
            r#"
INSERT INTO payment_orders (
  id,
  order_no,
  wallet_id,
  user_id,
  amount_usd,
  refunded_amount_usd,
  refundable_amount_usd,
  payment_method,
  status,
  gateway_response,
  created_at,
  paid_at,
  credited_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  0,
  $5,
  $6,
  'credited',
  $7,
  NOW(),
  NOW(),
  NOW()
)
            "#,
        )
        .bind(&order_id)
        .bind(&order_no)
        .bind(wallet_id)
        .bind(user_id.as_deref())
        .bind(amount_usd)
        .bind(payment_method)
        .bind(&gateway_response)
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        let after_recharge = before_recharge + amount_usd;
        let wallet_row = sqlx::query(
            r#"
UPDATE wallets
SET
  balance = $2,
  total_recharged = total_recharged + $3,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(wallet_id)
        .bind(after_recharge)
        .bind(amount_usd)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wallet = admin_wallet_snapshot_from_row(&wallet_row)?;

        let reason_code = if matches!(payment_method, "card_code" | "gift_code" | "card_recharge") {
            "topup_card_code"
        } else {
            "topup_admin_manual"
        };
        sqlx::query(
            r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'recharge',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $9,
  'payment_order',
  $10,
  $11,
  $12,
  NOW()
)
            "#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(wallet_id)
        .bind(reason_code)
        .bind(amount_usd)
        .bind(before_recharge + before_gift)
        .bind(after_recharge + before_gift)
        .bind(before_recharge)
        .bind(after_recharge)
        .bind(before_gift)
        .bind(&order_id)
        .bind(operator_id)
        .bind(
            description
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("管理员充值"),
        )
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;

        Ok(Some((
            wallet,
            AdminWalletPaymentOrderRecord {
                id: order_id,
                order_no,
                wallet_id: wallet_id.to_string(),
                user_id,
                amount_usd,
                pay_amount: None,
                pay_currency: None,
                exchange_rate: None,
                refunded_amount_usd: 0.0,
                refundable_amount_usd: amount_usd,
                payment_method: payment_method.to_string(),
                gateway_order_id: None,
                status: "credited".to_string(),
                gateway_response: Some(gateway_response),
                created_at_unix_secs: created_at,
                paid_at_unix_secs: Some(created_at),
                credited_at_unix_secs: Some(created_at),
                expires_at_unix_secs: None,
            },
        )))
    }

    pub(crate) async fn list_admin_payment_orders(
        &self,
        status: Option<&str>,
        payment_method: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Option<(Vec<AdminWalletPaymentOrderRecord>, u64)>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_wallet_payment_order_store.as_ref() {
            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let mut items = store
                .lock()
                .expect("admin wallet payment order store should lock")
                .values()
                .filter(|order| {
                    payment_method.is_none_or(|expected| order.payment_method == expected)
                        && status.is_none_or(|expected| {
                            let effective_status = if order.status == "pending"
                                && order
                                    .expires_at_unix_secs
                                    .is_some_and(|value| value < now_unix_secs)
                            {
                                "expired"
                            } else {
                                order.status.as_str()
                            };
                            effective_status == expected
                        })
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(None);
        };
        let count_row = sqlx::query(
            r#"
SELECT COUNT(*) AS total
FROM payment_orders
WHERE ($1::TEXT IS NULL OR payment_method = $1)
  AND (
    $2::TEXT IS NULL
    OR (
      CASE
        WHEN status = 'pending' AND expires_at IS NOT NULL AND expires_at < NOW() THEN 'expired'
        ELSE status
      END
    ) = $2
  )
            "#,
        )
        .bind(payment_method)
        .bind(status)
        .fetch_one(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let total = count_row
            .try_get::<i64, _>("total")
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .max(0) as u64;
        let rows = sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE ($1::TEXT IS NULL OR payment_method = $1)
  AND (
    $2::TEXT IS NULL
    OR (
      CASE
        WHEN status = 'pending' AND expires_at IS NOT NULL AND expires_at < NOW() THEN 'expired'
        ELSE status
      END
    ) = $2
  )
ORDER BY created_at DESC
OFFSET $3
LIMIT $4
            "#,
        )
        .bind(payment_method)
        .bind(status)
        .bind(i64::try_from(offset).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .bind(i64::try_from(limit).map_err(|err| GatewayError::Internal(err.to_string()))?)
        .fetch_all(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let items = rows
            .iter()
            .map(admin_wallet_payment_order_from_row)
            .collect::<Result<Vec<_>, GatewayError>>()?;
        Ok(Some((items, total)))
    }

    pub(crate) async fn list_admin_payment_callbacks(
        &self,
        payment_method: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Option<(Vec<AdminPaymentCallbackRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (payment_method, limit, offset);

        #[cfg(test)]
        if let Some(store) = self.admin_payment_callback_store.as_ref() {
            let mut items = store
                .lock()
                .expect("admin payment callback store should lock")
                .values()
                .filter(|callback| {
                    payment_method.is_none_or(|expected| callback.payment_method == expected)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn list_admin_wallet_transactions(
        &self,
        wallet_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Option<(Vec<AdminWalletTransactionRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (wallet_id, limit, offset);

        #[cfg(test)]
        if let Some(store) = self.admin_wallet_transaction_store.as_ref() {
            let mut items = store
                .lock()
                .expect("admin wallet transaction store should lock")
                .values()
                .filter(|transaction| transaction.wallet_id == wallet_id)
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn list_admin_wallet_refunds(
        &self,
        wallet_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Option<(Vec<AdminWalletRefundRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (wallet_id, limit, offset);

        #[cfg(test)]
        if let Some(store) = self.admin_wallet_refund_store.as_ref() {
            let mut items = store
                .lock()
                .expect("admin wallet refund store should lock")
                .values()
                .filter(|refund| refund.wallet_id == wallet_id)
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn list_admin_wallet_refund_requests(
        &self,
        status: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Option<(Vec<AdminWalletRefundRecord>, u64)>, GatewayError> {
        #[cfg(not(test))]
        let _ = (status, limit, offset);

        #[cfg(test)]
        if let (Some(wallet_store), Some(refund_store)) = (
            self.auth_wallet_store.as_ref(),
            self.admin_wallet_refund_store.as_ref(),
        ) {
            let wallets = wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .clone();
            let mut items = refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .values()
                .filter(|refund| status.is_none_or(|expected| refund.status == expected))
                .filter(|refund| {
                    wallets
                        .get(&refund.wallet_id)
                        .is_some_and(|wallet| wallet.user_id.is_some())
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by(|left, right| {
                right
                    .created_at_unix_secs
                    .cmp(&left.created_at_unix_secs)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>();
            return Ok(Some((items, total)));
        }

        Ok(None)
    }

    pub(crate) async fn read_admin_payment_order(
        &self,
        order_id: &str,
    ) -> Result<AdminWalletMutationOutcome<AdminWalletPaymentOrderRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_wallet_payment_order_store.as_ref() {
            return Ok(store
                .lock()
                .expect("admin wallet payment order store should lock")
                .get(order_id)
                .cloned()
                .map(AdminWalletMutationOutcome::Applied)
                .unwrap_or(AdminWalletMutationOutcome::NotFound));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let row = sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE id = $1
LIMIT 1
            "#,
        )
        .bind(order_id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        match row {
            Some(row) => Ok(AdminWalletMutationOutcome::Applied(
                admin_wallet_payment_order_from_row(&row)?,
            )),
            None => Ok(AdminWalletMutationOutcome::NotFound),
        }
    }

    pub(crate) async fn admin_expire_payment_order(
        &self,
        order_id: &str,
    ) -> Result<AdminWalletMutationOutcome<(AdminWalletPaymentOrderRecord, bool)>, GatewayError>
    {
        #[cfg(test)]
        if let Some(store) = self.admin_wallet_payment_order_store.as_ref() {
            let mut guard = store
                .lock()
                .expect("admin wallet payment order store should lock");
            let Some(order) = guard.get_mut(order_id) else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            if order.status == "credited" {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "credited order cannot be expired".to_string(),
                ));
            }
            if order.status == "expired" {
                return Ok(AdminWalletMutationOutcome::Applied((order.clone(), false)));
            }
            if order.status != "pending" {
                return Ok(AdminWalletMutationOutcome::Invalid(format!(
                    "only pending order can be expired: {}",
                    order.status
                )));
            }
            let mut gateway_response =
                admin_payment_gateway_response_map(order.gateway_response.take());
            gateway_response.insert(
                "expire_reason".to_string(),
                serde_json::Value::String("admin_mark_expired".to_string()),
            );
            gateway_response.insert(
                "expired_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
            order.status = "expired".to_string();
            order.gateway_response = Some(serde_json::Value::Object(gateway_response));
            return Ok(AdminWalletMutationOutcome::Applied((order.clone(), true)));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(row) = sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let order = admin_wallet_payment_order_from_row(&row)?;
        if order.status == "credited" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "credited order cannot be expired".to_string(),
            ));
        }
        if order.status == "expired" {
            tx.commit()
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            return Ok(AdminWalletMutationOutcome::Applied((order, false)));
        }
        if order.status != "pending" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(format!(
                "only pending order can be expired: {}",
                order.status
            )));
        }
        let mut gateway_response =
            admin_payment_gateway_response_map(order.gateway_response.clone());
        gateway_response.insert(
            "expire_reason".to_string(),
            serde_json::Value::String("admin_mark_expired".to_string()),
        );
        gateway_response.insert(
            "expired_at".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );
        let row = sqlx::query(
            r#"
UPDATE payment_orders
SET
  status = 'expired',
  gateway_response = $2
WHERE id = $1
RETURNING
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
            "#,
        )
        .bind(order_id)
        .bind(serde_json::Value::Object(gateway_response))
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok(AdminWalletMutationOutcome::Applied((
            admin_wallet_payment_order_from_row(&row)?,
            true,
        )))
    }

    pub(crate) async fn admin_fail_payment_order(
        &self,
        order_id: &str,
    ) -> Result<AdminWalletMutationOutcome<AdminWalletPaymentOrderRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.admin_wallet_payment_order_store.as_ref() {
            let mut guard = store
                .lock()
                .expect("admin wallet payment order store should lock");
            let Some(order) = guard.get_mut(order_id) else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            if order.status == "credited" {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "credited order cannot be failed".to_string(),
                ));
            }
            let mut gateway_response =
                admin_payment_gateway_response_map(order.gateway_response.take());
            gateway_response.insert(
                "failure_reason".to_string(),
                serde_json::Value::String("admin_mark_failed".to_string()),
            );
            gateway_response.insert(
                "failed_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
            order.status = "failed".to_string();
            order.gateway_response = Some(serde_json::Value::Object(gateway_response));
            return Ok(AdminWalletMutationOutcome::Applied(order.clone()));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(row) = sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let order = admin_wallet_payment_order_from_row(&row)?;
        if order.status == "credited" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "credited order cannot be failed".to_string(),
            ));
        }
        let mut gateway_response =
            admin_payment_gateway_response_map(order.gateway_response.clone());
        gateway_response.insert(
            "failure_reason".to_string(),
            serde_json::Value::String("admin_mark_failed".to_string()),
        );
        gateway_response.insert(
            "failed_at".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );
        let row = sqlx::query(
            r#"
UPDATE payment_orders
SET
  status = 'failed',
  gateway_response = $2
WHERE id = $1
RETURNING
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
            "#,
        )
        .bind(order_id)
        .bind(serde_json::Value::Object(gateway_response))
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok(AdminWalletMutationOutcome::Applied(
            admin_wallet_payment_order_from_row(&row)?,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn admin_credit_payment_order(
        &self,
        order_id: &str,
        gateway_order_id: Option<&str>,
        pay_amount: Option<f64>,
        pay_currency: Option<&str>,
        exchange_rate: Option<f64>,
        gateway_response_patch: Option<serde_json::Value>,
        operator_id: Option<&str>,
    ) -> Result<AdminWalletMutationOutcome<(AdminWalletPaymentOrderRecord, bool)>, GatewayError>
    {
        #[cfg(test)]
        if let (Some(order_store), Some(wallet_store)) = (
            self.admin_wallet_payment_order_store.as_ref(),
            self.auth_wallet_store.as_ref(),
        ) {
            let mut orders = order_store
                .lock()
                .expect("admin wallet payment order store should lock");
            let Some(order) = orders.get_mut(order_id) else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            if order.status == "credited" {
                return Ok(AdminWalletMutationOutcome::Applied((order.clone(), false)));
            }
            if matches!(order.status.as_str(), "failed" | "expired" | "refunded") {
                return Ok(AdminWalletMutationOutcome::Invalid(format!(
                    "payment order is not creditable: {}",
                    order.status
                )));
            }
            if order
                .expires_at_unix_secs
                .is_some_and(|value| value < chrono::Utc::now().timestamp().max(0) as u64)
            {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "payment order expired".to_string(),
                ));
            }

            let mut wallets = wallet_store.lock().expect("auth wallet store should lock");
            let Some(wallet) = wallets.get_mut(&order.wallet_id) else {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "wallet not found".to_string(),
                ));
            };
            if wallet.status != "active" {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "wallet is not active".to_string(),
                ));
            }

            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let mut gateway_response =
                admin_payment_gateway_response_map(order.gateway_response.take());
            if let Some(serde_json::Value::Object(map)) = gateway_response_patch {
                gateway_response.extend(map);
            }
            gateway_response.insert("manual_credit".to_string(), serde_json::Value::Bool(true));
            gateway_response.insert(
                "credited_by".to_string(),
                operator_id
                    .map(|value| serde_json::Value::String(value.to_string()))
                    .unwrap_or(serde_json::Value::Null),
            );

            wallet.balance += order.amount_usd;
            wallet.total_recharged += order.amount_usd;
            wallet.updated_at_unix_secs = now_unix_secs;

            if let Some(value) = gateway_order_id {
                order.gateway_order_id = Some(value.to_string());
            }
            if let Some(value) = pay_amount {
                order.pay_amount = Some(value);
            }
            if let Some(value) = pay_currency {
                order.pay_currency = Some(value.to_string());
            }
            if let Some(value) = exchange_rate {
                order.exchange_rate = Some(value);
            }
            order.status = "credited".to_string();
            order.paid_at_unix_secs = order.paid_at_unix_secs.or(Some(now_unix_secs));
            order.credited_at_unix_secs = Some(now_unix_secs);
            order.refundable_amount_usd = order.amount_usd;
            order.gateway_response = Some(serde_json::Value::Object(gateway_response));
            return Ok(AdminWalletMutationOutcome::Applied((order.clone(), true)));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(order_row) = sqlx::query(
            r#"
SELECT
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
FROM payment_orders
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let order = admin_wallet_payment_order_from_row(&order_row)?;
        if order.status == "credited" {
            tx.commit()
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            return Ok(AdminWalletMutationOutcome::Applied((order, false)));
        }
        if matches!(order.status.as_str(), "failed" | "expired" | "refunded") {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(format!(
                "payment order is not creditable: {}",
                order.status
            )));
        }
        if order
            .expires_at_unix_secs
            .is_some_and(|value| value < chrono::Utc::now().timestamp().max(0) as u64)
        {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "payment order expired".to_string(),
            ));
        }

        let Some(wallet_row) = sqlx::query(
            r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM wallets
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(&order.wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "wallet not found".to_string(),
            ));
        };
        let wallet_status = wallet_row
            .try_get::<String, _>("status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        if wallet_status != "active" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "wallet is not active".to_string(),
            ));
        }

        let before_recharge = wallet_row
            .try_get::<f64, _>("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_gift = wallet_row
            .try_get::<f64, _>("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_total = before_recharge + before_gift;
        let after_recharge = before_recharge + order.amount_usd;
        let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;

        let wallet_row = sqlx::query(
            r#"
UPDATE wallets
SET
  balance = $2,
  total_recharged = total_recharged + $3,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(&order.wallet_id)
        .bind(after_recharge)
        .bind(order.amount_usd)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let _wallet = admin_wallet_snapshot_from_row(&wallet_row)?;

        sqlx::query(
            r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'recharge',
  'topup_gateway',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $8,
  'payment_order',
  $9,
  NULL,
  $10,
  NOW()
)
            "#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&order.wallet_id)
        .bind(order.amount_usd)
        .bind(before_total)
        .bind(after_recharge + before_gift)
        .bind(before_recharge)
        .bind(after_recharge)
        .bind(before_gift)
        .bind(order_id)
        .bind(format!("充值到账({})", order.payment_method))
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        let mut gateway_response =
            admin_payment_gateway_response_map(order.gateway_response.clone());
        if let Some(serde_json::Value::Object(map)) = gateway_response_patch {
            gateway_response.extend(map);
        }
        gateway_response.insert("manual_credit".to_string(), serde_json::Value::Bool(true));
        gateway_response.insert(
            "credited_by".to_string(),
            operator_id
                .map(|value| serde_json::Value::String(value.to_string()))
                .unwrap_or(serde_json::Value::Null),
        );
        let next_gateway_order_id = gateway_order_id
            .map(ToOwned::to_owned)
            .or(order.gateway_order_id.clone());
        let next_pay_amount = pay_amount.or(order.pay_amount);
        let next_pay_currency = pay_currency
            .map(ToOwned::to_owned)
            .or(order.pay_currency.clone());
        let next_exchange_rate = exchange_rate.or(order.exchange_rate);
        let next_paid_at_unix_secs = order.paid_at_unix_secs.or(Some(now_unix_secs));

        let row = sqlx::query(
            r#"
UPDATE payment_orders
SET
  gateway_order_id = $2,
  gateway_response = $3,
  pay_amount = $4,
  pay_currency = $5,
  exchange_rate = $6,
  status = 'credited',
  paid_at = COALESCE(to_timestamp($7), NOW()),
  credited_at = NOW(),
  refundable_amount_usd = amount_usd
WHERE id = $1
RETURNING
  id,
  order_no,
  wallet_id,
  user_id,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  CAST(pay_amount AS DOUBLE PRECISION) AS pay_amount,
  pay_currency,
  CAST(exchange_rate AS DOUBLE PRECISION) AS exchange_rate,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd,
  payment_method,
  gateway_order_id,
  gateway_response,
  status,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM paid_at) AS BIGINT) AS paid_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM credited_at) AS BIGINT) AS credited_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM expires_at) AS BIGINT) AS expires_at_unix_secs
            "#,
        )
        .bind(order_id)
        .bind(next_gateway_order_id)
        .bind(serde_json::Value::Object(gateway_response))
        .bind(next_pay_amount)
        .bind(next_pay_currency)
        .bind(next_exchange_rate)
        .bind(i64::try_from(next_paid_at_unix_secs.unwrap_or(now_unix_secs)).unwrap_or_default())
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok(AdminWalletMutationOutcome::Applied((
            admin_wallet_payment_order_from_row(&row)?,
            true,
        )))
    }

    pub(crate) async fn count_active_admin_users(&self) -> Result<u64, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let total = store
                .lock()
                .expect("auth user store should lock")
                .values()
                .filter(|user| {
                    user.role.eq_ignore_ascii_case("admin") && user.is_active && !user.is_deleted
                })
                .count() as u64;
            return Ok(total);
        }

        self.data
            .count_active_admin_users()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn admin_process_wallet_refund(
        &self,
        wallet_id: &str,
        refund_id: &str,
        operator_id: Option<&str>,
    ) -> Result<
        AdminWalletMutationOutcome<(
            aether_data::repository::wallet::StoredWalletSnapshot,
            AdminWalletRefundRecord,
            AdminWalletTransactionRecord,
        )>,
        GatewayError,
    > {
        #[cfg(test)]
        if let (Some(wallet_store), Some(refund_store)) = (
            self.auth_wallet_store.as_ref(),
            self.admin_wallet_refund_store.as_ref(),
        ) {
            let Some(wallet) = wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .get(wallet_id)
                .cloned()
            else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            let Some(refund) = refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .get(refund_id)
                .filter(|refund| refund.wallet_id == wallet_id)
                .cloned()
            else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            if !matches!(refund.status.as_str(), "approved" | "pending_approval") {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "refund status is not approvable".to_string(),
                ));
            }

            let amount_usd = refund.amount_usd;
            let mut updated_wallet = wallet.clone();
            let before_recharge = updated_wallet.balance;
            let before_gift = updated_wallet.gift_balance;
            let before_total = before_recharge + before_gift;
            let after_recharge = before_recharge - amount_usd;
            if after_recharge < 0.0 {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "refund amount exceeds refundable recharge balance".to_string(),
                ));
            }

            let mut updated_order = None;
            if let Some(payment_order_id) = refund.payment_order_id.clone() {
                let Some(order_store) = self.admin_wallet_payment_order_store.as_ref() else {
                    return Ok(AdminWalletMutationOutcome::Unavailable);
                };
                let Some(order) = order_store
                    .lock()
                    .expect("admin wallet payment order store should lock")
                    .get(&payment_order_id)
                    .cloned()
                else {
                    return Ok(AdminWalletMutationOutcome::Invalid(
                        "payment order not found".to_string(),
                    ));
                };
                if amount_usd > order.refundable_amount_usd {
                    return Ok(AdminWalletMutationOutcome::Invalid(
                        "refund amount exceeds refundable amount".to_string(),
                    ));
                }
                let mut order = order;
                order.refunded_amount_usd += amount_usd;
                order.refundable_amount_usd -= amount_usd;
                updated_order = Some(order);
            }

            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            updated_wallet.balance = after_recharge;
            updated_wallet.total_refunded = (updated_wallet.total_refunded + amount_usd).max(0.0);
            updated_wallet.updated_at_unix_secs = now_unix_secs;

            let transaction = AdminWalletTransactionRecord {
                id: uuid::Uuid::new_v4().to_string(),
                wallet_id: updated_wallet.id.clone(),
                category: "refund".to_string(),
                reason_code: "refund_out".to_string(),
                amount: -amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + before_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: before_gift,
                link_type: Some("refund_request".to_string()),
                link_id: Some(refund.id.clone()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some("退款占款".to_string()),
                created_at_unix_secs: now_unix_secs,
            };

            let mut updated_refund = refund.clone();
            updated_refund.status = "processing".to_string();
            updated_refund.approved_by = operator_id.map(ToOwned::to_owned);
            updated_refund.processed_by = operator_id.map(ToOwned::to_owned);
            updated_refund.processed_at_unix_secs = Some(now_unix_secs);
            updated_refund.updated_at_unix_secs = now_unix_secs;

            wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .insert(updated_wallet.id.clone(), updated_wallet.clone());
            refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .insert(updated_refund.id.clone(), updated_refund.clone());
            if let Some(updated_order) = updated_order {
                self.admin_wallet_payment_order_store
                    .as_ref()
                    .expect("admin wallet payment order store should exist")
                    .lock()
                    .expect("admin wallet payment order store should lock")
                    .insert(updated_order.id.clone(), updated_order);
            }

            return Ok(AdminWalletMutationOutcome::Applied((
                updated_wallet,
                updated_refund,
                transaction,
            )));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;

        let Some(refund_row) = sqlx::query(
            r#"
SELECT
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests
WHERE id = $1 AND wallet_id = $2
FOR UPDATE
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let refund = admin_wallet_refund_from_row(&refund_row)?;
        if !matches!(refund.status.as_str(), "approved" | "pending_approval") {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "refund status is not approvable".to_string(),
            ));
        }

        let Some(wallet_row) = sqlx::query(
            r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM wallets
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "wallet not found".to_string(),
            ));
        };
        let before_recharge = wallet_row
            .try_get::<f64, _>("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_gift = wallet_row
            .try_get::<f64, _>("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_total = before_recharge + before_gift;
        let amount_usd = refund.amount_usd;
        let after_recharge = before_recharge - amount_usd;
        if after_recharge < 0.0 {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "refund amount exceeds refundable recharge balance".to_string(),
            ));
        }

        if let Some(payment_order_id) = refund.payment_order_id.as_deref() {
            let Some(order_row) = sqlx::query(
                r#"
SELECT
  id,
  CAST(refunded_amount_usd AS DOUBLE PRECISION) AS refunded_amount_usd,
  CAST(refundable_amount_usd AS DOUBLE PRECISION) AS refundable_amount_usd
FROM payment_orders
WHERE id = $1
FOR UPDATE
                "#,
            )
            .bind(payment_order_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            else {
                let _ = tx.rollback().await;
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "payment order not found".to_string(),
                ));
            };
            let refundable_amount = order_row
                .try_get::<f64, _>("refundable_amount_usd")
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            if amount_usd > refundable_amount {
                let _ = tx.rollback().await;
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "refund amount exceeds refundable amount".to_string(),
                ));
            }
            sqlx::query(
                r#"
UPDATE payment_orders
SET
  refunded_amount_usd = refunded_amount_usd + $2,
  refundable_amount_usd = refundable_amount_usd - $2
WHERE id = $1
                "#,
            )
            .bind(payment_order_id)
            .bind(amount_usd)
            .execute(&mut *tx)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        }

        let wallet_row = sqlx::query(
            r#"
UPDATE wallets
SET
  balance = $2,
  total_refunded = total_refunded + $3,
  updated_at = NOW()
WHERE id = $1
RETURNING
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(wallet_id)
        .bind(after_recharge)
        .bind(amount_usd)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wallet = admin_wallet_snapshot_from_row(&wallet_row)?;

        let transaction_id = uuid::Uuid::new_v4().to_string();
        let created_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
        sqlx::query(
            r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'refund',
  'refund_out',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'refund_request',
  $10,
  $11,
  '退款占款',
  NOW()
)
            "#,
        )
        .bind(&transaction_id)
        .bind(wallet_id)
        .bind(-amount_usd)
        .bind(before_total)
        .bind(after_recharge + before_gift)
        .bind(before_recharge)
        .bind(after_recharge)
        .bind(before_gift)
        .bind(before_gift)
        .bind(refund_id)
        .bind(operator_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        let refund_row = sqlx::query(
            r#"
UPDATE refund_requests
SET
  status = 'processing',
  approved_by = $3,
  processed_by = $3,
  processed_at = NOW(),
  updated_at = NOW()
WHERE id = $1 AND wallet_id = $2
RETURNING
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .bind(operator_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let refund = admin_wallet_refund_from_row(&refund_row)?;

        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;

        Ok(AdminWalletMutationOutcome::Applied((
            wallet,
            refund,
            AdminWalletTransactionRecord {
                id: transaction_id,
                wallet_id: wallet_id.to_string(),
                category: "refund".to_string(),
                reason_code: "refund_out".to_string(),
                amount: -amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + before_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: before_gift,
                link_type: Some("refund_request".to_string()),
                link_id: Some(refund_id.to_string()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some("退款占款".to_string()),
                created_at_unix_secs,
            },
        )))
    }

    pub(crate) async fn admin_complete_wallet_refund(
        &self,
        wallet_id: &str,
        refund_id: &str,
        gateway_refund_id: Option<&str>,
        payout_reference: Option<&str>,
        payout_proof: Option<serde_json::Value>,
    ) -> Result<AdminWalletMutationOutcome<AdminWalletRefundRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(refund_store) = self.admin_wallet_refund_store.as_ref() {
            let Some(refund) = refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .get(refund_id)
                .filter(|refund| refund.wallet_id == wallet_id)
                .cloned()
            else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            if refund.status != "processing" {
                return Ok(AdminWalletMutationOutcome::Invalid(
                    "refund status must be processing before completion".to_string(),
                ));
            }
            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            let mut updated_refund = refund;
            updated_refund.status = "succeeded".to_string();
            updated_refund.gateway_refund_id = gateway_refund_id.map(ToOwned::to_owned);
            updated_refund.payout_reference = payout_reference.map(ToOwned::to_owned);
            updated_refund.payout_proof = payout_proof;
            updated_refund.completed_at_unix_secs = Some(now_unix_secs);
            updated_refund.updated_at_unix_secs = now_unix_secs;
            refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .insert(updated_refund.id.clone(), updated_refund.clone());
            return Ok(AdminWalletMutationOutcome::Applied(updated_refund));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(current_refund) = sqlx::query(
            r#"
SELECT status
FROM refund_requests
WHERE id = $1 AND wallet_id = $2
FOR UPDATE
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let status = current_refund
            .try_get::<String, _>("status")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        if status != "processing" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "refund status must be processing before completion".to_string(),
            ));
        }

        let refund_row = sqlx::query(
            r#"
UPDATE refund_requests
SET
  status = 'succeeded',
  gateway_refund_id = $3,
  payout_reference = $4,
  payout_proof = $5,
  completed_at = NOW(),
  updated_at = NOW()
WHERE id = $1 AND wallet_id = $2
RETURNING
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .bind(gateway_refund_id)
        .bind(payout_reference)
        .bind(payout_proof)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let refund = admin_wallet_refund_from_row(&refund_row)?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        Ok(AdminWalletMutationOutcome::Applied(refund))
    }

    pub(crate) async fn admin_fail_wallet_refund(
        &self,
        wallet_id: &str,
        refund_id: &str,
        reason: &str,
        operator_id: Option<&str>,
    ) -> Result<
        AdminWalletMutationOutcome<(
            aether_data::repository::wallet::StoredWalletSnapshot,
            AdminWalletRefundRecord,
            Option<AdminWalletTransactionRecord>,
        )>,
        GatewayError,
    > {
        #[cfg(test)]
        if let (Some(wallet_store), Some(refund_store)) = (
            self.auth_wallet_store.as_ref(),
            self.admin_wallet_refund_store.as_ref(),
        ) {
            let Some(wallet) = wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .get(wallet_id)
                .cloned()
            else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };
            let Some(refund) = refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .get(refund_id)
                .filter(|refund| refund.wallet_id == wallet_id)
                .cloned()
            else {
                return Ok(AdminWalletMutationOutcome::NotFound);
            };

            let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
            if matches!(refund.status.as_str(), "pending_approval" | "approved") {
                let mut updated_refund = refund;
                updated_refund.status = "failed".to_string();
                updated_refund.failure_reason = Some(reason.to_string());
                updated_refund.updated_at_unix_secs = now_unix_secs;
                refund_store
                    .lock()
                    .expect("admin wallet refund store should lock")
                    .insert(updated_refund.id.clone(), updated_refund.clone());
                return Ok(AdminWalletMutationOutcome::Applied((
                    wallet,
                    updated_refund,
                    None,
                )));
            }
            if refund.status != "processing" {
                return Ok(AdminWalletMutationOutcome::Invalid(format!(
                    "cannot fail refund in status: {}",
                    refund.status
                )));
            }

            let amount_usd = refund.amount_usd;
            let before_recharge = wallet.balance;
            let before_gift = wallet.gift_balance;
            let before_total = before_recharge + before_gift;
            let after_recharge = before_recharge + amount_usd;

            let mut updated_wallet = wallet.clone();
            updated_wallet.balance = after_recharge;
            updated_wallet.total_refunded = (updated_wallet.total_refunded - amount_usd).max(0.0);
            updated_wallet.updated_at_unix_secs = now_unix_secs;

            let transaction = AdminWalletTransactionRecord {
                id: uuid::Uuid::new_v4().to_string(),
                wallet_id: updated_wallet.id.clone(),
                category: "refund".to_string(),
                reason_code: "refund_revert".to_string(),
                amount: amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + before_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: before_gift,
                link_type: Some("refund_request".to_string()),
                link_id: Some(refund.id.clone()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some("退款失败回补".to_string()),
                created_at_unix_secs: now_unix_secs,
            };

            if let Some(payment_order_id) = refund.payment_order_id.clone() {
                let Some(order_store) = self.admin_wallet_payment_order_store.as_ref() else {
                    return Ok(AdminWalletMutationOutcome::Unavailable);
                };
                let maybe_order = order_store
                    .lock()
                    .expect("admin wallet payment order store should lock")
                    .get(&payment_order_id)
                    .cloned();
                if let Some(mut order) = maybe_order {
                    order.refunded_amount_usd -= amount_usd;
                    order.refundable_amount_usd += amount_usd;
                    order_store
                        .lock()
                        .expect("admin wallet payment order store should lock")
                        .insert(order.id.clone(), order);
                }
            }

            let mut updated_refund = refund;
            updated_refund.status = "failed".to_string();
            updated_refund.failure_reason = Some(reason.to_string());
            updated_refund.updated_at_unix_secs = now_unix_secs;

            wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .insert(updated_wallet.id.clone(), updated_wallet.clone());
            refund_store
                .lock()
                .expect("admin wallet refund store should lock")
                .insert(updated_refund.id.clone(), updated_refund.clone());

            return Ok(AdminWalletMutationOutcome::Applied((
                updated_wallet,
                updated_refund,
                Some(transaction),
            )));
        }

        let Some(pool) = self.postgres_pool() else {
            return Ok(AdminWalletMutationOutcome::Unavailable);
        };
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let Some(refund_row) = sqlx::query(
            r#"
SELECT
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
FROM refund_requests
WHERE id = $1 AND wallet_id = $2
FOR UPDATE
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::NotFound);
        };
        let refund = admin_wallet_refund_from_row(&refund_row)?;

        if matches!(refund.status.as_str(), "pending_approval" | "approved") {
            let refund_row = sqlx::query(
                r#"
UPDATE refund_requests
SET
  status = 'failed',
  failure_reason = $3,
  updated_at = NOW()
WHERE id = $1 AND wallet_id = $2
RETURNING
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
                "#,
            )
            .bind(refund_id)
            .bind(wallet_id)
            .bind(reason)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            let refund = admin_wallet_refund_from_row(&refund_row)?;
            let wallet_row = sqlx::query(
                r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM wallets
WHERE id = $1
                "#,
            )
            .bind(wallet_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            let wallet = admin_wallet_snapshot_from_row(&wallet_row)?;
            tx.commit()
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            return Ok(AdminWalletMutationOutcome::Applied((wallet, refund, None)));
        }
        if refund.status != "processing" {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(format!(
                "cannot fail refund in status: {}",
                refund.status
            )));
        }

        let Some(wallet_row) = sqlx::query(
            r#"
SELECT
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
FROM wallets
WHERE id = $1
FOR UPDATE
            "#,
        )
        .bind(wallet_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        else {
            let _ = tx.rollback().await;
            return Ok(AdminWalletMutationOutcome::Invalid(
                "wallet not found".to_string(),
            ));
        };

        let amount_usd = refund.amount_usd;
        let before_recharge = wallet_row
            .try_get::<f64, _>("balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_gift = wallet_row
            .try_get::<f64, _>("gift_balance")
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let before_total = before_recharge + before_gift;
        let after_recharge = before_recharge + amount_usd;

        let wallet_row = sqlx::query(
            r#"
UPDATE wallets
SET
  balance = $2,
  total_refunded = GREATEST(total_refunded - $3, 0),
  updated_at = NOW()
WHERE id = $1
RETURNING
  id,
  user_id,
  api_key_id,
  CAST(balance AS DOUBLE PRECISION) AS balance,
  CAST(gift_balance AS DOUBLE PRECISION) AS gift_balance,
  limit_mode,
  currency,
  status,
  CAST(total_recharged AS DOUBLE PRECISION) AS total_recharged,
  CAST(total_consumed AS DOUBLE PRECISION) AS total_consumed,
  CAST(total_refunded AS DOUBLE PRECISION) AS total_refunded,
  CAST(total_adjusted AS DOUBLE PRECISION) AS total_adjusted,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs
            "#,
        )
        .bind(wallet_id)
        .bind(after_recharge)
        .bind(amount_usd)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wallet = admin_wallet_snapshot_from_row(&wallet_row)?;

        let transaction_id = uuid::Uuid::new_v4().to_string();
        let created_at_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
        sqlx::query(
            r#"
INSERT INTO wallet_transactions (
  id,
  wallet_id,
  category,
  reason_code,
  amount,
  balance_before,
  balance_after,
  recharge_balance_before,
  recharge_balance_after,
  gift_balance_before,
  gift_balance_after,
  link_type,
  link_id,
  operator_id,
  description,
  created_at
)
VALUES (
  $1,
  $2,
  'refund',
  'refund_revert',
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'refund_request',
  $10,
  $11,
  '退款失败回补',
  NOW()
)
            "#,
        )
        .bind(&transaction_id)
        .bind(wallet_id)
        .bind(amount_usd)
        .bind(before_total)
        .bind(after_recharge + before_gift)
        .bind(before_recharge)
        .bind(after_recharge)
        .bind(before_gift)
        .bind(before_gift)
        .bind(refund_id)
        .bind(operator_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

        if let Some(payment_order_id) = refund.payment_order_id.as_deref() {
            if sqlx::query(
                r#"
UPDATE payment_orders
SET
  refunded_amount_usd = refunded_amount_usd - $2,
  refundable_amount_usd = refundable_amount_usd + $2
WHERE id = $1
                "#,
            )
            .bind(payment_order_id)
            .bind(amount_usd)
            .execute(&mut *tx)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .rows_affected()
                == 0
            {
                // Python 语义下缺失 payment_order 时直接跳过，不报错。
            }
        }

        let refund_row = sqlx::query(
            r#"
UPDATE refund_requests
SET
  status = 'failed',
  failure_reason = $3,
  updated_at = NOW()
WHERE id = $1 AND wallet_id = $2
RETURNING
  id,
  refund_no,
  wallet_id,
  user_id,
  payment_order_id,
  source_type,
  source_id,
  refund_mode,
  CAST(amount_usd AS DOUBLE PRECISION) AS amount_usd,
  status,
  reason,
  failure_reason,
  gateway_refund_id,
  payout_method,
  payout_reference,
  payout_proof,
  requested_by,
  approved_by,
  processed_by,
  CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT) AS created_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM updated_at) AS BIGINT) AS updated_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM processed_at) AS BIGINT) AS processed_at_unix_secs,
  CAST(EXTRACT(EPOCH FROM completed_at) AS BIGINT) AS completed_at_unix_secs
            "#,
        )
        .bind(refund_id)
        .bind(wallet_id)
        .bind(reason)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let refund = admin_wallet_refund_from_row(&refund_row)?;
        tx.commit()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;

        Ok(AdminWalletMutationOutcome::Applied((
            wallet,
            refund,
            Some(AdminWalletTransactionRecord {
                id: transaction_id,
                wallet_id: wallet_id.to_string(),
                category: "refund".to_string(),
                reason_code: "refund_revert".to_string(),
                amount: amount_usd,
                balance_before: before_total,
                balance_after: after_recharge + before_gift,
                recharge_balance_before: before_recharge,
                recharge_balance_after: after_recharge,
                gift_balance_before: before_gift,
                gift_balance_after: before_gift,
                link_type: Some("refund_request".to_string()),
                link_id: Some(refund_id.to_string()),
                operator_id: operator_id.map(ToOwned::to_owned),
                description: Some("退款失败回补".to_string()),
                created_at_unix_secs,
            }),
        )))
    }

    pub(crate) async fn count_user_pending_refunds(
        &self,
        user_id: &str,
    ) -> Result<u64, GatewayError> {
        #[cfg(test)]
        {
            let _ = user_id;
            if self.auth_user_store.is_some() {
                return Ok(0);
            }
        }

        self.data
            .count_user_pending_refunds(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn count_user_pending_payment_orders(
        &self,
        user_id: &str,
    ) -> Result<u64, GatewayError> {
        #[cfg(test)]
        {
            let _ = user_id;
            if self.auth_user_store.is_some() {
                return Ok(0);
            }
        }

        self.data
            .count_user_pending_payment_orders(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_local_auth_user(&self, user_id: &str) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_user_store.as_ref() {
            let removed = store
                .lock()
                .expect("auth user store should lock")
                .remove(user_id)
                .is_some();
            if removed {
                if let Some(wallet_store) = self.auth_wallet_store.as_ref() {
                    wallet_store
                        .lock()
                        .expect("auth wallet store should lock")
                        .retain(|_, wallet| wallet.user_id.as_deref() != Some(user_id));
                }
                if let Some(session_store) = self.auth_session_store.as_ref() {
                    let prefix = format!("{user_id}:");
                    session_store
                        .lock()
                        .expect("auth session store should lock")
                        .retain(|key, _| !key.starts_with(&prefix));
                }
            }
            return Ok(removed);
        }

        self.data
            .delete_local_auth_user(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn register_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        initial_gift_usd: f64,
        unlimited: bool,
    ) -> Result<
        Option<(
            aether_data::repository::users::StoredUserAuthRecord,
            aether_data::repository::wallet::StoredWalletSnapshot,
        )>,
        GatewayError,
    > {
        #[cfg(test)]
        if let (Some(user_store), Some(wallet_store)) = (
            self.auth_user_store.as_ref(),
            self.auth_wallet_store.as_ref(),
        ) {
            let now = chrono::Utc::now();
            let user = aether_data::repository::users::StoredUserAuthRecord::new(
                uuid::Uuid::new_v4().to_string(),
                email,
                email_verified,
                username,
                Some(password_hash),
                "user".to_string(),
                "local".to_string(),
                None,
                None,
                None,
                true,
                false,
                Some(now),
                None,
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            let gift_balance = if unlimited {
                0.0
            } else {
                initial_gift_usd.max(0.0)
            };
            let wallet = aether_data::repository::wallet::StoredWalletSnapshot::new(
                uuid::Uuid::new_v4().to_string(),
                Some(user.id.clone()),
                None,
                0.0,
                gift_balance,
                if unlimited {
                    "unlimited".to_string()
                } else {
                    "finite".to_string()
                },
                "USD".to_string(),
                "active".to_string(),
                0.0,
                0.0,
                0.0,
                gift_balance,
                now.timestamp(),
            )
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
            user_store
                .lock()
                .expect("auth user store should lock")
                .insert(user.id.clone(), user.clone());
            wallet_store
                .lock()
                .expect("auth wallet store should lock")
                .insert(wallet.id.clone(), wallet.clone());
            return Ok(Some((user, wallet)));
        }

        self.data
            .register_local_auth_user(
                email,
                email_verified,
                username,
                password_hash,
                initial_gift_usd,
                unlimited,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_user_session(
        &self,
        user_id: &str,
        session_id: &str,
    ) -> Result<Option<data::StoredUserSessionRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let key = format!("{user_id}:{session_id}");
            return Ok(store
                .lock()
                .expect("auth session store should lock")
                .get(&key)
                .cloned());
        }

        self.data
            .find_user_session(user_id, session_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<Vec<data::StoredUserSessionRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let prefix = format!("{user_id}:");
            let now = chrono::Utc::now();
            let mut sessions = store
                .lock()
                .expect("auth session store should lock")
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(_, session)| session.clone())
                .filter(|session| !session.is_revoked() && !session.is_expired(now))
                .collect::<Vec<_>>();
            sessions.sort_by(|left, right| {
                right
                    .last_seen_at
                    .cmp(&left.last_seen_at)
                    .then_with(|| right.created_at.cmp(&left.created_at))
            });
            return Ok(sessions);
        }

        self.data
            .list_user_sessions(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn touch_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        touched_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let key = format!("{user_id}:{session_id}");
            let mut guard = store.lock().expect("auth session store should lock");
            if let Some(session) = guard.get_mut(&key) {
                session.last_seen_at = Some(touched_at);
                if let Some(ip_address) = ip_address {
                    session.ip_address = Some(ip_address.to_string());
                }
                if let Some(user_agent) = user_agent {
                    session.user_agent = Some(user_agent.chars().take(1000).collect());
                }
                session.updated_at = Some(touched_at);
                return Ok(true);
            }
            return Ok(false);
        }

        self.data
            .touch_user_session(user_id, session_id, touched_at, ip_address, user_agent)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_user_session_device_label(
        &self,
        user_id: &str,
        session_id: &str,
        device_label: &str,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let key = format!("{user_id}:{session_id}");
            let mut guard = store.lock().expect("auth session store should lock");
            if let Some(session) = guard.get_mut(&key) {
                session.device_label = Some(device_label.chars().take(120).collect());
                session.updated_at = Some(updated_at);
                return Ok(true);
            }
            return Ok(false);
        }

        self.data
            .update_user_session_device_label(user_id, session_id, device_label, updated_at)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn create_user_session(
        &self,
        session: data::StoredUserSessionRecord,
    ) -> Result<Option<data::StoredUserSessionRecord>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let now = session
                .created_at
                .or(session.updated_at)
                .or(session.last_seen_at)
                .unwrap_or_else(chrono::Utc::now);
            let mut guard = store.lock().expect("auth session store should lock");
            for existing in guard.values_mut() {
                if existing.user_id == session.user_id
                    && existing.client_device_id == session.client_device_id
                    && !existing.is_revoked()
                    && !existing.is_expired(now)
                {
                    existing.revoked_at = Some(now);
                    existing.revoke_reason = Some("replaced_by_new_login".to_string());
                    existing.updated_at = Some(now);
                }
            }
            guard.insert(
                format!("{}:{}", session.user_id, session.id),
                session.clone(),
            );
            return Ok(Some(session));
        }

        self.data
            .create_user_session(&session)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn rotate_user_session_refresh_token(
        &self,
        user_id: &str,
        session_id: &str,
        previous_refresh_token_hash: &str,
        next_refresh_token_hash: &str,
        rotated_at: chrono::DateTime<chrono::Utc>,
        expires_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let key = format!("{user_id}:{session_id}");
            let mut guard = store.lock().expect("auth session store should lock");
            if let Some(session) = guard.get_mut(&key) {
                session.prev_refresh_token_hash = Some(previous_refresh_token_hash.to_string());
                session.refresh_token_hash = next_refresh_token_hash.to_string();
                session.rotated_at = Some(rotated_at);
                session.expires_at = Some(expires_at);
                session.last_seen_at = Some(rotated_at);
                if let Some(ip_address) = ip_address {
                    session.ip_address = Some(ip_address.to_string());
                }
                if let Some(user_agent) = user_agent {
                    session.user_agent = Some(user_agent.chars().take(1000).collect());
                }
                session.updated_at = Some(rotated_at);
                return Ok(true);
            }
            return Ok(false);
        }

        self.data
            .rotate_user_session_refresh_token(
                user_id,
                session_id,
                previous_refresh_token_hash,
                next_refresh_token_hash,
                rotated_at,
                expires_at,
                ip_address,
                user_agent,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn revoke_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<bool, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let key = format!("{user_id}:{session_id}");
            let mut guard = store.lock().expect("auth session store should lock");
            if let Some(session) = guard.get_mut(&key) {
                session.revoked_at = Some(revoked_at);
                session.revoke_reason = Some(reason.chars().take(100).collect());
                session.updated_at = Some(revoked_at);
                return Ok(true);
            }
            return Ok(false);
        }

        self.data
            .revoke_user_session(user_id, session_id, revoked_at, reason)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn revoke_all_user_sessions(
        &self,
        user_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<u64, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_session_store.as_ref() {
            let prefix = format!("{user_id}:");
            let mut revoked = 0_u64;
            let mut guard = store.lock().expect("auth session store should lock");
            for (key, session) in guard.iter_mut() {
                if !key.starts_with(&prefix) || session.revoked_at.is_some() {
                    continue;
                }
                session.revoked_at = Some(revoked_at);
                session.revoke_reason = Some(reason.chars().take(100).collect());
                session.updated_at = Some(revoked_at);
                revoked += 1;
            }
            return Ok(revoked);
        }

        self.data
            .revoke_all_user_sessions(user_id, revoked_at, reason)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_request_audit_bundle(
        &self,
        request_id: &str,
        attempted_only: bool,
        now_unix_secs: u64,
    ) -> Result<Option<usage::RequestAuditBundle>, GatewayError> {
        self.data
            .read_request_audit_bundle(request_id, attempted_only, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_auth_api_key_snapshot(
        &self,
        user_id: &str,
        api_key_id: &str,
        now_unix_secs: u64,
    ) -> Result<Option<data::StoredGatewayAuthApiKeySnapshot>, GatewayError> {
        self.data
            .read_auth_api_key_snapshot(user_id, api_key_id, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_auth_api_key_snapshot_by_key_hash(
        &self,
        key_hash: &str,
        now_unix_secs: u64,
    ) -> Result<Option<data::StoredGatewayAuthApiKeySnapshot>, GatewayError> {
        self.data
            .read_auth_api_key_snapshot_by_key_hash(key_hash, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_auth_api_key_snapshots_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<aether_data::repository::auth::StoredAuthApiKeySnapshot>, GatewayError> {
        self.data
            .list_auth_api_key_snapshots_by_ids(api_key_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_auth_api_key_export_records_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .list_auth_api_key_export_records_by_user_ids(user_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_auth_api_key_export_records_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .list_auth_api_key_export_records_by_ids(api_key_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_auth_api_key_export_standalone_records_page(
        &self,
        query: &aether_data::repository::auth::StandaloneApiKeyExportListQuery,
    ) -> Result<Vec<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .list_auth_api_key_export_standalone_records_page(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn count_auth_api_key_export_standalone_records(
        &self,
        is_active: Option<bool>,
    ) -> Result<u64, GatewayError> {
        self.data
            .count_auth_api_key_export_standalone_records(is_active)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_auth_api_key_export_records_by_user_ids(
        &self,
        user_ids: &[String],
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, GatewayError> {
        self.data
            .summarize_auth_api_key_export_records_by_user_ids(user_ids, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_auth_api_key_export_non_standalone_records(
        &self,
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, GatewayError> {
        self.data
            .summarize_auth_api_key_export_non_standalone_records(now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_auth_api_key_export_standalone_records(
        &self,
    ) -> Result<Vec<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .list_auth_api_key_export_standalone_records()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_auth_api_key_export_standalone_records(
        &self,
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::auth::AuthApiKeyExportSummary, GatewayError> {
        self.data
            .summarize_auth_api_key_export_standalone_records(now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_auth_api_key_export_standalone_record_by_id(
        &self,
        api_key_id: &str,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .find_auth_api_key_export_standalone_record_by_id(api_key_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_non_admin_export_users(
        &self,
    ) -> Result<Vec<aether_data::repository::users::StoredUserExportRow>, GatewayError> {
        self.data
            .list_non_admin_export_users()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_export_users(
        &self,
    ) -> Result<Vec<aether_data::repository::users::StoredUserExportRow>, GatewayError> {
        self.data
            .list_export_users()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_export_users(
        &self,
    ) -> Result<aether_data::repository::users::UserExportSummary, GatewayError> {
        self.data
            .summarize_export_users()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_export_users_page(
        &self,
        query: &aether_data::repository::users::UserExportListQuery,
    ) -> Result<Vec<aether_data::repository::users::StoredUserExportRow>, GatewayError> {
        self.data
            .list_export_users_page(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_export_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<aether_data::repository::users::StoredUserExportRow>, GatewayError> {
        self.data
            .find_export_user_by_id(user_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_user_auth_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<aether_data::repository::users::StoredUserAuthRecord>, GatewayError> {
        self.data
            .list_user_auth_by_ids(user_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn create_user_api_key(
        &self,
        record: aether_data::repository::auth::CreateUserApiKeyRecord,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .create_user_api_key(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn create_standalone_api_key(
        &self,
        record: aether_data::repository::auth::CreateStandaloneApiKeyRecord,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .create_standalone_api_key(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_user_api_key_basic(
        &self,
        record: aether_data::repository::auth::UpdateUserApiKeyBasicRecord,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .update_user_api_key_basic(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_standalone_api_key_basic(
        &self,
        record: aether_data::repository::auth::UpdateStandaloneApiKeyBasicRecord,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .update_standalone_api_key_basic(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn set_user_api_key_active(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .set_user_api_key_active(user_id, api_key_id, is_active)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn set_standalone_api_key_active(
        &self,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .set_standalone_api_key_active(api_key_id, is_active)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn set_user_api_key_locked(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_locked: bool,
    ) -> Result<bool, GatewayError> {
        self.data
            .set_user_api_key_locked(user_id, api_key_id, is_locked)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn set_user_api_key_allowed_providers(
        &self,
        user_id: &str,
        api_key_id: &str,
        allowed_providers: Option<Vec<String>>,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .set_user_api_key_allowed_providers(user_id, api_key_id, allowed_providers)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn set_user_api_key_force_capabilities(
        &self,
        user_id: &str,
        api_key_id: &str,
        force_capabilities: Option<serde_json::Value>,
    ) -> Result<Option<aether_data::repository::auth::StoredAuthApiKeyExportRecord>, GatewayError>
    {
        self.data
            .set_user_api_key_force_capabilities(user_id, api_key_id, force_capabilities)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_user_api_key(
        &self,
        user_id: &str,
        api_key_id: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_user_api_key(user_id, api_key_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_standalone_api_key(
        &self,
        api_key_id: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_standalone_api_key(api_key_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) fn has_auth_api_key_writer(&self) -> bool {
        self.data.has_auth_api_key_writer()
    }

    pub(crate) async fn touch_auth_api_key_last_used_best_effort(&self, api_key_id: &str) {
        let api_key_id = api_key_id.trim();
        if api_key_id.is_empty() || !self.has_auth_api_key_writer() {
            return;
        }
        if !self.auth_api_key_last_used_cache.should_touch(
            api_key_id,
            AUTH_API_KEY_LAST_USED_TTL,
            AUTH_API_KEY_LAST_USED_MAX_ENTRIES,
        ) {
            return;
        }
        if let Err(err) = self.data.touch_auth_api_key_last_used(api_key_id).await {
            tracing::warn!(
                api_key_id = %api_key_id,
                error = ?err,
                "gateway auth api key last_used_at touch failed"
            );
        }
    }

    pub(crate) async fn read_minimal_candidate_selection(
        &self,
        api_format: &str,
        global_model_name: &str,
        require_streaming: bool,
        auth_snapshot: Option<&data::StoredGatewayAuthApiKeySnapshot>,
    ) -> Result<Vec<scheduler::GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
        self.data
            .read_minimal_candidate_selection(
                api_format,
                global_model_name,
                require_streaming,
                auth_snapshot,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<
        Vec<aether_data::repository::candidate_selection::StoredMinimalCandidateSelectionRow>,
        GatewayError,
    > {
        self.data
            .list_minimal_candidate_selection_rows_for_api_format(api_format)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<
        Vec<aether_data::repository::candidate_selection::StoredMinimalCandidateSelectionRow>,
        GatewayError,
    > {
        self.data
            .list_minimal_candidate_selection_rows(api_format, global_model_name)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_provider_quota_snapshot(
        &self,
        provider_id: &str,
    ) -> Result<Option<aether_data::repository::quota::StoredProviderQuotaSnapshot>, GatewayError>
    {
        self.data
            .find_provider_quota_by_provider_id(provider_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_recent_request_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<aether_data::repository::candidates::StoredRequestCandidate>, GatewayError>
    {
        self.data
            .list_recent_request_candidates(limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_request_candidate(
        &self,
        candidate: aether_data::repository::candidates::UpsertRequestCandidateRecord,
    ) -> Result<Option<aether_data::repository::candidates::StoredRequestCandidate>, GatewayError>
    {
        self.data
            .upsert_request_candidate(candidate)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_gemini_file_mapping(
        &self,
        record: aether_data::repository::gemini_file_mappings::UpsertGeminiFileMappingRecord,
    ) -> Result<
        Option<aether_data::repository::gemini_file_mappings::StoredGeminiFileMapping>,
        GatewayError,
    > {
        self.data
            .upsert_gemini_file_mapping(record)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_gemini_file_mappings(
        &self,
        query: &aether_data::repository::gemini_file_mappings::GeminiFileMappingListQuery,
    ) -> Result<
        aether_data::repository::gemini_file_mappings::StoredGeminiFileMappingListPage,
        GatewayError,
    > {
        self.data
            .list_gemini_file_mappings(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_gemini_file_mappings(
        &self,
        now_unix_secs: u64,
    ) -> Result<aether_data::repository::gemini_file_mappings::GeminiFileMappingStats, GatewayError>
    {
        self.data
            .summarize_gemini_file_mappings(now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_gemini_file_mapping_by_file_name(
        &self,
        file_name: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_gemini_file_mapping_by_file_name(file_name)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_gemini_file_mapping_by_id(
        &self,
        mapping_id: &str,
    ) -> Result<
        Option<aether_data::repository::gemini_file_mappings::StoredGeminiFileMapping>,
        GatewayError,
    > {
        self.data
            .delete_gemini_file_mapping_by_id(mapping_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_expired_gemini_file_mappings(
        &self,
        now_unix_secs: u64,
    ) -> Result<usize, GatewayError> {
        self.data
            .delete_expired_gemini_file_mappings(now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn cache_set_string_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: u64,
    ) -> Result<(), GatewayError> {
        self.data
            .cache_set_string_with_ttl(key, value, ttl_seconds)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn cache_delete_key(&self, key: &str) -> Result<(), GatewayError> {
        self.data
            .cache_delete_key(key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_wallet(
        &self,
        lookup: aether_data::repository::wallet::WalletLookupKey<'_>,
    ) -> Result<Option<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        #[cfg(test)]
        if let Some(store) = self.auth_wallet_store.as_ref() {
            let wallet = {
                let wallets = store.lock().expect("auth wallet store should lock");
                match lookup {
                    aether_data::repository::wallet::WalletLookupKey::WalletId(wallet_id) => {
                        wallets.get(wallet_id).cloned()
                    }
                    aether_data::repository::wallet::WalletLookupKey::UserId(user_id) => wallets
                        .values()
                        .find(|wallet| wallet.user_id.as_deref() == Some(user_id))
                        .cloned(),
                    aether_data::repository::wallet::WalletLookupKey::ApiKeyId(api_key_id) => {
                        wallets
                            .values()
                            .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id))
                            .cloned()
                    }
                }
            };
            if wallet.is_some() {
                return Ok(wallet);
            }
        }

        self.data
            .find_wallet(lookup)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_wallet_snapshot_for_auth(
        &self,
        user_id: &str,
        api_key_id: &str,
        api_key_is_standalone: bool,
    ) -> Result<Option<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        let lookup = if api_key_is_standalone {
            if api_key_id.trim().is_empty() {
                None
            } else {
                Some(aether_data::repository::wallet::WalletLookupKey::ApiKeyId(
                    api_key_id,
                ))
            }
        } else if !user_id.trim().is_empty() {
            Some(aether_data::repository::wallet::WalletLookupKey::UserId(
                user_id,
            ))
        } else if !api_key_id.trim().is_empty() {
            Some(aether_data::repository::wallet::WalletLookupKey::ApiKeyId(
                api_key_id,
            ))
        } else {
            None
        };

        let Some(lookup) = lookup else {
            return Ok(None);
        };

        self.find_wallet(lookup).await
    }

    pub(crate) async fn list_wallet_snapshots_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        self.data
            .list_wallets_by_user_ids(user_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_wallet_snapshots_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<aether_data::repository::wallet::StoredWalletSnapshot>, GatewayError> {
        self.data
            .list_wallets_by_api_key_ids(api_key_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn record_shadow_result_sample(
        &self,
        sample: aether_data::repository::shadow_results::RecordShadowResultSample,
    ) -> Result<Option<aether_data::repository::shadow_results::StoredShadowResult>, GatewayError>
    {
        self.data
            .record_shadow_result_sample(sample)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_recent_shadow_results(
        &self,
        limit: usize,
    ) -> Result<Vec<aether_data::repository::shadow_results::StoredShadowResult>, GatewayError>
    {
        self.data
            .list_recent_shadow_results(limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }
}
