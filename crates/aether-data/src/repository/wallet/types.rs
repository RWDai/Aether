use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletLookupKey<'a> {
    WalletId(&'a str),
    UserId(&'a str),
    ApiKeyId(&'a str),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredWalletSnapshot {
    pub id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub balance: f64,
    pub gift_balance: f64,
    pub limit_mode: String,
    pub currency: String,
    pub status: String,
    pub total_recharged: f64,
    pub total_consumed: f64,
    pub total_refunded: f64,
    pub total_adjusted: f64,
    pub updated_at_unix_secs: u64,
}

impl StoredWalletSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        user_id: Option<String>,
        api_key_id: Option<String>,
        balance: f64,
        gift_balance: f64,
        limit_mode: String,
        currency: String,
        status: String,
        total_recharged: f64,
        total_consumed: f64,
        total_refunded: f64,
        total_adjusted: f64,
        updated_at_unix_secs: i64,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "wallet.id is empty".to_string(),
            ));
        }
        if limit_mode.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "wallet.limit_mode is empty".to_string(),
            ));
        }
        if currency.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "wallet.currency is empty".to_string(),
            ));
        }
        if status.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "wallet.status is empty".to_string(),
            ));
        }
        if !balance.is_finite()
            || !gift_balance.is_finite()
            || !total_recharged.is_finite()
            || !total_consumed.is_finite()
            || !total_refunded.is_finite()
            || !total_adjusted.is_finite()
        {
            return Err(crate::DataLayerError::UnexpectedValue(
                "wallet numeric value is not finite".to_string(),
            ));
        }
        Ok(Self {
            id,
            user_id,
            api_key_id,
            balance,
            gift_balance,
            limit_mode,
            currency,
            status,
            total_recharged,
            total_consumed,
            total_refunded,
            total_adjusted,
            updated_at_unix_secs: u64::try_from(updated_at_unix_secs).map_err(|_| {
                crate::DataLayerError::UnexpectedValue(
                    "wallet.updated_at_unix_secs is negative".to_string(),
                )
            })?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UsageSettlementInput {
    pub request_id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub provider_id: Option<String>,
    pub status: String,
    pub billing_status: String,
    pub total_cost_usd: f64,
    pub actual_total_cost_usd: f64,
    pub finalized_at_unix_secs: Option<u64>,
}

impl UsageSettlementInput {
    pub fn validate(&self) -> Result<(), crate::DataLayerError> {
        if self.request_id.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "wallet settlement request_id cannot be empty".to_string(),
            ));
        }
        if self.status.trim().is_empty() || self.billing_status.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "wallet settlement status cannot be empty".to_string(),
            ));
        }
        if !self.total_cost_usd.is_finite() || !self.actual_total_cost_usd.is_finite() {
            return Err(crate::DataLayerError::InvalidInput(
                "wallet settlement cost must be finite".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredUsageSettlement {
    pub request_id: String,
    pub wallet_id: Option<String>,
    pub billing_status: String,
    pub wallet_balance_before: Option<f64>,
    pub wallet_balance_after: Option<f64>,
    pub wallet_recharge_balance_before: Option<f64>,
    pub wallet_recharge_balance_after: Option<f64>,
    pub wallet_gift_balance_before: Option<f64>,
    pub wallet_gift_balance_after: Option<f64>,
    pub provider_monthly_used_usd: Option<f64>,
    pub finalized_at_unix_secs: Option<u64>,
}

#[async_trait]
pub trait WalletReadRepository: Send + Sync {
    async fn find(
        &self,
        key: WalletLookupKey<'_>,
    ) -> Result<Option<StoredWalletSnapshot>, crate::DataLayerError>;

    async fn list_wallets_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, crate::DataLayerError>;

    async fn list_wallets_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, crate::DataLayerError>;
}

#[async_trait]
pub trait WalletWriteRepository: Send + Sync {
    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, crate::DataLayerError>;
}

pub trait WalletRepository: WalletReadRepository + WalletWriteRepository + Send + Sync {}

impl<T> WalletRepository for T where T: WalletReadRepository + WalletWriteRepository + Send + Sync {}

#[cfg(test)]
mod tests {
    use super::{StoredWalletSnapshot, UsageSettlementInput};

    #[test]
    fn rejects_invalid_wallet_snapshot() {
        assert!(StoredWalletSnapshot::new(
            "".to_string(),
            None,
            None,
            1.0,
            0.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            1,
        )
        .is_err());
    }

    #[test]
    fn rejects_invalid_settlement_input() {
        let input = UsageSettlementInput {
            request_id: "".to_string(),
            user_id: None,
            api_key_id: None,
            provider_id: None,
            status: "completed".to_string(),
            billing_status: "pending".to_string(),
            total_cost_usd: 0.1,
            actual_total_cost_usd: 0.1,
            finalized_at_unix_secs: None,
        };
        assert!(input.validate().is_err());
    }
}
