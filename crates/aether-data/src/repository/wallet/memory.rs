use std::collections::BTreeMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::types::{
    StoredUsageSettlement, StoredWalletSnapshot, UsageSettlementInput, WalletLookupKey,
    WalletReadRepository, WalletWriteRepository,
};
use crate::DataLayerError;

#[derive(Debug, Default)]
pub struct InMemoryWalletRepository {
    wallets_by_id: RwLock<BTreeMap<String, StoredWalletSnapshot>>,
    provider_monthly_used: RwLock<BTreeMap<String, f64>>,
}

impl InMemoryWalletRepository {
    pub fn seed<I>(items: I) -> Self
    where
        I: IntoIterator<Item = StoredWalletSnapshot>,
    {
        let mut wallets_by_id = BTreeMap::new();
        for item in items {
            wallets_by_id.insert(item.id.clone(), item);
        }
        Self {
            wallets_by_id: RwLock::new(wallets_by_id),
            provider_monthly_used: RwLock::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl WalletReadRepository for InMemoryWalletRepository {
    async fn find(
        &self,
        key: WalletLookupKey<'_>,
    ) -> Result<Option<StoredWalletSnapshot>, DataLayerError> {
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(match key {
            WalletLookupKey::WalletId(wallet_id) => wallets.get(wallet_id).cloned(),
            WalletLookupKey::UserId(user_id) => wallets
                .values()
                .find(|wallet| wallet.user_id.as_deref() == Some(user_id))
                .cloned(),
            WalletLookupKey::ApiKeyId(api_key_id) => wallets
                .values()
                .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id))
                .cloned(),
        })
    }

    async fn list_wallets_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, DataLayerError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        let user_set: std::collections::BTreeSet<&str> =
            user_ids.iter().map(String::as_str).collect();
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(wallets
            .values()
            .filter(|wallet| {
                wallet
                    .user_id
                    .as_deref()
                    .map(|user_id| user_set.contains(user_id))
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }

    async fn list_wallets_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredWalletSnapshot>, DataLayerError> {
        if api_key_ids.is_empty() {
            return Ok(Vec::new());
        }
        let key_set: std::collections::BTreeSet<&str> =
            api_key_ids.iter().map(String::as_str).collect();
        let wallets = self.wallets_by_id.read().expect("wallet repo lock");
        Ok(wallets
            .values()
            .filter(|wallet| {
                wallet
                    .api_key_id
                    .as_deref()
                    .map(|api_key_id| key_set.contains(api_key_id))
                    .unwrap_or(false)
            })
            .cloned()
            .collect())
    }
}

#[async_trait]
impl WalletWriteRepository for InMemoryWalletRepository {
    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        input.validate()?;
        if input.billing_status != "pending" {
            return Ok(Some(StoredUsageSettlement {
                request_id: input.request_id,
                wallet_id: None,
                billing_status: input.billing_status,
                wallet_balance_before: None,
                wallet_balance_after: None,
                wallet_recharge_balance_before: None,
                wallet_recharge_balance_after: None,
                wallet_gift_balance_before: None,
                wallet_gift_balance_after: None,
                provider_monthly_used_usd: None,
                finalized_at_unix_secs: input.finalized_at_unix_secs,
            }));
        }

        let mut wallets = self.wallets_by_id.write().expect("wallet repo lock");
        let wallet_id = input
            .api_key_id
            .as_deref()
            .and_then(|api_key_id| {
                wallets
                    .values()
                    .find(|wallet| wallet.api_key_id.as_deref() == Some(api_key_id))
                    .map(|wallet| wallet.id.clone())
            })
            .or_else(|| {
                input.user_id.as_deref().and_then(|user_id| {
                    wallets
                        .values()
                        .find(|wallet| wallet.user_id.as_deref() == Some(user_id))
                        .map(|wallet| wallet.id.clone())
                })
            });
        let wallet = wallet_id
            .as_deref()
            .and_then(|wallet_id| wallets.get_mut(wallet_id));

        let final_billing_status = if input.status == "completed" {
            "settled"
        } else {
            "void"
        };

        let mut settlement = StoredUsageSettlement {
            request_id: input.request_id,
            wallet_id: None,
            billing_status: final_billing_status.to_string(),
            wallet_balance_before: None,
            wallet_balance_after: None,
            wallet_recharge_balance_before: None,
            wallet_recharge_balance_after: None,
            wallet_gift_balance_before: None,
            wallet_gift_balance_after: None,
            provider_monthly_used_usd: None,
            finalized_at_unix_secs: input.finalized_at_unix_secs,
        };

        if let Some(wallet) = wallet {
            let before_recharge = wallet.balance;
            let before_gift = wallet.gift_balance;
            let before_total = before_recharge + before_gift;
            settlement.wallet_id = Some(wallet.id.clone());
            settlement.wallet_balance_before = Some(before_total);
            settlement.wallet_recharge_balance_before = Some(before_recharge);
            settlement.wallet_gift_balance_before = Some(before_gift);

            if final_billing_status == "settled" {
                if wallet.limit_mode.eq_ignore_ascii_case("unlimited") {
                    wallet.total_consumed += input.total_cost_usd;
                } else {
                    let gift_deduction = before_gift.max(0.0).min(input.total_cost_usd);
                    let recharge_deduction = input.total_cost_usd - gift_deduction;
                    wallet.gift_balance = before_gift - gift_deduction;
                    wallet.balance = before_recharge - recharge_deduction;
                    wallet.total_consumed += input.total_cost_usd;
                }
            }

            settlement.wallet_recharge_balance_after = Some(wallet.balance);
            settlement.wallet_gift_balance_after = Some(wallet.gift_balance);
            settlement.wallet_balance_after = Some(wallet.balance + wallet.gift_balance);
        }

        if final_billing_status == "settled" {
            if let Some(provider_id) = input.provider_id {
                let mut quotas = self
                    .provider_monthly_used
                    .write()
                    .expect("provider quota lock");
                let value = quotas.entry(provider_id).or_insert(0.0);
                *value += input.actual_total_cost_usd;
                settlement.provider_monthly_used_usd = Some(*value);
            }
        }

        Ok(Some(settlement))
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryWalletRepository;
    use crate::repository::wallet::{
        StoredWalletSnapshot, UsageSettlementInput, WalletLookupKey, WalletReadRepository,
        WalletWriteRepository,
    };

    fn sample_wallet() -> StoredWalletSnapshot {
        StoredWalletSnapshot::new(
            "wallet-1".to_string(),
            Some("user-1".to_string()),
            Some("key-1".to_string()),
            10.0,
            2.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            100,
        )
        .expect("wallet should build")
    }

    #[tokio::test]
    async fn finds_wallet_by_owner() {
        let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
        let wallet = repository
            .find(WalletLookupKey::UserId("user-1"))
            .await
            .expect("lookup should succeed")
            .expect("wallet should exist");
        assert_eq!(wallet.id, "wallet-1");
    }

    #[tokio::test]
    async fn settles_usage_against_wallet_and_provider_quota() {
        let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
        let settlement = repository
            .settle_usage(UsageSettlementInput {
                request_id: "req-1".to_string(),
                user_id: Some("user-1".to_string()),
                api_key_id: Some("key-1".to_string()),
                provider_id: Some("provider-1".to_string()),
                status: "completed".to_string(),
                billing_status: "pending".to_string(),
                total_cost_usd: 3.0,
                actual_total_cost_usd: 1.5,
                finalized_at_unix_secs: Some(200),
            })
            .await
            .expect("settlement should succeed")
            .expect("settlement should exist");

        assert_eq!(settlement.billing_status, "settled");
        assert_eq!(settlement.wallet_balance_before, Some(12.0));
        assert_eq!(settlement.wallet_balance_after, Some(9.0));
        assert_eq!(settlement.provider_monthly_used_usd, Some(1.5));
    }
}
