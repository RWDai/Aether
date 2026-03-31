use serde::{Deserialize, Serialize};

use crate::quantize_money;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderBillingType {
    MonthlyQuota,
    PayAsYouGo,
    FreeTier,
    Unknown,
}

impl ProviderBillingType {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "monthly_quota" => Self::MonthlyQuota,
            "pay_as_you_go" => Self::PayAsYouGo,
            "free_tier" => Self::FreeTier,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderQuotaSnapshot {
    pub provider_id: String,
    pub billing_type: ProviderBillingType,
    pub monthly_quota_usd: Option<f64>,
    pub monthly_used_usd: f64,
    pub quota_reset_day: Option<u64>,
    pub quota_last_reset_at_unix_secs: Option<u64>,
    pub quota_expires_at_unix_secs: Option<u64>,
    pub is_active: bool,
}

impl ProviderQuotaSnapshot {
    pub fn remaining_quota_usd(&self) -> Option<f64> {
        self.monthly_quota_usd
            .map(|quota| quantize_money(quota - self.monthly_used_usd))
    }

    pub fn is_expired(&self, now_unix_secs: u64) -> bool {
        self.quota_expires_at_unix_secs
            .is_some_and(|expires_at| expires_at <= now_unix_secs)
    }

    pub fn should_reset(&self, now_unix_secs: u64) -> bool {
        if self.billing_type != ProviderBillingType::MonthlyQuota || !self.is_active {
            return false;
        }
        let Some(reset_day) = self.quota_reset_day.filter(|value| *value > 0) else {
            return false;
        };
        let Some(last_reset) = self.quota_last_reset_at_unix_secs else {
            return true;
        };
        now_unix_secs.saturating_sub(last_reset) >= reset_day.saturating_mul(24 * 60 * 60)
    }
}

#[cfg(test)]
mod tests {
    use super::{ProviderBillingType, ProviderQuotaSnapshot};

    #[test]
    fn monthly_quota_resets_after_period() {
        let snapshot = ProviderQuotaSnapshot {
            provider_id: "provider-1".to_string(),
            billing_type: ProviderBillingType::MonthlyQuota,
            monthly_quota_usd: Some(20.0),
            monthly_used_usd: 5.0,
            quota_reset_day: Some(7),
            quota_last_reset_at_unix_secs: Some(1_000),
            quota_expires_at_unix_secs: None,
            is_active: true,
        };

        assert!(!snapshot.should_reset(1_000 + 6 * 24 * 60 * 60));
        assert!(snapshot.should_reset(1_000 + 7 * 24 * 60 * 60));
        assert_eq!(snapshot.remaining_quota_usd(), Some(15.0));
    }
}
