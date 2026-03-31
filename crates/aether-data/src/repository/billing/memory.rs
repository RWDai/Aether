use std::collections::BTreeMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::types::{BillingReadRepository, StoredBillingModelContext};
use crate::DataLayerError;

type BillingContextKey = (String, String, Option<String>);
type BillingContextMap = BTreeMap<BillingContextKey, StoredBillingModelContext>;

#[derive(Debug, Default)]
pub struct InMemoryBillingReadRepository {
    by_key: RwLock<BillingContextMap>,
}

impl InMemoryBillingReadRepository {
    pub fn seed<I>(items: I) -> Self
    where
        I: IntoIterator<Item = StoredBillingModelContext>,
    {
        let mut by_key = BTreeMap::new();
        for item in items {
            by_key.insert(
                (
                    item.provider_id.clone(),
                    item.global_model_name.clone(),
                    item.provider_api_key_id.clone(),
                ),
                item,
            );
        }
        Self {
            by_key: RwLock::new(by_key),
        }
    }
}

#[async_trait]
impl BillingReadRepository for InMemoryBillingReadRepository {
    async fn find_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        let key = (
            provider_id.to_string(),
            global_model_name.to_string(),
            provider_api_key_id.map(ToOwned::to_owned),
        );
        let by_key = self.by_key.read().expect("billing repository lock");
        if let Some(value) = by_key.get(&key) {
            return Ok(Some(value.clone()));
        }

        if let Some(value) = by_key
            .get(&(provider_id.to_string(), global_model_name.to_string(), None))
            .cloned()
        {
            return Ok(Some(value));
        }

        Ok(by_key
            .iter()
            .find(|((stored_provider_id, stored_model_name, _), _)| {
                stored_provider_id == provider_id && stored_model_name == global_model_name
            })
            .map(|(_, value)| value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::InMemoryBillingReadRepository;
    use crate::repository::billing::{BillingReadRepository, StoredBillingModelContext};

    fn sample_context() -> StoredBillingModelContext {
        StoredBillingModelContext::new(
            "provider-1".to_string(),
            Some("pay_as_you_go".to_string()),
            Some("key-1".to_string()),
            Some(json!({"openai:chat": 0.8})),
            Some(60),
            "global-model-1".to_string(),
            "gpt-5".to_string(),
            Some(json!({"streaming": true})),
            Some(0.02),
            Some(json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0}]})),
            Some("model-1".to_string()),
            Some("gpt-5-upstream".to_string()),
            None,
            Some(0.01),
            None,
        )
        .expect("billing context should build")
    }

    #[tokio::test]
    async fn falls_back_to_provider_without_key_scope() {
        let repository = InMemoryBillingReadRepository::seed(vec![sample_context()]);
        let stored = repository
            .find_model_context("provider-1", Some("key-2"), "gpt-5")
            .await
            .expect("lookup should succeed")
            .expect("context should exist");

        assert_eq!(stored.provider_id, "provider-1");
        assert_eq!(stored.global_model_name, "gpt-5");
    }
}
