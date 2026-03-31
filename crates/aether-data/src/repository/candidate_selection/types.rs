use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredProviderModelMapping {
    pub name: String,
    pub priority: i32,
    pub api_formats: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredMinimalCandidateSelectionRow {
    pub provider_id: String,
    pub provider_name: String,
    pub provider_type: String,
    pub provider_priority: i32,
    pub provider_is_active: bool,
    pub endpoint_id: String,
    pub endpoint_api_format: String,
    pub endpoint_api_family: Option<String>,
    pub endpoint_kind: Option<String>,
    pub endpoint_is_active: bool,
    pub key_id: String,
    pub key_name: String,
    pub key_auth_type: String,
    pub key_is_active: bool,
    pub key_api_formats: Option<Vec<String>>,
    pub key_allowed_models: Option<Vec<String>>,
    pub key_capabilities: Option<serde_json::Value>,
    pub key_internal_priority: i32,
    pub key_global_priority_by_format: Option<serde_json::Value>,
    pub model_id: String,
    pub global_model_id: String,
    pub global_model_name: String,
    pub global_model_mappings: Option<Vec<String>>,
    pub global_model_supports_streaming: Option<bool>,
    pub model_provider_model_name: String,
    pub model_provider_model_mappings: Option<Vec<StoredProviderModelMapping>>,
    pub model_supports_streaming: Option<bool>,
    pub model_is_active: bool,
    pub model_is_available: bool,
}

impl StoredMinimalCandidateSelectionRow {
    pub fn supports_streaming(&self) -> bool {
        self.model_supports_streaming
            .or(self.global_model_supports_streaming)
            .unwrap_or(true)
    }

    pub fn key_supports_api_format(&self, api_format: &str) -> bool {
        let target = api_format.trim();
        match self.key_api_formats.as_deref() {
            None => true,
            Some(formats) => formats
                .iter()
                .any(|value| value.eq_ignore_ascii_case(target)),
        }
    }
}

#[async_trait]
pub trait MinimalCandidateSelectionReadRepository: Send + Sync {
    async fn list_for_exact_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, crate::DataLayerError>;

    async fn list_for_exact_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, crate::DataLayerError>;
}

pub trait MinimalCandidateSelectionRepository:
    MinimalCandidateSelectionReadRepository + Send + Sync
{
}

impl<T> MinimalCandidateSelectionRepository for T where
    T: MinimalCandidateSelectionReadRepository + Send + Sync
{
}

#[cfg(test)]
mod tests {
    use super::{StoredMinimalCandidateSelectionRow, StoredProviderModelMapping};

    fn sample_row() -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-1".to_string(),
            provider_name: "OpenAI".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: "key-1".to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 50,
            key_global_priority_by_format: None,
            model_id: "model-1".to_string(),
            global_model_id: "global-model-1".to_string(),
            global_model_name: "gpt-4.1".to_string(),
            global_model_mappings: Some(vec!["gpt-4\\.1-.*".to_string()]),
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "gpt-4.1-upstream".to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: "gpt-4.1-canary".to_string(),
                priority: 1,
                api_formats: Some(vec!["openai:chat".to_string()]),
            }]),
            model_supports_streaming: None,
            model_is_active: true,
            model_is_available: true,
        }
    }

    #[test]
    fn defaults_streaming_support_to_true() {
        let mut row = sample_row();
        row.model_supports_streaming = None;
        row.global_model_supports_streaming = None;

        assert!(row.supports_streaming());
    }

    #[test]
    fn key_api_formats_none_means_support_all_formats() {
        let mut row = sample_row();
        row.key_api_formats = None;

        assert!(row.key_supports_api_format("openai:chat"));
        assert!(row.key_supports_api_format("openai:responses"));
    }
}
