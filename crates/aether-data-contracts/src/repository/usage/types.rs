use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredRequestUsageAudit {
    pub id: String,
    pub request_id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub username: Option<String>,
    pub api_key_name: Option<String>,
    pub provider_name: String,
    pub model: String,
    pub target_model: Option<String>,
    pub provider_id: Option<String>,
    pub provider_endpoint_id: Option<String>,
    pub provider_api_key_id: Option<String>,
    pub request_type: Option<String>,
    pub api_format: Option<String>,
    pub api_family: Option<String>,
    pub endpoint_kind: Option<String>,
    pub endpoint_api_format: Option<String>,
    pub provider_api_family: Option<String>,
    pub provider_endpoint_kind: Option<String>,
    pub has_format_conversion: bool,
    pub is_stream: bool,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_ephemeral_5m_input_tokens: u64,
    pub cache_creation_ephemeral_1h_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_cost_usd: f64,
    pub cache_read_cost_usd: f64,
    pub output_price_per_1m: Option<f64>,
    pub total_cost_usd: f64,
    pub actual_total_cost_usd: f64,
    pub status_code: Option<u16>,
    pub error_message: Option<String>,
    pub error_category: Option<String>,
    pub response_time_ms: Option<u64>,
    pub first_byte_time_ms: Option<u64>,
    pub status: String,
    pub billing_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_request_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_request_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_response_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_response_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_metadata: Option<Value>,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_secs: u64,
    pub finalized_at_unix_secs: Option<u64>,
}

impl StoredRequestUsageAudit {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        request_id: String,
        user_id: Option<String>,
        api_key_id: Option<String>,
        username: Option<String>,
        api_key_name: Option<String>,
        provider_name: String,
        model: String,
        target_model: Option<String>,
        provider_id: Option<String>,
        provider_endpoint_id: Option<String>,
        provider_api_key_id: Option<String>,
        request_type: Option<String>,
        api_format: Option<String>,
        api_family: Option<String>,
        endpoint_kind: Option<String>,
        endpoint_api_format: Option<String>,
        provider_api_family: Option<String>,
        provider_endpoint_kind: Option<String>,
        has_format_conversion: bool,
        is_stream: bool,
        input_tokens: i32,
        output_tokens: i32,
        total_tokens: i32,
        total_cost_usd: f64,
        actual_total_cost_usd: f64,
        status_code: Option<i32>,
        error_message: Option<String>,
        error_category: Option<String>,
        response_time_ms: Option<i32>,
        first_byte_time_ms: Option<i32>,
        status: String,
        billing_status: String,
        created_at_unix_ms: i64,
        updated_at_unix_secs: i64,
        finalized_at_unix_secs: Option<i64>,
    ) -> Result<Self, crate::DataLayerError> {
        if request_id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.request_id is empty".to_string(),
            ));
        }
        if provider_name.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.provider_name is empty".to_string(),
            ));
        }
        if model.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.model is empty".to_string(),
            ));
        }
        if status.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.status is empty".to_string(),
            ));
        }
        if billing_status.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.billing_status is empty".to_string(),
            ));
        }
        if !total_cost_usd.is_finite() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.total_cost_usd is not finite".to_string(),
            ));
        }
        if !actual_total_cost_usd.is_finite() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "usage.actual_total_cost_usd is not finite".to_string(),
            ));
        }

        Ok(Self {
            id,
            request_id,
            user_id,
            api_key_id,
            username,
            api_key_name,
            provider_name,
            model,
            target_model,
            provider_id,
            provider_endpoint_id,
            provider_api_key_id,
            request_type,
            api_format,
            api_family,
            endpoint_kind,
            endpoint_api_format,
            provider_api_family,
            provider_endpoint_kind,
            has_format_conversion,
            is_stream,
            input_tokens: parse_u64(input_tokens, "usage.input_tokens")?,
            output_tokens: parse_u64(output_tokens, "usage.output_tokens")?,
            total_tokens: parse_u64(total_tokens, "usage.total_tokens")?,
            cache_creation_input_tokens: 0,
            cache_creation_ephemeral_5m_input_tokens: 0,
            cache_creation_ephemeral_1h_input_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_cost_usd: 0.0,
            cache_read_cost_usd: 0.0,
            output_price_per_1m: None,
            total_cost_usd,
            actual_total_cost_usd,
            status_code: parse_u16(status_code, "usage.status_code")?,
            error_message,
            error_category,
            response_time_ms: parse_optional_u64(response_time_ms, "usage.response_time_ms")?,
            first_byte_time_ms: parse_optional_u64(first_byte_time_ms, "usage.first_byte_time_ms")?,
            status,
            billing_status,
            request_headers: None,
            request_body: None,
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
            request_metadata: None,
            created_at_unix_ms: parse_timestamp(created_at_unix_ms, "usage.created_at_unix_ms")?,
            updated_at_unix_secs: parse_timestamp(
                updated_at_unix_secs,
                "usage.updated_at_unix_secs",
            )?,
            finalized_at_unix_secs: finalized_at_unix_secs
                .map(|value| parse_timestamp(value, "usage.finalized_at_unix_secs"))
                .transpose()?,
        })
    }

    pub fn with_cache_input_tokens(
        mut self,
        cache_creation_input_tokens: u64,
        cache_read_input_tokens: u64,
    ) -> Self {
        self.cache_creation_input_tokens = cache_creation_input_tokens;
        self.cache_read_input_tokens = cache_read_input_tokens;
        self
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredProviderUsageWindow {
    pub provider_id: String,
    pub window_start_unix_secs: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time_ms: f64,
    pub total_cost_usd: f64,
}

impl StoredProviderUsageWindow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider_id: String,
        window_start_unix_secs: i64,
        total_requests: i64,
        successful_requests: i64,
        failed_requests: i64,
        avg_response_time_ms: f64,
        total_cost_usd: f64,
    ) -> Result<Self, crate::DataLayerError> {
        if provider_id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "provider usage window provider_id is empty".to_string(),
            ));
        }
        if !avg_response_time_ms.is_finite() || !total_cost_usd.is_finite() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "provider usage window value is not finite".to_string(),
            ));
        }

        Ok(Self {
            provider_id,
            window_start_unix_secs: parse_timestamp(
                window_start_unix_secs,
                "provider_usage_tracking.window_start_unix_secs",
            )?,
            total_requests: parse_timestamp(
                total_requests,
                "provider_usage_tracking.total_requests",
            )?,
            successful_requests: parse_timestamp(
                successful_requests,
                "provider_usage_tracking.successful_requests",
            )?,
            failed_requests: parse_timestamp(
                failed_requests,
                "provider_usage_tracking.failed_requests",
            )?,
            avg_response_time_ms,
            total_cost_usd,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct StoredProviderUsageSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time_ms: f64,
    pub total_cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct UsageAuditListQuery {
    pub created_from_unix_secs: Option<u64>,
    pub created_until_unix_secs: Option<u64>,
    pub user_id: Option<String>,
    pub provider_name: Option<String>,
    pub model: Option<String>,
}

#[async_trait]
pub trait UsageReadRepository: Send + Sync {
    async fn find_by_id(
        &self,
        id: &str,
    ) -> Result<Option<StoredRequestUsageAudit>, crate::DataLayerError>;

    async fn find_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Option<StoredRequestUsageAudit>, crate::DataLayerError>;

    async fn list_usage_audits(
        &self,
        query: &UsageAuditListQuery,
    ) -> Result<Vec<StoredRequestUsageAudit>, crate::DataLayerError>;

    async fn list_recent_usage_audits(
        &self,
        user_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StoredRequestUsageAudit>, crate::DataLayerError>;

    async fn summarize_total_tokens_by_api_key_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<std::collections::BTreeMap<String, u64>, crate::DataLayerError>;

    async fn summarize_provider_usage_since(
        &self,
        provider_id: &str,
        since_unix_secs: u64,
    ) -> Result<StoredProviderUsageSummary, crate::DataLayerError>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UpsertUsageRecord {
    pub request_id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub username: Option<String>,
    pub api_key_name: Option<String>,
    pub provider_name: String,
    pub model: String,
    pub target_model: Option<String>,
    pub provider_id: Option<String>,
    pub provider_endpoint_id: Option<String>,
    pub provider_api_key_id: Option<String>,
    pub request_type: Option<String>,
    pub api_format: Option<String>,
    pub api_family: Option<String>,
    pub endpoint_kind: Option<String>,
    pub endpoint_api_format: Option<String>,
    pub provider_api_family: Option<String>,
    pub provider_endpoint_kind: Option<String>,
    pub has_format_conversion: Option<bool>,
    pub is_stream: Option<bool>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_creation_ephemeral_5m_input_tokens: Option<u64>,
    pub cache_creation_ephemeral_1h_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_cost_usd: Option<f64>,
    pub cache_read_cost_usd: Option<f64>,
    pub output_price_per_1m: Option<f64>,
    pub total_cost_usd: Option<f64>,
    pub actual_total_cost_usd: Option<f64>,
    pub status_code: Option<u16>,
    pub error_message: Option<String>,
    pub error_category: Option<String>,
    pub response_time_ms: Option<u64>,
    pub first_byte_time_ms: Option<u64>,
    pub status: String,
    pub billing_status: String,
    pub request_headers: Option<Value>,
    pub request_body: Option<Value>,
    pub provider_request_headers: Option<Value>,
    pub provider_request_body: Option<Value>,
    pub response_headers: Option<Value>,
    pub response_body: Option<Value>,
    pub client_response_headers: Option<Value>,
    pub client_response_body: Option<Value>,
    pub request_metadata: Option<Value>,
    pub finalized_at_unix_secs: Option<u64>,
    pub created_at_unix_ms: Option<u64>,
    pub updated_at_unix_secs: u64,
}

impl UpsertUsageRecord {
    pub fn validate(&self) -> Result<(), crate::DataLayerError> {
        if self.request_id.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "usage upsert request_id cannot be empty".to_string(),
            ));
        }
        if self.provider_name.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "usage upsert provider_name cannot be empty".to_string(),
            ));
        }
        if self.model.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "usage upsert model cannot be empty".to_string(),
            ));
        }
        if self.status.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "usage upsert status cannot be empty".to_string(),
            ));
        }
        if self.billing_status.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "usage upsert billing_status cannot be empty".to_string(),
            ));
        }
        if let Some(value) = self.total_cost_usd {
            if !value.is_finite() {
                return Err(crate::DataLayerError::InvalidInput(
                    "usage upsert total_cost_usd must be finite".to_string(),
                ));
            }
        }
        if let Some(value) = self.cache_creation_cost_usd {
            if !value.is_finite() {
                return Err(crate::DataLayerError::InvalidInput(
                    "usage upsert cache_creation_cost_usd must be finite".to_string(),
                ));
            }
        }
        if let Some(value) = self.cache_read_cost_usd {
            if !value.is_finite() {
                return Err(crate::DataLayerError::InvalidInput(
                    "usage upsert cache_read_cost_usd must be finite".to_string(),
                ));
            }
        }
        if let Some(value) = self.output_price_per_1m {
            if !value.is_finite() {
                return Err(crate::DataLayerError::InvalidInput(
                    "usage upsert output_price_per_1m must be finite".to_string(),
                ));
            }
        }
        if let Some(value) = self.actual_total_cost_usd {
            if !value.is_finite() {
                return Err(crate::DataLayerError::InvalidInput(
                    "usage upsert actual_total_cost_usd must be finite".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait UsageWriteRepository: Send + Sync {
    async fn upsert(
        &self,
        usage: UpsertUsageRecord,
    ) -> Result<StoredRequestUsageAudit, crate::DataLayerError>;
}

pub trait UsageRepository: UsageReadRepository + UsageWriteRepository + Send + Sync {}

impl<T> UsageRepository for T where T: UsageReadRepository + UsageWriteRepository + Send + Sync {}

fn parse_u64(value: i32, field_name: &str) -> Result<u64, crate::DataLayerError> {
    u64::try_from(value).map_err(|_| {
        crate::DataLayerError::UnexpectedValue(format!("invalid {field_name}: {value}"))
    })
}

fn parse_optional_u64(
    value: Option<i32>,
    field_name: &str,
) -> Result<Option<u64>, crate::DataLayerError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                crate::DataLayerError::UnexpectedValue(format!("invalid {field_name}: {value}"))
            })
        })
        .transpose()
}

fn parse_u16(value: Option<i32>, field_name: &str) -> Result<Option<u16>, crate::DataLayerError> {
    value
        .map(|value| {
            u16::try_from(value).map_err(|_| {
                crate::DataLayerError::UnexpectedValue(format!("invalid {field_name}: {value}"))
            })
        })
        .transpose()
}

fn parse_timestamp(value: i64, field_name: &str) -> Result<u64, crate::DataLayerError> {
    u64::try_from(value).map_err(|_| {
        crate::DataLayerError::UnexpectedValue(format!("invalid {field_name}: {value}"))
    })
}

#[cfg(test)]
mod tests {
    use super::{StoredRequestUsageAudit, UpsertUsageRecord};
    use serde_json::json;

    #[test]
    fn rejects_empty_request_id() {
        assert!(StoredRequestUsageAudit::new(
            "usage-1".to_string(),
            "".to_string(),
            None,
            None,
            None,
            None,
            "OpenAI".to_string(),
            "gpt-4.1".to_string(),
            None,
            None,
            None,
            None,
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            false,
            false,
            10,
            20,
            30,
            0.1,
            0.1,
            Some(200),
            None,
            None,
            Some(120),
            Some(80),
            "completed".to_string(),
            "settled".to_string(),
            100,
            101,
            Some(102),
        )
        .is_err());
    }

    #[test]
    fn rejects_negative_token_count() {
        assert!(StoredRequestUsageAudit::new(
            "usage-1".to_string(),
            "req-1".to_string(),
            None,
            None,
            None,
            None,
            "OpenAI".to_string(),
            "gpt-4.1".to_string(),
            None,
            None,
            None,
            None,
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            false,
            false,
            -1,
            20,
            30,
            0.1,
            0.1,
            Some(200),
            None,
            None,
            Some(120),
            Some(80),
            "completed".to_string(),
            "settled".to_string(),
            100,
            101,
            Some(102),
        )
        .is_err());
    }

    #[test]
    fn rejects_invalid_upsert_payload() {
        let record = UpsertUsageRecord {
            request_id: "".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: None,
            provider_endpoint_id: None,
            provider_api_key_id: None,
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: Some(false),
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: None,
            cache_creation_ephemeral_1h_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_cost_usd: None,
            cache_read_cost_usd: None,
            output_price_per_1m: None,
            total_cost_usd: None,
            actual_total_cost_usd: None,
            status_code: Some(200),
            error_message: None,
            error_category: None,
            response_time_ms: Some(120),
            first_byte_time_ms: None,
            status: "completed".to_string(),
            billing_status: "pending".to_string(),
            request_headers: Some(json!({"authorization": "Bearer test"})),
            request_body: Some(json!({"model": "gpt-5"})),
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
            request_metadata: None,
            finalized_at_unix_secs: None,
            created_at_unix_ms: Some(100),
            updated_at_unix_secs: 101,
        };

        assert!(record.validate().is_err());
    }
}
