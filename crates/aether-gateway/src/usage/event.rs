use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use aether_data::DataLayerError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const USAGE_EVENT_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UsageEventType {
    Pending,
    Streaming,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub(crate) struct UsageEventData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api_key_name: Option<String>,
    pub(crate) provider_name: String,
    pub(crate) model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_api_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api_family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint_api_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_api_family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_endpoint_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) has_format_conversion: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) is_stream: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache_creation_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache_read_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) output_price_per_1m: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) total_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) actual_total_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) response_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) first_byte_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_request_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_request_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) response_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) response_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) client_response_headers: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) client_response_body: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) request_metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct UsageEvent {
    pub(crate) event_type: UsageEventType,
    pub(crate) request_id: String,
    pub(crate) timestamp_ms: u64,
    pub(crate) data: UsageEventData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UsageEventEnvelope {
    v: u8,
    #[serde(rename = "type")]
    event_type: UsageEventType,
    request_id: String,
    timestamp_ms: u64,
    data: UsageEventData,
}

impl UsageEvent {
    pub(crate) fn new(
        event_type: UsageEventType,
        request_id: impl Into<String>,
        data: UsageEventData,
    ) -> Self {
        Self {
            event_type,
            request_id: request_id.into(),
            timestamp_ms: now_ms(),
            data,
        }
    }

    pub(crate) fn to_stream_fields(&self) -> Result<BTreeMap<String, String>, DataLayerError> {
        let payload = UsageEventEnvelope {
            v: USAGE_EVENT_VERSION,
            event_type: self.event_type,
            request_id: self.request_id.clone(),
            timestamp_ms: self.timestamp_ms,
            data: self.data.clone(),
        };
        let payload = serde_json::to_string(&payload).map_err(|err| {
            DataLayerError::UnexpectedValue(format!(
                "failed to serialize usage event payload: {err}"
            ))
        })?;
        Ok(BTreeMap::from([("payload".to_string(), payload)]))
    }

    pub(crate) fn from_stream_fields(
        fields: &BTreeMap<String, String>,
    ) -> Result<Self, DataLayerError> {
        let payload = fields.get("payload").ok_or_else(|| {
            DataLayerError::UnexpectedValue(
                "usage event stream entry missing payload field".to_string(),
            )
        })?;
        let envelope: UsageEventEnvelope = serde_json::from_str(payload).map_err(|err| {
            DataLayerError::UnexpectedValue(format!(
                "failed to deserialize usage event payload: {err}"
            ))
        })?;
        if envelope.v != USAGE_EVENT_VERSION {
            return Err(DataLayerError::UnexpectedValue(format!(
                "unsupported usage event version: {}",
                envelope.v
            )));
        }

        Ok(Self {
            event_type: envelope.event_type,
            request_id: envelope.request_id,
            timestamp_ms: envelope.timestamp_ms,
            data: envelope.data,
        })
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{UsageEvent, UsageEventData, UsageEventType};

    #[test]
    fn usage_event_round_trips_through_stream_fields() {
        let event = UsageEvent::new(
            UsageEventType::Completed,
            "req-1",
            UsageEventData {
                provider_name: "OpenAI".to_string(),
                model: "gpt-5".to_string(),
                input_tokens: Some(10),
                output_tokens: Some(20),
                ..UsageEventData::default()
            },
        );

        let fields = event.to_stream_fields().expect("event should serialize");
        let parsed = UsageEvent::from_stream_fields(&fields).expect("event should parse");

        assert_eq!(parsed.request_id, "req-1");
        assert_eq!(parsed.event_type, UsageEventType::Completed);
        assert_eq!(parsed.data.total_tokens, None);
        assert_eq!(parsed.data.output_tokens, Some(20));
    }
}
