use super::*;

pub(super) fn augment_sync_report_context(
    report_context: Option<serde_json::Value>,
    provider_request_headers: &BTreeMap<String, String>,
    provider_request_body: &serde_json::Value,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let mut report_context = match report_context {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => serde_json::Map::new(),
        None => serde_json::Map::new(),
    };

    report_context.insert(
        "provider_request_headers".to_string(),
        serde_json::to_value(provider_request_headers)
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    report_context.insert(
        "provider_request_body".to_string(),
        provider_request_body.clone(),
    );

    Ok(Some(serde_json::Value::Object(report_context)))
}
