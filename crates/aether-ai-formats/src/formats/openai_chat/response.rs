use serde_json::Value;

use crate::{
    canonical::{
        canonical_to_openai_chat_response, from_openai_chat_to_canonical_response,
        CanonicalResponse,
    },
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalResponse> {
    from_openai_chat_to_canonical_response(body)
}

pub fn to(response: &CanonicalResponse, ctx: &FormatContext) -> Option<Value> {
    let mut body = canonical_to_openai_chat_response(response);
    if body.get("service_tier").is_none() {
        if let Some(service_tier) = ctx
            .report_context_value()
            .get("original_request_body")
            .and_then(Value::as_object)
            .and_then(|request| request.get("service_tier"))
            .cloned()
        {
            body["service_tier"] = service_tier;
        }
    }
    Some(body)
}
