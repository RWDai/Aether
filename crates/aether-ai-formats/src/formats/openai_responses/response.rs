use serde_json::Value;

use crate::{
    canonical::{
        canonical_to_openai_responses_compact_response, canonical_to_openai_responses_response,
        from_openai_responses_to_canonical_response, CanonicalResponse,
    },
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalResponse> {
    from_openai_responses_to_canonical_response(body)
}

pub fn to(response: &CanonicalResponse, ctx: &FormatContext) -> Option<Value> {
    Some(canonical_to_openai_responses_response(
        response,
        &ctx.report_context_value(),
    ))
}

pub fn to_compact(response: &CanonicalResponse, ctx: &FormatContext) -> Option<Value> {
    Some(canonical_to_openai_responses_compact_response(
        response,
        &ctx.report_context_value(),
    ))
}
