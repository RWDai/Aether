use serde_json::Value;

use crate::{
    canonical::{
        canonical_to_gemini_response, from_gemini_to_canonical_response, CanonicalResponse,
    },
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalResponse> {
    from_gemini_to_canonical_response(body)
}

pub fn to(response: &CanonicalResponse, ctx: &FormatContext) -> Option<Value> {
    canonical_to_gemini_response(response, &ctx.report_context_value())
}
