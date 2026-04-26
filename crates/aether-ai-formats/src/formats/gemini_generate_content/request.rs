use serde_json::Value;

use crate::{
    canonical::{canonical_to_gemini_request, from_gemini_to_canonical_request, CanonicalRequest},
    context::FormatContext,
};

pub fn from(body: &Value, ctx: &FormatContext) -> Option<CanonicalRequest> {
    from_gemini_to_canonical_request(body, ctx.request_path.as_deref().unwrap_or_default())
}

pub fn to(request: &CanonicalRequest, ctx: &FormatContext) -> Option<Value> {
    canonical_to_gemini_request(
        request,
        ctx.mapped_model_or(request.model.as_str()),
        ctx.upstream_is_stream,
    )
}
