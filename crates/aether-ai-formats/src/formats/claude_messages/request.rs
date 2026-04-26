use serde_json::Value;

use crate::{
    canonical::{canonical_to_claude_request, from_claude_to_canonical_request, CanonicalRequest},
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalRequest> {
    from_claude_to_canonical_request(body)
}

pub fn to(request: &CanonicalRequest, ctx: &FormatContext) -> Option<Value> {
    canonical_to_claude_request(
        request,
        ctx.mapped_model_or(request.model.as_str()),
        ctx.upstream_is_stream,
    )
}
