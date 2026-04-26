use serde_json::Value;

use crate::{
    canonical::{
        canonical_to_claude_response, from_claude_to_canonical_response, CanonicalResponse,
    },
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalResponse> {
    from_claude_to_canonical_response(body)
}

pub fn to(response: &CanonicalResponse, _ctx: &FormatContext) -> Option<Value> {
    Some(canonical_to_claude_response(response))
}
