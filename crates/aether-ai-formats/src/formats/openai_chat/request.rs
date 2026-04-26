use serde_json::{json, Value};

use crate::{
    canonical::{
        canonical_to_openai_chat_request, from_openai_chat_to_canonical_request, CanonicalRequest,
    },
    context::FormatContext,
};

pub fn from(body: &Value, _ctx: &FormatContext) -> Option<CanonicalRequest> {
    from_openai_chat_to_canonical_request(body)
}

pub fn to(request: &CanonicalRequest, ctx: &FormatContext) -> Option<Value> {
    let mut body = canonical_to_openai_chat_request(request);
    force_stream_options(&mut body, ctx.upstream_is_stream);
    Some(body)
}

fn force_stream_options(body: &mut Value, upstream_is_stream: bool) {
    if !upstream_is_stream {
        return;
    }
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert("stream".to_string(), Value::Bool(true));
    match object.get_mut("stream_options") {
        Some(Value::Object(stream_options)) => {
            stream_options.insert("include_usage".to_string(), Value::Bool(true));
        }
        _ => {
            object.insert(
                "stream_options".to_string(),
                json!({
                    "include_usage": true,
                }),
            );
        }
    }
}
