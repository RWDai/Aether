use serde_json::json;
use serde_json::Value;

use crate::formats::context::FormatContext;
use crate::formats::openai::embedding::request::mapped_embedding_model;
use crate::protocol::canonical::CanonicalRequest;

pub fn to(request: &CanonicalRequest, ctx: &FormatContext) -> Option<Value> {
    let embedding = request.embedding.as_ref()?;
    let items = embedding.input.as_string_items()?;
    if items.is_empty() || items.iter().any(|value| value.trim().is_empty()) {
        return None;
    }
    let model = mapped_embedding_model(request, ctx.mapped_model_or(request.model.as_str()));
    if items.len() == 1 {
        return Some(json!({
            "model": model,
            "content": {
                "parts": [{"text": items[0]}]
            }
        }));
    }
    Some(json!({
        "model": model,
        "requests": items.into_iter().map(|text| {
            json!({
                "model": model,
                "content": {
                    "parts": [{"text": text}]
                }
            })
        }).collect::<Vec<_>>()
    }))
}
