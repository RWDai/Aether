use serde_json::{json, Map, Value};

pub(super) fn parse_openai_function_arguments(arguments: Option<&Value>) -> Option<Value> {
    match arguments.cloned().unwrap_or(Value::Object(Map::new())) {
        Value::Object(object) => Some(Value::Object(object)),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Some(Value::Object(Map::new()))
            } else {
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(Value::Object(object)) => Some(Value::Object(object)),
                    Ok(other) => Some(json!({ "raw": other })),
                    Err(_) => Some(json!({ "raw": text })),
                }
            }
        }
        other => Some(json!({ "raw": other })),
    }
}

pub(super) fn build_generated_tool_call_id(index: usize) -> String {
    format!("call_auto_{index}")
}
