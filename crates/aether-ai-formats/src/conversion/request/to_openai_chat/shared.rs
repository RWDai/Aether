use serde_json::Value;

pub fn extract_openai_text_content(content: Option<&Value>) -> Option<String> {
    match content {
        None | Some(Value::Null) => Some(String::new()),
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Array(parts)) => {
            let mut collected = Vec::new();
            for part in parts {
                let part_object = part.as_object()?;
                let part_type = part_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if matches!(part_type, "text" | "input_text") {
                    if let Some(text) = part_object.get("text").and_then(Value::as_str) {
                        if !text.trim().is_empty() {
                            collected.push(text.to_string());
                        }
                    }
                }
            }
            Some(collected.join("\n"))
        }
        _ => None,
    }
}

pub fn parse_openai_tool_result_content(content: Option<&Value>) -> Value {
    match content {
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Value::String(String::new())
            } else {
                serde_json::from_str::<Value>(trimmed)
                    .unwrap_or_else(|_| Value::String(raw.clone()))
            }
        }
        Some(Value::Array(parts)) => {
            let texts = parts
                .iter()
                .filter_map(|part| {
                    part.as_object()
                        .and_then(|object| object.get("text"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect::<Vec<_>>();
            if texts.is_empty() {
                Value::Array(parts.clone())
            } else {
                Value::String(texts.join("\n"))
            }
        }
        Some(value) => value.clone(),
        None => Value::String(String::new()),
    }
}

pub(super) fn canonical_json_string(value: Value) -> String {
    match value {
        Value::String(text) => text,
        other => serde_json::to_string(&other).unwrap_or_else(|_| "null".to_string()),
    }
}
