use serde_json::{json, Map, Value};

use super::super::to_openai_chat::{extract_openai_text_content, parse_openai_tool_result_content};
use crate::planner::openai::extract_openai_reasoning_effort;

pub fn normalize_openai_responses_request_to_openai_chat_request(
    body_json: &Value,
) -> Option<Value> {
    let request = body_json.as_object()?;
    let mut output = Map::new();
    if let Some(model) = request.get("model") {
        output.insert("model".to_string(), model.clone());
    }

    let mut messages = Vec::new();
    if let Some(instructions) = request.get("instructions") {
        let text = extract_openai_text_content(Some(instructions))?;
        if !text.trim().is_empty() {
            messages.push(json!({
                "role": "system",
                "content": text,
            }));
        }
    }
    messages.extend(normalize_openai_responses_input_to_openai_chat_messages(
        request.get("input"),
    )?);
    output.insert("messages".to_string(), Value::Array(messages));

    if let Some(max_output_tokens) = request.get("max_output_tokens").cloned() {
        output.insert("max_completion_tokens".to_string(), max_output_tokens);
    }
    for passthrough_key in [
        "temperature",
        "top_p",
        "metadata",
        "store",
        "service_tier",
        "prompt_cache_key",
        "prompt_cache_retention",
        "parallel_tool_calls",
        "stop",
        "stream",
        "stream_options",
        "user",
        "safety_identifier",
        "top_logprobs",
    ] {
        if let Some(value) = request.get(passthrough_key) {
            output.insert(passthrough_key.to_string(), value.clone());
        }
    }
    if let Some(reasoning_effort) = extract_openai_reasoning_effort(request) {
        output.insert(
            "reasoning_effort".to_string(),
            Value::String(reasoning_effort),
        );
    }
    if let Some(response_format) = request
        .get("text")
        .and_then(Value::as_object)
        .and_then(|text| text.get("format"))
        .cloned()
    {
        output.insert("response_format".to_string(), response_format);
    }
    if let Some(verbosity) = request
        .get("text")
        .and_then(Value::as_object)
        .and_then(|text| text.get("verbosity"))
        .cloned()
    {
        output.insert("verbosity".to_string(), verbosity);
    }
    if let Some(tools) = normalize_openai_responses_tools_to_openai_chat(request.get("tools"))? {
        output.insert("tools".to_string(), Value::Array(tools));
    }
    if let Some(web_search_options) =
        extract_openai_responses_web_search_options(request.get("tools").and_then(Value::as_array))
    {
        output.insert("web_search_options".to_string(), web_search_options);
    }
    if let Some(tool_choice) =
        normalize_openai_responses_tool_choice_to_openai_chat(request.get("tool_choice"))?
    {
        output.insert("tool_choice".to_string(), tool_choice);
    }

    Some(Value::Object(output))
}

fn normalize_openai_responses_input_to_openai_chat_messages(
    input: Option<&Value>,
) -> Option<Vec<Value>> {
    let Some(input) = input else {
        return Some(Vec::new());
    };
    match input {
        Value::Null => Some(Vec::new()),
        Value::String(text) => {
            if text.trim().is_empty() {
                Some(Vec::new())
            } else {
                Some(vec![json!({
                    "role": "user",
                    "content": text,
                })])
            }
        }
        Value::Array(items) => {
            let mut messages = Vec::new();
            let mut next_generated_tool_call_index = 0usize;
            for item in items {
                if let Some(item_text) = item.as_str() {
                    if !item_text.trim().is_empty() {
                        messages.push(json!({
                            "role": "user",
                            "content": item_text,
                        }));
                    }
                    continue;
                }
                let item_object = item.as_object()?;
                let item_type = item_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("message")
                    .trim()
                    .to_ascii_lowercase();
                match item_type.as_str() {
                    "message" => {
                        let role = item_object
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or("user")
                            .trim()
                            .to_ascii_lowercase();
                        if role == "system" || role == "developer" {
                            let text = extract_openai_text_content(item_object.get("content"))?;
                            if !text.trim().is_empty() {
                                messages.push(json!({
                                    "role": "system",
                                    "content": text,
                                }));
                            }
                            continue;
                        }
                        let normalized_content =
                            normalize_openai_responses_message_content(item_object.get("content"))?;
                        let mut message = serde_json::Map::new();
                        message.insert("role".to_string(), Value::String(role.clone()));
                        message.insert("content".to_string(), normalized_content);
                        if role == "assistant" {
                            if let Some(refusal) = extract_openai_responses_message_refusal(
                                item_object.get("content"),
                            )? {
                                message.insert("refusal".to_string(), Value::String(refusal));
                            }
                        }
                        messages.push(Value::Object(message));
                    }
                    "function_call" => {
                        let tool_name = item_object
                            .get("name")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())?;
                        let call_id = item_object
                            .get("call_id")
                            .or_else(|| item_object.get("id"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| {
                                let generated =
                                    format!("call_auto_{next_generated_tool_call_index}");
                                next_generated_tool_call_index += 1;
                                generated
                            });
                        let arguments = item_object
                            .get("arguments")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| "{}".to_string());
                        messages.push(json!({
                            "role": "assistant",
                            "content": Value::Array(Vec::new()),
                            "tool_calls": [{
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": tool_name,
                                    "arguments": arguments,
                                }
                            }]
                        }));
                    }
                    "function_call_output" => {
                        let tool_call_id = item_object
                            .get("call_id")
                            .or_else(|| item_object.get("tool_call_id"))
                            .or_else(|| item_object.get("id"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| {
                                let generated =
                                    format!("call_auto_{next_generated_tool_call_index}");
                                next_generated_tool_call_index += 1;
                                generated
                            });
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": tool_call_id,
                            "content": parse_openai_tool_result_content(item_object.get("output")),
                        }));
                    }
                    _ => {}
                }
            }
            Some(messages)
        }
        _ => None,
    }
}

fn normalize_openai_responses_message_content(content: Option<&Value>) -> Option<Value> {
    let Some(content) = content else {
        return Some(Value::Array(Vec::new()));
    };
    match content {
        Value::String(text) => Some(Value::String(text.clone())),
        Value::Array(parts) => {
            let mut normalized = Vec::new();
            for part in parts {
                let part_object = part.as_object()?;
                let part_type = part_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                match part_type.as_str() {
                    "input_text" | "output_text" | "text" => {
                        if let Some(text) = part_object.get("text").and_then(Value::as_str) {
                            normalized.push(json!({
                                "type": "text",
                                "text": text,
                            }));
                        }
                    }
                    "input_image" | "output_image" | "image_url" => {
                        let image_url = part_object
                            .get("image_url")
                            .and_then(|value| {
                                value.as_str().map(ToOwned::to_owned).or_else(|| {
                                    value
                                        .as_object()
                                        .and_then(|object| object.get("url"))
                                        .and_then(Value::as_str)
                                        .map(ToOwned::to_owned)
                                })
                            })
                            .or_else(|| {
                                part_object
                                    .get("url")
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned)
                            })?;
                        let detail = part_object
                            .get("detail")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                            .or_else(|| {
                                part_object
                                    .get("image_url")
                                    .and_then(Value::as_object)
                                    .and_then(|image| image.get("detail"))
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned)
                            });
                        let mut image = Map::new();
                        image.insert("url".to_string(), Value::String(image_url));
                        if let Some(detail) = detail {
                            image.insert("detail".to_string(), Value::String(detail));
                        }
                        normalized.push(json!({
                            "type": "image_url",
                            "image_url": image,
                        }));
                    }
                    "input_file" => {
                        let mut file = Map::new();
                        if let Some(file_data) = part_object.get("file_data").cloned() {
                            file.insert("file_data".to_string(), file_data);
                        }
                        if let Some(file_id) = part_object.get("file_id").cloned() {
                            file.insert("file_id".to_string(), file_id);
                        }
                        if let Some(filename) = part_object.get("filename").cloned() {
                            file.insert("filename".to_string(), filename);
                        }
                        if !file.is_empty() {
                            normalized.push(json!({
                                "type": "file",
                                "file": file,
                            }));
                        }
                    }
                    _ => {}
                }
            }
            Some(Value::Array(normalized))
        }
        _ => Some(content.clone()),
    }
}

fn extract_openai_responses_message_refusal(content: Option<&Value>) -> Option<Option<String>> {
    let Some(content) = content else {
        return Some(None);
    };
    match content {
        Value::Array(parts) => {
            let mut refusals = Vec::new();
            for part in parts {
                let part_object = part.as_object()?;
                let part_type = part_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                if part_type == "refusal" {
                    if let Some(refusal) = part_object.get("refusal").and_then(Value::as_str) {
                        if !refusal.trim().is_empty() {
                            refusals.push(refusal.to_string());
                        }
                    }
                }
            }
            if refusals.is_empty() {
                Some(None)
            } else {
                Some(Some(refusals.join("\n")))
            }
        }
        _ => Some(None),
    }
}

fn normalize_openai_responses_tools_to_openai_chat(
    tools: Option<&Value>,
) -> Option<Option<Vec<Value>>> {
    let Some(Value::Array(tool_values)) = tools else {
        return Some(None);
    };
    let mut normalized = Vec::new();
    for tool in tool_values {
        let tool_object = tool.as_object()?;
        let tool_type = tool_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("function")
            .trim()
            .to_ascii_lowercase();
        if tool_type.starts_with("web_search") {
            continue;
        }
        if tool_object.get("function").is_some() || tool_type != "function" {
            continue;
        }
        let name = tool_object
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())?;
        let mut function = Map::new();
        function.insert("name".to_string(), Value::String(name.to_string()));
        if let Some(description) = tool_object.get("description") {
            function.insert("description".to_string(), description.clone());
        }
        if let Some(parameters) = tool_object.get("parameters") {
            function.insert("parameters".to_string(), parameters.clone());
        }
        normalized.push(json!({
            "type": "function",
            "function": function,
        }));
    }
    Some((!normalized.is_empty()).then_some(normalized))
}

fn extract_openai_responses_web_search_options(tools: Option<&Vec<Value>>) -> Option<Value> {
    let tool_values = tools?;
    for tool in tool_values {
        let tool_object = tool.as_object()?;
        let tool_type = tool_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if !tool_type.starts_with("web_search") {
            continue;
        }
        let mut options = Map::new();
        if let Some(search_context_size) = tool_object.get("search_context_size").cloned() {
            options.insert("search_context_size".to_string(), search_context_size);
        }
        if let Some(user_location) = tool_object.get("user_location").and_then(Value::as_object) {
            let mut approximate = Map::new();
            for field in ["city", "country", "region", "timezone"] {
                if let Some(value) = user_location.get(field).cloned() {
                    approximate.insert(field.to_string(), value);
                }
            }
            if !approximate.is_empty() {
                options.insert(
                    "user_location".to_string(),
                    json!({
                        "type": "approximate",
                        "approximate": approximate,
                    }),
                );
            }
        }
        if !options.is_empty() {
            return Some(Value::Object(options));
        }
    }
    None
}

fn normalize_openai_responses_tool_choice_to_openai_chat(
    tool_choice: Option<&Value>,
) -> Option<Option<Value>> {
    let Some(tool_choice) = tool_choice else {
        return Some(None);
    };
    match tool_choice {
        Value::Object(object)
            if object.get("function").is_none()
                && object
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case("function")) =>
        {
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            Some(Some(json!({
                "type": "function",
                "function": {
                    "name": name,
                }
            })))
        }
        _ => Some(Some(tool_choice.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_openai_responses_request_to_openai_chat_request;
    use serde_json::json;

    #[test]
    fn preserves_openai_responses_text_and_passthrough_fields_when_normalizing_to_chat() {
        let request = json!({
            "model": "gpt-5",
            "max_output_tokens": 128,
            "input": [{
                "role": "user",
                "content": [{"type": "input_text", "text": "hi"}]
            }],
            "text": {
                "format": {
                    "type": "json_schema",
                    "json_schema": {"name": "answer", "schema": {"type": "object"}}
                },
                "verbosity": "high"
            },
            "prompt_cache_key": "cache-key-456",
            "prompt_cache_retention": "persist",
            "service_tier": "flex",
            "user": "user-456",
            "safety_identifier": "safe-456",
            "top_logprobs": 4
        });

        let converted = normalize_openai_responses_request_to_openai_chat_request(&request)
            .expect("responses request should normalize to chat");

        assert_eq!(converted["max_completion_tokens"], 128);
        assert_eq!(
            converted["response_format"],
            json!({
                "type": "json_schema",
                "json_schema": {"name": "answer", "schema": {"type": "object"}}
            })
        );
        assert_eq!(converted["verbosity"], "high");
        assert_eq!(converted["prompt_cache_key"], "cache-key-456");
        assert_eq!(converted["prompt_cache_retention"], "persist");
        assert_eq!(converted["service_tier"], "flex");
        assert_eq!(converted["user"], "user-456");
        assert_eq!(converted["safety_identifier"], "safe-456");
        assert_eq!(converted["top_logprobs"], 4);
    }

    #[test]
    fn preserves_assistant_refusal_when_normalizing_to_chat() {
        let request = json!({
            "model": "gpt-5",
            "input": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "refusal", "refusal": "cannot comply"}]
            }]
        });

        let converted = normalize_openai_responses_request_to_openai_chat_request(&request)
            .expect("responses request should normalize to chat");

        assert_eq!(converted["messages"][0]["role"], "assistant");
        assert_eq!(converted["messages"][0]["refusal"], "cannot comply");
        assert_eq!(converted["messages"][0]["content"], json!([]));
    }

    #[test]
    fn passes_through_stream_options() {
        let request = json!({
            "model": "gpt-5",
            "stream": true,
            "stream_options": {
                "include_usage": true
            },
            "input": "hello"
        });

        let converted = normalize_openai_responses_request_to_openai_chat_request(&request)
            .expect("responses request should normalize to chat");

        assert_eq!(converted["stream"], true);
        assert_eq!(converted["stream_options"]["include_usage"], true);
    }

    #[test]
    fn preserves_stream_options_without_forcing_include_usage_during_normalization() {
        let request = json!({
            "model": "gpt-5",
            "stream": true,
            "stream_options": {
                "include_usage": false,
                "extra": "keep-me"
            },
            "input": "hello"
        });

        let converted = normalize_openai_responses_request_to_openai_chat_request(&request)
            .expect("responses request should normalize to chat");

        assert_eq!(converted["stream_options"]["include_usage"], false);
        assert_eq!(converted["stream_options"]["extra"], "keep-me");
    }
}
