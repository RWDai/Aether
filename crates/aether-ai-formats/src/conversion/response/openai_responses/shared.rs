use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiResponsesResponseUsage {
    pub prompt_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

pub fn build_openai_responses_response(
    response_id: &str,
    model: &str,
    text: &str,
    function_calls: Vec<Value>,
    prompt_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
) -> Value {
    let content = if text.is_empty() {
        Vec::new()
    } else {
        vec![json!({
            "type": "output_text",
            "text": text,
            "annotations": []
        })]
    };
    build_openai_responses_response_with_content(
        response_id,
        model,
        content,
        Vec::new(),
        function_calls,
        OpenAiResponsesResponseUsage {
            prompt_tokens,
            output_tokens,
            total_tokens,
        },
    )
}

pub fn build_openai_responses_response_with_reasoning(
    response_id: &str,
    model: &str,
    text: &str,
    reasoning_summaries: Vec<String>,
    function_calls: Vec<Value>,
    usage: OpenAiResponsesResponseUsage,
) -> Value {
    let content = if text.is_empty() {
        Vec::new()
    } else {
        vec![json!({
            "type": "output_text",
            "text": text,
            "annotations": []
        })]
    };
    build_openai_responses_response_with_content(
        response_id,
        model,
        content,
        reasoning_summaries,
        function_calls,
        usage,
    )
}

pub fn build_openai_responses_response_with_content(
    response_id: &str,
    model: &str,
    content: Vec<Value>,
    reasoning_summaries: Vec<String>,
    function_calls: Vec<Value>,
    usage: OpenAiResponsesResponseUsage,
) -> Value {
    let mut output = Vec::new();
    for (index, summary) in reasoning_summaries.into_iter().enumerate() {
        let trimmed = summary.trim();
        if trimmed.is_empty() {
            continue;
        }
        output.push(json!({
            "type": "reasoning",
            "id": format!("{response_id}_rs_{index}"),
            "status": "completed",
            "summary": [{
                "type": "summary_text",
                "text": trimmed,
            }]
        }));
    }
    if !content.is_empty() {
        output.push(json!({
            "type": "message",
            "id": format!("{response_id}_msg"),
            "role": "assistant",
            "status": "completed",
            "content": content
        }));
    }
    output.extend(function_calls);
    json!({
        "id": response_id,
        "object": "response",
        "status": "completed",
        "model": model,
        "output": output,
        "usage": {
            "input_tokens": usage.prompt_tokens,
            "output_tokens": usage.output_tokens,
            "total_tokens": usage.total_tokens,
        }
    })
}

pub(super) fn build_generated_tool_call_id(index: usize) -> String {
    format!("call_auto_{index}")
}

pub(super) fn canonicalize_tool_arguments(value: Option<Value>) -> String {
    match value {
        Some(Value::String(text)) => text,
        Some(other) => serde_json::to_string(&other).unwrap_or_else(|_| "null".to_string()),
        None => "{}".to_string(),
    }
}
