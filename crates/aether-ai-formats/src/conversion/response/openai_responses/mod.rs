mod from_chat;
mod shared;
mod to_chat;

pub use from_chat::convert_openai_chat_response_to_openai_responses;
pub use shared::{
    build_openai_responses_response, build_openai_responses_response_with_content,
    build_openai_responses_response_with_reasoning, OpenAiResponsesResponseUsage,
};
pub use to_chat::convert_openai_responses_response_to_openai_chat;

use serde_json::Value;

pub fn convert_claude_response_to_openai_responses(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let chat_response = super::to_openai_chat::convert_claude_chat_response_to_openai_chat(
        body_json,
        report_context,
    )?;
    convert_openai_chat_response_to_openai_responses(&chat_response, report_context, false)
}

pub fn convert_gemini_response_to_openai_responses(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let chat_response = super::to_openai_chat::convert_gemini_chat_response_to_openai_chat(
        body_json,
        report_context,
    )?;
    convert_openai_chat_response_to_openai_responses(&chat_response, report_context, false)
}

#[cfg(test)]
mod tests {
    use super::{
        convert_openai_chat_response_to_openai_responses,
        convert_openai_responses_response_to_openai_chat,
    };
    use serde_json::json;

    #[test]
    fn converts_chat_response_to_responses_wire_shape() {
        let response = json!({
            "id": "chatcmpl_1",
            "object": "chat.completion",
            "model": "gpt-5",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "done"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });

        let converted =
            convert_openai_chat_response_to_openai_responses(&response, &json!({}), false)
                .expect("responses response");

        assert_eq!(converted["object"], "response");
        assert_eq!(converted["output"][0]["content"][0]["text"], "done");
        assert_eq!(converted["usage"]["input_tokens"], 1);
    }

    #[test]
    fn converts_responses_wire_shape_to_chat_response() {
        let response = json!({
            "id": "resp_1",
            "object": "response",
            "status": "completed",
            "model": "gpt-5",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done", "annotations": []}]
            }]
        });

        let converted = convert_openai_responses_response_to_openai_chat(&response, &json!({}))
            .expect("chat response");

        assert_eq!(converted["object"], "chat.completion");
        assert_eq!(converted["choices"][0]["message"]["content"], "done");
    }
}
