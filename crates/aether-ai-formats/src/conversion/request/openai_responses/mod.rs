mod from_chat;
mod to_chat;

pub use from_chat::convert_openai_chat_request_to_openai_responses_request;
pub use to_chat::normalize_openai_responses_request_to_openai_chat_request;

#[cfg(test)]
mod tests {
    use super::{
        convert_openai_chat_request_to_openai_responses_request,
        normalize_openai_responses_request_to_openai_chat_request,
    };
    use serde_json::json;

    #[test]
    fn converts_chat_to_responses_wire_shape() {
        let request = json!({
            "model": "gpt-5",
            "messages": [{"role": "user", "content": "hello"}],
            "max_completion_tokens": 16
        });

        let converted = convert_openai_chat_request_to_openai_responses_request(
            &request,
            "gpt-5-mini",
            false,
            false,
        )
        .expect("responses request");

        assert_eq!(converted["model"], "gpt-5-mini");
        assert_eq!(converted["input"][0]["type"], "message");
        assert_eq!(converted["max_output_tokens"], 16);
    }

    #[test]
    fn normalizes_responses_wire_shape_to_chat() {
        let request = json!({
            "model": "gpt-5",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}]
            }]
        });

        let converted = normalize_openai_responses_request_to_openai_chat_request(&request)
            .expect("chat request");

        assert_eq!(converted["messages"][0]["role"], "user");
        assert_eq!(converted["messages"][0]["content"][0]["type"], "text");
        assert_eq!(converted["messages"][0]["content"][0]["text"], "hello");
    }
}
