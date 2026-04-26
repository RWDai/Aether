mod claude;
mod gemini;
mod shared;

pub use claude::normalize_claude_request_to_openai_chat_request;
pub use gemini::normalize_gemini_request_to_openai_chat_request;
pub use shared::{extract_openai_text_content, parse_openai_tool_result_content};
