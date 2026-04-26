//! Pairwise request adapters kept for compatibility and focused tests.
//!
//! New request routing should use the registry so every conversion passes
//! through the typed canonical IR.

pub mod from_openai_chat;
pub mod openai_responses;
pub mod to_openai_chat;

pub use from_openai_chat::{
    convert_openai_chat_request_to_claude_request, convert_openai_chat_request_to_gemini_request,
};
pub use openai_responses::{
    convert_openai_chat_request_to_openai_responses_request,
    normalize_openai_responses_request_to_openai_chat_request,
};
pub use to_openai_chat::{
    extract_openai_text_content, normalize_claude_request_to_openai_chat_request,
    normalize_gemini_request_to_openai_chat_request, parse_openai_tool_result_content,
};
