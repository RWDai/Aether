//! Pairwise response adapters kept for compatibility and focused tests.
//!
//! New response routing should use the registry so every conversion passes
//! through the typed canonical IR.

pub mod from_openai_chat;
pub mod openai_responses;
pub mod to_openai_chat;

pub use from_openai_chat::{
    convert_openai_chat_response_to_claude_chat, convert_openai_chat_response_to_gemini_chat,
};
pub use openai_responses::{
    build_openai_responses_response, build_openai_responses_response_with_content,
    build_openai_responses_response_with_reasoning, convert_claude_response_to_openai_responses,
    convert_gemini_response_to_openai_responses, convert_openai_chat_response_to_openai_responses,
    convert_openai_responses_response_to_openai_chat, OpenAiResponsesResponseUsage,
};
pub use to_openai_chat::{
    convert_claude_chat_response_to_openai_chat, convert_gemini_chat_response_to_openai_chat,
};
