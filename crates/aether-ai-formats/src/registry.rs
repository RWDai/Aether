use std::{error::Error, fmt};

use serde_json::{json, Value};

use crate::{
    canonical::{
        canonical_to_claude_request, canonical_to_claude_response, canonical_to_gemini_request,
        canonical_to_gemini_response, canonical_to_openai_chat_request,
        canonical_to_openai_chat_response, canonical_to_openai_responses_compact_request,
        canonical_to_openai_responses_compact_response, canonical_to_openai_responses_request,
        canonical_to_openai_responses_response, from_claude_to_canonical_request,
        from_claude_to_canonical_response, from_gemini_to_canonical_request,
        from_gemini_to_canonical_response, from_openai_chat_to_canonical_request,
        from_openai_chat_to_canonical_response, from_openai_responses_to_canonical_request,
        from_openai_responses_to_canonical_response, CanonicalRequest, CanonicalResponse,
    },
    formats::FormatId,
};

#[derive(Debug, Clone, Default)]
pub struct FormatContext {
    pub mapped_model: Option<String>,
    pub request_path: Option<String>,
    pub upstream_is_stream: bool,
    pub report_context: Option<Value>,
}

impl FormatContext {
    pub fn with_mapped_model(mut self, mapped_model: impl Into<String>) -> Self {
        self.mapped_model = Some(mapped_model.into());
        self
    }

    pub fn with_request_path(mut self, request_path: impl Into<String>) -> Self {
        self.request_path = Some(request_path.into());
        self
    }

    pub fn with_upstream_stream(mut self, upstream_is_stream: bool) -> Self {
        self.upstream_is_stream = upstream_is_stream;
        self
    }

    pub fn with_report_context(mut self, report_context: Value) -> Self {
        self.report_context = Some(report_context);
        self
    }

    fn mapped_model_or<'a>(&'a self, fallback: &'a str) -> &'a str {
        self.mapped_model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(fallback)
    }

    fn report_context_value(&self) -> Value {
        self.report_context.clone().unwrap_or_else(|| {
            json!({
                "mapped_model": self.mapped_model,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    UnsupportedFormat(String),
    RequestParseFailed { format: String },
    RequestEmitFailed { format: String },
    ResponseParseFailed { format: String },
    ResponseEmitFailed { format: String },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat(format) => write!(f, "unsupported AI format: {format}"),
            Self::RequestParseFailed { format } => {
                write!(f, "failed to parse {format} request")
            }
            Self::RequestEmitFailed { format } => write!(f, "failed to emit {format} request"),
            Self::ResponseParseFailed { format } => {
                write!(f, "failed to parse {format} response")
            }
            Self::ResponseEmitFailed { format } => write!(f, "failed to emit {format} response"),
        }
    }
}

impl Error for FormatError {}

pub fn parse_request(
    source_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<CanonicalRequest, FormatError> {
    let source = parse_format(source_format)?;
    match source {
        FormatId::OpenAiChat => from_openai_chat_to_canonical_request(body),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            from_openai_responses_to_canonical_request(body)
        }
        FormatId::ClaudeMessages => from_claude_to_canonical_request(body),
        FormatId::GeminiGenerateContent => {
            from_gemini_to_canonical_request(body, ctx.request_path.as_deref().unwrap_or_default())
        }
    }
    .ok_or_else(|| FormatError::RequestParseFailed {
        format: source.as_str().to_string(),
    })
}

pub fn emit_request(
    target_format: &str,
    request: &CanonicalRequest,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let target = parse_format(target_format)?;
    let mut request = request.clone();
    if let Some(mapped_model) = ctx
        .mapped_model
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        request.model = mapped_model.to_string();
    }
    let mapped_model = ctx.mapped_model_or(request.model.as_str());
    match target {
        FormatId::OpenAiChat => {
            let mut body = canonical_to_openai_chat_request(&request);
            force_openai_chat_stream_options(&mut body, ctx.upstream_is_stream);
            Some(body)
        }
        FormatId::OpenAiResponses => {
            canonical_to_openai_responses_request(&request, mapped_model, ctx.upstream_is_stream)
        }
        FormatId::OpenAiResponsesCompact => {
            canonical_to_openai_responses_compact_request(&request, mapped_model)
        }
        FormatId::ClaudeMessages => {
            canonical_to_claude_request(&request, mapped_model, ctx.upstream_is_stream)
        }
        FormatId::GeminiGenerateContent => {
            canonical_to_gemini_request(&request, mapped_model, ctx.upstream_is_stream)
        }
    }
    .ok_or_else(|| FormatError::RequestEmitFailed {
        format: target.as_str().to_string(),
    })
}

pub fn convert_request(
    source_format: &str,
    target_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let request = parse_request(source_format, body, ctx)?;
    emit_request(target_format, &request, ctx)
}

pub fn parse_response(
    source_format: &str,
    body: &Value,
    _ctx: &FormatContext,
) -> Result<CanonicalResponse, FormatError> {
    let source = parse_format(source_format)?;
    match source {
        FormatId::OpenAiChat => from_openai_chat_to_canonical_response(body),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            from_openai_responses_to_canonical_response(body)
        }
        FormatId::ClaudeMessages => from_claude_to_canonical_response(body),
        FormatId::GeminiGenerateContent => from_gemini_to_canonical_response(body),
    }
    .ok_or_else(|| FormatError::ResponseParseFailed {
        format: source.as_str().to_string(),
    })
}

pub fn emit_response(
    target_format: &str,
    response: &CanonicalResponse,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let target = parse_format(target_format)?;
    let report_context = ctx.report_context_value();
    match target {
        FormatId::OpenAiChat => {
            let mut response = canonical_to_openai_chat_response(response);
            if response.get("service_tier").is_none() {
                if let Some(service_tier) = report_context
                    .get("original_request_body")
                    .and_then(Value::as_object)
                    .and_then(|request| request.get("service_tier"))
                    .cloned()
                {
                    response["service_tier"] = service_tier;
                }
            }
            Some(response)
        }
        FormatId::OpenAiResponses => Some(canonical_to_openai_responses_response(
            response,
            &report_context,
        )),
        FormatId::OpenAiResponsesCompact => Some(canonical_to_openai_responses_compact_response(
            response,
            &report_context,
        )),
        FormatId::ClaudeMessages => Some(canonical_to_claude_response(response)),
        FormatId::GeminiGenerateContent => canonical_to_gemini_response(response, &report_context),
    }
    .ok_or_else(|| FormatError::ResponseEmitFailed {
        format: target.as_str().to_string(),
    })
}

pub fn convert_response(
    source_format: &str,
    target_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let mut response = parse_response(source_format, body, ctx)?;
    if response.model.trim().is_empty() || response.model == "unknown" {
        if let Some(mapped_model) = ctx
            .mapped_model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            response.model = mapped_model.to_string();
        }
    }
    emit_response(target_format, &response, ctx)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTranscoderSpec {
    pub source: FormatId,
    pub target: FormatId,
}

pub fn build_stream_transcoder(
    source_format: &str,
    target_format: &str,
    _ctx: &FormatContext,
) -> Result<StreamTranscoderSpec, FormatError> {
    Ok(StreamTranscoderSpec {
        source: parse_format(source_format)?,
        target: parse_format(target_format)?,
    })
}

fn parse_format(format: &str) -> Result<FormatId, FormatError> {
    FormatId::parse(format).ok_or_else(|| FormatError::UnsupportedFormat(format.to_string()))
}

fn force_openai_chat_stream_options(body: &mut Value, upstream_is_stream: bool) {
    if !upstream_is_stream {
        return;
    }
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert("stream".to_string(), Value::Bool(true));
    match object.get_mut("stream_options") {
        Some(Value::Object(stream_options)) => {
            stream_options.insert("include_usage".to_string(), Value::Bool(true));
        }
        _ => {
            object.insert(
                "stream_options".to_string(),
                json!({
                    "include_usage": true,
                }),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{convert_request, FormatContext};
    use crate::formats::FormatId;

    #[test]
    fn cli_alias_routes_to_openai_responses() {
        assert_eq!(
            FormatId::parse("openai:cli"),
            Some(FormatId::OpenAiResponses)
        );
    }

    #[test]
    fn converts_openai_chat_to_responses_via_registry() {
        let body = json!({
            "model": "gpt-source",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let ctx = FormatContext::default().with_mapped_model("gpt-target");

        let converted = convert_request("openai:chat", "openai:responses", &body, &ctx)
            .expect("request conversion should succeed");

        assert_eq!(converted["model"], "gpt-target");
        assert_eq!(converted["input"][0]["type"], "message");
        assert_eq!(converted["input"][0]["content"][0]["type"], "input_text");
    }
}
