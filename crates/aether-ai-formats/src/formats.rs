use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormatFamily {
    OpenAi,
    Claude,
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormatProfile {
    Default,
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormatId {
    OpenAiChat,
    OpenAiResponses,
    OpenAiResponsesCompact,
    ClaudeMessages,
    GeminiGenerateContent,
}

impl FormatId {
    pub fn parse(value: &str) -> Option<Self> {
        value.parse().ok()
    }

    pub fn canonical(self) -> Self {
        self
    }

    pub fn family(self) -> FormatFamily {
        match self {
            Self::OpenAiChat | Self::OpenAiResponses | Self::OpenAiResponsesCompact => {
                FormatFamily::OpenAi
            }
            Self::ClaudeMessages => FormatFamily::Claude,
            Self::GeminiGenerateContent => FormatFamily::Gemini,
        }
    }

    pub fn profile(self) -> FormatProfile {
        match self {
            Self::OpenAiResponsesCompact => FormatProfile::Compact,
            _ => FormatProfile::Default,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiChat => "openai:chat",
            Self::OpenAiResponses => "openai:responses",
            Self::OpenAiResponsesCompact => "openai:responses:compact",
            Self::ClaudeMessages => "claude:messages",
            Self::GeminiGenerateContent => "gemini:generate_content",
        }
    }
}

impl fmt::Display for FormatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FormatId {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai:chat" | "/v1/chat/completions" => Ok(Self::OpenAiChat),
            "openai:responses" | "openai:cli" | "/v1/responses" => Ok(Self::OpenAiResponses),
            "openai:responses:compact" | "openai:compact" | "/v1/responses/compact" => {
                Ok(Self::OpenAiResponsesCompact)
            }
            "claude:messages" | "claude:chat" | "claude:cli" | "/v1/messages" => {
                Ok(Self::ClaudeMessages)
            }
            "gemini:generate_content" | "gemini:chat" | "gemini:cli" => {
                Ok(Self::GeminiGenerateContent)
            }
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FormatId;

    #[test]
    fn normalizes_legacy_aliases() {
        assert_eq!(
            FormatId::parse("openai:cli"),
            Some(FormatId::OpenAiResponses)
        );
        assert_eq!(
            FormatId::parse("openai:compact"),
            Some(FormatId::OpenAiResponsesCompact)
        );
        assert_eq!(
            FormatId::parse("claude:cli"),
            Some(FormatId::ClaudeMessages)
        );
        assert_eq!(
            FormatId::parse("gemini:chat"),
            Some(FormatId::GeminiGenerateContent)
        );
    }
}
