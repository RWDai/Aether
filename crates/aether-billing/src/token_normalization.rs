#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiFamily {
    OpenAi,
    Claude,
    Gemini,
    Unknown,
}

fn parse_api_family(api_format: Option<&str>) -> ApiFamily {
    let Some(api_format) = api_format else {
        return ApiFamily::Unknown;
    };
    let family = api_format
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match family.as_str() {
        "openai" => ApiFamily::OpenAi,
        "claude" | "anthropic" => ApiFamily::Claude,
        "gemini" | "google" => ApiFamily::Gemini,
        _ => ApiFamily::Unknown,
    }
}

pub fn normalize_input_tokens_for_billing(
    api_format: Option<&str>,
    input_tokens: i64,
    cache_read_tokens: i64,
) -> i64 {
    if input_tokens <= 0 {
        return input_tokens.max(0);
    }
    if cache_read_tokens <= 0 {
        return input_tokens;
    }

    match parse_api_family(api_format) {
        ApiFamily::Claude => input_tokens,
        ApiFamily::OpenAi | ApiFamily::Gemini => (input_tokens - cache_read_tokens).max(0),
        ApiFamily::Unknown => input_tokens,
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_input_tokens_for_billing;

    #[test]
    fn subtracts_cache_tokens_for_openai_and_gemini() {
        assert_eq!(
            normalize_input_tokens_for_billing(Some("openai:chat"), 100, 20),
            80
        );
        assert_eq!(
            normalize_input_tokens_for_billing(Some("gemini:chat"), 100, 20),
            80
        );
    }

    #[test]
    fn keeps_input_tokens_for_claude() {
        assert_eq!(
            normalize_input_tokens_for_billing(Some("claude:chat"), 100, 20),
            100
        );
    }
}
