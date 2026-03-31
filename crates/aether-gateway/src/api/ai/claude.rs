pub(crate) fn normalized_signature(api_format: &str) -> Option<&'static str> {
    match api_format {
        "claude:chat" => Some("claude:chat"),
        "claude:cli" => Some("claude:cli"),
        _ => None,
    }
}

pub(crate) fn local_path(api_format: &str) -> Option<&'static str> {
    match api_format {
        "claude" | "claude:chat" | "claude:cli" => Some("/v1/messages"),
        _ => None,
    }
}
