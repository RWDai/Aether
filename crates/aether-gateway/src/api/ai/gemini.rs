pub(crate) fn normalized_signature(api_format: &str) -> Option<&'static str> {
    match api_format {
        "gemini:chat" => Some("gemini:chat"),
        "gemini:cli" => Some("gemini:cli"),
        "gemini:video" => Some("gemini:video"),
        _ => None,
    }
}

pub(crate) fn local_path(api_format: &str) -> Option<&'static str> {
    match api_format {
        "gemini" | "gemini:chat" | "gemini:cli" => Some("/v1beta/models/{model}:{action}"),
        "gemini:video" => Some("/v1beta/models/{model}:predictLongRunning"),
        _ => None,
    }
}
