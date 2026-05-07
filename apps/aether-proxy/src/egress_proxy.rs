use base64::Engine;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamProxyScheme {
    Http,
    Socks5,
    Socks5h,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpstreamProxyConfig {
    raw: String,
    scheme: UpstreamProxyScheme,
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
}

impl UpstreamProxyConfig {
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("upstream proxy URL must not be empty".to_string());
        }

        let parsed =
            Url::parse(trimmed).map_err(|err| format!("invalid upstream proxy URL: {err}"))?;
        let scheme = match parsed.scheme().to_ascii_lowercase().as_str() {
            "http" => UpstreamProxyScheme::Http,
            "socks5" => UpstreamProxyScheme::Socks5,
            "socks5h" => UpstreamProxyScheme::Socks5h,
            other => {
                return Err(format!(
                    "unsupported upstream proxy scheme `{other}`; use http, socks5, or socks5h"
                ))
            }
        };
        let host = parsed
            .host_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "upstream proxy URL must include a host".to_string())?
            .to_string();
        let port = parsed.port().unwrap_or(match scheme {
            UpstreamProxyScheme::Http => 80,
            UpstreamProxyScheme::Socks5 | UpstreamProxyScheme::Socks5h => 1080,
        });
        let username = non_empty_url_part(parsed.username());
        let password = parsed.password().and_then(non_empty_url_part);

        Ok(Self {
            raw: trimmed.to_string(),
            scheme,
            host,
            port,
            username,
            password,
        })
    }

    pub(crate) fn scheme(&self) -> UpstreamProxyScheme {
        self.scheme
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub(crate) fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub(crate) fn uses_remote_dns(&self) -> bool {
        self.scheme == UpstreamProxyScheme::Socks5h
    }

    pub(crate) fn basic_auth_header(&self) -> Option<String> {
        let username = self.username()?;
        let mut credentials = String::with_capacity(
            username.len() + self.password.as_ref().map(|value| value.len()).unwrap_or(0) + 1,
        );
        credentials.push_str(username);
        credentials.push(':');
        if let Some(password) = self.password() {
            credentials.push_str(password);
        }
        Some(format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(credentials)
        ))
    }

    pub(crate) fn redacted_url(&self) -> String {
        let Ok(mut parsed) = Url::parse(&self.raw) else {
            return "<invalid>".to_string();
        };
        if !parsed.username().is_empty() {
            let _ = parsed.set_username("****");
        }
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some("****"));
        }
        parsed.to_string()
    }
}

fn non_empty_url_part(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_proxy_with_default_port() {
        let proxy = UpstreamProxyConfig::parse("http://proxy.example").expect("proxy should parse");

        assert_eq!(proxy.scheme(), UpstreamProxyScheme::Http);
        assert_eq!(proxy.host(), "proxy.example");
        assert_eq!(proxy.port(), 80);
    }

    #[test]
    fn parses_socks5h_proxy_with_auth() {
        let proxy = UpstreamProxyConfig::parse("socks5h://user:pass@127.0.0.1:1080")
            .expect("proxy should parse");

        assert_eq!(proxy.scheme(), UpstreamProxyScheme::Socks5h);
        assert_eq!(proxy.username(), Some("user"));
        assert_eq!(proxy.password(), Some("pass"));
        assert!(proxy.uses_remote_dns());
        assert_eq!(
            proxy.basic_auth_header().as_deref(),
            Some("Basic dXNlcjpwYXNz")
        );
    }

    #[test]
    fn rejects_unsupported_proxy_scheme() {
        let error = UpstreamProxyConfig::parse("https://proxy.example:8443")
            .expect_err("https proxy scheme should be rejected");

        assert!(error.contains("unsupported upstream proxy scheme"));
    }
}
