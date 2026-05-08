#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct HttpClientConfig {
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub pool_idle_timeout_ms: Option<u64>,
    pub pool_max_idle_per_host: Option<usize>,
    pub tcp_keepalive_ms: Option<u64>,
    pub tcp_nodelay: bool,
    pub http2_adaptive_window: bool,
    pub use_rustls_tls: bool,
    pub user_agent: Option<String>,
    pub proxy_url: Option<String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: None,
            request_timeout_ms: None,
            pool_idle_timeout_ms: None,
            pool_max_idle_per_host: None,
            tcp_keepalive_ms: None,
            tcp_nodelay: true,
            http2_adaptive_window: false,
            use_rustls_tls: true,
            user_agent: None,
            proxy_url: None,
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct HttpRetryConfig {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 200,
            max_delay_ms: 2_000,
        }
    }
}

impl HttpRetryConfig {
    pub fn normalized(self) -> Self {
        let max_attempts = self.max_attempts.max(1);
        let base_delay_ms = self.base_delay_ms.max(1);
        let max_delay_ms = self.max_delay_ms.max(base_delay_ms);
        Self {
            max_attempts,
            base_delay_ms,
            max_delay_ms,
        }
    }

    pub fn delay_for_retry(self, retry_index: u32) -> std::time::Duration {
        let config = self.normalized();
        let factor = 2_u64.saturating_pow(retry_index.min(20));
        let delay_ms = config
            .base_delay_ms
            .saturating_mul(factor)
            .min(config.max_delay_ms);
        std::time::Duration::from_millis(delay_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::HttpRetryConfig;

    #[test]
    fn normalizes_retry_bounds() {
        let config = HttpRetryConfig {
            max_attempts: 0,
            base_delay_ms: 0,
            max_delay_ms: 5,
        }
        .normalized();

        assert_eq!(config.max_attempts, 1);
        assert_eq!(config.base_delay_ms, 1);
        assert_eq!(config.max_delay_ms, 5);
    }

    #[test]
    fn caps_exponential_retry_delay() {
        let config = HttpRetryConfig {
            max_attempts: 3,
            base_delay_ms: 100,
            max_delay_ms: 250,
        };

        assert_eq!(
            config.delay_for_retry(0),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(
            config.delay_for_retry(1),
            std::time::Duration::from_millis(200)
        );
        assert_eq!(
            config.delay_for_retry(2),
            std::time::Duration::from_millis(250)
        );
    }
}
