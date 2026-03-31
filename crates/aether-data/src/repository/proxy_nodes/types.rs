use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredProxyNode {
    pub id: String,
    pub name: String,
    pub ip: String,
    pub port: i32,
    pub region: Option<String>,
    pub is_manual: bool,
    pub proxy_url: Option<String>,
    pub proxy_username: Option<String>,
    pub proxy_password: Option<String>,
    pub status: String,
    pub registered_by: Option<String>,
    pub last_heartbeat_at_unix_secs: Option<u64>,
    pub heartbeat_interval: i32,
    pub active_connections: i32,
    pub total_requests: i64,
    pub avg_latency_ms: Option<f64>,
    pub failed_requests: i64,
    pub dns_failures: i64,
    pub stream_errors: i64,
    pub proxy_metadata: Option<serde_json::Value>,
    pub hardware_info: Option<serde_json::Value>,
    pub estimated_max_concurrency: Option<i32>,
    pub tunnel_mode: bool,
    pub tunnel_connected: bool,
    pub tunnel_connected_at_unix_secs: Option<u64>,
    pub remote_config: Option<serde_json::Value>,
    pub config_version: i32,
    pub created_at_unix_secs: Option<u64>,
    pub updated_at_unix_secs: Option<u64>,
}

impl StoredProxyNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        name: String,
        ip: String,
        port: i32,
        is_manual: bool,
        status: String,
        heartbeat_interval: i32,
        active_connections: i32,
        total_requests: i64,
        failed_requests: i64,
        dns_failures: i64,
        stream_errors: i64,
        tunnel_mode: bool,
        tunnel_connected: bool,
        config_version: i32,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.id is empty".to_string(),
            ));
        }
        if name.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.name is empty".to_string(),
            ));
        }
        if ip.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.ip is empty".to_string(),
            ));
        }
        if status.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.status is empty".to_string(),
            ));
        }

        Ok(Self {
            id,
            name,
            ip,
            port,
            region: None,
            is_manual,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            status,
            registered_by: None,
            last_heartbeat_at_unix_secs: None,
            heartbeat_interval,
            active_connections,
            total_requests,
            avg_latency_ms: None,
            failed_requests,
            dns_failures,
            stream_errors,
            proxy_metadata: None,
            hardware_info: None,
            estimated_max_concurrency: None,
            tunnel_mode,
            tunnel_connected,
            tunnel_connected_at_unix_secs: None,
            remote_config: None,
            config_version,
            created_at_unix_secs: None,
            updated_at_unix_secs: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_runtime_fields(
        mut self,
        region: Option<String>,
        registered_by: Option<String>,
        last_heartbeat_at_unix_secs: Option<u64>,
        avg_latency_ms: Option<f64>,
        proxy_metadata: Option<serde_json::Value>,
        hardware_info: Option<serde_json::Value>,
        estimated_max_concurrency: Option<i32>,
        tunnel_connected_at_unix_secs: Option<u64>,
        remote_config: Option<serde_json::Value>,
        created_at_unix_secs: Option<u64>,
        updated_at_unix_secs: Option<u64>,
    ) -> Self {
        self.region = region;
        self.registered_by = registered_by;
        self.last_heartbeat_at_unix_secs = last_heartbeat_at_unix_secs;
        self.avg_latency_ms = avg_latency_ms;
        self.proxy_metadata = proxy_metadata;
        self.hardware_info = hardware_info;
        self.estimated_max_concurrency = estimated_max_concurrency;
        self.tunnel_connected_at_unix_secs = tunnel_connected_at_unix_secs;
        self.remote_config = remote_config;
        self.created_at_unix_secs = created_at_unix_secs;
        self.updated_at_unix_secs = updated_at_unix_secs;
        self
    }

    pub fn with_manual_proxy_fields(
        mut self,
        proxy_url: Option<String>,
        proxy_username: Option<String>,
        proxy_password: Option<String>,
    ) -> Self {
        self.proxy_url = proxy_url;
        self.proxy_username = proxy_username;
        self.proxy_password = proxy_password;
        self
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProxyNodeHeartbeatMutation {
    pub node_id: String,
    pub heartbeat_interval: Option<i32>,
    pub active_connections: Option<i32>,
    pub total_requests_delta: Option<i64>,
    pub avg_latency_ms: Option<f64>,
    pub failed_requests_delta: Option<i64>,
    pub dns_failures_delta: Option<i64>,
    pub stream_errors_delta: Option<i64>,
    pub proxy_metadata: Option<serde_json::Value>,
    pub proxy_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProxyNodeTunnelStatusMutation {
    pub node_id: String,
    pub connected: bool,
    pub conn_count: i32,
    pub detail: Option<String>,
    pub observed_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredProxyNodeEvent {
    pub id: i64,
    pub node_id: String,
    pub event_type: String,
    pub detail: Option<String>,
    pub created_at_unix_secs: Option<u64>,
}

pub fn normalize_proxy_metadata(
    proxy_metadata: Option<&serde_json::Value>,
    proxy_version: Option<&str>,
) -> Option<serde_json::Value> {
    let mut normalized = match proxy_metadata {
        Some(serde_json::Value::Object(map)) => map.clone(),
        Some(_) | None => serde_json::Map::new(),
    };

    let raw_version = normalized
        .remove("version")
        .and_then(|value| value.as_str().map(str::to_string));
    let version = proxy_version
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(20).collect::<String>())
        .or_else(|| {
            raw_version
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(20).collect::<String>())
        });
    if let Some(version) = version {
        normalized.insert("version".to_string(), serde_json::Value::String(version));
    }

    if normalized.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(normalized))
    }
}

#[async_trait]
pub trait ProxyNodeReadRepository: Send + Sync {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, crate::DataLayerError>;

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;

    async fn list_proxy_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeEvent>, crate::DataLayerError>;
}

#[async_trait]
pub trait ProxyNodeWriteRepository: Send + Sync {
    async fn apply_heartbeat(
        &self,
        mutation: &ProxyNodeHeartbeatMutation,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;

    async fn update_tunnel_status(
        &self,
        mutation: &ProxyNodeTunnelStatusMutation,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;
}
