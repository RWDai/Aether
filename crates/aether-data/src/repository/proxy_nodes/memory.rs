use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::types::{
    normalize_proxy_metadata, ProxyNodeHeartbeatMutation, ProxyNodeReadRepository,
    ProxyNodeTunnelStatusMutation, ProxyNodeWriteRepository, StoredProxyNode, StoredProxyNodeEvent,
};
use crate::DataLayerError;

#[derive(Debug, Default)]
pub struct InMemoryProxyNodeRepository {
    nodes: RwLock<BTreeMap<String, StoredProxyNode>>,
    events: RwLock<Vec<StoredProxyNodeEvent>>,
}

impl InMemoryProxyNodeRepository {
    pub fn seed<I>(nodes: I) -> Self
    where
        I: IntoIterator<Item = StoredProxyNode>,
    {
        Self {
            nodes: RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| (node.id.clone(), node))
                    .collect(),
            ),
            events: RwLock::new(Vec::new()),
        }
    }

    pub fn seed_with_events<I, J>(nodes: I, events: J) -> Self
    where
        I: IntoIterator<Item = StoredProxyNode>,
        J: IntoIterator<Item = StoredProxyNodeEvent>,
    {
        Self {
            nodes: RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| (node.id.clone(), node))
                    .collect(),
            ),
            events: RwLock::new(events.into_iter().collect()),
        }
    }

    fn now_unix_secs() -> Option<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
    }

    fn next_event_id(events: &[StoredProxyNodeEvent]) -> i64 {
        events.iter().map(|event| event.id).max().unwrap_or(0) + 1
    }
}

#[async_trait]
impl ProxyNodeReadRepository for InMemoryProxyNodeRepository {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, DataLayerError> {
        let nodes = self.nodes.read().expect("proxy node repository lock");
        let mut items = nodes.values().cloned().collect::<Vec<_>>();
        items.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
        Ok(items)
    }

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let nodes = self.nodes.read().expect("proxy node repository lock");
        Ok(nodes.get(node_id).cloned())
    }

    async fn list_proxy_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeEvent>, DataLayerError> {
        let events = self.events.read().expect("proxy node repository lock");
        let mut items = events
            .iter()
            .filter(|event| event.node_id == node_id)
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .created_at_unix_secs
                .unwrap_or(0)
                .cmp(&left.created_at_unix_secs.unwrap_or(0))
                .then(right.id.cmp(&left.id))
        });
        items.truncate(limit);
        Ok(items)
    }
}

#[async_trait]
impl ProxyNodeWriteRepository for InMemoryProxyNodeRepository {
    async fn apply_heartbeat(
        &self,
        mutation: &ProxyNodeHeartbeatMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let mut nodes = self.nodes.write().expect("proxy node repository lock");
        let Some(node) = nodes.get_mut(&mutation.node_id) else {
            return Ok(None);
        };
        if !node.tunnel_mode {
            return Err(DataLayerError::InvalidInput(
                "non-tunnel mode is no longer supported, please upgrade aether-proxy to use tunnel mode"
                    .to_string(),
            ));
        }

        let now = Self::now_unix_secs();
        node.last_heartbeat_at_unix_secs = now;
        if node.status != "online" || !node.tunnel_connected {
            node.status = "online".to_string();
            node.tunnel_connected = true;
            node.tunnel_connected_at_unix_secs = now;
            node.updated_at_unix_secs = now;
        }

        if let Some(value) = mutation.heartbeat_interval {
            node.heartbeat_interval = value;
        }
        if let Some(value) = mutation.active_connections {
            node.active_connections = value;
        }
        if let Some(value) = mutation.avg_latency_ms {
            node.avg_latency_ms = Some(value);
        }
        let normalized_proxy_metadata = normalize_proxy_metadata(
            mutation.proxy_metadata.as_ref(),
            mutation.proxy_version.as_deref(),
        );
        if let Some(value) = normalized_proxy_metadata {
            node.proxy_metadata = Some(value);
        }
        if let Some(value) = mutation.total_requests_delta.filter(|value| *value > 0) {
            node.total_requests += value;
        }
        if let Some(value) = mutation.failed_requests_delta.filter(|value| *value > 0) {
            node.failed_requests += value;
        }
        if let Some(value) = mutation.dns_failures_delta.filter(|value| *value > 0) {
            node.dns_failures += value;
        }
        if let Some(value) = mutation.stream_errors_delta.filter(|value| *value > 0) {
            node.stream_errors += value;
        }

        Ok(Some(node.clone()))
    }

    async fn update_tunnel_status(
        &self,
        mutation: &ProxyNodeTunnelStatusMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let mut nodes = self.nodes.write().expect("proxy node repository lock");
        let Some(node) = nodes.get_mut(&mutation.node_id) else {
            return Ok(None);
        };

        let event_time = mutation
            .observed_at_unix_secs
            .or_else(Self::now_unix_secs)
            .unwrap_or(0);
        let event_type = if mutation.connected {
            "connected"
        } else {
            "disconnected"
        };
        let event_detail = mutation.detail.clone().unwrap_or_else(|| {
            format!(
                "[hub_node_status] conn_count={}",
                i32::max(mutation.conn_count, 0)
            )
        });
        let mut events = self.events.write().expect("proxy node repository lock");
        if let Some(last_transition) = node.tunnel_connected_at_unix_secs {
            if event_time < last_transition {
                let event_id = Self::next_event_id(&events);
                events.push(StoredProxyNodeEvent {
                    id: event_id,
                    node_id: mutation.node_id.clone(),
                    event_type: event_type.to_string(),
                    detail: Some(format!("[stale_ignored] {event_detail}")),
                    created_at_unix_secs: Self::now_unix_secs(),
                });
                return Ok(Some(node.clone()));
            }
        }

        node.tunnel_connected = mutation.connected;
        node.tunnel_connected_at_unix_secs = Some(event_time);
        node.status = if mutation.connected {
            "online".to_string()
        } else {
            "offline".to_string()
        };
        node.updated_at_unix_secs = Some(event_time);
        let event_id = Self::next_event_id(&events);
        events.push(StoredProxyNodeEvent {
            id: event_id,
            node_id: mutation.node_id.clone(),
            event_type: event_type.to_string(),
            detail: Some(event_detail),
            created_at_unix_secs: Some(event_time),
        });
        Ok(Some(node.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryProxyNodeRepository;
    use crate::repository::proxy_nodes::{
        ProxyNodeHeartbeatMutation, ProxyNodeReadRepository, ProxyNodeTunnelStatusMutation,
        ProxyNodeWriteRepository, StoredProxyNode, StoredProxyNodeEvent,
    };
    use serde_json::json;

    fn sample_node() -> StoredProxyNode {
        StoredProxyNode::new(
            "node-1".to_string(),
            "proxy-1".to_string(),
            "127.0.0.1".to_string(),
            0,
            false,
            "offline".to_string(),
            30,
            0,
            0,
            0,
            0,
            0,
            true,
            false,
            2,
        )
        .expect("node should build")
        .with_runtime_fields(
            Some("test".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(json!({"allowed_ports": [443]})),
            Some(1_700_000_000),
            Some(1_700_000_001),
        )
    }

    #[tokio::test]
    async fn applies_heartbeat_and_tunnel_status_mutations() {
        let repository = InMemoryProxyNodeRepository::seed(vec![sample_node()]);

        let heartbeat = repository
            .apply_heartbeat(&ProxyNodeHeartbeatMutation {
                node_id: "node-1".to_string(),
                heartbeat_interval: Some(45),
                active_connections: Some(5),
                total_requests_delta: Some(8),
                avg_latency_ms: Some(12.5),
                failed_requests_delta: Some(2),
                dns_failures_delta: Some(1),
                stream_errors_delta: Some(3),
                proxy_metadata: Some(json!({"arch": "arm64"})),
                proxy_version: Some("1.2.3".to_string()),
            })
            .await
            .expect("heartbeat should succeed")
            .expect("node should exist");

        assert_eq!(heartbeat.status, "online");
        assert_eq!(heartbeat.heartbeat_interval, 45);
        assert_eq!(heartbeat.active_connections, 5);
        assert_eq!(heartbeat.total_requests, 8);
        assert_eq!(heartbeat.failed_requests, 2);
        assert_eq!(heartbeat.dns_failures, 1);
        assert_eq!(heartbeat.stream_errors, 3);
        assert_eq!(
            heartbeat
                .proxy_metadata
                .as_ref()
                .and_then(|value| value.get("version"))
                .and_then(|value| value.as_str()),
            Some("1.2.3")
        );

        let stale = repository
            .update_tunnel_status(&ProxyNodeTunnelStatusMutation {
                node_id: "node-1".to_string(),
                connected: false,
                conn_count: 0,
                detail: None,
                observed_at_unix_secs: Some(1),
            })
            .await
            .expect("status update should succeed")
            .expect("node should exist");
        assert_eq!(stale.status, "online");

        let stale_events = repository
            .list_proxy_node_events("node-1", 10)
            .await
            .expect("list events should succeed");
        assert_eq!(stale_events.len(), 1);
        assert_eq!(stale_events[0].event_type, "disconnected");
        assert_eq!(
            stale_events[0].detail.as_deref(),
            Some("[stale_ignored] [hub_node_status] conn_count=0")
        );

        let updated = repository
            .update_tunnel_status(&ProxyNodeTunnelStatusMutation {
                node_id: "node-1".to_string(),
                connected: false,
                conn_count: 0,
                detail: None,
                observed_at_unix_secs: Some(1_800_000_000),
            })
            .await
            .expect("status update should succeed")
            .expect("node should exist");
        assert_eq!(updated.status, "offline");
        assert!(!updated.tunnel_connected);

        let events = repository
            .list_proxy_node_events("node-1", 10)
            .await
            .expect("list events should succeed");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "disconnected");
        assert_eq!(events[0].created_at_unix_secs, Some(1_800_000_000));
        assert_eq!(
            events[0].detail.as_deref(),
            Some("[hub_node_status] conn_count=0")
        );

        let found = repository
            .find_proxy_node("node-1")
            .await
            .expect("find should succeed")
            .expect("node should exist");
        assert_eq!(found.status, "offline");
    }

    #[tokio::test]
    async fn lists_seeded_proxy_node_events_in_descending_order() {
        let repository = InMemoryProxyNodeRepository::seed_with_events(
            vec![sample_node()],
            vec![
                StoredProxyNodeEvent {
                    id: 1,
                    node_id: "node-1".to_string(),
                    event_type: "connected".to_string(),
                    detail: Some("older".to_string()),
                    created_at_unix_secs: Some(1_710_000_000),
                },
                StoredProxyNodeEvent {
                    id: 2,
                    node_id: "node-1".to_string(),
                    event_type: "disconnected".to_string(),
                    detail: Some("newer".to_string()),
                    created_at_unix_secs: Some(1_710_000_100),
                },
            ],
        );

        let events = repository
            .list_proxy_node_events("node-1", 1)
            .await
            .expect("list events should succeed");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 2);
        assert_eq!(events[0].detail.as_deref(), Some("newer"));
    }
}
