mod memory;
mod mysql;
mod postgres;
mod sqlite;
mod types;

pub use memory::InMemoryProxyNodeRepository;
pub use mysql::MysqlProxyNodeReadRepository;
pub use postgres::SqlxProxyNodeRepository;
pub use sqlite::SqliteProxyNodeReadRepository;
pub use types::{
    normalize_proxy_node_scheduling_state, proxy_node_accepts_new_tunnels, proxy_reported_version,
    reconcile_remote_config_after_heartbeat, remote_config_scheduling_state,
    remote_config_upgrade_target, ProxyNodeHeartbeatMutation, ProxyNodeManualCreateMutation,
    ProxyNodeManualUpdateMutation, ProxyNodeReadRepository, ProxyNodeRegistrationMutation,
    ProxyNodeRemoteConfigMutation, ProxyNodeTrafficMutation, ProxyNodeTunnelStatusMutation,
    ProxyNodeWriteRepository, StoredProxyNode, StoredProxyNodeEvent,
    PROXY_NODE_SCHEDULING_STATE_CORDONED, PROXY_NODE_SCHEDULING_STATE_DRAINING,
};
