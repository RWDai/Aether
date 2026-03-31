use super::*;
use crate::gateway::maintenance::spawn_pending_cleanup_worker;
use crate::gateway::maintenance::spawn_pool_monitor_worker;
use crate::gateway::maintenance::spawn_stats_aggregation_worker;
use crate::gateway::maintenance::spawn_stats_hourly_aggregation_worker;
use crate::gateway::maintenance::spawn_usage_cleanup_worker;
use crate::gateway::maintenance::spawn_wallet_daily_usage_aggregation_worker;

impl AppState {
    pub fn new(
        upstream_base_url: impl Into<String>,
        control_base_url: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        Self::new_with_executor(upstream_base_url, control_base_url, None)
    }

    pub fn new_with_executor(
        upstream_base_url: impl Into<String>,
        control_base_url: Option<String>,
        executor_base_url: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        let client = build_http_client(&HttpClientConfig {
            connect_timeout_ms: Some(10_000),
            request_timeout_ms: Some(300_000),
            http2_adaptive_window: true,
            ..HttpClientConfig::default()
        })?;
        Ok(Self {
            upstream_base_url: normalize_upstream_base_url(upstream_base_url.into()),
            control_base_url: control_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            executor_base_url: executor_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            data: Arc::new(GatewayDataState::disabled()),
            usage_runtime: Arc::new(usage::UsageRuntime::disabled()),
            video_tasks: Arc::new(VideoTaskService::new(
                VideoTaskTruthSourceMode::PythonSyncReport,
            )),
            video_task_poller: None,
            request_gate: None,
            distributed_request_gate: None,
            client,
            auth_context_cache: Arc::new(AuthContextCache::default()),
            auth_api_key_last_used_cache: Arc::new(AuthApiKeyLastUsedCache::default()),
            oauth_refresh: Arc::new(provider_transport::LocalOAuthRefreshCoordinator::new()),
            direct_plan_bypass_cache: Arc::new(DirectPlanBypassCache::default()),
            scheduler_affinity_cache: Arc::new(SchedulerAffinityCache::default()),
            fallback_metrics: Arc::new(fallback_metrics::GatewayFallbackMetrics::default()),
            frontdoor_cors: None,
            frontdoor_user_rpm: Arc::new(FrontdoorUserRpmLimiter::new(
                FrontdoorUserRpmConfig::default(),
            )),
            provider_key_rpm_resets: Arc::new(StdMutex::new(HashMap::new())),
            admin_monitoring_error_stats_reset_at: Arc::new(StdMutex::new(None)),
            provider_delete_tasks: Arc::new(StdMutex::new(HashMap::new())),
            #[cfg(test)]
            provider_oauth_state_store: None,
            #[cfg(test)]
            provider_oauth_device_session_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            provider_oauth_batch_task_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            auth_session_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            auth_email_verification_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            auth_email_delivery_store: Some(Arc::new(StdMutex::new(Vec::new()))),
            #[cfg(test)]
            auth_user_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            auth_user_model_capability_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            auth_wallet_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_wallet_payment_order_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_payment_callback_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_wallet_transaction_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_wallet_refund_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_billing_rule_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_billing_collector_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_security_blacklist_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_security_whitelist_store: Some(Arc::new(StdMutex::new(
                std::collections::BTreeSet::new(),
            ))),
            #[cfg(test)]
            admin_monitoring_cache_affinity_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            admin_monitoring_redis_key_store: Some(Arc::new(StdMutex::new(HashMap::new()))),
            #[cfg(test)]
            provider_oauth_token_url_overrides: Arc::new(StdMutex::new(HashMap::new())),
        })
    }

    pub fn with_data_config(
        mut self,
        config: GatewayDataConfig,
    ) -> Result<Self, aether_data::DataLayerError> {
        self.data = Arc::new(GatewayDataState::from_config(config)?);
        Ok(self)
    }

    pub fn with_video_task_truth_source_mode(mut self, mode: VideoTaskTruthSourceMode) -> Self {
        self.video_tasks = Arc::new(self.video_tasks.with_truth_source_mode(mode));
        self
    }

    pub fn with_usage_runtime_config(
        mut self,
        config: usage::UsageRuntimeConfig,
    ) -> Result<Self, aether_data::DataLayerError> {
        self.usage_runtime = Arc::new(usage::UsageRuntime::new(config)?);
        Ok(self)
    }

    pub fn with_video_task_poller_config(mut self, interval: Duration, batch_size: usize) -> Self {
        self.video_task_poller = Some(VideoTaskPollerConfig {
            interval,
            batch_size: batch_size.max(1),
        });
        self
    }

    pub fn with_request_concurrency_limit(mut self, limit: usize) -> Self {
        self.request_gate = Some(Arc::new(ConcurrencyGate::new(
            "gateway_requests",
            limit.max(1),
        )));
        self
    }

    pub fn with_distributed_request_concurrency_gate(
        mut self,
        gate: DistributedConcurrencyGate,
    ) -> Self {
        self.distributed_request_gate = Some(Arc::new(gate));
        self
    }

    pub fn with_frontdoor_cors_config(mut self, config: FrontdoorCorsConfig) -> Self {
        self.frontdoor_cors = Some(Arc::new(config));
        self
    }

    pub fn with_frontdoor_user_rpm_config(mut self, config: FrontdoorUserRpmConfig) -> Self {
        self.frontdoor_user_rpm = Arc::new(FrontdoorUserRpmLimiter::new(config));
        self
    }

    pub fn has_data_backends(&self) -> bool {
        self.data.has_backends()
    }

    pub(crate) fn has_auth_api_key_reader(&self) -> bool {
        self.data.has_auth_api_key_reader()
    }

    pub(crate) fn frontdoor_cors(&self) -> Option<Arc<FrontdoorCorsConfig>> {
        self.frontdoor_cors.clone()
    }

    pub(crate) fn frontdoor_user_rpm(&self) -> Arc<FrontdoorUserRpmLimiter> {
        Arc::clone(&self.frontdoor_user_rpm)
    }

    pub(crate) fn mark_provider_key_rpm_reset(&self, key_id: &str, now_unix_secs: u64) {
        let mut resets = self
            .provider_key_rpm_resets
            .lock()
            .expect("provider key rpm reset cache should lock");
        let min_kept = now_unix_secs.saturating_sub(scheduler::PROVIDER_KEY_RPM_WINDOW_SECS);
        resets.retain(|_, reset_at| *reset_at >= min_kept);
        resets.insert(key_id.to_string(), now_unix_secs);
    }

    pub(crate) fn provider_key_rpm_reset_at(
        &self,
        key_id: &str,
        now_unix_secs: u64,
    ) -> Option<u64> {
        let mut resets = self
            .provider_key_rpm_resets
            .lock()
            .expect("provider key rpm reset cache should lock");
        let min_kept = now_unix_secs.saturating_sub(scheduler::PROVIDER_KEY_RPM_WINDOW_SECS);
        resets.retain(|_, reset_at| *reset_at >= min_kept);
        resets.get(key_id).copied()
    }

    pub(crate) fn admin_monitoring_error_stats_reset_at(&self) -> Option<u64> {
        *self
            .admin_monitoring_error_stats_reset_at
            .lock()
            .expect("admin monitoring error stats reset cache should lock")
    }

    pub(crate) fn mark_admin_monitoring_error_stats_reset(&self, now_unix_secs: u64) {
        let mut reset_at = self
            .admin_monitoring_error_stats_reset_at
            .lock()
            .expect("admin monitoring error stats reset cache should lock");
        *reset_at = Some(now_unix_secs);
    }

    pub(crate) async fn read_system_config_json_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, GatewayError> {
        self.data
            .find_system_config_value(key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_system_config_json_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, GatewayError> {
        self.data
            .upsert_system_config_value(key, value, description)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<crate::gateway::data::StoredSystemConfigEntry>, GatewayError> {
        self.data
            .list_system_config_entries()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<crate::gateway::data::StoredSystemConfigEntry, GatewayError> {
        self.data
            .upsert_system_config_entry(key, value, description)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_system_config_value(&self, key: &str) -> Result<bool, GatewayError> {
        self.data
            .delete_system_config_value(key)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_admin_system_stats(
        &self,
    ) -> Result<(u64, u64, u64, u64), GatewayError> {
        self.data
            .read_admin_system_stats()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, GatewayError> {
        self.data
            .find_proxy_node(node_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, GatewayError> {
        self.data
            .list_proxy_nodes()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_proxy_node_events(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredProxyNodeEvent>, GatewayError> {
        self.data
            .list_proxy_node_events(node_id, limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn apply_proxy_node_heartbeat(
        &self,
        mutation: &ProxyNodeHeartbeatMutation,
    ) -> Result<Option<StoredProxyNode>, GatewayError> {
        self.data
            .apply_proxy_node_heartbeat(mutation)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_proxy_node_tunnel_status(
        &self,
        mutation: &ProxyNodeTunnelStatusMutation,
    ) -> Result<Option<StoredProxyNode>, GatewayError> {
        self.data
            .update_proxy_node_tunnel_status(mutation)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) fn request_concurrency_snapshot(&self) -> Option<ConcurrencySnapshot> {
        self.request_gate.as_ref().map(|gate| gate.snapshot())
    }

    pub(crate) async fn distributed_request_concurrency_snapshot(
        &self,
    ) -> Result<Option<DistributedConcurrencySnapshot>, DistributedConcurrencyError> {
        match self.distributed_request_gate.as_ref() {
            Some(gate) => gate.snapshot().await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn metric_samples(&self) -> Vec<MetricSample> {
        let mut samples = vec![service_up_sample("aether-gateway")];
        if let Some(snapshot) = self.request_concurrency_snapshot() {
            samples.extend(snapshot.to_metric_samples("gateway_requests"));
        }
        if let Some(gate) = self.distributed_request_gate.as_ref() {
            match gate.snapshot().await {
                Ok(snapshot) => {
                    samples.extend(snapshot.to_metric_samples("gateway_requests_distributed"));
                }
                Err(_) => samples.push(
                    MetricSample::new(
                        "concurrency_unavailable",
                        "Whether the distributed concurrency gate is currently unavailable.",
                        MetricKind::Gauge,
                        1,
                    )
                    .with_labels(vec![MetricLabel::new(
                        "gate",
                        "gateway_requests_distributed",
                    )]),
                ),
            }
        }
        samples.extend(self.fallback_metrics.metric_samples());
        samples
    }

    pub(crate) fn record_fallback_metric(
        &self,
        kind: GatewayFallbackMetricKind,
        decision: Option<&GatewayControlDecision>,
        plan_kind: Option<&str>,
        execution_path: Option<&str>,
        reason: GatewayFallbackReason,
    ) {
        self.fallback_metrics
            .record(kind, decision, plan_kind, execution_path, reason);
    }

    pub(crate) async fn try_acquire_request_permit(
        &self,
    ) -> Result<Option<AdmissionPermit>, RequestAdmissionError> {
        let local = self
            .request_gate
            .as_ref()
            .map(|gate| gate.try_acquire())
            .transpose()
            .map_err(RequestAdmissionError::Local)?;
        let distributed = match self.distributed_request_gate.as_ref() {
            Some(gate) => Some(
                gate.try_acquire()
                    .await
                    .map_err(RequestAdmissionError::Distributed)?,
            ),
            None => None,
        };
        Ok(AdmissionPermit::from_parts(local, distributed))
    }

    pub fn has_auth_api_key_data_reader(&self) -> bool {
        self.data.has_auth_api_key_reader()
    }

    pub fn has_gemini_file_mapping_data_reader(&self) -> bool {
        self.data.has_gemini_file_mapping_reader()
    }

    pub fn has_gemini_file_mapping_data_writer(&self) -> bool {
        self.data.has_gemini_file_mapping_writer()
    }

    pub fn has_redis_data_backend(&self) -> bool {
        self.data.has_redis_backend()
    }

    pub(crate) fn redis_kv_runner(&self) -> Option<aether_data::redis::RedisKvRunner> {
        self.data.kv_runner()
    }

    pub(crate) fn postgres_pool(&self) -> Option<aether_data::postgres::PostgresPool> {
        self.data.postgres_pool()
    }

    pub(crate) fn remove_scheduler_affinity_cache_entry(&self, cache_key: &str) -> bool {
        self.scheduler_affinity_cache.remove(cache_key).is_some()
    }

    pub fn with_video_task_store_path(
        mut self,
        path: impl Into<std::path::PathBuf>,
    ) -> std::io::Result<Self> {
        self.video_tasks = Arc::new(VideoTaskService::with_file_store(
            self.video_tasks.truth_source_mode(),
            path,
        )?);
        Ok(self)
    }

    pub fn spawn_background_tasks(&self) -> Vec<JoinHandle<()>> {
        let mut tasks = Vec::new();
        if let Some(handle) = self.usage_runtime.spawn_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = wallet::spawn_provider_quota_reset_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_audit_cleanup_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_db_maintenance_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_wallet_daily_usage_aggregation_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_stats_aggregation_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_usage_cleanup_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_pool_monitor_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_stats_hourly_aggregation_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_pending_cleanup_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_provider_checkin_worker(self.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_request_candidate_cleanup_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_gemini_file_mapping_cleanup_worker(self.data.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_model_fetch_worker(self.clone()) {
            tasks.push(handle);
        }
        if let Some(handle) = spawn_video_task_poller(self.clone()) {
            tasks.push(handle);
        }
        tasks
    }
}
