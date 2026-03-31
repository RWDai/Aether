use std::sync::Arc;

use aether_contracts::ExecutionPlan;
use aether_data::DataLayerError;
use tracing::warn;

use super::config::UsageRuntimeConfig;
use super::event::UsageEventType;
use super::reporting::{GatewayStreamReportRequest, GatewaySyncReportRequest};
use super::worker::UsageQueueWorker;
use super::write::{
    build_pending_usage_record, build_stream_terminal_usage_event, build_streaming_usage_record,
    build_sync_terminal_usage_event,
};
use crate::gateway::billing::enrich_usage_event_with_billing;
use crate::gateway::data::GatewayDataState;
use crate::gateway::wallet::settle_usage_if_needed;

#[derive(Debug, Clone)]
pub(crate) struct UsageRuntime {
    config: UsageRuntimeConfig,
}

impl Default for UsageRuntime {
    fn default() -> Self {
        Self::disabled()
    }
}

impl UsageRuntime {
    pub(crate) fn disabled() -> Self {
        Self {
            config: UsageRuntimeConfig::disabled(),
        }
    }

    pub(crate) fn new(config: UsageRuntimeConfig) -> Result<Self, DataLayerError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub(crate) fn can_spawn_worker(&self, data: &GatewayDataState) -> bool {
        self.is_enabled() && data.has_usage_writer() && data.has_usage_worker_runner()
    }

    pub(crate) fn spawn_worker(
        &self,
        data: Arc<GatewayDataState>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.can_spawn_worker(&data) {
            return None;
        }
        let runner = data.usage_worker_runner()?;
        let worker = UsageQueueWorker::new(runner, data, self.config.clone()).ok()?;
        Some(worker.spawn())
    }

    pub(crate) async fn record_pending(
        &self,
        data: &GatewayDataState,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let now_unix_secs = now_unix_secs();
        match build_pending_usage_record(plan, report_context, now_unix_secs) {
            Ok(record) => {
                if let Err(err) = data.upsert_usage(record).await {
                    warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to record sync pending usage");
                }
            }
            Err(err) => {
                warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to build sync pending usage")
            }
        }
    }

    pub(crate) async fn record_stream_started(
        &self,
        data: &GatewayDataState,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        status_code: u16,
        headers: &std::collections::BTreeMap<String, String>,
        telemetry: Option<&aether_contracts::ExecutionTelemetry>,
    ) {
        if !self.is_enabled() {
            return;
        }
        let now_unix_secs = now_unix_secs();
        match build_streaming_usage_record(
            plan,
            report_context,
            status_code,
            headers,
            telemetry,
            now_unix_secs,
        ) {
            Ok(record) => {
                if let Err(err) = data.upsert_usage(record).await {
                    warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to record stream usage");
                }
            }
            Err(err) => {
                warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to build stream usage")
            }
        }
    }

    pub(crate) async fn record_sync_terminal(
        &self,
        data: &GatewayDataState,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        payload: &GatewaySyncReportRequest,
    ) {
        if !self.is_enabled() {
            return;
        }
        let event_type = sync_event_type(payload);
        match build_sync_terminal_usage_event(plan, report_context, payload, event_type) {
            Ok(mut event) => {
                if let Err(err) = enrich_usage_event_with_billing(data, &mut event).await {
                    warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to enrich sync usage event with billing");
                }
                self.enqueue_or_write_terminal(data, event).await
            }
            Err(err) => {
                warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to build sync terminal usage event")
            }
        }
    }

    pub(crate) async fn record_stream_terminal(
        &self,
        data: &GatewayDataState,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        payload: &GatewayStreamReportRequest,
        cancelled: bool,
    ) {
        if !self.is_enabled() {
            return;
        }
        let event_type = if cancelled {
            UsageEventType::Cancelled
        } else {
            stream_event_type(payload)
        };
        match build_stream_terminal_usage_event(plan, report_context, payload, event_type) {
            Ok(mut event) => {
                if let Err(err) = enrich_usage_event_with_billing(data, &mut event).await {
                    warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to enrich stream usage event with billing");
                }
                self.enqueue_or_write_terminal(data, event).await
            }
            Err(err) => {
                warn!(error = %err, request_id = %plan.request_id, "usage runtime failed to build stream terminal usage event")
            }
        }
    }

    async fn enqueue_or_write_terminal(&self, data: &GatewayDataState, event: super::UsageEvent) {
        if let Some(runner) = data.usage_worker_runner() {
            match super::queue::UsageQueue::new(runner, self.config.clone()) {
                Ok(queue) => match queue.enqueue(&event).await {
                    Ok(_) => return,
                    Err(err) => {
                        warn!(error = %err, request_id = %event.request_id, "usage runtime failed to enqueue terminal usage event; falling back to direct write")
                    }
                },
                Err(err) => {
                    warn!(error = %err, request_id = %event.request_id, "usage runtime failed to build queue; falling back to direct write")
                }
            }
        }

        match super::write::build_upsert_usage_record_from_event(&event) {
            Ok(record) => match data.upsert_usage(record).await {
                Ok(Some(stored)) => {
                    if let Err(err) = settle_usage_if_needed(data, &stored).await {
                        warn!(error = %err, request_id = %event.request_id, "usage runtime failed to settle terminal usage directly");
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(error = %err, request_id = %event.request_id, "usage runtime failed to upsert terminal usage directly");
                }
            },
            Err(err) => {
                warn!(error = %err, request_id = %event.request_id, "usage runtime failed to build terminal usage upsert")
            }
        }
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn sync_event_type(payload: &GatewaySyncReportRequest) -> UsageEventType {
    if payload.status_code == 499 || payload.report_kind.contains("cancel") {
        UsageEventType::Cancelled
    } else if payload.status_code >= 400
        || payload
            .body_json
            .as_ref()
            .and_then(|value| value.get("error"))
            .is_some()
    {
        UsageEventType::Failed
    } else {
        UsageEventType::Completed
    }
}

fn stream_event_type(payload: &GatewayStreamReportRequest) -> UsageEventType {
    if payload.status_code == 499 || payload.report_kind.contains("cancel") {
        UsageEventType::Cancelled
    } else if payload.status_code >= 400 {
        UsageEventType::Failed
    } else {
        UsageEventType::Completed
    }
}
