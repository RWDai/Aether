use std::sync::Arc;
use std::time::Duration;

use aether_data::redis::{RedisConsumerName, RedisStreamEntry, RedisStreamRunner};
use tracing::warn;

use super::config::UsageRuntimeConfig;
use super::event::UsageEvent;
use super::queue::UsageQueue;
use super::write::build_upsert_usage_record_from_event;
use crate::gateway::data::GatewayDataState;
use crate::gateway::wallet::settle_usage_if_needed;

pub(crate) struct UsageQueueWorker {
    queue: UsageQueue,
    data: Arc<GatewayDataState>,
    consumer: RedisConsumerName,
    config: UsageRuntimeConfig,
}

impl UsageQueueWorker {
    pub(crate) fn new(
        runner: RedisStreamRunner,
        data: Arc<GatewayDataState>,
        config: UsageRuntimeConfig,
    ) -> Result<Self, aether_data::DataLayerError> {
        let queue = UsageQueue::new(runner, config.clone())?;
        let consumer = RedisConsumerName(consumer_name());
        Ok(Self {
            queue,
            data,
            consumer,
            config,
        })
    }

    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run_forever().await })
    }

    async fn run_forever(self) {
        if let Err(err) = self.queue.ensure_consumer_group().await {
            warn!(error = %err, "usage worker failed to ensure consumer group");
            return;
        }

        let mut reclaim_interval =
            tokio::time::interval(Duration::from_millis(self.config.reclaim_interval_ms));
        reclaim_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        reclaim_interval.tick().await;

        loop {
            tokio::select! {
                _ = reclaim_interval.tick() => {
                    match self.queue.claim_stale(&self.consumer, "0-0").await {
                        Ok(entries) => {
                            if let Err(err) = self.process_entries(entries).await {
                                warn!(error = %err, "usage worker failed while reclaiming stale entries");
                            }
                        }
                        Err(err) => warn!(error = %err, "usage worker failed to reclaim stale entries"),
                    }
                }
                result = self.queue.read_group(&self.consumer) => {
                    match result {
                        Ok(entries) => {
                            if let Err(err) = self.process_entries(entries).await {
                                warn!(error = %err, "usage worker failed to process queue entries");
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "usage worker failed to read queue");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
        }
    }

    async fn process_entries(
        &self,
        entries: Vec<RedisStreamEntry>,
    ) -> Result<(), aether_data::DataLayerError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ack_ids = Vec::new();
        for entry in entries {
            match self.process_entry(&entry).await {
                Ok(should_ack) => {
                    if should_ack {
                        ack_ids.push(entry.id.clone());
                    }
                }
                Err(err) => {
                    if !ack_ids.is_empty() {
                        let _ = self.queue.ack_and_delete(&ack_ids).await;
                    }
                    return Err(err);
                }
            }
        }

        if !ack_ids.is_empty() {
            self.queue.ack_and_delete(&ack_ids).await?;
        }

        Ok(())
    }

    async fn process_entry(
        &self,
        entry: &RedisStreamEntry,
    ) -> Result<bool, aether_data::DataLayerError> {
        let event = match UsageEvent::from_stream_fields(&entry.fields) {
            Ok(event) => event,
            Err(err) => {
                self.queue.push_dead_letter(entry, &err.to_string()).await?;
                return Ok(true);
            }
        };

        write_event_record(self.data.as_ref(), &event).await?;
        Ok(true)
    }
}

async fn write_event_record(
    data: &GatewayDataState,
    event: &UsageEvent,
) -> Result<(), aether_data::DataLayerError> {
    let record = build_upsert_usage_record_from_event(event)?;
    if let Some(stored) = data.upsert_usage(record).await? {
        settle_usage_if_needed(data, &stored).await?;
    }
    Ok(())
}

fn consumer_name() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "aether-gateway".to_string());
    format!("{host}:{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aether_data::repository::usage::{InMemoryUsageReadRepository, UsageReadRepository};

    use super::write_event_record;
    use crate::gateway::data::GatewayDataState;
    use crate::gateway::usage::{UsageEvent, UsageEventData, UsageEventType};

    #[tokio::test]
    async fn worker_writes_usage_record_from_terminal_event() {
        let repository = Arc::new(InMemoryUsageReadRepository::default());
        let data = GatewayDataState::with_usage_repository_for_tests(repository.clone());
        let event = UsageEvent::new(
            UsageEventType::Completed,
            "req-worker-123".to_string(),
            UsageEventData {
                user_id: Some("user-worker-123".to_string()),
                api_key_id: Some("api-key-worker-123".to_string()),
                provider_name: "openai".to_string(),
                provider_id: Some("provider-worker-123".to_string()),
                provider_endpoint_id: Some("endpoint-worker-123".to_string()),
                provider_api_key_id: Some("provider-key-worker-123".to_string()),
                model: "gpt-5".to_string(),
                api_format: Some("openai:chat".to_string()),
                endpoint_api_format: Some("openai:chat".to_string()),
                is_stream: Some(false),
                status_code: Some(200),
                input_tokens: Some(4),
                output_tokens: Some(6),
                total_tokens: Some(10),
                response_time_ms: Some(52),
                ..UsageEventData::default()
            },
        );

        write_event_record(&data, &event)
            .await
            .expect("worker should write usage record");

        let stored = repository
            .find_by_request_id("req-worker-123")
            .await
            .expect("usage lookup should succeed")
            .expect("usage record should exist");

        assert_eq!(stored.status, "completed");
        assert_eq!(stored.billing_status, "pending");
        assert_eq!(stored.total_tokens, 10);
        assert_eq!(stored.response_time_ms, Some(52));
        assert_eq!(stored.user_id.as_deref(), Some("user-worker-123"));
    }
}
