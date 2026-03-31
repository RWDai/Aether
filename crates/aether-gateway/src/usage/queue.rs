use serde_json::json;

use aether_data::redis::{
    RedisConsumerGroup, RedisConsumerName, RedisStreamEntry, RedisStreamName,
    RedisStreamReclaimConfig, RedisStreamRunner,
};
use aether_data::DataLayerError;

use super::{UsageEvent, UsageRuntimeConfig};

#[derive(Debug, Clone)]
pub(crate) struct UsageQueue {
    runner: RedisStreamRunner,
    config: UsageRuntimeConfig,
    stream: RedisStreamName,
    group: RedisConsumerGroup,
    dlq_stream: RedisStreamName,
}

impl UsageQueue {
    pub(crate) fn new(
        runner: RedisStreamRunner,
        config: UsageRuntimeConfig,
    ) -> Result<Self, DataLayerError> {
        config.validate()?;
        Ok(Self {
            runner,
            stream: RedisStreamName(config.stream_key.clone()),
            group: RedisConsumerGroup(config.consumer_group.clone()),
            dlq_stream: RedisStreamName(config.dlq_stream_key.clone()),
            config,
        })
    }

    pub(crate) async fn ensure_consumer_group(&self) -> Result<(), DataLayerError> {
        self.runner
            .ensure_consumer_group(&self.stream, &self.group, "0-0")
            .await
    }

    pub(crate) async fn enqueue(&self, event: &UsageEvent) -> Result<String, DataLayerError> {
        let fields = event.to_stream_fields()?;
        self.runner
            .append_fields_with_maxlen(&self.stream, &fields, Some(self.config.stream_maxlen))
            .await
    }

    pub(crate) async fn read_group(
        &self,
        consumer: &RedisConsumerName,
    ) -> Result<Vec<RedisStreamEntry>, DataLayerError> {
        self.runner
            .read_group(&self.stream, &self.group, consumer)
            .await
    }

    pub(crate) async fn claim_stale(
        &self,
        consumer: &RedisConsumerName,
        start_id: &str,
    ) -> Result<Vec<RedisStreamEntry>, DataLayerError> {
        Ok(self
            .runner
            .claim_stale(
                &self.stream,
                &self.group,
                consumer,
                start_id,
                RedisStreamReclaimConfig {
                    min_idle_ms: self.config.reclaim_idle_ms,
                    count: self.config.reclaim_count,
                },
            )
            .await?
            .entries)
    }

    pub(crate) async fn ack_and_delete(&self, ids: &[String]) -> Result<(), DataLayerError> {
        self.runner.ack(&self.stream, &self.group, ids).await?;
        self.runner.delete(&self.stream, ids).await?;
        Ok(())
    }

    pub(crate) async fn push_dead_letter(
        &self,
        entry: &RedisStreamEntry,
        error: &str,
    ) -> Result<String, DataLayerError> {
        self.runner
            .append_json(
                &self.dlq_stream,
                "payload",
                &json!({
                    "entry_id": entry.id,
                    "fields": entry.fields,
                    "error": error,
                }),
            )
            .await
    }
}
