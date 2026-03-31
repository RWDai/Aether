use crate::redis::{
    RedisClient, RedisClientConfig, RedisClientFactory, RedisKeyspace, RedisKvRunner,
    RedisKvRunnerConfig, RedisLockRunner, RedisLockRunnerConfig, RedisStreamRunner,
    RedisStreamRunnerConfig,
};
use crate::DataLayerError;

#[derive(Debug, Clone)]
pub struct RedisBackend {
    config: RedisClientConfig,
    client: RedisClient,
}

impl RedisBackend {
    pub fn from_config(config: RedisClientConfig) -> Result<Self, DataLayerError> {
        let factory = RedisClientFactory::new(config.clone())?;
        let client = factory.connect_lazy()?;
        Ok(Self { config, client })
    }

    pub fn config(&self) -> &RedisClientConfig {
        &self.config
    }

    pub fn client(&self) -> &RedisClient {
        &self.client
    }

    pub fn client_clone(&self) -> RedisClient {
        self.client.clone()
    }

    pub fn keyspace(&self) -> RedisKeyspace {
        self.config.keyspace()
    }

    pub fn lock_runner(
        &self,
        config: RedisLockRunnerConfig,
    ) -> Result<RedisLockRunner, DataLayerError> {
        RedisLockRunner::new(self.client_clone(), self.keyspace(), config)
    }

    pub fn stream_runner(
        &self,
        config: RedisStreamRunnerConfig,
    ) -> Result<RedisStreamRunner, DataLayerError> {
        RedisStreamRunner::new(self.client_clone(), self.keyspace(), config)
    }

    pub fn kv_runner(&self, config: RedisKvRunnerConfig) -> Result<RedisKvRunner, DataLayerError> {
        RedisKvRunner::new(self.client_clone(), self.keyspace(), config)
    }
}

#[cfg(test)]
mod tests {
    use super::RedisBackend;
    use crate::redis::{
        RedisClientConfig, RedisKvRunnerConfig, RedisLockRunnerConfig, RedisStreamRunnerConfig,
    };

    #[test]
    fn backend_retains_config_client_and_shared_runners() {
        let config = RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        };

        let backend = RedisBackend::from_config(config.clone()).expect("backend should build");

        assert_eq!(backend.config(), &config);
        assert_eq!(backend.keyspace().key("audit"), "aether:audit");
        let _client_ref = backend.client();
        let _client_clone = backend.client_clone();
        let _lock_runner = backend
            .lock_runner(RedisLockRunnerConfig::default())
            .expect("lock runner should build");
        let _stream_runner = backend
            .stream_runner(RedisStreamRunnerConfig::default())
            .expect("stream runner should build");
        let _kv_runner = backend
            .kv_runner(RedisKvRunnerConfig::default())
            .expect("kv runner should build");
    }
}
