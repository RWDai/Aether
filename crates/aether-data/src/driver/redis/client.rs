use crate::driver::redis::RedisKeyspace;
use crate::error::RedisResultExt;
use crate::DataLayerError;

pub type RedisClient = redis::Client;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RedisClientConfig {
    pub url: String,
    pub key_prefix: Option<String>,
}

impl RedisClientConfig {
    pub fn validate(&self) -> Result<(), DataLayerError> {
        let raw = self.url.trim();
        if raw.is_empty() {
            return Err(DataLayerError::InvalidConfiguration(
                "redis url cannot be empty".to_string(),
            ));
        }
        url::Url::parse(raw).map_err(|err| {
            DataLayerError::InvalidConfiguration(format!("invalid redis url: {err}"))
        })?;
        Ok(())
    }

    pub fn keyspace(&self) -> RedisKeyspace {
        RedisKeyspace::new(self.key_prefix.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct RedisClientFactory {
    config: RedisClientConfig,
}

impl RedisClientFactory {
    pub fn new(config: RedisClientConfig) -> Result<Self, DataLayerError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &RedisClientConfig {
        &self.config
    }

    pub fn connect_lazy(&self) -> Result<RedisClient, DataLayerError> {
        RedisClient::open(self.config.url.clone()).map_redis_err()
    }
}

#[cfg(test)]
mod tests {
    use super::{RedisClientConfig, RedisClientFactory};

    #[test]
    fn factory_builds_lazy_client_from_valid_config() {
        let config = RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        };
        let factory = RedisClientFactory::new(config.clone()).expect("factory should build");

        assert_eq!(factory.config(), &config);
        let _client = factory
            .connect_lazy()
            .expect("lazy redis client should build");
    }
}
