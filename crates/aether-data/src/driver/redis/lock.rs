use std::future::Future;
use std::time::Duration;

use crate::driver::redis::{RedisClient, RedisKeyspace};
use crate::error::RedisResultExt;
use crate::DataLayerError;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedisLockKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisLockLease {
    pub key: RedisLockKey,
    pub owner: String,
    pub token: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisLockRunnerConfig {
    pub command_timeout_ms: Option<u64>,
    pub default_ttl_ms: u64,
}

impl Default for RedisLockRunnerConfig {
    fn default() -> Self {
        Self {
            command_timeout_ms: Some(1_000),
            default_ttl_ms: 15_000,
        }
    }
}

impl RedisLockRunnerConfig {
    pub fn validate(&self) -> Result<(), DataLayerError> {
        if matches!(self.command_timeout_ms, Some(0)) {
            return Err(DataLayerError::InvalidConfiguration(
                "redis lock command_timeout_ms must be positive".to_string(),
            ));
        }
        if self.default_ttl_ms == 0 {
            return Err(DataLayerError::InvalidConfiguration(
                "redis lock default_ttl_ms must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RedisLockRunner {
    client: RedisClient,
    keyspace: RedisKeyspace,
    config: RedisLockRunnerConfig,
}

impl RedisLockRunner {
    pub fn new(
        client: RedisClient,
        keyspace: RedisKeyspace,
        config: RedisLockRunnerConfig,
    ) -> Result<Self, DataLayerError> {
        config.validate()?;
        Ok(Self {
            client,
            keyspace,
            config,
        })
    }

    pub fn client(&self) -> &RedisClient {
        &self.client
    }

    pub fn keyspace(&self) -> &RedisKeyspace {
        &self.keyspace
    }

    pub fn config(&self) -> RedisLockRunnerConfig {
        self.config
    }

    pub async fn try_acquire(
        &self,
        key: &RedisLockKey,
        owner: &str,
        ttl_ms: Option<u64>,
    ) -> Result<Option<RedisLockLease>, DataLayerError> {
        validate_owner(owner)?;
        validate_key(key)?;
        let ttl_ms = self.resolve_ttl_ms(ttl_ms)?;
        let token = format!("{owner}:{}", Uuid::new_v4());

        self.run_with_timeout("redis lock acquire", async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_redis_err()?;
            let status = redis::cmd("SET")
                .arg(&key.0)
                .arg(&token)
                .arg("NX")
                .arg("PX")
                .arg(ttl_ms)
                .query_async::<Option<String>>(&mut connection)
                .await
                .map_redis_err()?;

            Ok(status.map(|_| RedisLockLease {
                key: key.clone(),
                owner: owner.to_string(),
                token,
                ttl_ms,
            }))
        })
        .await
    }

    pub async fn release(&self, lease: &RedisLockLease) -> Result<bool, DataLayerError> {
        validate_lease(lease)?;
        self.run_with_timeout("redis lock release", async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_redis_err()?;
            let deleted = redis::Script::new(
                "if redis.call('get', KEYS[1]) == ARGV[1] then \
                     return redis.call('del', KEYS[1]) \
                 else \
                     return 0 \
                 end",
            )
            .key(&lease.key.0)
            .arg(&lease.token)
            .invoke_async::<i32>(&mut connection)
            .await
            .map_redis_err()?;
            Ok(deleted > 0)
        })
        .await
    }

    pub async fn renew(
        &self,
        lease: &RedisLockLease,
        ttl_ms: Option<u64>,
    ) -> Result<bool, DataLayerError> {
        validate_lease(lease)?;
        let ttl_ms = self.resolve_ttl_ms(ttl_ms)?;

        self.run_with_timeout("redis lock renew", async {
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_redis_err()?;
            let renewed = redis::Script::new(
                "if redis.call('get', KEYS[1]) == ARGV[1] then \
                     return redis.call('pexpire', KEYS[1], ARGV[2]) \
                 else \
                     return 0 \
                 end",
            )
            .key(&lease.key.0)
            .arg(&lease.token)
            .arg(ttl_ms)
            .invoke_async::<i32>(&mut connection)
            .await
            .map_redis_err()?;
            Ok(renewed > 0)
        })
        .await
    }

    async fn run_with_timeout<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, DataLayerError>
    where
        F: Future<Output = Result<T, DataLayerError>>,
    {
        if let Some(timeout_ms) = self.config.command_timeout_ms {
            tokio::time::timeout(Duration::from_millis(timeout_ms), future)
                .await
                .map_err(|_| {
                    DataLayerError::TimedOut(format!("{operation} exceeded {timeout_ms}ms timeout"))
                })?
        } else {
            future.await
        }
    }

    fn resolve_ttl_ms(&self, ttl_ms: Option<u64>) -> Result<u64, DataLayerError> {
        let ttl_ms = ttl_ms.unwrap_or(self.config.default_ttl_ms);
        if ttl_ms == 0 {
            return Err(DataLayerError::InvalidInput(
                "redis lock ttl_ms must be positive".to_string(),
            ));
        }
        Ok(ttl_ms)
    }
}

fn validate_owner(owner: &str) -> Result<(), DataLayerError> {
    if owner.trim().is_empty() {
        return Err(DataLayerError::InvalidInput(
            "redis lock owner cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_key(key: &RedisLockKey) -> Result<(), DataLayerError> {
    if key.0.trim().is_empty() {
        return Err(DataLayerError::InvalidInput(
            "redis lock key cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_lease(lease: &RedisLockLease) -> Result<(), DataLayerError> {
    validate_key(&lease.key)?;
    validate_owner(&lease.owner)?;
    if lease.token.trim().is_empty() {
        return Err(DataLayerError::InvalidInput(
            "redis lock token cannot be empty".to_string(),
        ));
    }
    if lease.ttl_ms == 0 {
        return Err(DataLayerError::InvalidInput(
            "redis lock ttl_ms must be positive".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RedisLockKey, RedisLockLease, RedisLockRunner, RedisLockRunnerConfig};
    use crate::driver::redis::{RedisClientConfig, RedisClientFactory};

    fn sample_runner() -> RedisLockRunner {
        let client = RedisClientFactory::new(RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        })
        .expect("factory should build")
        .connect_lazy()
        .expect("client should build");

        RedisLockRunner::new(
            client,
            RedisClientConfig {
                url: "redis://127.0.0.1/0".to_string(),
                key_prefix: Some("aether".to_string()),
            }
            .keyspace(),
            RedisLockRunnerConfig::default(),
        )
        .expect("runner should build")
    }

    #[test]
    fn validates_runner_config() {
        assert!(RedisLockRunnerConfig {
            command_timeout_ms: Some(0),
            ..RedisLockRunnerConfig::default()
        }
        .validate()
        .is_err());
        assert!(RedisLockRunnerConfig {
            default_ttl_ms: 0,
            ..RedisLockRunnerConfig::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn runner_reuses_client_and_keyspace() {
        let runner = sample_runner();

        assert_eq!(runner.config(), RedisLockRunnerConfig::default());
        assert_eq!(runner.keyspace().lock_key("poller").0, "aether:lock:poller");
        let _client_ref = runner.client();
    }

    #[tokio::test]
    async fn rejects_invalid_owner_or_lease_before_network() {
        let runner = sample_runner();

        assert!(runner
            .try_acquire(&RedisLockKey("aether:lock:poller".to_string()), "", None)
            .await
            .is_err());
        assert!(runner
            .release(&RedisLockLease {
                key: RedisLockKey("aether:lock:poller".to_string()),
                owner: "worker-1".to_string(),
                token: String::new(),
                ttl_ms: 1_000,
            })
            .await
            .is_err());
        assert!(runner
            .renew(
                &RedisLockLease {
                    key: RedisLockKey("aether:lock:poller".to_string()),
                    owner: "worker-1".to_string(),
                    token: "token-1".to_string(),
                    ttl_ms: 1_000,
                },
                Some(0),
            )
            .await
            .is_err());
    }
}
