use std::fmt;

use super::RedisBackend;
use crate::driver::redis::{RedisLockRunner, RedisLockRunnerConfig};
use crate::DataLayerError;

#[derive(Clone, Default)]
pub struct DataLockBackends {
    redis: Option<RedisLockRunner>,
}

impl fmt::Debug for DataLockBackends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataLockBackends")
            .field("has_redis", &self.redis.is_some())
            .finish()
    }
}

impl DataLockBackends {
    pub(crate) fn from_redis(redis: Option<&RedisBackend>) -> Result<Self, DataLayerError> {
        Ok(Self {
            redis: redis
                .map(|backend| backend.lock_runner(RedisLockRunnerConfig::default()))
                .transpose()?,
        })
    }

    pub fn redis(&self) -> Option<RedisLockRunner> {
        self.redis.clone()
    }

    pub fn has_any(&self) -> bool {
        self.redis.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataLockBackends;
    use crate::backend::RedisBackend;
    use crate::driver::redis::RedisClientConfig;

    #[test]
    fn builds_redis_lock_runner_from_backend() {
        let backend = RedisBackend::from_config(RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        })
        .expect("redis backend should build");

        let locks =
            DataLockBackends::from_redis(Some(&backend)).expect("lock backends should build");

        assert!(locks.has_any());
        assert!(locks.redis().is_some());
    }
}
