use std::fmt;

use super::RedisBackend;
use crate::driver::redis::{RedisStreamRunner, RedisStreamRunnerConfig};
use crate::DataLayerError;

#[derive(Clone, Default)]
pub struct DataWorkerBackends {
    redis: Option<RedisStreamRunner>,
}

impl fmt::Debug for DataWorkerBackends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataWorkerBackends")
            .field("has_redis", &self.redis.is_some())
            .finish()
    }
}

impl DataWorkerBackends {
    pub(crate) fn from_redis(redis: Option<&RedisBackend>) -> Result<Self, DataLayerError> {
        Ok(Self {
            redis: redis
                .map(|backend| backend.stream_runner(RedisStreamRunnerConfig::default()))
                .transpose()?,
        })
    }

    pub fn redis(&self) -> Option<RedisStreamRunner> {
        self.redis.clone()
    }

    pub fn has_any(&self) -> bool {
        self.redis.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataWorkerBackends;
    use crate::backend::RedisBackend;
    use crate::driver::redis::RedisClientConfig;

    #[test]
    fn builds_redis_stream_runner_from_backend() {
        let backend = RedisBackend::from_config(RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        })
        .expect("redis backend should build");

        let workers =
            DataWorkerBackends::from_redis(Some(&backend)).expect("worker backends should build");

        assert!(workers.has_any());
        assert!(workers.redis().is_some());
    }
}
