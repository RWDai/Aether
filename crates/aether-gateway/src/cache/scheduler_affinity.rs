use std::time::Duration;

use aether_cache::ExpiringMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchedulerAffinityTarget {
    pub(crate) provider_id: String,
    pub(crate) endpoint_id: String,
    pub(crate) key_id: String,
}

#[derive(Debug, Default)]
pub(crate) struct SchedulerAffinityCache {
    entries: ExpiringMap<String, SchedulerAffinityTarget>,
}

impl SchedulerAffinityCache {
    pub(crate) fn get_fresh(
        &self,
        cache_key: &str,
        ttl: Duration,
    ) -> Option<SchedulerAffinityTarget> {
        self.entries.get_fresh(&cache_key.to_string(), ttl)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert(
        &self,
        cache_key: String,
        target: SchedulerAffinityTarget,
        ttl: Duration,
        max_entries: usize,
    ) {
        self.entries.insert(cache_key, target, ttl, max_entries);
    }

    pub(crate) fn remove(&self, cache_key: &str) -> Option<SchedulerAffinityTarget> {
        self.entries.remove(&cache_key.to_string())
    }
}
