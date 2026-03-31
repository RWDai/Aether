use self::affinity::{
    build_scheduler_affinity_cache_key, candidate_affinity_hash, candidate_key,
    compare_affinity_order, matches_affinity_target, remember_scheduler_affinity,
};
use self::model::{
    auth_snapshot_allows_api_format, auth_snapshot_allows_model, auth_snapshot_allows_provider,
    candidate_model_names, candidate_supports_required_capability,
    extract_global_priority_for_format, matches_model_mapping, normalize_api_format,
    read_requested_model_rows, resolve_provider_model_name, resolve_requested_global_model_name,
    row_supports_required_capability, select_provider_model_name,
};
use self::selection::{
    is_candidate_selectable, read_provider_concurrent_limits, read_provider_key_rpm_states,
    reorder_candidates_by_scheduler_health, should_skip_provider_quota,
};

mod affinity;
mod model;
mod selection;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aether_data::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredProviderModelMapping,
};
use aether_data::repository::candidates::StoredRequestCandidate;
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::repository::quota::StoredProviderQuotaSnapshot;
use aether_data::DataLayerError;
use aether_wallet::{ProviderBillingType, ProviderQuotaSnapshot};
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::gateway::cache::SchedulerAffinityTarget;
use crate::gateway::data::{GatewayDataState, StoredGatewayAuthApiKeySnapshot};
use crate::gateway::{AppState, GatewayError};

use super::health::{
    count_recent_active_requests_for_api_key, count_recent_active_requests_for_provider,
    effective_provider_key_health_score, is_candidate_in_recent_failure_cooldown,
    is_provider_key_circuit_open, provider_key_health_bucket, provider_key_health_score,
    provider_key_rpm_allows_request_since,
};

const SCHEDULER_AFFINITY_TTL: Duration = Duration::from_secs(300);
#[cfg_attr(not(test), allow(dead_code))]
const SCHEDULER_AFFINITY_MAX_ENTRIES: usize = 10_000;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(crate) struct GatewayMinimalCandidateSelectionCandidate {
    pub(crate) provider_id: String,
    pub(crate) provider_name: String,
    pub(crate) provider_type: String,
    pub(crate) provider_priority: i32,
    pub(crate) endpoint_id: String,
    pub(crate) endpoint_api_format: String,
    pub(crate) key_id: String,
    pub(crate) key_name: String,
    pub(crate) key_auth_type: String,
    pub(crate) key_internal_priority: i32,
    pub(crate) key_global_priority_for_format: Option<i32>,
    pub(crate) key_capabilities: Option<serde_json::Value>,
    pub(crate) model_id: String,
    pub(crate) global_model_id: String,
    pub(crate) global_model_name: String,
    pub(crate) selected_provider_model_name: String,
    pub(crate) mapping_matched_model: Option<String>,
}

#[allow(dead_code)]
pub(crate) async fn read_minimal_candidate_selection(
    state: &GatewayDataState,
    api_format: &str,
    requested_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&StoredGatewayAuthApiKeySnapshot>,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, DataLayerError> {
    let normalized_api_format = normalize_api_format(api_format);
    if normalized_api_format.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_snapshot_allows_api_format(auth_snapshot, &normalized_api_format) {
        return Ok(Vec::new());
    }

    let Some((resolved_global_model_name, rows)) =
        read_requested_model_rows(state, &normalized_api_format, requested_model_name).await?
    else {
        return Ok(Vec::new());
    };

    if !auth_snapshot_allows_model(
        auth_snapshot,
        requested_model_name,
        resolved_global_model_name.as_str(),
    ) {
        return Ok(Vec::new());
    }

    let mut candidates = Vec::new();
    for row in rows {
        if !auth_snapshot_allows_provider(auth_snapshot, &row.provider_id, &row.provider_name) {
            continue;
        }
        if require_streaming && !row.supports_streaming() {
            continue;
        }
        let Some((selected_provider_model_name, mapping_matched_model)) =
            resolve_provider_model_name(&row, requested_model_name, &normalized_api_format)
        else {
            continue;
        };

        candidates.push(GatewayMinimalCandidateSelectionCandidate {
            provider_id: row.provider_id,
            provider_name: row.provider_name,
            provider_type: row.provider_type,
            provider_priority: row.provider_priority,
            endpoint_id: row.endpoint_id,
            endpoint_api_format: row.endpoint_api_format,
            key_id: row.key_id,
            key_name: row.key_name,
            key_auth_type: row.key_auth_type,
            key_internal_priority: row.key_internal_priority,
            key_global_priority_for_format: extract_global_priority_for_format(
                row.key_global_priority_by_format.as_ref(),
                &normalized_api_format,
            )?,
            key_capabilities: row.key_capabilities,
            model_id: row.model_id,
            global_model_id: row.global_model_id,
            global_model_name: row.global_model_name,
            selected_provider_model_name,
            mapping_matched_model,
        });
    }

    let affinity_key = auth_snapshot
        .map(|snapshot| snapshot.api_key_id.trim())
        .filter(|value| !value.is_empty());
    candidates.sort_by(|left, right| {
        left.key_global_priority_for_format
            .unwrap_or(i32::MAX)
            .cmp(&right.key_global_priority_for_format.unwrap_or(i32::MAX))
            .then_with(|| compare_affinity_order(left, right, affinity_key))
            .then(left.provider_priority.cmp(&right.provider_priority))
            .then(left.key_internal_priority.cmp(&right.key_internal_priority))
            .then(left.provider_id.cmp(&right.provider_id))
            .then(left.endpoint_id.cmp(&right.endpoint_id))
            .then(left.key_id.cmp(&right.key_id))
            .then(
                left.selected_provider_model_name
                    .cmp(&right.selected_provider_model_name),
            )
    });

    Ok(candidates)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn select_minimal_candidate(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&StoredGatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Option<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let affinity_cache_key =
        build_scheduler_affinity_cache_key(auth_snapshot, api_format, global_model_name);
    let selected = collect_selectable_candidates(
        state,
        api_format,
        global_model_name,
        require_streaming,
        auth_snapshot,
        now_unix_secs,
    )
    .await?
    .into_iter()
    .next();
    if let Some(candidate) = selected.as_ref() {
        remember_scheduler_affinity(affinity_cache_key.as_deref(), state, candidate);
    }
    Ok(selected)
}

pub(crate) async fn list_selectable_candidates(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&StoredGatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    collect_selectable_candidates(
        state,
        api_format,
        global_model_name,
        require_streaming,
        auth_snapshot,
        now_unix_secs,
    )
    .await
}

pub(crate) async fn list_selectable_candidates_for_required_capability_without_requested_model(
    state: &AppState,
    candidate_api_format: &str,
    required_capability: &str,
    require_streaming: bool,
    auth_snapshot: Option<&StoredGatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let normalized_api_format = normalize_api_format(candidate_api_format);
    let required_capability = required_capability.trim();
    if normalized_api_format.is_empty() || required_capability.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_snapshot_allows_api_format(auth_snapshot, &normalized_api_format) {
        return Ok(Vec::new());
    }

    let rows = state
        .list_minimal_candidate_selection_rows_for_api_format(&normalized_api_format)
        .await?;
    let mut model_names = BTreeSet::new();
    for row in rows {
        if !auth_snapshot_allows_provider(auth_snapshot, &row.provider_id, &row.provider_name) {
            continue;
        }
        if !row_supports_required_capability(&row, required_capability) {
            continue;
        }
        if require_streaming && !row.supports_streaming() {
            continue;
        }
        if !auth_snapshot_allows_model(
            auth_snapshot,
            &row.global_model_name,
            &row.global_model_name,
        ) {
            continue;
        }
        model_names.insert(row.global_model_name);
    }

    for global_model_name in model_names {
        let candidates = list_selectable_candidates(
            state,
            &normalized_api_format,
            &global_model_name,
            require_streaming,
            auth_snapshot,
            now_unix_secs,
        )
        .await?;
        let filtered = candidates
            .into_iter()
            .filter(|candidate| {
                candidate_supports_required_capability(candidate, required_capability)
            })
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            return Ok(filtered);
        }
    }

    Ok(Vec::new())
}

async fn collect_selectable_candidates(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&StoredGatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let mut candidates = state
        .read_minimal_candidate_selection(
            api_format,
            global_model_name,
            require_streaming,
            auth_snapshot,
        )
        .await?;
    let recent_candidates = state.read_recent_request_candidates(128).await?;
    let provider_concurrent_limits = read_provider_concurrent_limits(state, &candidates).await?;
    let provider_key_rpm_states = read_provider_key_rpm_states(state, &candidates).await?;
    reorder_candidates_by_scheduler_health(
        &mut candidates,
        &provider_key_rpm_states,
        auth_snapshot,
    );
    let affinity_cache_key =
        build_scheduler_affinity_cache_key(auth_snapshot, api_format, global_model_name);
    let cached_affinity_target = affinity_cache_key.as_deref().and_then(|cache_key| {
        state
            .scheduler_affinity_cache
            .get_fresh(cache_key, SCHEDULER_AFFINITY_TTL)
    });

    if let Some((api_key_id, limit)) = auth_snapshot.and_then(|snapshot| {
        usize::try_from(snapshot.api_key_concurrent_limit?)
            .ok()
            .and_then(|limit| {
                if limit == 0 {
                    return None;
                }
                Some((snapshot.api_key_id.as_str(), limit))
            })
    }) {
        let active_requests =
            count_recent_active_requests_for_api_key(&recent_candidates, api_key_id, now_unix_secs);
        if active_requests >= limit {
            return Ok(Vec::new());
        }
    }

    let mut selected = Vec::new();
    let mut selected_keys = BTreeSet::new();

    if let Some(target) = cached_affinity_target.as_ref() {
        if let Some(candidate) = candidates
            .iter()
            .find(|candidate| matches_affinity_target(candidate, target))
            .cloned()
        {
            if is_candidate_selectable(
                &candidate,
                &recent_candidates,
                &provider_concurrent_limits,
                &provider_key_rpm_states,
                now_unix_secs,
                cached_affinity_target.as_ref(),
                state,
            )
            .await?
            {
                selected_keys.insert(candidate_key(&candidate));
                selected.push(candidate);
            }
        }
    }

    for candidate in candidates {
        if selected_keys.contains(&candidate_key(&candidate)) {
            continue;
        }
        if !is_candidate_selectable(
            &candidate,
            &recent_candidates,
            &provider_concurrent_limits,
            &provider_key_rpm_states,
            now_unix_secs,
            cached_affinity_target.as_ref(),
            state,
        )
        .await?
        {
            continue;
        }
        selected_keys.insert(candidate_key(&candidate));
        selected.push(candidate);
    }

    Ok(selected)
}
