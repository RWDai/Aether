use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::{ExecutionPlan, ExecutionTimeouts, ProxySnapshot, RequestBody};
use aether_data::repository::video_tasks::{
    StoredVideoTask, UpsertVideoTask, VideoTaskStatus as StoredVideoTaskStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::gateway::provider_transport::{
    resolve_local_gemini_auth, resolve_local_standard_auth, resolve_transport_execution_timeouts,
    supports_local_gemini_transport, supports_local_standard_transport,
    GatewayProviderTransportSnapshot,
};
use crate::gateway::GatewayControlAuthContext;

const DEFAULT_VIDEO_TASK_POLL_INTERVAL_SECONDS: u32 = 10;
const DEFAULT_VIDEO_TASK_MAX_POLL_COUNT: u32 = 360;

pub(crate) use self::service::VideoTaskService;
pub use self::types::VideoTaskTruthSourceMode;
pub(crate) use self::types::{
    GeminiVideoTaskSeed, LocalVideoTaskContentAction, LocalVideoTaskFollowUpPlan,
    LocalVideoTaskPersistence, LocalVideoTaskReadRefreshPlan, LocalVideoTaskReadResponse,
    LocalVideoTaskRegistryMutation, LocalVideoTaskSeed, LocalVideoTaskSnapshot,
    LocalVideoTaskStatus, LocalVideoTaskSuccessPlan, LocalVideoTaskTransport, OpenAiVideoTaskSeed,
    VideoTaskSyncReportMode,
};

pub(crate) use self::helpers::{
    extract_gemini_short_id_from_cancel_path, extract_gemini_short_id_from_path,
    extract_openai_task_id_from_cancel_path, extract_openai_task_id_from_content_path,
    extract_openai_task_id_from_path, extract_openai_task_id_from_remix_path,
};

use self::helpers::{
    build_video_follow_up_report_context, context_text, context_u64, current_unix_timestamp_secs,
    gemini_metadata_video_url, generate_local_short_id, local_status_from_stored,
    map_openai_task_status, non_empty_owned, parse_video_content_variant, request_body_string,
    request_body_text, request_body_u32, resolve_follow_up_auth,
    resolve_local_video_registry_mutation,
};
use self::store::{FileVideoTaskStore, InMemoryVideoTaskStore, VideoTaskStore};
use self::types::LocalVideoTaskProjectionTarget;

mod helpers;
mod seed;
mod service;
mod store;
mod types;

#[cfg(test)]
mod tests;
