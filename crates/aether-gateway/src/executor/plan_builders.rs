use super::*;

pub(crate) struct LocalSyncPlanAndReport {
    pub(crate) plan: ExecutionPlan,
    pub(crate) report_kind: Option<String>,
    pub(crate) report_context: Option<serde_json::Value>,
}

pub(crate) struct LocalStreamPlanAndReport {
    pub(crate) plan: ExecutionPlan,
    pub(crate) report_kind: Option<String>,
    pub(crate) report_context: Option<serde_json::Value>,
}

#[path = "plan_builders/shared.rs"]
mod shared;
#[path = "plan_builders/stream.rs"]
mod stream;
#[path = "plan_builders/sync.rs"]
mod sync;

pub(crate) use stream::{
    build_gemini_stream_plan_from_decision, build_openai_cli_stream_plan_from_decision,
    build_standard_stream_plan_from_decision,
};
pub(super) use stream::{
    build_openai_chat_stream_plan_from_decision, build_passthrough_stream_plan_from_decision,
};
pub(super) use sync::build_openai_chat_sync_plan_from_decision;
pub(crate) use sync::{
    build_gemini_sync_plan_from_decision, build_openai_cli_sync_plan_from_decision,
    build_passthrough_sync_plan_from_decision, build_standard_sync_plan_from_decision,
};
