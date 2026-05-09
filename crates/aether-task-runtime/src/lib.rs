use std::future::Future;

use aether_runtime::task::spawn_named;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum TaskKind {
    Scheduled,
    Daemon,
    OnDemand,
    FireAndForget,
}

impl TaskKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Daemon => "daemon",
            Self::OnDemand => "on_demand",
            Self::FireAndForget => "fire_and_forget",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum TaskStatus {
    Queued,
    Running,
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

impl TaskStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Retrying => "retrying",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskDefinition {
    pub key: &'static str,
    pub kind: TaskKind,
    pub trigger: &'static str,
    pub singleton: bool,
    pub persist_history: bool,
    pub retry_policy: RetryPolicy,
}

impl TaskDefinition {
    pub const fn new(
        key: &'static str,
        kind: TaskKind,
        trigger: &'static str,
        singleton: bool,
        persist_history: bool,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            key,
            kind,
            trigger,
            singleton,
            persist_history,
            retry_policy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskContext<TPayload = serde_json::Value> {
    run_id: String,
    task_key: String,
    payload: Option<TPayload>,
    cancellation_token: CancellationToken,
}

impl<TPayload> TaskContext<TPayload> {
    pub fn new(
        run_id: impl Into<String>,
        task_key: impl Into<String>,
        payload: Option<TPayload>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            task_key: task_key.into(),
            payload,
            cancellation_token,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn task_key(&self) -> &str {
        &self.task_key
    }

    pub fn payload(&self) -> Option<&TPayload> {
        self.payload.as_ref()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation_token.cancelled().await;
    }
}

#[derive(Debug)]
pub struct TaskSupervisor {
    cancellation_token: CancellationToken,
    join_set: JoinSet<()>,
    supervised_task_count: usize,
}

impl TaskSupervisor {
    pub fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            join_set: JoinSet::new(),
            supervised_task_count: 0,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn spawn_named<F>(&mut self, task_name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.supervised_task_count = self.supervised_task_count.saturating_add(1);
        let cancellation_token = self.cancellation_token.clone();
        self.join_set.spawn(async move {
            let mut handle = spawn_named(task_name, future);
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    handle.abort();
                    let _ = handle.await;
                }
                result = &mut handle => {
                    if let Err(error) = result {
                        warn!(task = task_name, error = ?error, "supervised task failed");
                    }
                }
            }
        });
    }

    pub fn supervise_handle(&mut self, task_name: &'static str, mut handle: JoinHandle<()>) {
        self.supervised_task_count = self.supervised_task_count.saturating_add(1);
        let cancellation_token = self.cancellation_token.clone();
        self.join_set.spawn(async move {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    handle.abort();
                    let _ = handle.await;
                }
                result = &mut handle => {
                    if let Err(error) = result {
                        warn!(task = task_name, error = ?error, "supervised task failed");
                    }
                }
            }
        });
    }

    pub fn is_empty(&self) -> bool {
        self.supervised_task_count == 0
    }

    pub fn task_count(&self) -> usize {
        self.supervised_task_count
    }

    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    pub async fn shutdown(mut self) {
        self.cancel();
        while self.join_set.join_next().await.is_some() {}
    }
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}
