mod memory;
mod mysql;
mod postgres;
mod sqlite;

#[allow(unused_imports)]
pub(crate) use aether_data_contracts::repository::background_tasks::{
    BackgroundTaskKind, BackgroundTaskListQuery, BackgroundTaskReadRepository,
    BackgroundTaskRepository, BackgroundTaskStatus, BackgroundTaskSummary,
    BackgroundTaskWriteRepository, StoredBackgroundTaskEvent, StoredBackgroundTaskRun,
    StoredBackgroundTaskRunPage, UpsertBackgroundTaskEvent, UpsertBackgroundTaskRun,
};

pub use memory::InMemoryBackgroundTaskRepository;
pub use mysql::MysqlBackgroundTaskRepository;
pub use postgres::SqlxBackgroundTaskRepository;
pub use sqlite::SqliteBackgroundTaskRepository;
