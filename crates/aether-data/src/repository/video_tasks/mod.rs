mod memory;
mod mysql;
mod postgres;
mod sqlite;

#[allow(unused_imports)]
pub(crate) use aether_data_contracts::repository::video_tasks::{
    StoredVideoTask, UpsertVideoTask, VideoTaskLookupKey, VideoTaskModelCount,
    VideoTaskQueryFilter, VideoTaskReadRepository, VideoTaskRepository, VideoTaskStatus,
    VideoTaskStatusCount, VideoTaskWriteRepository,
};
pub use memory::InMemoryVideoTaskRepository;
pub use mysql::MysqlVideoTaskRepository;
pub use postgres::{SqlxVideoTaskReadRepository, SqlxVideoTaskRepository};
pub use sqlite::SqliteVideoTaskRepository;
