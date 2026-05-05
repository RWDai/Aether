pub mod memory;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
pub mod types;

pub use memory::InMemoryGeminiFileMappingRepository;
pub use mysql::MysqlGeminiFileMappingRepository;
pub use postgres::SqlxGeminiFileMappingRepository;
pub use sqlite::SqliteGeminiFileMappingRepository;
pub use types::{
    GeminiFileMappingListQuery, GeminiFileMappingMimeTypeCount, GeminiFileMappingReadRepository,
    GeminiFileMappingRepository, GeminiFileMappingStats, GeminiFileMappingWriteRepository,
    StoredGeminiFileMapping, StoredGeminiFileMappingListPage, UpsertGeminiFileMappingRecord,
};
