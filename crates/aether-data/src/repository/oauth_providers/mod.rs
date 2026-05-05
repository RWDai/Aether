mod memory;
mod mysql;
mod postgres;
mod sqlite;
mod types;

pub use memory::InMemoryOAuthProviderRepository;
pub use mysql::MysqlOAuthProviderRepository;
pub use postgres::SqlxOAuthProviderRepository;
pub use sqlite::SqliteOAuthProviderRepository;
pub use types::{
    EncryptedSecretUpdate, OAuthProviderReadRepository, OAuthProviderRepository,
    OAuthProviderWriteRepository, StoredOAuthProviderConfig, UpsertOAuthProviderConfigRecord,
};
