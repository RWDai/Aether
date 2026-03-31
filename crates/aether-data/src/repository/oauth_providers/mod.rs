mod memory;
mod sql;
mod types;

pub use memory::InMemoryOAuthProviderRepository;
pub use sql::SqlxOAuthProviderRepository;
pub use types::{
    EncryptedSecretUpdate, OAuthProviderReadRepository, OAuthProviderRepository,
    OAuthProviderWriteRepository, StoredOAuthProviderConfig, UpsertOAuthProviderConfigRecord,
};
