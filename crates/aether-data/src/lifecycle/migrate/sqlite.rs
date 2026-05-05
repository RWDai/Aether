use sqlx::{
    migrate::{Migrate, MigrateError, Migrator},
    SqlitePool,
};

use super::{
    is_missing_sqlx_migrations_table_error, pending_migrations_from_applied_for,
    PendingMigrationInfo,
};

pub(super) static MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

pub async fn pending_migrations(
    pool: &SqlitePool,
) -> Result<Vec<PendingMigrationInfo>, MigrateError> {
    let mut conn = pool.acquire().await?;
    let applied_migrations = match conn.list_applied_migrations().await {
        Ok(applied_migrations) => applied_migrations,
        Err(err) if is_missing_sqlx_migrations_table_error(&err) => Vec::new(),
        Err(err) => return Err(err),
    };
    Ok(pending_migrations_from_applied_for(
        &MIGRATOR,
        &applied_migrations,
    ))
}

pub async fn prepare_database_for_startup(
    pool: &SqlitePool,
) -> Result<Vec<PendingMigrationInfo>, MigrateError> {
    pending_migrations(pool).await
}
