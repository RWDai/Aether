mod pool;

pub use pool::{SqlitePool, SqlitePoolConfig, SqlitePoolFactory};

use crate::DataLayerError;
use sqlx::{sqlite::SqliteRow, Row};

pub(crate) fn sqlite_real(row: &SqliteRow, field: &str) -> Result<f64, DataLayerError> {
    match row.try_get::<f64, _>(field) {
        Ok(value) => Ok(value),
        Err(real_err) => match row.try_get::<i64, _>(field) {
            Ok(value) => Ok(value as f64),
            Err(_) => Err(DataLayerError::sql(real_err)),
        },
    }
}

pub(crate) fn sqlite_optional_real(
    row: &SqliteRow,
    field: &str,
) -> Result<Option<f64>, DataLayerError> {
    match row.try_get::<Option<f64>, _>(field) {
        Ok(value) => Ok(value),
        Err(real_err) => match row.try_get::<Option<i64>, _>(field) {
            Ok(value) => Ok(value.map(|value| value as f64)),
            Err(_) => Err(DataLayerError::sql(real_err)),
        },
    }
}
