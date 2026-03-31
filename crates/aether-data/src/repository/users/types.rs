use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredUserSummary {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
    pub role: String,
    pub is_active: bool,
    pub is_deleted: bool,
}

impl StoredUserSummary {
    pub fn new(
        id: String,
        username: String,
        email: Option<String>,
        role: String,
        is_active: bool,
        is_deleted: bool,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.id is empty".to_string(),
            ));
        }
        if username.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.username is empty".to_string(),
            ));
        }
        if role.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.role is empty".to_string(),
            ));
        }
        Ok(Self {
            id,
            username,
            email,
            role,
            is_active,
            is_deleted,
        })
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredUserAuthRecord {
    pub id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub username: String,
    pub password_hash: Option<String>,
    pub role: String,
    pub auth_source: String,
    pub allowed_providers: Option<Vec<String>>,
    pub allowed_api_formats: Option<Vec<String>>,
    pub allowed_models: Option<Vec<String>>,
    pub is_active: bool,
    pub is_deleted: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub last_login_at: Option<DateTime<Utc>>,
}

impl StoredUserAuthRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: Option<String>,
        role: String,
        auth_source: String,
        allowed_providers: Option<Value>,
        allowed_api_formats: Option<Value>,
        allowed_models: Option<Value>,
        is_active: bool,
        is_deleted: bool,
        created_at: Option<DateTime<Utc>>,
        last_login_at: Option<DateTime<Utc>>,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.id is empty".to_string(),
            ));
        }
        if username.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.username is empty".to_string(),
            ));
        }
        if role.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.role is empty".to_string(),
            ));
        }
        if auth_source.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.auth_source is empty".to_string(),
            ));
        }

        Ok(Self {
            id,
            email,
            email_verified,
            username,
            password_hash,
            role,
            auth_source,
            allowed_providers: parse_string_list(allowed_providers, "users.allowed_providers")?,
            allowed_api_formats: parse_string_list(
                allowed_api_formats,
                "users.allowed_api_formats",
            )?,
            allowed_models: parse_string_list(allowed_models, "users.allowed_models")?,
            is_active,
            is_deleted,
            created_at,
            last_login_at,
        })
    }

    pub fn to_summary(&self) -> Result<StoredUserSummary, crate::DataLayerError> {
        StoredUserSummary::new(
            self.id.clone(),
            self.username.clone(),
            self.email.clone(),
            self.role.clone(),
            self.is_active,
            self.is_deleted,
        )
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredUserExportRow {
    pub id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub username: String,
    pub password_hash: Option<String>,
    pub role: String,
    pub auth_source: String,
    pub allowed_providers: Option<Vec<String>>,
    pub allowed_api_formats: Option<Vec<String>>,
    pub allowed_models: Option<Vec<String>>,
    pub rate_limit: Option<i32>,
    pub model_capability_settings: Option<Value>,
    pub is_active: bool,
}

impl StoredUserExportRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: Option<String>,
        role: String,
        auth_source: String,
        allowed_providers: Option<Value>,
        allowed_api_formats: Option<Value>,
        allowed_models: Option<Value>,
        rate_limit: Option<i32>,
        model_capability_settings: Option<Value>,
        is_active: bool,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.id is empty".to_string(),
            ));
        }
        if username.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.username is empty".to_string(),
            ));
        }
        if role.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.role is empty".to_string(),
            ));
        }
        if auth_source.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "users.auth_source is empty".to_string(),
            ));
        }

        Ok(Self {
            id,
            email,
            email_verified,
            username,
            password_hash,
            role,
            auth_source,
            allowed_providers: parse_string_list(allowed_providers, "users.allowed_providers")?,
            allowed_api_formats: parse_string_list(
                allowed_api_formats,
                "users.allowed_api_formats",
            )?,
            allowed_models: parse_string_list(allowed_models, "users.allowed_models")?,
            rate_limit,
            model_capability_settings: normalize_optional_json(model_capability_settings),
            is_active,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UserExportListQuery {
    pub skip: usize,
    pub limit: usize,
    pub role: Option<String>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct UserExportSummary {
    pub total: u64,
    pub active: u64,
}

#[async_trait]
pub trait UserReadRepository: Send + Sync {
    async fn list_users_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserSummary>, crate::DataLayerError>;

    async fn list_export_users(&self) -> Result<Vec<StoredUserExportRow>, crate::DataLayerError>;

    async fn list_export_users_page(
        &self,
        query: &UserExportListQuery,
    ) -> Result<Vec<StoredUserExportRow>, crate::DataLayerError>;

    async fn summarize_export_users(&self) -> Result<UserExportSummary, crate::DataLayerError>;

    async fn find_export_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserExportRow>, crate::DataLayerError>;

    async fn list_non_admin_export_users(
        &self,
    ) -> Result<Vec<StoredUserExportRow>, crate::DataLayerError>;

    async fn find_user_auth_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, crate::DataLayerError>;

    async fn list_user_auth_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserAuthRecord>, crate::DataLayerError>;

    async fn find_user_auth_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<StoredUserAuthRecord>, crate::DataLayerError>;
}

fn normalize_optional_json(value: Option<Value>) -> Option<Value> {
    match value {
        Some(Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

fn parse_string_list(
    value: Option<Value>,
    field_name: &str,
) -> Result<Option<Vec<String>>, crate::DataLayerError> {
    let Some(value) = value else {
        return Ok(None);
    };
    parse_string_list_value(&value, field_name)
}

fn parse_string_list_value(
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<String>>, crate::DataLayerError> {
    match value {
        Value::Null => Ok(None),
        Value::Array(array) => parse_string_list_array(array, field_name).map(Some),
        Value::String(raw) => parse_embedded_string_list(raw, field_name),
        _ => Err(crate::DataLayerError::UnexpectedValue(format!(
            "{field_name} is not a JSON array"
        ))),
    }
}

fn parse_embedded_string_list(
    raw: &str,
    field_name: &str,
) -> Result<Option<Vec<String>>, crate::DataLayerError> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        return Ok(None);
    }

    if let Ok(decoded) = serde_json::from_str::<Value>(raw) {
        return parse_string_list_value(&decoded, field_name);
    }

    Ok(Some(vec![raw.to_string()]))
}

fn parse_string_list_array(
    array: &[Value],
    field_name: &str,
) -> Result<Vec<String>, crate::DataLayerError> {
    let mut items = Vec::with_capacity(array.len());
    for item in array {
        let Some(item) = item.as_str() else {
            return Err(crate::DataLayerError::UnexpectedValue(format!(
                "{field_name} contains a non-string item"
            )));
        };
        let item = item.trim();
        if !item.is_empty() {
            items.push(item.to_string());
        }
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{StoredUserAuthRecord, StoredUserExportRow};

    #[test]
    fn builds_user_export_row_with_allowed_lists() {
        let row = StoredUserExportRow::new(
            "user-1".to_string(),
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            Some("hash".to_string()),
            "user".to_string(),
            "local".to_string(),
            Some(serde_json::json!(["openai", "anthropic"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
            Some(60),
            Some(serde_json::json!({"gpt-4.1": {"cache_1h": true}})),
            true,
        )
        .expect("row should build");

        assert_eq!(
            row.allowed_providers,
            Some(vec!["openai".to_string(), "anthropic".to_string()])
        );
        assert_eq!(
            row.allowed_api_formats,
            Some(vec!["openai:chat".to_string()])
        );
        assert_eq!(row.allowed_models, Some(vec!["gpt-4.1".to_string()]));
        assert_eq!(
            row.model_capability_settings,
            Some(serde_json::json!({"gpt-4.1": {"cache_1h": true}}))
        );
    }

    #[test]
    fn accepts_embedded_string_lists_for_user_export_row() {
        let row = StoredUserExportRow::new(
            "user-1".to_string(),
            None,
            false,
            "alice".to_string(),
            None,
            "user".to_string(),
            "local".to_string(),
            Some(serde_json::json!("[\"openai\"]")),
            Some(serde_json::json!("null")),
            Some(serde_json::json!("gpt-4.1")),
            None,
            Some(Value::Null),
            true,
        )
        .expect("row should build");

        assert_eq!(row.allowed_providers, Some(vec!["openai".to_string()]));
        assert_eq!(row.allowed_api_formats, None);
        assert_eq!(row.allowed_models, Some(vec!["gpt-4.1".to_string()]));
        assert_eq!(row.model_capability_settings, None);
    }

    #[test]
    fn rejects_object_allowed_providers_for_user_export_row() {
        let result = StoredUserExportRow::new(
            "user-1".to_string(),
            None,
            false,
            "alice".to_string(),
            None,
            "user".to_string(),
            "local".to_string(),
            Some(serde_json::json!({"bad": true})),
            None,
            None,
            None,
            None,
            true,
        );

        assert!(result.is_err());
    }

    #[test]
    fn builds_user_auth_record_with_allowed_lists() {
        let row = StoredUserAuthRecord::new(
            "user-1".to_string(),
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            Some("hash".to_string()),
            "user".to_string(),
            "local".to_string(),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
            true,
            false,
            None,
            None,
        )
        .expect("auth row should build");

        assert_eq!(row.allowed_providers, Some(vec!["openai".to_string()]));
        assert_eq!(
            row.allowed_api_formats,
            Some(vec!["openai:chat".to_string()])
        );
        assert_eq!(row.allowed_models, Some(vec!["gpt-4.1".to_string()]));
    }
}
