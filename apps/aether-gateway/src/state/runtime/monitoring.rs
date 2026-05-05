use std::collections::BTreeMap;

use aether_data::repository::audit::AuditLogListQuery;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use super::{AppState, GatewayError};

impl AppState {
    pub(crate) async fn list_admin_audit_logs(
        &self,
        cutoff_time: DateTime<Utc>,
        username_pattern: Option<&str>,
        event_type: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<Value>, usize), GatewayError> {
        let query = AuditLogListQuery {
            cutoff_unix_secs: cutoff_unix_secs(cutoff_time),
            username_pattern: username_pattern.map(str::to_string),
            event_type: event_type.map(str::to_string),
            limit,
            offset,
        };
        let page = self
            .data
            .list_admin_audit_logs(&query)
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin audit logs read failed: {err}"))
            })?;
        let total = usize::try_from(page.total).unwrap_or(usize::MAX);
        let items = page
            .items
            .iter()
            .map(|record| {
                json!({
                    "id": record.id,
                    "event_type": record.event_type,
                    "user_id": record.user_id,
                    "user_email": record.user_email,
                    "user_username": record.user_username,
                    "description": record.description,
                    "ip_address": record.ip_address,
                    "status_code": record.status_code,
                    "error_message": record.error_message,
                    "metadata": record.metadata,
                    "created_at": record.created_at_rfc3339(),
                })
            })
            .collect();
        Ok((items, total))
    }

    pub(crate) async fn list_admin_suspicious_activities(
        &self,
        cutoff_time: DateTime<Utc>,
    ) -> Result<Vec<Value>, GatewayError> {
        let activities = self
            .data
            .list_admin_suspicious_activities(cutoff_unix_secs(cutoff_time))
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin suspicious activities read failed: {err}"))
            })?;
        Ok(activities
            .iter()
            .map(|record| {
                json!({
                    "id": record.id,
                    "event_type": record.event_type,
                    "user_id": record.user_id,
                    "description": record.description,
                    "ip_address": record.ip_address,
                    "metadata": record.metadata,
                    "created_at": record.created_at_rfc3339(),
                })
            })
            .collect())
    }

    pub(crate) async fn read_admin_user_behavior_event_counts(
        &self,
        user_id: &str,
        cutoff_time: DateTime<Utc>,
    ) -> Result<BTreeMap<String, u64>, GatewayError> {
        self.data
            .read_admin_user_behavior_event_counts(user_id, cutoff_unix_secs(cutoff_time))
            .await
            .map_err(|err| {
                GatewayError::Internal(format!("admin user behavior read failed: {err}"))
            })
    }

    pub(crate) async fn list_user_audit_logs(
        &self,
        user_id: &str,
        cutoff_time: DateTime<Utc>,
        event_type: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<Value>, usize), GatewayError> {
        let query = AuditLogListQuery {
            cutoff_unix_secs: cutoff_unix_secs(cutoff_time),
            username_pattern: None,
            event_type: event_type.map(str::to_string),
            limit,
            offset,
        };
        let page = self
            .data
            .list_user_audit_logs(user_id, &query)
            .await
            .map_err(|err| GatewayError::Internal(format!("user audit logs read failed: {err}")))?;
        let total = usize::try_from(page.total).unwrap_or(usize::MAX);
        let items = page
            .items
            .iter()
            .map(|record| {
                json!({
                    "id": record.id,
                    "event_type": record.event_type,
                    "description": record.description,
                    "ip_address": record.ip_address,
                    "status_code": record.status_code,
                    "created_at": record.created_at_rfc3339(),
                })
            })
            .collect();
        Ok((items, total))
    }
}

fn cutoff_unix_secs(cutoff_time: DateTime<Utc>) -> u64 {
    cutoff_time.timestamp().max(0) as u64
}
