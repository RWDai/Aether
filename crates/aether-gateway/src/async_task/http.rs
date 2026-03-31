use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::ExecutionResult;
use aether_data::repository::video_tasks::{
    StoredVideoTask, UpsertVideoTask, VideoTaskQueryFilter, VideoTaskStatus,
};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::response::Redirect;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::query::VideoTaskVideoSource;
use super::{
    finalize_video_task_if_terminal, read_video_task_detail, read_video_task_page,
    read_video_task_stats, read_video_task_video_source,
};
use crate::gateway::{AppState, GatewayError};

#[derive(Debug)]
pub(crate) enum CancelVideoTaskError {
    NotFound,
    InvalidStatus(VideoTaskStatus),
    Response(axum::response::Response),
    Gateway(GatewayError),
}

impl From<GatewayError> for CancelVideoTaskError {
    fn from(value: GatewayError) -> Self {
        Self::Gateway(value)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListVideoTasksQuery {
    pub(crate) status: Option<String>,
    pub(crate) user_id: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) client_api_format: Option<String>,
    pub(crate) page: Option<usize>,
    pub(crate) page_size: Option<usize>,
}

pub(crate) async fn list_video_tasks(
    State(state): State<AppState>,
    Query(query): Query<ListVideoTasksQuery>,
) -> Result<Json<super::query::VideoTaskPageResponse>, axum::response::Response> {
    let filter = parse_filter(&query)?;
    let response = read_video_task_page(
        &state,
        &filter,
        query.page.unwrap_or(1),
        query.page_size.unwrap_or(20),
    )
    .await
    .map_err(IntoResponse::into_response)?;
    Ok(Json(response))
}

pub(crate) async fn get_video_task_stats(
    State(state): State<AppState>,
    Query(query): Query<ListVideoTasksQuery>,
) -> Result<Json<super::query::VideoTaskStatsResponse>, axum::response::Response> {
    let filter = parse_filter(&query)?;
    let response = read_video_task_stats(&state, &filter, current_unix_secs())
        .await
        .map_err(IntoResponse::into_response)?;
    Ok(Json(response))
}

pub(crate) async fn get_video_task_detail(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<aether_data::repository::video_tasks::StoredVideoTask>, axum::response::Response> {
    let task = read_video_task_detail(&state, &task_id)
        .await
        .map_err(IntoResponse::into_response)?;

    match task {
        Some(task) => Ok(Json(task)),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": "Video task not found",
                }
            })),
        )
            .into_response()),
    }
}

pub(crate) async fn cancel_video_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<Value>, axum::response::Response> {
    let stored = cancel_video_task_record(&state, &task_id)
        .await
        .map_err(|err| match err {
            CancelVideoTaskError::NotFound => (
                axum::http::StatusCode::NOT_FOUND,
                Json(json!({
                    "error": {
                        "message": "Video task not found",
                    }
                })),
            )
                .into_response(),
            CancelVideoTaskError::InvalidStatus(status) => (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": {
                        "message": format!(
                            "Cannot cancel task with status: {}",
                            video_task_status_name(status),
                        ),
                    }
                })),
            )
                .into_response(),
            CancelVideoTaskError::Response(response) => response,
            CancelVideoTaskError::Gateway(err) => err.into_response(),
        })?;

    Ok(Json(json!({
        "id": stored.id,
        "status": "cancelled",
        "message": "Task cancelled successfully",
    })))
}

pub(crate) async fn get_video_task_video(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<axum::response::Response, axum::response::Response> {
    let Some(source) = read_video_task_video_source(&state, &task_id)
        .await
        .map_err(IntoResponse::into_response)?
    else {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": "Video task or video not found",
                }
            })),
        )
            .into_response());
    };

    build_video_task_video_response(&state, &task_id, source)
        .await
        .map_err(IntoResponse::into_response)
}

pub(crate) async fn cancel_video_task_record(
    state: &AppState,
    task_id: &str,
) -> Result<StoredVideoTask, CancelVideoTaskError> {
    let Some(task) = read_video_task_detail(state, task_id).await? else {
        return Err(CancelVideoTaskError::NotFound);
    };

    if matches!(
        task.status,
        VideoTaskStatus::Completed
            | VideoTaskStatus::Failed
            | VideoTaskStatus::Cancelled
            | VideoTaskStatus::Expired
            | VideoTaskStatus::Deleted
    ) {
        return Err(CancelVideoTaskError::InvalidStatus(task.status));
    }

    let trace_id = format!("async-task-admin-cancel-{task_id}");
    if let Some(cancel_plan) = build_video_task_cancel_plan(&task) {
        state
            .hydrate_video_task_for_route(Some(cancel_plan.route_family), &cancel_plan.request_path)
            .await?;

        let body_json = json!({});
        let follow_up = state.video_tasks.prepare_follow_up_sync_plan(
            cancel_plan.plan_kind,
            &cancel_plan.request_path,
            Some(&body_json),
            None,
            &trace_id,
        );

        if let (Some(executor_base_url), Some(follow_up)) =
            (state.executor_base_url.as_deref(), follow_up)
        {
            execute_video_task_cancel_plan(state, &trace_id, executor_base_url, follow_up.plan)
                .await
                .map_err(CancelVideoTaskError::Response)?;
        }

        state
            .video_tasks
            .apply_finalize_mutation(&cancel_plan.request_path, cancel_plan.report_kind);
    }

    let request_metadata = build_cancelled_request_metadata(state, &task).await?;
    let stored = persist_cancelled_video_task(state, &task, request_metadata)
        .await?
        .ok_or_else(|| {
            CancelVideoTaskError::Gateway(GatewayError::Internal(
                "video task repository is unavailable".to_string(),
            ))
        })?;
    finalize_video_task_if_terminal(state, &stored).await;
    Ok(stored)
}

pub(crate) async fn build_video_task_video_response(
    state: &AppState,
    task_id: &str,
    source: VideoTaskVideoSource,
) -> Result<axum::response::Response, GatewayError> {
    match source {
        VideoTaskVideoSource::Redirect { url } => Ok(Redirect::temporary(&url).into_response()),
        VideoTaskVideoSource::Proxy {
            url,
            header_name,
            header_value,
            filename,
        } => proxy_video_stream(state, task_id, &url, &header_name, &header_value, &filename).await,
    }
}

fn parse_filter(
    query: &ListVideoTasksQuery,
) -> Result<VideoTaskQueryFilter, axum::response::Response> {
    let status = match query.status.as_deref() {
        Some(value) => Some(VideoTaskStatus::from_database(value).map_err(|err| {
            (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": {
                        "message": err.to_string(),
                    }
                })),
            )
                .into_response()
        })?),
        None => None,
    };

    Ok(VideoTaskQueryFilter {
        user_id: query.user_id.clone(),
        status,
        model_substring: query.model.clone(),
        client_api_format: query.client_api_format.clone(),
    })
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone)]
struct VideoTaskCancelPlan<'a> {
    route_family: &'a str,
    plan_kind: &'a str,
    report_kind: &'a str,
    request_path: String,
}

fn build_video_task_cancel_plan(task: &StoredVideoTask) -> Option<VideoTaskCancelPlan<'_>> {
    let provider_api_format = task
        .provider_api_format
        .as_deref()
        .or(task.client_api_format.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    match provider_api_format {
        "openai:video" => Some(VideoTaskCancelPlan {
            route_family: "openai",
            plan_kind: "openai_video_cancel_sync",
            report_kind: "openai_video_cancel_sync_finalize",
            request_path: format!("/v1/videos/{}/cancel", task.id),
        }),
        "gemini:video" => {
            let short_id = task.short_id.as_deref().unwrap_or(task.id.as_str()).trim();
            let model = task
                .model
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())?;
            Some(VideoTaskCancelPlan {
                route_family: "gemini",
                plan_kind: "gemini_video_cancel_sync",
                report_kind: "gemini_video_cancel_sync_finalize",
                request_path: format!("/v1beta/models/{model}/operations/{short_id}:cancel"),
            })
        }
        _ => None,
    }
}

async fn execute_video_task_cancel_plan(
    state: &AppState,
    trace_id: &str,
    executor_base_url: &str,
    plan: aether_contracts::ExecutionPlan,
) -> Result<(), axum::response::Response> {
    let response = state
        .client
        .post(format!("{executor_base_url}/v1/execute/sync"))
        .json(&plan)
        .send()
        .await
        .map_err(|err| {
            GatewayError::UpstreamUnavailable {
                trace_id: trace_id.to_string(),
                message: err.to_string(),
            }
            .into_response()
        })?;

    if response.status() != axum::http::StatusCode::OK {
        return Err(GatewayError::UpstreamUnavailable {
            trace_id: trace_id.to_string(),
            message: format!("executor returned HTTP {}", response.status()),
        }
        .into_response());
    }

    let result: ExecutionResult = response.json().await.map_err(|err| {
        GatewayError::Internal(format!("failed to parse executor cancel response: {err}"))
            .into_response()
    })?;

    if result.status_code >= 400 {
        let status = axum::http::StatusCode::from_u16(result.status_code)
            .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
        let body_json = result
            .body
            .and_then(|body| body.json_body)
            .unwrap_or_else(|| {
                json!({
                    "error": {
                        "message": result
                            .error
                            .as_ref()
                            .map(|error| error.message.clone())
                            .unwrap_or_else(|| format!("executor returned {}", result.status_code)),
                    }
                })
            });
        return Err((status, Json(body_json)).into_response());
    }

    Ok(())
}

async fn build_cancelled_request_metadata(
    state: &AppState,
    task: &StoredVideoTask,
) -> Result<Option<Value>, GatewayError> {
    let mut metadata = match task.request_metadata.clone() {
        Some(Value::Object(object)) => object,
        _ => Map::new(),
    };
    let mut snapshot_value = metadata.get("rust_local_snapshot").cloned();
    if snapshot_value.is_none() {
        snapshot_value = state
            .reconstruct_video_task_snapshot(task)
            .await?
            .map(|snapshot| {
                serde_json::to_value(snapshot)
                    .map_err(|err| GatewayError::Internal(err.to_string()))
            })
            .transpose()?;
    }
    if let Some(snapshot_value_ref) = snapshot_value.as_mut() {
        mark_snapshot_value_cancelled(snapshot_value_ref);
        metadata.insert(
            "rust_owner".to_string(),
            Value::String("async_task".to_string()),
        );
        metadata.insert(
            "rust_local_snapshot".to_string(),
            snapshot_value_ref.clone(),
        );
        return Ok(Some(Value::Object(metadata)));
    }

    Ok(task.request_metadata.clone())
}

fn mark_snapshot_value_cancelled(snapshot_value: &mut Value) {
    if let Some(object) = snapshot_value
        .get_mut("OpenAi")
        .and_then(Value::as_object_mut)
    {
        object.insert("status".to_string(), Value::String("Cancelled".to_string()));
        return;
    }
    if let Some(object) = snapshot_value
        .get_mut("Gemini")
        .and_then(Value::as_object_mut)
    {
        object.insert("status".to_string(), Value::String("Cancelled".to_string()));
    }
}

async fn persist_cancelled_video_task(
    state: &AppState,
    task: &StoredVideoTask,
    request_metadata: Option<Value>,
) -> Result<Option<StoredVideoTask>, GatewayError> {
    let now_unix_secs = current_unix_secs();
    state
        .data
        .upsert_video_task(UpsertVideoTask {
            id: task.id.clone(),
            short_id: task.short_id.clone(),
            request_id: task.request_id.clone(),
            user_id: task.user_id.clone(),
            api_key_id: task.api_key_id.clone(),
            username: task.username.clone(),
            api_key_name: task.api_key_name.clone(),
            external_task_id: task.external_task_id.clone(),
            provider_id: task.provider_id.clone(),
            endpoint_id: task.endpoint_id.clone(),
            key_id: task.key_id.clone(),
            client_api_format: task.client_api_format.clone(),
            provider_api_format: task.provider_api_format.clone(),
            format_converted: task.format_converted,
            model: task.model.clone(),
            prompt: task.prompt.clone(),
            original_request_body: task.original_request_body.clone(),
            duration_seconds: task.duration_seconds,
            resolution: task.resolution.clone(),
            aspect_ratio: task.aspect_ratio.clone(),
            size: task.size.clone(),
            status: VideoTaskStatus::Cancelled,
            progress_percent: task.progress_percent,
            progress_message: task.progress_message.clone(),
            retry_count: task.retry_count,
            poll_interval_seconds: task.poll_interval_seconds,
            next_poll_at_unix_secs: None,
            poll_count: task.poll_count,
            max_poll_count: task.max_poll_count,
            created_at_unix_secs: task.created_at_unix_secs,
            submitted_at_unix_secs: task.submitted_at_unix_secs,
            completed_at_unix_secs: Some(now_unix_secs),
            updated_at_unix_secs: now_unix_secs,
            error_code: task.error_code.clone(),
            error_message: task.error_message.clone(),
            video_url: task.video_url.clone(),
            request_metadata,
        })
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))
}

fn video_task_status_name(status: VideoTaskStatus) -> &'static str {
    match status {
        VideoTaskStatus::Pending => "pending",
        VideoTaskStatus::Submitted => "submitted",
        VideoTaskStatus::Queued => "queued",
        VideoTaskStatus::Processing => "processing",
        VideoTaskStatus::Completed => "completed",
        VideoTaskStatus::Failed => "failed",
        VideoTaskStatus::Cancelled => "cancelled",
        VideoTaskStatus::Expired => "expired",
        VideoTaskStatus::Deleted => "deleted",
    }
}

async fn proxy_video_stream(
    state: &AppState,
    task_id: &str,
    url: &str,
    header_name: &str,
    header_value: &str,
    filename: &str,
) -> Result<axum::response::Response, GatewayError> {
    let response = state
        .client
        .get(url)
        .header(header_name, header_value)
        .send()
        .await
        .map_err(|err| GatewayError::UpstreamUnavailable {
            trace_id: task_id.to_string(),
            message: err.to_string(),
        })?;

    if response.status().is_client_error() || response.status().is_server_error() {
        return Err(GatewayError::UpstreamUnavailable {
            trace_id: task_id.to_string(),
            message: format!("video upstream returned HTTP {}", response.status()),
        });
    }

    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| axum::http::HeaderValue::from_static("video/mp4"));
    let content_length = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .cloned();
    let cache_control = response
        .headers()
        .get(axum::http::header::CACHE_CONTROL)
        .cloned();
    let body = Body::from_stream(response.bytes_stream());

    let mut outbound = axum::http::Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    outbound
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, content_type);
    outbound.headers_mut().insert(
        axum::http::header::CONTENT_DISPOSITION,
        axum::http::HeaderValue::from_str(&format!("inline; filename=\"{filename}\""))
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    if let Some(content_length) = content_length {
        outbound
            .headers_mut()
            .insert(axum::http::header::CONTENT_LENGTH, content_length);
    }
    if let Some(cache_control) = cache_control {
        outbound
            .headers_mut()
            .insert(axum::http::header::CACHE_CONTROL, cache_control);
    } else {
        outbound.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("private, max-age=3600"),
        );
    }
    Ok(outbound)
}
