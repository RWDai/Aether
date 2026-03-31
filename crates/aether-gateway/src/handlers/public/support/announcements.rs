#[derive(Debug, Deserialize)]
struct AdminAnnouncementCreateRequest {
    title: String,
    content: String,
    #[serde(rename = "type", default = "default_announcement_kind")]
    kind: String,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    is_pinned: bool,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AdminAnnouncementUpdateRequest {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    is_active: Option<bool>,
    #[serde(default)]
    is_pinned: Option<bool>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
}

fn default_announcement_kind() -> String {
    "info".to_string()
}

fn public_announcement_id_from_path(request_path: &str) -> Option<&str> {
    let announcement_id = request_path.strip_prefix("/api/announcements/")?;
    if announcement_id.is_empty()
        || announcement_id.contains('/')
        || matches!(announcement_id, "active" | "users")
    {
        return None;
    }
    Some(announcement_id)
}

fn announcement_user_read_status_id_from_path(request_path: &str) -> Option<&str> {
    let announcement_id = request_path
        .trim_end_matches('/')
        .strip_prefix("/api/announcements/")?
        .strip_suffix("/read-status")?;
    if announcement_id.is_empty() || announcement_id.contains('/') || announcement_id == "users" {
        return None;
    }
    Some(announcement_id)
}

async fn mark_all_active_announcements_as_read(
    state: &AppState,
    user_id: &str,
    now_unix_secs: u64,
) -> Result<(), GatewayError> {
    const ANNOUNCEMENT_PAGE_SIZE: usize = 200;

    let mut offset = 0usize;
    loop {
        let page = state
            .list_announcements(&aether_data::repository::announcements::AnnouncementListQuery {
                active_only: true,
                offset,
                limit: ANNOUNCEMENT_PAGE_SIZE,
                now_unix_secs: Some(now_unix_secs),
            })
            .await?;
        if page.items.is_empty() {
            break;
        }

        for announcement in &page.items {
            state
                .mark_announcement_as_read(user_id, &announcement.id, now_unix_secs)
                .await?;
        }

        offset += page.items.len();
        if offset as u64 >= page.total {
            break;
        }
    }

    Ok(())
}

async fn maybe_build_local_announcement_user_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_family.as_deref() != Some("announcement_user_legacy") {
        return None;
    }

    if !state.has_announcement_data_reader() {
        return Some(build_public_support_maintenance_response(
            "Announcement user routes require Rust maintenance backend",
        ));
    }

    let auth = match resolve_authenticated_local_user(state, request_context, headers).await {
        Ok(value) => value,
        Err(response) => return Some(response),
    };

    match decision.route_kind.as_deref() {
        Some("unread_count")
            if request_context.request_method == http::Method::GET
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/announcements/users/me/unread-count"
                        | "/api/announcements/users/me/unread-count/"
                ) =>
        {
            let now_unix_secs = Utc::now().timestamp().max(0) as u64;
            let unread_count = match state
                .count_unread_active_announcements(&auth.user.id, now_unix_secs)
                .await
            {
                Ok(value) => value,
                Err(err) => return Some(build_announcement_gateway_error_response(err)),
            };

            Some(
                build_auth_json_response(
                    http::StatusCode::OK,
                    json!({ "unread_count": unread_count }),
                    None,
                ),
            )
        }
        Some("read_all")
            if request_context.request_method == http::Method::POST
                && matches!(
                    request_context.request_path.as_str(),
                    "/api/announcements/read-all" | "/api/announcements/read-all/"
                ) =>
        {
            if !state.has_announcement_data_writer() {
                return Some(build_public_support_maintenance_response(
                    "Announcement user routes require Rust maintenance backend",
                ));
            }

            let now_unix_secs = Utc::now().timestamp().max(0) as u64;
            if let Err(err) =
                mark_all_active_announcements_as_read(state, &auth.user.id, now_unix_secs).await
            {
                return Some(build_announcement_gateway_error_response(err));
            }

            Some(
                build_auth_json_response(
                    http::StatusCode::OK,
                    json!({ "message": "已全部标记为已读" }),
                    None,
                ),
            )
        }
        Some("read_status") if request_context.request_method == http::Method::PATCH => {
            if !state.has_announcement_data_writer() {
                return Some(build_public_support_maintenance_response(
                    "Announcement user routes require Rust maintenance backend",
                ));
            }

            let Some(announcement_id) =
                announcement_user_read_status_id_from_path(&request_context.request_path)
            else {
                return Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Announcement not found" })),
                    )
                        .into_response(),
                );
            };

            match state.find_announcement_by_id(announcement_id).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Some(
                        (
                            http::StatusCode::NOT_FOUND,
                            Json(json!({ "detail": "Announcement not found" })),
                        )
                            .into_response(),
                    );
                }
                Err(err) => return Some(build_announcement_gateway_error_response(err)),
            }

            let now_unix_secs = Utc::now().timestamp().max(0) as u64;
            if let Err(err) = state
                .mark_announcement_as_read(&auth.user.id, announcement_id, now_unix_secs)
                .await
            {
                return Some(build_announcement_gateway_error_response(err));
            }

            Some(
                build_auth_json_response(
                    http::StatusCode::OK,
                    json!({ "message": "公告已标记为已读" }),
                    None,
                ),
            )
        }
        _ => None,
    }
}

fn parse_public_announcements_query(
    query: Option<&str>,
    active_only_default: bool,
    limit_default: usize,
) -> Result<aether_data::repository::announcements::AnnouncementListQuery, String> {
    let active_only = query_param_optional_bool(query, "active_only").unwrap_or(active_only_default);
    let limit = match query_param_value(query, "limit") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "limit must be a valid integer".to_string())?,
        None => limit_default,
    };
    let offset = match query_param_value(query, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "offset must be a valid integer".to_string())?,
        None => 0,
    };

    Ok(aether_data::repository::announcements::AnnouncementListQuery {
        active_only,
        offset,
        limit,
        now_unix_secs: Some(Utc::now().timestamp().max(0) as u64),
    })
}

fn build_public_announcement_list_payload(
    page: aether_data::repository::announcements::StoredAnnouncementPage,
) -> serde_json::Value {
    json!({
        "items": page.items.into_iter().map(|announcement| {
            let mut payload = build_public_announcement_payload(&announcement);
            if let Some(object) = payload.as_object_mut() {
                object.insert("is_active".to_string(), json!(announcement.is_active));
            }
            payload
        }).collect::<Vec<_>>(),
        "total": page.total,
    })
}

fn build_public_announcement_payload(
    announcement: &aether_data::repository::announcements::StoredAnnouncement,
) -> serde_json::Value {
    json!({
        "id": announcement.id,
        "title": announcement.title,
        "content": announcement.content,
        "type": announcement.kind,
        "priority": announcement.priority,
        "is_pinned": announcement.is_pinned,
        "author": {
            "id": announcement.author_id,
            "username": announcement.author_username,
        },
        "start_time": announcement.start_time_unix_secs.and_then(unix_secs_to_rfc3339),
        "end_time": announcement.end_time_unix_secs.and_then(unix_secs_to_rfc3339),
        "created_at": unix_secs_to_rfc3339(announcement.created_at_unix_secs),
        "updated_at": unix_secs_to_rfc3339(announcement.updated_at_unix_secs),
    })
}

fn parse_announcement_timestamp(
    raw: Option<&str>,
    field_name: &str,
) -> Result<Option<u64>, String> {
    match raw {
        Some(value) => parse_optional_rfc3339_unix_secs(value, field_name).map(Some),
        None => Ok(None),
    }
}

fn announcement_kind_is_valid(kind: &str) -> bool {
    matches!(kind, "info" | "warning" | "maintenance" | "important")
}

fn build_announcement_gateway_error_response(err: GatewayError) -> Response<Body> {
    let detail = match err {
        GatewayError::UpstreamUnavailable { message, .. }
        | GatewayError::ControlUnavailable { message, .. }
        | GatewayError::Internal(message) => message,
    };
    (
        http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "detail": detail })),
    )
        .into_response()
}

async fn maybe_build_local_admin_announcements_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.control_decision.as_ref() else {
        return Ok(None);
    };
    if decision.route_family.as_deref() != Some("announcements_manage") {
        return Ok(None);
    }
    let Some(admin_principal) = decision.admin_principal.as_ref() else {
        return Ok(None);
    };
    if !state.has_announcement_data_writer() {
        return Ok(Some(build_proxy_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            "maintenance_mode",
            "Announcements write routes require Rust maintenance backend",
            Some(json!({
                "error": "Announcements write routes require Rust maintenance backend",
            })),
        )));
    }

    if decision.route_kind.as_deref() == Some("create_announcement")
        && request_context.request_method == http::Method::POST
        && matches!(
            request_context.request_path.as_str(),
            "/api/announcements" | "/api/announcements/"
        )
    {
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response(),
            ));
        };
        let payload = match serde_json::from_slice::<AdminAnnouncementCreateRequest>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求数据验证失败" })),
                    )
                        .into_response(),
                ));
            }
        };
        if !announcement_kind_is_valid(&payload.kind) {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "Invalid announcement type" })),
                )
                    .into_response(),
            ));
        }
        let start_time_unix_secs =
            match parse_announcement_timestamp(payload.start_time.as_deref(), "start_time") {
                Ok(value) => value,
                Err(detail) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": detail })),
                        )
                            .into_response(),
                    ));
                }
            };
        let end_time_unix_secs =
            match parse_announcement_timestamp(payload.end_time.as_deref(), "end_time") {
                Ok(value) => value,
                Err(detail) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": detail })),
                        )
                            .into_response(),
                    ));
                }
            };

        let record = aether_data::repository::announcements::CreateAnnouncementRecord {
            title: payload.title,
            content: payload.content,
            kind: payload.kind,
            priority: payload.priority,
            is_pinned: payload.is_pinned,
            author_id: admin_principal.user_id.clone(),
            start_time_unix_secs,
            end_time_unix_secs,
        };
        let created = match state.create_announcement(record).await {
            Ok(Some(created)) => created,
            Ok(None) => {
                return Ok(Some(build_proxy_error_response(
                    http::StatusCode::SERVICE_UNAVAILABLE,
                    "maintenance_mode",
                    "Announcements write routes require Rust maintenance backend",
                    Some(json!({
                        "error": "Announcements write routes require Rust maintenance backend",
                    })),
                )));
            }
            Err(GatewayError::Internal(message)) if message.starts_with("invalid input: ") => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": message.trim_start_matches("invalid input: ") })),
                    )
                        .into_response(),
                ));
            }
            Err(err) => return Ok(Some(build_announcement_gateway_error_response(err))),
        };

        return Ok(Some(
            Json(json!({
                "id": created.id,
                "title": created.title,
                "message": "公告创建成功",
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("update_announcement")
        && request_context.request_method == http::Method::PUT
    {
        let Some(announcement_id) = public_announcement_id_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Announcement not found" })),
                )
                    .into_response(),
            ));
        };
        let Some(request_body) = request_body else {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "请求数据验证失败" })),
                )
                    .into_response(),
            ));
        };
        let payload = match serde_json::from_slice::<AdminAnnouncementUpdateRequest>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求数据验证失败" })),
                    )
                        .into_response(),
                ));
            }
        };
        if payload
            .kind
            .as_deref()
            .is_some_and(|kind| !announcement_kind_is_valid(kind))
        {
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "Invalid announcement type" })),
                )
                    .into_response(),
            ));
        }
        let start_time_unix_secs =
            match parse_announcement_timestamp(payload.start_time.as_deref(), "start_time") {
                Ok(value) => value,
                Err(detail) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": detail })),
                        )
                            .into_response(),
                    ));
                }
            };
        let end_time_unix_secs =
            match parse_announcement_timestamp(payload.end_time.as_deref(), "end_time") {
                Ok(value) => value,
                Err(detail) => {
                    return Ok(Some(
                        (
                            http::StatusCode::BAD_REQUEST,
                            Json(json!({ "detail": detail })),
                        )
                            .into_response(),
                    ));
                }
            };

        let updated = match state
            .update_announcement(aether_data::repository::announcements::UpdateAnnouncementRecord {
                announcement_id: announcement_id.to_string(),
                title: payload.title,
                content: payload.content,
                kind: payload.kind,
                priority: payload.priority,
                is_active: payload.is_active,
                is_pinned: payload.is_pinned,
                start_time_unix_secs,
                end_time_unix_secs,
            })
            .await
        {
            Ok(Some(updated)) => updated,
            Ok(None) => {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Announcement not found" })),
                    )
                        .into_response(),
                ));
            }
            Err(GatewayError::Internal(message)) if message.starts_with("invalid input: ") => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": message.trim_start_matches("invalid input: ") })),
                    )
                        .into_response(),
                ));
            }
            Err(err) => return Ok(Some(build_announcement_gateway_error_response(err))),
        };
        let _ = updated;

        return Ok(Some(
            Json(json!({
                "message": "公告更新成功",
            }))
            .into_response(),
        ));
    }

    if decision.route_kind.as_deref() == Some("delete_announcement")
        && request_context.request_method == http::Method::DELETE
    {
        let Some(announcement_id) = public_announcement_id_from_path(&request_context.request_path)
        else {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Announcement not found" })),
                )
                    .into_response(),
            ));
        };

        let deleted = match state.delete_announcement(announcement_id).await {
            Ok(value) => value,
            Err(err) => return Ok(Some(build_announcement_gateway_error_response(err))),
        };
        if !deleted {
            return Ok(Some(
                (
                    http::StatusCode::NOT_FOUND,
                    Json(json!({ "detail": "Announcement not found" })),
                )
                    .into_response(),
            ));
        }

        return Ok(Some(
            Json(json!({
                "message": "公告已删除",
            }))
            .into_response(),
        ));
    }

    Ok(None)
}
