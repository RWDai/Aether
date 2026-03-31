async fn handle_admin_provider_oauth_batch_import_task_status(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let Some((provider_id, task_id)) =
        admin_provider_oauth_batch_import_task_path(&request_context.request_path)
    else {
        return Ok(build_internal_hub_error_response(
            http::StatusCode::NOT_FOUND,
            "批量导入任务不存在",
        ));
    };
    let payload = match read_provider_oauth_batch_task_payload(state, &provider_id, &task_id).await
    {
        Ok(Some(payload)) => payload,
        Ok(None) => {
            return Ok(build_internal_hub_error_response(
                http::StatusCode::NOT_FOUND,
                "批量导入任务不存在或已过期",
            ));
        }
        Err(_) => {
            return Ok(build_internal_hub_error_response(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "provider oauth batch task redis unavailable",
            ));
        }
    };
    Ok(Json(payload).into_response())
}
