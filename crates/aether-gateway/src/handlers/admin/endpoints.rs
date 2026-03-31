include!("endpoints/health.rs");
include!("endpoints/rpm.rs");
include!("endpoints/keys.rs");
include!("endpoints/routes.rs");

async fn maybe_build_local_admin_endpoints_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    if let Some(response) =
        maybe_build_local_admin_endpoints_health_response(state, request_context).await?
    {
        return Ok(Some(response));
    }

    if let Some(response) =
        maybe_build_local_admin_endpoints_rpm_response(state, request_context).await?
    {
        return Ok(Some(response));
    }

    if let Some(response) =
        maybe_build_local_admin_endpoints_keys_response(state, request_context, request_body)
            .await?
    {
        return Ok(Some(response));
    }

    if let Some(response) =
        maybe_build_local_admin_endpoints_routes_response(state, request_context, request_body)
            .await?
    {
        return Ok(Some(response));
    }

    Ok(None)
}
