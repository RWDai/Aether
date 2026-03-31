include!("provider_ops/architectures.rs");
include!("provider_ops/providers.rs");

async fn maybe_build_local_admin_provider_ops_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    if let Some(response) =
        maybe_build_local_admin_provider_ops_architectures_response(request_context).await?
    {
        return Ok(Some(response));
    }

    if let Some(response) =
        maybe_build_local_admin_provider_ops_providers_response(state, request_context, request_body)
            .await?
    {
        return Ok(Some(response));
    }

    Ok(None)
}
