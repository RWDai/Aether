use super::*;

pub fn build_router(upstream_base_url: impl Into<String>) -> Result<Router, reqwest::Error> {
    build_router_with_control(upstream_base_url, None)
}

pub fn build_router_with_control(
    upstream_base_url: impl Into<String>,
    control_base_url: Option<String>,
) -> Result<Router, reqwest::Error> {
    Ok(build_router_with_state(AppState::new(
        upstream_base_url,
        control_base_url,
    )?))
}

pub fn build_router_with_endpoints(
    upstream_base_url: impl Into<String>,
    control_base_url: Option<String>,
    executor_base_url: Option<String>,
) -> Result<Router, reqwest::Error> {
    Ok(build_router_with_state(AppState::new_with_executor(
        upstream_base_url,
        control_base_url,
        executor_base_url,
    )?))
}

pub fn build_router_with_state(state: AppState) -> Router {
    let cors_state = state.clone();
    let mut router = Router::<AppState>::new();
    router = api::mount_core_routes(router);
    router = api::mount_operational_routes(router);
    router = api::mount_ai_routes(router);
    router = api::mount_public_support_routes(router);
    router = api::mount_oauth_routes(router);
    router = api::mount_internal_routes(router);
    router = api::mount_admin_routes(router);
    let mut router = router
        .route("/{*path}", any(proxy_request))
        .with_state(state);
    if cors_state.frontdoor_cors().is_some() {
        router = router.layer(axum::middleware::from_fn_with_state(
            cors_state,
            middleware::frontdoor_cors_middleware,
        ));
    }
    router
}

pub(crate) async fn metrics(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl axum::response::IntoResponse {
    prometheus_response(&state.metric_samples().await)
}

#[derive(Debug)]
pub(crate) enum RequestAdmissionError {
    Local(ConcurrencyError),
    Distributed(DistributedConcurrencyError),
}

pub async fn serve_tcp(
    bind: &str,
    upstream_base_url: &str,
    control_base_url: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_tcp_with_endpoints(bind, upstream_base_url, control_base_url, None).await
}

pub async fn serve_tcp_with_endpoints(
    bind: &str,
    upstream_base_url: &str,
    control_base_url: Option<&str>,
    executor_base_url: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let router = build_router_with_endpoints(
        upstream_base_url.to_string(),
        control_base_url.map(ToOwned::to_owned),
        executor_base_url.map(ToOwned::to_owned),
    )?;
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
