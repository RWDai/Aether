use axum::routing::{any, post};
use axum::Router;

use crate::gateway::{proxy_request, AppState};

pub(crate) fn mount_internal_routes(router: Router<AppState>) -> Router<AppState> {
    router
        .route(
            "/api/internal/gateway/{*legacy_gateway_path}",
            any(proxy_request),
        )
        .route("/api/internal/hub/heartbeat", post(proxy_request))
        .route("/api/internal/hub/node-status", post(proxy_request))
}
