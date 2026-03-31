#[cfg(test)]
use axum::http::Uri;

#[path = "auth/mod.rs"]
mod auth;
#[path = "execute.rs"]
mod execute;
#[path = "public.rs"]
mod public;
#[path = "route.rs"]
mod route;

pub(crate) use auth::{
    request_model_local_rejection, resolve_executor_auth_context,
    should_buffer_request_for_local_auth, trusted_auth_local_rejection,
    GatewayAdminPrincipalContext, GatewayControlAuthContext, GatewayLocalAuthRejection,
};
pub(crate) use execute::{allows_control_execute_emergency, maybe_execute_via_control};
pub(crate) use public::{resolve_public_request_context, GatewayPublicRequestContext};
#[cfg(test)]
pub(crate) use route::classify_control_route;
pub(crate) use route::{resolve_control_route, GatewayControlDecision};

#[cfg(test)]
mod tests;
