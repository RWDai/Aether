#![allow(
    dead_code,
    unused_assignments,
    unused_imports,
    unused_mut,
    unused_variables,
    clippy::bool_assert_comparison,
    clippy::collapsible_if,
    clippy::empty_line_after_outer_attr,
    clippy::field_reassign_with_default,
    clippy::if_same_then_else,
    clippy::large_enum_variant,
    clippy::manual_div_ceil,
    clippy::manual_find,
    clippy::match_like_matches_macro,
    clippy::needless_as_bytes,
    clippy::needless_lifetimes,
    clippy::nonminimal_bool,
    clippy::question_mark,
    clippy::redundant_closure,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::useless_concat
)]

mod gateway;

pub use gateway::{
    build_router, build_router_with_control, build_router_with_endpoints, build_router_with_state,
    serve_tcp, serve_tcp_with_endpoints, AppState, FrontdoorCorsConfig, FrontdoorUserRpmConfig,
    GatewayDataConfig, UsageRuntimeConfig, VideoTaskTruthSourceMode,
};
