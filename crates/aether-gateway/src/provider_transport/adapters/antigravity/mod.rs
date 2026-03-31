#![allow(dead_code, unused_imports)]

mod auth;
mod policy;
mod request;
mod url;

pub(crate) use auth::{
    build_antigravity_static_identity_headers, resolve_local_antigravity_request_auth,
    AntigravityRequestAuth, AntigravityRequestAuthSupport, AntigravityRequestAuthUnsupportedReason,
    ANTIGRAVITY_PROVIDER_TYPE, ANTIGRAVITY_REQUEST_USER_AGENT,
};
pub(crate) use policy::{
    classify_local_antigravity_request_support, AntigravityRequestSideSpec,
    AntigravityRequestSideSupport, AntigravityRequestSideUnsupportedReason,
};
pub(crate) use request::{
    build_antigravity_safe_v1internal_request, classify_antigravity_safe_request_body,
    AntigravityEnvelopeRequestType, AntigravityRequestEnvelopeSupport,
    AntigravityRequestEnvelopeUnsupportedReason,
};
pub(crate) use url::{
    build_antigravity_v1internal_url, AntigravityRequestUrlAction,
    ANTIGRAVITY_V1INTERNAL_PATH_TEMPLATE,
};
