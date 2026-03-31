#![allow(dead_code, unused_imports)]

pub(crate) mod auth;
pub(crate) mod policy;
pub(crate) mod url;

pub(crate) use auth::{
    resolve_local_vertex_api_key_query_auth, VertexApiKeyQueryAuth, VERTEX_API_KEY_QUERY_PARAM,
};
pub(crate) use policy::{
    supports_local_vertex_api_key_gemini_transport,
    supports_local_vertex_api_key_gemini_transport_with_network,
    supports_local_vertex_api_key_imagen_transport,
    supports_local_vertex_api_key_imagen_transport_with_network,
};
pub(crate) use url::{
    build_vertex_api_key_gemini_content_url, build_vertex_api_key_imagen_content_url,
    VERTEX_API_KEY_BASE_URL,
};

pub(crate) const PROVIDER_TYPE: &str = "vertex_ai";
