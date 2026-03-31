#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GatewayCredentialCarrier {
    AuthorizationBearer,
    XApiKey,
    ApiKey,
    XGoogApiKey,
    QueryKey,
    CookieHeader,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct GatewayTrustedAuthHeaders {
    pub(super) user_id: String,
    pub(super) api_key_id: String,
    pub(super) balance_remaining: Option<f64>,
    pub(super) access_allowed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GatewayTrustedAdminHeaders {
    pub(super) user_id: String,
    pub(super) user_role: String,
    pub(super) session_id: Option<String>,
    pub(super) management_token_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct GatewayCredentialBundle {
    pub(super) authorization_bearer: Option<String>,
    pub(super) x_api_key: Option<String>,
    pub(super) api_key: Option<String>,
    pub(super) x_goog_api_key: Option<String>,
    pub(super) query_key: Option<String>,
    pub(super) cookie_header: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GatewayPrimaryCredential {
    ProviderApiKey {
        raw: String,
        carrier: GatewayCredentialCarrier,
    },
    BearerToken {
        raw: String,
        carrier: GatewayCredentialCarrier,
    },
    CookieHeader {
        raw: String,
        carrier: GatewayCredentialCarrier,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct GatewayExtractedCredentials {
    pub(super) trusted_headers: Option<GatewayTrustedAuthHeaders>,
    pub(super) trusted_admin_headers: Option<GatewayTrustedAdminHeaders>,
    pub(super) bundle: GatewayCredentialBundle,
    pub(super) primary: Option<GatewayPrimaryCredential>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum GatewayPrincipalCandidate {
    TrustedHeaders(GatewayTrustedAuthHeaders),
    ApiKeyHash {
        key_hash: String,
        carrier: GatewayCredentialCarrier,
    },
    DeferredBearerToken {
        raw: String,
        carrier: GatewayCredentialCarrier,
    },
    DeferredCookieHeader {
        raw: String,
        carrier: GatewayCredentialCarrier,
    },
}
