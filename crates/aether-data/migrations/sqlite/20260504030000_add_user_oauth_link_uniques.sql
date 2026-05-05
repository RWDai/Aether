CREATE UNIQUE INDEX IF NOT EXISTS uq_user_oauth_links_provider_user
    ON user_oauth_links (provider_type, provider_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_oauth_links_user_provider
    ON user_oauth_links (user_id, provider_type);
