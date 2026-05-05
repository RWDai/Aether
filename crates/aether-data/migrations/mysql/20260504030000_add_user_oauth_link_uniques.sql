ALTER TABLE user_oauth_links
    ADD UNIQUE KEY uq_user_oauth_links_provider_user (provider_type, provider_user_id),
    ADD UNIQUE KEY uq_user_oauth_links_user_provider (user_id, provider_type);
