CREATE TABLE IF NOT EXISTS user_preferences (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    avatar_url TEXT,
    bio TEXT,
    default_provider_id TEXT,
    theme TEXT NOT NULL DEFAULT 'light',
    language TEXT NOT NULL DEFAULT 'zh-CN',
    timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
    email_notifications INTEGER NOT NULL DEFAULT 1,
    usage_alerts INTEGER NOT NULL DEFAULT 1,
    announcement_notifications INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS user_preferences_default_provider_id_idx
    ON user_preferences (default_provider_id);
CREATE INDEX IF NOT EXISTS user_preferences_user_id_idx
    ON user_preferences (user_id);

CREATE TABLE IF NOT EXISTS user_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    client_device_id TEXT NOT NULL,
    device_label TEXT,
    device_type TEXT NOT NULL DEFAULT 'unknown',
    browser_name TEXT,
    browser_version TEXT,
    os_name TEXT,
    os_version TEXT,
    device_model TEXT,
    ip_address TEXT,
    user_agent TEXT,
    client_hints TEXT,
    refresh_token_hash TEXT NOT NULL,
    prev_refresh_token_hash TEXT,
    rotated_at INTEGER,
    last_seen_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    revoke_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS user_sessions_user_active_idx
    ON user_sessions (user_id, revoked_at, expires_at);
CREATE INDEX IF NOT EXISTS user_sessions_user_device_idx
    ON user_sessions (user_id, client_device_id);
