CREATE TABLE IF NOT EXISTS stats_hourly (
    id TEXT PRIMARY KEY,
    hour_utc INTEGER NOT NULL UNIQUE,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    error_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    actual_total_cost REAL NOT NULL DEFAULT 0,
    avg_response_time_ms REAL NOT NULL DEFAULT 0,
    is_complete INTEGER NOT NULL DEFAULT 0,
    aggregated_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stats_hourly_user (
    id TEXT PRIMARY KEY,
    hour_utc INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    error_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (hour_utc, user_id)
);

CREATE TABLE IF NOT EXISTS stats_hourly_user_model (
    id TEXT PRIMARY KEY,
    hour_utc INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    model TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (hour_utc, user_id, model)
);

CREATE TABLE IF NOT EXISTS stats_hourly_model (
    id TEXT PRIMARY KEY,
    hour_utc INTEGER NOT NULL,
    model TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    avg_response_time_ms REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (hour_utc, model)
);

CREATE TABLE IF NOT EXISTS stats_hourly_provider (
    id TEXT PRIMARY KEY,
    hour_utc INTEGER NOT NULL,
    provider_name TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (hour_utc, provider_name)
);

CREATE TABLE IF NOT EXISTS stats_daily (
    id TEXT PRIMARY KEY,
    "date" INTEGER NOT NULL UNIQUE,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    error_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    actual_total_cost REAL NOT NULL DEFAULT 0,
    avg_response_time_ms REAL NOT NULL DEFAULT 0,
    fallback_count INTEGER NOT NULL DEFAULT 0,
    unique_models INTEGER NOT NULL DEFAULT 0,
    unique_providers INTEGER NOT NULL DEFAULT 0,
    is_complete INTEGER NOT NULL DEFAULT 0,
    aggregated_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stats_daily_model (
    id TEXT PRIMARY KEY,
    "date" INTEGER NOT NULL,
    model TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    avg_response_time_ms REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE ("date", model)
);

CREATE TABLE IF NOT EXISTS stats_daily_provider (
    id TEXT PRIMARY KEY,
    "date" INTEGER NOT NULL,
    provider_name TEXT NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE ("date", provider_name)
);

CREATE TABLE IF NOT EXISTS stats_daily_api_key (
    id TEXT PRIMARY KEY,
    api_key_id TEXT NOT NULL,
    "date" INTEGER NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    error_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    api_key_name TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE ("date", api_key_id)
);

CREATE TABLE IF NOT EXISTS stats_daily_error (
    id TEXT PRIMARY KEY,
    "date" INTEGER NOT NULL,
    error_category TEXT NOT NULL,
    provider_name TEXT,
    model TEXT,
    count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE ("date", error_category, provider_name, model)
);

CREATE TABLE IF NOT EXISTS stats_user_daily (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    "date" INTEGER NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    error_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost REAL NOT NULL DEFAULT 0,
    username TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE ("date", user_id)
);
