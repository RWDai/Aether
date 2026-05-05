CREATE TABLE IF NOT EXISTS audit_logs (
    id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    user_id VARCHAR(64),
    api_key_id VARCHAR(64),
    description TEXT NOT NULL,
    ip_address VARCHAR(64),
    user_agent VARCHAR(512),
    request_id VARCHAR(128),
    event_metadata TEXT,
    status_code INT,
    error_message TEXT,
    created_at BIGINT NOT NULL,
    KEY audit_logs_created_at_idx (created_at),
    KEY audit_logs_event_type_idx (event_type),
    KEY audit_logs_request_id_idx (request_id),
    KEY audit_logs_user_id_idx (user_id)
);
