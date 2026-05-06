CREATE TABLE IF NOT EXISTS public.auth_modules (
    id character varying(36) PRIMARY KEY,
    module_type character varying(128) NOT NULL UNIQUE,
    enabled boolean DEFAULT true NOT NULL,
    config json DEFAULT '{}'::json NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
