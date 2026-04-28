ALTER TABLE IF EXISTS public.usage
  ADD COLUMN IF NOT EXISTS client_ip character varying(45),
  ADD COLUMN IF NOT EXISTS user_agent text;

CREATE INDEX IF NOT EXISTS ix_usage_client_ip
    ON public.usage USING btree (client_ip);
