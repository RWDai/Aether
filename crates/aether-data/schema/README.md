# Public Schema Snapshot

This directory stores a searchable snapshot of the current local Postgres `public` schema.

Purpose:
- Make legacy Python-era columns discoverable without reverse-tracing every migration.
- Keep `migrations/20260403000000_baseline.sql` as a no-op handoff point instead of stuffing the full legacy schema into a fake baseline.

Files:
- `current-public-tables.tsv`: `table_name`, `column_count`
- `current-public-columns.tsv`: `table_name`, `ordinal_position`, `column_name`, `data_type`, `is_nullable`, `column_default`, `column_comment`

Source:
- Generated from the local `aether` Postgres database on `2026-04-10`.

Refresh commands:
```bash
docker compose exec -T postgres psql -U postgres -d aether -At -F $'\t' -c "SELECT table_name, COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' GROUP BY table_name ORDER BY table_name;"
docker compose exec -T postgres psql -U postgres -d aether -At -F $'\t' -c "SELECT c.table_name, c.ordinal_position, c.column_name, c.data_type, c.is_nullable, COALESCE(c.column_default, ''), COALESCE(pg_catalog.col_description((quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass::oid, c.ordinal_position), '') FROM information_schema.columns c WHERE c.table_schema = 'public' ORDER BY c.table_name, c.ordinal_position;"
```

This snapshot is documentation only. Runtime code must still treat the actual database as source of truth.
