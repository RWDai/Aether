# aether-data-schema

`aether-data-schema` is the logical schema generator for `aether-data`.

It owns:

- parsing `crates/aether-data/schema/logical/*.toml`
- validating table, column, index, unique constraint, and foreign-key metadata
- emitting driver-specific DDL for Postgres, MySQL, and SQLite
- checking that generated schema artifacts are current
- cleaning generated driver baseline directories before writing fresh output

It does not own runtime migrations or repository SQL. `aether-data` still owns
`sqlx::migrate!`, backfills, export/import, and repository implementations.

## Commands

From the workspace root:

```bash
cargo run -p aether-data-schema --bin aether-schema -- check
cargo run -p aether-data-schema --bin aether-schema -- generate
cargo run -p aether-data-schema --bin aether-schema -- print --driver postgres
cargo run -p aether-data-schema --bin aether-schema -- print --driver mysql
cargo run -p aether-data-schema --bin aether-schema -- print --driver sqlite
```

The normal `aether-data` maintenance entrypoint wraps these:

```bash
bash crates/aether-data/schema/compose_schema.sh generate
bash crates/aether-data/schema/compose_schema.sh check
```

## Source And Output

Input:

```text
crates/aether-data/schema/logical/*.toml
```

Output:

```text
crates/aether-data/schema/generated/README.md
crates/aether-data/schema/generated/{postgres,mysql,sqlite}/baseline/
```

Each logical TOML file becomes one generated `.sql` file per driver, and each
driver output directory gets a generated `manifest.txt`.

`generated/**` is machine-written. The generated directory README and each
generated SQL/manifest file state that the content should not be edited by hand.
Edit `logical/*.toml` instead.

The generator writes one manifest per driver output directory. A stale README,
stale generated file, or extra generated driver file is treated as an error by
`aether-schema check`.

## Current Coverage

Logical schema now covers the clean baseline table set plus MySQL/SQLite table
creation migrations:

- identity, API keys, audit logs, announcements, management tokens, preferences,
  and sessions
- provider catalog, provider keys/endpoints, model catalog, request candidates,
  Gemini file mappings, and video tasks
- auth config, OAuth providers, LDAP config, and OAuth links
- proxy nodes and proxy events
- wallet, payment, refund, redeem-code, and settlement snapshot tables
- usage capture and portable stats aggregation tables

Postgres-only historical follow-up migrations still live in driver-specific SQL
until their shape is normalized or intentionally kept as overrides.
