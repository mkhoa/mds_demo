---
name: querying-the-warehouse
description: Use to run SQL queries against the warehouse_db Postgres database — inspecting tables, row counts, schemas, or answering data questions. Use when a user asks what is in a table or wants a number from the data.
---

# Querying the Warehouse

Run `psql` inside the `warehouse_db` container. Credentials live in the
container's environment, so wrap the query in `sh -c` and reference the
container's own `$POSTGRES_USER` / `$POSTGRES_DB`:

    DC="docker compose -p mds_demo -f /workspace/docker-compose.yml"
    $DC exec -T warehouse_db sh -c \
      'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT ..."'

The data warehouse database is `warehouse` (the value of `$POSTGRES_DB`).

## Common queries

- List schemas: `\dn`
- List tables in a layer: `\dt stg.*` (or `bdh.*`, `adl.*`)
- Row count: `SELECT count(*) FROM <schema>.<table>;`
- Columns: `\d <schema>.<table>`

## Rules

Read-only by default: `SELECT`, `\dt`, `\d`, `count(*)`. Never `DROP`,
`TRUNCATE`, or `DELETE` against warehouse schemas — that is out of scope.
For analytical scans, pg_duckdb is available; standard SQL is fine for
inspection.
