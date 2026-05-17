---
name: operating-services
description: Use to inspect, health-check, read logs from, or restart MDS Demo platform services. Use when a service is down, slow, unhealthy, or a user asks to restart something.
---

# Operating Services

All lifecycle commands use Compose with an explicit project name and the
mounted compose file:

    DC="docker compose -p mds_demo -f /workspace/docker-compose.yml"

## Inspect

- List services and state: `$DC ps`
- Recent logs: `$DC logs --tail 200 <service>`
- Live resource usage: `docker stats --no-stream`
- Health detail: `docker inspect --format '{{json .State.Health}}' <container>`

## Restart

- Restart one service: `$DC restart <service>`
- After a restart, confirm with `$DC ps` and a fresh `logs --tail 50`.

## Health-check sweep

For each service, report state (running / restarting / exited) and, where a
healthcheck exists (`minio`, `warehouse_db`, `redis`), its health status.
Flag anything not `running`/`healthy`.

## Boundaries

Do NOT run `up`, `down`, `build`, or `create` — container re-creation is out
of scope and needs host paths unavailable here. `restart` is always safe.

If you hit `permission denied` on the Docker socket, see
`.claude/knowledge/runbooks.md`.


## MDS Platform Context
- **Warehouse:** Postgres 17 (`warehouse_db`) with `pg_duckdb` and `pgvector`.
- **Storage:** MinIO (`dwhfilesystem`) for landing area.
- **Orchestration:** Mage AI with dbt-core.
- **Federation:** Trino for cross-source joins.
- **BI:** Metabase dashboards.
