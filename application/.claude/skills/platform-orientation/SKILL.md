---
name: platform-orientation
description: Use to understand the MDS Demo platform topology — which services exist, how they connect, and how data flows from sources through MinIO and the warehouse to BI. Use when a task spans multiple services or you need to locate where something runs.
---

# Platform Orientation

The MDS Demo platform is a Modern Data Stack in Docker Compose (project
`mds_demo`, network `ndsnet`).

## Data flow

```
Sources -> MinIO (raw landing, s3://dwhfilesystem) -> dlt (in Mage) ->
warehouse_db.raw -> dbt -> stg -> bdh -> adl -> Metabase / JupyterLab
```

## Services and what they do

- `minio` — S3-compatible object storage. Data lake bucket: `dwhfilesystem`.
- `warehouse_db` — Postgres 17 warehouse. Databases: `warehouse` (data),
  plus `metastore`, `metabase`, `openwebui`, `litellm` app DBs.
- `magic` — Mage.ai. Runs dlt (extract/load) and dbt (transform). The dbt
  project is at `/home/src/mds_demo/dbt/data_warehouse` inside this container.
- `trino` — federated SQL over Postgres, Iceberg, and Delta catalogs.
- `hive-metastore` — Delta Lake table metadata for Trino.
- `metabase` — BI dashboards.
- `jupyterlab` — notebooks.

## dbt schema layers

`raw` (views over MinIO parquet) -> `stg` (bronze, cleaned tables) ->
`bdh` (silver, conformed dims/facts) -> `adl` (gold, aggregated KPIs).

## How to act on a service

See the `operating-services` skill. All commands go through:
`docker compose -p mds_demo -f /workspace/docker-compose.yml ...`
