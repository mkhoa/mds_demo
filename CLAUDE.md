# MDS Demo — Claude Instructions

Self-contained Modern Data Stack reference platform running entirely in Docker Compose. Read this file before making any changes so you understand how the pieces fit together.

---

## Stack at a glance

| Layer | Tool | Image / Build |
|---|---|---|
| Object storage | MinIO | `minio/minio` |
| Data warehouse | PostgreSQL 17 + pg_duckdb + pgvector + PostGIS | custom `application/warehouse_db/Dockerfile` |
| Hive Metastore | Hive HMS (for Delta Lake / Trino) | `starburstdata/hive:3.1.3-e.8` |
| Extract & Load | dlt (inside Mage pipelines) | — |
| Transformation | dbt (`data_warehouse` project, inside Mage) | — |
| Orchestration | Mage.ai | custom `application/mage_ai/Dockerfile` |
| Federated query | Trino | `trinodb/trino:latest` |
| BI / Dashboards | Metabase | `metabase/metabase` |
| Data science | JupyterLab | custom `application/jupyterlab/Dockerfile` |
| End-user chat | OpenWebUI | `ghcr.io/open-webui/open-webui` |
| Operator agent | NousResearch Hermes | custom `application/hermes/Dockerfile` |
| Model gateway | LiteLLM | `ghcr.io/berriai/litellm` |
| Agent cache | Redis | `redis/redis-stack` |

---

## Service ports

| Service | Port | Notes |
|---|---|---|
| Mage.ai | 6789 | Orchestration UI |
| MinIO API | 9000 | S3 endpoint |
| MinIO Console | 9001 | Admin UI |
| Trino | 8080 | Federated SQL (HTTP only, single coordinator) |
| Hive Metastore | 9083 | Thrift — internal use by Trino only |
| Metabase | 3000 | Dashboards |
| JupyterLab | 8888 | Notebooks |
| OpenWebUI | 8090 | End-user chat UI |
| Hermes | 8642 | Operator agent (OpenAI-compatible API) |
| Hermes dashboard | 9119 | Agent run dashboard |
| LiteLLM | 4000 | Model gateway |
| warehouse_db (Postgres) | 5432 | Main warehouse |

All containers share the `ndsnet` bridge network and reach each other by service name.

---

## Data flow

```
Sources (APIs, files, crawlers)
        │
        ▼
    MinIO (raw landing zone)
    s3://dwhfilesystem/ , s3://landing_area/
        │
        ▼  dlt (Extract + Load, orchestrated by Mage)
        │
        ▼
  warehouse_db  ──── Postgres 17 + pg_duckdb + pgvector + PostGIS
  (raw schema)       port 5432  db=warehouse
        │
        ▼  dbt (Transform, orchestrated by Mage)
        │
        ├── raw schema    (external views over MinIO parquet via pg_duckdb)
        ├── stg schema    (bronze — cleaned, typed, validated tables)
        ├── bdh schema    (silver — conformed, surrogate keys)
        └── adl schema    (gold — aggregated KPIs and summaries)
              │
              ├── Metabase (port 3000)   BI dashboards
              ├── JupyterLab (port 8888) ad-hoc analysis
              └── OpenWebUI (port 8090)  end-user chat / RAG

MinIO files (Delta / Iceberg format)
        │
        ├── Trino delta catalog  ←  Hive Metastore (thrift://hive-metastore:9083)
        └── Trino iceberg catalog ←  JDBC catalog stored in warehouse_db
```

---

## Agent operations layer

End users operate the platform through chat:

```
End user -> OpenWebUI (8090) -> Hermes (8642) -> LiteLLM (4000) -> LLM providers
                                   |
                                   +-- platform tasks -> claude-ops -p "<task>"
                                          headless Claude Code, cwd application/
                                          loads application/CLAUDE.md + .claude/
                                          drives the stack via the Docker socket
```

- `application/CLAUDE.md` + `application/.claude/` are a **separate Claude Code
  environment** for the operator agent — distinct from the repo-root `.claude/`
  and `.gemini/` developer environments. Do not conflate them.
- `litellm` and `open-webui` use the existing `warehouse_db` (databases
  `litellm` and `openwebui`, created by `init/04-agent-dbs.sh`).
- The operator agent may run dbt/Mage/MinIO/warehouse tasks and restart
  services; it does not edit `docker-compose.yml`/`.env` or re-create containers.

---

## Databases inside warehouse_db (Postgres 17)

| Database | Purpose |
|---|---|
| `warehouse` | Main data warehouse — all dlt/dbt schemas (raw, stg, bdh, adl). Also stores Iceberg JDBC catalog metadata and Metabase app metadata. |
| `anythingllm` | AnythingLLM app DB (Prisma) + pgvector embeddings table (`anythingllm_vectors`) (legacy — service removed; DB retained) |
| `metastore` | Hive Metastore schema (used by hive-metastore service for Delta Lake table metadata) |
| `openwebui` | OpenWebUI app DB + pgvector RAG store |
| `litellm` | LiteLLM gateway app DB |

Extensions enabled in `warehouse` database: `pg_duckdb`, `vector`, `postgis`, `postgis_raster`, DuckDB `spatial` extension.

---

## warehouse_db Dockerfile & init

**`application/warehouse_db/Dockerfile`** — extends `pgduckdb/pgduckdb:17-main`, adds `postgresql-17-pgvector` and `postgresql-17-postgis-3`.

**Init scripts** (run once on first boot, in order):
- `01-extensions.sql` — creates all extensions + DuckDB spatial in `warehouse`
- `02-anythingllm-db.sh` — creates `anythingllm` database + installs `vector` in it
- `03-extra-dbs.sh` — creates `metastore` and `metabase` databases idempotently

---

## Environment variables (`.env`)

Key groups — do not hardcode these in config files:

| Var | Used by |
|---|---|
| `MINIO_ADMIN` / `MINIO_PWD` / `MINIO_URL` | MinIO, Mage, Hive Metastore, Trino |
| `WAREHOUSE_DB_USER` / `_PASS` / `_DBNAME` | warehouse_db, Hive Metastore, Metabase, Trino catalogs |
| `MB_DB_DBNAME` | Metabase (stored in `warehouse` db) |

---

## Mage.ai project layout

All Mage code lives under `application/mage_ai/mds_demo/`:

```
mds_demo/
├── pipelines/          # Mage pipelines (see list below)
├── data_loaders/       # dlt source blocks
├── data_exporters/     # output blocks
├── transformers/       # Python transform blocks
├── dbt/
│   └── data_warehouse/ # dbt project (profiles.yml → warehouse_db:5432/warehouse)
│       └── models/
│           ├── raw/    # +materialized: view  (external over MinIO via pg_duckdb)
│           ├── stg/    # +materialized: table (bronze)
│           ├── bdh/    # +materialized: table (silver)
│           └── adl/    # +materialized: table (gold)
├── io_config.yaml      # connection profiles (MinIO as AWS S3, Postgres, Trino, etc.)
├── metadata.yaml       # Mage project settings (spark_config still present but Spark removed)
└── jars/               # Hadoop-AWS, Delta Spark, AWS SDK jars (legacy, Spark removed)
```

**Important:** `metadata.yaml` still references `spark://spark-master:7077` — Spark is no longer running. Do not add pipelines that depend on the Spark config. The jars in `mds_demo/jars/` are unused; clean them up before a production deploy.

### Active pipelines

| Pipeline | Pattern |
|---|---|
| `data_ingestion_pipeline` | Generic dlt MinIO → raw schema loader |
| `balance_sheet_pipeline` | dlt load + dbt transform (finance) |
| `income_statement_pipeline` | dlt load + dbt transform (finance) |
| `finance_master_pipeline` | Master finance pipeline |
| `nyc_taxi_ingestion` | S3 → staging tables |
| `vn_weather_historical` | OpenMeteo API → Postgres |
| `vn_retailer_modern_trade_crawling` | Crawler → MinIO → Postgres |
| `loading_meta_popuplation_high_density_maps` | Geo/population data |
| `write_delta_lake_raw_area` | Writes Delta Lake tables to MinIO |
| `dbt_model` | Runs dbt deps + compile + run |
| `dbt_metabase_sync` | Syncs dbt metadata to Metabase |

Scaffolded/example (not production): `cheerful_frog`, `cosmic_waterfall`, `example_pipeline`, `radiant_river`, `silent_frost`, `sincere_sea`, `test_data`, `wise_moon`.

---

## dbt data model layers

| Schema | Materialization | Pattern |
|---|---|---|
| `raw` | view | External views over MinIO parquet using `pg_duckdb`. No data copy. |
| `stg` | table | Cleaned, typed, null-filtered tables. One model per source entity. |
| `bdh` | table | Business hub — conformed dimension/fact tables with surrogate keys. |
| `adl` | table | Analytics / gold layer — aggregated KPIs, daily summaries, zone stats. |

dbt connects to `warehouse_db:5432` (db=`warehouse`) via `profiles.yml`. The `on-run-start` hook calls `setup_minio_secret()` macro which configures pg_duckdb's S3 credentials for the `raw` external views.

Current domains: `nyc_taxi`, `vn_gis`, `vn_weather`.

---

## Trino catalogs

| Catalog name | Connector | Backend |
|---|---|---|
| `postgresql` | PostgreSQL | Direct JDBC → `warehouse_db:5432/warehouse` |
| `iceberg` | Iceberg (JDBC catalog) | Metadata in `warehouse_db/warehouse`, files on MinIO `s3://dwhfilesystem/` |
| `delta` | Delta Lake | Metadata via Hive Metastore (`thrift://hive-metastore:9083`), files on MinIO `s3://dwhfilesystem/` |

Trino runs as a single coordinator (no separate workers). Max heap: 2 GB. Config at `application/trino/etc/`, catalogs at `application/trino/catalog/`.

---

## Storage layout (MinIO buckets via `./storage/`)

```
storage/
├── dwhfilesystem/      # Primary data lake — Delta + Iceberg tables, parquet files
│   └── landing_area/   # Raw file drops (GeoJSON, CSV, Excel from crawlers)
├── export/             # Export bucket
├── hive/               # Hive warehouse directory (legacy Delta tables)
└── mage/               # Mage pipeline variable storage
```

---

## Key patterns and conventions

1. **Single warehouse, multiple engines.** All analytical tools (Metabase, JupyterLab, OpenWebUI) connect to `warehouse_db:5432/warehouse`. pg_duckdb adds columnar performance without a second warehouse.
2. **dlt for EL, dbt for T.** dlt loads raw data into the `raw` schema; dbt transforms upward through `stg → bdh → adl`. Never write transformation logic in dlt blocks.
3. **MinIO is S3.** All S3 references use `http://minio:9000` inside the network. Bucket name for the data lake is `dwhfilesystem`.
4. **Hive Metastore is for Delta Lake only.** Iceberg uses the JDBC catalog (no Hive needed). Do not use Hive Metastore for Iceberg tables.
5. **No Spark.** Spark was removed. Do not add Spark-dependent pipelines or reference `spark://spark-master:7077`. The metadata.yaml entry is stale.
6. **warehouse_db hosts multiple databases.** `warehouse` (data), `anythingllm` (legacy, retained), `metastore` (Hive), `metabase` (Metabase app), `openwebui` (OpenWebUI), `litellm` (LiteLLM). All created by init scripts at first boot.
7. **Credentials live in `.env` only.** Trino catalog `.properties` files have credentials hardcoded (Trino doesn't support env var substitution). These must stay in sync with `.env` manually.

---

## Things to be aware of before making changes

- **Trino catalog credentials are duplicated.** `application/trino/catalog/*.properties` hardcode `warehouse`/`warehouse` and `admin`/`admin123`. If you change `.env` credentials, update the catalog files too.
- **`metadata.yaml` has stale Spark config.** Safe to leave as-is since Spark is not running, but don't rely on those settings.
- **`03-extra-dbs.sh` creates `metabase` db** — Metabase now uses `warehouse_db` as its backing store (not a separate `metabase_db` container).
