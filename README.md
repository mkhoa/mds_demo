# Modern Data Stack Demo

A self-contained, Docker Compose–based reference implementation of a modern data platform built around an **ELT** workflow. Files land in **MinIO**, **dlt** loads them into a **PostgreSQL** warehouse (with the **pg_duckdb** extension for fast analytical queries), **dbt** models the data in place, and **Mage.ai** orchestrates the whole flow. **Metabase** powers BI, **JupyterLab** powers data science, and **AnythingLLM** adds a local LLM/RAG layer on top of the warehouse.

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Object storage | **MinIO** | S3-compatible store for raw extracts (CSV / Excel / files dropped by sources) |
| Data warehouse | **PostgreSQL + pg_duckdb** | Single source of truth — Postgres for transactional storage, DuckDB extension for fast OLAP queries on the same data |
| Extract & Load | **dlt** (data load tool) | Python-native EL — pulls files from MinIO / APIs into Postgres with schema evolution |
| Transformation | **dbt** (`data_warehouse` project) | SQL-based modelling on top of Postgres, run from inside Mage |
| Orchestration | **Mage.ai** | Pipelines, scheduling, UI; also hosts the dbt project |
| Data Visualization | **Metabase** (+ Postgres) | Dashboards and SQL questions against the warehouse |
| Data Science | **JupyterLab** | Notebooks for ad-hoc analysis, modelling, and experimentation |
| LLM / RAG | **AnythingLLM** | Local LLM workspace — chat over documents and warehouse data |

## Architecture

```
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────────────┐
   │   Sources    │      │    MinIO     │      │     dlt      │      │  PostgreSQL          │
   │ APIs / files │ ───▶ │ raw CSV /    │ ───▶ │  extract +   │ ───▶ │  + pg_duckdb         │
   │   crawlers   │      │  Excel /     │      │     load     │      │  raw / stg / marts   │
   └──────────────┘      │   files      │      └──────────────┘      └─────────┬────────────┘
                         └──────────────┘                                      │
                                                                               │
                          ┌──────────────────┬───────────────────┬─────────────┼────────────────┐
                          ▼                  ▼                   ▼             ▼                ▼
                    ┌──────────┐      ┌──────────────┐    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │   dbt    │      │  JupyterLab  │    │   Metabase   │  │ AnythingLLM  │  │   ad-hoc     │
                    │ models   │      │ (data sci.)  │    │ (dashboards) │  │  (chat/RAG)  │  │     SQL      │
                    └──────────┘      └──────────────┘    └──────────────┘  └──────────────┘  └──────────────┘
                          ▲
                          │ orchestration / scheduling
                          │
                    ┌──────────┐
                    │ Mage.ai  │
                    └──────────┘
```

Mage triggers dlt extract/load jobs (reading raw files from MinIO) and dbt builds. Everything queryable lives in one Postgres instance — `pg_duckdb` lets analytical workloads (Metabase, notebooks, AnythingLLM) hit the same tables with DuckDB-class performance.

## Repository layout

```
.
├── docker-compose.yml          # All services and networking
├── .env / env.dev              # Credentials and project config
├── application/
│   ├── mage_ai/                # Mage project, pipelines, dlt sources, dbt models
│   │   └── mds_demo/
│   │       ├── pipelines/      # Mage pipelines (balance sheet, income statement,
│   │       │                   #   finance master, ingestion, crawlers, ...)
│   │       └── dbt/data_warehouse/   # dbt project (profiles, models)
│   ├── jupyterlab/             # Custom Jupyter image + notebooks
│   ├── metabase/               # Metabase plugins / data
│   └── anythingllm/            # AnythingLLM workspace + storage
├── storage/                    # MinIO data volume (raw extracts, exports)
└── database/                   # Postgres volumes (warehouse + metabase)
```

## Active services and ports

| Service | URL / Port | Notes |
|---|---|---|
| Mage.ai | http://localhost:6789 | Orchestration UI; project = `${PROJECT_NAME}` |
| MinIO API | http://localhost:9000 | S3 endpoint for raw file landings |
| MinIO Console | http://localhost:9001 | Login with `MINIO_ADMIN` / `MINIO_PWD` |
| JupyterLab | http://localhost:8888 | Data science notebooks |
| Metabase | http://localhost:3000 | Dashboards; backed by `metabase_db` Postgres |
| AnythingLLM | http://localhost:3001 | Local LLM / RAG workspace |
| Warehouse Postgres | localhost:5432 | Postgres + `pg_duckdb` — connect dbt, Metabase, notebooks, AnythingLLM here |
| Metabase Postgres | localhost:5433 | Metabase's own metadata DB |

All containers share the `ndsnet` bridge network, so within Compose they reach each other by service name (e.g. `minio:9000`, `warehouse_db:5432`).

## Getting started

1. Copy / edit credentials in `.env` (defaults: `admin` / `admin123`).
2. Bring up the stack:
   ```bash
   docker compose up -d
   ```
3. Open MinIO at http://localhost:9001 and create the bucket(s) used as raw landing zones (e.g. `raw`, `exports`).
4. Open Mage at http://localhost:6789 to browse pipelines under `application/mage_ai/mds_demo/pipelines/`.
5. Run a dlt-based ingestion pipeline from Mage to pull files from MinIO into Postgres (`raw` schema by default).
6. Run dbt models from within Mage — the project lives at `application/mage_ai/mds_demo/dbt/data_warehouse` and materialises `staging` and `marts` schemas in the same Postgres instance.
7. Connect Metabase (http://localhost:3000), JupyterLab (http://localhost:8888), and AnythingLLM (http://localhost:3001) to the warehouse Postgres for visualization, data science, and LLM-powered Q&A respectively.

## Why pg_duckdb?

Postgres handles ingestion, updates, and small lookups well, but analytical scans over millions of rows are slow. The [`pg_duckdb`](https://github.com/duckdb/pg_duckdb) extension embeds DuckDB inside Postgres, so the same tables can be queried with a vectorised columnar engine — no second warehouse to maintain, and Metabase / dbt / notebooks all keep using a single Postgres connection.

## Pipelines included

The Mage project ships with several example/finance-oriented pipelines, including:

- `data_ingestion_pipeline` – generic dlt-based loader from MinIO into the `raw` schema
- `balance_sheet_pipeline`, `income_statement_pipeline`, `finance_master_pipeline` – financial datasets ingested via dlt and modelled in dbt
- `vn_retailer_modern_trade_crawling` – Vietnam retailer crawler (lands files in MinIO, then loads to Postgres)
- `loading_meta_popuplation_high_density_maps` – population/geo data load
- Plus several scaffolded pipelines (`cheerful_frog`, `silent_frost`, `wise_moon`, …)

## Stopping

```bash
docker compose down          # stop containers
docker compose down -v       # also remove anonymous volumes (data in ./database and ./storage persists)
```
