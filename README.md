# Modern Data Stack Demo

A self-contained, Docker Compose–based reference implementation of a modern data platform built around an **ELT** workflow. Files land in **MinIO**, **dlt** loads them into a **PostgreSQL** warehouse (with the **pg_duckdb** extension for fast analytical queries), **dbt** models the data in place, and **Mage.ai** orchestrates the whole flow. **Metabase** powers BI and **JupyterLab** powers data science. On top sits an **agent operations layer** — **OpenWebUI**, **Hermes**, and **LiteLLM** — that lets end users operate and maintain the platform through chat: Hermes delegates data-platform tasks to a headless **Claude Code** agent.

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Object storage | **MinIO** | S3-compatible store for raw extracts (CSV / Excel / files dropped by sources) |
| Data warehouse | **PostgreSQL + pg_duckdb** | Single source of truth — Postgres for transactional storage, DuckDB extension for fast OLAP queries on the same data |
| Extract & Load | **dlt** (data load tool) | Python-native EL — pulls files from MinIO / APIs into Postgres with schema evolution |
| Transformation | **dbt** (`data_warehouse` project) | SQL-based modelling on top of Postgres, run from inside Mage |
| Orchestration | **Mage.ai** | Pipelines, scheduling, UI; also hosts the dbt project |
| Federated query | **Trino** | SQL across Postgres, Iceberg, and Delta Lake catalogs |
| Data Visualization | **Metabase** | Dashboards and SQL questions against the warehouse |
| Data Science | **JupyterLab** | Notebooks for ad-hoc analysis, modelling, and experimentation |
| End-user chat | **OpenWebUI** | Chat front door — end users operate the platform here |
| Operator agent | **Hermes** | Agent that fields chat requests and delegates platform tasks to Claude Code |
| Model gateway | **LiteLLM** | Routes model calls to Gemini / OpenRouter / Anthropic |
| Agent cache | **Redis** | Session / cache store for OpenWebUI |

## Architecture

### Data flow

```
   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────────────┐
   │   Sources    │      │    MinIO     │      │     dlt      │      │  PostgreSQL          │
   │ APIs / files │ ───▶ │ raw CSV /    │ ───▶ │  extract +   │ ───▶ │  + pg_duckdb         │
   │   crawlers   │      │  Excel /     │      │     load     │      │  raw/stg/bdh/adl     │
   └──────────────┘      │   files      │      └──────────────┘      └─────────┬────────────┘
                         └──────────────┘                                      │
                                              ┌──────────────┬──────────────────┼──────────────┐
                                              ▼              ▼                  ▼              ▼
                                        ┌──────────┐  ┌──────────────┐   ┌──────────────┐  ┌──────────┐
                                        │   dbt    │  │  JupyterLab  │   │   Metabase   │  │ ad-hoc   │
                                        │ models   │  │ (data sci.)  │   │ (dashboards) │  │   SQL    │
                                        └──────────┘  └──────────────┘   └──────────────┘  └──────────┘
                                              ▲
                                              │ orchestration / scheduling
                                        ┌──────────┐
                                        │ Mage.ai  │
                                        └──────────┘
```

Mage triggers dlt extract/load jobs (reading raw files from MinIO) and dbt builds. Everything queryable lives in one Postgres instance — `pg_duckdb` lets analytical workloads (Metabase, notebooks) hit the same tables with DuckDB-class performance.

### Agent operations layer

```
   End user
      │  chat
      ▼
 ┌──────────┐      ┌──────────┐      ┌──────────┐
 │ OpenWebUI│ ───▶ │  Hermes  │ ───▶ │ LiteLLM  │ ──▶ Gemini / OpenRouter / Anthropic
 │  :8090   │      │  :8642   │      │  :4000   │
 └──────────┘      └────┬─────┘      └──────────┘
                        │ data-platform tasks
                        ▼
                  claude-ops ──▶ headless Claude Code (the operator)
                        │          loads application/CLAUDE.md + application/.claude/
                        ▼
                  dbt · Mage · warehouse · MinIO · service lifecycle
```

End users chat in **OpenWebUI**. **Hermes** answers directly for general questions; for anything that touches the data platform it runs `claude-ops`, which launches a headless **Claude Code** agent rooted at `application/`. That agent loads a dedicated operator environment (`application/CLAUDE.md` + `application/.claude/` — skills, commands, subagents, runbooks) and drives the stack through the mounted Docker socket: running dbt, triggering Mage pipelines, querying the warehouse, managing MinIO, and restarting services. **LiteLLM** is the shared model gateway. See [Agent operations layer](#agent-operations-layer-usage) below for usage.

## Repository layout

```
.
├── docker-compose.yml          # All services and networking
├── .env / env.dev              # Credentials and project config
├── CLAUDE.md                   # Platform overview for developers
├── application/
│   ├── mage_ai/                # Mage project: pipelines, dlt sources, dbt models
│   │   └── mds_demo/
│   │       ├── pipelines/      # Mage pipelines (ingestion, finance, crawlers, ...)
│   │       └── dbt/data_warehouse/   # dbt project — raw → stg → bdh → adl
│   ├── warehouse_db/           # Custom Postgres image (pg_duckdb + pgvector + PostGIS) + init scripts
│   ├── jupyterlab/             # Custom Jupyter image + notebooks
│   ├── metabase/               # Metabase data
│   ├── trino/                  # Trino coordinator config + catalogs
│   ├── litellm/                # LiteLLM gateway config
│   ├── hermes/                 # Hermes operator image (Claude Code + Docker CLI), claude-ops, SOUL.md
│   ├── CLAUDE.md               # Operator agent charter — a separate Claude Code environment
│   └── .claude/                # Operator skills, commands, subagents, knowledge base
├── storage/                    # MinIO data volume (raw extracts, exports)
├── database/                   # Postgres data volume (warehouse)
└── data/                       # Agent-layer runtime data (open-webui, hermes, redis)
```

## Active services and ports

| Service | URL / Port | Notes |
|---|---|---|
| Mage.ai | http://localhost:6789 | Orchestration UI; project = `${PROJECT_NAME}` |
| MinIO API | http://localhost:9000 | S3 endpoint for raw file landings |
| MinIO Console | http://localhost:9001 | Login with `MINIO_ADMIN` / `MINIO_PWD` |
| Trino | http://localhost:8080 | Federated SQL coordinator |
| JupyterLab | http://localhost:8888 | Data science notebooks |
| Metabase | http://localhost:3000 | Dashboards; metadata stored in `warehouse_db` |
| OpenWebUI | http://localhost:8090 | End-user chat front door |
| Hermes | http://localhost:8642 | Operator agent (OpenAI-compatible API) |
| Hermes dashboard | http://localhost:9119 | Agent run dashboard |
| LiteLLM | http://localhost:4000 | Model gateway |
| Warehouse Postgres | localhost:5432 | Postgres + `pg_duckdb` — dbt, Metabase, notebooks, and the agent layer all connect here |

All containers share the `ndsnet` bridge network, so within Compose they reach each other by service name (e.g. `minio:9000`, `warehouse_db:5432`, `litellm:4000`).

## Getting started

1. Copy / edit credentials in `.env` (defaults: `admin` / `admin123`). Set `GEMINI_API_KEY`, `LITELLM_MASTER_KEY`, and `WEBUI_SECRET_KEY` for the agent layer.
2. Bring up the stack:
   ```bash
   docker compose up -d
   ```
3. Open MinIO at http://localhost:9001 and create the bucket(s) used as raw landing zones (e.g. `dwhfilesystem`).
4. Open Mage at http://localhost:6789 to browse pipelines under `application/mage_ai/mds_demo/pipelines/`.
5. Run a dlt-based ingestion pipeline from Mage to pull files from MinIO into Postgres (`raw` schema by default).
6. Run dbt models from within Mage — the project lives at `application/mage_ai/mds_demo/dbt/data_warehouse` and materialises the `stg`, `bdh`, and `adl` schemas in the same Postgres instance.
7. Connect Metabase (http://localhost:3000) and JupyterLab (http://localhost:8888) to the warehouse Postgres for visualization and data science.
8. Open OpenWebUI at http://localhost:8090, create the first account, and chat with the platform — see below.

## Agent operations layer (usage)

End users operate the platform by chatting in **OpenWebUI**. Hermes decides whether to answer directly or delegate to the Claude Code operator.

**One-time setup** — the operator's Claude Code needs a credential. Either complete an interactive login:

```bash
docker compose exec hermes sh -lc 'CLAUDE_CONFIG_DIR=/opt/data/claude claude'   # then run /login
```

or drop an existing `~/.claude` config into `data/hermes/claude/`.

**How it works:**

- The operator environment is `application/CLAUDE.md` + `application/.claude/` — a **separate Claude Code environment**, distinct from the repo-root developer setup. Hermes mounts the whole `application/` folder and runs the operator rooted there.
- The operator is bounded: it runs dbt / Mage / warehouse / MinIO tasks and can restart services, but does not edit `docker-compose.yml` / `.env` or re-create containers.
- To change Hermes' behaviour, edit `application/hermes/SOUL.md` and rebuild: `docker compose build hermes && docker compose up -d hermes`.

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
