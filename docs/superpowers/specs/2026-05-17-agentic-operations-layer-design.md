# Agentic Operations Layer — Design

**Date:** 2026-05-17
**Status:** Approved for planning
**Author:** mkhoa + Claude

## Goal

Add an agentic operations layer to the MDS Demo platform so that end users can
operate and maintain the open-source data stack through a chat interface. The
end state: a user asks a question or issues a task in **OpenWebUI**, the
**Hermes** agent decides how to handle it, and any data-platform task is
delegated to **Claude Code** running headless with a purpose-built set of
operator skills.

Reference setups:
- `~/Development/llm-webui-app` — existing OpenWebUI / Hermes / LiteLLM stack.
- `https://github.com/ai-analyst-lab/ai-analyst` — the `.claude/` organization
  pattern (always-on skills + command-triggered skills + agents + knowledge base).

## Scope decisions (confirmed)

| Decision | Choice |
|---|---|
| Operator agent control | Data ops **+ service lifecycle** (restart / logs / health). Editing `docker-compose.yml` / `.env` is out of scope. |
| AnythingLLM | **Removed.** OpenWebUI replaces it as the end-user chat + RAG front door. |
| Claude Code auth | **Claude subscription via LiteLLM** — Claude Code routes through LiteLLM's `anthropic-claude-*` models using forwarded subscription OAuth. |
| Compose integration | **Single `docker-compose.yml`** — new services join `ndsnet`; `open-webui` / `litellm` app DBs live on the existing `warehouse_db`. |

## Architecture

### New services (added to `docker-compose.yml`, all on `ndsnet`)

| Service | Image / Build | Port | Role |
|---|---|---|---|
| `open-webui` | `ghcr.io/open-webui/open-webui:main` | 8090 | End-user chat UI + RAG |
| `redis` | `redis/redis-stack:latest` | internal | OpenWebUI cache / session store |
| `litellm` | `ghcr.io/berriai/litellm:main-stable` | 4000 | Model gateway (Gemini / OpenRouter / Anthropic) |
| `hermes` | build `application/hermes/` | 8642 | Operator agent (NousResearch Hermes) |
| `hermes-dashboard` | `nousresearch/hermes-agent:latest` | 9119 | Agent run dashboard |

### Request flow

```
End user
   │
   ▼
OpenWebUI (8090) ──── chat UI, RAG (pgvector in warehouse_db / openwebui DB)
   │  OpenAI-compatible API  → OPENAI_API_BASE_URL=http://hermes:8642/v1
   ▼
Hermes (8642) ──── operator agent
   │   │
   │   └─ for any data-platform task: claude-ops -p "<task>"   (headless Claude Code)
   │            cwd = /workspace/application  → loads application/.claude/
   │            ├─ dbt / Mage / warehouse / MinIO        (data ops)
   │            └─ docker CLI via /var/run/docker.sock   (service lifecycle)
   ▼
LiteLLM (4000) ──── model gateway → Gemini / OpenRouter / Anthropic (subscription OAuth)
```

### The Hermes → Claude Code bridge

- The `hermes` image bakes in the **Claude Code CLI**, the **`docker` CLI**, and
  a wrapper script **`claude-ops`**.
- `claude-ops` runs `claude` headless with:
  - `ANTHROPIC_BASE_URL=http://litellm:4000`
  - `ANTHROPIC_MODEL=anthropic-claude-sonnet`, `ANTHROPIC_SMALL_FAST_MODEL=anthropic-claude-haiku`
  - `ANTHROPIC_CUSTOM_HEADERS` carrying the LiteLLM master key (subscription-OAuth
    forwarding, per the `claude-litellm` pattern in `llm-webui-app`)
  - working directory `/workspace/application`
- Hermes is instructed (via its persona / a Hermes skill) to delegate any
  data-platform request by invoking `claude-ops -p "<task>"`. Claude Code then
  auto-loads `application/.claude/` (CLAUDE.md, skills, commands, agents).
- **Rejected alternative:** running Claude Code as an MCP server consumed by
  Hermes — more moving parts than the headless-subprocess pattern, which
  `llm-webui-app` already proves works.

### Hermes container mounts

| Mount | Mode | Purpose |
|---|---|---|
| `./application:/workspace/application` | rw | Working tree — contains `application/.claude` |
| `/var/run/docker.sock:/var/run/docker.sock` | — | Service lifecycle (restart / logs / stats / health) |
| `./data/hermes:/opt/data` | rw | Hermes agent state |

Service lifecycle uses the plain `docker` CLI against **container names**
(`docker restart magic`, `docker logs trino`, `docker stats`). This avoids the
docker-compose host-path-mapping problem that arises when `docker compose` runs
from inside a container whose project directory does not match the host path.
Container re-creation (`docker compose up`) is intentionally out of scope.

### Database reuse

`open-webui` and `litellm` get their application databases on the existing
`warehouse_db` Postgres instance — consistent with how the `anythingllm`,
`metastore`, and `metabase` databases already live there.

New init script **`application/warehouse_db/init/04-agent-dbs.sh`**:
- Creates database `openwebui` if absent.
- Creates database `litellm` if absent.
- Enables the `vector` extension inside `openwebui` (OpenWebUI pgvector RAG).

Idempotent, mirroring the existing `03-extra-dbs.sh`. Note: init scripts only
run on **first boot** of `warehouse_db`; for an existing volume the two
databases must be created manually once (documented in the plan).

## `application/.claude/` — operator configuration

Organized on the ai-analyst pattern, adapted from "generate slide decks" to
"operate a data platform". This is a **separate, narrower project scope** from
the repo-root `mds_demo/.claude` (the human dev environment) — the two do not
interfere.

```
application/.claude/
├── CLAUDE.md                  # operator charter: stack map, service names/ports,
│                              #   safety boundaries, which skill for which task
├── settings.json              # enables dbt plugin; permissions allowlist
│                              #   (docker, dbt, mc, psql)
├── skills/
│   ├── platform-orientation/      # always-on: the ndsnet map, how services connect
│   ├── operating-services/        # docker ps / logs / restart / stats + health sweeps
│   ├── running-dbt-models/        # deps / compile / run / test / show in data_warehouse
│   ├── managing-mage-pipelines/   # build blocks + trigger pipelines
│   ├── querying-the-warehouse/    # psql / pg_duckdb against warehouse_db
│   └── managing-minio-storage/    # mc bucket / object operations
├── commands/
│   ├── health-check.md            # /health-check — sweep every service
│   ├── run-dbt.md                 # /run-dbt <selector>
│   ├── run-pipeline.md            # /run-pipeline <name>
│   └── restart-service.md         # /restart-service <name>
├── agents/
│   ├── pipeline-operator.md       # runs an EL+T pipeline end to end
│   └── incident-investigator.md   # diagnoses a failing / unhealthy service
└── knowledge/
    ├── runbooks.md                # common incident → fix runbooks
    └── corrections.md             # learned gotchas, appended over time
```

### Skill content sources

- `managing-mage-pipelines` — ported from the existing
  `.gemini/skills/mage-ai-blocks` skill (project layout rules, block templates).
- `running-dbt-models` — stays thin; the `dbt` plugin (already enabled) provides
  the dbt mechanics. This skill only carries project-specific selectors, paths
  (`dbt/data_warehouse`), and the schema-layer conventions (`raw → stg → bdh → adl`).
- The other skills are written fresh against the stack described in the root
  `CLAUDE.md`.

### Scope guardrails

The operator `CLAUDE.md` and `settings.json` explicitly bound the agent:
- **Allowed:** run dbt, build/trigger Mage pipelines, query the warehouse,
  manage MinIO objects, and `restart` / `logs` / `stats` / health-check containers.
- **Out of scope:** editing `docker-compose.yml` or `.env`, re-creating
  containers, destructive data deletion. Matches the "data ops + lifecycle"
  decision, not "full maintenance".

## Migration & cleanup

| Item | Action |
|---|---|
| `anything-llm` service | Removed from `docker-compose.yml` (frees port 3001) |
| AnythingLLM `.env` block | Removed |
| `anythingllm` database + `data/anythingllm/` | **Left in place** — no destructive deletion; flagged as safe-to-remove later |
| `.env` | Add `LITELLM_MASTER_KEY`, `OPENROUTER_API_KEY`, OpenWebUI/LiteLLM settings; reuse existing `GEMINI_API_KEY` |
| Telegram wiring for Hermes | Out of scope for this change (can be added later) |
| Root `CLAUDE.md` | Update stack table, ports table, data-flow diagram; remove AnythingLLM; add an operator-agent section |

## Files created / modified

**Created:**
- `application/litellm/config.yaml` (mounted into the official LiteLLM image; no custom Dockerfile)
- `application/hermes/Dockerfile`, `application/hermes/init.sh`, `application/hermes/claude-ops`
- `application/warehouse_db/init/04-agent-dbs.sh`
- `application/.claude/CLAUDE.md`, `application/.claude/settings.json`
- `application/.claude/skills/*` (6 skills)
- `application/.claude/commands/*` (4 commands)
- `application/.claude/agents/*` (2 agents)
- `application/.claude/knowledge/*` (2 files)

**Modified:**
- `docker-compose.yml` — add 5 services, remove `anything-llm`
- `.env` — add agent-layer variables, remove AnythingLLM block
- `CLAUDE.md` (root) — documentation updates

## Ports after this change

| Service | Port | Change |
|---|---|---|
| OpenWebUI | 8090 | new |
| LiteLLM | 4000 | new |
| Hermes | 8642 | new |
| Hermes dashboard | 9119 | new |
| AnythingLLM | ~~3001~~ | removed |

All other ports unchanged.

## Testing / verification

1. `docker compose config` parses cleanly.
2. `docker compose up -d` brings all services healthy; `warehouse_db` shows
   `openwebui` and `litellm` databases.
3. LiteLLM `/health` responds; a test completion routes to Gemini.
4. From the `hermes` container, `claude-ops -p "list the running services"`
   returns output (proves the bridge + LiteLLM + Claude Code auth).
5. `/health-check` command sweeps every service and reports status.
6. `/run-dbt <selector>` runs a dbt model and reports the result.
7. End-to-end: a prompt in OpenWebUI ("restart the trino service") reaches
   Hermes, delegates to `claude-ops`, and the container restarts.

## Open considerations (non-blocking)

- The host-path-mapping caveat means container re-creation is deliberately
  excluded; if "full maintenance" is wanted later, mount the repo at its host
  path inside Hermes.
- Secrets currently live in `.env`; LiteLLM and Hermes read them via
  `env_file`. No secret manager is introduced in this change.
