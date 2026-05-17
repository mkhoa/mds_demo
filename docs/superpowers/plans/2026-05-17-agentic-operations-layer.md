# Agentic Operations Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an OpenWebUI + Hermes + LiteLLM layer to the MDS Demo stack so end users can operate the data platform through chat, with Hermes delegating platform tasks to a headless Claude Code operator environment.

**Architecture:** Five new Docker Compose services join the existing `ndsnet`. End users chat in OpenWebUI → Hermes (operator agent) → LiteLLM (model gateway). For data-platform tasks Hermes runs `claude-ops -p "<task>"`, a headless Claude Code rooted at `application/` that loads `application/CLAUDE.md` + `application/.claude/` operator skills and drives the stack via the mounted Docker socket. OpenWebUI and LiteLLM reuse the existing `warehouse_db` Postgres for their app databases. AnythingLLM is removed.

**Tech Stack:** Docker Compose, OpenWebUI, NousResearch Hermes, LiteLLM, Claude Code CLI, PostgreSQL 17 (pgvector), Redis.

**Spec:** `docs/superpowers/specs/2026-05-17-agentic-operations-layer-design.md`

**Refinements adopted during planning** (improvements on the spec, no scope change):
- Lifecycle uses `docker compose -p mds_demo -f /workspace/docker-compose.yml <sub>` (restart/logs/ps/exec). These subcommands address services by name and do **not** hit the host-path-mapping problem — only `up`/`build`/`create` do, and those stay out of scope. Hermes therefore also mounts `docker-compose.yml` and `.env` read-only.
- The operator environment is plugin-free: `running-dbt-models` is a self-contained skill rather than depending on a marketplace dbt plugin being installed inside the container.

---

## File Structure

**Created:**
- `application/litellm/config.yaml` — LiteLLM model gateway config (mounted into the official image)
- `application/hermes/Dockerfile` — Hermes image + Claude Code CLI + Docker CLI/Compose
- `application/hermes/init.sh` — Hermes entrypoint (socket perms, config patch, SOUL install)
- `application/hermes/claude-ops` — wrapper that runs headless Claude Code as the operator
- `application/hermes/SOUL.md` — Hermes persona: delegate platform tasks via `claude-ops`
- `application/warehouse_db/init/04-agent-dbs.sh` — creates `openwebui` + `litellm` databases
- `application/CLAUDE.md` — operator charter (project root of the operator environment)
- `application/.claude/settings.json` — operator permissions
- `application/.claude/skills/{platform-orientation,operating-services,running-dbt-models,managing-mage-pipelines,querying-the-warehouse,managing-minio-storage}/SKILL.md`
- `application/.claude/commands/{health-check,run-dbt,run-pipeline,restart-service}.md`
- `application/.claude/agents/{pipeline-operator,incident-investigator}.md`
- `application/.claude/knowledge/{runbooks,corrections}.md`

**Modified:**
- `docker-compose.yml` — add `litellm`, `redis`, `hermes`, `hermes-dashboard`, `open-webui`; remove `anything-llm`
- `.env` — add agent-layer variables; remove AnythingLLM-only variables (keep `GEMINI_API_KEY`)
- `.gitignore` — ignore the new `data/` runtime directories
- `CLAUDE.md` (root) — document the agent layer; remove AnythingLLM

---

## Task 1: Environment variables

**Files:**
- Modify: `.env`
- Modify: `.gitignore`

- [ ] **Step 1: Read `.env` to see current contents**

Run: `cat .env`
Note the `# AnythingLLM` block. `GEMINI_API_KEY` lives inside it and **must be preserved** — LiteLLM reuses it.

- [ ] **Step 2: Remove AnythingLLM-only variables from `.env`**

Delete exactly these lines (by key), keeping their `# AnythingLLM` comment line removed too:
`DATABASE_URL`, `LLM_PROVIDER`, `EMBEDDING_MODEL_PREF`, `GEMINI_LLM_MODEL_PREF`, `EMBEDDING_ENGINE`, `GEMINI_EMBEDDING_API_KEY`, `VECTOR_DB`, `PGVECTOR_CONNECTION_STRING`, `PGVECTOR_TABLE_NAME`, `JWT_SECRET`, `AGENT_GSE_CTX`, `AGENT_GSE_KEY`, `STORAGE_DIR`, `SERVER_PORT`, `SIG_KEY`, `SIG_SALT`.

**Do NOT delete** `GEMINI_API_KEY`, and do NOT touch the `# Postgres Extension Configuration` block (`POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DATABASE`/`POSTGRES_USER`/`POSTGRES_PASSWORD`).

- [ ] **Step 3: Generate two secrets**

Run: `echo "LITELLM_MASTER_KEY=sk-$(openssl rand -hex 24)"; echo "WEBUI_SECRET_KEY=$(openssl rand -hex 32)"`
Copy both output lines for the next step.

- [ ] **Step 4: Append the agent-layer section to `.env`**

Append this block to the end of `.env`, substituting the two generated values and moving the existing `GEMINI_API_KEY` line here:

```
# --- Agent layer (LiteLLM / OpenWebUI / Hermes) ---
GEMINI_API_KEY=<existing value, preserved from the old AnythingLLM block>
LITELLM_MASTER_KEY=<value from Step 3>
WEBUI_SECRET_KEY=<value from Step 3>
OPENROUTER_API_KEY=
```

`OPENROUTER_API_KEY` is intentionally empty — Gemini and Anthropic models work without it; set it later only if OpenRouter models are wanted.

- [ ] **Step 5: Ensure runtime data directories are gitignored**

Append to `.gitignore` if not already present:

```
# Agent layer runtime data
data/open-webui/
data/redis/
data/hermes/
```

- [ ] **Step 6: Verify `.env` has no duplicate keys and `GEMINI_API_KEY` survived**

Run: `grep -c '^GEMINI_API_KEY=' .env`
Expected: `1`

Run: `grep -E '^(LITELLM_MASTER_KEY|WEBUI_SECRET_KEY)=' .env | wc -l`
Expected: `2`

- [ ] **Step 7: Commit**

```bash
git add .env .gitignore
git commit -m "chore: add agent-layer env vars, drop AnythingLLM vars"
```

---

## Task 2: warehouse_db init script for agent databases

**Files:**
- Create: `application/warehouse_db/init/04-agent-dbs.sh`

- [ ] **Step 1: Create the init script**

Create `application/warehouse_db/init/04-agent-dbs.sh`:

```bash
#!/bin/bash
set -e

# Create per-service application databases on the shared warehouse_db instance,
# mirroring 03-extra-dbs.sh. Runs only on first boot of an empty data volume.
for db in openwebui litellm; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1 || \
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "CREATE DATABASE $db;"
done

# OpenWebUI uses pgvector for RAG inside its own database.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname openwebui \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x application/warehouse_db/init/04-agent-dbs.sh`

- [ ] **Step 3: Verify shell syntax**

Run: `bash -n application/warehouse_db/init/04-agent-dbs.sh`
Expected: no output, exit code 0.

- [ ] **Step 4: Commit**

```bash
git add application/warehouse_db/init/04-agent-dbs.sh
git commit -m "feat: add warehouse_db init for openwebui/litellm databases"
```

---

## Task 3: LiteLLM gateway config

**Files:**
- Create: `application/litellm/config.yaml`

- [ ] **Step 1: Create the LiteLLM config**

Create `application/litellm/config.yaml`:

```yaml
model_list:
  # --- Google Gemini (reuses GEMINI_API_KEY from .env) ---
  - model_name: gemini-flash-latest
    litellm_params:
      model: gemini/gemini-flash-latest
      api_key: os.environ/GEMINI_API_KEY
  - model_name: gemini-pro-latest
    litellm_params:
      model: gemini/gemini-pro-latest
      api_key: os.environ/GEMINI_API_KEY
  - model_name: gemini-flash-lite-latest
    litellm_params:
      model: gemini/gemini-flash-lite-latest
      api_key: os.environ/GEMINI_API_KEY

  # --- OpenRouter (optional; works only if OPENROUTER_API_KEY is set) ---
  - model_name: glm-5.1
    litellm_params:
      model: openrouter/z-ai/glm-5.1
      api_key: os.environ/OPENROUTER_API_KEY
  - model_name: kimi-k2.6
    litellm_params:
      model: openrouter/moonshotai/kimi-k2.6
      api_key: os.environ/OPENROUTER_API_KEY

  # --- Claude via Claude Code subscription OAuth ---
  # No api_key: the credential is the OAuth token forwarded from the client
  # (see general_settings.forward_client_headers_to_llm_api).
  # https://docs.litellm.ai/docs/tutorials/claude_code_max_subscription
  - model_name: anthropic-claude-sonnet
    litellm_params:
      model: anthropic/claude-sonnet-4-6
  - model_name: anthropic-claude-haiku
    litellm_params:
      model: anthropic/claude-haiku-4-5
  - model_name: anthropic-claude-opus
    litellm_params:
      model: anthropic/claude-opus-4-7

litellm_settings:
  drop_params: true

general_settings:
  master_key: os.environ/LITELLM_MASTER_KEY
  # Forward the client's Authorization header (Claude Code's subscription OAuth
  # token) to Anthropic. Required for the anthropic-claude-* routing above.
  forward_client_headers_to_llm_api: true
```

- [ ] **Step 2: Verify YAML parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('application/litellm/config.yaml'))" && echo OK`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add application/litellm/config.yaml
git commit -m "feat: add LiteLLM gateway config"
```

---

## Task 4: Add `litellm` + `redis` services, remove `anything-llm`

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Remove the `anything-llm` service**

Delete the entire `anything-llm:` service block from `docker-compose.yml` (from the line `  anything-llm:` through its last `      - ndsnet` line, inclusive).

- [ ] **Step 2: Add the `litellm` and `redis` services**

Insert these two services immediately before the top-level `networks:` key in `docker-compose.yml`:

```yaml
  litellm:
    image: ghcr.io/berriai/litellm:main-stable
    container_name: litellm
    command: ["--config", "/app/config.yaml", "--port", "4000"]
    environment:
      LITELLM_MASTER_KEY: ${LITELLM_MASTER_KEY}
      GEMINI_API_KEY: ${GEMINI_API_KEY}
      OPENROUTER_API_KEY: ${OPENROUTER_API_KEY}
      DATABASE_URL: postgresql://${WAREHOUSE_DB_USER}:${WAREHOUSE_DB_PASS}@warehouse_db:5432/litellm
      STORE_MODEL_IN_DB: "True"
    ports:
      - "4000:4000"
    volumes:
      - ./application/litellm/config.yaml:/app/config.yaml:ro
    depends_on:
      warehouse_db:
        condition: service_healthy
    networks:
      - ndsnet
    restart: unless-stopped

  redis:
    image: redis/redis-stack:latest
    container_name: redis
    volumes:
      - ./data/redis:/data
    networks:
      - ndsnet
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "redis-cli ping | grep PONG"]
      interval: 30s
      timeout: 3s
      retries: 5
      start_period: 20s
```

- [ ] **Step 2b: Verify the AnythingLLM block was fully removed**

Run: `grep -c 'anything' docker-compose.yml || true`
Expected: `0`

- [ ] **Step 3: Verify Compose still parses with variables resolved**

Run: `docker compose config -q && echo OK`
Expected: `OK` (no errors, no unresolved-variable warnings).

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add litellm + redis services, remove anything-llm"
```

---

## Task 5: Hermes operator image

**Files:**
- Create: `application/hermes/Dockerfile`
- Create: `application/hermes/init.sh`
- Create: `application/hermes/claude-ops`
- Create: `application/hermes/SOUL.md`

- [ ] **Step 1: Inspect the Hermes base image**

Run: `docker run --rm nousresearch/hermes-agent:latest sh -lc 'node --version; which curl tar; cat /etc/os-release | head -2'`
Expected: a Node version prints and `curl`/`tar` resolve. If `node` is missing, stop and report — the Dockerfile below assumes Node is present (the Hermes data dir's `npm-global`/`.npm` folders indicate it is). If `curl` is missing, substitute `wget -O` in Step 2.

- [ ] **Step 2: Create the Dockerfile**

Create `application/hermes/Dockerfile`:

```dockerfile
FROM nousresearch/hermes-agent:latest

USER root

# Claude Code CLI — the operator delegate Hermes drives for platform tasks.
RUN npm install -g @anthropic-ai/claude-code && claude --version

# Docker CLI + Compose plugin — distro-agnostic static binaries. Used against
# the host Docker socket mounted at /var/run/docker.sock for service lifecycle.
ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.32.4
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" -o /tmp/docker.tgz \
 && tar -xzf /tmp/docker.tgz -C /tmp \
 && mv /tmp/docker/docker /usr/local/bin/docker \
 && rm -rf /tmp/docker /tmp/docker.tgz \
 && mkdir -p /usr/local/lib/docker/cli-plugins \
 && curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" -o /usr/local/lib/docker/cli-plugins/docker-compose \
 && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
 && docker --version && docker compose version

COPY claude-ops /usr/local/bin/claude-ops
COPY init.sh /usr/local/bin/hermes-init.sh
RUN chmod +x /usr/local/bin/claude-ops /usr/local/bin/hermes-init.sh

CMD ["sh", "/usr/local/bin/hermes-init.sh"]
```

- [ ] **Step 3: Create the `claude-ops` wrapper**

Create `application/hermes/claude-ops`:

```sh
#!/bin/sh
# claude-ops — run headless Claude Code as the MDS Demo platform operator.
# Routes through LiteLLM's anthropic-claude-* models using Claude Code's
# subscription OAuth token (forwarded by LiteLLM to Anthropic).
set -e

if [ -z "${LITELLM_MASTER_KEY}" ]; then
  echo "claude-ops: LITELLM_MASTER_KEY is not set in the container." >&2
  exit 1
fi

export CLAUDE_CONFIG_DIR="${CLAUDE_CONFIG_DIR:-/opt/data/claude}"
export DISABLE_AUTOUPDATER=1
export ANTHROPIC_BASE_URL="http://litellm:4000"
export ANTHROPIC_MODEL="anthropic-claude-sonnet"
export ANTHROPIC_SMALL_FAST_MODEL="anthropic-claude-haiku"
export ANTHROPIC_CUSTOM_HEADERS="x-litellm-api-key: Bearer ${LITELLM_MASTER_KEY}"

# The operator environment is rooted at the mounted application/ folder.
cd /workspace/application
exec claude --permission-mode bypassPermissions "$@"
```

- [ ] **Step 4: Create `init.sh`**

Create `application/hermes/init.sh`:

```sh
#!/bin/sh
set -e

# Make the mounted Docker socket usable for service lifecycle. Best-effort:
# only succeeds when this entrypoint runs as root (the image default).
chmod 666 /var/run/docker.sock 2>/dev/null || true

. /opt/hermes/.venv/bin/activate

# Install the operator persona on first boot (does not overwrite edits).
if [ -f /usr/local/share/hermes/SOUL.md ] && [ ! -s /opt/data/SOUL.md ]; then
  cp /usr/local/share/hermes/SOUL.md /opt/data/SOUL.md
fi

# Point Hermes' own model at LiteLLM (mirrors the llm-webui-app init.sh).
if [ -f /opt/data/config.yaml ]; then
  sed -i \
    -e "s|^  default: \".*\"\$|  default: \"$HERMES_MODEL\"|" \
    -e "s|^  provider: \".*\"\$|  provider: \"$HERMES_PROVIDER\"|" \
    -e "s|^  base_url: \".*\"\$|  base_url: \"$HERMES_BASE_URL\"|" \
    /opt/data/config.yaml
  if grep -q '^  api_key: ' /opt/data/config.yaml; then
    sed -i "s|^  api_key: \".*\"\$|  api_key: \"$HERMES_API_KEY\"|" /opt/data/config.yaml
  else
    sed -i "s|^  # api_key: \"your-key-here\".*\$|  api_key: \"$HERMES_API_KEY\"|" /opt/data/config.yaml
  fi
fi

exec hermes gateway run
```

- [ ] **Step 5: Add the SOUL.md copy into the Dockerfile**

The init script reads `/usr/local/share/hermes/SOUL.md`. Add this line to `application/hermes/Dockerfile`, immediately after the `COPY claude-ops ...` / `COPY init.sh ...` lines and before the `RUN chmod` line:

```dockerfile
RUN mkdir -p /usr/local/share/hermes
COPY SOUL.md /usr/local/share/hermes/SOUL.md
```

- [ ] **Step 6: Create `SOUL.md`**

Create `application/hermes/SOUL.md`:

```markdown
# Hermes — MDS Demo Platform Operator

You are the operations agent for the **MDS Demo** data platform. End users
chat with you through OpenWebUI to inspect and run the platform.

## Delegation rule

For ANY task that touches the data platform — running dbt, building or
triggering Mage pipelines, querying the warehouse, managing MinIO storage,
or checking/restarting services — delegate to the Claude Code operator by
running this terminal command:

    claude-ops -p "<the task, stated clearly and completely>"

Wait for it to finish, then relay its result to the user in plain language.
`claude-ops` runs headless Claude Code rooted at the platform's `application/`
folder, with operator skills for every supported task.

For general questions, casual conversation, or clarifying what the user wants,
answer directly without delegating.

Be concise. Confirm before anything destructive.
```

- [ ] **Step 7: Verify the shell scripts parse**

Run: `sh -n application/hermes/init.sh && sh -n application/hermes/claude-ops && echo OK`
Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add application/hermes/
git commit -m "feat: add Hermes operator image (Claude Code + Docker CLI)"
```

---

## Task 6: Add `hermes` + `hermes-dashboard` services

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the two services**

Insert immediately before the top-level `networks:` key in `docker-compose.yml`:

```yaml
  hermes:
    build:
      context: ./application/hermes
      dockerfile: Dockerfile
    image: hermes-operator:local
    container_name: hermes
    environment:
      HERMES_UID: 1000
      HERMES_GID: 1000
      API_SERVER_ENABLED: "true"
      API_SERVER_HOST: 0.0.0.0
      API_SERVER_PORT: 8642
      API_SERVER_KEY: ${LITELLM_MASTER_KEY}
      API_SERVER_MODEL_NAME: hermes
      HERMES_MODEL: gemini-flash-latest
      HERMES_PROVIDER: custom
      HERMES_BASE_URL: http://litellm:4000/v1
      HERMES_API_KEY: ${LITELLM_MASTER_KEY}
      LITELLM_MASTER_KEY: ${LITELLM_MASTER_KEY}
      CLAUDE_CONFIG_DIR: /opt/data/claude
      COMPOSE_PROJECT_NAME: mds_demo
    ports:
      - "8642:8642"
    volumes:
      - ./application:/workspace/application
      - ./docker-compose.yml:/workspace/docker-compose.yml:ro
      - ./.env:/workspace/.env:ro
      - ./data/hermes:/opt/data
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      - litellm
    networks:
      - ndsnet
    restart: unless-stopped

  hermes-dashboard:
    image: nousresearch/hermes-agent:latest
    container_name: hermes-dashboard
    command: dashboard --host 0.0.0.0 --insecure
    environment:
      HERMES_UID: 1000
      HERMES_GID: 1000
      GATEWAY_HEALTH_URL: http://hermes:8642
    ports:
      - "9119:9119"
    volumes:
      - ./data/hermes:/opt/data
    depends_on:
      - hermes
    networks:
      - ndsnet
    restart: unless-stopped
```

- [ ] **Step 2: Verify Compose parses**

Run: `docker compose config -q && echo OK`
Expected: `OK`

- [ ] **Step 3: Build the Hermes image**

Run: `docker compose build hermes`
Expected: build succeeds; the final layer prints a `docker compose version` and `claude` version. If the build fails on a missing `node`/`curl`, apply the fallback noted in Task 5 Step 1.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add hermes + hermes-dashboard services"
```

---

## Task 7: Add the `open-webui` service

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add the service**

Insert immediately before the top-level `networks:` key in `docker-compose.yml`:

```yaml
  open-webui:
    image: ghcr.io/open-webui/open-webui:main
    container_name: open-webui
    environment:
      PORT: 8090
      DATABASE_URL: postgresql://${WAREHOUSE_DB_USER}:${WAREHOUSE_DB_PASS}@warehouse_db:5432/openwebui
      REDIS_URL: redis://redis:6379
      VECTOR_DB: pgvector
      PGVECTOR_DB_URL: postgresql://${WAREHOUSE_DB_USER}:${WAREHOUSE_DB_PASS}@warehouse_db:5432/openwebui
      OPENAI_API_BASE_URL: http://hermes:8642/v1
      OPENAI_API_KEY: ${LITELLM_MASTER_KEY}
      WEBUI_SECRET_KEY: ${WEBUI_SECRET_KEY}
      WEBUI_AUTH: "true"
      ENABLE_SIGNUP: "true"
    ports:
      - "8090:8090"
    volumes:
      - ./data/open-webui:/app/backend/data
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      warehouse_db:
        condition: service_healthy
      redis:
        condition: service_started
      hermes:
        condition: service_started
    networks:
      - ndsnet
    restart: unless-stopped
```

- [ ] **Step 2: Verify Compose parses**

Run: `docker compose config -q && echo OK`
Expected: `OK`

- [ ] **Step 3: Confirm all five new services are present**

Run: `docker compose config --services | sort`
Expected: the list includes `hermes`, `hermes-dashboard`, `litellm`, `open-webui`, `redis` and no longer includes `anything-llm`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add open-webui service"
```

---

## Task 8: Operator environment scaffolding

**Files:**
- Create: `application/CLAUDE.md`
- Create: `application/.claude/settings.json`

- [ ] **Step 1: Create the operator charter**

Create `application/CLAUDE.md`:

```markdown
# MDS Demo — Platform Operator

You are the **operations agent** for the MDS Demo data platform. You run
headless (`claude -p`), invoked by the Hermes agent, to carry out
data-platform tasks for end users chatting in OpenWebUI.

This is a **self-contained operator environment**. Your project root is this
`application/` directory. Never look for configuration above it.

## What you operate

A Modern Data Stack in Docker Compose (project name `mds_demo`):

| Service | Ports | Role |
|---|---|---|
| `minio` | 9000 / 9001 | S3 object storage / data lake |
| `warehouse_db` | 5432 | Postgres 17 warehouse (pg_duckdb, pgvector, PostGIS) |
| `magic` | 6789 | Mage.ai orchestration; hosts dbt + dlt |
| `trino` | 8080 | Federated SQL |
| `metabase` | 3000 | BI dashboards |
| `hive-metastore` | 9083 | Delta Lake metadata |
| `jupyterlab` | 8888 | Notebooks |
| `litellm` / `hermes` / `open-webui` | 4000 / 8642 / 8090 | The agent layer |

All services share the `ndsnet` network.

## How to address services

Always use Compose with an explicit project name and the mounted compose file:

    docker compose -p mds_demo -f /workspace/docker-compose.yml <command> <service>

Examples: `... ps`, `... logs --tail 100 trino`, `... restart magic`,
`... exec -T warehouse_db sh -c '...'`.

Container re-creation (`up`, `down`, `build`, `create`) is **out of scope** —
those need host paths this environment does not have. Use `restart` instead.

## Data model layers (dbt, inside the `magic` container)

`raw` (external views over MinIO) -> `stg` (bronze) -> `bdh` (silver) ->
`adl` (gold). dbt project path inside `magic`: `/home/src/mds_demo/dbt/data_warehouse`.

## Your skills

- `platform-orientation` — service topology and how data flows
- `operating-services` — inspect, restart, and health-check services
- `running-dbt-models` — run and test dbt models
- `managing-mage-pipelines` — build blocks and trigger pipelines
- `querying-the-warehouse` — run SQL against `warehouse_db`
- `managing-minio-storage` — inspect and manage MinIO buckets/objects

## Safety boundaries

ALLOWED: run dbt; build/trigger Mage pipelines; query the warehouse; manage
MinIO objects; restart/inspect/health-check services; edit files under
`application/`.

OUT OF SCOPE — refuse and explain why:
- Editing `docker-compose.yml` or `.env`.
- Re-creating containers (`docker compose up` / `down` / `build` / `create`).
- Destructive data deletion (dropping warehouse schemas, deleting MinIO buckets).

When unsure, inspect first and report. Never guess at a destructive action.

## Knowledge base

Before non-trivial work, read `.claude/knowledge/runbooks.md`. When you find a
fix or gotcha worth keeping, append it to `.claude/knowledge/corrections.md`.
```

- [ ] **Step 2: Create operator settings**

Create `application/.claude/settings.json`:

```json
{
  "permissions": {
    "allow": [
      "Bash(docker compose -p mds_demo -f /workspace/docker-compose.yml *)",
      "Bash(docker ps*)",
      "Bash(docker stats*)",
      "Bash(docker inspect*)",
      "Bash(docker logs*)"
    ]
  }
}
```

(`claude-ops` runs with `--permission-mode bypassPermissions`, so this allowlist
mainly documents intent and applies if the environment is ever run interactively.)

- [ ] **Step 3: Verify JSON is valid**

Run: `python3 -m json.tool application/.claude/settings.json > /dev/null && echo OK`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add application/CLAUDE.md application/.claude/settings.json
git commit -m "feat: add operator Claude Code environment scaffolding"
```

---

## Task 9: Operator skills

**Files:**
- Create: `application/.claude/skills/platform-orientation/SKILL.md`
- Create: `application/.claude/skills/operating-services/SKILL.md`
- Create: `application/.claude/skills/running-dbt-models/SKILL.md`
- Create: `application/.claude/skills/managing-mage-pipelines/SKILL.md`
- Create: `application/.claude/skills/querying-the-warehouse/SKILL.md`
- Create: `application/.claude/skills/managing-minio-storage/SKILL.md`

- [ ] **Step 1: Create `platform-orientation`**

Create `application/.claude/skills/platform-orientation/SKILL.md`:

````markdown
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
````

- [ ] **Step 2: Create `operating-services`**

Create `application/.claude/skills/operating-services/SKILL.md`:

```markdown
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
```

- [ ] **Step 3: Create `running-dbt-models`**

Create `application/.claude/skills/running-dbt-models/SKILL.md`:

```markdown
---
name: running-dbt-models
description: Use to run, test, compile, or inspect dbt models in the MDS Demo data_warehouse project. Use when a user asks to refresh a table, run transformations, or test data quality.
---

# Running dbt Models

dbt lives inside the `magic` (Mage) container. Run it via Compose `exec`:

    DC="docker compose -p mds_demo -f /workspace/docker-compose.yml"
    DBT_DIR=/home/src/mds_demo/dbt/data_warehouse

## Commands

- Install packages: `$DC exec -T magic sh -c "cd $DBT_DIR && dbt deps"`
- Run a model and its layer:
  `$DC exec -T magic sh -c "cd $DBT_DIR && dbt run --select <model>"`
- Run a whole schema layer: `... dbt run --select stg` (or `bdh`, `adl`)
- Test: `$DC exec -T magic sh -c "cd $DBT_DIR && dbt test --select <model>"`
- Preview results: `... dbt show --select <model> --limit 20`

## Layers

Build upward: `raw` -> `stg` -> `bdh` -> `adl`. To refresh a gold table,
run its `adl` model; dbt resolves upstream `ref()` dependencies.

## On failure

Read the dbt error block. Common causes: an upstream model not built yet
(run the upstream layer first), or a source not loaded (run the matching
Mage pipeline — see `managing-mage-pipelines`). If the `--profiles-dir` or
project path differs from the above, record the correct values in
`.claude/knowledge/corrections.md`.
```

- [ ] **Step 4: Create `managing-mage-pipelines`**

Copy the existing Gemini skill body, then replace its frontmatter.

Run: `mkdir -p application/.claude/skills/managing-mage-pipelines && cp .gemini/skills/mage-ai-blocks/SKILL.md application/.claude/skills/managing-mage-pipelines/SKILL.md`

Then replace the YAML frontmatter at the top of the copied file (the block between the first two `---` lines) with exactly:

```markdown
---
name: managing-mage-pipelines
description: Use to build Mage AI blocks (loaders, transformers, exporters) and to trigger or inspect pipelines in the MDS Demo project. Use when a user asks to add a data source, build an ingestion step, or run a pipeline.
---
```

Then append this section to the end of that file:

```markdown

## Triggering pipelines

Run a pipeline from the `magic` container:

    DC="docker compose -p mds_demo -f /workspace/docker-compose.yml"
    $DC exec -T magic mage run mds_demo <pipeline_name>

List pipelines by inspecting `application/mage_ai/mds_demo/pipelines/`.
After a run, verify the loaded data with the `querying-the-warehouse` skill.
```

- [ ] **Step 5: Create `querying-the-warehouse`**

Create `application/.claude/skills/querying-the-warehouse/SKILL.md`:

```markdown
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
```

- [ ] **Step 6: Create `managing-minio-storage`**

Create `application/.claude/skills/managing-minio-storage/SKILL.md`:

```markdown
---
name: managing-minio-storage
description: Use to inspect or manage MinIO object storage — listing buckets and objects, checking sizes, or uploading/downloading files in the data lake. Use when a task involves the raw landing zone or s3://dwhfilesystem.
---

# Managing MinIO Storage

The `mc` client is not in this container. Run it as a one-shot container on
`ndsnet`, configuring an alias from MinIO's own environment:

    docker run --rm --network mds_demo_ndsnet --entrypoint sh minio/mc -c '
      mc alias set local http://minio:9000 "$MINIO_ADMIN" "$MINIO_PWD" >/dev/null &&
      mc ls local/'

`MINIO_ADMIN` / `MINIO_PWD` are in `/workspace/.env`; pass them with
`-e MINIO_ADMIN=... -e MINIO_PWD=...` or read them from that file first.

## Common operations

- List buckets: `mc ls local/`
- List a bucket: `mc ls --recursive local/dwhfilesystem/`
- Object info: `mc stat local/dwhfilesystem/<path>`
- Bucket size: `mc du local/dwhfilesystem/`

## Buckets

`dwhfilesystem` is the primary data lake (Delta + Iceberg + parquet);
`dwhfilesystem/landing_area/` is the raw file drop zone.

## Rules

Listing and stat are always safe. Do NOT delete buckets or run recursive
`mc rm` — destructive deletion is out of scope. Confirm any single-object
removal with the user first.
```

- [ ] **Step 7: Verify every SKILL.md has valid frontmatter**

Run:
```bash
for f in application/.claude/skills/*/SKILL.md; do
  head -1 "$f" | grep -qx -- '---' && echo "OK  $f" || echo "BAD $f"
done
```
Expected: six `OK` lines.

- [ ] **Step 8: Commit**

```bash
git add application/.claude/skills/
git commit -m "feat: add operator skills (services, dbt, mage, warehouse, minio)"
```

---

## Task 10: Operator slash commands

**Files:**
- Create: `application/.claude/commands/health-check.md`
- Create: `application/.claude/commands/run-dbt.md`
- Create: `application/.claude/commands/run-pipeline.md`
- Create: `application/.claude/commands/restart-service.md`

- [ ] **Step 1: Create `health-check.md`**

Create `application/.claude/commands/health-check.md`:

```markdown
---
description: Sweep every platform service and report its health
---

Use the `operating-services` skill. Run a full health-check sweep of the
MDS Demo platform:

1. `docker compose -p mds_demo -f /workspace/docker-compose.yml ps`
2. For every service, report: state (running / restarting / exited) and,
   where a healthcheck exists, its health status.
3. For anything not running or not healthy, pull `logs --tail 50` and
   summarize the likely cause.

Finish with a one-line overall verdict: HEALTHY or NEEDS ATTENTION.
```

- [ ] **Step 2: Create `run-dbt.md`**

Create `application/.claude/commands/run-dbt.md`:

```markdown
---
description: Run dbt models for a given selector
argument-hint: <dbt selector, e.g. stg or fact_place>
---

Use the `running-dbt-models` skill. Run dbt for the selector: `$ARGUMENTS`

1. Run `dbt run --select $ARGUMENTS` in the `magic` container.
2. If it fails on a missing upstream, build the upstream layer first, then retry.
3. Report which models built, their row counts (via `querying-the-warehouse`),
   and any failures.
```

- [ ] **Step 3: Create `run-pipeline.md`**

Create `application/.claude/commands/run-pipeline.md`:

```markdown
---
description: Trigger a Mage pipeline by name
argument-hint: <pipeline name>
---

Use the `managing-mage-pipelines` skill. Trigger the pipeline: `$ARGUMENTS`

1. Confirm the pipeline exists under `application/mage_ai/mds_demo/pipelines/`.
2. Run `mage run mds_demo $ARGUMENTS` in the `magic` container.
3. Report success or failure; on failure, summarize the error from the output.
```

- [ ] **Step 4: Create `restart-service.md`**

Create `application/.claude/commands/restart-service.md`:

```markdown
---
description: Restart a single platform service
argument-hint: <service name>
---

Use the `operating-services` skill. Restart the service: `$ARGUMENTS`

1. Show its current state with `ps`.
2. `docker compose -p mds_demo -f /workspace/docker-compose.yml restart $ARGUMENTS`
3. Wait, then confirm it is running again and show fresh `logs --tail 30`.

Never run `up`/`down`/`build` — only `restart`.
```

- [ ] **Step 5: Verify all four files exist with frontmatter**

Run:
```bash
for f in application/.claude/commands/*.md; do
  head -1 "$f" | grep -qx -- '---' && echo "OK  $f" || echo "BAD $f"
done
```
Expected: four `OK` lines.

- [ ] **Step 6: Commit**

```bash
git add application/.claude/commands/
git commit -m "feat: add operator slash commands"
```

---

## Task 11: Operator subagents and knowledge base

**Files:**
- Create: `application/.claude/agents/pipeline-operator.md`
- Create: `application/.claude/agents/incident-investigator.md`
- Create: `application/.claude/knowledge/runbooks.md`
- Create: `application/.claude/knowledge/corrections.md`

- [ ] **Step 1: Create `pipeline-operator.md`**

Create `application/.claude/agents/pipeline-operator.md`:

```markdown
---
name: pipeline-operator
description: Runs an MDS Demo data pipeline end to end — triggers ingestion, then builds the dbt layers that depend on it, and verifies the result.
---

You run a data pipeline from raw load through to the gold layer.

Steps:
1. Trigger the relevant Mage pipeline (`managing-mage-pipelines` skill).
2. Build the dbt layers downstream of the loaded source, in order
   `stg -> bdh -> adl` (`running-dbt-models` skill).
3. Verify final row counts with the `querying-the-warehouse` skill.
4. Report a concise summary: what loaded, what built, final counts, failures.

Stay within the operator safety boundaries in `application/CLAUDE.md`. Do not
re-create containers. If a step fails, stop, report clearly, and do not retry
blindly.
```

- [ ] **Step 2: Create `incident-investigator.md`**

Create `application/.claude/agents/incident-investigator.md`:

```markdown
---
name: incident-investigator
description: Diagnoses a failing or unhealthy MDS Demo service — gathers state and logs, identifies the root cause, and proposes a fix.
---

You diagnose a platform incident. You investigate; you do not apply risky fixes
without surfacing them first.

Steps:
1. Establish scope: `docker compose -p mds_demo -f /workspace/docker-compose.yml ps`.
2. For the affected service, gather `logs --tail 200` and `docker inspect` health.
3. Check `.claude/knowledge/runbooks.md` for a matching known issue.
4. State the root cause in one or two sentences.
5. Propose the fix. If it is a `restart`, you may do it. Anything beyond a
   restart — report it and stop.
6. If you learned something new, append it to `.claude/knowledge/corrections.md`.
```

- [ ] **Step 3: Create `runbooks.md`**

Create `application/.claude/knowledge/runbooks.md`:

```markdown
# Runbooks

Known platform issues and their fixes. Check here before deep investigation.

## Docker socket: "permission denied"

`docker` commands fail with a permission error on `/var/run/docker.sock`.
Cause: the socket's group is not accessible to the operator user. The Hermes
`init.sh` runs `chmod 666` on the socket at startup; if the container was
started before that ran, restart `hermes`. If it persists, the host socket
group GID must be added to the container — flag this to a human.

## A service shows "unhealthy"

Check the healthcheck target. `minio`, `warehouse_db`, and `redis` have
healthchecks; an unhealthy state usually means the service is still starting
(wait) or its dependency is down (check `warehouse_db` first — most services
depend on it).

## dbt run fails: "relation does not exist"

An upstream model or source is not built. Build the upstream layer first
(`raw`/`stg`/`bdh`) or run the Mage pipeline that loads the source, then retry.

## Mage pipeline not found

Pipeline names come from directories under
`application/mage_ai/mds_demo/pipelines/`. List that folder to get exact names.
```

- [ ] **Step 4: Create `corrections.md`**

Create `application/.claude/knowledge/corrections.md`:

```markdown
# Corrections

Append a dated entry whenever you discover a fix, a wrong assumption, or a
gotcha worth remembering. Newest entries at the top.

Format:

## YYYY-MM-DD — <short title>
**Observed:** what happened.
**Cause:** the root cause.
**Fix:** what resolved it.

<!-- entries below -->
```

- [ ] **Step 5: Verify files exist**

Run: `ls application/.claude/agents/ application/.claude/knowledge/`
Expected: `incident-investigator.md  pipeline-operator.md` and `corrections.md  runbooks.md`.

- [ ] **Step 6: Commit**

```bash
git add application/.claude/agents/ application/.claude/knowledge/
git commit -m "feat: add operator subagents and knowledge base"
```

---

## Task 12: Update root documentation

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the "Stack at a glance" table**

In `CLAUDE.md`, in the `## Stack at a glance` table: remove the AnythingLLM row,
and add these rows:

```markdown
| End-user chat | OpenWebUI | `ghcr.io/open-webui/open-webui` |
| Operator agent | NousResearch Hermes | custom `application/hermes/Dockerfile` |
| Model gateway | LiteLLM | `ghcr.io/berriai/litellm` |
| Agent cache | Redis | `redis/redis-stack` |
```

- [ ] **Step 2: Update the "Service ports" table**

Remove the AnythingLLM row; add:

```markdown
| OpenWebUI | 8090 | End-user chat UI |
| Hermes | 8642 | Operator agent (OpenAI-compatible API) |
| Hermes dashboard | 9119 | Agent run dashboard |
| LiteLLM | 4000 | Model gateway |
```

- [ ] **Step 3: Add an "Agent operations layer" section**

Add this section to `CLAUDE.md` after the `## Data flow` section:

````markdown
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
````

- [ ] **Step 4: Update the AnythingLLM references**

Remove the `## AnythingLLM configuration` section. In the "Databases inside
warehouse_db" table, leave the `anythingllm` row but append " (legacy — service
removed; DB retained)" to its description, and add `openwebui` and `litellm`
rows. Remove AnythingLLM bullets from "Things to be aware of".

- [ ] **Step 5: Verify no stale AnythingLLM service references remain**

Run: `grep -n -i 'anythingllm\|anything-llm' CLAUDE.md`
Expected: at most the single legacy `anythingllm` database-table row; no
references to it as a running service.

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document agent operations layer, remove AnythingLLM"
```

---

## Task 13: End-to-end bring-up and verification

**Files:** none (operational verification)

- [ ] **Step 1: Validate the full Compose file**

Run: `docker compose config -q && echo OK`
Expected: `OK`

- [ ] **Step 2: Create the warehouse databases if `warehouse_db` already has a volume**

The `04-agent-dbs.sh` init script runs only on a fresh `warehouse_db` volume.
If `database/warehouse_db` already exists, create the databases once manually:

```bash
docker compose up -d warehouse_db
docker compose exec -T warehouse_db sh -c '
  for db in openwebui litellm; do
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tc \
      "SELECT 1 FROM pg_database WHERE datname='"'"'$db'"'"'" | grep -q 1 ||
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE $db;"
  done
  psql -U "$POSTGRES_USER" -d openwebui -c "CREATE EXTENSION IF NOT EXISTS vector;"'
```

Run: `docker compose exec -T warehouse_db sh -c 'psql -U "$POSTGRES_USER" -lqt' | cut -d'|' -f1`
Expected: the list includes `openwebui` and `litellm`.

- [ ] **Step 3: Bring up the stack**

Run: `docker compose up -d --build`
Then: `docker compose ps`
Expected: all services `running`; `minio`, `warehouse_db`, `redis` report `healthy`.

- [ ] **Step 4: Verify LiteLLM**

Run: `curl -s http://localhost:4000/health/liveliness`
Expected: a success response (e.g. `"I'm alive!"`).

- [ ] **Step 5: One-time Claude Code login for the operator (interactive)**

The operator's Claude Code needs a subscription OAuth token in its config dir
(`./data/hermes/claude`). Run an interactive login once:

```bash
docker compose exec -it hermes sh -lc 'CLAUDE_CONFIG_DIR=/opt/data/claude claude'
```

Complete `/login` in the TUI, then exit. Confirm the credential was written:

Run: `ls data/hermes/claude/`
Expected: a `.credentials.json` file is present.

- [ ] **Step 6: Verify the Hermes -> Claude Code bridge**

Run:
```bash
docker compose exec -T hermes claude-ops -p "Run a platform health check and give the one-line verdict."
```
Expected: Claude Code loads the operator environment, runs the
`operating-services` skill / `/health-check` flow against the live stack, and
prints a `HEALTHY` or `NEEDS ATTENTION` verdict. This proves the full chain:
`claude-ops` -> LiteLLM -> Anthropic auth -> operator skills -> Docker socket.

- [ ] **Step 7: Verify OpenWebUI end to end**

Open `http://localhost:8090`, create the first account, and send a chat
message such as: `List the running platform services and their health.`
Expected: OpenWebUI -> Hermes -> (delegates to `claude-ops`) -> a service list
with health status comes back in the chat.

- [ ] **Step 8: Final commit**

If Steps 1-7 surfaced any fixes (e.g. an adjusted image tag or socket-permission
tweak), commit them:

```bash
git add -A
git commit -m "fix: adjustments from agent-layer bring-up verification"
```

If nothing changed, skip this commit.

---

## Self-Review Notes

- **Spec coverage:** all five services (Task 4/6/7), DB reuse + init (Task 2),
  the `claude-ops` bridge (Task 5), the operator environment incl. `application/`
  root `CLAUDE.md` and `.claude/` skills/commands/agents/knowledge (Tasks 8-11),
  AnythingLLM removal (Task 4 + Task 12), `.env` changes (Task 1), and root-doc
  updates (Task 12) each map to a task. End-to-end verification mirrors the
  spec's testing section (Task 13).
- **Spec deviations** (recorded at the top of this plan): lifecycle via
  `docker compose` subcommands (compose file mounted read-only) instead of
  raw `docker` against guessed container names; operator environment is
  plugin-free with a self-contained `running-dbt-models` skill.
- **Out of scope, per spec:** Telegram wiring for Hermes; deletion of the
  legacy `anythingllm` database and `data/anythingllm/` directory.
