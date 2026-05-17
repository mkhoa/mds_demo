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
