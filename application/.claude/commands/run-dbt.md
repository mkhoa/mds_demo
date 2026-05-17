---
description: Run dbt models for a given selector
argument-hint: <dbt selector, e.g. stg or fact_place>
---

Use the `running-dbt-models` skill. Run dbt for the selector: `$ARGUMENTS`

1. Run `dbt run --select $ARGUMENTS` in the `magic` container.
2. If it fails on a missing upstream, build the upstream layer first, then retry.
3. Report which models built, their row counts (via `querying-the-warehouse`),
   and any failures.
