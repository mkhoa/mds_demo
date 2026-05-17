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
