# Corrections

Append a dated entry whenever you discover a fix, a wrong assumption, or a
gotcha worth remembering. Newest entries at the top.

Format:

## YYYY-MM-DD — <short title>
**Observed:** what happened.
**Cause:** the root cause.
**Fix:** what resolved it.

<!-- entries below -->
## 2026-05-17 — Default to Parquet for Data Loading
**Observed:** The `ecommerce_load` pipeline was using CSV despite status reports indicating Parquet, causing confusion.
**Cause:** Generic exporter blocks defaulted to Parquet but were manually overridden to CSV, and the agent lacked a clear convention to favor Parquet.
**Fix:** Updated `CLAUDE.md` to establish Parquet as the default format for all data ingestion. CSV is now only used if explicitly requested.
