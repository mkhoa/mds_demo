# BDH Star Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the silver layer (`bdh`) star schema for era5 weather and Overture maps data, mapped to Vietnam 2025 ward boundaries via spatial join, plus reusable `dim_date` / `dim_month` / `dim_year` from `dbt_utils.date_spine`.

**Architecture:** Add a new BigQuery → MinIO → raw → stg ingestion path for Overture `division_area` polygons (the spatial bridge between era5 `location_id` and `ma_xa`). Then build seven dimension tables (`dim_year`, `dim_month`, `dim_date`, `dim_ward`, `dim_division_area`, `dim_place_category`, `dim_land_class`) and three fact tables (`fact_era5_weather`, `fact_place`, `fact_land_feature`). Spatial joins run via pg_duckdb / DuckDB spatial — no PostGIS `::geography` casts.

**Tech Stack:** dbt 1.x (Postgres adapter), pg_duckdb (DuckDB spatial extension), dbt_utils 1.3.3 (date_spine, generate_surrogate_key), Mage.ai (Python loaders, dbt orchestration), Google BigQuery (source for division_area), MinIO (S3 landing).

**Spec:** [`docs/superpowers/specs/2026-04-26-bdh-star-schema-design.md`](../specs/2026-04-26-bdh-star-schema-design.md)

**Conventions used in this plan:**

- Working directory for shell commands: `/home/mkhoa/Development/mds_demo`
- All dbt commands run inside the `magic` container:
  `docker compose exec magic bash -lc 'cd /home/src/mds_demo/dbt/data_warehouse && <dbt command>'`
- All paths are relative to repo root unless absolute.
- Each task ends with a commit.
- TDD-for-dbt pattern: declare model + tests in `schema.yml` first, run `dbt parse` (expect failure: model SQL missing), then write the SQL, then `dbt build --select <model>` (expect pass: build + tests).

---

## File Structure

**New files:**

```
application/mage_ai/mds_demo/
├── data_loaders/overture_maps/
│   └── load_overture_division_area.py                  # Task 1
├── pipelines/overture_maps_division_area_ingestion/
│   └── metadata.yaml                                   # Task 1
└── dbt/data_warehouse/
    ├── macros/
    │   └── parquet_scans.sql                           # Task 2 (modify - append macro)
    └── models/
        ├── raw/overture_maps/
        │   ├── raw_overture_maps__division_area.sql    # Task 3
        │   └── schema.yml                              # Task 3 (modify)
        ├── stg/overture_maps/
        │   ├── stg_overture_maps__division_area.sql    # Task 4
        │   └── schema.yml                              # Task 4 (modify)
        └── bdh/
            ├── dim/
            │   ├── dim_year.sql                        # Task 5
            │   ├── dim_month.sql                       # Task 6
            │   ├── dim_date.sql                        # Task 7
            │   ├── dim_ward.sql                        # Task 8
            │   ├── dim_division_area.sql               # Task 9
            │   ├── dim_place_category.sql              # Task 10
            │   ├── dim_land_class.sql                  # Task 11
            │   └── schema.yml                          # written incrementally per task
            ├── era5_weather/
            │   ├── fact_era5_weather.sql               # Task 12
            │   └── schema.yml                          # Task 12
            └── overture_maps/
                ├── fact_place.sql                      # Task 13
                ├── fact_land_feature.sql               # Task 14
                └── schema.yml                          # Task 13, then 14
```

**Each file's responsibility:**

- `load_overture_division_area.py` — single BigQuery query to MinIO landing (mirrors `load_overture_base.py`).
- `parquet_scans.sql` — typed-scan macro for division_area parquet (mirrors existing macros).
- `raw_*` — pg_duckdb view over MinIO parquet, no transformations.
- `stg_*` — cleaned, typed table with PostGIS geometry + ingestion partition columns.
- `dim_*` — bdh dimension tables, all with `sk_*` MD5 surrogate keys via `dbt_utils.generate_surrogate_key`.
- `fact_*` — bdh fact tables, joined to dims at build time.

---

## Task 1: Overture division_area Mage pipeline

**Files:**
- Create: `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_division_area.py`
- Create: `application/mage_ai/mds_demo/pipelines/overture_maps_division_area_ingestion/metadata.yaml`

- [ ] **Step 1.1: Create the BigQuery data loader**

Write `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_division_area.py`:

```python
import pandas as pd
from google.cloud import bigquery

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps division_area polygons for Vietnam from BigQuery public dataset.
    These polygons are the spatial bridge between era5 location_id and Vietnamese
    ma_xa (used downstream in dim_division_area to assign each division to a ward).
    Geometry is serialised as WKT for parquet storage.
    Auth via GOOGLE_APPLICATION_CREDENTIALS env var.
    """
    query = """
SELECT
    id,
    subtype,
    names.primary                 AS location_name,
    country,
    region,
    ST_Y(ST_CENTROID(geometry))   AS latitude,
    ST_X(ST_CENTROID(geometry))   AS longitude,
    ST_ASTEXT(geometry)           AS geometry_wkt
FROM `bigquery-public-data.overture_maps.division_area`
WHERE country = 'VN'
ORDER BY subtype
"""

    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df):,} division_area rows for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No rows returned — check BigQuery credentials and query'
    required = {'id', 'subtype', 'location_name', 'country', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found'
    assert df['geometry_wkt'].notna().all(), 'Null geometry_wkt values found'
    assert (df['country'] == 'VN').all(), 'Non-VN rows leaked into output'
```

- [ ] **Step 1.2: Create the pipeline metadata**

Write `application/mage_ai/mds_demo/pipelines/overture_maps_division_area_ingestion/metadata.yaml`:

```yaml
blocks:
- all_upstream_blocks_executed: true
  color: null
  configuration:
    file_source:
      path: data_loaders/overture_maps/load_overture_division_area.py
  downstream_blocks:
  - generic_components/write2landing_block
  executor_config: null
  executor_type: local_python
  has_callback: false
  language: python
  name: overture_maps/load_overture_division_area
  retry_config: null
  status: updated
  timeout: null
  type: data_loader
  upstream_blocks: []
  uuid: overture_maps/load_overture_division_area
- all_upstream_blocks_executed: false
  color: null
  configuration:
    file_source:
      path: data_exporters/generic_components/write2landing_block.py
  downstream_blocks: []
  executor_config: null
  executor_type: local_python
  has_callback: false
  language: python
  name: generic_components/write2landing_block
  retry_config: null
  status: updated
  timeout: null
  type: data_exporter
  upstream_blocks:
  - overture_maps/load_overture_division_area
  uuid: generic_components/write2landing_block
cache_block_output_in_memory: false
callbacks: []
concurrency_config: {}
conditionals: []
created_at: null
data_integration: null
description: 'Load Overture Maps division_area polygons for Vietnam from BigQuery public dataset. Provides the polygons used by bdh dim_division_area to spatially map era5 location_id to ma_xa. Writes to MinIO landing area as Hive-partitioned parquet.'
executor_config: {}
executor_count: 1
executor_type: null
extensions: {}
name: overture_maps_division_area_ingestion
notification_config: {}
remote_variables_dir: null
retry_config: {}
run_pipeline_in_one_process: false
settings:
  triggers: null
spark_config: {}
tags:
- gis
- overture
- bigquery
type: python
uuid: overture_maps_division_area_ingestion
variables:
  bucket_name: dwhfilesystem
  file_format: parquet
  ingestion_data: overture_maps_division_area
variables_dir: /home/src/mage_data/mds_demo
widgets: []
```

- [ ] **Step 1.3: Trigger the pipeline once to land data on MinIO**

Run from repo root:

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo && mage run mds_demo overture_maps_division_area_ingestion'
```

Expected: pipeline runs successfully and lands a parquet file at
`s3://dwhfilesystem/landing_area/overture_maps_division_area/year=YYYY/month=M/day=D/overture_maps_division_area_HHMMSS.parquet`.

Verify on MinIO console (http://localhost:9001) or via:

```bash
docker compose exec minio bash -lc 'mc ls -r minio/dwhfilesystem/landing_area/overture_maps_division_area/'
```

(If `mc` is not aliased, replace with the appropriate MinIO client invocation.)

Expected: at least one `.parquet` file listed.

- [ ] **Step 1.4: Commit**

```bash
git add application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_division_area.py \
        application/mage_ai/mds_demo/pipelines/overture_maps_division_area_ingestion/metadata.yaml
git commit -m "feat: add overture_maps_division_area_ingestion Mage pipeline"
```

---

## Task 2: parquet_scans macro for division_area

**Files:**
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql` (append new macro)

- [ ] **Step 2.1: Append the new macro**

Add at the end of `application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql`:

```sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Overture Maps — Division Area (admin polygons used as spatial bridge to ma_xa)
-- Layout: s3://dwhfilesystem/landing_area/overture_maps_division_area/year=Y/month=M/day=D/*.parquet
-- Source: bigquery-public-data.overture_maps.division_area, filtered to country='VN'
-- ──────────────────────────────────────────────────────────────────────────────
{% macro overture_maps_division_area_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                 AS id,
    r['subtype']::VARCHAR            AS subtype,
    r['location_name']::VARCHAR      AS location_name,
    r['country']::VARCHAR            AS country,
    r['region']::VARCHAR             AS region,
    r['latitude']::DOUBLE PRECISION  AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR       AS geometry_wkt,
    r['year']::INTEGER               AS year,
    r['month']::INTEGER              AS month,
    r['day']::INTEGER                AS day
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/overture_maps_division_area/**/*.parquet', partition_filter) }}
{% endmacro %}
```

- [ ] **Step 2.2: Verify macro parses**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt parse'
```

Expected: `Encountered an error: ...` only if there's a syntax issue. Should print `Performance info: ...` and exit 0 cleanly.

- [ ] **Step 2.3: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql
git commit -m "feat: add overture_maps_division_area_typed_scan macro"
```

---

## Task 3: raw_overture_maps__division_area model

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__division_area.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/schema.yml`

- [ ] **Step 3.1: Add schema.yml entry first (the contract)**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/schema.yml`:

```yaml

  - name: raw_overture_maps__division_area
    description: >
      External view reading Overture Maps division_area polygons for Vietnam from
      the MinIO landing area via pg_duckdb. Sourced from
      bigquery-public-data.overture_maps.division_area (country='VN'). Used
      downstream as the spatial bridge between era5 location_id and Vietnamese
      ma_xa (see bdh dim_division_area).
    columns:
      - name: id
        description: Overture Maps globally unique division_area identifier
      - name: subtype
        description: Division subtype (country, region, county, locality, …)
      - name: location_name
        description: Primary name (Overture names.primary)
      - name: country
        description: ISO country code (always 'VN')
      - name: region
        description: ISO region code (e.g., VN-SG)
      - name: latitude
        description: Centroid latitude
      - name: longitude
        description: Centroid longitude
      - name: geometry_wkt
        description: Full division polygon as WKT string
      - name: year
        description: Ingest year (Hive partition column)
      - name: month
        description: Ingest month (Hive partition column)
      - name: day
        description: Ingest day (Hive partition column)
```

- [ ] **Step 3.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__division_area.sql`:

```sql
{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_division_area_typed_scan() }}
```

- [ ] **Step 3.3: Build and verify**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select raw_overture_maps__division_area'
```

Expected: `1 of 1 OK created sql view model raw.raw_overture_maps__division_area`. Tests pass (only model parse).

Sanity check — query the view from inside the container:

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS rows, COUNT(DISTINCT subtype) AS subtypes FROM raw.raw_overture_maps__division_area;"
```

Expected: `rows` > 0; `subtypes` ≥ 1.

- [ ] **Step 3.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__division_area.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/schema.yml
git commit -m "feat: add raw_overture_maps__division_area dbt model"
```

---

## Task 4: stg_overture_maps__division_area model

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__division_area.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/schema.yml`

- [ ] **Step 4.1: Add schema.yml entry**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/schema.yml`:

```yaml

  - name: stg_overture_maps__division_area
    description: >
      Bronze staging model for Overture Maps division_area polygons in Vietnam.
      Geometry promoted via ST_GeomFromText. Rows missing id or geometry are
      filtered. Incrementally loaded by Hive partition watermark, delete+insert
      on id for dedup across re-runs. Source: raw_overture_maps__division_area.
    columns:
      - name: id
        description: Overture division_area unique id
        data_tests:
          - not_null
          - unique
      - name: subtype
        description: Division subtype (country, region, county, locality, …)
      - name: location_name
        description: Primary name
      - name: country
        description: ISO country code (always 'VN')
      - name: region
        description: ISO region code
      - name: latitude
        description: Centroid latitude
      - name: longitude
        description: Centroid longitude
      - name: geometry
        description: Division polygon (EPSG:4326), populated via ST_GeomFromText(geometry_wkt)
        data_tests:
          - not_null
      - name: year
        description: Ingest year (Hive partition)
      - name: month
        description: Ingest month (Hive partition)
      - name: day
        description: Ingest day (Hive partition)
```

- [ ] **Step 4.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__division_area.sql`:

```sql
{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['id'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      - Full refresh:  reads the raw view (full parquet scan via pg_duckdb).
      - Incremental:   calls overture_maps_division_area_typed_scan() with a partition
                       filter built from max (year, month, day) already in this table.
                       delete+insert on id handles dedup across re-runs.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ overture_maps_division_area_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_overture_maps__division_area') }}
    {% endif %}

),

cleaned AS (
    SELECT
        id,
        subtype,
        location_name,
        country,
        region,
        latitude,
        longitude,
        ST_GeomFromText(geometry_wkt)::geometry  AS geometry,
        year,
        month,
        day
    FROM source
    WHERE id           IS NOT NULL
      AND geometry_wkt IS NOT NULL
)

SELECT * FROM cleaned
```

- [ ] **Step 4.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select stg_overture_maps__division_area'
```

Expected:
- `1 of 1 OK created incremental model stg.stg_overture_maps__division_area`
- 3 of 3 PASS for `not_null_stg_overture_maps__division_area_id`, `unique_stg_overture_maps__division_area_id`, `not_null_stg_overture_maps__division_area_geometry`.

- [ ] **Step 4.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__division_area.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/schema.yml
git commit -m "feat: add stg_overture_maps__division_area dbt model"
```

---

## Task 5: dim_year

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_year.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 5.1: Create the schema.yml with dim_year entry**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml
version: 2

models:
  - name: dim_year
    description: >
      Year dimension generated from dbt_utils.date_spine, range 2015–2030.
      One row per calendar year. Reusable across all bdh fact tables for
      year-level analytics.
    columns:
      - name: sk_year
        description: Surrogate key — generate_surrogate_key([year])
        data_tests:
          - not_null
          - unique
      - name: year
        description: Calendar year (2015–2030)
        data_tests:
          - not_null
          - unique
      - name: year_start_date
        description: First day of the year (Jan 1)
      - name: year_end_date
        description: Last day of the year (Dec 31)
      - name: is_leap_year
        description: True if year is a leap year
      - name: is_current_year
        description: True if year matches the year of CURRENT_DATE at build time
```

- [ ] **Step 5.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_year.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH years AS (
    {{ dbt_utils.date_spine(
        datepart="year",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        EXTRACT(YEAR FROM date_year)::INT       AS year,
        date_year::DATE                          AS year_start_date,
        (DATE_TRUNC('year', date_year) + INTERVAL '1 year - 1 day')::DATE AS year_end_date
    FROM years
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year']) }}                             AS sk_year,
    year,
    year_start_date,
    year_end_date,
    (year % 4 = 0 AND (year % 100 <> 0 OR year % 400 = 0))                       AS is_leap_year,
    (year = EXTRACT(YEAR FROM CURRENT_DATE)::INT)                                AS is_current_year
FROM enriched
```

- [ ] **Step 5.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_year'
```

Expected:
- `1 of 1 OK created sql table model bdh.dim_year`
- All schema.yml tests PASS (4 generic tests).
- Row count check:

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) FROM bdh.dim_year;"
```

Expected: `16` (2015–2030 inclusive).

- [ ] **Step 5.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_year.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_year dimension"
```

---

## Task 6: dim_month

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_month.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 6.1: Append dim_month entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_month
    description: >
      Month dimension generated from dbt_utils.date_spine. One row per (year, month)
      from 2015-01 to 2030-12. Includes quarter, month name/abbreviation, and a
      foreign key to dim_year.
    columns:
      - name: sk_month
        description: Surrogate key — generate_surrogate_key([year, month])
        data_tests:
          - not_null
          - unique
      - name: sk_year
        description: FK to dim_year
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_year')
              field: sk_year
      - name: year
        description: Calendar year
        data_tests:
          - not_null
      - name: month
        description: Calendar month (1–12)
        data_tests:
          - not_null
      - name: month_name
        description: Full month name (January, February, …)
      - name: month_abbr
        description: 3-letter month abbreviation (Jan, Feb, …)
      - name: quarter
        description: Calendar quarter (1–4)
      - name: quarter_label
        description: Quarter label (Q1, Q2, Q3, Q4)
      - name: month_start_date
        description: First day of the month
      - name: month_end_date
        description: Last day of the month
      - name: is_current_month
        description: True if (year, month) matches CURRENT_DATE at build time
```

- [ ] **Step 6.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_month.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH months AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        EXTRACT(YEAR  FROM date_month)::INT                                  AS year,
        EXTRACT(MONTH FROM date_month)::INT                                  AS month,
        date_month::DATE                                                      AS month_start_date,
        (DATE_TRUNC('month', date_month) + INTERVAL '1 month - 1 day')::DATE  AS month_end_date,
        TO_CHAR(date_month, 'FMMonth')                                        AS month_name,
        TO_CHAR(date_month, 'Mon')                                            AS month_abbr,
        EXTRACT(QUARTER FROM date_month)::INT                                 AS quarter
    FROM months
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }}                AS sk_month,
    {{ dbt_utils.generate_surrogate_key(['year']) }}                         AS sk_year,
    year,
    month,
    month_name,
    month_abbr,
    quarter,
    'Q' || quarter::TEXT                                                      AS quarter_label,
    month_start_date,
    month_end_date,
    (year  = EXTRACT(YEAR  FROM CURRENT_DATE)::INT
     AND month = EXTRACT(MONTH FROM CURRENT_DATE)::INT)                       AS is_current_month
FROM enriched
```

- [ ] **Step 6.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_month'
```

Expected:
- Build OK.
- All schema.yml tests PASS, including `relationships` from `sk_year` to `dim_year.sk_year`.
- Row count: 16 years × 12 months = 192.

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) FROM bdh.dim_month;"
```

Expected: `192`.

- [ ] **Step 6.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_month.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_month dimension"
```

---

## Task 7: dim_date

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_date.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 7.1: Append dim_date entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_date
    description: >
      Daily date dimension generated from dbt_utils.date_spine, range 2015-01-01
      to 2030-12-31 (inclusive). One row per calendar day with FKs to dim_month
      and dim_year. day_of_week uses 0=Sunday convention to match the existing
      bdh_nyc_taxi__trips.pickup_dow column.
    columns:
      - name: sk_date
        description: Surrogate key — generate_surrogate_key([date_day])
        data_tests:
          - not_null
          - unique
      - name: sk_month
        description: FK to dim_month
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_month')
              field: sk_month
      - name: sk_year
        description: FK to dim_year
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_year')
              field: sk_year
      - name: date_day
        description: Calendar date (DATE), natural key
        data_tests:
          - not_null
          - unique
      - name: day_of_month
        description: Day of month (1–31)
      - name: day_of_week
        description: Day of week (0=Sunday, 6=Saturday)
      - name: day_of_week_name
        description: Full day name (Sunday, Monday, …)
      - name: day_of_year
        description: Day of year (1–366)
      - name: week_of_year
        description: ISO week number
      - name: is_weekend
        description: True if day_of_week is 0 (Sun) or 6 (Sat)
      - name: is_current_date
        description: True if date_day = CURRENT_DATE at build time
```

- [ ] **Step 7.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_date.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH days AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        date_day::DATE                                AS date_day,
        EXTRACT(YEAR  FROM date_day)::INT             AS year,
        EXTRACT(MONTH FROM date_day)::INT             AS month,
        EXTRACT(DAY   FROM date_day)::INT             AS day_of_month,
        EXTRACT(DOW   FROM date_day)::INT             AS day_of_week,    -- Postgres: 0=Sun ... 6=Sat
        TO_CHAR(date_day, 'FMDay')                    AS day_of_week_name,
        EXTRACT(DOY   FROM date_day)::INT             AS day_of_year,
        EXTRACT(WEEK  FROM date_day)::INT             AS week_of_year
    FROM days
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }}                       AS sk_date,
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }}                  AS sk_month,
    {{ dbt_utils.generate_surrogate_key(['year']) }}                           AS sk_year,
    date_day,
    day_of_month,
    day_of_week,
    day_of_week_name,
    day_of_year,
    week_of_year,
    (day_of_week IN (0, 6))                                                    AS is_weekend,
    (date_day = CURRENT_DATE)                                                  AS is_current_date
FROM enriched
```

- [ ] **Step 7.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_date'
```

Expected:
- Build OK, ~5,844 rows (16 years × 365.25 ≈ 5,844).
- All schema.yml tests PASS.

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS rows, MIN(date_day) AS min_date, MAX(date_day) AS max_date FROM bdh.dim_date;"
```

Expected: `rows = 5844`, `min_date = 2015-01-01`, `max_date = 2030-12-31`.

- [ ] **Step 7.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_date.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_date dimension"
```

---

## Task 8: dim_ward

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_ward.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 8.1: Append dim_ward entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_ward
    description: >
      Vietnamese 2025 ward / commune / township dimension. One row per ma_xa
      (post-2025 reorganisation: districts removed, wards report directly to
      provinces). Includes population/area measures and the EPSG:4326 polygon
      geometry used for spatial joins by dim_division_area, fact_place, and
      fact_land_feature. Source: stg_vn_gis__new_ward.
    columns:
      - name: sk_ward
        description: Surrogate key — generate_surrogate_key([ma_xa])
        data_tests:
          - not_null
          - unique
      - name: ma_xa
        description: Ward / commune code (natural key)
        data_tests:
          - not_null
          - unique
      - name: ten_xa
        description: Ward / commune name
        data_tests:
          - not_null
      - name: ma_tinh
        description: Province code
        data_tests:
          - not_null
      - name: ten_tinh
        description: Province name
      - name: loai
        description: Unit type in Vietnamese (Xã, Phường, Thị trấn)
      - name: loai_en
        description: Unit type in English (Commune, Ward, Township)
      - name: dtich_km2
        description: Area in km²
      - name: dan_so
        description: Population
      - name: matdo_km2
        description: Population density (people per km²)
      - name: centroid_lat
        description: Centroid latitude (EPSG:4326)
      - name: centroid_long
        description: Centroid longitude (EPSG:4326)
      - name: geometry
        description: Ward boundary polygon (EPSG:4326), populated via ST_GeomFromGeoJSON
        data_tests:
          - not_null
```

- [ ] **Step 8.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_ward.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_vn_gis__new_ward') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ma_xa']) }}    AS sk_ward,
    ma_xa,
    ten_xa,
    ma_tinh,
    ten_tinh,
    loai,
    loai_en,
    dtich_km2,
    dan_so,
    matdo_km2,
    lat                                                   AS centroid_lat,
    long                                                  AS centroid_long,
    ST_GeomFromGeoJSON(geometry_json)::geometry           AS geometry
FROM source
```

- [ ] **Step 8.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_ward'
```

Expected:
- Build OK.
- Tests PASS: `not_null` + `unique` on `sk_ward` and `ma_xa`; `not_null` on `ten_xa`, `ma_tinh`, `geometry`.
- Row count matches `stg_vn_gis__new_ward`:

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS dim_rows, (SELECT COUNT(*) FROM stg.stg_vn_gis__new_ward) AS stg_rows FROM bdh.dim_ward;"
```

Expected: `dim_rows = stg_rows`.

- [ ] **Step 8.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_ward.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_ward dimension"
```

---

## Task 9: dim_division_area (with spatial join)

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_division_area.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 9.1: Append dim_division_area entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_division_area
    description: >
      Overture division_area dimension for Vietnam. One row per division_area
      with the ma_xa it overlaps most (largest geodesic intersection area via
      DuckDB ST_Area_Spheroid). This is the spatial bridge era5 weather uses to
      reach Vietnamese ward boundaries. division_areas with no ward overlap are
      excluded. Source: stg_overture_maps__division_area + dim_ward.
    columns:
      - name: sk_division_area
        description: Surrogate key — generate_surrogate_key([division_area_id])
        data_tests:
          - not_null
          - unique
      - name: division_area_id
        description: Overture division_area id (natural key)
        data_tests:
          - not_null
          - unique
      - name: subtype
        description: Division subtype
      - name: location_name
        description: Primary name
      - name: country
        description: ISO country code (always 'VN')
      - name: region
        description: ISO region code
      - name: centroid_lat
        description: Centroid latitude
      - name: centroid_long
        description: Centroid longitude
      - name: geometry
        description: Division polygon (EPSG:4326)
      - name: sk_ward
        description: FK to dim_ward — the largest-overlap winner
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_ward')
              field: sk_ward
      - name: ma_xa
        description: Denormalised natural key of the assigned ward
        data_tests:
          - not_null
      - name: area_overlap_km2
        description: Geodesic km² of intersection with the assigned ward (diagnostic)
      - name: area_overlap_ratio
        description: area_overlap_km2 / total_division_km2 (0–1, diagnostic)
```

- [ ] **Step 9.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_division_area.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

/*
    For each Overture division_area, find the dim_ward with the largest geodesic
    overlap (ST_Area_Spheroid via DuckDB spatial). Returns one row per division
    that intersects at least one ward.
*/

WITH division AS (
    SELECT
        id            AS division_area_id,
        subtype,
        location_name,
        country,
        region,
        latitude      AS centroid_lat,
        longitude     AS centroid_long,
        geometry
    FROM {{ ref('stg_overture_maps__division_area') }}
),

wards AS (
    SELECT
        sk_ward,
        ma_xa,
        geometry
    FROM {{ ref('dim_ward') }}
),

overlaps AS (
    SELECT
        d.division_area_id,
        w.sk_ward,
        w.ma_xa,
        ST_Area_Spheroid(ST_Intersection(d.geometry, w.geometry)) / 1e6 AS overlap_km2,
        ST_Area_Spheroid(d.geometry)                              / 1e6 AS division_total_km2
    FROM division d
    JOIN wards w
      ON ST_Intersects(d.geometry, w.geometry)
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY division_area_id
            ORDER BY overlap_km2 DESC
        ) AS rn
    FROM overlaps
),

assigned AS (
    SELECT
        division_area_id,
        sk_ward,
        ma_xa,
        overlap_km2                                       AS area_overlap_km2,
        overlap_km2 / NULLIF(division_total_km2, 0)       AS area_overlap_ratio
    FROM ranked
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['d.division_area_id']) }}  AS sk_division_area,
    d.division_area_id,
    d.subtype,
    d.location_name,
    d.country,
    d.region,
    d.centroid_lat,
    d.centroid_long,
    d.geometry,
    a.sk_ward,
    a.ma_xa,
    a.area_overlap_km2,
    a.area_overlap_ratio
FROM division d
JOIN assigned a USING (division_area_id)
```

- [ ] **Step 9.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_division_area'
```

Expected:
- Build OK (this is the most expensive build of the plan — every division × every ward overlap).
- All tests PASS, including `relationships` from `sk_ward` to `dim_ward`.

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS rows, COUNT(DISTINCT sk_ward) AS distinct_wards FROM bdh.dim_division_area;"
```

Expected: `rows > 0` and `distinct_wards > 0`.

- [ ] **Step 9.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_division_area.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_division_area with spatial join to dim_ward"
```

---

## Task 10: dim_place_category

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_place_category.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 10.1: Append dim_place_category entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_place_category
    description: >
      Place category dimension. One row per distinct primary_category seen in
      stg_overture_maps__places. category_group is derived as the first
      dot-delimited segment (e.g. 'eat_and_drink' from 'eat_and_drink.restaurant').
    columns:
      - name: sk_place_category
        description: Surrogate key — generate_surrogate_key([primary_category])
        data_tests:
          - not_null
          - unique
      - name: primary_category
        description: Overture primary_category (natural key)
        data_tests:
          - not_null
          - unique
      - name: category_group
        description: First dot-delimited segment of primary_category
```

- [ ] **Step 10.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_place_category.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT DISTINCT primary_category
    FROM {{ ref('stg_overture_maps__places') }}
    WHERE primary_category IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['primary_category']) }}  AS sk_place_category,
    primary_category,
    SPLIT_PART(primary_category, '.', 1)                          AS category_group
FROM source
```

- [ ] **Step 10.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_place_category'
```

Expected: Build OK, all tests PASS.

- [ ] **Step 10.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_place_category.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_place_category dimension"
```

---

## Task 11: dim_land_class

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_land_class.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`

- [ ] **Step 11.1: Append dim_land_class entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml`:

```yaml

  - name: dim_land_class
    description: >
      Land class dimension. One row per distinct (subtype, class) seen in
      stg_overture_maps__base. e.g. ('physical', 'forest'), ('landuse', 'national_park').
    columns:
      - name: sk_land_class
        description: Surrogate key — generate_surrogate_key([subtype, class])
        data_tests:
          - not_null
          - unique
      - name: subtype
        description: Overture land subtype (e.g., physical, landuse)
        data_tests:
          - not_null
      - name: class
        description: Overture land class (e.g., forest, national_park)
        data_tests:
          - not_null
```

- [ ] **Step 11.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_land_class.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT DISTINCT subtype, class
    FROM {{ ref('stg_overture_maps__base') }}
    WHERE subtype IS NOT NULL
      AND class   IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['subtype', 'class']) }}  AS sk_land_class,
    subtype,
    class
FROM source
```

- [ ] **Step 11.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select dim_land_class'
```

Expected: Build OK, all tests PASS.

- [ ] **Step 11.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/dim_land_class.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/dim/schema.yml
git commit -m "feat: add bdh dim_land_class dimension"
```

---

## Task 12: fact_era5_weather

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/era5_weather/fact_era5_weather.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/era5_weather/schema.yml`

- [ ] **Step 12.1: Create the schema.yml**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/era5_weather/schema.yml`:

```yaml
version: 2

models:
  - name: fact_era5_weather
    description: >
      Daily ERA5-Land weather fact, one row per (observation_date, division_area).
      sk_ward is denormalised via dim_division_area to enable fast ward-level
      analytics without re-doing a spatial join. Rows whose location_id is not
      present in dim_division_area are excluded (which means it had no ward
      overlap — see dim_division_area filter).
    columns:
      - name: sk_era5_weather
        description: Surrogate key — generate_surrogate_key([observation_date, location_id])
        data_tests:
          - not_null
          - unique
      - name: sk_date
        description: FK to dim_date
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: sk_date
      - name: sk_division_area
        description: FK to dim_division_area
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_division_area')
              field: sk_division_area
      - name: sk_ward
        description: FK to dim_ward (denormalised from dim_division_area)
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_ward')
              field: sk_ward
      - name: observation_date
        description: Date of the ERA5-Land daily observation
        data_tests:
          - not_null
      - name: division_area_id
        description: Overture division_area id (= stg location_id)
        data_tests:
          - not_null
      - name: avg_temp_c
        description: Mean 2m air temperature for the locality polygon (°C)
      - name: max_temp_c
        description: Maximum 2m air temperature for the locality polygon (°C)
      - name: precip_mm
        description: Total precipitation (mm); NULL when no rain event
      - name: wind_speed_ms
        description: Wind speed at 10m (m/s)
      - name: soil_moisture
        description: Volumetric soil water in layer 1 (m³/m³)
```

- [ ] **Step 12.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/era5_weather/fact_era5_weather.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH weather AS (
    SELECT
        observation_date,
        location_id      AS division_area_id,
        avg_temp_c,
        max_temp_c,
        precip_mm,
        wind_speed_ms,
        soil_moisture
    FROM {{ ref('stg_era5_weather__locality') }}
),

division AS (
    SELECT
        division_area_id,
        sk_division_area,
        sk_ward
    FROM {{ ref('dim_division_area') }}
),

date_dim AS (
    SELECT
        date_day,
        sk_date
    FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['w.observation_date', 'w.division_area_id']) }} AS sk_era5_weather,
    dt.sk_date,
    d.sk_division_area,
    d.sk_ward,
    w.observation_date,
    w.division_area_id,
    w.avg_temp_c,
    w.max_temp_c,
    w.precip_mm,
    w.wind_speed_ms,
    w.soil_moisture
FROM weather  w
JOIN division d ON d.division_area_id = w.division_area_id
JOIN date_dim dt ON dt.date_day        = w.observation_date
```

- [ ] **Step 12.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select fact_era5_weather'
```

Expected:
- Build OK.
- All schema.yml tests PASS, including all three `relationships` tests.
- Row count check (no orphan weather rows that survived the JOINs):

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS fact_rows,
          (SELECT COUNT(*) FROM stg.stg_era5_weather__locality
            WHERE location_id IN (SELECT division_area_id FROM bdh.dim_division_area)
              AND observation_date BETWEEN '2015-01-01' AND '2030-12-31') AS expected_rows
   FROM bdh.fact_era5_weather;"
```

Expected: `fact_rows = expected_rows`.

- [ ] **Step 12.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/era5_weather/
git commit -m "feat: add bdh fact_era5_weather"
```

---

## Task 13: fact_place

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_place.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml`

- [ ] **Step 13.1: Create the schema.yml**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml`:

```yaml
version: 2

models:
  - name: fact_place
    description: >
      Overture places (POIs) fact. One row per place. sk_ward is assigned via
      point-in-polygon (ST_Within) — places outside any ward polygon get
      sk_ward = NULL (e.g., points just outside Vietnam coastlines or
      misgeocoded entries). sk_date_ingested derives from the (year, month, day)
      ingestion partition.
    columns:
      - name: sk_place
        description: Surrogate key — generate_surrogate_key([place_id])
        data_tests:
          - not_null
          - unique
      - name: sk_ward
        description: FK to dim_ward (nullable)
        data_tests:
          - relationships:
              to: ref('dim_ward')
              field: sk_ward
              where: "sk_ward IS NOT NULL"
      - name: sk_place_category
        description: FK to dim_place_category
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_place_category')
              field: sk_place_category
      - name: sk_date_ingested
        description: FK to dim_date — the ingestion date derived from year/month/day partition
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: sk_date
      - name: place_id
        description: Overture place id (natural key)
        data_tests:
          - not_null
          - unique
      - name: place_name
        description: Place primary name
      - name: address
        description: Freeform address
      - name: phone
        description: Primary phone number
      - name: social_link
        description: Primary social media URL
      - name: latitude
        description: Place latitude
      - name: longitude
        description: Place longitude
      - name: geometry
        description: Place point geometry (EPSG:4326)
```

- [ ] **Step 13.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_place.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH places AS (
    SELECT
        id              AS place_id,
        place_name,
        primary_category,
        address,
        phone,
        social_link,
        latitude,
        longitude,
        geometry,
        year,
        month,
        day
    FROM {{ ref('stg_overture_maps__places') }}
),

wards AS (
    SELECT sk_ward, geometry FROM {{ ref('dim_ward') }}
),

categories AS (
    SELECT sk_place_category, primary_category FROM {{ ref('dim_place_category') }}
),

date_dim AS (
    SELECT sk_date, date_day FROM {{ ref('dim_date') }}
),

place_with_ward AS (
    SELECT
        p.*,
        w.sk_ward
    FROM places p
    LEFT JOIN wards w
      ON ST_Within(p.geometry, w.geometry)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.place_id']) }}                       AS sk_place,
    p.sk_ward,
    c.sk_place_category,
    dt.sk_date                                                                    AS sk_date_ingested,
    p.place_id,
    p.place_name,
    p.address,
    p.phone,
    p.social_link,
    p.latitude,
    p.longitude,
    p.geometry
FROM place_with_ward p
JOIN categories c ON c.primary_category = p.primary_category
JOIN date_dim   dt ON dt.date_day       = MAKE_DATE(p.year, p.month, p.day)
```

- [ ] **Step 13.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select fact_place'
```

Expected:
- Build OK.
- All tests PASS, including `relationships` (with `where: "sk_ward IS NOT NULL"` for the nullable FK).

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS rows,
          COUNT(sk_ward) AS rows_with_ward,
          COUNT(*) - COUNT(sk_ward) AS rows_without_ward
   FROM bdh.fact_place;"
```

Expected: `rows > 0`. `rows_without_ward` should be small relative to `rows_with_ward` (places in Vietnam should mostly fall in some ward).

- [ ] **Step 13.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_place.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml
git commit -m "feat: add bdh fact_place with point-in-polygon ward mapping"
```

---

## Task 14: fact_land_feature

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_land_feature.sql`
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml`

- [ ] **Step 14.1: Append fact_land_feature entry to schema.yml**

Append to `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml`:

```yaml

  - name: fact_land_feature
    description: >
      Land feature × ward intersection fact. One row per (land_feature_id, ma_xa)
      pair where ST_Intersects is true. area_km2_in_ward is the geodesic km² of
      the intersection (DuckDB ST_Area_Spheroid). Lets you answer "how many km²
      of forest in ward X". sk_date_ingested derives from the ingestion partition.
    columns:
      - name: sk_land_feature
        description: Surrogate key — generate_surrogate_key([land_feature_id, ma_xa])
        data_tests:
          - not_null
          - unique
      - name: sk_ward
        description: FK to dim_ward
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_ward')
              field: sk_ward
      - name: sk_land_class
        description: FK to dim_land_class
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_land_class')
              field: sk_land_class
      - name: sk_date_ingested
        description: FK to dim_date — the ingestion date derived from year/month/day partition
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: sk_date
      - name: land_feature_id
        description: Overture land feature id (NOT unique here — same id appears once per overlapping ward)
        data_tests:
          - not_null
      - name: land_name
        description: Land feature primary name
      - name: area_km2_in_ward
        description: Geodesic km² of intersection between feature and ward (ST_Area_Spheroid)
      - name: area_km2_total
        description: Geodesic km² of the entire feature polygon
      - name: overlap_ratio
        description: area_km2_in_ward / area_km2_total (0–1)
```

- [ ] **Step 14.2: Create the SQL model**

Write `application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_land_feature.sql`:

```sql
{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

/*
    For each Overture land feature × Vietnam ward intersection, emit one row
    with the geodesic intersection area. fact_land_feature is denormalized at
    (feature × ward) grain so analytics can roll up either by ward (sum
    area_km2_in_ward) or by feature (max overlap_ratio).
*/

WITH land AS (
    SELECT
        id              AS land_feature_id,
        subtype,
        class,
        land_name,
        geometry,
        year,
        month,
        day
    FROM {{ ref('stg_overture_maps__base') }}
),

wards AS (
    SELECT sk_ward, ma_xa, geometry FROM {{ ref('dim_ward') }}
),

land_class AS (
    SELECT sk_land_class, subtype, class FROM {{ ref('dim_land_class') }}
),

date_dim AS (
    SELECT sk_date, date_day FROM {{ ref('dim_date') }}
),

intersected AS (
    SELECT
        l.land_feature_id,
        l.subtype,
        l.class,
        l.land_name,
        l.year,
        l.month,
        l.day,
        w.sk_ward,
        w.ma_xa,
        ST_Area_Spheroid(ST_Intersection(l.geometry, w.geometry)) / 1e6 AS area_km2_in_ward,
        ST_Area_Spheroid(l.geometry)                              / 1e6 AS area_km2_total
    FROM land l
    JOIN wards w ON ST_Intersects(l.geometry, w.geometry)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['i.land_feature_id', 'i.ma_xa']) }}    AS sk_land_feature,
    i.sk_ward,
    lc.sk_land_class,
    dt.sk_date                                                                   AS sk_date_ingested,
    i.land_feature_id,
    i.land_name,
    i.area_km2_in_ward,
    i.area_km2_total,
    i.area_km2_in_ward / NULLIF(i.area_km2_total, 0)                             AS overlap_ratio
FROM intersected i
JOIN land_class lc ON lc.subtype = i.subtype AND lc.class = i.class
JOIN date_dim   dt ON dt.date_day = MAKE_DATE(i.year, i.month, i.day)
```

- [ ] **Step 14.3: Build and run tests**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select fact_land_feature'
```

Expected:
- Build OK (this is the second-most-expensive build — every land feature × every overlapping ward).
- All tests PASS.

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c \
  "SELECT COUNT(*) AS rows,
          COUNT(DISTINCT land_feature_id) AS distinct_features,
          COUNT(DISTINCT sk_ward)         AS distinct_wards,
          ROUND(SUM(area_km2_in_ward)::numeric, 2) AS total_overlap_km2
   FROM bdh.fact_land_feature;"
```

Expected: `rows >= distinct_features` (most features overlap >1 ward); `total_overlap_km2` looks plausible for Vietnam (< 350,000).

- [ ] **Step 14.4: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/fact_land_feature.sql \
        application/mage_ai/mds_demo/dbt/data_warehouse/models/bdh/overture_maps/schema.yml
git commit -m "feat: add bdh fact_land_feature with feature x ward overlap"
```

---

## Task 15: Final end-to-end build

**Files:** none (full-build verification)

- [ ] **Step 15.1: Run the entire bdh layer top-to-bottom**

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select tag:bdh+ +bdh.*'
```

If `tag:bdh` is not configured, use:

```bash
docker compose exec magic bash -lc \
  'cd /home/src/mds_demo/dbt/data_warehouse && dbt build --select +fact_era5_weather +fact_place +fact_land_feature'
```

Expected: every model builds, every schema.yml test passes. The `+` prefix pulls in all upstream dependencies, ensuring a coherent rebuild.

- [ ] **Step 15.2: Sanity-check a cross-fact query**

```bash
docker compose exec warehouse_db psql -U warehouse -d warehouse -c "
SELECT
    w.ten_xa,
    w.ten_tinh,
    ROUND(AVG(f.avg_temp_c)::numeric, 2)         AS avg_temp_c_2025,
    COUNT(DISTINCT p.sk_place)                   AS num_places,
    ROUND(SUM(lf.area_km2_in_ward)::numeric, 2)  AS land_km2_mapped
FROM bdh.dim_ward         w
LEFT JOIN bdh.fact_era5_weather  f  ON f.sk_ward  = w.sk_ward
                                    AND f.observation_date BETWEEN '2025-01-01' AND '2025-12-31'
LEFT JOIN bdh.fact_place         p  ON p.sk_ward  = w.sk_ward
LEFT JOIN bdh.fact_land_feature  lf ON lf.sk_ward = w.sk_ward
GROUP BY w.ten_xa, w.ten_tinh
ORDER BY num_places DESC NULLS LAST
LIMIT 10;
"
```

Expected: a 10-row result joining all three facts via `dim_ward`, with non-null values for at least the `num_places` column. This proves the star schema joins end-to-end.

- [ ] **Step 15.3: No commit needed**

This task is verification only.

---

## Self-review checklist (already performed)

- ✅ Spec coverage:
  - division_area ingestion → Task 1–4
  - dim_year / dim_month / dim_date → Tasks 5–7
  - dim_ward / dim_division_area (spatial join) → Tasks 8–9
  - dim_place_category / dim_land_class → Tasks 10–11
  - fact_era5_weather → Task 12
  - fact_place (point-in-polygon) → Task 13
  - fact_land_feature (polygon overlap) → Task 14
  - End-to-end build verification → Task 15
- ✅ No `TBD` / `TODO` / placeholder text in any task — every step has the actual SQL/YAML/Python.
- ✅ Type / signature consistency: `sk_ward` references match across `dim_ward`, `dim_division_area`, `fact_era5_weather`, `fact_place`, `fact_land_feature`. `sk_date` references match across `dim_date`, `dim_month`, `fact_era5_weather`, `fact_place`, `fact_land_feature`. `division_area_id` is consistent. `MAKE_DATE(year, month, day)` is used for ingestion-date FK derivation in both `fact_place` and `fact_land_feature`.
- ✅ All `relationships` tests reference fields that exist on the parent dim.
- ✅ Each `dbt_utils.generate_surrogate_key` call lists at least one column.
