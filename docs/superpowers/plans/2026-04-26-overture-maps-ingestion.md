# Overture Maps Ingestion Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two Mage pipelines that load Overture Maps `base` (land features) and `places` (POIs) for Vietnam from BigQuery public dataset into MinIO, then expose them via dbt raw views and incremental staging tables in the warehouse.

**Architecture:** Two independent pipelines (`overture_maps_base_ingestion`, `overture_maps_places_ingestion`) each follow the standard `loader → generic_components/write2landing_block` pattern. dbt raw views read from MinIO via pg_duckdb typed scan macros; stg incremental models clean and promote geometry to PostGIS using `ST_GeomFromText()`.

**Tech Stack:** Mage AI (BigQuery connector), google-cloud-bigquery, dbt (PostgreSQL adapter, pg_duckdb), MinIO (S3), PostGIS, DuckDB spatial

---

## File Map

**New files:**
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_base.py`
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_places.py`
- `application/mage_ai/mds_demo/pipelines/overture_maps_base_ingestion/metadata.yaml`
- `application/mage_ai/mds_demo/pipelines/overture_maps_places_ingestion/metadata.yaml`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__base.sql`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__places.sql`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/schema.yml`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__base.sql`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__places.sql`
- `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/schema.yml`

**Modified files:**
- `application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql` — append two new typed scan macros

---

## Task 1: Add Overture Maps scan macros to parquet_scans.sql

**Files:**
- Modify: `application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql`

- [ ] **Step 1: Append both typed scan macros to the end of parquet_scans.sql**

Append after the last line (line 165) of the existing file:

```sql


-- ──────────────────────────────────────────────────────────────────────────────
-- Overture Maps — Base (land features)
-- Layout: s3://dwhfilesystem/landing_area/overture_maps_base/year=Y/month=M/day=D/*.parquet
-- Source: bigquery-public-data.overture_maps.land, filtered to Vietnam
-- ──────────────────────────────────────────────────────────────────────────────
{% macro overture_maps_base_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                AS id,
    r['subtype']::VARCHAR           AS subtype,
    r['class']::VARCHAR             AS class,
    r['land_name']::VARCHAR         AS land_name,
    r['latitude']::DOUBLE PRECISION AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR      AS geometry_wkt,
    r['year']::INTEGER              AS year,
    r['month']::INTEGER             AS month,
    r['day']::INTEGER               AS day
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/overture_maps_base/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- Overture Maps — Places (POIs)
-- Layout: s3://dwhfilesystem/landing_area/overture_maps_places/year=Y/month=M/day=D/*.parquet
-- Source: bigquery-public-data.overture_maps.place, filtered to Vietnam
-- ──────────────────────────────────────────────────────────────────────────────
{% macro overture_maps_places_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                AS id,
    r['place_name']::VARCHAR        AS place_name,
    r['primary_category']::VARCHAR  AS primary_category,
    r['address']::VARCHAR           AS address,
    r['phone']::VARCHAR             AS phone,
    r['social_link']::VARCHAR       AS social_link,
    r['latitude']::DOUBLE PRECISION AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR      AS geometry_wkt,
    r['year']::INTEGER              AS year,
    r['month']::INTEGER             AS month,
    r['day']::INTEGER               AS day
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/overture_maps_places/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
{% endmacro %}
```

- [ ] **Step 2: Verify dbt can parse the macro file**

```bash
docker compose exec mage_ai bash -c "cd /home/src/mage_ai/mds_demo/dbt/data_warehouse && dbt compile --select raw_nyc_taxi__trips 2>&1 | tail -5"
```

Expected: `Completed successfully` (existing model still compiles, confirming macro file has no syntax error).

- [ ] **Step 3: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/macros/parquet_scans.sql
git commit -m "feat: add overture_maps base and places typed scan macros"
```

---

## Task 2: Create dbt raw models and schema for overture_maps

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__base.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/raw_overture_maps__places.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/schema.yml`

- [ ] **Step 1: Create raw_overture_maps__base.sql**

```sql
{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_base_typed_scan() }}
```

- [ ] **Step 2: Create raw_overture_maps__places.sql**

```sql
{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

{{ overture_maps_places_typed_scan() }}
```

- [ ] **Step 3: Create raw/overture_maps/schema.yml**

```yaml
version: 2

models:
  - name: raw_overture_maps__base
    description: >
      External view reading Overture Maps land features for Vietnam from the MinIO landing
      area via pg_duckdb. Sourced from bigquery-public-data.overture_maps.land.
    columns:
      - name: id
        description: Overture Maps globally unique feature identifier
      - name: subtype
        description: High-level land category (e.g., physical, landuse)
      - name: class
        description: Specific land class (e.g., forest, grass, sand, national_park)
      - name: land_name
        description: Primary name of the land feature
      - name: latitude
        description: Latitude of the feature centroid
      - name: longitude
        description: Longitude of the feature centroid
      - name: geometry_wkt
        description: Full polygon geometry as WKT string
      - name: year
        description: Ingest year (Hive partition column)
      - name: month
        description: Ingest month (Hive partition column)
      - name: day
        description: Ingest day (Hive partition column)

  - name: raw_overture_maps__places
    description: >
      External view reading Overture Maps places (POIs) for Vietnam from the MinIO landing
      area via pg_duckdb. Sourced from bigquery-public-data.overture_maps.place.
    columns:
      - name: id
        description: Overture Maps globally unique feature identifier
      - name: place_name
        description: Primary name of the place
      - name: primary_category
        description: Primary Overture Maps category
      - name: address
        description: Freeform address string (first address entry)
      - name: phone
        description: Primary phone number
      - name: social_link
        description: Primary social media URL
      - name: latitude
        description: Latitude of the place (point geometry)
      - name: longitude
        description: Longitude of the place (point geometry)
      - name: geometry_wkt
        description: Point geometry as WKT string
      - name: year
        description: Ingest year (Hive partition column)
      - name: month
        description: Ingest month (Hive partition column)
      - name: day
        description: Ingest day (Hive partition column)
```

- [ ] **Step 4: Verify dbt compiles both raw models**

```bash
docker compose exec mage_ai bash -c "cd /home/src/mage_ai/mds_demo/dbt/data_warehouse && dbt compile --select raw_overture_maps__base raw_overture_maps__places 2>&1 | tail -5"
```

Expected: `Completed successfully` with 2 models compiled.

- [ ] **Step 5: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/raw/overture_maps/
git commit -m "feat: add dbt raw views for overture_maps base and places"
```

---

## Task 3: Create dbt stg models and schema for overture_maps

**Files:**
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__base.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/stg_overture_maps__places.sql`
- Create: `application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/schema.yml`

- [ ] **Step 1: Create stg_overture_maps__base.sql**

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
      - Incremental:   calls overture_maps_base_typed_scan() with a partition filter
                       built from max (year, month, day) already in this table. The filter
                       is embedded inside duckdb.query() so DuckDB applies Hive partition
                       pruning. delete+insert on id handles dedup across re-runs.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ overture_maps_base_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_overture_maps__base') }}
    {% endif %}

),

cleaned AS (
    SELECT
        id,
        subtype,
        class,
        land_name,
        latitude,
        longitude,
        ST_GeomFromText(geometry_wkt)::geometry     AS geometry,
        year,
        month,
        day
    FROM source
    WHERE id           IS NOT NULL
      AND geometry_wkt IS NOT NULL
)

SELECT * FROM cleaned
```

- [ ] **Step 2: Create stg_overture_maps__places.sql**

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
      - Incremental:   calls overture_maps_places_typed_scan() with a partition filter
                       built from max (year, month, day) already in this table. The filter
                       is embedded inside duckdb.query() so DuckDB applies Hive partition
                       pruning. delete+insert on id handles dedup across re-runs.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ overture_maps_places_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_overture_maps__places') }}
    {% endif %}

),

cleaned AS (
    SELECT
        id,
        place_name,
        primary_category,
        address,
        phone,
        social_link,
        latitude,
        longitude,
        ST_GeomFromText(geometry_wkt)::geometry     AS geometry,
        year,
        month,
        day
    FROM source
    WHERE id        IS NOT NULL
      AND latitude  IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT * FROM cleaned
```

- [ ] **Step 3: Create stg/overture_maps/schema.yml**

```yaml
version: 2

models:
  - name: stg_overture_maps__base
    description: >
      Bronze staging model for Overture Maps land features in Vietnam.
      Geometry promoted to PostGIS via ST_GeomFromText(). Rows with null id or
      geometry filtered. Incrementally loaded using Hive partition watermark.
    columns:
      - name: id
        description: Overture Maps globally unique feature identifier
        data_tests:
          - not_null
          - unique
      - name: subtype
        description: High-level land category (e.g., physical, landuse)
      - name: class
        description: Specific land class (e.g., forest, grass, national_park)
      - name: land_name
        description: Primary name of the land feature
      - name: latitude
        description: Centroid latitude
      - name: longitude
        description: Centroid longitude
      - name: geometry
        description: PostGIS geometry of the land feature polygon
        data_tests:
          - not_null
      - name: year
        description: Ingest year (Hive partition column)
      - name: month
        description: Ingest month (Hive partition column)
      - name: day
        description: Ingest day (Hive partition column)

  - name: stg_overture_maps__places
    description: >
      Bronze staging model for Overture Maps places (POIs) in Vietnam.
      Geometry promoted to PostGIS via ST_GeomFromText(). Rows without
      coordinates filtered. Incrementally loaded using Hive partition watermark.
    columns:
      - name: id
        description: Overture Maps globally unique feature identifier
        data_tests:
          - not_null
          - unique
      - name: place_name
        description: Primary name of the place
      - name: primary_category
        description: Overture primary category
      - name: address
        description: Freeform address string (first entry)
      - name: phone
        description: Primary phone number
      - name: social_link
        description: Primary social media URL
      - name: latitude
        description: Latitude of the place
        data_tests:
          - not_null
      - name: longitude
        description: Longitude of the place
        data_tests:
          - not_null
      - name: geometry
        description: PostGIS point geometry
        data_tests:
          - not_null
      - name: year
        description: Ingest year (Hive partition column)
      - name: month
        description: Ingest month (Hive partition column)
      - name: day
        description: Ingest day (Hive partition column)
```

- [ ] **Step 4: Verify dbt compiles both stg models**

```bash
docker compose exec mage_ai bash -c "cd /home/src/mage_ai/mds_demo/dbt/data_warehouse && dbt compile --select stg_overture_maps__base stg_overture_maps__places 2>&1 | tail -5"
```

Expected: `Completed successfully` with 2 models compiled.

- [ ] **Step 5: Commit**

```bash
git add application/mage_ai/mds_demo/dbt/data_warehouse/models/stg/overture_maps/
git commit -m "feat: add dbt stg incremental models for overture_maps base and places"
```

---

## Task 4: Create Overture Maps base data loader

**Files:**
- Create: `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_base.py`

- [ ] **Step 1: Create the loader file**

```python
import pandas as pd
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps base (land features) for Vietnam from BigQuery public dataset.
    Uses ST_INTERSECTS with the Vietnam country boundary for spatial filtering.
    Geometry is serialised as WKT string for parquet storage.
    """
    from mage_ai.io.bigquery import BigQuery
    from mage_ai.io.config import ConfigFileLoader
    from mage_ai.data_preparation.repo_manager import get_repo_path

    query = """
WITH Vietnam_Boundary AS (
    SELECT geometry AS geom
    FROM `bigquery-public-data.overture_maps.division_area`
    WHERE country = 'VN' AND subtype = 'country'
)
SELECT
    l.id,
    l.subtype,
    l.class,
    l.names.primary                     AS land_name,
    ST_Y(ST_CENTROID(l.geometry))       AS latitude,
    ST_X(ST_CENTROID(l.geometry))       AS longitude,
    ST_ASTEXT(l.geometry)               AS geometry_wkt
FROM `bigquery-public-data.overture_maps.land` AS l
INNER JOIN Vietnam_Boundary AS vn ON ST_INTERSECTS(l.geometry, vn.geom)
"""

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    df = BigQuery.with_config(ConfigFileLoader(config_path, 'default')).load(query)
    print(f"Loaded {len(df):,} base land features for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No features returned — check BigQuery credentials and query'
    required = {'id', 'subtype', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found in base features'
    assert df['geometry_wkt'].notna().all(), 'Null geometry_wkt values found'
```

- [ ] **Step 2: Commit**

```bash
git add application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_base.py
git commit -m "feat: add Overture Maps base data loader (BigQuery)"
```

---

## Task 5: Create overture_maps_base_ingestion pipeline

**Files:**
- Create: `application/mage_ai/mds_demo/pipelines/overture_maps_base_ingestion/metadata.yaml`

- [ ] **Step 1: Create the pipeline directory and metadata.yaml**

```yaml
blocks:
- all_upstream_blocks_executed: true
  color: null
  configuration:
    file_source:
      path: data_loaders/overture_maps/load_overture_base.py
  downstream_blocks:
  - generic_components/write2landing_block
  executor_config: null
  executor_type: local_python
  has_callback: false
  language: python
  name: overture_maps/load_overture_base
  retry_config: null
  status: updated
  timeout: null
  type: data_loader
  upstream_blocks: []
  uuid: overture_maps/load_overture_base
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
  - overture_maps/load_overture_base
  uuid: generic_components/write2landing_block
cache_block_output_in_memory: false
callbacks: []
concurrency_config: {}
conditionals: []
created_at: null
data_integration: null
description: 'Load Overture Maps base (land features) for Vietnam from BigQuery public
  dataset. Spatial filter via ST_INTERSECTS with Vietnam country boundary. Writes
  to MinIO landing area as Hive-partitioned parquet.'
executor_config: {}
executor_count: 1
executor_type: null
extensions: {}
name: overture_maps_base_ingestion
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
uuid: overture_maps_base_ingestion
variables:
  bucket_name: dwhfilesystem
  file_format: parquet
  ingestion_data: overture_maps_base
variables_dir: /home/src/mage_data/mds_demo
widgets: []
```

- [ ] **Step 2: Commit**

```bash
git add application/mage_ai/mds_demo/pipelines/overture_maps_base_ingestion/
git commit -m "feat: add overture_maps_base_ingestion Mage pipeline"
```

---

## Task 6: Create Overture Maps places data loader

**Files:**
- Create: `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_places.py`

- [ ] **Step 1: Create the loader file**

```python
import pandas as pd
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Overture Maps places (POIs) for Vietnam from BigQuery public dataset.
    Uses ST_INTERSECTS with the Vietnam country boundary for spatial filtering.
    Geometry is serialised as WKT string for parquet storage.
    """
    from mage_ai.io.bigquery import BigQuery
    from mage_ai.io.config import ConfigFileLoader
    from mage_ai.data_preparation.repo_manager import get_repo_path

    query = """
WITH Vietnam_Boundary AS (
    SELECT geometry AS geom
    FROM `bigquery-public-data.overture_maps.division_area`
    WHERE country = 'VN' AND subtype = 'country'
)
SELECT
    p.id,
    p.names.primary                                     AS place_name,
    p.categories.primary                                AS primary_category,
    p.addresses.list[SAFE_OFFSET(0)].element.freeform   AS address,
    p.phones.list[SAFE_OFFSET(0)].element               AS phone,
    p.socials.list[SAFE_OFFSET(0)].element              AS social_link,
    ST_Y(p.geometry)                                    AS latitude,
    ST_X(p.geometry)                                    AS longitude,
    ST_ASTEXT(p.geometry)                               AS geometry_wkt
FROM `bigquery-public-data.overture_maps.place` AS p
INNER JOIN Vietnam_Boundary AS vn ON ST_INTERSECTS(p.geometry, vn.geom)
WHERE p.names.primary IS NOT NULL
"""

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    df = BigQuery.with_config(ConfigFileLoader(config_path, 'default')).load(query)
    print(f"Loaded {len(df):,} places for Vietnam.")
    return df


@test
def test_output(df, *args) -> None:
    assert df is not None, 'DataFrame is None'
    assert len(df) > 0, 'No places returned — check BigQuery credentials and query'
    required = {'id', 'place_name', 'latitude', 'longitude', 'geometry_wkt'}
    assert required.issubset(df.columns), f'Missing columns: {required - set(df.columns)}'
    assert df['id'].notna().all(), 'Null ids found in places'
    assert df['latitude'].notna().all(), 'Null latitude values found'
    assert df['longitude'].notna().all(), 'Null longitude values found'
```

- [ ] **Step 2: Commit**

```bash
git add application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_places.py
git commit -m "feat: add Overture Maps places data loader (BigQuery)"
```

---

## Task 7: Create overture_maps_places_ingestion pipeline

**Files:**
- Create: `application/mage_ai/mds_demo/pipelines/overture_maps_places_ingestion/metadata.yaml`

- [ ] **Step 1: Create the pipeline directory and metadata.yaml**

```yaml
blocks:
- all_upstream_blocks_executed: true
  color: null
  configuration:
    file_source:
      path: data_loaders/overture_maps/load_overture_places.py
  downstream_blocks:
  - generic_components/write2landing_block
  executor_config: null
  executor_type: local_python
  has_callback: false
  language: python
  name: overture_maps/load_overture_places
  retry_config: null
  status: updated
  timeout: null
  type: data_loader
  upstream_blocks: []
  uuid: overture_maps/load_overture_places
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
  - overture_maps/load_overture_places
  uuid: generic_components/write2landing_block
cache_block_output_in_memory: false
callbacks: []
concurrency_config: {}
conditionals: []
created_at: null
data_integration: null
description: 'Load Overture Maps places (POIs) for Vietnam from BigQuery public dataset.
  Spatial filter via ST_INTERSECTS with Vietnam country boundary. Writes to MinIO
  landing area as Hive-partitioned parquet.'
executor_config: {}
executor_count: 1
executor_type: null
extensions: {}
name: overture_maps_places_ingestion
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
uuid: overture_maps_places_ingestion
variables:
  bucket_name: dwhfilesystem
  file_format: parquet
  ingestion_data: overture_maps_places
variables_dir: /home/src/mage_data/mds_demo
widgets: []
```

- [ ] **Step 2: Commit**

```bash
git add application/mage_ai/mds_demo/pipelines/overture_maps_places_ingestion/
git commit -m "feat: add overture_maps_places_ingestion Mage pipeline"
```

---

## Task 8: End-to-end verification checklist

These steps require the stack to be running (`docker compose up -d`).

- [ ] **Step 1: Confirm BigQuery credentials are configured in io_config.yaml**

Check that `io_config.yaml` has a valid `GOOGLE_SERVICE_ACC_CREDS` (or `GOOGLE_APPLICATION_CREDENTIALS` env var is set in the Mage container). If not, add the service account JSON under the `default` profile:

```yaml
# io_config.yaml — default profile
default:
  # ... existing keys ...
  GOOGLE_SERVICE_ACC_CREDS: '{"type": "service_account", ...}'
```

- [ ] **Step 2: Run the base pipeline in Mage UI**

Open http://localhost:6789, navigate to `overture_maps_base_ingestion`, click Run. Confirm:
- Block `overture_maps/load_overture_base` completes with a row count logged
- Block `generic_components/write2landing_block` completes with no errors
- File appears in MinIO at `dwhfilesystem/landing_area/overture_maps_base/year=.../`

- [ ] **Step 3: Run the places pipeline in Mage UI**

Navigate to `overture_maps_places_ingestion`, click Run. Confirm same success pattern with file at `dwhfilesystem/landing_area/overture_maps_places/year=.../`.

- [ ] **Step 4: Run dbt models**

```bash
docker compose exec mage_ai bash -c "cd /home/src/mage_ai/mds_demo/dbt/data_warehouse && dbt run --select raw_overture_maps__base raw_overture_maps__places stg_overture_maps__base stg_overture_maps__places 2>&1 | tail -10"
```

Expected: 4 models pass (2 views created, 2 incremental tables created).

- [ ] **Step 5: Run dbt tests**

```bash
docker compose exec mage_ai bash -c "cd /home/src/mage_ai/mds_demo/dbt/data_warehouse && dbt test --select stg_overture_maps__base stg_overture_maps__places 2>&1 | tail -10"
```

Expected: All schema tests pass (`not_null` and `unique` on `id`; `not_null` on `latitude`, `longitude`, `geometry`).
