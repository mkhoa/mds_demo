# Overture Maps Ingestion Pipeline — Design Spec

**Date:** 2026-04-26  
**Status:** Approved  
**Scope:** Single Mage pipeline loading Overture Maps `base` (land features) and `places` (POIs) themes for Vietnam from BigQuery public dataset into the raw/stg dbt layers.

---

## 1. Architecture

One Mage pipeline (`overture_maps_ingestion`) with two independent loader blocks — one per theme — feeding into two instances of the shared `write2landing_block` exporter. Both branches run in parallel.

```
overture_maps_ingestion
  ├── load_overture_base   ──→ write2landing_base   → MinIO overture_maps_base
  └── load_overture_places ──→ write2landing_places → MinIO overture_maps_places
```

---

## 2. Data Source

**BigQuery public dataset:** `bigquery-public-data.overture_maps.*`

Both loaders use Mage's built-in BigQuery connector (`mage_ai.io.bigquery.BigQuery`) with credentials from `io_config.yaml`. No `overturemaps` Python library required.

**Vietnam spatial filter:** applied via `ST_INTERSECTS` with the country boundary from `bigquery-public-data.overture_maps.division_area` (country = 'VN', subtype = 'country').

---

## 3. BigQuery Queries

### Places (POIs)

```sql
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
```

### Base (Land Features)

```sql
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
```

Notes:
- `LIMIT 5000` removed for production; full Vietnam dataset loaded on each run.
- `geometry` column converted to WKT string via `ST_ASTEXT()` for parquet storage compatibility and consistent downstream handling.

---

## 4. Data Loaders

**Files:**
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_base.py`
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_places.py`

**Pattern:**
```python
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
from mage_ai.data_preparation.repo_manager import get_repo_path

@data_loader
def load_data(*args, **kwargs):
    query = """..."""  # theme-specific BigQuery SQL
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    loader = BigQuery.with_config(ConfigFileLoader(config_path, 'default'))
    return loader.load(query)
```

**Output columns:**

| Theme | Columns |
|---|---|
| `places` | `id, place_name, primary_category, address, phone, social_link, latitude, longitude, geometry_wkt` |
| `base` | `id, subtype, class, land_name, latitude, longitude, geometry_wkt` |

`geometry_wkt` is a WKT string — converted to PostGIS geometry in dbt stg via `ST_GeomFromText()`.

---

## 5. Pipeline Variables

```yaml
variables:
  bucket_name: dwhfilesystem
  file_format: parquet
```

`ingestion_data` is overridden per-block via `block_settings` (see Section 5b).

---

## 5b. Pipeline Block Structure (Mage metadata.yaml)

```yaml
blocks:
  - name: overture_maps/load_overture_base
    type: data_loader
    upstream_blocks: []
    downstream_blocks: [write2landing_base]

  - name: generic_components/write2landing_block
    uuid: write2landing_base
    block_settings:
      variables:
        ingestion_data: overture_maps_base
    upstream_blocks: [overture_maps/load_overture_base]
    downstream_blocks: []

  - name: overture_maps/load_overture_places
    type: data_loader
    upstream_blocks: []
    downstream_blocks: [write2landing_places]

  - name: generic_components/write2landing_block
    uuid: write2landing_places
    block_settings:
      variables:
        ingestion_data: overture_maps_places
    upstream_blocks: [overture_maps/load_overture_places]
    downstream_blocks: []
```

Both branches run in parallel. The `write2landing_block` file is reused unchanged.

---

## 6. MinIO Landing Paths

```
s3://dwhfilesystem/landing_area/overture_maps_base/
  year=YYYY/month=M/day=D/overture_maps_base_HHMMSS.parquet

s3://dwhfilesystem/landing_area/overture_maps_places/
  year=YYYY/month=M/day=D/overture_maps_places_HHMMSS.parquet
```

Hive-style partitioning handled by `write2landing_block` (unchanged).

---

## 7. dbt Models

### Raw views (external, no data copy)

| File | Macro |
|---|---|
| `models/raw/overture_maps/raw_overture_maps__base.sql` | `{{ overture_maps_base_typed_scan() }}` |
| `models/raw/overture_maps/raw_overture_maps__places.sql` | `{{ overture_maps_places_typed_scan() }}` |

Both: `materialized: view`, `schema: raw`.

### Staging models (incremental, typed, cleaned)

| File | Unique key | Partition cols |
|---|---|---|
| `models/stg/overture_maps/stg_overture_maps__base.sql` | `['id']` | `year, month, day` |
| `models/stg/overture_maps/stg_overture_maps__places.sql` | `['id']` | `year, month, day` |

Both: `materialized: incremental`, `incremental_strategy: delete+insert`, uses `get_partition_watermark()` macro.

Geometry promoted to PostGIS: `ST_GeomFromText(geometry_wkt)::geometry AS geometry`.

### Macros (added to `macros/parquet_scans.sql`)

**Base scan:**
```sql
{% macro overture_maps_base_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR          AS id,
    r['subtype']::VARCHAR     AS subtype,
    r['class']::VARCHAR       AS class,
    r['land_name']::VARCHAR   AS land_name,
    r['latitude']::DOUBLE     AS latitude,
    r['longitude']::DOUBLE    AS longitude,
    r['geometry_wkt']::VARCHAR AS geometry_wkt,
    r['year']::INTEGER        AS year,
    r['month']::INTEGER       AS month,
    r['day']::INTEGER         AS day
FROM duckdb.query($duckdb$
    SELECT * FROM read_parquet(
        's3://dwhfilesystem/landing_area/overture_maps_base/**/*.parquet',
        hive_partitioning = true,
        filename = true
    )
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
{% endmacro %}
```

**Places scan:**
```sql
{% macro overture_maps_places_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                AS id,
    r['place_name']::VARCHAR        AS place_name,
    r['primary_category']::VARCHAR  AS primary_category,
    r['address']::VARCHAR           AS address,
    r['phone']::VARCHAR             AS phone,
    r['social_link']::VARCHAR       AS social_link,
    r['latitude']::DOUBLE           AS latitude,
    r['longitude']::DOUBLE          AS longitude,
    r['geometry_wkt']::VARCHAR      AS geometry_wkt,
    r['year']::INTEGER              AS year,
    r['month']::INTEGER             AS month,
    r['day']::INTEGER               AS day
FROM duckdb.query($duckdb$
    SELECT * FROM read_parquet(
        's3://dwhfilesystem/landing_area/overture_maps_places/**/*.parquet',
        hive_partitioning = true,
        filename = true
    )
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
{% endmacro %}
```

### Schema YAMLs

`schema.yml` added for both raw and stg models with `not_null` and `unique` tests on `id`.

---

## 8. Dependencies

Add to `application/mage_ai/requirements.txt` (already present from prior update):
```
google-cloud-bigquery
db-dtypes
```

`shapely`, `pyarrow`, and `overturemaps` are **not** required for this approach.

BigQuery credentials must be configured in `io_config.yaml` under `GOOGLE_SERVICE_ACC_CREDS` or equivalent.

---

## 9. Data Flow Summary

```
BigQuery public dataset (bigquery-public-data.overture_maps.*)
        ↓  ST_INTERSECTS with Vietnam boundary (server-side spatial filter)
    Mage BigQuery loader → pandas DataFrame
        ↓  geometry as WKT string (ST_ASTEXT)
    write2landing_block
        ↓
s3://dwhfilesystem/landing_area/overture_maps_{theme}/year=Y/month=M/day=D/
        ↓  pg_duckdb external view + typed scan macro
    raw.overture_maps__base / raw.overture_maps__places
        ↓  dbt incremental + partition watermark + ST_GeomFromText()
    stg.overture_maps__base / stg.overture_maps__places
```

---

## 10. Out of Scope

- `bdh` / `adl` layer models (to be added when downstream use case is defined)
- Mage schedule trigger (to be configured separately)
- Other Overture themes (buildings, transportation, divisions, addresses)
- `land_use`, `land_cover`, `water`, `infrastructure` subtables from the base theme (only `land` table queried)
