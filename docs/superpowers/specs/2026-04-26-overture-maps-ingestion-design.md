# Overture Maps Ingestion Pipeline — Design Spec

**Date:** 2026-04-26  
**Status:** Approved  
**Scope:** Single Mage pipeline loading Overture Maps `base` and `places` themes for Vietnam into the raw/stg dbt layers.

---

## 1. Architecture

One Mage pipeline (`overture_maps_ingestion`) with two independent loader blocks — one per theme — feeding into the shared `write2landing_block` exporter. Each theme is an independent DAG branch (parallel execution).

```
overture_maps_ingestion
  ├── load_overture_base  ──────────────┐
  │                                     ├── write2landing_block (×2 runs)
  └── load_overture_places ─────────────┘
```

Both blocks reuse the existing `generic_components/write2landing_block` exporter unchanged.

---

## 2. Bounding Box

Vietnam filter applied at download time:

| Parameter | Value |
|---|---|
| `lon_min` | 102.14441 |
| `lat_min` | 8.1790665 |
| `lon_max` | 114.3337595 |
| `lat_max` | 23.393395 |

Passed as pipeline variables and forwarded to the `overturemaps` library via `bbox=(lon_min, lat_min, lon_max, lat_max)`.

---

## 3. Data Loaders

**Files:**
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_base.py`
- `application/mage_ai/mds_demo/data_loaders/overture_maps/load_overture_places.py`

**Pattern:**
1. Read pipeline variables for bbox and release (optional override).
2. Call `overturemaps.record_batch_reader(type=<theme_type>, bbox=bbox)` → PyArrow RecordBatchReader.
3. Read all batches into a PyArrow Table.
4. Flatten/extract key columns (id, type, subtype, names, geometry, update_time, sources, version, bbox_min/max).
5. Convert geometry column (WKB binary) → GeoJSON string via `shapely` (`to_geojson()`) — consistent with `vn_gis` pattern.
6. Convert to pandas DataFrame and return.

**Theme types:**

| Loader | `overturemaps` type arg | Key output columns |
|---|---|---|
| `load_overture_base` | `base` | `id, type, subtype, class, names_primary, geometry_json, update_time, version` |
| `load_overture_places` | `place` | `id, type, categories_primary, confidence, names_primary, addresses, geometry_json, update_time, version` |

**Geometry:** stored as GeoJSON string column `geometry_json` (VARCHAR) — mirrors `vn_gis` raw pattern, allows `ST_GeomFromGeoJSON()` in dbt stg.

---

## 4. Pipeline Variables

```yaml
variables:
  bbox_lon_min: 102.14441
  bbox_lat_min: 8.1790665
  bbox_lon_max: 114.3337595
  bbox_lat_max: 23.393395
  bucket_name: dwhfilesystem
  file_format: parquet
  ingestion_data_base: overture_maps_base
  ingestion_data_places: overture_maps_places
```

Each loader block reads `ingestion_data` from its own local variable override to ensure separate landing paths.

---

## 5. MinIO Landing Paths

```
s3://dwhfilesystem/landing_area/overture_maps_base/
  year=YYYY/month=M/day=D/overture_maps_base_HHMMSS.parquet

s3://dwhfilesystem/landing_area/overture_maps_places/
  year=YYYY/month=M/day=D/overture_maps_places_HHMMSS.parquet
```

Hive-style partitioning handled by `write2landing_block` (unchanged).

---

## 5b. Pipeline Block Structure (Mage metadata.yaml)

Since both themes share `write2landing_block` but need different `ingestion_data` values, the pipeline references the exporter block **twice** with per-block variable overrides via `block_settings`:

```yaml
blocks:
  - name: overture_maps/load_overture_base
    type: data_loader
    upstream_blocks: []
    downstream_blocks: [generic_components/write2landing_base]

  - name: generic_components/write2landing_block
    uuid: write2landing_base
    block_settings:
      variables:
        ingestion_data: overture_maps_base
    upstream_blocks: [overture_maps/load_overture_base]

  - name: overture_maps/load_overture_places
    type: data_loader
    upstream_blocks: []
    downstream_blocks: [generic_components/write2landing_places]

  - name: generic_components/write2landing_block
    uuid: write2landing_places
    block_settings:
      variables:
        ingestion_data: overture_maps_places
    upstream_blocks: [overture_maps/load_overture_places]
```

Both branches run in parallel. The `write2landing_block` file is reused unchanged.

---

## 6. dbt Models

### Raw views (external, no data copy)

| File | SQL pattern |
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

Geometry column promoted to PostGIS: `ST_GeomFromGeoJSON(geometry_json)::geometry AS geometry`.

### Macros (added to `macros/parquet_scans.sql`)

```sql
{% macro overture_maps_base_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR              AS id,
    r['type']::VARCHAR            AS type,
    r['subtype']::VARCHAR         AS subtype,
    r['class']::VARCHAR           AS class,
    r['names_primary']::VARCHAR   AS names_primary,
    r['geometry_json']::VARCHAR   AS geometry_json,
    r['update_time']::VARCHAR     AS update_time,
    r['version']::INTEGER         AS version,
    r['year']::INTEGER            AS year,
    r['month']::INTEGER           AS month,
    r['day']::INTEGER             AS day
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

Similar macro for `overture_maps_places_typed_scan`.

### Schema YAMLs

`schema.yml` files added for both raw and stg models with column-level descriptions and `not_null` / `unique` tests on `id`.

---

## 7. Dependencies

Add to `application/mage_ai/requirements.txt`:
```
overturemaps
shapely
pyarrow
```

`pyarrow` is likely pulled in transitively by `deltalake` but must be listed explicitly. `shapely` and `overturemaps` are not currently present.

---

## 8. Data Flow Summary

```
overturemaps library (DuckDB → Overture S3)
        ↓  bbox filter at source
    PyArrow Table
        ↓  geometry WKB → GeoJSON string
    pandas DataFrame
        ↓  write2landing_block
s3://dwhfilesystem/landing_area/overture_maps_{theme}/year=Y/month=M/day=D/
        ↓  pg_duckdb external view
    raw.overture_maps__base / raw.overture_maps__places
        ↓  dbt incremental + partition watermark
    stg.overture_maps__base / stg.overture_maps__places
```

---

## 9. Out of Scope

- `bdh` / `adl` layer models (to be added when downstream use case is defined)
- Mage schedule trigger (to be configured separately by user)
- H3 spatial indexing (future enhancement)
- Other Overture themes (buildings, transportation, divisions, addresses)
