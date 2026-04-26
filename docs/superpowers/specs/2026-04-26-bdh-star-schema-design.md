# BDH (Silver) Star Schema — era5 weather, Overture maps, and date dimensions

**Date:** 2026-04-26
**Project:** `application/mage_ai/mds_demo/dbt/data_warehouse`
**Layer:** `bdh` (silver / Business Data Hub)

## Goal

Build the first proper star-schema slice of the bdh layer covering:

- **era5 weather** observations, mapped to Vietnamese 2025 ward boundaries (`ma_xa`) via Overture `division_area` polygons.
- **Overture maps** places (POIs) and base land features, also mapped to `ma_xa`.
- **Reusable date dimensions** (`dim_date`, `dim_month`, `dim_year`) generated with `dbt_utils.date_spine`, available for every future bdh fact.

Existing `bdh_nyc_taxi__trips` is a flat denormalized fact, not a strict star. This work establishes the star-schema convention for future bdh models without altering nyc_taxi.

## Architecture

```
                         stg layer
   stg_era5_weather__locality        ──┐
   stg_overture_maps__base           ──┤
   stg_overture_maps__places         ──┤    new in this work:
   stg_vn_gis__new_ward              ──┤      stg_overture_maps__division_area
                                       │
                                       ▼
                         bdh layer (star schema)

                ┌─── dim_year ◀── dim_month ◀── dim_date ───┐
                │                                            │
                │   ┌── dim_ward ────┐                       │
                │   │                 │                      │
                │   ├── dim_division_area (FK: sk_ward) ─────┤
                │   ├── dim_place_category                   │
                │   └── dim_land_class                       │
                │                                            │
                ▼                                            ▼
       fact_era5_weather             fact_place             fact_land_feature
```

### Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Spatial source for era5 → ward | Overture `division_area` polygons | era5 has only `location_id`; division_area provides the polygon needed for accurate intersection |
| division_area → ma_xa cardinality | 1-to-1 (largest overlap) | Simplifies fact joins; data discrepancies will be reviewed later |
| Overture base modeling | Fact (`fact_land_feature`) at (feature × ward) grain | Enables "forest area per ward" measures |
| Date dim hierarchy | `dim_date → dim_month → dim_year` with FKs | Lets BI tools auto-walk the hierarchy |
| Date range | 2015-01-01 → 2030-12-31 | Covers ERA5 history with buffer |
| Surrogate keys | `dbt_utils.generate_surrogate_key` (MD5) | Stable, matches existing nyc_taxi pattern |
| Ingestion scope | New Mage pipeline included | Turnkey reproducible build |
| Spatial query engine | **pg_duckdb (DuckDB `spatial` extension)** | All bdh queries run via pg_duckdb. PostGIS is installed but not used for query-time spatial ops. Geodesic area uses `ST_Area_Spheroid(geom)` (no `::geography` cast — DuckDB has no geography type). |

## New ingestion: Overture division_area

**Mage pipeline:** `overture_maps_division_area_ingestion`

Pattern matches existing `overture_maps_base_ingestion` and `overture_maps_places_ingestion`:

1. Extract from BigQuery (`bigquery-public-data.overture_maps.division_area`) filtered to `country='VN'`.
2. Land parquet on MinIO at `s3://dwhfilesystem/raw/overture_maps/division_area/year=YYYY/month=MM/day=DD/`.
3. `raw_overture_maps__division_area` — view over MinIO via `pg_duckdb`.
4. `stg_overture_maps__division_area` — incremental (delete+insert on `id`), partition watermark via existing `get_partition_watermark` macro.

**BigQuery extraction query:**

```sql
SELECT
    id,
    subtype,
    names.primary AS location_name,
    country,
    region,
    ST_Y(ST_CENTROID(geometry)) AS latitude,
    ST_X(ST_CENTROID(geometry)) AS longitude,
    ST_ASTEXT(geometry)         AS geometry_wkt
FROM `bigquery-public-data.overture_maps.division_area`
WHERE country = 'VN'
ORDER BY subtype;
```

> Note: the original spec listed `theme` and `type` columns. Verified during Task 1 implementation that these don't exist on `bigquery-public-data.overture_maps.division_area` (the table's actual columns are `id, country, sources, subtype, admin_level, class, names, is_land, is_territorial, region, division_id, version, bbox, geometry`). For division_area in this dataset, `theme` would always be `'admins'` and `type` would always be `'division_area'` — both redundant, so removed.

**stg_overture_maps__division_area columns:**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | Overture unique id (also used as `division_area_id` in dim) |
| `subtype` | TEXT | locality, region, county, … |
| `location_name` | TEXT | from Overture `names.primary` |
| `country` | TEXT | `VN` |
| `region` | TEXT | ISO region code |
| `latitude` | NUMERIC | centroid lat |
| `longitude` | NUMERIC | centroid lng |
| `geometry` | GEOMETRY | EPSG:4326, populated via `ST_GeomFromText(geometry_wkt)` (matches existing stg pattern; readable by pg_duckdb at query time) |
| `year`, `month`, `day` | INT | ingestion partition columns |

Tests: `unique` + `not_null` on `id`; `not_null` on `geometry`.

## Date dimensions

All three materialized as `+materialized: table`. Generated with `dbt_utils.date_spine` over **2015-01-01 → 2030-12-31**.

### `dim_year` — 16 rows

| Column | Type | Notes |
|---|---|---|
| `sk_year` | TEXT | `generate_surrogate_key(['year'])` |
| `year` | INT | 2015–2030 |
| `year_start_date` | DATE | Jan 1 |
| `year_end_date` | DATE | Dec 31 |
| `is_leap_year` | BOOLEAN | |
| `is_current_year` | BOOLEAN | `year = EXTRACT(YEAR FROM CURRENT_DATE)` |

### `dim_month` — 192 rows

| Column | Type | Notes |
|---|---|---|
| `sk_month` | TEXT | `generate_surrogate_key(['year','month'])` |
| `sk_year` | TEXT | FK → `dim_year` |
| `year` | INT | |
| `month` | INT | 1–12 |
| `month_name` | TEXT | January … December |
| `month_abbr` | TEXT | Jan … Dec |
| `quarter` | INT | 1–4 |
| `quarter_label` | TEXT | `Q1`, `Q2`, … |
| `month_start_date` | DATE | first day of month |
| `month_end_date` | DATE | last day of month |
| `is_current_month` | BOOLEAN | |

### `dim_date` — ~5,840 rows

| Column | Type | Notes |
|---|---|---|
| `sk_date` | TEXT | `generate_surrogate_key(['date_day'])` |
| `sk_month` | TEXT | FK → `dim_month` |
| `sk_year` | TEXT | FK → `dim_year` |
| `date_day` | DATE | natural key |
| `day_of_month` | INT | 1–31 |
| `day_of_week` | INT | 0=Sunday … 6=Saturday (matches existing nyc_taxi convention) |
| `day_of_week_name` | TEXT | Sunday … Saturday |
| `day_of_year` | INT | 1–366 |
| `week_of_year` | INT | ISO week |
| `is_weekend` | BOOLEAN | `day_of_week IN (0, 6)` |
| `is_current_date` | BOOLEAN | `date_day = CURRENT_DATE` |

Tests on each: `unique` + `not_null` on the surrogate key and natural key; `relationships` test from FK columns to parent dim.

## Spatial dimensions

### `dim_ward` — built from `stg_vn_gis__new_ward`

| Column | Type | Notes |
|---|---|---|
| `sk_ward` | TEXT | `generate_surrogate_key(['ma_xa'])` |
| `ma_xa` | TEXT | natural key |
| `ten_xa` | TEXT | ward name |
| `ma_tinh` | TEXT | province code |
| `ten_tinh` | TEXT | province name |
| `loai` | TEXT | unit type (Vietnamese) |
| `loai_en` | TEXT | unit type (English) |
| `dtich_km2` | NUMERIC | area km² |
| `dan_so` | INT | population |
| `matdo_km2` | NUMERIC | density |
| `centroid_lat` | NUMERIC | from existing stg `lat` |
| `centroid_long` | NUMERIC | from existing stg `long` |
| `geometry` | GEOMETRY | EPSG:4326, populated via `ST_GeomFromGeoJSON(geometry_json)` (matches existing stg pattern; readable by pg_duckdb at query time) |

Tests: `unique` + `not_null` on `sk_ward` and `ma_xa`.

### `dim_division_area` — built from `stg_overture_maps__division_area` + `dim_ward`

The single source of truth for the division_area → ma_xa spatial mapping.

| Column | Type | Notes |
|---|---|---|
| `sk_division_area` | TEXT | `generate_surrogate_key(['division_area_id'])` |
| `division_area_id` | TEXT | Overture `id` |
| `subtype` | TEXT | |
| `location_name` | TEXT | |
| `country` | TEXT | |
| `region` | TEXT | |
| `centroid_lat` | NUMERIC | |
| `centroid_long` | NUMERIC | |
| `geometry` | GEOMETRY | |
| `sk_ward` | TEXT | FK → `dim_ward` (largest-overlap winner) |
| `ma_xa` | TEXT | denormalized for query convenience |
| `area_overlap_km2` | NUMERIC | the overlap area used for assignment (diagnostic) |
| `area_overlap_ratio` | NUMERIC | overlap_km2 / total division area km² (0–1, diagnostic) |

**Spatial join logic** (pg_duckdb / DuckDB spatial — geodesic area via `ST_Area_Spheroid`, m² → km²):

```sql
WITH overlaps AS (
  SELECT
    d.id           AS division_area_id,
    w.ma_xa,
    ST_Area_Spheroid(ST_Intersection(d.geometry, w.geometry)) / 1e6 AS overlap_km2,
    ST_Area_Spheroid(d.geometry)                              / 1e6 AS division_total_km2
  FROM {{ ref('stg_overture_maps__division_area') }} d
  JOIN {{ ref('dim_ward') }} w
    ON ST_Intersects(d.geometry, w.geometry)
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY division_area_id
           ORDER BY overlap_km2 DESC
         ) AS rn
  FROM overlaps
)
SELECT division_area_id, ma_xa, overlap_km2, overlap_km2 / NULLIF(division_total_km2, 0) AS overlap_ratio
FROM ranked
WHERE rn = 1
```

Tests: `unique` + `not_null` on `sk_division_area` and `division_area_id`; `relationships` test on `sk_ward → dim_ward.sk_ward`.

## Category dimensions

### `dim_place_category` — distinct `primary_category` values from `stg_overture_maps__places`

| Column | Type | Notes |
|---|---|---|
| `sk_place_category` | TEXT | `generate_surrogate_key(['primary_category'])` |
| `primary_category` | TEXT | natural key |
| `category_group` | TEXT | derived: `split_part(primary_category, '.', 1)` |

Tests: `unique` + `not_null` on `sk_place_category` and `primary_category`.

### `dim_land_class` — distinct `(subtype, class)` from `stg_overture_maps__base`

| Column | Type | Notes |
|---|---|---|
| `sk_land_class` | TEXT | `generate_surrogate_key(['subtype','class'])` |
| `subtype` | TEXT | e.g. `physical`, `landuse` |
| `class` | TEXT | e.g. `forest`, `national_park` |

Tests: `unique` + `not_null` on `sk_land_class`.

## Fact tables

### `fact_era5_weather` — grain: `(observation_date, division_area_id)`

Built by joining `stg_era5_weather__locality` → `dim_division_area` (on `location_id = division_area_id`) → `dim_date` (on `observation_date = date_day`). The division_area's `sk_ward` is denormalized onto every weather row for cheap ward-level analytics.

| Column | Type | Notes |
|---|---|---|
| `sk_era5_weather` | TEXT | `generate_surrogate_key(['observation_date','location_id'])` |
| `sk_date` | TEXT | FK → `dim_date` |
| `sk_division_area` | TEXT | FK → `dim_division_area` |
| `sk_ward` | TEXT | FK → `dim_ward` (denormalized via dim_division_area) |
| `observation_date` | DATE | |
| `division_area_id` | TEXT | = stg `location_id` |
| `avg_temp_c` | NUMERIC | measure |
| `max_temp_c` | NUMERIC | measure |
| `precip_mm` | NUMERIC | measure |
| `wind_speed_ms` | NUMERIC | measure |
| `soil_moisture` | NUMERIC | measure |

Tests: `unique` + `not_null` on `sk_era5_weather`; `not_null` + `relationships` on each FK.

### `fact_place` — grain: `place_id`

Built by joining `stg_overture_maps__places` → `dim_ward` (point-in-polygon: `ST_Within(place.geometry, ward.geometry)`) → `dim_place_category` (on `primary_category`) → `dim_date` (ingestion date from year/month/day partition).

| Column | Type | Notes |
|---|---|---|
| `sk_place` | TEXT | `generate_surrogate_key(['place_id'])` |
| `sk_ward` | TEXT | FK → `dim_ward`, **nullable** for places outside any ward polygon |
| `sk_place_category` | TEXT | FK → `dim_place_category` |
| `sk_date_ingested` | TEXT | FK → `dim_date`, derived from `make_date(year, month, day)` |
| `place_id` | TEXT | natural key |
| `place_name` | TEXT | |
| `address` | TEXT | |
| `phone` | TEXT | |
| `social_link` | TEXT | |
| `latitude` | NUMERIC | |
| `longitude` | NUMERIC | |
| `geometry` | GEOMETRY | point |

Tests: `unique` + `not_null` on `sk_place`; `relationships` on `sk_place_category` and `sk_date_ingested`. `sk_ward` allowed null but `relationships where sk_ward IS NOT NULL`.

### `fact_land_feature` — grain: `(land_feature_id, ma_xa)` overlap

Built by joining `stg_overture_maps__base` to `dim_ward` on `ST_Intersects`. One row per overlap pair.

| Column | Type | Notes |
|---|---|---|
| `sk_land_feature` | TEXT | `generate_surrogate_key(['land_feature_id','ma_xa'])` |
| `sk_ward` | TEXT | FK → `dim_ward` |
| `sk_land_class` | TEXT | FK → `dim_land_class` |
| `sk_date_ingested` | TEXT | FK → `dim_date` |
| `land_feature_id` | TEXT | Overture id (NOT unique here — repeats across wards) |
| `land_name` | TEXT | |
| `area_km2_in_ward` | NUMERIC | `ST_Area_Spheroid(ST_Intersection(land.geometry, ward.geometry)) / 1e6` (DuckDB spatial, geodesic) |
| `area_km2_total` | NUMERIC | `ST_Area_Spheroid(land.geometry) / 1e6` (DuckDB spatial, geodesic) |
| `overlap_ratio` | NUMERIC | `area_km2_in_ward / area_km2_total` |

Tests: `unique` + `not_null` on `sk_land_feature`; `relationships` on each FK.

## Materialization

All bdh models materialize as `+materialized: table` (existing project default for the `bdh` layer). The expensive spatial joins (place point-in-polygon, land feature intersection, division-area overlap) run at full build. Acceptable for the current data volume; if performance becomes an issue, dim/fact-specific incremental strategies can be added later without changing the schema.

## Directory layout

```
application/mage_ai/mds_demo/dbt/data_warehouse/models/
├── raw/
│   └── overture_maps/
│       └── raw_overture_maps__division_area.sql       # NEW
├── stg/
│   └── overture_maps/
│       └── stg_overture_maps__division_area.sql       # NEW
└── bdh/
    ├── dim/
    │   ├── dim_year.sql                               # NEW
    │   ├── dim_month.sql                              # NEW
    │   ├── dim_date.sql                               # NEW
    │   ├── dim_ward.sql                               # NEW
    │   ├── dim_division_area.sql                      # NEW
    │   ├── dim_place_category.sql                     # NEW
    │   ├── dim_land_class.sql                         # NEW
    │   └── schema.yml                                 # NEW
    ├── era5_weather/
    │   ├── fact_era5_weather.sql                      # NEW
    │   └── schema.yml                                 # NEW
    ├── overture_maps/
    │   ├── fact_place.sql                             # NEW
    │   ├── fact_land_feature.sql                      # NEW
    │   └── schema.yml                                 # NEW
    └── nyc_taxi/                                      # untouched
        └── ...
```

Mage pipeline directory:

```
application/mage_ai/mds_demo/pipelines/
└── overture_maps_division_area_ingestion/             # NEW
    ├── metadata.yaml
    └── ... (BigQuery loader → MinIO writer blocks, mirroring overture_maps_base_ingestion)
```

## Out of scope

- Refactoring `bdh_nyc_taxi__trips` into the new star pattern — left as-is for now.
- Adl (gold) aggregations on top of these facts — separate work.
- Incremental materialization for any of the new bdh models.
- Cross-checks between `stg_vn_gis__old_ward` and `stg_vn_gis__new_ward` mappings — flagged as future work pending data discrepancy review.
- Performance tuning of spatial joins (indexes, clustering).

## Testing strategy

Each model gets a `schema.yml` with:

- `unique` + `not_null` on the surrogate key and natural key.
- `not_null` on every FK column (except `fact_place.sk_ward`, which is nullable).
- `relationships` test from every FK to the parent dim's natural key column.
- For dims with derived attributes (e.g., `is_weekend`, `quarter_label`), spot tests via `accepted_values` where applicable.

Build sequence verified with `dbt build` (run + test together).
