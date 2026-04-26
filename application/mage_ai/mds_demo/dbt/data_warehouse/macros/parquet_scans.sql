{#
  parquet_scans.sql

  Typed scan macros for each MinIO data domain.

  Why this exists: pg_duckdb's duckdb.query() is opaque to PostgreSQL's planner,
  so predicates applied on top of the resulting view are NOT pushed into DuckDB and
  no Hive partition pruning occurs. The only way to get real partition pruning is to
  embed the WHERE clause inside the duckdb.query() dollar-quoted string.

  Pattern:
    duckdb_parquet_from(s3_path, partition_filter) — shared boilerplate, returns the
    FROM fragment. Domain macros call it and define only their column SELECT lists.

  Usage:
    {{ nyc_taxi_typed_scan() }}                         -- full scan (raw view)
    {{ nyc_taxi_typed_scan('(year, month, day) >= (2024, 3, 1)') }}  -- pruned (stg incremental)
#}


-- ──────────────────────────────────────────────────────────────────────────────
-- Base: shared DuckDB read_parquet wrapper
-- All domain macros delegate here for the FROM clause.
-- ──────────────────────────────────────────────────────────────────────────────
{% macro duckdb_parquet_from(s3_path, partition_filter='') %}
duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        '{{ s3_path }}',
        hive_partitioning = true,
        filename          = true
    )
    {% if partition_filter %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- NYC Taxi
-- Layout: s3://dwhfilesystem/landing_area/nyc_taxi/year=Y/month=M/day=D/*.parquet
-- ──────────────────────────────────────────────────────────────────────────────
{% macro nyc_taxi_typed_scan(partition_filter='') %}
SELECT
    r['VendorID']::BIGINT                          AS vendor_id,
    r['taxi_type']::VARCHAR                        AS taxi_type,
    r['year']::INTEGER                             AS year,
    r['month']::INTEGER                            AS month,
    r['day']::INTEGER                              AS day,
    r['pickup_datetime']::TIMESTAMP                AS pickup_datetime,
    r['dropoff_datetime']::TIMESTAMP               AS dropoff_datetime,
    r['passenger_count']::DOUBLE PRECISION         AS passenger_count,
    r['trip_distance']::DOUBLE PRECISION           AS trip_distance,
    r['RatecodeID']::DOUBLE PRECISION              AS rate_code_id,
    r['store_and_fwd_flag']::VARCHAR               AS store_and_fwd_flag,
    r['PULocationID']::BIGINT                      AS pu_location_id,
    r['DOLocationID']::BIGINT                      AS do_location_id,
    r['payment_type']::BIGINT                      AS payment_type,
    r['fare_amount']::DOUBLE PRECISION             AS fare_amount,
    r['extra']::DOUBLE PRECISION                   AS extra,
    r['mta_tax']::DOUBLE PRECISION                 AS mta_tax,
    r['tip_amount']::DOUBLE PRECISION              AS tip_amount,
    r['tolls_amount']::DOUBLE PRECISION            AS tolls_amount,
    r['improvement_surcharge']::DOUBLE PRECISION   AS improvement_surcharge,
    r['total_amount']::DOUBLE PRECISION            AS total_amount,
    r['congestion_surcharge']::DOUBLE PRECISION    AS congestion_surcharge,
    r['airport_fee']::DOUBLE PRECISION             AS airport_fee,
    r['filename']::VARCHAR                         AS _filename
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/nyc_taxi/**/*.parquet', partition_filter) }}
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- Vietnam Weather (daily observations, Open-Meteo Archive API)
-- Layout: s3://dwhfilesystem/landing_area/vn_weather/year=Y/month=M/day=D/*.parquet
-- ──────────────────────────────────────────────────────────────────────────────
{% macro vn_weather_typed_scan(partition_filter='') %}
SELECT
    r['date']::DATE                                      AS date,
    r['ma_xa']::VARCHAR                                  AS ma_xa,
    r['ten_xa']::VARCHAR                                 AS ten_xa,
    r['ma_tinh']::VARCHAR                                AS ma_tinh,
    r['ten_tinh']::VARCHAR                               AS ten_tinh,
    r['lat']::DOUBLE PRECISION                           AS lat,
    r['long']::DOUBLE PRECISION                          AS long,
    r['weather_code']::INTEGER                           AS weather_code,
    r['temperature_2m_max']::DOUBLE PRECISION            AS temperature_2m_max,
    r['temperature_2m_min']::DOUBLE PRECISION            AS temperature_2m_min,
    r['temperature_2m_mean']::DOUBLE PRECISION           AS temperature_2m_mean,
    r['precipitation_sum']::DOUBLE PRECISION             AS precipitation_sum,
    r['rain_sum']::DOUBLE PRECISION                      AS rain_sum,
    r['sunshine_duration']::DOUBLE PRECISION             AS sunshine_duration,
    r['shortwave_radiation_sum']::DOUBLE PRECISION       AS shortwave_radiation_sum,
    r['wind_speed_10m_max']::DOUBLE PRECISION            AS wind_speed_10m_max,
    r['et0_fao_evapotranspiration']::DOUBLE PRECISION    AS et0_fao_evapotranspiration,
    r['year']::INTEGER                                   AS year,
    r['month']::INTEGER                                  AS month,
    r['day']::INTEGER                                    AS day,
    r['filename']::VARCHAR                               AS _filename
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/vn_weather/**/*.parquet', partition_filter) }}
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- ERA5-Land Weather (daily locality stats via BigQuery + ST_REGIONSTATS)
-- Layout: s3://dwhfilesystem/landing_area/era5_weather/year=Y/month=M/day=D/*.parquet
-- Source: ECMWF/ERA5_LAND/DAILY_AGGR × bigquery-public-data.overture_maps.division_area
-- ──────────────────────────────────────────────────────────────────────────────
{% macro era5_weather_typed_scan(partition_filter='') %}
SELECT
    r['location_id']::VARCHAR                AS location_id,
    r['location_name']::VARCHAR              AS location_name,
    r['subtype']::VARCHAR                    AS subtype,
    r['observation_date']::TIMESTAMP         AS observation_date,
    r['avg_temp_c']::DOUBLE PRECISION        AS avg_temp_c,
    r['max_temp_c']::DOUBLE PRECISION        AS max_temp_c,
    r['precip_mm']::DOUBLE PRECISION         AS precip_mm,
    r['wind_speed_ms']::DOUBLE PRECISION     AS wind_speed_ms,
    r['soil_moisture']::DOUBLE PRECISION     AS soil_moisture,
    r['year']::INTEGER                       AS year,
    r['month']::INTEGER                      AS month,
    r['day']::INTEGER                        AS day,
    r['filename']::VARCHAR                   AS _filename
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/era5_weather/**/*.parquet', partition_filter) }}
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- Overture Maps — Base (land features)
-- Layout: s3://dwhfilesystem/landing_area/overture_maps_base/year=Y/month=M/day=D/*.parquet
-- Source: bigquery-public-data.overture_maps.land, filtered to Vietnam
-- ──────────────────────────────────────────────────────────────────────────────
{% macro overture_maps_base_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                 AS id,
    r['subtype']::VARCHAR            AS subtype,
    r['class']::VARCHAR              AS class,
    r['land_name']::VARCHAR          AS land_name,
    r['latitude']::DOUBLE PRECISION  AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR       AS geometry_wkt,
    r['year']::INTEGER               AS year,
    r['month']::INTEGER              AS month,
    r['day']::INTEGER                AS day
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/overture_maps_base/**/*.parquet', partition_filter) }}
{% endmacro %}


-- ──────────────────────────────────────────────────────────────────────────────
-- Overture Maps — Places (POIs)
-- Layout: s3://dwhfilesystem/landing_area/overture_maps_places/year=Y/month=M/day=D/*.parquet
-- Source: bigquery-public-data.overture_maps.place, filtered to Vietnam
-- ──────────────────────────────────────────────────────────────────────────────
{% macro overture_maps_places_typed_scan(partition_filter='') %}
SELECT
    r['id']::VARCHAR                 AS id,
    r['place_name']::VARCHAR         AS place_name,
    r['primary_category']::VARCHAR   AS primary_category,
    r['address']::VARCHAR            AS address,
    r['phone']::VARCHAR              AS phone,
    r['social_link']::VARCHAR        AS social_link,
    r['latitude']::DOUBLE PRECISION  AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR       AS geometry_wkt,
    r['year']::INTEGER               AS year,
    r['month']::INTEGER              AS month,
    r['day']::INTEGER                AS day
FROM {{ duckdb_parquet_from('s3://dwhfilesystem/landing_area/overture_maps_places/**/*.parquet', partition_filter) }}
{% endmacro %}


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
