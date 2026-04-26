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
