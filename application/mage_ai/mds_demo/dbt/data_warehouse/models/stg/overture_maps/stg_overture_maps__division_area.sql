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
        geometry_wkt,
        year,
        month,
        day
    FROM source
    WHERE id           IS NOT NULL
      AND geometry_wkt IS NOT NULL
)

SELECT * FROM cleaned
