{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['observation_date', 'location_id'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      - Full refresh:  reads raw_era5_weather__locality (full parquet scan).
      - Incremental:   calls era5_weather_typed_scan() with a partition filter
                       built from the latest ingestion (year, month, day) already
                       in this table, so DuckDB only opens new partition directories.
                       unique_key = (observation_date, location_id) deduplicates
                       within the re-read window.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ era5_weather_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_era5_weather__locality') }}
    {% endif %}

),

cleaned AS (
    SELECT
        observation_date::DATE                      AS observation_date,
        location_id,
        location_name,
        subtype,

        -- Temperature (°C)
        avg_temp_c,
        max_temp_c,

        -- Precipitation (mm; null when zero — no rain event)
        NULLIF(precip_mm, 0)                        AS precip_mm,

        -- Wind (m/s)
        wind_speed_ms,

        -- Soil moisture (m³/m³)
        soil_moisture,

        -- Ingestion partition (used by get_partition_watermark on next run)
        year,
        month,
        day,

        _filename

    FROM source
    WHERE observation_date IS NOT NULL
      AND location_id      IS NOT NULL
)

SELECT * FROM cleaned
