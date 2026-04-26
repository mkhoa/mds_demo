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
    Staging layer reads from the materialized raw table.
*/

WITH source AS (

    SELECT * FROM {{ ref('raw_era5_weather__locality') }}
    {% if is_incremental() %}
    WHERE observation_date >= (SELECT MAX(observation_date) FROM {{ this }})
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
