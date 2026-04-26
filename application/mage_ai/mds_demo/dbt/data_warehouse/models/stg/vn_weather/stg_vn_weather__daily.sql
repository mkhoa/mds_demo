{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['date', 'ma_xa'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Staging layer reads from the materialized raw table.
*/

WITH source AS (

    SELECT * FROM {{ ref('raw_vn_weather__daily') }}
    {% if is_incremental() %}
    WHERE date >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}

),

cleaned AS (
    SELECT
        -- identifiers
        date,
        ma_xa,
        ten_xa,
        ma_tinh,
        ten_tinh,

        -- coordinates
        lat,
        long,

        -- partition helpers
        year,
        month,
        day,

        -- weather classification
        weather_code,

        -- temperature (°C)
        temperature_2m_max,
        temperature_2m_min,
        temperature_2m_mean,

        -- precipitation (mm; null if zero)
        NULLIF(precipitation_sum, 0) AS precipitation_sum,
        NULLIF(rain_sum,          0) AS rain_sum,

        -- solar radiation
        ROUND((sunshine_duration / 3600.0)::NUMERIC, 2) AS sunshine_duration_hours,
        shortwave_radiation_sum,

        -- wind (km/h)
        wind_speed_10m_max,

        -- evapotranspiration — FAO-56 Penman-Monteith (mm)
        et0_fao_evapotranspiration

    FROM source
    WHERE
        date   IS NOT NULL
        AND ma_xa IS NOT NULL
)

SELECT * FROM cleaned
