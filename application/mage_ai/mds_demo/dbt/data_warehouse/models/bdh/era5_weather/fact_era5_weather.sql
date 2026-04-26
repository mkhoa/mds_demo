{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH weather AS (
    SELECT
        observation_date,
        location_id      AS division_area_id,
        avg_temp_c,
        max_temp_c,
        precip_mm,
        wind_speed_ms,
        soil_moisture
    FROM {{ ref('stg_era5_weather__locality') }}
),

division AS (
    SELECT
        division_area_id,
        sk_division_area,
        sk_ward
    FROM {{ ref('dim_division_area') }}
),

date_dim AS (
    SELECT
        date_day,
        sk_date
    FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['w.observation_date', 'w.division_area_id']) }} AS sk_era5_weather,
    dt.sk_date,
    d.sk_division_area,
    d.sk_ward,
    w.observation_date,
    w.division_area_id,
    w.avg_temp_c,
    w.max_temp_c,
    w.precip_mm,
    w.wind_speed_ms,
    w.soil_moisture
FROM weather  w
JOIN division d ON d.division_area_id = w.division_area_id
JOIN date_dim dt ON dt.date_day        = w.observation_date
