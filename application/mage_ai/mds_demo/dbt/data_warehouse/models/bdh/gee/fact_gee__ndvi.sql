{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'sk_gee_ndvi'
    )
}}

/*
    Silver / Business Data Hub.
    Tabular NDVI zonal statistics joined with conformed dimensions.
*/

WITH ndvi AS (
    SELECT * FROM {{ ref('stg_gee__ndvi') }}
    {% if is_incremental() %}
    WHERE image_date >= (SELECT MAX(image_date) FROM {{ this }})
    {% endif %}
),

ward AS (
    SELECT
        sk_ward,
        ma_xa
    FROM {{ ref('dim_ward') }}
),

date_dim AS (
    SELECT
        sk_date,
        date_day
    FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['n.image_date', 'n.ma_xa']) }} AS sk_gee_ndvi,
    w.sk_ward,
    dt.sk_date,
    n.image_date,
    n.ma_xa,
    n.ndvi_mean,
    n.ndvi_min,
    n.ndvi_max,
    n.ndvi_p25,
    n.ndvi_p75,
    n.pixel_count,
    n.source
FROM ndvi n
JOIN ward w ON w.ma_xa = n.ma_xa
JOIN date_dim dt ON dt.date_day = n.image_date
