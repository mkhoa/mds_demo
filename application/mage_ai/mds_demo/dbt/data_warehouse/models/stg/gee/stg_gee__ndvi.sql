{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['image_date', 'ma_xa'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental staging table for GEE NDVI zonal statistics.
    Reads from the materialized raw table.
*/

WITH source AS (

    SELECT * FROM {{ ref('raw_gee__ndvi') }}
    {% if is_incremental() %}
    WHERE image_date >= (SELECT MAX(image_date) FROM {{ this }})
    {% endif %}

),

cleaned AS (
    SELECT
        image_date,
        ma_xa,
        ten_xa,
        ma_tinh,
        ten_tinh,
        ndvi_mean,
        ndvi_min,
        ndvi_max,
        ndvi_p25,
        ndvi_p75,
        pixel_count,
        source,
        year,
        month,
        day
    FROM source
    WHERE image_date IS NOT NULL
      AND ma_xa IS NOT NULL
)

SELECT * FROM cleaned
