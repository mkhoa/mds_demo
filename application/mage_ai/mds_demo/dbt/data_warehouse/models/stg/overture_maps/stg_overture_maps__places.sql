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
    Staging layer reads from the materialized raw table.
*/

WITH source AS (

    SELECT * FROM {{ ref('raw_overture_maps__places') }}
    {% if is_incremental() %}
    WHERE (year, month, day) >= (SELECT MAX(year), MAX(month), MAX(day) FROM {{ this }})
    {% endif %}

),

cleaned AS (
    SELECT
        id,
        place_name,
        primary_category,
        address,
        phone,
        social_link,
        latitude,
        longitude,
        geometry_wkt,
        year,
        month,
        day
    FROM source
    WHERE id        IS NOT NULL
      AND latitude  IS NOT NULL
      AND longitude IS NOT NULL
)

SELECT * FROM cleaned
