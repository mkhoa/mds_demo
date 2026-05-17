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

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
{% endif %}

WITH source AS (

    SELECT * FROM {{ ref('raw_overture_maps__division_area') }}
    {% if is_incremental() %}
    WHERE (year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }})
    {% endif %}

),

cleaned AS (

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
