{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT DISTINCT primary_category
    FROM {{ ref('stg_overture_maps__places') }}
    WHERE primary_category IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['primary_category']) }}  AS sk_place_category,
    primary_category,
    SPLIT_PART(primary_category, '.', 1)                          AS category_group
FROM source
