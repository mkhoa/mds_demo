{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT DISTINCT subtype, class
    FROM {{ ref('stg_overture_maps__base') }}
    WHERE subtype IS NOT NULL
      AND class   IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['subtype', 'class']) }}  AS sk_land_class,
    subtype,
    class
FROM source
