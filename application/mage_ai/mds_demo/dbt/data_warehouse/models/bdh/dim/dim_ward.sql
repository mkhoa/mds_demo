{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_vn_gis__new_ward') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ma_xa']) }}    AS sk_ward,
    ma_xa,
    ten_xa,
    ma_tinh,
    ten_tinh,
    loai,
    loai_en,
    dtich_km2,
    dan_so,
    matdo_km2,
    lat                                                   AS centroid_lat,
    long                                                  AS centroid_long,
    ST_GeomFromGeoJSON(geometry_json)::geometry           AS geometry
FROM source
