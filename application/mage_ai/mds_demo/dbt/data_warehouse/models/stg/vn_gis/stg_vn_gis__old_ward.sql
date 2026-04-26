{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['ma_xa'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      Reference / slowly-changing data — no time partition columns.
      delete+insert on ma_xa; re-processes rows where geometry or classification changed.
*/

WITH source AS (
    SELECT * FROM {{ ref('raw_vn_gis__old_ward') }}

    {% if is_incremental() %}
    WHERE ma_xa NOT IN (SELECT ma_xa FROM {{ this }})
       OR ma_xa IN (
            SELECT s.ma_xa
            FROM {{ ref('raw_vn_gis__old_ward') }} s
            JOIN {{ this }} t USING (ma_xa)
            WHERE s.geometry_json IS DISTINCT FROM t.geometry_json
               OR s.loai          IS DISTINCT FROM t.loai
        )
    {% endif %}
),

cleaned AS (
    SELECT
        -- identifiers
        ma_xa,
        ten_xa,
        ma_huyen,
        ten_huyen,
        ma_tinh,
        ten_tinh,

        -- classification
        loai,
        CASE loai
            WHEN 'Xã'       THEN 'Commune'
            WHEN 'Phường'   THEN 'Ward'
            WHEN 'Thị trấn' THEN 'Township'
            ELSE 'Unknown'
        END                             AS loai_en,
        cap,
        stt,

        -- geometry as validated GeoJSON string
        geometry_json,

        -- centroid of the ward boundary (EPSG:4326)
        ST_Y(ST_Centroid(ST_GeomFromGeoJSON(geometry_json))) AS lat,
        ST_X(ST_Centroid(ST_GeomFromGeoJSON(geometry_json))) AS long

    FROM source
    WHERE
        ma_xa         IS NOT NULL
        AND ma_tinh   IS NOT NULL
        AND geometry_json IS NOT NULL
)

SELECT * FROM cleaned
