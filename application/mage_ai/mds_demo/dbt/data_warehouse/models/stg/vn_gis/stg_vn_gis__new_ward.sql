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
      delete+insert on ma_xa means new wards are inserted and any updated
      ward (boundary or metadata change) replaces the old row. Full refresh
      rebuilds from the raw GeoJSON on MinIO.
*/

WITH source AS (
    SELECT * FROM {{ ref('raw_vn_gis__new_ward') }}

    {% if is_incremental() %}
    WHERE ma_xa NOT IN (SELECT ma_xa FROM {{ this }})
       OR ma_xa IN (
            -- re-process rows where any tracked field has changed
            SELECT s.ma_xa
            FROM {{ ref('raw_vn_gis__new_ward') }} s
            JOIN {{ this }} t USING (ma_xa)
            WHERE s.geometry_json IS DISTINCT FROM t.geometry_json
               OR s.dan_so        IS DISTINCT FROM t.dan_so
               OR s.sap_nhap      IS DISTINCT FROM t.sap_nhap
        )
    {% endif %}
),

cleaned AS (
    SELECT
        -- identifiers
        ma_xa,
        ten_xa,
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

        -- merger & admin metadata
        sap_nhap,
        tru_so,

        -- measures
        NULLIF(dtich_km2,  0)           AS dtich_km2,
        NULLIF(dan_so,     0)           AS dan_so,
        NULLIF(matdo_km2,  0)           AS matdo_km2,

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
