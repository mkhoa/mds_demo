{{
    config(
        materialized = 'table'
    )
}}

/*
    External view over MinIO landing area GeoJSON via pg_duckdb.

    Source: s3://dwhfilesystem/landing_area/vn_gis/vn_old_ward.geojson
    Administrative boundary: Vietnam ward/commune level (pre-2025 reorganisation)
*/

SELECT
    r['ma_xa']::VARCHAR             AS ma_xa,
    r['ten_xa']::VARCHAR            AS ten_xa,
    r['loai']::VARCHAR              AS loai,
    r['cap']::INTEGER               AS cap,
    r['stt']::INTEGER               AS stt,
    r['ma_huyen']::VARCHAR          AS ma_huyen,
    r['ten_huyen']::VARCHAR         AS ten_huyen,
    r['ma_tinh']::VARCHAR           AS ma_tinh,
    r['ten_tinh']::VARCHAR          AS ten_tinh,
    r['geometry_json']::VARCHAR     AS geometry_json,

    -- file provenance
    'vn_old_ward.geojson'::VARCHAR  AS _filename

FROM duckdb.query($duckdb$
    SELECT
        ma_xa,
        ten_xa,
        loai,
        cap,
        stt,
        ma_huyen,
        ten_huyen,
        ma_tinh,
        ten_tinh,
        ST_AsGeoJSON(geom) AS geometry_json
    FROM ST_Read('s3://dwhfilesystem/landing_area/vn_gis/vn_old_ward.geojson')
$duckdb$) AS r
