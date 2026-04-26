{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    External view over MinIO landing area GeoJSON via pg_duckdb.

    Source: s3://dwhfilesystem/landing_area/vn_gis/vn_new_ward.geojson
    Administrative boundary: Vietnam ward/commune level (post-2025 reorganisation)
    Note: district level removed — wards now report directly to province.
*/

SELECT
    r['ma_xa']::VARCHAR             AS ma_xa,
    r['ten_xa']::VARCHAR            AS ten_xa,
    r['loai']::VARCHAR              AS loai,
    r['cap']::INTEGER               AS cap,
    r['stt']::INTEGER               AS stt,
    r['sap_nhap']::VARCHAR          AS sap_nhap,
    r['tru_so']::VARCHAR            AS tru_so,
    r['dtich_km2']::DOUBLE PRECISION AS dtich_km2,
    r['dan_so']::DOUBLE PRECISION   AS dan_so,
    r['matdo_km2']::DOUBLE PRECISION AS matdo_km2,
    r['ma_tinh']::VARCHAR           AS ma_tinh,
    r['ten_tinh']::VARCHAR          AS ten_tinh,
    r['geometry_json']::VARCHAR     AS geometry_json,

    -- file provenance
    'vn_new_ward.geojson'::VARCHAR  AS _filename

FROM duckdb.query($duckdb$
    SELECT
        ma_xa,
        ten_xa,
        loai,
        cap,
        stt,
        sap_nhap,
        tru_so,
        dtich_km2,
        dan_so,
        matdo_km2,
        ma_tinh,
        ten_tinh,
        ST_AsGeoJSON(geom) AS geometry_json
    FROM ST_Read('s3://dwhfilesystem/landing_area/vn_gis/vn_new_ward.geojson')
$duckdb$) AS r
