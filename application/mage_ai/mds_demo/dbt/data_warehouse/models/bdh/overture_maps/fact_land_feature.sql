{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

/*
    Spatial work runs inside duckdb.query() to avoid BLOB roundtrip. Area uses
    ST_Area_Spheroid(ST_FlipCoordinates(...)) — workaround for the DuckDB v1.4.3
    bug where ST_Area_Spheroid returns NaN at longitudes >= 90°.

    Dedup note: stg_overture_maps__base carries pre-existing duplicate ids from
    multiple ingestion runs. We pick the latest partition per id via
    ROW_NUMBER() before the spatial intersection so we don't multiply dupes by
    every ward overlap.

    duckdb.query() returns a single record column 'r'; columns must be
    extracted via r['col']::type.
*/

WITH intersected AS (
    SELECT
        (r['land_feature_id'])::text             AS land_feature_id,
        (r['subtype'])::text                     AS subtype,
        (r['class'])::text                       AS class,
        (r['land_name'])::text                   AS land_name,
        (r['year'])::int                         AS year,
        (r['month'])::int                        AS month,
        (r['day'])::int                          AS day,
        (r['sk_ward'])::text                     AS sk_ward,
        (r['ma_xa'])::text                       AS ma_xa,
        (r['area_km2_in_ward'])::double precision AS area_km2_in_ward,
        (r['area_km2_total'])::double precision   AS area_km2_total
    FROM duckdb.query($duckdb$
        WITH land_raw AS (
            SELECT
                id AS land_feature_id,
                subtype,
                class,
                land_name,
                year,
                month,
                day,
                ST_GeomFromText(geometry_wkt) AS geom,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY year DESC, month DESC, day DESC
                ) AS rn
            FROM pgduckdb.stg.overture_maps_base
        ),
        land AS (
            SELECT * FROM land_raw WHERE rn = 1
        ),
        wards AS (
            SELECT
                sk_ward,
                ma_xa,
                ST_GeomFromText(geometry_wkt) AS geom
            FROM pgduckdb.bdh.dim_ward
        )
        SELECT
            l.land_feature_id,
            l.subtype,
            l.class,
            l.land_name,
            l.year,
            l.month,
            l.day,
            w.sk_ward,
            w.ma_xa,
            ST_Area_Spheroid(ST_FlipCoordinates(ST_Intersection(l.geom, w.geom))) / 1e6 AS area_km2_in_ward,
            ST_Area_Spheroid(ST_FlipCoordinates(l.geom))                          / 1e6 AS area_km2_total
        FROM land l
        JOIN wards w ON ST_Intersects(l.geom, w.geom)
    $duckdb$) AS r
),

land_class AS (
    SELECT sk_land_class, subtype, class FROM {{ ref('dim_land_class') }}
),

date_dim AS (
    SELECT sk_date, date_day FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['i.land_feature_id', 'i.ma_xa']) }}    AS sk_land_feature,
    i.sk_ward,
    lc.sk_land_class,
    dt.sk_date                                                                   AS sk_date_ingested,
    i.land_feature_id,
    i.land_name,
    i.area_km2_in_ward,
    i.area_km2_total,
    i.area_km2_in_ward / NULLIF(i.area_km2_total, 0)                             AS overlap_ratio
FROM intersected i
JOIN land_class lc ON lc.subtype = i.subtype AND lc.class = i.class
JOIN date_dim   dt ON dt.date_day = MAKE_DATE(i.year, i.month, i.day)
