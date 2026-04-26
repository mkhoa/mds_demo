{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

/*
    For each Overture division_area, find the dim_ward with the largest geodesic
    overlap. Spatial work runs inside duckdb.query() because:
      - DuckDB GEOMETRY persists to Postgres as opaque BLOB; we keep WKT text
        and parse on entry with ST_GeomFromText.
      - DuckDB v1.4.3 ST_Area_Spheroid returns NaN for longitudes >= 90°. We
        wrap with ST_FlipCoordinates to swap (lon,lat) → (lat,lon), which the
        function consumes correctly.
    Returns one row per division that intersects at least one ward.
*/

WITH assigned AS (
    SELECT
        (r['division_area_id'])::text             AS division_area_id,
        (r['sk_ward'])::text                      AS sk_ward,
        (r['ma_xa'])::text                        AS ma_xa,
        (r['area_overlap_km2'])::double precision AS area_overlap_km2,
        (r['area_overlap_ratio'])::double precision AS area_overlap_ratio
    FROM duckdb.query($duckdb$
        WITH division AS (
            SELECT
                id            AS division_area_id,
                ST_GeomFromText(geometry_wkt) AS geom
            FROM pgduckdb.stg.overture_maps_division_area
        ),
        wards AS (
            SELECT
                sk_ward,
                ma_xa,
                ST_GeomFromText(geometry_wkt) AS geom
            FROM pgduckdb.bdh.dim_ward
        ),
        ward_overlaps AS (
            SELECT
                d.division_area_id,
                w.sk_ward,
                w.ma_xa,
                ST_Area_Spheroid(ST_FlipCoordinates(ST_Intersection(d.geom, w.geom))) / 1e6 AS overlap_km2,
                ST_Area_Spheroid(ST_FlipCoordinates(d.geom))                          / 1e6 AS division_total_km2
            FROM division d
            JOIN wards w
              ON ST_Intersects(d.geom, w.geom)
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY division_area_id
                    ORDER BY overlap_km2 DESC
                ) AS rn
            FROM ward_overlaps
        )
        SELECT
            division_area_id,
            sk_ward,
            ma_xa,
            overlap_km2                                       AS area_overlap_km2,
            overlap_km2 / NULLIF(division_total_km2, 0)       AS area_overlap_ratio
        FROM ranked
        WHERE rn = 1
    $duckdb$) AS r
),

division AS (
    SELECT
        id            AS division_area_id,
        subtype,
        location_name,
        country,
        region,
        latitude      AS centroid_lat,
        longitude     AS centroid_long,
        geometry_wkt
    FROM {{ ref('stg_overture_maps__division_area') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['d.division_area_id']) }}  AS sk_division_area,
    d.division_area_id,
    d.subtype,
    d.location_name,
    d.country,
    d.region,
    d.centroid_lat,
    d.centroid_long,
    d.geometry_wkt,
    a.sk_ward,
    a.ma_xa,
    a.area_overlap_km2,
    a.area_overlap_ratio
FROM division d
JOIN assigned a USING (division_area_id)
