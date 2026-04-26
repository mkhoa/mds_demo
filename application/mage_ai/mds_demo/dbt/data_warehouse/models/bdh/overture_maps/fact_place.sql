{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

/*
    Spatial join (point-in-polygon) runs inside duckdb.query() to avoid the
    BLOB roundtrip on geometry columns. We parse WKT on entry; ST_Within does
    not need ST_FlipCoordinates because it's a topological predicate (no
    geodesy involved). Places outside any ward polygon get sk_ward = NULL via
    LEFT JOIN.

    Dedup note: stg_overture_maps__places carries pre-existing duplicate ids
    from multiple ingestion runs. We pick the latest partition per id with
    ROW_NUMBER() inside both the duckdb.query() block and the outer Postgres
    CTEs so the fact has one row per place_id.

    duckdb.query() returns a single record column 'r'; columns must be
    extracted via r['col']::type.
*/

WITH place_with_ward AS (
    SELECT
        (r['place_id'])::text  AS place_id,
        (r['sk_ward'])::text   AS sk_ward
    FROM duckdb.query($duckdb$
        WITH places_raw AS (
            SELECT
                id AS place_id,
                ST_GeomFromText(geometry_wkt) AS geom,
                year, month, day,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY year DESC, month DESC, day DESC
                ) AS rn
            FROM pgduckdb.stg.overture_maps_places
        ),
        places AS (
            SELECT place_id, geom FROM places_raw WHERE rn = 1
        ),
        wards AS (
            SELECT
                sk_ward,
                ST_GeomFromText(geometry_wkt) AS geom
            FROM pgduckdb.bdh.dim_ward
        )
        SELECT
            p.place_id,
            w.sk_ward
        FROM places p
        LEFT JOIN wards w
          ON ST_Within(p.geom, w.geom)
    $duckdb$) AS r
),

places_raw AS (
    SELECT
        id              AS place_id,
        place_name,
        primary_category,
        address,
        phone,
        social_link,
        latitude,
        longitude,
        geometry_wkt,
        year,
        month,
        day,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY year DESC, month DESC, day DESC
        ) AS rn
    FROM {{ ref('stg_overture_maps__places') }}
),

places AS (
    SELECT * FROM places_raw WHERE rn = 1
),

categories AS (
    SELECT sk_place_category, primary_category FROM {{ ref('dim_place_category') }}
),

date_dim AS (
    SELECT sk_date, date_day FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.place_id']) }}                       AS sk_place,
    pw.sk_ward,
    c.sk_place_category,
    dt.sk_date                                                                    AS sk_date_ingested,
    p.place_id,
    p.place_name,
    p.address,
    p.phone,
    p.social_link,
    p.latitude,
    p.longitude,
    p.geometry_wkt
FROM places p
LEFT JOIN place_with_ward pw ON pw.place_id      = p.place_id
JOIN     categories       c  ON c.primary_category = p.primary_category
JOIN     date_dim         dt ON dt.date_day        = MAKE_DATE(p.year, p.month, p.day)
