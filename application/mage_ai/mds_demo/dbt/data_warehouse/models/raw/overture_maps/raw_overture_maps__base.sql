{{
    config(
        unique_key = ['id', 'year', 'month', 'day']
    )
}}

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

SELECT
    r['id']::VARCHAR                 AS id,
    r['subtype']::VARCHAR            AS subtype,
    r['class']::VARCHAR              AS class,
    r['land_name']::VARCHAR          AS land_name,
    r['latitude']::DOUBLE PRECISION  AS latitude,
    r['longitude']::DOUBLE PRECISION AS longitude,
    r['geometry_wkt']::VARCHAR       AS geometry_wkt,
    r['year']::INTEGER               AS year,
    r['month']::INTEGER              AS month,
    r['day']::INTEGER                AS day
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/overture_maps_base/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if is_incremental() %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
