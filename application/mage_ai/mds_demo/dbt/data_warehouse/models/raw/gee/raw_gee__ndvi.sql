{{
    config(
        unique_key = ['image_date', 'ma_xa', '_filename']
    )
}}

/*
    Incremental raw table over GEE zonal stats Parquet files.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

SELECT
    r['image_date']::DATE            AS image_date,
    r['ma_xa']::VARCHAR              AS ma_xa,
    r['ten_xa']::VARCHAR             AS ten_xa,
    r['ma_tinh']::VARCHAR            AS ma_tinh,
    r['ten_tinh']::VARCHAR           AS ten_tinh,
    r['ndvi_mean']::DOUBLE PRECISION AS ndvi_mean,
    r['ndvi_min']::DOUBLE PRECISION  AS ndvi_min,
    r['ndvi_max']::DOUBLE PRECISION  AS ndvi_max,
    r['ndvi_p25']::DOUBLE PRECISION  AS ndvi_p25,
    r['ndvi_p75']::DOUBLE PRECISION  AS ndvi_p75,
    r['pixel_count']::BIGINT         AS pixel_count,
    r['source']::VARCHAR             AS source,
    r['year']::INTEGER               AS year,
    r['month']::INTEGER              AS month,
    r['day']::INTEGER                AS day,
    r['filename']::VARCHAR           AS _filename
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/gee_ndvi/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if is_incremental() %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
