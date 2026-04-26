{{
    config(
        unique_key = ['observation_date', 'location_id', '_filename']
    )
}}

/*
    Incremental raw table over MinIO landing area Parquet files.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

SELECT
    r['location_id']::VARCHAR                AS location_id,
    r['location_name']::VARCHAR              AS location_name,
    r['subtype']::VARCHAR                    AS subtype,
    r['observation_date']::TIMESTAMP         AS observation_date,
    r['avg_temp_c']::DOUBLE PRECISION        AS avg_temp_c,
    r['max_temp_c']::DOUBLE PRECISION        AS max_temp_c,
    r['precip_mm']::DOUBLE PRECISION         AS precip_mm,
    r['wind_speed_ms']::DOUBLE PRECISION     AS wind_speed_ms,
    r['soil_moisture']::DOUBLE PRECISION     AS soil_moisture,
    r['year']::INTEGER                       AS year,
    r['month']::INTEGER                      AS month,
    r['day']::INTEGER                        AS day,
    r['filename']::VARCHAR                   AS _filename
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/era5_weather/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if is_incremental() %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
