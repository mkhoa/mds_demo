{{
    config(
        unique_key = ['date', 'ma_xa', '_filename']
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
    r['date']::DATE                                      AS date,
    r['ma_xa']::VARCHAR                                  AS ma_xa,
    r['ten_xa']::VARCHAR                                 AS ten_xa,
    r['ma_tinh']::VARCHAR                                AS ma_tinh,
    r['ten_tinh']::VARCHAR                               AS ten_tinh,
    r['lat']::DOUBLE PRECISION                           AS lat,
    r['long']::DOUBLE PRECISION                          AS long,
    r['weather_code']::INTEGER                           AS weather_code,
    r['temperature_2m_max']::DOUBLE PRECISION            AS temperature_2m_max,
    r['temperature_2m_min']::DOUBLE PRECISION            AS temperature_2m_min,
    r['temperature_2m_mean']::DOUBLE PRECISION           AS temperature_2m_mean,
    r['precipitation_sum']::DOUBLE PRECISION             AS precipitation_sum,
    r['rain_sum']::DOUBLE PRECISION                      AS rain_sum,
    r['sunshine_duration']::DOUBLE PRECISION             AS sunshine_duration,
    r['shortwave_radiation_sum']::DOUBLE PRECISION       AS shortwave_radiation_sum,
    r['wind_speed_10m_max']::DOUBLE PRECISION            AS wind_speed_10m_max,
    r['et0_fao_evapotranspiration']::DOUBLE PRECISION    AS et0_fao_evapotranspiration,
    r['year']::INTEGER                                   AS year,
    r['month']::INTEGER                                  AS month,
    r['day']::INTEGER                                    AS day,
    r['filename']::VARCHAR                               AS _filename
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/vn_weather/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if is_incremental() %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
