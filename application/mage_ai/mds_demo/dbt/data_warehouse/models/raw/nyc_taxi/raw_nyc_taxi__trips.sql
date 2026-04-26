{{
    config(
        unique_key = ['vendor_id', 'pickup_datetime', '_filename']
    )
}}

/*
    Incremental raw table over MinIO landing area parquet files.
    The only place where pg_duckdb and S3 partition pruning is handled.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

SELECT
    r['VendorID']::BIGINT                          AS vendor_id,
    r['taxi_type']::VARCHAR                        AS taxi_type,
    r['year']::INTEGER                             AS year,
    r['month']::INTEGER                            AS month,
    r['day']::INTEGER                              AS day,
    r['pickup_datetime']::TIMESTAMP                AS pickup_datetime,
    r['dropoff_datetime']::TIMESTAMP               AS dropoff_datetime,
    r['passenger_count']::DOUBLE PRECISION         AS passenger_count,
    r['trip_distance']::DOUBLE PRECISION           AS trip_distance,
    r['RatecodeID']::DOUBLE PRECISION              AS rate_code_id,
    r['store_and_fwd_flag']::VARCHAR               AS store_and_fwd_flag,
    r['PULocationID']::BIGINT                      AS pu_location_id,
    r['DOLocationID']::BIGINT                      AS do_location_id,
    r['payment_type']::BIGINT                      AS payment_type,
    r['fare_amount']::DOUBLE PRECISION             AS fare_amount,
    r['extra']::DOUBLE PRECISION                   AS extra,
    r['mta_tax']::DOUBLE PRECISION                 AS mta_tax,
    r['tip_amount']::DOUBLE PRECISION              AS tip_amount,
    r['tolls_amount']::DOUBLE PRECISION            AS tolls_amount,
    r['improvement_surcharge']::DOUBLE PRECISION   AS improvement_surcharge,
    r['total_amount']::DOUBLE PRECISION            AS total_amount,
    r['congestion_surcharge']::DOUBLE PRECISION    AS congestion_surcharge,
    r['airport_fee']::DOUBLE PRECISION             AS airport_fee,
    r['filename']::VARCHAR                         AS _filename
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/nyc_taxi/**/*.parquet',
        hive_partitioning = true,
        filename          = true
    )
    {% if is_incremental() %}WHERE {{ partition_filter }}{% endif %}
$duckdb$) AS r
