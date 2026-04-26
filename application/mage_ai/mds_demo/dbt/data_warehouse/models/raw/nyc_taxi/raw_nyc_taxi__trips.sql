{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    External view over MinIO landing area parquet files via pg_duckdb.

    Path layout (hive-partitioned):
        s3://dwhfilesystem/landing_area/nyc_taxi/year=YYYY/month=M/day=D/
    DuckDB automatically extracts year, month, and day from the directory names,
    enabling partition pruning when downstream models filter on those columns.
*/

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
    r['airport_fee']::DOUBLE PRECISION             AS airport_fee
FROM duckdb.query($duckdb$
    SELECT *
    FROM read_parquet(
        's3://dwhfilesystem/landing_area/nyc_taxi/**/*.parquet',
        hive_partitioning = true
    )
$duckdb$) AS r
