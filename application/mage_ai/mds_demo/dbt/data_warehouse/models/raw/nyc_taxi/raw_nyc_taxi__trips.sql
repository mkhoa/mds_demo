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
    DuckDB automatically extracts year, month, and day from the directory names.

    Column casts are defined in the nyc_taxi_typed_scan() macro (macros/parquet_scans.sql)
    so the stg incremental path can reuse them with a partition filter embedded in the
    DuckDB query string for real partition pruning.
*/

{{ nyc_taxi_typed_scan() }}
