{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    External view over MinIO landing area Parquet files via pg_duckdb.

    Path layout (hive-partitioned):
        s3://dwhfilesystem/landing_area/vn_weather/year=YYYY/month=M/day=D/
    Source: Mage AI pipeline vn_weather_historical → Open-Meteo Archive API
    Timezone: Asia/Ho_Chi_Minh

    Column casts are defined in the vn_weather_typed_scan() macro (macros/parquet_scans.sql)
    so the stg incremental path can reuse them with a partition filter embedded in the
    DuckDB query string for real partition pruning.
*/

{{ vn_weather_typed_scan() }}
