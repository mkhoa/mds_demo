{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    External view over MinIO landing area Parquet files via pg_duckdb.

    Path layout (hive-partitioned by ingestion date):
        s3://dwhfilesystem/landing_area/era5_weather/year=YYYY/month=M/day=D/
    Source: Mage AI pipeline vn_era5_weather_bq
            → BigQuery ST_REGIONSTATS × ECMWF/ERA5_LAND/DAILY_AGGR
            × bigquery-public-data.overture_maps.division_area (VN localities)

    Column casts are defined in the era5_weather_typed_scan() macro
    (macros/parquet_scans.sql) so the stg incremental model can reuse them
    with an embedded partition filter for Hive partition pruning.
*/

{{ era5_weather_typed_scan() }}
