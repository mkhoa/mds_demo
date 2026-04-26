{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

/*
    External view over MinIO landing area Parquet files via pg_duckdb.

    Source: Mage AI pipeline gee_ndvi_ingestion → Google Earth Engine
    Collection: MODIS/061/MOD13Q1 (250 m, 16-day composites)
    Grain: one row per ward (ma_xa) × MODIS composite date (image_date)

    Path layout (hive-partitioned by image_date):
        s3://dwhfilesystem/landing_area/gee_ndvi/year=YYYY/month=M/day=D/

    Column casts are defined in gee_ndvi_typed_scan() (macros/parquet_scans.sql)
    so the stg incremental model can reuse them with a partition filter for pruning.
*/

{{ gee_ndvi_typed_scan() }}
