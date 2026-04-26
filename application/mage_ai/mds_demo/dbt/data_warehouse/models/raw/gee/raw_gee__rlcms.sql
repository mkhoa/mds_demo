{{
    config(
        materialized = 'view',
        schema       = 'raw'
    )
}}

/*
    External view over MinIO landing area Parquet files via pg_duckdb.

    Source: Mage AI pipeline gee_rlcms_ingestion → Google Earth Engine
    Collection: projects/servir-mekong/rlcms/landcover (30 m, annual)
    Grain: one row per ward (ma_xa) × year × land-cover class code (long format)

    Path layout (hive-partitioned by year):
        s3://dwhfilesystem/landing_area/gee_rlcms/year=YYYY/

    Column casts are defined in gee_rlcms_typed_scan() (macros/parquet_scans.sql).
*/

{{ gee_rlcms_typed_scan() }}
