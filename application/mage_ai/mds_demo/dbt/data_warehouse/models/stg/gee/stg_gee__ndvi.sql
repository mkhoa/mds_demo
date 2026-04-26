{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['image_date', 'ma_xa'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      Partition key = image_date (year/month/day of the MODIS composite).
      The watermark is the max image_date already in this table — only composites
      from that date onward are re-read (the last date is always re-processed to
      catch pipeline re-runs). delete+insert on (image_date, ma_xa) handles dedup.

      Full refresh rebuilds from all parquet files on MinIO.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ gee_ndvi_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_gee__ndvi') }}
    {% endif %}

),

cleaned AS (
    SELECT
        -- time dimension
        image_date,
        year,
        month,
        day,

        -- identifiers (join key to stg.vn_gis_new_ward / stg.vn_gis_old_ward)
        ma_xa,
        ten_xa,
        ma_tinh,
        ten_tinh,

        -- NDVI statistics (MODIS/061/MOD13Q1, scaled to [-0.2, 1.0])
        ndvi_mean,
        ndvi_min,
        ndvi_max,
        ndvi_p25,
        ndvi_p75,

        -- pixel count is a data-quality signal; null when zero
        NULLIF(pixel_count, 0) AS pixel_count,

        -- NDVI spread — larger values indicate heterogeneous vegetation cover
        ROUND((ndvi_p75 - ndvi_p25)::NUMERIC, 6) AS ndvi_iqr,

        -- provenance
        source

    FROM source
    WHERE
        image_date IS NOT NULL
        AND ma_xa  IS NOT NULL
        -- exclude pixels with no valid data
        AND pixel_count > 0
        AND ndvi_mean BETWEEN -0.2 AND 1.0
)

SELECT * FROM cleaned
