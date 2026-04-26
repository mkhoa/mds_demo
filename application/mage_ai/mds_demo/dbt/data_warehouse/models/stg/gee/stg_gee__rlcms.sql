{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['year', 'ma_xa', 'class_code'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      Annual data — partition key is year only (no month/day).
      Watermark = MAX(year) already in this table; re-process that year and all
      newer ones so late-arriving annual composites are caught.
      delete+insert on (year, ma_xa, class_code) handles re-run dedup.
*/

{% if is_incremental() %}
    {% set wm_sql %}SELECT COALESCE(MAX(year), 1999) FROM {{ this }}{% endset %}
    {% set wm_year = run_query(wm_sql).rows[0][0] %}
    {% set partition_filter = 'year >= ' ~ wm_year %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ gee_rlcms_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_gee__rlcms') }}
    {% endif %}

),

labelled AS (
    SELECT
        -- time dimension (annual)
        year,

        -- identifiers
        ma_xa,
        ten_xa,
        ma_tinh,
        ten_tinh,

        -- land-cover class
        class_code,

        /*
          SERVIR-Mekong RLCMS class labels for mainland SEA.
          Verify against the actual GEE asset before publishing downstream.
        */
        CASE class_code
            WHEN 1  THEN 'Forest'
            WHEN 2  THEN 'Shrubland'
            WHEN 3  THEN 'Grassland'
            WHEN 4  THEN 'Wetland'
            WHEN 5  THEN 'Cropland'
            WHEN 6  THEN 'Urban / Built-up'
            WHEN 7  THEN 'Barren'
            WHEN 8  THEN 'Open Water'
            WHEN 9  THEN 'Mangrove'
            WHEN 10 THEN 'Plantation / Tree crop'
            ELSE         'Unknown (' || class_code::TEXT || ')'
        END AS class_name,

        -- area measures
        NULLIF(pixel_count, 0)                          AS pixel_count,
        ROUND(area_km2::NUMERIC, 6)                     AS area_km2,
        ROUND(pct_area::NUMERIC, 4)                     AS pct_area,

        -- provenance
        source

    FROM source
    WHERE
        year       IS NOT NULL
        AND ma_xa  IS NOT NULL
        AND class_code IS NOT NULL
        AND pixel_count > 0
)

SELECT * FROM labelled
