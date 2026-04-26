{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['vendor_id', 'pickup_datetime', 'pu_location_id', 'do_location_id'],
        on_schema_change     = 'sync_all_columns',
        schema               = 'stg'
    )
}}

/*
    Incremental strategy:
      - Full refresh:  reads the raw view (full parquet scan via pg_duckdb).
      - Incremental:   calls nyc_taxi_typed_scan() with a partition filter built from
                       the max (year, month, day) already in this table. The filter is
                       embedded inside the duckdb.query() string so DuckDB applies Hive
                       partition pruning — only directories at or after the watermark
                       are opened. The last partition is always re-read to catch late-
                       arriving files; delete+insert on unique_key handles dedup.
*/

{% if is_incremental() %}
    {% set wm = get_partition_watermark(this) %}
    {% set partition_filter %}(year, month, day) >= ({{ wm.year }}, {{ wm.month }}, {{ wm.day }}){% endset %}
{% endif %}

WITH source AS (

    {% if is_incremental() %}
        {{ nyc_taxi_typed_scan(partition_filter) }}
    {% else %}
        SELECT * FROM {{ ref('raw_nyc_taxi__trips') }}
    {% endif %}

),

cleaned AS (
    SELECT
        -- identifiers
        vendor_id,
        taxi_type,
        pu_location_id,
        do_location_id,
        rate_code_id,
        payment_type,

        -- timestamps
        pickup_datetime,
        dropoff_datetime,

        -- measures
        GREATEST(passenger_count, 0)        AS passenger_count,
        GREATEST(trip_distance,   0.0)      AS trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge,
        COALESCE(airport_fee,          0.0) AS airport_fee,
        total_amount,

        -- flags
        CASE store_and_fwd_flag WHEN 'Y' THEN TRUE ELSE FALSE END AS store_and_fwd_flag,

        -- partition helpers (kept for downstream joins and future partition pruning)
        year,
        month,
        day

    FROM source
    WHERE
        pickup_datetime  IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND dropoff_datetime > pickup_datetime
        AND trip_distance > 0
        AND total_amount  > 0
        AND pu_location_id IS NOT NULL
        AND do_location_id IS NOT NULL
),

enriched AS (
    SELECT
        *,
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0 AS trip_duration_minutes,
        CASE
            WHEN payment_type = 1 THEN 'Credit Card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No Charge'
            WHEN payment_type = 4 THEN 'Dispute'
            ELSE 'Unknown'
        END AS payment_type_name,
        CASE rate_code_id::INT
            WHEN 1 THEN 'Standard'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau/Westchester'
            WHEN 5 THEN 'Negotiated'
            WHEN 6 THEN 'Group Ride'
            ELSE 'Unknown'
        END AS rate_code_name
    FROM cleaned
)

SELECT * FROM enriched
