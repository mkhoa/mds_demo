{{
    config(
        materialized = 'view',
        schema = 'stg'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_nyc_taxi__trips') }}
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
        GREATEST(passenger_count, 0)   AS passenger_count,
        GREATEST(trip_distance,   0.0) AS trip_distance,
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

        -- partition helpers (kept as integers for downstream joins)
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
