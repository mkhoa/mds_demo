{{
    config(
        materialized = 'table',
        schema = 'bdh'
    )
}}

/*
    Silver / Business Data Hub.
    Full, conformed trip fact table ready for analytical queries.
    Adds surrogate key and business-friendly column layout.
*/

WITH trips AS (
    SELECT * FROM {{ ref('stg_nyc_taxi__trips') }}
)

SELECT
    -- surrogate key
    MD5(
        COALESCE(taxi_type, '') || '|' ||
        COALESCE(pickup_datetime::TEXT, '') || '|' ||
        COALESCE(dropoff_datetime::TEXT, '') || '|' ||
        COALESCE(pu_location_id::TEXT, '') || '|' ||
        COALESCE(do_location_id::TEXT, '') || '|' ||
        COALESCE(total_amount::TEXT, '')
    ) AS trip_id,

    -- dimensions
    taxi_type,
    vendor_id,
    year,
    month,
    day,
    DATE(pickup_datetime)           AS trip_date,
    EXTRACT(HOUR FROM pickup_datetime)::INT AS pickup_hour,
    EXTRACT(DOW  FROM pickup_datetime)::INT AS pickup_dow,    -- 0=Sunday
    pu_location_id,
    do_location_id,
    rate_code_id,
    rate_code_name,
    payment_type,
    payment_type_name,
    store_and_fwd_flag,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- trip measures
    passenger_count,
    trip_distance,
    trip_duration_minutes,

    -- fare components
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount

FROM trips
