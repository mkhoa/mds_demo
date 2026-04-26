{{
    config(
        materialized = 'table',
        schema = 'adl'
    )
}}

/*
    Gold / Analytics Data Layer.
    Monthly pickup and dropoff statistics per TLC Taxi Zone and taxi type.
    Used for geographic demand analysis and zone-level revenue reporting.
*/

WITH trips AS (
    SELECT * FROM {{ ref('bdh_nyc_taxi__trips') }}
)

SELECT
    year,
    month,
    taxi_type,
    pu_location_id,

    -- pickup metrics
    COUNT(*)                                           AS pickup_trips,
    ROUND(SUM(trip_distance)::NUMERIC, 2)              AS pickup_total_miles,
    ROUND(AVG(trip_distance)::NUMERIC, 2)              AS pickup_avg_miles,
    ROUND(SUM(total_amount)::NUMERIC, 2)               AS pickup_total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)               AS pickup_avg_fare,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2)      AS pickup_avg_duration_min,

    -- busiest hours
    MODE() WITHIN GROUP (ORDER BY pickup_hour)         AS peak_pickup_hour,
    MODE() WITHIN GROUP (ORDER BY pickup_dow)          AS peak_pickup_dow

FROM trips
GROUP BY year, month, taxi_type, pu_location_id
ORDER BY year, month, taxi_type, pickup_trips DESC
