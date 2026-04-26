{{
    config(
        materialized = 'table',
        schema = 'adl'
    )
}}

/*
    Gold / Analytics Data Layer.
    Daily trip volume, revenue, and efficiency KPIs per taxi type.
    Primary reporting grain for dashboards and time-series analysis.
*/

WITH trips AS (
    SELECT * FROM {{ ref('bdh_nyc_taxi__trips') }}
)

SELECT
    trip_date,
    taxi_type,
    year,
    month,
    day,

    -- volume
    COUNT(*)                                               AS total_trips,
    SUM(passenger_count)                                   AS total_passengers,

    -- distance & time
    ROUND(SUM(trip_distance)::NUMERIC, 2)                  AS total_miles,
    ROUND(AVG(trip_distance)::NUMERIC, 2)                  AS avg_trip_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2)          AS avg_trip_duration_min,

    -- revenue
    ROUND(SUM(total_amount)::NUMERIC, 2)                   AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                   AS avg_fare,
    ROUND(SUM(tip_amount)::NUMERIC, 2)                     AS total_tips,
    ROUND(AVG(tip_amount)::NUMERIC, 2)                     AS avg_tip,

    -- surcharges
    ROUND(SUM(congestion_surcharge)::NUMERIC, 2)           AS total_congestion_surcharge,
    ROUND(SUM(airport_fee)::NUMERIC, 2)                    AS total_airport_fees,

    -- efficiency
    ROUND(
        CASE WHEN SUM(trip_duration_minutes) > 0
             THEN SUM(trip_distance) / (SUM(trip_duration_minutes) / 60.0)
             ELSE NULL
        END::NUMERIC, 2
    )                                                      AS avg_speed_mph,

    -- payment mix
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END)     AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END)     AS cash_trips

FROM trips
GROUP BY trip_date, taxi_type, year, month, day
ORDER BY trip_date, taxi_type
