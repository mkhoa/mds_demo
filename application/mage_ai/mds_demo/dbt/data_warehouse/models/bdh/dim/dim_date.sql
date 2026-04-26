{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH days AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        date_day::DATE                                AS date_day,
        EXTRACT(YEAR  FROM date_day)::INT             AS year,
        EXTRACT(MONTH FROM date_day)::INT             AS month,
        EXTRACT(DAY   FROM date_day)::INT             AS day_of_month,
        EXTRACT(DOW   FROM date_day)::INT             AS day_of_week,    -- Postgres: 0=Sun ... 6=Sat
        TO_CHAR(date_day, 'FMDay')                    AS day_of_week_name,
        EXTRACT(DOY   FROM date_day)::INT             AS day_of_year,
        EXTRACT(WEEK  FROM date_day)::INT             AS week_of_year
    FROM days
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }}                       AS sk_date,
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }}                  AS sk_month,
    {{ dbt_utils.generate_surrogate_key(['year']) }}                           AS sk_year,
    date_day,
    day_of_month,
    day_of_week,
    day_of_week_name,
    day_of_year,
    week_of_year,
    (day_of_week IN (0, 6))                                                    AS is_weekend,
    (date_day = CURRENT_DATE)                                                  AS is_current_date
FROM enriched
