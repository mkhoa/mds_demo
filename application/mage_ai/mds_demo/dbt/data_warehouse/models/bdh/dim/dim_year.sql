{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH years AS (
    {{ dbt_utils.date_spine(
        datepart="year",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        EXTRACT(YEAR FROM date_year)::INT       AS year,
        date_year::DATE                          AS year_start_date,
        (DATE_TRUNC('year', date_year) + INTERVAL '1 year - 1 day')::DATE AS year_end_date
    FROM years
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year']) }}                             AS sk_year,
    year,
    year_start_date,
    year_end_date,
    (year % 4 = 0 AND (year % 100 <> 0 OR year % 400 = 0))                       AS is_leap_year,
    (year = EXTRACT(YEAR FROM CURRENT_DATE)::INT)                                AS is_current_year
FROM enriched
