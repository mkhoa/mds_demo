{{
    config(
        materialized = 'table',
        schema       = 'bdh'
    )
}}

WITH months AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        EXTRACT(YEAR  FROM date_month)::INT                                  AS year,
        EXTRACT(MONTH FROM date_month)::INT                                  AS month,
        date_month::DATE                                                      AS month_start_date,
        (DATE_TRUNC('month', date_month) + INTERVAL '1 month - 1 day')::DATE  AS month_end_date,
        TO_CHAR(date_month, 'FMMonth')                                        AS month_name,
        TO_CHAR(date_month, 'Mon')                                            AS month_abbr,
        EXTRACT(QUARTER FROM date_month)::INT                                 AS quarter
    FROM months
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'month']) }}                AS sk_month,
    {{ dbt_utils.generate_surrogate_key(['year']) }}                         AS sk_year,
    year,
    month,
    month_name,
    month_abbr,
    quarter,
    'Q' || quarter::TEXT                                                      AS quarter_label,
    month_start_date,
    month_end_date,
    (year  = EXTRACT(YEAR  FROM CURRENT_DATE)::INT
     AND month = EXTRACT(MONTH FROM CURRENT_DATE)::INT)                       AS is_current_month
FROM enriched
