
{{
    config(
        materialized='table',
    )
}}


SELECT DATE(date_day) AS dt,
FROM (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=var("start_date"),
        end_date=var("end_dt")
    )
    }}
)
WHERE DATE(date_day) < DATE('{{ run_started_at.strftime("%Y-%m-%d") }}')
    AND DATE(date_day) < CURRENT_DATE('-08:00')
