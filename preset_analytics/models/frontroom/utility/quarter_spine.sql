{{
    config(
        materialized='table'
    )
}}

{# our q1 start date is Feb 1.  #}
{% set quarter_adjustment = "DATE_ADD(dt, INTERVAL -1 MONTH)" %}
{% set quarter_adjustment_extended = "DATE_ADD(" + quarter_adjustment + ", INTERVAL -14 DAY)" %}

SELECT
  dt,
  {{ format_quarter(quarter_adjustment) }} AS quarter,
  -- the 14 days after the quarter are used in analyses
  NULLIF(
    {{ format_quarter(quarter_adjustment_extended) }},
    {{ format_quarter(quarter_adjustment) }}
  ) AS quarter_extended
FROM {{ ref('date_spine') }}
