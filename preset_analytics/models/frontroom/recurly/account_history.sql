{{ config(materialized="table") }}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
)

SELECT T.dt, A.*
FROM date_spine T
JOIN{{ ref('src_account_history') }} AS A ON
  T.dt > A.effective_from AND T.dt < COALESCE(A.effective_to, '3000-01-01')
