{{ config(
    materialized='table'
) }}

WITH latest AS (
  SELECT *, row_number() over (partition by id order by loaded_at DESC) AS rn
  FROM {{ ref('src_shortcut_iterations') }}
)

SELECT * EXCEPT(rn)
FROM latest
WHERE rn = 1
