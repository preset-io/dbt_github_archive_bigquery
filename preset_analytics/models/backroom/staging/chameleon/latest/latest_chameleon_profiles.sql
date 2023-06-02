{{ config(
    materialized='table'
) }}

WITH latest AS (
  SELECT *, row_number() over (partition by chameleon_id order by loaded_at DESC) AS rn
  FROM {{ ref('src_chameleon_profiles') }}
)

SELECT * EXCEPT(rn)
FROM latest
WHERE rn = 1
