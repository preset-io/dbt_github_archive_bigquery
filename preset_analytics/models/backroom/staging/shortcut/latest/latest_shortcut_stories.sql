{{ config(
    materialized='table'
) }}

WITH latest AS (
  SELECT *, story_name AS name, row_number() over (partition by id order by loaded_at DESC) AS rn
  FROM {{ ref('src_shortcut_stories') }}
)

SELECT * EXCEPT(rn)
FROM latest
WHERE rn = 1
