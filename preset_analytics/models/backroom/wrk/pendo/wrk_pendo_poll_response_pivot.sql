-- Lookup here https://docs.google.com/spreadsheets/d/1k2RB8a0efZv8rvJ0kRWtrxrmJyebCPxbU59gDtmmO1A/edit#gid=0

WITH distinct_response AS (
  SELECT
    user_id,
    MAX(CASE WHEN question_id = '5s29i0pfe1h' THEN response END) AS pendo_used_superset,
    MAX(CASE WHEN question_id = 'm4xdm240nu' THEN response END) AS pendo_department,
    MAX(CASE WHEN question_id = 'mcyh7sxmqr' THEN response END) AS pendo_role,
  FROM {{ ref('poll_response') }}
  GROUP BY 1
)

SELECT
  user_id,
  COALESCE(pendo_used_superset, 'Unknown') AS pendo_used_superset,
  COALESCE(NULLIF(pendo_department, 'Select an option'), 'Unknown') AS pendo_department,
  COALESCE(NULLIF(pendo_role, 'Select an option'), 'Unknown') AS pendo_role, -- added logic
FROM distinct_response
