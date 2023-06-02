{{ config(materialized='table') }}
-- Early on, we didn't log manager_user_id properly
-- This creates a lookup so we can assign early events to the right people
SELECT
    workspace_id,
    superset_user_id,
    MAX(manager_user_id) AS manager_user_id,
FROM {{ ref('wrk_superset_events') }}
WHERE manager_user_id IS NOT NULL
    AND workspace_id IS NOT NULL
GROUP BY 1, 2
