{{
    config(
        materialized='table',
    )
}}

WITH team_users AS (
    SELECT
        team_id,
        user_id,
        created_dttm, -- WIP: could be used to create a team over time view
        has_superset_experience,
        role,
        department
    FROM {{ ref('manager_user') }} AS user
    CROSS JOIN UNNEST(user.team_id_array) AS team_id
)

SELECT
    tu.team_id,
    w.workspace_title,
    tu.user_id,
    tu.created_dttm,
    tu.has_superset_experience AS pendo_used_superset,
    tu.role AS pendo_department,
    tu.department AS pendo_role
FROM team_users AS tu
LEFT JOIN {{ ref('manager_workspace_membership_latest') }} AS wm
    ON tu.user_id = wm.user_id
LEFT JOIN {{ ref('manager_workspace') }} AS w
    ON wm.workspace_id = w.workspace_id
