{{ config(materialized='table') }}

SELECT
    COALESCE(A.team_id, B.team_id) AS team_id,
    COALESCE(A.dt, B.dt) AS dt,
    CASE WHEN B.db_created_count_that_day > 0 THEN 'success' ELSE 'failure' END AS outcome,
    A.attempts,
    A.validate_parameters,
    A.test_connection,
    A.attempt_engines,
    A.manager_user_id,
    F.hs_contact_id,
    B.created_database_engine_that_day,
    B.db_created_count_that_day,
    D.created_database_engine_all_time,
    D.db_created_count_all_time,
    E.db_modified_count_that_day,
    {{ team_attributes(alias="C", include_team_id=False) }}
FROM (
        SELECT
            team_id,
            DATE(dttm) AS dt,
            manager_user_id,
            ARRAY_AGG(DISTINCT database_engine IGNORE NULLS) AS attempt_engines,
            COUNT(*) AS attempts,
            SUM(CASE WHEN action LIKE '%validate_parameters%' THEN 1 ELSE 0 END) AS validate_parameters,
            SUM(CASE WHEN action LIKE '%test_connection%' THEN 1 ELSE 0 END) AS test_connection,
        FROM {{ ref('superset_event_log') }}
        WHERE (action LIKE '%validate_parameters%' OR action LIKE '%test_connection%')
            AND dttm >= DATE('2021-08-18')
        GROUP BY 1, 2, 3
) AS A
LEFT JOIN (
        SELECT
            team_id,
            DATE(created_dttm) AS dt,
            COUNT(*) AS db_created_count_that_day,
            ARRAY_AGG(DISTINCT database_engine IGNORE NULLS) AS created_database_engine_that_day,
        FROM {{ ref('superset_database_connection') }}
        WHERE created_dttm >= DATE('2021-08-18')
            AND NOT is_example
        GROUP BY 1, 2
) AS B
ON A.team_id = B.team_id
    AND A.dt = B.dt
LEFT JOIN (
        SELECT
            team_id,
            COUNT(*) AS db_created_count_all_time,
            ARRAY_AGG(DISTINCT database_engine IGNORE NULLS) AS created_database_engine_all_time,
        FROM {{ ref('superset_database_connection') }}
        WHERE NOT is_example
        GROUP BY 1
) AS D
ON A.team_id = D.team_id
LEFT JOIN {{ ref('manager_team') }} AS C
    ON A.team_id = C.team_id
LEFT JOIN (
        SELECT
            team_id,
            DATE(last_modified_dttm) AS dt,
            COUNT(*) AS db_modified_count_that_day,
            ARRAY_AGG(DISTINCT database_engine IGNORE NULLS) AS modified_database_engine_that_day,
        FROM {{ ref('superset_database_connection') }}
        WHERE created_dttm >= DATE('2021-08-18')
            AND NOT is_example
        GROUP BY 1, 2
) AS E
ON A.team_id = E.team_id
    AND A.dt = E.dt
LEFT JOIN {{ ref('manager_user') }} AS F ON A.manager_user_id = F.user_id
