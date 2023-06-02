SELECT
    A.ds,
    A.id,
    A.workspace_id,
    A.team_id,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id AS superset_creator_user_id,
    B.manager_user_id AS creator_user_id,
    A.last_modified_user_id AS superset_last_modified_user_id,
    C.manager_user_id AS last_modified_user_id,
    A.name,
    A.description,
    A.type,
    A.crontab,
    A.is_active,
    A.database_id,
    A.dashboard_id,
    A.chart_id,
    COALESCE(A.database_engine, D.engine) AS database_engine,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_report_schedule_dedup') }} AS A
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS B
    ON A.workspace_id = B.workspace_id
        AND A.creator_user_id = B.superset_user_id
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS C
    ON A.workspace_id = C.workspace_id
        AND A.last_modified_user_id = C.superset_user_id
LEFT JOIN {{ ref('src_superset_database_connection_dedup') }} AS D
    ON A.ds = D.ds
        AND A.workspace_id = D.workspace_id
        AND A.database_id = D.id
WHERE A.ds >= {{ var("start_date") }}
