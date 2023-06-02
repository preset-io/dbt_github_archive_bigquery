SELECT
    ------PK------
    A.ds,
    CONCAT(A.workspace_id, '_', A.id) AS chart_key,
    --------------

    A.id AS chart_id,
    A.slice_name AS chart_name,
    CONCAT(A.workspace_id, '_', A.datasource_id) AS dataset_key,
    CONCAT(A.workspace_id, '_', D.database_id) AS database_key,
    COALESCE(A.datasource_name, D.name) AS dataset_name,

    A.workspace_id,
    A.team_id,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id AS superset_creator_user_id,
    A.last_modified_user_id AS superset_last_modified_user_id,
    B.manager_user_id AS creator_user_id,
    C.manager_user_id AS last_modified_user_id,
    A.description,
    A.viz_type,
    COALESCE(
        A.is_example,
        A.creator_user_id IS NULL OR A.creator_user_id = 1
    ) AS is_example,
    A.datasource_id,
    COALESCE(A.datasource_name, D.name) AS datasource_name,
    COALESCE(A.database_id, D.database_id) AS database_id,
    COALESCE(A.database_engine, E.engine) AS database_engine,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,

FROM {{ ref('src_superset_slice_dedup') }} AS A
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS B
    ON A.workspace_id = B.workspace_id
        AND A.creator_user_id = B.superset_user_id
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS C
    ON A.workspace_id = C.workspace_id
        AND A.last_modified_user_id = C.superset_user_id
LEFT JOIN {{ ref('src_superset_dataset_dedup') }} AS D
    ON A.ds = D.ds
        AND A.workspace_id = D.workspace_id
        AND A.datasource_id = D.id
LEFT JOIN {{ ref('src_superset_database_connection_dedup') }} AS E
    ON A.ds = E.ds
        AND A.workspace_id = E.workspace_id
        AND A.database_id = E.id
WHERE A.ds >= {{ var("start_date") }}
