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
    A.is_certified,
    A.dataset_type,
    A.database_id,
    COALESCE(A.database_engine, D.engine) AS database_engine,
    A.main_dttm_col,
    A.offset,
    A.is_featured,
    A.cache_timeout,
    A.schema,
    A.filter_select_enabled,
    A.is_sqllab_view,
    A.uuid,
    A.is_managed_externally,
    -- AS column_count,
    COALESCE(A.column_count, COUNT(E.id) OVER (PARTITION BY A.ds, A.workspace_id, A.id)) AS metric_count,
    -- AS calc_col_count,
    A.creator_user_id = 1 OR A.creator_user_id IS NULL AS is_example,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_dataset_dedup') }} AS A
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
LEFT JOIN {{ ref('src_superset_sql_metric_dedup') }} AS E
    ON A.ds = E.ds
        AND A.workspace_id = E.workspace_id
        AND A.id = E.table_id
WHERE A.ds >= {{ var("start_date") }}
