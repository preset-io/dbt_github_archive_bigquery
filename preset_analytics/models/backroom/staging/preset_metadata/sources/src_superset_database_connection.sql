SELECT
    A.ds,
    A.id,
    A.uuid,
    A.workspace_id,
    A.team_id,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id AS superset_creator_user_id,
    B.manager_user_id AS creator_user_id,
    A.last_modified_user_id AS superset_last_modified_user_id,
    C.manager_user_id AS last_modified_user_id,
    A.impersonate_user,
    A.name,
    A.verbose_name,
    A.engine,
    A.is_example,
    A.cache_timeout,
    A.allow_dml,
    A.allow_csv_upload,
    A.allow_ctas,
    A.allow_run_async,
    A.allow_multi_schema_metadata_fetch,
    A.allow_cvas,
    A.select_as_create_table_as,
    A.expose_in_sqllab,
    A.force_ctas_schema,
    A.configuration_method,
    A.is_managed_externally,
    A.has_ssh_tunnel,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_database_connection_dedup') }} AS A
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS B
    ON A.workspace_id = B.workspace_id
        AND A.creator_user_id = B.superset_user_id
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS C
    ON A.workspace_id = C.workspace_id
        AND A.last_modified_user_id = C.superset_user_id
WHERE A.ds >= {{ var("start_date") }}
