SELECT
    A.ds,
    A.id,
    A.workspace_id,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    A.name,
    A.verbose_name,
    A.metric_type,
    A.table_id,
    A.d3format,
    A.uuid,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_sql_metric_dedup') }} AS A
WHERE A.ds >= {{ var("start_date") }}
