SELECT
    A.uuid,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    A.dashboard_id,
    A.allow_domain_list,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.uuid, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_embedded_dashboard_dedup') }} AS A
WHERE A.ds >= {{ var("start_date") }}
