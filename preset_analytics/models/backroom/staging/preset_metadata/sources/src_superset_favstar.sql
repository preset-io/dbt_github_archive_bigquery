SELECT
    A.ds,
    A.id,
    A.workspace_id,
    A.user_id,
    A.class_name,
    A.obj_id,
    A.dttm,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_favstar_dedup') }} AS A
WHERE A.ds >= {{ var("start_date") }}
