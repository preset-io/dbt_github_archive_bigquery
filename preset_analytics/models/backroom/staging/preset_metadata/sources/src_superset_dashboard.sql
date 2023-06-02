WITH chart_dashboard_agg AS (
    SELECT
        A.ds,
        A.workspace_id,
        A.dashboard_id,
        COUNT(DISTINCT B.id) AS num_charts
    FROM {{ ref('src_superset_dashboard_chart_dedup') }} AS A
    LEFT JOIN {{ ref('src_superset_slice_dedup') }} AS B
        ON A.ds = B.ds
            AND A.workspace_id = B.workspace_id
            AND A.slice_id = B.id
    GROUP BY 1, 2, 3
)

SELECT
    CONCAT(A.workspace_id, '_', A.id) AS dashboard_key,
    A.id AS dashboard_id,
    A.description AS dashboard_description,
    CASE
        WHEN LOWER(A.dashboard_title) LIKE '%untitled%' THEN TRUE ELSE A.is_example
    END AS is_untitled,
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
    A.dashboard_title,
    A.description,
    A.slug,
    COALESCE(
        A.is_example,
        A.creator_user_id IS NULL OR A.creator_user_id = 1
    ) AS is_example,
    A.json_metadata,
    NULLIF(NULLIF(JSON_VALUE(A.json_metadata, '$.color_scheme'),'""'),'') AS selected_color_scheme,
    COALESCE(A.chart_count, D.num_charts) AS chart_count,
    json_metadata like '%NATIVE_FILTER-%' as has_filter,
    json_metadata like '%NATIVE_FILTER_DIVIDER-%' as has_divider,
    json_metadata like '%NATIVE_FILTER-%' and json_metadata like '%NATIVE_FILTER_DIVIDER-%' as has_both_filter_divider,
    json_metadata like '%NATIVE_FILTER-%' and not json_metadata like '%NATIVE_FILTER_DIVIDER-%' as has_only_filter,
    not json_metadata like '%NATIVE_FILTER-%' and json_metadata like '%NATIVE_FILTER_DIVIDER-%' as has_only_divider,
    A.published,
    A.uuid,
    A.certified_by,
    A.certification_details,
    A.is_managed_externally,
    A.ds AS effective_from,
    LEAD(A.ds) OVER (PARTITION BY A.id, A.workspace_id ORDER BY A.ds) AS effective_to,
FROM {{ ref('src_superset_dashboard_dedup') }} AS A
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS B
    ON A.workspace_id = B.workspace_id
        AND A.creator_user_id = B.superset_user_id
LEFT JOIN {{ ref('wrk_manager_user_lookup') }} AS C
    ON A.workspace_id = C.workspace_id
        AND A.last_modified_user_id = C.superset_user_id
LEFT JOIN chart_dashboard_agg AS D
    ON A.ds = D.ds
        AND A.workspace_id = D.workspace_id
        AND A.id = D.dashboard_id
WHERE A.ds >= {{ var("start_date") }}
