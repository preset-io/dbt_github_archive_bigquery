-- Charts, to count the number of ACTIVE (consumed) charts
SELECT
    'chart' AS entity_type,
    A.dt,
    A.team_id,
    A.workspace_hash,
    A.chart_key AS entity_id,
    B.is_example AS is_example,
    A.event_id,
FROM {{ ref('_wrk_egaf_event_base') }} AS A
INNER JOIN {{ ref('superset_slice_latest') }} AS B
    ON A.chart_key = B.chart_key
WHERE A.chart_key IS NOT NULL
