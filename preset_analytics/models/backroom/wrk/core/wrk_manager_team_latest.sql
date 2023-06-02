SELECT
    *
FROM {{ ref('wrk_manager_team') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('wrk_manager_team') }})
