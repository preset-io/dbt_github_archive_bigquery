SELECT *
FROM {{ ref('preset_teams') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('preset_teams') }})
