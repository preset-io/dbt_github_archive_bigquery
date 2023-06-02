SELECT
  A.email
  , A.chameleon_id
  , B.user_id
FROM {{ ref('latest_chameleon_profiles') }} AS A
INNER JOIN {{ ref('manager_preset_user_latest') }} AS B
  ON A.email = B.email
