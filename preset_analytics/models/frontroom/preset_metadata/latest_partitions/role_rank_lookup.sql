SELECT
  DISTINCT
  role_rank,
  role_identifier,
FROM {{ ref('manager_workspace_membership_latest') }}
