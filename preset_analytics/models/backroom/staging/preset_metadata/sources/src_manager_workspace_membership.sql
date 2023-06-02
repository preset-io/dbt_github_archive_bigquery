SELECT
  A.ds,
  A.id,
  A.created_dttm,
  A.last_modified_dttm,
  A.creator_user_id,
  A.last_modified_user_id,
  A.user_id,
  A.workspace_id,
  A.role_identifier AS role_identifier_raw,
  COALESCE(B.in_product_role_identifier, A.role_identifier) AS role_identifier,
  A.group_id,
  CASE
    WHEN role_identifier = 'Admin' THEN 100
    WHEN role_identifier = 'PresetAlpha' THEN 50
    WHEN role_identifier = 'PresetNoAccess' THEN 0
    WHEN role_identifier = 'Admin' THEN 100
    WHEN role_identifier = 'PresetReportsOnly' THEN 10
    WHEN role_identifier = 'PresetDashboardsOnly' THEN 20
    WHEN role_identifier = 'PresetGamma' THEN 40
    WHEN role_identifier = 'PresetBeta' THEN 45
  END AS role_rank,
FROM {{ ref('src_manager_workspace_membership_dedup') }} AS A
LEFT JOIN {{ ref('seed_workspace_role_mappings') }} AS B
  ON A.role_identifier = B.raw_role_identifier
WHERE A.ds >= {{ var("start_date") }}
