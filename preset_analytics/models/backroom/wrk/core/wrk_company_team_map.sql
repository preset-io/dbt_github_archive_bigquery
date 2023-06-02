WITH company_team_user_count AS (
  SELECT
    hs_company_id AS company_id,
    team_id,
    COUNT(1) AS association_count
  FROM {{ ref('wrk_manager_user_latest') }} AS A
  INNER JOIN {{ ref('manager_team_membership_latest') }} AS B
    ON A.user_id = B.user_id
  WHERE A.user_id NOT IN (SELECT user_id FROM {{ ref('src_manager_preset_user') }} WHERE is_preset)
  GROUP BY 1, 2
),

company_contact AS (
  SELECT
    ce_company_id AS company_id,
    COUNT(DISTINCT contact_id) AS company_contact_count,
  FROM {{ ref('hubspot__contacts') }}
  GROUP BY 1
),

company_deal AS (
  SELECT
    company_id,
    num_associated_deals,
    recent_deal_amount,
  FROM {{ ref('hubspot__companies') }}
),

team_user AS (
  SELECT
    team_id,
    COUNT(DISTINCT user_id) AS team_member_count
  FROM {{ ref('manager_team_membership_latest') }}
  WHERE user_id NOT IN (SELECT user_id FROM {{ ref('src_manager_preset_user') }} WHERE is_preset)
  GROUP BY 1
)

SELECT
  A.company_id,
  A.team_id,
  B.company_contact_count,
  C.team_member_count,
  A.association_count,
  D.num_associated_deals,
  D.recent_deal_amount,
  ROW_NUMBER() OVER (PARTITION BY A.company_id ORDER BY C.team_member_count DESC) AS team_rank_for_company,
  ROW_NUMBER() OVER (PARTITION BY A.company_id ORDER BY C.team_member_count DESC) = 1 AS is_primary_team_for_company,
  ROW_NUMBER() OVER (PARTITION BY A.team_id ORDER BY D.num_associated_deals DESC, D.recent_deal_amount DESC, B.company_contact_count DESC) AS company_rank_for_team,
  ROW_NUMBER() OVER (PARTITION BY A.team_id ORDER BY D.num_associated_deals DESC, D.recent_deal_amount DESC, B.company_contact_count DESC) = 1 AS is_primary_company_for_team,
FROM company_team_user_count AS A
INNER JOIN company_contact AS B
  ON CAST(A.company_id AS STRING) = CAST(B.company_id AS STRING)
INNER JOIN team_user AS C
  ON A.team_id = C.team_id
LEFT JOIN company_deal AS D
  ON CAST(A.company_id AS STRING) = CAST(D.company_id AS STRING)
