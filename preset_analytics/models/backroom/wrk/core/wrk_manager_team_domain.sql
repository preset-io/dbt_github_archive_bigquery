WITH domain_agg AS (
  SELECT
    A.ds,
    A.team_id,
    B.email_domain,
    SUM(CASE WHEN {{ is_generic_email_condition("B.email_domain") }} THEN 0.1 ELSE 1 END) AS domain_count
  FROM {{ ref('src_manager_team_membership') }} AS A
  INNER JOIN {{ ref('wrk_manager_user') }} AS B
    ON A.ds = B.ds
      AND A.user_id = B.user_id
  GROUP BY 1, 2, 3
),

domain_window AS (
  SELECT
    ds,
    team_id,
    email_domain,
    domain_count,
    RANK() OVER (PARTITION BY ds, team_id ORDER BY domain_count DESC) AS domain_rank,
  FROM domain_agg
)

SELECT
  ds,
  team_id,
  MAX(email_domain) AS primary_email_domain,
FROM domain_window
WHERE domain_rank = 1
GROUP BY 1, 2
