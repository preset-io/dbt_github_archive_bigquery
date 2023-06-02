{{ config(materialized='table') }}
SELECT
  dt,
  team_id,
  SUM(arr) AS arr,
  SUM(arr) / 12 AS mrr,
  SUM(sold_seats) AS seats,
  SUM(number_of_creator_licenses) AS number_of_creator_licenses,
  SUM(number_of_viewer_licenses) AS number_of_viewer_licenses,
  SUM(number_of_embedded_view_licenses) AS number_of_embedded_view_licenses,
  COUNT(
    CASE
      WHEN deal_type LIKE 'renewal' AND deal_stage_short NOT LIKE '%closed%'
        THEN deal_id
    END
    ) > 0 AS has_open_renewal_deal
FROM {{ ref('wrk_hubspot_deal_history') }} AS A
INNER JOIN {{ ref('wrk_company_team_map') }} AS B
  ON team_rank_for_company = 1
  AND A.company_id = B.company_id
GROUP BY 1, 2
HAVING arr > 0
