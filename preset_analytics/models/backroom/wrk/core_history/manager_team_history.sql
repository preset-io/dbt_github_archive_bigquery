{{ config(
    materialized='table',
) }}

SELECT
  A.*,
  LAG(arr, 364) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS arr_364_days_ago,
  LAG(arr, 182) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS arr_182_days_ago,
  LAG(arr, 91) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS arr_91_days_ago,
  LAG(recurly_arr, 364) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS recurly_arr_364_days_ago,
  LAG(recurly_arr, 182) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS recurly_arr_182_days_ago,
  LAG(recurly_arr, 91) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS recurly_arr_91_days_ago,
  LAG(sales_led_arr, 364) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS sales_led_arr_364_days_ago,
  LAG(sales_led_arr, 182) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS sales_led_arr_182_days_ago,
  LAG(sales_led_arr, 91) OVER (PARTITION BY A.team_id ORDER BY ds ASC) AS sales_led_arr_91_days_ago,
  CASE
    WHEN max_sales_led_arr > 0 THEN 'SALES_LED'
    WHEN max_recurly_arr > 0 THEN 'SELF_SERVE'
    ELSE ''
  END AS nrr_attribution,
FROM {{ ref('wrk_manager_team_history') }} AS A
LEFT JOIN (
  SELECT
    team_id,
    MAX(sales_led_arr) AS max_sales_led_arr,
    MAX(recurly_arr) AS max_recurly_arr,
  FROM {{ ref('wrk_manager_team_history') }}
  GROUP BY 1
) AS B ON A.team_id = B.team_id
