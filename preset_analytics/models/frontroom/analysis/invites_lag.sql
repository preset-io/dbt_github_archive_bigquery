{{ config(materialized='table') }}
/*
	For each team (one row per team here!)
	Looking at timing around when they sent their first invite
*/
SELECT
  A.team_id,
  A.created_dttm AS team_created_dttm,
  B.created_dttm AS invite_created_dttm,
  A.is_preset,
  DATETIME_DIFF(B.created_dttm, A.created_dttm, HOUR) AS lag_hour,
  CASE
    WHEN B.created_dttm IS NULL THEN 'Z.NO_CONN'
    WHEN DATETIME_DIFF(B.created_dttm, A.created_dttm, HOUR) <1 THEN '1.<1H'
    WHEN DATETIME_DIFF(B.created_dttm, A.created_dttm, HOUR) <24 THEN '2.<1D'
    WHEN DATETIME_DIFF(B.created_dttm, A.created_dttm, HOUR) <24*7 THEN '3.<1W'
    ELSE '4>1W'
  END AS lag_cat,
FROM {{ ref('manager_team') }} AS A
LEFT OUTER JOIN (
  -- First non-example database created
  SELECT team_id, MIN(created_dttm) AS created_dttm
  FROM {{ ref('manager_invite') }}
  GROUP BY 1
) AS B ON A.team_id = B.team_id
