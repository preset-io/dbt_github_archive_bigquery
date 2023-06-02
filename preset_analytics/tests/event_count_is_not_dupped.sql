SELECT
  A.dt,
  SUM(actions_wrk_log),
  SUM(actions_log),
  SUM(actions_log) / SUM(actions_wrk_log) AS  ratio
FROM (
  SELECT CAST(dttm AS DATE) AS dt, COUNT(1) AS actions_wrk_log
  FROM {{ ref('wrk_superset_events') }}
  GROUP BY 1
  ORDER BY 1 DESC
) AS A
JOIN (
  SELECT dt, COUNT(1) AS actions_log
  FROM {{ ref('superset_event_log') }}
  GROUP BY 1
  ORDER BY 1 DESC
) AS B ON A.dt = B.dt
GROUP BY 1
HAVING SUM(actions_wrk_log) <> SUM(actions_log)
