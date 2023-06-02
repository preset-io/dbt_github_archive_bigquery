{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

SELECT
  ds.dt,
  COALESCE(
    REPLACE(
      NET.REG_DOMAIN(pv.referrer),
      NET.PUBLIC_SUFFIX(pv.referrer),
      ''),
    "unknown"
  ) AS root_domain,
  COALESCE(NET.REG_DOMAIN(pv.referrer), "unknown") AS domain,
  COALESCE(NET.PUBLIC_SUFFIX(pv.referrer), "unknown") AS suffix,
  COALESCE(NET.HOST(pv.referrer), "unknown") AS host,
  COUNT(*) AS num_page_views,
  COUNT(DISTINCT pv.anonymous_id) AS num_distinct_visitors
FROM date_spine AS ds
LEFT JOIN {{ ref('wrk_segment_unified_page_views') }} AS pv
  ON ds.dt >= DATE(pv.original_timestamp)
GROUP BY 1, 2, 3, 4, 5
