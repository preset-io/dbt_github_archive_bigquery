{{ config(
  enabled=false
) }}

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ip_address ORDER BY processed_dttm DESC) AS rn
  FROM {{ ref('ip_address_w_geo_stage') }}
)

SELECT
  ip_address,
  processed_dttm,
  geo_raw,
  -- unnested fields from the raw json response
  geo_raw.IPv4 AS ipv4,
  geo_raw.city,
  geo_raw.country_code,
  geo_raw.country_name,
  geo_raw.latitude,
  geo_raw.longitude,
  geo_raw.postal,
  geo_raw.state,
FROM ranked
WHERE rn = 1
