{{ config(materialized='table') }}
SELECT
  B.feature_request_id,
  A.join_type,
  A.company_id,
  A.deal_id,

FROM (
  SELECT
    A.from_id AS feature_request_id,
    B.id AS company_id,
    NULL AS deal_id,
    A.`type` AS join_type,
  FROM {{ source('fivetran_hubspot', 'feature_requests_to_company') }} AS A
  JOIN {{ source('fivetran_hubspot', 'company') }} AS B ON A.to_id = B.property_hs_object_id
  UNION ALL
  SELECT
    A.from_id AS feature_request_id,
    NULL AS company_id,
    B.deal_id,
    A.`type` AS join_type,
  FROM {{ source('fivetran_hubspot', 'feature_requests_to_deal') }} AS A
  JOIN {{ source('fivetran_hubspot', 'deal') }} AS B ON A.to_id = B.property_hs_object_id
) AS A
JOIN {{ ref('hsft_feature_requests') }} AS B ON A.feature_request_id = B.hs_object_id
