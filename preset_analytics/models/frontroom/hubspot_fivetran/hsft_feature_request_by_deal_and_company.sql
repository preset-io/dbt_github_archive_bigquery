{{ config(materialized='table') }}
SELECT
  Z.join_type,
  -- Fields likely to be used
	A.*,

  B.deal_id,
  B.deal_name,
  B.deal_amount,
  B.deal_stage_short,

  {% for s in ['hs_company_name', 'hs_company_id']%}
    COALESCE(C.{{ s }}, CC.{{ s }}) AS {{ s }},
  {% endfor %}
FROM {{ ref('wrk_feature_request_join') }} AS Z
JOIN {{ ref('hsft_feature_requests') }} AS A ON A.feature_request_id = Z.feature_request_id
LEFT JOIN {{ ref('hubspot_deal') }} AS B ON Z.deal_id = B.deal_id
LEFT JOIN {{ ref('hs_companies') }} AS C ON Z.company_id = C.hs_company_id
LEFT JOIN {{ ref('hs_companies') }} AS CC ON B.company_id = CC.hs_company_id
