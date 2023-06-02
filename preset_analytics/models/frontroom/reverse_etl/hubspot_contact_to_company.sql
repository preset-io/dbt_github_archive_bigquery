SELECT
    CONCAT(user_id, '_', hs_company_id) AS pk,
    CAST(user_id AS STRING) AS user_id,
    hs_company_id,
FROM {{ ref('manager_user') }}
WHERE hs_contact_id IS NOT NULL
    AND hs_company_id IS NOT NULL
