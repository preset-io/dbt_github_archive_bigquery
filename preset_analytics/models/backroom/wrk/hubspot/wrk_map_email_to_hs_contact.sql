SELECT
    LOWER(email) AS email,
    contact_id,
FROM {{ ref('hubspot__contacts') }}
WHERE email IS NOT NULL
