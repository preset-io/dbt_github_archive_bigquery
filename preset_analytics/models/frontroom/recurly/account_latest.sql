SELECT A.*
FROM {{ ref('src_account_history') }} AS A
JOIN (
    SELECT account_id, MAX(account_updated_at) AS account_updated_at
    FROM {{ ref('src_account_history') }} AS A
    GROUP BY 1
) AS B ON A.account_id = B.account_id AND A.account_updated_at = B.account_updated_at
