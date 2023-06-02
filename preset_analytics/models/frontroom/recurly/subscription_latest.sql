SELECT A.*
FROM {{ ref('src_subscription_history') }} AS A
JOIN (
    SELECT subscription_id, MAX(subscription_updated_at) AS subscription_updated_at
    FROM {{ ref('src_subscription_history') }} AS A
    GROUP BY 1
) AS B ON A.subscription_id = B.subscription_id AND A.subscription_updated_at = B.subscription_updated_at
