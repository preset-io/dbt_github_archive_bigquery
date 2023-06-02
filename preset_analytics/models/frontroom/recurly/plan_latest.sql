SELECT A.*
FROM {{ ref('src_plan_history') }} AS A
JOIN (
    SELECT plan_id, MAX(plan_updated_at) AS plan_updated_at
    FROM {{ ref('src_plan_history') }} AS A
    GROUP BY 1
) AS B ON A.plan_id = B.plan_id AND A.plan_updated_at = B.plan_updated_at
