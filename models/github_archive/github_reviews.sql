{{ config(materialized='table') }}
SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    review_id,
    JSON_EXTRACT_SCALAR(payload, '$.review.state') AS review_state,
    JSON_EXTRACT_SCALAR(payload, '$.review.user.login') AS reviewer_username,

    B.* EXCEPT(pr_id),
    ROW_NUMBER() OVER(PARTITION BY review_id ORDER BY created_at DESC) AS row_num,

  FROM (
    SELECT
      JSON_EXTRACT_SCALAR(payload, '$.review.id') AS review_id,
      *
    FROM {{ ref("__raw_github_events")}}
    WHERE pr_id IS NOT NULL AND `type` in  ('PullRequestReviewEvent')
  ) AS A
  JOIN {{ ref("github_pull_requests")}} AS B ON A.pr_id = B.pr_id
  WHERE review_id IS NOT NULL
) AS qry
WHERE qry.row_num = 1
