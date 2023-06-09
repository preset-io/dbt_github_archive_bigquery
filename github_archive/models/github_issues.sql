{{ config(materialized='table') }}
SELECT * EXCEPT(row_num)
FROM (
  SELECT
    issue_id,
    repo.name AS repo,
    community,

    JSON_EXTRACT_SCALAR(payload, '$.issue.number') AS issue_number,
    JSON_EXTRACT_SCALAR(payload, '$.issue.html_url') AS issue_url,
    JSON_EXTRACT_SCALAR(payload, '$.issue.state') AS issue_state,
    JSON_EXTRACT_SCALAR(payload, '$.issue.title') AS issue_title,
    JSON_EXTRACT_ARRAY(payload, '$.issue.labels') AS issue_labels,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.reactions.total_count') AS INT) AS issue_reactions,

    ROW_NUMBER() OVER(PARTITION BY issue_id ORDER BY created_at DESC) AS row_num,
    FIRST_VALUE(created_at) OVER(PARTITION BY issue_id ORDER BY created_at) AS created_at,
    FIRST_VALUE(CASE WHEN JSON_EXTRACT_SCALAR(payload, '$.issue.state') = 'closed' THEN created_at END IGNORE NULLS) OVER(PARTITION BY issue_id ORDER BY created_at) AS closed_at,
    FIRST_VALUE(created_at) OVER(PARTITION BY issue_id ORDER BY created_at DESC) AS last_event_at,
  FROM {{ ref("__raw_github_events")}}
  WHERE issue_id IS NOT NULL AND `type` IN  ('IssuesEvent', 'IssueCommentEvent')
) AS qry
WHERE row_num = 1
