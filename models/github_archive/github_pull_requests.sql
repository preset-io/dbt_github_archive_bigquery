{{ config(materialized='table') }}
SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    pr_id,
    repo.name AS repo,
    community,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.number') AS pr_number,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.html_url') AS pr_url,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.state') AS pr_state,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.title') AS pr_title,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') AS pr_is_merged,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged_by') AS pr_merged_by,
    JSON_EXTRACT_ARRAY(payload, '$.pull_request.labels') AS pr_labels,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged_mergeable_state') AS pr_mergeable_state,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.commits') AS INT) AS pr_commits,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.additions') AS INT) AS pr_additions,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.deletions') AS INT) AS pr_deletions,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.changed_files') AS INT) AS pr_changed_files,
    CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.reactions.total_count') AS INT) AS pr_reactions,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.comments') AS INT) AS pr_comments,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.review_comments') AS INT) AS pr_review_comments,
    ROW_NUMBER() OVER(PARTITION BY pr_id ORDER BY created_at DESC) AS row_num,
    FIRST_VALUE(created_at) OVER(PARTITION BY pr_id ORDER BY created_at DESC) AS modified_at,
    FIRST_VALUE(actor.login) OVER(PARTITION BY pr_id ORDER BY created_at) AS pr_author,
  FROM {{ ref("__raw_github_events")}}
  WHERE pr_id IS NOT NULL AND `type` in  ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent')
) AS qry
WHERE row_num = 1
